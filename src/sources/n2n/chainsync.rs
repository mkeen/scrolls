use std::io::Read;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::miniprotocols::chainsync::HeaderContent;
use pallas::network::miniprotocols::{blockfetch, chainsync, Point};

use gasket::error::AsWorkError;
use log::log;
use pallas::network::multiplexer::StdChannel;
use sled::IVec;

use crate::sources::n2n::transport::Transport;
use crate::{crosscut, model, sources::utils, storage, Error};

use crate::prelude::*;

fn to_traverse<'b>(header: &'b HeaderContent) -> Result<MultiEraHeader<'b>, Error> {
    MultiEraHeader::decode(
        header.variant,
        header.byron_prefix.map(|x| x.0),
        &header.cbor,
    )
    .map_err(Error::cbor)
}

pub type OutputPort = gasket::messaging::OutputPort<model::RawBlockPayload>;

pub struct Worker {
    address: String,
    min_depth: usize,
    policy: crosscut::policies::RuntimePolicy,
    chain_buffer: chainsync::RollbackBuffer,
    chain: crosscut::ChainWellKnownInfo,
    blocks: crosscut::historic::BufferBlocks,
    intersect: crosscut::IntersectConfig,
    cursor: storage::Cursor,
    finalize: Option<crosscut::FinalizeConfig>,
    chainsync: Option<chainsync::N2NClient<StdChannel>>,
    blockfetch: Option<blockfetch::Client<StdChannel>>,
    output: OutputPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        address: String,
        min_depth: usize,
        policy: crosscut::policies::RuntimePolicy,
        chain: crosscut::ChainWellKnownInfo,
        blocks: crosscut::historic::BufferBlocks,
        intersect: crosscut::IntersectConfig,
        finalize: Option<crosscut::FinalizeConfig>,
        cursor: storage::Cursor,
        output: OutputPort,
    ) -> Self {
        Self {
            address,
            min_depth,
            policy,
            chain,
            blocks,
            intersect,
            finalize,
            cursor,
            output,
            chainsync: None,
            blockfetch: None,
            block_count: Default::default(),
            chain_tip: Default::default(),
            chain_buffer: chainsync::RollbackBuffer::new(),
        }
    }

    fn on_roll_forward(
        &mut self,
        content: chainsync::HeaderContent,
    ) -> Result<(), gasket::error::Error> {
        // parse the header and extract the point of the chain

        let header = to_traverse(&content)
            .apply_policy(&self.policy)
            .or_panic()?;

        let header = match header {
            Some(x) => x,
            None => return Ok(()),
        };

        let point = Point::Specific(header.slot(), header.hash().to_vec());

        // track the new point in our memory buffer
        log::warn!("rolling forward to point {:?}", point);
        self.chain_buffer.roll_forward(point);

        Ok(())
    }

    fn on_rollback(&mut self, point: &Point) -> Result<(), gasket::error::Error> {
        // TODO SET TIP OR UNDO 141
        match self.chain_buffer.roll_back(point) {
            chainsync::RollbackEffect::Handled => {
                log::warn!("handled rollback within buffer {:?}", point);
                Ok(())
            }
            chainsync::RollbackEffect::OutOfScope => {
                // todo instead return "None" and just be normal
                self.blocks.enqueue_rollback_batch(point);
                if let Some(cbor) =  self.blocks.tip_block() {
                    if let Ok(block) = MultiEraBlock::decode(&cbor) {
                        self.chain_buffer.roll_forward(
                            Point::Specific(block.slot(), block.hash().to_vec())
                        ); // todo maybe remove this roll forward
                    }
                }

                log::warn!("rolling backward from point {:?}", point);

                Ok(())
            }
        }
    }

    fn request_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("requesting next block");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .request_next()
            .or_restart()?;

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.on_roll_forward(h)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.on_rollback(&p)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }

    fn await_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("awaiting next block (blocking)");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .recv_while_must_reply()
            .or_restart()?;

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.on_roll_forward(h)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.on_rollback(&p)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            _ => unreachable!("protocol invariant not respected in chain-sync state machine"),
        }
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let transport = Transport::setup(&self.address, self.chain.magic).or_retry()?;

        let mut chainsync = chainsync::N2NClient::new(transport.channel2);

        let start =
            utils::define_chainsync_start(&self.intersect, &mut self.cursor, &mut chainsync)
                .or_retry()?;

        let start = start.ok_or(Error::IntersectNotFound).or_panic()?;

        log::info!("chain-sync intersection is {:?}", start);

        self.chainsync = Some(chainsync);

        let blockfetch = blockfetch::Client::new(transport.channel3);

        self.blockfetch = Some(blockfetch);

        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        match self.chainsync.as_ref().unwrap().has_agency() {
            true => self.request_next()?,
            false => self.await_next()?,
        };

        // see if we have points that already reached certain depth
        let ready = self.chain_buffer.pop_with_depth(self.min_depth);


        let mut blocks_to_roll_back: Vec<Vec<u8>> = Vec::default();

        let mut started_rollback = false;

        match self.blocks.rollback_pop() {
            Ok(block) => match block {
                None => (),
                Some(block) => {
                    log::warn!("got block from rollback queue");
                    started_rollback = true;
                    self.output.send(model::RawBlockPayload::roll_back(block))?;
                }
            },
            Err(_) => {
                ()
            }
        }

        log::warn!("I am now working");

        if started_rollback {
            let depth = self.blocks.rollback_queue_depth();
            if crosscut::should_finalize(&self.finalize, &Point::Origin, depth > 0, started_rollback) {
                log::warn!("sending rollback done");

                return Ok(gasket::runtime::WorkOutcome::Done);
            }

        } else {
            for point in ready {
                log::debug!("requesting block fetch for point {:?}", point);

                let block = self
                    .blockfetch
                    .as_mut()
                    .unwrap()
                    .fetch_single(point.clone())
                    .or_restart()?;

                self.blocks.insert_block(&point, &block);

                self.output
                    .send(model::RawBlockPayload::roll_forward(block))?;

                self.block_count.inc(1);

                // evaluate if we should finalize the thread according to config
                let depth = self.blocks.rollback_queue_depth();
                if crosscut::should_finalize(&self.finalize, &point, depth > 0, started_rollback) {
                    log::warn!("sending done");

                    return Ok(gasket::runtime::WorkOutcome::Done);
                }

            }

        }

        // let rollback_to_point: Option<Point> = match self.chain_buffer.latest() {
        //     None => {
        //         match self.blocks.tip_block() {
        //             None => {None}
        //             Some(tip_block) => {
        //                 let block = MultiEraBlock::decode(tip_block.as_slice())
        //                     .map_err(crate::Error::cbor)
        //                     .apply_policy(&self.policy)
        //                     .or_panic()?;
        //
        //                 match block {
        //                     None => None,
        //                     Some(block) => Some(Point::Specific(block.slot(), block.hash().to_vec()))
        //                 }
        //             }
        //         }
        //
        //     },
        //     Some(_) => None
        // };


        // match rollback_to_point {
        //     None => {}
        //     Some(rollback_point) => {
        //         log::warn!("found {} rollback points points with required min depth -- also -- {}", ready.len(), rollback_point.slot_or_default());
        //     }
        // }






        // request download of blocks for confirmed points


        log::warn!("I am now saying i am busy");

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
