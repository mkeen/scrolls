use futures::{StreamExt, TryFutureExt};
use pallas::ledger::traverse::MultiEraHeader;
use pallas::network::miniprotocols::chainsync::HeaderContent;
use pallas::network::miniprotocols::{blockfetch, chainsync, Point};

use gasket::error::AsWorkError;
use log::log;
use pallas::network::multiplexer::StdChannel;
use tonic::{IntoRequest, Response, Streaming};
use tonic::transport::Channel;
use utxorpc::proto::sync::v1::chain_sync_service_client::ChainSyncServiceClient;
use utxorpc::proto::sync::v1::{FollowTipRequest, FollowTipResponse};

use crate::sources::grpc::transport::Transport;
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
    intersect: crosscut::IntersectConfig,
    cursor: storage::Cursor,
    finalize: Option<crosscut::FinalizeConfig>,
    chainsync: Option<ChainSyncServiceClient<Channel>>,
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
            intersect,
            finalize,
            chainsync: None,
            cursor,
            output,
            block_count: Default::default(),
            chain_tip: Default::default(),
            chain_buffer: chainsync::RollbackBuffer::new(),
        }
    }

    // fn on_roll_forward(
    //     &mut self,
    //     content: chainsync::HeaderContent,
    // ) -> Result<(), gasket::error::Error> {
    //     // parse the header and extract the point of the chain
    //     // let header = to_traverse(&content)
    //     //     .apply_policy(&self.policy)
    //     //     .or_panic()?;
    //     //
    //     // let header = match header {
    //     //     Some(x) => x,
    //     //     None => return Ok(()),
    //     // };
    //     //
    //     //
    //     // let point = Point::Specific(header.slot(), header.hash().to_vec());
    //
    //     // track the new point in our memory buffer
    //     log::debug!("rolling forward to point {:?}", point);
    //     self.chain_buffer.roll_forward(point);
    //
    //     Ok(())
    // }

    // fn on_rollback(&mut self, point: &Point) -> Result<(), gasket::error::Error> {
    //     log::debug!("rolling block to point {:?}", point);
    //
    //     match self.chain_buffer.roll_back(point) {
    //         chainsync::RollbackEffect::Handled => {
    //             log::debug!("handled rollback within buffer {:?}", point);
    //         }
    //         chainsync::RollbackEffect::OutOfScope => {
    //             log::debug!("rollback out of buffer scope, sending event down the pipeline");
    //             self.output
    //                 .send(model::RawBlockPayload::roll_back(point.clone()))?;
    //         }
    //     }
    //
    //     Ok(())
    // }

    // fn request_next(&mut self) -> Result<(), gasket::error::Error> {
    //     log::info!("requesting next block");
    //
    //
    //
    //     // let next = self
    //     //     .chainsync
    //     //     .as_mut()
    //     //     .unwrap()
    //     //     .into_request()
    //     //     .or_restart()?;
    //     //
    //     //
    //     //
    //     // match next {
    //     //     chainsync::NextResponse::RollForward(h, t) => {
    //     //         self.on_roll_forward(h)?;
    //     //         self.chain_tip.set(t.1 as i64);
    //     //         Ok(())
    //     //     }
    //     //     chainsync::NextResponse::RollBackward(p, t) => {
    //     //         self.on_rollback(&p)?;
    //     //         self.chain_tip.set(t.1 as i64);
    //     //         Ok(())
    //     //     }
    //     //     chainsync::NextResponse::Await => {
    //     //         log::info!("chain-sync reached the tip of the chain");
    //     //         Ok(())
    //     //     }
    //     // }
    // }

    // fn await_next(&mut self) -> Result<(), gasket::error::Error> {
    //     log::info!("awaiting next block (blocking)");
    //
    //     match self.chainsync.unwrap().follow_tip(FollowTipRequest{
    //         intersect: vec![],
    //     }) {
    //         Ok(tonic::client::GrpcService(i)) => i,
    //         _ => unreachable!("protocol invariant not respected in chain-sync state machine"),
    //     }
    // }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let transport = Transport::setup(&self.address).unwrap();
        self.chainsync = Some(transport.channel6);
        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        println!("Working");
        // match self.chainsync.as_ref().unwrap().has_agency() {
        //     true => self.request_next()?,
        //     false => self.await_next()?,
        // };
        //
        // // see if we have points that already reached certain depth
        // let ready = self.chain_buffer.pop_with_depth(self.min_depth);
        log::debug!("found {} points with required min depth", "");

        // // request download of blocks for confirmed points
        // for point in ready {
        //     log!("Were in a point i guess")
        // }

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
