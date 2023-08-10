use std::collections::HashMap;
use log::{debug, error, info};
use pallas::codec::minicbor::bytes::nil;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{crosscut, model, prelude::*};
use crate::model::BlockContext;

use super::Reducer;

type InputPort = gasket::messaging::TwoPhaseInputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::CRDTCommand>;

pub struct Worker {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
    policy: crosscut::policies::RuntimePolicy,
    ops_count: gasket::metrics::Counter,
    last_block: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        reducers: Vec<Reducer>,
        input: InputPort,
        output: OutputPort,
        policy: crosscut::policies::RuntimePolicy,
    ) -> Self {
        Worker {
            reducers,
            input,
            output,
            policy,
            ops_count: Default::default(),
            last_block: Default::default(),
        }
    }

    fn reduce_block<'b>(
        &mut self,
        block: &'b Vec<u8>,
        ctx: &model::BlockContext,
    ) -> Result<(), gasket::error::Error> {
        let block = MultiEraBlock::decode(block)
            .map_err(crate::Error::cbor)
            .apply_policy(&self.policy)
            .or_panic()?;

        let block = match block {
            Some(x) => x,
            None => return Ok(()),
        };

        self.last_block.set(block.number() as i64);

        self.output.send(gasket::messaging::Message::from(
            model::CRDTCommand::block_starting(&block),
        ))?;

        for reducer in self.reducers.iter_mut() {
            reducer.reduce_block(&block, ctx, false, &mut self.output)?;
            self.ops_count.inc(1);
        }

        self.output.send(gasket::messaging::Message::from(
            model::CRDTCommand::block_finished(&block),
        ))?;

        Ok(())
    }

    fn reduce_rollback_blocks<'b>(
        &mut self,
        blocks: &'b Vec<Vec<u8>>,
        ctx: &'b Vec<model::BlockContext>,
    ) -> Result<(), gasket::error::Error> {
        let reversed_blocks = blocks.iter().rev();

        let mut reversed_contexts = ctx.clone();
        reversed_contexts.reverse();

        for (k, block) in reversed_blocks.enumerate() {
            let block = MultiEraBlock::decode(block)
                .map_err(crate::Error::cbor)
                .apply_policy(&self.policy);

            if let Ok(block) = block {
                if let Some(block) = block.as_ref() {
                    let default_context = BlockContext::default();

                    let context = match reversed_contexts.get(k) {
                        None => &default_context,
                        Some(context) => context
                    };

                    for reducer in self.reducers.iter_mut() {
                        reducer.reduce_block(block, context, true, &mut self.output)?;
                        self.ops_count.inc(1);
                    }
                }
            }
        }

        Ok(())
    }
}


impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("ops_count", &self.ops_count)
            .with_gauge("last_block", &self.last_block)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            model::EnrichedBlockPayload::RollForward(block, ctx) => {
                self.reduce_block(&block, &ctx)?
            }
            model::EnrichedBlockPayload::RollBack(blocks_to_rollback, contexts) => {
                error!("running rollback reducers {} {}", blocks_to_rollback.len(), contexts.len());
                self.reduce_rollback_blocks(&blocks_to_rollback, &contexts)?;
            }
        }

        self.input.commit();
        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
