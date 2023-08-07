use std::time::Duration;
use futures::executor::block_on;

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};
use log::log;

use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};
use pallas::codec::minicbor::bytes::nil;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::{Batch, IVec};

use crate::{
    bootstrap, crosscut,
    model::{self, BlockContext},
    prelude::AppliesPolicy,
};

type InputPort = gasket::messaging::TwoPhaseInputPort<model::RawBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::EnrichedBlockPayload>;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub db_path: String,
}

impl Config {
    pub fn boostrapper(self, policy: &crosscut::policies::RuntimePolicy) -> Bootstrapper {
        Bootstrapper {
            config: self,
            policy: policy.clone(),
            input: Default::default(),
            output: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    input: InputPort,
    output: OutputPort,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            config: self.config,
            policy: self.policy,
            db: None,
            db_state_restoration: None,
            input: self.input,
            output: self.output,
            inserts_counter: Default::default(),
            remove_counter: Default::default(),
            matches_counter: Default::default(),
            mismatches_counter: Default::default(),
            blocks_counter: Default::default(),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(600)),
                ..Default::default()
            },
            Some("enrich-sled"),
        ));
    }
}

pub struct Worker {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    db: Option<sled::Db>,
    db_state_restoration: Option<sled::Tree>,
    input: InputPort,
    output: OutputPort,
    inserts_counter: gasket::metrics::Counter,
    remove_counter: gasket::metrics::Counter,
    matches_counter: gasket::metrics::Counter,
    mismatches_counter: gasket::metrics::Counter,
    blocks_counter: gasket::metrics::Counter,
}

struct SledTxValue(u16, Vec<u8>);

impl TryInto<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let SledTxValue(era, body) = self;
        minicbor::to_vec((era, body))
            .map(|x| IVec::from(x))
            .map_err(crate::Error::cbor)
    }
}

impl TryFrom<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let (tag, body): (u16, Vec<u8>) = minicbor::decode(&value).map_err(crate::Error::cbor)?;

        Ok(SledTxValue(tag, body))
    }
}

#[inline]
fn fetch_referenced_utxo<'a>(
    db: &sled::Db,
    utxo_ref: &OutputRef,
) -> Result<Option<(OutputRef, Era, Vec<u8>)>, crate::Error> {
    if let Some(ivec) = db
        .get(utxo_ref.to_string())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;
        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Ok(None)
    }
}

impl Worker {
    #[inline]
    fn insert_produced_utxos(&self, db: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let key: IVec = format!("{}#{}", tx.hash(), idx).as_bytes().into();

                let era = tx.era().into();
                let body = output.encode();
                let value: IVec = SledTxValue(era, body).try_into()?;

                insert_batch.insert(key, value)
            }
        }

        db.apply_batch(insert_batch)
            .map_err(crate::Error::storage)?;

        self.inserts_counter.inc(txs.len() as u64);

        Ok(())
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<BlockContext, crate::Error> {
        let mut ctx = BlockContext::default();

        let required: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.requires())
            .map(|input| input.output_ref())
            .collect();

        let matches: Result<Vec<_>, crate::Error> = required
            .par_iter()
            .map(|utxo_ref| fetch_referenced_utxo(db, utxo_ref))
            .collect();

        for m in matches? {
            if let Some((key, era, cbor)) = m {
                ctx.import_ref_output(&key, era, cbor);
                self.matches_counter.inc(1);
            } else {
                self.mismatches_counter.inc(1);
            }
        }

        Ok(ctx)
    }

    fn remove_consumed_utxos(&self, db: &sled::Db, t: &sled::Tree, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        let mut consumed_batch = Batch::default();
        let mut restore_batch = Batch::default();

        for key in keys.iter() {
            match db.get(key.to_string().as_bytes()) {
                Ok(current_value) => {
                    match current_value {
                        None => {}
                        Some(c) => { restore_batch.insert(key.to_string().as_bytes(), c) }
                    }

                }

                Err(_) => {}
            }

            consumed_batch.remove(key.to_string().as_bytes());
        }

        t.apply_batch(restore_batch).map_err(crate::Error::storage)?;
        db.apply_batch(consumed_batch).map_err(crate::Error::storage)?;

        self.remove_counter.inc(keys.len() as u64);

        Ok(())
    }

    fn restore_consumed_utxos(&self, db: &sled::Db, t: &sled::Tree, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        let mut restore_batch = sled::Batch::default();
        for key in keys.iter() {
            let current_value = block_on(t.get(key.to_string()).unwrap().as_bytes());
            match current_value {
                Ok(v) => {
                    match v {
                        None => {}
                        Some(old_value) => {
                            if !old_value.is_empty() {
                                log::warn!("overwriting old value back into play {}", key);
                                restore_batch.insert(key.to_string().as_bytes(), old_value);
                                if db.len() > 500 {
                                    db.pop_min().unwrap();
                                }

                            }

                        }

                    }

                }

                Err(_) => {}
            }

        }

        db.apply_batch(restore_batch).unwrap();

        Ok(())
    }

}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("enrich_inserts", &self.inserts_counter)
            .with_counter("enrich_removes", &self.remove_counter)
            .with_counter("enrich_matches", &self.matches_counter)
            .with_counter("enrich_mismatches", &self.mismatches_counter)
            .with_counter("enrich_blocks", &self.blocks_counter)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        let db = self.db.as_ref().unwrap();
        let restoration_db = self.db_state_restoration.as_ref().unwrap();

        match msg.payload {
            model::RawBlockPayload::RollForward(cbor) => {
                let block = MultiEraBlock::decode(&cbor)
                    .map_err(crate::Error::cbor)
                    .apply_policy(&self.policy)
                    .or_panic()?;

                let block = match block {
                    Some(x) => x,
                    None => return Ok(gasket::runtime::WorkOutcome::Partial),
                };


                let txs = block.txs();

                // first we insert new utxo produced in this block
                self.insert_produced_utxos(db, &txs).or_restart()?;

                // then we fetch referenced utxo in this block
                let ctx = self.par_fetch_referenced_utxos(db, &txs).or_restart()?;

                // and finally we remove utxos consumed by the block
                self.remove_consumed_utxos(db, restoration_db, &txs).or_restart()?;

                self.output
                    .send(model::EnrichedBlockPayload::roll_forward(cbor, ctx))?;

                self.blocks_counter.inc(1);
            }
            model::RawBlockPayload::RollBack(x, cbor) => {
                let block = MultiEraBlock::decode(&cbor)
                    .map_err(crate::Error::cbor)
                    .apply_policy(&self.policy)
                    .or_panic().unwrap();

                match block {
                    None => {}
                    Some(block) => {
                        let ctx = self.par_fetch_referenced_utxos(db, &block.txs()).or_restart()?;
                        self.restore_consumed_utxos(&db, &restoration_db, &block.txs()).unwrap();
                        self.output
                            .send(model::EnrichedBlockPayload::roll_forward(cbor, ctx))?;
                    }

                }


            }

        };

        self.input.commit();
        Ok(WorkOutcome::Partial)
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let db = sled::open(&self.config.db_path).or_retry()?;
        let restoration = db.open_tree("state_restoration").or_retry()?;
        self.db = Some(db);
        self.db_state_restoration = Some(restoration);

        Ok(())
    }

    fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        match &self.db {
            Some(db) => {
                db.flush().or_panic()?;
                Ok(())
            }
            None => Ok(()),
        }?;

        match &self.db_state_restoration {
            Some(tree) => {
                tree.flush().or_panic()?;
                Ok(())
            }
            None => Ok(()),
        }?;

        Ok(())
    }
}
