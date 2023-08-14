use std::cell::RefCell;
use std::future::IntoFuture;
use std::process::Output;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use futures::executor::block_on;
use futures::{join, TryFutureExt};
use futures::future::err;

use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};
use gasket::error::Error;
use log::{error, warn};

use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::{Db, IVec};

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
    pub consumed_ring_path: Option<String>,
    pub produced_ring_path: Option<String>,
}

impl Config {
    pub fn boostrapper(mut self, policy: &crosscut::policies::RuntimePolicy, blocks: &crosscut::historic::BlockConfig) -> Bootstrapper {
        self.consumed_ring_path = Some(blocks.consumed_ring_path.clone());
        self.produced_ring_path = Some(blocks.produced_ring_path.clone());

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
            consumed_ring: None,
            produced_ring: None,
            flushing: false,
            should_flush: false,
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
    consumed_ring: Option<sled::Db>,
    produced_ring: Option<sled::Db>,
    flushing: bool,
    should_flush: bool,
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
        .get(utxo_ref.to_string().as_bytes())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;
        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Ok(None)
    }
}

#[inline]
fn prune_tree(db: &sled::Db) {
    error!("pruning tree");

    let mut keys_to_drop: Vec<sled::IVec> = vec![];
    let mut drop_keys_batch = sled::Batch::default();

    let mut count: u64 = 0;
    let mut above_count: u64 = 0;
    while count < 1000000 {
        match db.iter().next() {
            None => {
                count = 1000000;
                continue
            },
            Some(next) => {
                match next {
                    Ok((key, _)) => {
                        count += 1;
                        if count <= 500000 {
                            keys_to_drop.push(key)
                        } else {
                            above_count += 1;
                        }
                    }
                    Err(_) => {
                        count = 1000000;
                        continue
                    }
                }
            }
        }
    }

    if above_count >= 500000 {
        for k in keys_to_drop.clone() {
            db.remove(k);
        }

        log::warn!("dropping {} keys", keys_to_drop.len());



    }

    error!("done pruning tree");
}

impl Worker {
    fn clean_dbs(&self) -> Result<(), ()> {
        warn!("cleaning dbs");
        let result = match self.db_refs_all() {
            Ok(inner) => {
                match inner {
                    Some((db, produced_ring, consumed_ring)) => {
                        db.flush().or_retry().expect("panic");
                        prune_tree(produced_ring);
                        produced_ring.flush().or_retry().expect("panic");
                        prune_tree(consumed_ring);
                        consumed_ring.flush().or_retry().expect("panic");
                        Ok(())
                    }
                    _ => Err(())
                }

            },
            Err(e) => Err(e)
        };

        warn!("done cleaning dbs");
        result

    }

    fn db_refs_all(&self) -> Result<Option<(&sled::Db, &sled::Db, &sled::Db)>, ()> {
        match (self.db_ref_main(), self.db_ref_produced_ring(), self.db_ref_consumed_ring()) {
            (Some(db), Some(produced_ring), Some(consumed_ring)) => {
                Ok(Some((db, produced_ring, consumed_ring)))
            },
            _ => Err(())
        }
    }

    fn db_ref_main(&self) -> Option<&sled::Db> {
        match self.db.as_ref() {
            None => None,
            Some(db) => Some(db)
        }
    }

    fn db_ref_produced_ring(&self) -> Option<&sled::Db> {
        match self.produced_ring.as_ref() {
            None => None,
            Some(db) => Some(db)
        }
    }

    fn db_ref_consumed_ring(&self) -> Option<&sled::Db> {
        match self.produced_ring.as_ref() {
            None => None,
            Some(db) => Some(db)
        }
    }

    fn insert_produced_utxos(&self, db: &sled::Db, produced_ring: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        log::warn!("annotating tx");

        let mut insert_batch = sled::Batch::default();
        let mut rollback_insert_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let key = format!("{}#{}", tx.hash(), idx);

                let era = tx.era().into();
                let body = output.encode();
                let value: IVec = SledTxValue(era, body).try_into()?;

                rollback_insert_batch.insert(key.as_bytes(), IVec::default());
                insert_batch.insert(key.as_bytes(), value)
            }
        }

        let batch_results = match (db.apply_batch(insert_batch).or_retry(),
                                   produced_ring.apply_batch(rollback_insert_batch).or_retry()) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(crate::Error::storage("failed to apply batches".to_string())),
        };

        self.inserts_counter.inc(txs.len() as u64);

        batch_results
    }

    fn remove_produced_utxos(&self, db: &sled::Db, produced_ring: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut insert = sled::Batch::default();
        let mut rollback_remove = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, _) in tx.produces() {
                insert.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
                rollback_remove.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
            }
        }

        match (produced_ring.apply_batch(rollback_remove), db.apply_batch(insert)) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(crate::Error::storage("failed to apply batches".to_string()))
        }
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

    fn get_removed_from_ring(&self, consumed_ring: &sled::Db, key: &[u8]) -> Result<Option<IVec>, crate::Error> {
        consumed_ring
            .get(key)
            .map_err(crate::Error::storage)
    }

    fn remove_consumed_utxos(&self, db: &sled::Db, consumed_ring: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut remove_batch = sled::Batch::default();
        let mut current_values_batch = sled::Batch::default();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter() {
            if let Some(current_value) = db
                .get(key.to_string())
                .map_err(crate::Error::storage).unwrap() {
                current_values_batch.insert(key.to_string().as_bytes(), current_value);
            }

            remove_batch.remove(key.to_string().as_bytes());
        }

        let result = match (db.apply_batch(remove_batch),
               consumed_ring.apply_batch(current_values_batch)) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(err3)) => Err(err3),
            (Err(err2), Ok(())) => Err(err2),
            (Err(err1), Err(_)) => Err(err1)
        };

        self.remove_counter.inc(keys.len() as u64);

        result.map_err(crate::Error::storage)
    }

    fn replace_consumed_utxos(&self, db: &sled::Db, consumed_ring: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();
        let mut remove_batch = sled::Batch::default();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter().rev() {
            if let Ok(Some(existing_value)) = self.get_removed_from_ring(consumed_ring, key.to_string().as_bytes()) {
                insert_batch.insert(key.to_string().as_bytes(), existing_value);
                remove_batch.remove(key.to_string().as_bytes());
            }

        }

        let result = match (db.apply_batch(insert_batch), consumed_ring.apply_batch(remove_batch)) {
            (Ok(_), Ok(_)) => Ok(()),
            (Ok(_), Err(err2)) => Err(err2),
            (Err(err3), Ok(_)) => Err(err3),
            (Err(_), Err(err1)) => Err(err1)
        };

        self.inserts_counter.inc(txs.len() as u64);

        result.map_err(crate::Error::storage)
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
        let mut ctx = BlockContext::default();
        let all_dbs = self.db_refs_all();
        if let Err(_) = all_dbs {
            log::warn!("skipping inserting utxos, no db yet");
            return Err(gasket::error::Error::RetryableError("db not connected".into()))
        }

        let all_dbs = all_dbs.unwrap();

        if let Some((db, produced_ring, consumed_ring)) = all_dbs {
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

                    let txs = &block.txs();

                    self.insert_produced_utxos(db, produced_ring, txs).or_panic()?;
                    let ctx = self.par_fetch_referenced_utxos(db, &txs).or_panic()?;

                    // and finally we remove utxos consumed by the block
                    self.remove_consumed_utxos(db, consumed_ring, &txs).or_restart()?;

                    self.clean_dbs().expect("todo panic");

                    self.output
                        .send(model::EnrichedBlockPayload::roll_forward(cbor, ctx))?;

                }
                model::RawBlockPayload::RollBack(cbor) => {
                    log::warn!("rolling back enrich data");

                    if !cbor.is_empty() {
                        let block = MultiEraBlock::decode(&cbor)
                            .map_err(crate::Error::cbor)
                            .apply_policy(&self.policy);

                        match block {
                            Ok(block) => {
                                let block = match block {
                                    Some(x) => x,
                                    None => return Ok(gasket::runtime::WorkOutcome::Partial),
                                };

                                let txs = block.txs();

                                // Revert Anything to do with this block
                                self.remove_produced_utxos(db, produced_ring, &txs).expect("todo: panic error");
                                self.replace_consumed_utxos(db, consumed_ring, &txs).expect("todo: panic error");

                                ctx = self.par_fetch_referenced_utxos(db, &txs).or_restart()?;

                                self.clean_dbs().expect("todo panic");
                            }
                            Err(_) => {
                                log::warn!("THIS SHOULD NEBVER SHOW UP ANYWHERE")
                            }
                        }
                    }

                    log::warn!("possibly sending dirty event back enrich data");
                    self.output
                        .send(model::EnrichedBlockPayload::roll_back(cbor, ctx))?;

                    self.blocks_counter.inc(1);
                }
            };
        } else {
            log::warn!("skipping inserting utxos, no db yet");
            return Err(gasket::error::Error::RetryableError("db not connected".into()))
        }

        self.input.commit();
        Ok(WorkOutcome::Partial)
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        log::warn!("opening db1");
        let db = sled::open(&self.config.db_path).or_retry()?;
        let consumed_ring = sled::open(self.config.consumed_ring_path.clone().unwrap_or_default()).or_retry()?;
        let produced_ring = sled::open(self.config.produced_ring_path.clone().unwrap_or_default()).or_retry()?;

        self.db = Some(db);
        self.consumed_ring = Some(consumed_ring);
        self.produced_ring = Some(produced_ring);

        log::warn!("alldb opened");

        Ok(())
    }

    fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        Ok(())
    }
}
