use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::crosscut::epochs::block_epoch;
use crate::model::Value;
use crate::{crosscut, model};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
}

impl Reducer {

    pub fn current_epoch(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let epoch_no = block_epoch(&self.chain, block);

        let crdt = model::CRDTCommand::HashSetValue(key.into(), "epoch_no".into(), epoch_no.to_string().into());

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn current_height(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let crdt = model::CRDTCommand::HashSetValue(key.into(), "height".into(), block.number().to_string().into());

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn current_slot(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let crdt = model::CRDTCommand::HashSetValue(key.into(), "slot_no".into(), block.slot().to_string().into());

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn current_block_hash(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let crdt = model::CRDTCommand::HashSetValue(key.into(), "block_hash".into(), Value::String(block.hash().to_string()));

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn current_block_era(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let crdt = model::CRDTCommand::HashSetValue(key.into(), "block_era".into(), Value::String(block.era().to_string()));

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn current_block_last_tx_hash(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        if !block.is_empty() {
            let crdt = model::CRDTCommand::HashSetValue(key.into(), "first_transaction_hash".into(), Value::String(block.txs().first().unwrap().hash().to_string()));

            output.send(gasket::messaging::Message::from(crdt))?;

            let crdt = model::CRDTCommand::HashSetValue(key.into(), "last_transaction_hash".into(), Value::String(block.txs().last().unwrap().hash().to_string()));

            output.send(gasket::messaging::Message::from(crdt))?;
        }

        Result::Ok(())
    }

    pub fn current_block_last_tx_count(
        &mut self,
        block: &MultiEraBlock,
        key: &str,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let crdt = model::CRDTCommand::HashSetValue(key.into(), "transactions_count".into(), Value::String(block.tx_count().to_string()));

        output.send(gasket::messaging::Message::from(crdt))?;

        Result::Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {

        let def_key_prefix = "last_block";

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}", prefix),
            None => format!("{}", def_key_prefix.to_string()),
        };

        self.current_epoch(block, &key, output)?;
        self.current_height(block, &key, output)?;
        self.current_slot(block, &key, output)?;
        self.current_block_hash(block, &key, output)?;
        self.current_block_era(block, &key, output)?;
        self.current_block_last_tx_hash(block, &key, output)?;
        self.current_block_last_tx_count(block, &key, output)?;

        Ok(())
    }
}

impl Config {
    pub fn plugin(self,
         chain: &crosscut::ChainWellKnownInfo
         ) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
        };

        super::Reducer::LastBlockParameters(reducer)
    }
}
