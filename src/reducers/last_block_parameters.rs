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

        let crdt = model::CRDTCommand::HashSetMulti(
            key,
            vec!["epoch_no".into(), "height".into(), "slot_no".into(), "block_hash".into(), "block_era".into(), "transactions_count".into(), "first_transaction_hash".into(), "last_transaction_hash".into()],
            vec![
                Value::BigInt(block_epoch(&self.chain, block) as i128),
                Value::BigInt(block.number().into()),
                Value::BigInt(block.slot().into()),
                block.hash().to_string().into(),
                block.era().to_string().into(),
                Value::BigInt(block.tx_count() as i128),
                block.txs().first().unwrap().hash().to_string().into(),
                block.txs().last().unwrap().hash().to_string().into(),
            ]
        );

        output.send(gasket::messaging::Message::from(crdt))
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
