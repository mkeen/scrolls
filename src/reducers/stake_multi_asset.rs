use std::collections::HashMap;
use pallas::ledger::traverse::{Asset, MultiEraOutput};
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use serde::{Deserialize, Serialize};

use crate::{crosscut, model, prelude::*};
use pallas::crypto::hash::Hash;

use crate::crosscut::epochs::block_epoch;
use std::str::FromStr;
use bech32::{ToBase32, Variant, Error};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::primitives::babbage::{Coin, Multiasset};
use crate::model::Value;
use crate::reducers::utxo_by_stake::any_address_to_stake_bech32;

#[derive(Serialize, Deserialize)]
struct MultiAssetSingleAgg {
    #[serde(rename = "policyId")]
    policy_id: String,
    #[serde(rename = "assetName")]
    asset_name: String,
    quantity: i64,
    fingerprint: String,
    #[serde(rename = "txHash")]
    tx_hash: String,
    #[serde(rename = "txIndex")]
    tx_index: i64,
}

impl MultiAssetSingleAgg {
    fn new(policy_id: Hash<28>, asset_name: &str, quantity: u64, tx_hash: &str, tx_index: i64) -> Result<(String, MultiAssetSingleAgg), &'static str> {
        match asset_fingerprint([
            hex::encode(policy_id).as_str(),
            hex::encode(asset_name.clone()).as_str()
        ]) {
            Ok(fingerprint) => Ok((fingerprint.to_string(), MultiAssetSingleAgg {
                policy_id: hex::encode(policy_id),
                asset_name: hex::encode(asset_name.clone()),
                quantity: quantity.try_into().unwrap(),
                fingerprint,
                tx_hash: tx_hash.to_string(),
                tx_index,
            })),
            Err(_) => Err("Failed to create asset fingerprint")

        }



    }

}

#[derive(Serialize, Deserialize)]
struct PreviousOwnerAgg {
    address: String,
    transferred_out: i64,
}

impl PreviousOwnerAgg {
    fn new(address: &str, transferred_out: u64) -> PreviousOwnerAgg {
        PreviousOwnerAgg {
            address: address.to_string(),
            transferred_out: transferred_out.try_into().unwrap(),
        }

    }

}

fn asset_fingerprint(
    data_list: [&str; 2],
) -> Result<String, Error> {
    let combined_parts = data_list.join("");
    let raw = hex::decode(combined_parts).unwrap();

    let mut hasher = Blake2bVar::new(20).unwrap();
    hasher.update(&raw);
    let mut buf = [0u8; 20];
    hasher.finalize_variable(&mut buf).unwrap();
    let base32_combined = buf.to_base32();
    bech32::encode("asset", base32_combined, Variant::Bech32)
}

#[derive(Deserialize, Copy, Clone)]
pub enum AggrType {
    Epoch,
}

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
    pub aggr_by: Option<AggrType>,

    /// Policies to match
    ///
    /// If specified only those policy ids as hex will be taken into account, if
    /// not all policy ids will be indexed.
    pub policy_ids_hex: Option<Vec<String>>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    chain: crosscut::ChainWellKnownInfo,
    policy_ids: Option<Vec<Hash<28>>>,
    time: crosscut::time::NaiveProvider,
}

impl Reducer {
    fn config_key(&self, subject: String, epoch_no: u64) -> String {
        let def_key_prefix = "asset_holders_by_asset_id";

        match &self.config.aggr_by {
            Some(aggr_type) if matches!(aggr_type, AggrType::Epoch) => {
                return match &self.config.key_prefix {
                    Some(prefix) => format!("{}.{}.{}", prefix, subject, epoch_no),
                    None => format!("{}.{}", def_key_prefix.to_string(), subject),
                };
            }
            _ => {
                return match &self.config.key_prefix {
                    Some(prefix) => format!("{}.{}", prefix, subject),
                    None => format!("{}.{}", def_key_prefix.to_string(), subject),
                };
            }
        };
    }

    fn is_policy_id_accepted(&self, policy_id: &Hash<28>) -> bool {
        return match &self.policy_ids {
            Some(pids) => pids.contains(&policy_id),
            None => true,
        };
    }

    fn stake_or_address(&self, address: &Address) -> String {
        match any_address_to_stake_bech32(address.to_owned()) {
            Some(stake_key) => stake_key,
            _ => address.to_bech32().unwrap().to_owned()
        }

    }

    fn process_produced_txo(
        &mut self,
        tx_output: &MultiEraOutput,
        timestamp: &u64,
        tx_hash: &str,
        tx_index: i64,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let address = tx_output
            .address()
            .map(|addr| addr.to_string())
            .or_panic()?;

        let address = tx_output.address().or_panic()?;

        for asset in tx_output.assets() {
            match asset {
                Asset::NativeAsset(policy_id, asset_name, quantity) => {
                    let (fingerprint, multi_asset) = MultiAssetSingleAgg::new(
                        policy_id,
                        hex::encode(asset_name).as_str(),
                        quantity,
                        tx_hash,
                        tx_index,
                    ).unwrap();


                    let mut map = serde_json::Map::new();
                    map.insert(fingerprint, serde_json::Value::String(self.stake_or_address(&address).to_string()));

                    let last_activity_crdt = model::CRDTCommand::LastWriteWins(
                        format!("{}.{}", self.config.key_prefix.as_deref().unwrap_or_default(), policy_id),
                        Value::Json(serde_json::Value::from(map)),
                        *timestamp,
                    );

                    log::error!("sending {}", "hi");
                    output.send(gasket::messaging::Message::from(last_activity_crdt))?;

                }
                _ => {}
            };
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for (tx_index, tx) in block.txs().into_iter().enumerate() {
            if filter_matches!(self, block, &tx, ctx) {
                let timestamp = self.time.slot_to_wallclock(block.slot().to_owned());
                for (_, meo) in tx.produces() {
                    self.process_produced_txo(&meo, &timestamp, hex::encode(tx.hash()).as_str(), tx_index.try_into().unwrap(), output)?;
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> super::Reducer {
        let policy_ids: Option<Vec<Hash<28>>> = match &self.policy_ids_hex {
            Some(pids) => {
                let ps = pids
                    .iter()
                    .map(|pid| Hash::<28>::from_str(pid).expect("invalid policy_id"))
                    .collect();

                Some(ps)
            }
            None => None,
        };

        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
            policy: policy.clone(),
            policy_ids: policy_ids.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::StakeMultiAsset(reducer)
    }
}
