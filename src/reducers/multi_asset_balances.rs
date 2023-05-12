use std::panic;
use pallas::ledger::traverse::{Asset, MultiEraOutput, MultiEraTx};
use pallas::ledger::traverse::{MultiEraBlock};
use serde::{Deserialize, Serialize};

use crate::{crosscut, model, prelude::*};

use bech32::{ToBase32, Variant, Error};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct MultiAssetSingleAgg {
    #[serde(rename = "policyId")]
    policy_id: String,
    #[serde(rename = "assetName")]
    asset_name: String,
    quantity: i64,
    fingerprint: String,
}

#[derive(Deserialize, Copy, Clone)]
pub enum Projection {
    Cbor,
    Json,
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
    pub native_asset_quantity_index: Option<bool>,
    pub native_asset_ownership_index: Option<bool>,
}

pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
    policy: RuntimePolicy,
    time: crosscut::time::NaiveProvider,
}

impl Reducer {
    fn stake_or_address_from_address(&self, address: &Address) -> String {
        match address {
            Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok() {
                Some(x) => x.to_bech32().unwrap_or(address.to_string()),
                _ => address.to_bech32().unwrap_or(address.to_string()),
            },

            Address::Byron(_) => address.to_string(),
            Address::Stake(stake) => stake.to_bech32().unwrap_or(address.to_string()),
        }

    }

    fn process_block<'b>(
        &self,
        output: &mut super::OutputPort,
        block: &'b MultiEraBlock<'b>
    ) -> Result<(), gasket::error::Error> {
        let mut fingerprint_tallies: HashMap<String, HashMap<String, i64>> = HashMap::new();

        for (_, tx) in block.txs().into_iter().enumerate() {
            let timestamp = self.time.slot_to_wallclock(block.slot().to_owned());
            for (_, meo) in tx.produces() {
                let address = self.stake_or_address_from_address(&meo.address().unwrap());
                for asset in meo.assets() {
                    if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                        let asset_name = hex::encode(asset_name);
                        if let Ok(fingerprint) = asset_fingerprint([policy_id.clone().to_string().as_str(), asset_name.as_str()]) {
                            if !fingerprint.is_empty() {
                                *fingerprint_tallies.entry(address.to_string()).or_insert(HashMap::new()).entry(fingerprint.clone()).or_insert(0_i64) += quantity as i64;
                            }

                        }

                    };

                }

            }

        }



        let conf_enable_quantity_index = self.config.native_asset_quantity_index.unwrap_or(true);
        let conf_enable_ownership_index = self.config.native_asset_ownership_index.unwrap_or(true);

        if !fingerprint_tallies.is_empty() && (conf_enable_quantity_index || conf_enable_ownership_index) {
            let prefix = self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string());

            if conf_enable_quantity_index {
                for (soa, quantity_map) in fingerprint_tallies.clone() {
                    for (fingerprint, quantity) in quantity_map {
                        let total_asset_count = model::CRDTCommand::HashCounter(
                            format!("{}.{}.tally", self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string()), soa),
                            fingerprint,
                            quantity
                        );

                        output.send(total_asset_count.into())?;
                    }

                }

            }

            if conf_enable_ownership_index {
                for (soa, quantity_map) in fingerprint_tallies.clone() {
                    for (fingerprint, _) in quantity_map {
                        let total_asset_count = model::CRDTCommand::SetAdd(
                            format!("{}.{}.owns", prefix, soa),
                            fingerprint,
                        );

                        output.send(total_asset_count.into())?;
                    }

                }

            }

        }

        Ok(())
    }

    fn process_spent_txo(
        &mut self,
        output: &mut super::OutputPort,
        timestamp: &u64,
        assets: &Vec<Asset>,
        stake_or_address: String,
    ) -> Result<(), gasket::error::Error> {
        let mut fingerprint_tallies: HashMap<String, i64> = HashMap::new();

        for asset in assets {
            if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                let asset_name = hex::encode(asset_name);
                if let Ok(fingerprint) = asset_fingerprint([policy_id.to_string().as_str(), asset_name.as_str()]) {
                    if !fingerprint.is_empty() && !stake_or_address.is_empty() {
                        *fingerprint_tallies.entry(fingerprint).or_insert(*quantity as i64) += *quantity as i64;
                    }

                }

            };

        }

        let conf_enable_quantity_index = self.config.native_asset_quantity_index.unwrap_or(true);
        let conf_enable_ownership_index = self.config.native_asset_ownership_index.unwrap_or(true);

        if !fingerprint_tallies.is_empty() && (conf_enable_quantity_index || conf_enable_ownership_index) {
            if conf_enable_quantity_index {
                for (fingerprint, &quantity) in &fingerprint_tallies {
                    let total_asset_count = model::CRDTCommand::HashCounter(
                        format!("{}.{}.tally", self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string()), stake_or_address),
                        fingerprint.into(),
                        -quantity
                    );

                    output.send(total_asset_count.into())?;
                }

            }

            if conf_enable_ownership_index {
                for fingerprint in fingerprint_tallies.keys() {
                    let total_asset_count = model::CRDTCommand::SetRemove(
                        format!("{}.{}.owns", self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string()), stake_or_address),
                        fingerprint.into()
                    );

                    output.send(total_asset_count.into())?;
                }

            }

        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        log::info!("running thing");
        self.process_block(
            output,
            block,
        ).unwrap_or_default();

        Ok(())
    }

}

impl Config {
    pub fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
            policy: policy.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::MultiAssetBalances(reducer)
    }

}
