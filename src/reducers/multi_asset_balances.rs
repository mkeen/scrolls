use pallas::ledger::traverse::{Asset, MultiEraInput, MultiEraOutput, MultiEraTx};
use pallas::ledger::traverse::{MultiEraBlock};
use serde::{Deserialize, Serialize};

use crate::{crosscut, model, prelude::*};

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use std::collections::HashMap;
use pallas::ledger::primitives::alonzo::Mint;
use crate::model::{Delta};
use std::result::Result;
use std::sync::Mutex;

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

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
}

fn asset_fingerprint(
    data_list: [&str; 2],
) -> Result<String, bech32::Error> {
    let combined_parts = data_list.join("");
    let raw = hex::decode(combined_parts).unwrap();
    let mut hasher = Blake2bVar::new(20).unwrap();
    hasher.update(&raw);
    let mut buf = [0u8; 20];
    hasher.finalize_variable(&mut buf).unwrap();
    let base32_combined = buf.to_base32();
    bech32::encode("asset", base32_combined, Variant::Bech32)
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
                Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                _ => address.to_bech32().unwrap_or(address.to_string()),
            },

            Address::Byron(_) => address.to_bech32().unwrap_or(address.to_string()),
            Address::Stake(stake) => stake.to_bech32().unwrap_or(address.to_string()),
        }

    }

    fn calculate_address_asset_balance_offsets(
        &self,
        address: &String,
        lovelace: &i64,
        assets: &Vec<Asset>,
        spending: bool,
    ) -> (HashMap<String, HashMap<String, i64>>, HashMap<String, HashMap<String, Vec<(String, i64)>>>) {
        let mut fingerprint_tallies: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let mut policy_asset_owners: HashMap<String, HashMap<String, Vec<(String, i64)>>> = HashMap::new();

        for asset in assets.clone() {
            if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                let asset_name = hex::encode(asset_name);

                if let Ok(fingerprint) = asset_fingerprint([policy_id.clone().to_string().as_str(), asset_name.as_str()]) {
                    if !fingerprint.is_empty() {
                        let adjusted_quality: i64 = match spending {
                            true => -(quantity as i64),
                            false => quantity as i64
                        };

                        *fingerprint_tallies.entry(address.clone())
                            .or_insert(HashMap::new())
                            .entry(fingerprint.clone())
                            .or_insert(0_i64) += adjusted_quality;

                        policy_asset_owners.entry(policy_id.clone().to_string())
                            .or_insert(HashMap::new())
                            .entry(fingerprint)
                            .or_insert(Vec::new())
                            .push((address.clone(), adjusted_quality));
                    }

                }

            };

        }

        *fingerprint_tallies.entry(address.to_string())
            .or_insert(HashMap::new())
            .entry("lovelace".to_string())
            .or_insert(0) += *lovelace;

        (fingerprint_tallies, policy_asset_owners)
    }

    fn process_asset_movement(
        &self,
        output: &mut super::OutputPort,
        soa: &String,
        lovelace: u64,
        assets: &Vec<Asset>,
        spending: bool,
        slot: u64,
    ) -> Result<(), gasket::error::Error> {
        let adjusted_lovelace = match spending {
            true => -(lovelace as i64),
            false => lovelace as i64
        };

        let (fingerprint_tallies, policy_asset_owners) = self.calculate_address_asset_balance_offsets(
            soa,
            &adjusted_lovelace,
            assets,
            spending
        );

        let prefix = self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string());

        if !fingerprint_tallies.is_empty() {
            for (soa, quantity_map) in fingerprint_tallies.clone() {
                for (fingerprint, quantity) in quantity_map {
                    if !fingerprint.is_empty() {
                        output.send(
                            gasket::messaging::Message::from(
                                model::CRDTCommand::HashCounter(
                                    format!("{}.{}", prefix, soa),
                                    fingerprint.to_owned(),
                                    quantity
                                )

                            )

                        );

                    }

                }

                output.send(gasket::messaging::Message::from(model::CRDTCommand::AnyWriteWins(
                    format!("{}.latest.{}", self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string()), soa),
                    self.time.slot_to_wallclock(slot).to_string().into(),
                )));

            }

        }

        if !policy_asset_owners.is_empty() {
            for (policy_id, asset_to_owner) in policy_asset_owners {
                for (fingerprint, soas) in asset_to_owner {
                    for (soa, quantity) in soas {
                        if !soa.is_empty() {
                            if quantity != 0 {
                                output.send(gasket::messaging::Message::from(model::CRDTCommand::HashCounter(
                                    format!("{}.owned.{}", prefix, fingerprint),
                                    soa.clone(),
                                    quantity as Delta,
                                )));

                            }

                        }

                    }

                }

            }

        }

        Ok(())
    }

    fn process_received(
        &self,
        output: &mut super::OutputPort,
        meo: &MultiEraOutput,
        slot: u64,
    ) -> Result<(), gasket::error::Error> {
        let received_to_soa = self.stake_or_address_from_address(&meo.address().unwrap());

        self.process_asset_movement(
            output,
            &received_to_soa,
            meo.lovelace_amount(),
            &meo.non_ada_assets(),
            false,
            slot
        )

    }

    fn process_spent(
        &self,
        output: &mut super::OutputPort,
        mei: &MultiEraInput,
        ctx: &model::BlockContext,
        slot: u64,
    ) -> Result<(), gasket::error::Error> {
        if let Ok(spent_output) = ctx.find_utxo(&mei.output_ref()) {
            let spent_from_soa = self.stake_or_address_from_address(&spent_output.address().unwrap());

            return self.process_asset_movement(
                output,
                &spent_from_soa,
                spent_output.lovelace_amount(),
                &spent_output.non_ada_assets(),
                true,
                slot,
            );

        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let slot = block.slot();

        for tx in block.txs() {
            for consumes in tx.consumes().iter() {
                self.process_spent(output, consumes, ctx, slot);
            }

            for (_, produces) in tx.produces().iter() {
                self.process_received(output, produces, slot);
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
        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
            policy: policy.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::MultiAssetBalances(reducer)
    }

}
