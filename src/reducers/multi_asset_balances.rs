use std::panic;
use pallas::ledger::traverse::{Asset, MultiEraInput, MultiEraOutput, MultiEraTx};
use pallas::ledger::traverse::{MultiEraBlock};
use serde::{Deserialize, Serialize};

use crate::{crosscut, model, prelude::*};

use bech32::{ToBase32, Variant, Error};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use std::collections::HashMap;
use std::str::from_utf8;
use gasket::messaging;
use pallas::ledger::primitives::alonzo::Mint;
use pallas::ledger::primitives::babbage::DatumOption::Hash;
use crate::model::{CRDTCommand, Delta};

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

    fn reconcile_asset_movement(
        &self,
        mut fingerprint_tallies: &HashMap<String, HashMap<String, i64>>,
        mut policy_asset_owners: &HashMap<String, HashMap<String, Vec<(String, i64)>>>
    ) -> Vec<CRDTCommand> {
        let mut messages: Vec<CRDTCommand> = vec![];
        let prefix = self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string());

        if !fingerprint_tallies.is_empty() {
            for (soa, quantity_map) in fingerprint_tallies.clone() {
                for (fingerprint, quantity) in quantity_map {
                    messages.push(model::CRDTCommand::HashCounter(
                        format!("{}.{}", prefix, soa),
                        fingerprint.to_owned(),
                        quantity
                    ));

                }
            }
        }

        if !policy_asset_owners.is_empty() {
            for (policy_id, asset_to_owner) in policy_asset_owners {
                for (fingerprint, soas) in asset_to_owner {
                    for (soa, quantity) in soas {
                        messages.push(model::CRDTCommand::SortedSetAdd(
                            format!("{}.{}.assets", prefix, policy_id),
                            fingerprint.clone(),
                            *quantity as Delta,
                        ));

                        messages.push(model::CRDTCommand::HashCounter(
                            format!("{}.{}.{}", prefix, policy_id, fingerprint),
                            soa.clone(),
                            *quantity as Delta,
                        ));

                    }

                }

            }

        }

        messages
    }

    fn process_minted_or_burned(
        &self,
        output: &mut super::OutputPort,
        mint: &Mint,
    ) -> Result<(), gasket::error::Error> {
        let mut policy_asset_supply: HashMap<String, HashMap<String, i64>> = HashMap::new();

        let prefix = self.config.key_prefix.clone().unwrap_or("soa-wallet".to_string());

        for (policy_id, policy_assets) in mint.iter() {
            for (policy_asset, mint_quantity) in policy_assets.iter() {
                if let Ok(fingerprint) = asset_fingerprint([policy_id.clone().to_string().as_str(), policy_asset.to_string().as_str()]) {
                    *policy_asset_supply.entry(policy_id.to_string()).or_insert(HashMap::new()).entry(fingerprint.to_string()).or_insert(0) += mint_quantity
                }

            }

        }

        for (policy_id, assets) in policy_asset_supply {
            for (fingerprint, quantity) in assets {
                let policy_assets_list = model::CRDTCommand::SortedSetAdd(
                    format!("{}.{}", prefix, policy_id),
                    fingerprint,
                    quantity as Delta,
                );

                output.send(policy_assets_list.into())?;
            }

        }

        Ok(())
    }

    fn process_received(
        &self,
        output: &mut super::OutputPort,
        meo: &MultiEraOutput,
    ) -> Result<(), gasket::error::Error> {
        let mut fingerprint_tallies: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let mut policy_asset_owners: HashMap<String, HashMap<String, Vec<(String, i64)>>> = HashMap::new();

        let received_to_soa = self.stake_or_address_from_address(&meo.address().unwrap());

        for asset in meo.non_ada_assets() {
            if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                let asset_name = hex::encode(asset_name);

                if let Ok(fingerprint) = asset_fingerprint([policy_id.clone().to_string().as_str(), asset_name.as_str()]) {
                    if !fingerprint.is_empty() {
                        *fingerprint_tallies.entry(received_to_soa.clone()).or_insert(HashMap::new()).entry(fingerprint.clone()).or_insert(0_i64) += quantity as i64;
                        policy_asset_owners.entry(policy_id.clone().to_string())
                            .or_insert(HashMap::new())
                            .entry(fingerprint)
                            .or_insert(Vec::new())
                            .push((received_to_soa.clone(), quantity as i64));
                    }

                }

            };

        }

        *fingerprint_tallies.entry(received_to_soa)
            .or_insert(HashMap::new())
            .entry("lovelace".to_string())
            .or_insert(0) += meo.lovelace_amount() as i64;

        for message in self.reconcile_asset_movement(&fingerprint_tallies, &policy_asset_owners) {
            output.send(message.into())?;
        }

        Ok(())
    }

    fn process_spent(
        &self,
        output: &mut super::OutputPort,
        mei: &MultiEraInput,
        ctx: &model::BlockContext,
    ) -> Result<(), gasket::error::Error> {
        let mut fingerprint_tallies: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let mut policy_asset_owners: HashMap<String, HashMap<String, Vec<(String, i64)>>> = HashMap::new();

        if let Ok(spent_output) = ctx.find_utxo(&mei.output_ref()) {
            let spent_from_soa = self.stake_or_address_from_address(&spent_output.address().unwrap());

            for asset in spent_output.non_ada_assets() {
                if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                    let asset_name = hex::encode(asset_name);

                    if let Ok(fingerprint) = asset_fingerprint([
                        policy_id.clone().to_string().as_str(),
                        asset_name.as_str()
                    ]) {
                        if !fingerprint.is_empty() {
                            *fingerprint_tallies.entry(spent_from_soa.to_string())
                                .or_insert(HashMap::new())
                                .entry(fingerprint.clone())
                                .or_insert(0_i64) -= quantity as i64;

                            policy_asset_owners.entry(policy_id.clone().to_string())
                                .or_insert(HashMap::new())
                                .entry(fingerprint)
                                .or_insert(Vec::new())
                                .push((spent_from_soa.clone(), -(quantity as i64)));
                        }

                    }

                };

            }

            *fingerprint_tallies.entry(spent_from_soa)
                .or_insert(HashMap::new())
                .entry("lovelace".to_string())
                .or_insert(0) -= spent_output.lovelace_amount() as i64;

            for message in self.reconcile_asset_movement(&fingerprint_tallies, &policy_asset_owners) {
                output.send(message.into())?;
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
        for tx in block.txs() {
            if let Some(mint) = tx.mint().as_alonzo() {
                self.process_minted_or_burned(output, mint)?;
            }

            for consumes in tx.consumes().iter() {
                self.process_spent(output, consumes, ctx)?;
            }

            for (_, produces) in tx.produces().iter() {
                self.process_received(output, produces)?;
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
