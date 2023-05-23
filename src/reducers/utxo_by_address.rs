use std::collections::HashMap;
use std::hash::Hash;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::{Asset, MultiEraOutput};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OutputRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<Vec<String>>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
}

// hash and index are stored in the key
#[derive(Deserialize, Serialize)]
pub struct DropKingMultiAssetUTXO {
    policy_id: String,
    name: String,
    quantity: u64,
    tx_address: String,
    fingerprint: String,
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

    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(())
        };

        let address = utxo.address().map(|x| x.to_string()).or_panic()?;

        if let Some(addresses) = &self.config.filter {
            if let Err(_) = addresses.binary_search(&address) {
                return Ok(());
            }
        }

        if let Ok(raw_address) = &utxo.address() {
            let soa = self.stake_or_address_from_address(raw_address);

            let crdt = model::CRDTCommand::set_remove(
                self.config.key_prefix.as_deref(),
                &soa,
                input.to_string(),
            );

            let crdt2 = model::CRDTCommand::unset_key(
                self.config.key_prefix.as_deref(),
                format!("{}#{}", hex::encode(input.hash()), input.index()),
            );

            output.send(crdt.into());
            output.send(crdt2.into());
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx: &MultiEraTx,
        tx_output: &MultiEraOutput,
        output_idx: usize,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let tx_hash = tx.hash();
        let address = tx_output.address().map(|addr| addr.to_string()).or_panic()?;

        if let Some(addresses) = &self.config.filter {
            if let Err(_) = addresses.binary_search(&address) {
                return Ok(());
            }
        }

        if let Ok(raw_address) = &tx_output.address() {
            let soa = self.stake_or_address_from_address(raw_address);

            // Basic utxo info
            let crdt = model::CRDTCommand::set_add(
                self.config.key_prefix.as_deref(),
                &soa,
                format!("{}#{}", tx_hash, output_idx),
            );

            output.send(crdt.into());

            // Advanced utxo info
            let mut assetsToIncludeInUtxo: Vec<String> = vec![];
            for asset in tx_output.non_ada_assets() {
                if let Asset::NativeAsset(policy_id, asset_name, quantity) = asset {
                    let asset_name = hex::encode(asset_name);

                    if let Ok(fingerprint) = asset_fingerprint([policy_id.clone().to_string().as_str(), asset_name.as_str()]) {
                        if !fingerprint.is_empty() {
                            let crdt2 = model::CRDTCommand::set_add(
                                self.config.key_prefix.as_deref(),
                                format!("{}#{}", tx_hash, output_idx).as_str(),
                                format!("{}/{}/{}/{}", address, hex::encode(policy_id), fingerprint, quantity),
                            );

                            output.send(crdt2.into());
                        }

                    }

                };


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
        for tx in block.txs().into_iter() {
            for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                self.process_consumed_txo(&ctx, &consumed, output).expect("TODO: panic message");
            }

            for (idx, produced) in tx.produces() {
                self.process_produced_txo(&tx, &produced, idx, output).expect("TODO: panic message");
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy: policy.clone(),
        };

        super::Reducer::UtxoByAddress(reducer)
    }
}
