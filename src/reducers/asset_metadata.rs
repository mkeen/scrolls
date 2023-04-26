use std::ops::Deref;

use bech32::{self, Error, ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use hex::{self};
use std::error::Error as Err;
use log::MetadataBuilder;

use pallas::ledger::primitives::alonzo::{Metadata, Metadatum, MetadatumLabel};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};
use pallas::codec::utils::{Int, KeyValuePairs};

use serde::Deserialize;
use serde_json;

use crate::{crosscut, model};

pub const CIP25_META: u64 = 721;

fn metadatum_to_value(m: &Metadatum) -> serde_json::Value {
    match m {
        Metadatum::Int(int_value) => {
            serde_json::Value::String(int_value.to_string())
        },
        Metadatum::Bytes(bytes) => serde_json::Value::String(hex::encode(bytes.as_slice())),
        Metadatum::Text(text) => serde_json::Value::String(text.clone()),
        Metadatum::Array(array) => {
            let json_array: Vec<serde_json::Value> = array.iter().map(metadatum_to_value).collect();
            serde_json::Value::Array(json_array)
        },
        Metadatum::Map(kv_pairs) => {
            let json_object = kv_pairs_to_hashmap(kv_pairs);
            serde_json::Value::Object(json_object)
        },

    }

}

fn kv_pairs_to_hashmap(kv_pairs: &KeyValuePairs<Metadatum, Metadatum>) -> serde_json::Map<String, serde_json::Value> {
    let mut hashmap = serde_json::Map::new();
    for (key, value) in kv_pairs.deref() {
        if let Metadatum::Text(key_str) = key {
            hashmap.insert(key_str.clone(), metadatum_to_value(value));
        }

    }

    hashmap
}

fn find_metadata_policy_assets(
    metadata: &Metadatum,
    target_policy_id: &str,
) -> Option<KeyValuePairs<Metadatum, Metadatum>> {
    if let Metadatum::Map(kv) = metadata {
        for (policy_label, policy_contents) in kv.iter() {
            if let Metadatum::Text(policy_label) = policy_label {
                if policy_label == target_policy_id {
                    if let Metadatum::Map(policy_inner_map) = policy_contents {
                        return Some(policy_inner_map.clone());
                    }

                }

            }

        }

    }

    None
}

pub fn asset_fingerprint(
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

fn get_asset_label (l: Metadatum) -> String {
    return (match l {
        Metadatum::Text(l) => Ok(l),
        Metadatum::Int(l) => Ok(l.to_string()),
        Metadatum::Bytes(l) => Ok(String::from_utf8(l.to_vec()).unwrap_or_default().to_string()),
        _ => Err("Malformed metadata")
    }).unwrap();

}

#[derive(Deserialize, Copy, Clone)]
pub enum Projection {
    Cbor,
    Json,
}

impl Default for Projection {
    fn default() -> Self {
        Self::Cbor
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
    pub projection: Option<Projection>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    time: crosscut::time::NaiveProvider,
}

impl Reducer {
    fn send(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        if let Some(safe_mint) = tx.mint().as_alonzo() {
            for (policy_id, assets) in safe_mint.iter() {
                let policy_id_str = hex::encode(policy_id);
                for (asset_name, _) in assets.iter().filter(|&(_, quantity)| quantity > &0) {
                    if let Ok(asset_name_str) = String::from_utf8(asset_name.to_vec()) {
                        if let Some(policy_map) = tx.metadata().find(MetadatumLabel::from(CIP25_META)) {
                            if let Some(policy_assets) = find_metadata_policy_assets(&policy_map, &policy_id_str) {
                                let filtered_policy_assets = policy_assets.iter().find(|(l, _)| {
                                    let asset_label = get_asset_label(l.to_owned());
                                    asset_label.as_str() == &asset_name_str
                                });

                                if let Some((_, asset_contents)) = filtered_policy_assets {
                                    if let Metadatum::Map(asset_contents) = asset_contents {
                                        if let Ok(fingerprint_str) = asset_fingerprint([&policy_id_str, hex::encode(&asset_name_str).as_str()]) {
                                            let mut metadata = serde_json::Map::new();
                                            let mut std_wrap_map = serde_json::Map::new();
                                            let mut policy_wrap_map = serde_json::Map::new();
                                            let mut asset_wrap_map = serde_json::Map::new();

                                            let timestamp = self.time.slot_to_wallclock(block.slot().to_owned());

                                            let asset_map = kv_pairs_to_hashmap(&asset_contents);
                                            asset_wrap_map.insert(asset_name_str.to_string(), serde_json::Value::Object(asset_map));
                                            policy_wrap_map.insert(policy_id_str.to_string(), serde_json::Value::Object(asset_wrap_map));
                                            std_wrap_map.insert(CIP25_META.to_string(), serde_json::Value::Object(policy_wrap_map));
                                            metadata.insert("metadata".to_string(), serde_json::Value::Object(std_wrap_map));
                                            metadata.insert("last_minted".to_string(), serde_json::Value::Number(
                                                serde_json::Number::from(timestamp)
                                            ));

                                            if let Ok(json_string) = serde_json::to_string_pretty(&metadata) {
                                                let metadata_crdt = model::CRDTCommand::AnyWriteWins(
                                                    format!("{}.{}", self.config.key_prefix.as_deref().unwrap_or_default(), fingerprint_str),
                                                    model::Value::String(json_string.to_owned()),
                                                );

                                                let asset_index_crdt = model::CRDTCommand::SetAdd(
                                                    format!("{}.{}", self.config.key_prefix.as_deref().unwrap_or_default(), policy_id_str),
                                                    fingerprint_str);

                                                output.send(gasket::messaging::Message::from(asset_index_crdt))?;
                                                output.send(gasket::messaging::Message::from(metadata_crdt))?;
                                            }

                                        }

                                    }

                                }

                            }

                        }

                    }

                }

            }

        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in &block.txs() {
            // Make sure the TX is worth processing for the use-case (metadata extraction). It should have minted at least one asset with the CIP25_META key present in metadata.
            // Currently this will send thru a TX that is just a burn with no mint, but it will be handled in the reducer.
            // Todo: could be cleaner using a filter
            if tx.mint().len() > 0 && tx.metadata().as_alonzo().iter().any(|meta| meta.iter().any(|(key, _)| *key == CIP25_META)) {
                self.send(block, tx, output)?;
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
        let worker = Reducer {
            config: self,
            policy: policy.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::MintAssetMetadata(worker)
    }

}
