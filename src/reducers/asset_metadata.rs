use std::collections::HashMap;
use std::ops::Deref;

use bech32::{self, ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use hex::{self};
use log::MetadataBuilder;
use pallas::codec::minicbor::Encoder;

use pallas::ledger::primitives::alonzo::{AuxiliaryData, Metadata, Metadatum, MetadatumLabel, ShelleyMaAuxiliaryData};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraMeta, MultiEraTx};
use pallas::codec::utils::{KeyValuePairs};
use pallas::ledger::primitives::Fragment;

use serde::Deserialize;
use serde_json;
use serde_json::json;

use crate::{crosscut, model};

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
    pub export_json: Option<bool>,
    pub historical_metadata: Option<bool>,
    pub policy_asset_index: Option<bool>,
    pub filter: Option<crosscut::filters::Predicate>,
    pub projection: Option<Projection>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    time: crosscut::time::NaiveProvider,
}

const CIP25_META: u64 = 721;

impl Reducer {
    fn find_metadata_policy_assets(&self, metadata: &Metadatum, target_policy_id: &str) -> Option<KeyValuePairs<Metadatum, Metadatum>> {
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

    fn asset_fingerprint(&self, data_list: [&str; 2]) -> Result<String, bech32::Error> {
        let combined_parts = data_list.join("");
        let raw = hex::decode(combined_parts).unwrap();

        let mut hasher = Blake2bVar::new(20).unwrap();
        hasher.update(&raw);
        let mut buf = [0u8; 20];
        hasher.finalize_variable(&mut buf).unwrap();
        let base32_combined = buf.to_base32();
        bech32::encode("asset", base32_combined, Variant::Bech32)
    }

    fn get_asset_label (&self, l: Metadatum) -> String {
        return (match l {
            Metadatum::Text(l) => Ok(l),
            Metadatum::Int(l) => Ok(l.to_string()),
            Metadatum::Bytes(l) => Ok(String::from_utf8(l.to_vec())
                .unwrap_or_default()
                .to_string()),
            _ => Err("Malformed metadata")
        }).unwrap();

    }

    fn get_wrapped_metadata_fragment(&self, asset_name: String, policy_id: String, asset_metadata: &KeyValuePairs<Metadatum, Metadatum>) -> Metadata {
        let asset_map = KeyValuePairs::from(
            vec![(Metadatum::Text(asset_name), Metadatum::Map(asset_metadata.clone())); 1]
        );

        let policy_map = KeyValuePairs::from(
            vec![(Metadatum::Text(policy_id.clone()), Metadatum::Map(asset_map))]
        );

        let meta_wrapper_721 = vec![(
            MetadatumLabel::from(CIP25_META),
            Metadatum::Map(policy_map.clone())
        )];

        Metadata::from(meta_wrapper_721)
    }

    fn send(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        fn kv_pairs_to_hashmap(kv_pairs: &KeyValuePairs<Metadatum, Metadatum>
        ) -> serde_json::Map<String, serde_json::Value> {
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

            let mut hashmap = serde_json::Map::new();
            for (key, value) in kv_pairs.deref() {
                if let Metadatum::Text(key_str) = key {
                    hashmap.insert(key_str.clone(), metadatum_to_value(value));
                }

            }

            hashmap
        }

        let prefix = self.config.key_prefix.as_deref().unwrap_or("asset-metadata");
        let should_export_json = self.config.export_json.unwrap_or(false);
        let should_keep_asset_index = self.config.policy_asset_index.unwrap_or(false);
        let should_keep_historical_metadata = self.config.historical_metadata.unwrap_or(false);

        if let Some(safe_mint) = tx.mint().as_alonzo() {
            for (policy_id, assets) in safe_mint.iter() {
                let policy_id_str = hex::encode(policy_id);
                let zero: i64 = 0;

                for (asset_name, _) in assets.iter().filter(|&(_, quantity)| quantity > &zero) {
                    if let Ok(asset_name_str) = String::from_utf8(asset_name.to_vec()) {
                        if let Some(policy_map) = tx.metadata().find(MetadatumLabel::from(CIP25_META)) {
                            if let Some(policy_assets) = self.find_metadata_policy_assets(&policy_map, &policy_id_str) {
                                // Identify the metadata items that are relevant to the current minted asset
                                let filtered_policy_assets = policy_assets.iter().find(|(l, _)| {
                                    let asset_label = self.get_asset_label(l.clone().to_owned());
                                    asset_label.as_str() == &asset_name_str
                                });

                                if let Some((_, asset_contents)) = filtered_policy_assets {
                                    if let Metadatum::Map(asset_metadata) = asset_contents {
                                        if let Ok(fingerprint_str) = self.asset_fingerprint([&policy_id_str.clone(), hex::encode(&asset_name_str).as_str()]) {
                                            let timestamp = self.time.slot_to_wallclock(block.slot().to_owned());

                                            let meta_payload = {
                                                let metadata_final = self.get_wrapped_metadata_fragment(asset_name_str, policy_id_str.clone(), asset_metadata);

                                                match should_export_json {
                                                    true => {
                                                        model::Value::String(serde_json::to_string(&metadata_final).unwrap())
                                                    },

                                                    false => {
                                                        model::Value::Cbor(metadata_final.encode_fragment().unwrap())
                                                    },

                                                }

                                            };

                                            let main_meta_command = {
                                                match should_keep_historical_metadata {
                                                    true => {
                                                        model::CRDTCommand::LastWriteWins(
                                                            format!("{}.{}", prefix, fingerprint_str),
                                                            meta_payload,
                                                            timestamp
                                                        )

                                                    }

                                                    false => {
                                                        model::CRDTCommand::AnyWriteWins(
                                                            format!("{}.{}", prefix, fingerprint_str),
                                                            meta_payload,
                                                        )

                                                    }

                                                }

                                            };

                                            output.send(gasket::messaging::Message::from(main_meta_command))?;

                                            if should_keep_asset_index {
                                                output.send(model::CRDTCommand::SetAdd(
                                                    format!("{}.{}", prefix, policy_id_str),
                                                    fingerprint_str,
                                                ).into())?;

                                            }

                                        };

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
