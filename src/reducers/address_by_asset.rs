use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock};
use serde::Deserialize;

use crate::{model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<Vec<String>>,
    pub policy_id_hex: String,
    pub convert_to_ascii: Option<bool>,
    pub soa: Option<bool>,
    pub also_store_inverted: Option<bool>
}

pub struct Reducer {
    config: Config,
    convert_to_ascii: bool,
    soa: bool,
    also_store_inverted: bool
}

impl Reducer {

    #[inline]
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

    #[inline]
    fn to_string_output(&self, asset: Asset) -> Option<String> {
        match asset.policy_hex() {
            Some(policy_id) if policy_id.eq(&self.config.policy_id_hex) => match asset {
                Asset::NativeAsset(_, name, _) => match self.convert_to_ascii {
                    true => String::from_utf8(name).ok(),
                    false => Some(hex::encode(name)),
                },
                _ => None,
            },
            _ => None,
        }
    }

    #[inline]
    pub fn process_txo(
        &self,
        txo: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let asset_names: Vec<_> = txo
            .non_ada_assets()
            .into_iter()
            .filter_map(|x| self.to_string_output(x))
            .collect();

        if asset_names.is_empty() {
            return Ok(());
        }

        let address = match self.soa {
            true => self.stake_or_address_from_address(&txo.address().unwrap()),
            _ => txo.address().map(|x| x.to_string()).unwrap(),
        };

        for asset in asset_names {
            output.send(model::CRDTCommand::any_write_wins(
                self.config.key_prefix.as_deref(),
                asset.clone(),
                address.clone(),
            ).into())?;

            if self.also_store_inverted {
                output.send(model::CRDTCommand::any_write_wins(
                    self.config.key_prefix.as_deref(),
                    address.clone(),
                    asset.clone(),
                ).into())?;
            }
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().iter() {
            if rollback {
                for input in tx.consumes() {
                    if let Ok(txo) = ctx.find_utxo(&input.output_ref()) {
                        self.process_txo(&txo, output)?;
                    }

                }

            } else {
                for (_, txo) in tx.produces() {
                    self.process_txo(&txo, output)?;
                }
            }

        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let convert_to_ascii = self.convert_to_ascii.unwrap_or(false);
        let soa = self.soa.unwrap_or(false);
        let also_store_inverted = self.also_store_inverted.unwrap_or(false);
        let reducer = Reducer {
            config: self,
            convert_to_ascii,
            soa,
            also_store_inverted,
        };

        super::Reducer::AddressByAsset(reducer)
    }
}
