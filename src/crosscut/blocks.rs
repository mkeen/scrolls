use gasket::error::AsWorkError;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use sled::{Db, IVec, Tree};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct RollbackData {
    db: Option<Db>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct Config {
    pub db_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_path: "/data/sled_default1".to_string()
        }
    }
}

impl RollbackData {
    pub fn open_db(config: Config) -> Self {
        let db = sled::open(config.db_path).or_retry().unwrap();

        RollbackData {
            db: Some(db),
        }

    }

    fn get_db_ref(&self) -> &Db {
        self.db.as_ref().unwrap()
    }

    pub fn close(&self) -> sled::Result<usize> {
        self.get_db_ref().flush()
    }

    pub fn get_rollback_range(&self, from: Point) -> (Option<Vec<u8>>, Vec<Vec<u8>>) {
        let mut last_valid_block: Option<Vec<u8>> = None;
        let mut current_block: Vec<u8> = vec![];
        let mut blocks_to_roll_back: Vec<Vec<u8>> = vec![];

        let db = self.get_db_ref();

        match from {
            Point::Origin => {
                // Todo map point to well known
                (None, vec![])
            }
            Point::Specific(slot, _) => {
                log::warn!("trying to get last valid block");
                last_valid_block = match db.get_lt(slot.to_string().as_bytes()).unwrap() {
                    None => None,
                    Some((_, value)) => Some(value.to_vec())
                };

                current_block = match db.get(slot.to_string().as_bytes()).unwrap() {
                    None => vec![],
                    Some(value) => value.to_vec()
                };

                blocks_to_roll_back.push(current_block.to_vec());

                let mut last_sibling_found = slot.clone().to_string();

                log::warn!("START BLOCKING");
                while let Some((current_slot, current_block)) = db.get_gt(last_sibling_found.to_string().as_bytes()).unwrap() {
                    last_sibling_found = std::str::from_utf8(&current_slot.to_vec()).unwrap().to_string();
                    blocks_to_roll_back.push(current_block.to_vec())
                }
                log::warn!("END BLOCKING {} {}", blocks_to_roll_back.len(), last_valid_block.clone().unwrap().len());

                (last_valid_block, blocks_to_roll_back)
            }
        }
    }

    pub fn insert_block(&self, point: &Point, block: &Vec<u8>) {
        let key = point.slot_or_default().to_string();
        let db = self.get_db_ref();
        db.insert(key.as_bytes(), IVec::from(block.clone())).unwrap();

        let current_len = db.len();

        // Trim excess blocks
        if current_len > 1000 {
            for _ in [0..(current_len - 1000)] {
                let (trim_key, _) = db.iter().next().unwrap().unwrap();
                db.remove(trim_key).unwrap();
            }
        }
    }

    pub fn get_block_at_point(&self, point: &Point) -> Option<Vec<u8>> {
        match self.get_db_ref().get(point.slot_or_default().to_string().as_bytes()) {
            Ok(block) => match block {
                None => None,
                Some(block) => Some(block.to_vec()),
            },
            Err(_) => None,
        }
    }
}
