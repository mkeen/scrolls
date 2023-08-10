use gasket::error::AsWorkError;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use sled::{Db, IVec, Tree};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct RollbackData {
    db: Db,
    block_tree: Tree,
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
        let block_tree = db.open_tree("blocks").unwrap();

        RollbackData {
            db,
            block_tree,
        }

    }

    pub fn get_rollback_range(&self, from: Point) -> (Option<Vec<u8>>, Vec<Vec<u8>>) {
        let mut last_valid_block: Option<Vec<u8>> = None;
        let mut current_block: Vec<u8> = vec![];
        let mut blocks_to_roll_back: Vec<Vec<u8>> = vec![];

        match from {
            Point::Origin => {
                // Todo map point to well known
                (None, vec![])
            }
            Point::Specific(slot, _) => {
                last_valid_block = match self.block_tree.get_lt(slot.to_string().as_bytes()).unwrap() {
                    None => None,
                    Some((_, value)) => Some(value.to_vec())
                };

                current_block = match self.block_tree.get(slot.to_string().as_bytes()).unwrap() {
                    None => vec![],
                    Some(value) => value.to_vec()
                };

                blocks_to_roll_back.push(current_block.to_vec());

                let mut last_sibling_found = slot.clone().to_string();

                while let Some((current_slot, current_block)) = self.block_tree.get_gt(last_sibling_found.to_string().as_bytes()).unwrap() {
                    last_sibling_found = std::str::from_utf8(&current_slot.to_vec()).unwrap().to_string();
                    blocks_to_roll_back.push(current_block.to_vec())
                }

                (last_valid_block, blocks_to_roll_back)
            }
        }
    }

    pub fn insert_block(&self, block: Vec<u8>, block_readable: MultiEraBlock) {
        let key = block_readable.slot().to_string();
        self.block_tree.insert(key.as_bytes(), IVec::from(block)).unwrap();

        let current_len = self.block_tree.len();

        // Trim excess blocks
        if current_len > 1000 {
            for _ in [0..(current_len - 1000)] {
                let (trim_key, _) = self.block_tree.iter().next().unwrap().unwrap();
                self.block_tree.remove(trim_key).unwrap();
            }
        }
    }

    pub fn get_block_at_point(&self, point: Point) -> Option<Vec<u8>> {
        match point {
            Point::Origin => {
                None
            }
            Point::Specific(slot, _) => {
                Some(self.block_tree.get(slot.to_string().as_bytes()).unwrap().unwrap().to_vec())
            }
        }
    }
}
