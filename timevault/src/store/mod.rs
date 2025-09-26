pub mod paths;
pub mod locks;
pub mod fsync;
pub mod disk;
pub mod partition;
pub mod plugins;
pub mod admin;

use crate::errors::Result;
use partition::PartitionHandle;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

pub struct Store {
    root: PathBuf,
    cfg: StoreConfig,
    partitions: RwLock<HashMap<Uuid, PartitionHandle>>,
    _store_lock: Option<std::fs::File>,
}

impl Store {
    pub fn open(root: &Path, cfg: StoreConfig) -> Result<Store> {
        let root = root.to_path_buf();
        std::fs::create_dir_all(paths::partitions_root(&root))?;
        let _store_lock = if cfg.read_only { None } else { Some(locks::acquire_store_lock(&root)?) };
        Ok(Store { root, cfg, partitions: RwLock::new(HashMap::new()), _store_lock })
    }

    pub fn open_partition(&self, partition: Uuid) -> Result<PartitionHandle> {
        let mut map = self.partitions.write();
        if let Some(h) = map.get(&partition) { return Ok(h.clone()); }
        let handle = PartitionHandle::open_with_opts(self.root.clone(), partition, self.cfg.read_only)?;
        map.insert(partition, handle.clone());
        Ok(handle)
    }

    pub fn list_partitions(&self) -> Result<Vec<Uuid>> { paths::list_partitions(&self.root) }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub read_only: bool,
}

impl Default for StoreConfig {
    fn default() -> Self { Self { read_only: false } }
}