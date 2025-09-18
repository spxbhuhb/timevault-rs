pub mod open;
pub mod paths;
pub mod locks;
pub mod fsync;

use crate::config::StoreConfig;
use crate::errors::{Result};
use crate::partition::PartitionHandle;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

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
