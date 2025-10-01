pub mod admin;
pub mod disk;
pub mod fsync;
pub mod locks;
pub mod partition;
pub mod paths;
pub mod plugins;
pub mod snapshot;
pub mod transfer;

use crate::errors::Result;
use crate::store::disk::metadata::MetadataJson;
use crate::store::partition::PartitionConfig;
use parking_lot::RwLock;
use partition::PartitionHandle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug)]
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
        Ok(Store {
            root,
            cfg,
            partitions: RwLock::new(HashMap::new()),
            _store_lock,
        })
    }

    pub fn open_partition(&self, partition: Uuid) -> Result<PartitionHandle> {
        let mut map = self.partitions.write();
        if let Some(h) = map.get(&partition) {
            return Ok(h.clone());
        }
        let handle = PartitionHandle::open_with_opts(self.root.clone(), partition, self.cfg.read_only)?;
        map.insert(partition, handle.clone());
        Ok(handle)
    }

    pub fn list_partitions(&self) -> Result<Vec<Uuid>> {
        paths::list_partitions(&self.root)
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    pub fn ensure_partition(&self, metadata: &MetadataJson) -> Result<PartitionHandle> {
        match self.open_partition(metadata.partition_id) {
            Ok(handle) => Ok(handle),
            Err(err) => match err {
                crate::errors::TvError::MissingFile { .. } | crate::errors::TvError::PartitionNotFound(_) => {
                    if self.cfg.read_only {
                        return Err(crate::errors::TvError::ReadOnly);
                    }
                    let cfg = partition_config_from_metadata(metadata);
                    let handle = PartitionHandle::create(self.root.clone(), metadata.partition_id, cfg)?;
                    self.partitions.write().insert(metadata.partition_id, handle.clone());
                    Ok(handle)
                }
                other => Err(other),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub read_only: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self { read_only: false }
    }
}

fn partition_config_from_metadata(meta: &MetadataJson) -> PartitionConfig {
    PartitionConfig {
        format_version: meta.format_version,
        format_plugin: meta.format_plugin.clone(),
        chunk_roll: meta.chunk_roll.clone(),
        index: meta.index.clone(),
        retention: meta.retention.clone(),
        key_is_timestamp: meta.key_is_timestamp,
        logical_purge: meta.logical_purge,
    }
}
