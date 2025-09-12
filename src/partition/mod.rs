pub mod append;
pub mod read;
pub mod roll;
pub mod retention;
pub mod recovery;
pub mod truncate;
pub mod purge;
pub mod common;

use crate::admin::stats::PartitionStats;
use crate::config::PartitionConfig;
use crate::errors::Result;
use parking_lot::{Mutex, RwLock};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone)]
pub struct PartitionHandle {
    inner: std::sync::Arc<PartitionInner>,
}

#[derive(Debug, Default, Clone)]
pub struct PartitionRuntime {
    // Partition context
    pub cur_partition_root: PathBuf,
    pub cur_partition_id: Uuid,
    // Current chunk state
    pub cur_chunk_id: Option<Uuid>,
    pub cur_chunk_min_order_key: u64,
    pub cur_chunk_max_order_key: u64,
    pub cur_chunk_size_bytes: u64,
    // Current index block builder / tail recovery state
    pub cur_index_block_min_order_key: u64,
    pub cur_index_block_max_order_key: u64,
    pub cur_index_block_record_count: u64,
    pub cur_index_block_size_bytes: u64,
    pub cur_index_block_start_off: u64,
    pub cur_index_block_len_bytes: u64,
    pub cur_last_record_bytes: Option<Vec<u8>>,
}

struct PartitionInner {
    pub root: PathBuf,
    pub id: Uuid,
    pub cfg: RwLock<PartitionConfig>,
    pub stats: Mutex<PartitionStats>,
    pub runtime: RwLock<PartitionRuntime>,
    pub read_only: bool,
}

#[derive(Debug, Clone)]
pub struct AppendAck { pub offset: u64 }

#[derive(Debug, Clone)]
pub struct PartitionConfigDelta { pub format_plugin: Option<String> }

impl PartitionHandle {
    pub fn create(root: PathBuf, id: Uuid, cfg: PartitionConfig) -> Result<Self> {
        // Prepare directories
        let part_dir = crate::store::paths::partition_dir(&root, id);
        std::fs::create_dir_all(crate::store::paths::chunks_dir(&part_dir))?;
        std::fs::create_dir_all(crate::store::paths::tmp_dir(&part_dir))?;
        std::fs::create_dir_all(crate::store::paths::gc_dir(&part_dir))?;
        // Write metadata.json
        let meta = crate::disk::metadata::MetadataJson {
            partition_id: id,
            format_version: cfg.format_version,
            format_plugin: cfg.format_plugin.clone(),
            chunk_roll: cfg.chunk_roll.clone(),
            index: cfg.index.clone(),
            retention: cfg.retention.clone(),
            key_is_timestamp: cfg.key_is_timestamp,
        };
        let meta_path = crate::store::paths::partition_metadata(&part_dir);
        crate::disk::atomic::atomic_write_json(&meta_path, &meta)?;
        // Create empty manifest.json
        let manifest_path = crate::store::paths::partition_manifest(&part_dir);
        if !manifest_path.exists() { std::fs::File::create(&manifest_path)?; }
        // Initialize runtime with partition context for newly created partition
        let mut rt = PartitionRuntime::default();
        rt.cur_partition_root = root.clone();
        rt.cur_partition_id = id;
        let inner = PartitionInner {
            root,
            id,
            cfg: RwLock::new(cfg),
            stats: Mutex::new(Default::default()),
            runtime: RwLock::new(rt),
            read_only: false,
        };
        Ok(Self { inner: std::sync::Arc::new(inner) })
    }

    pub fn open(root: PathBuf, id: Uuid) -> Result<Self> { Self::open_with_opts(root, id, false) }

    pub fn open_with_opts(root: PathBuf, id: Uuid, read_only: bool) -> Result<Self> {
        // Resolve plugin and config from metadata when present
        let part_dir = crate::store::paths::partition_dir(&root, id);
        let meta_path = crate::store::paths::partition_metadata(&part_dir);
        let (m, cfg): (crate::disk::metadata::MetadataJson, PartitionConfig) = if meta_path.exists() {
            let m = crate::disk::metadata::load_metadata(&meta_path)?;
            // Optionally validate id match; ignore mismatch to keep minimal
            let cfg = PartitionConfig { format_version: m.format_version, format_plugin: m.format_plugin.clone(), chunk_roll: m.chunk_roll.clone(), index: m.index.clone(), retention: m.retention.clone(), key_is_timestamp: m.key_is_timestamp };
            (m, cfg)
        } else {
            return Err(crate::errors::TvError::MissingFile { path: meta_path });
        };
        // Load full runtime using the resolved plugin and provided metadata
        let cache = recovery::load_partition_runtime_data(&root, id, &m)?;
        // Seed runtime with partition context
        let mut cache = cache;
        cache.cur_partition_root = root.clone();
        cache.cur_partition_id = id;
        let inner = PartitionInner {
            root,
            id,
            cfg: RwLock::new(cfg),
            stats: Mutex::new(Default::default()),
            runtime: RwLock::new(cache),
            read_only,
        };
        Ok(Self { inner: std::sync::Arc::new(inner) })
    }

    pub fn append(&self, order_key: u64, payload: &[u8]) -> Result<AppendAck> { append::append(self, order_key, payload) }
    pub fn read_range(&self, from_key: u64, to_key: u64) -> Result<Vec<u8>> { read::read_range(self, from_key, to_key) }
    pub fn force_roll(&self) -> Result<()> { roll::force_roll(self) }
    pub fn stats(&self) -> PartitionStats { self.inner.stats.lock().clone() }
    pub fn set_config(&self, delta: PartitionConfigDelta) -> Result<()> { append::set_config(self, delta) }
    pub fn truncate(&self, order_key: u64) -> Result<()> { truncate::truncate(self, order_key) }
    pub fn purge(&self, order_key: u64) -> Result<()> { purge::purge(self, order_key) }
}

impl PartitionHandle {
    pub fn id(&self) -> Uuid { self.inner.id }
    pub fn root(&self) -> &PathBuf { &self.inner.root }
    pub fn cfg(&self) -> PartitionConfig { self.inner.cfg.read().clone() }
    #[cfg(test)]
    pub fn cfg_mut_for_tests(&self) -> parking_lot::RwLockWriteGuard<'_, PartitionConfig> { self.inner.cfg.write() }
}
