use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub read_only: bool,
}

impl Default for StoreConfig {
    fn default() -> Self { Self { read_only: false } }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRollCfg { pub max_bytes: u64, pub max_hours: u64 }

impl Default for ChunkRollCfg { fn default() -> Self { Self { max_bytes: 256_000_000, max_hours: 24 } } }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexCfg { pub max_records: u32, pub max_hours: u64 }

impl Default for IndexCfg { fn default() -> Self { Self { max_records: 128, max_hours: 24 } } }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCfg { pub max_days: u64 }

impl Default for RetentionCfg { fn default() -> Self { Self { max_days: 365 } } }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub format_version: u32,
    pub format_plugin: String,
    pub chunk_roll: ChunkRollCfg,
    pub index: IndexCfg,
    pub retention: RetentionCfg,
    pub key_is_timestamp: bool,
    pub logical_purge: bool,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self { format_version: 1, format_plugin: "jsonl".into(), chunk_roll: Default::default(), index: Default::default(), retention: Default::default(), key_is_timestamp: true, logical_purge: false }
    }
}
