use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetadataJson {
    pub partition_id: Uuid,
    pub format_version: u32,
    pub format_plugin: String,
    pub chunk_roll: super::super::config::ChunkRollCfg,
    pub index: super::super::config::IndexCfg,
    pub retention: super::super::config::RetentionCfg,
    pub key_is_timestamp: bool,
}

pub fn load_metadata(path: &std::path::Path) -> crate::errors::Result<MetadataJson> {
    let s = std::fs::read_to_string(path)?;
    let m = serde_json::from_str::<MetadataJson>(&s)?;
    Ok(m)
}
