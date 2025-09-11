use serde::Serialize;

#[derive(Debug, Default, Clone, Serialize)]
pub struct PartitionStats {
    pub appends: u64,
    pub bytes: u64,
    pub current_chunk_size: u64,
}
