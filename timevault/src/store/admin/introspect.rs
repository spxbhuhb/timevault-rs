use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ManifestDumpEntry {
    pub chunk_id: String,
    pub min_order_key: u64,
    pub max_order_key: Option<u64>,
}
