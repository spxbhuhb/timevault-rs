use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ManifestDumpEntry {
    pub chunk_id: String,
    pub min_ts_ms: i64,
    pub max_ts_ms: Option<i64>,
}
