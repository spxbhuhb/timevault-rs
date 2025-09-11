use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TsMs(pub i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeMs {
    pub from_ms: i64,
    pub to_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BytesLen(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PartitionId(pub Uuid);
