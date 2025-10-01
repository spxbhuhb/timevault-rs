use thiserror::Error;
use std::path::PathBuf;

pub type Result<T, E = TvError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum TvError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde_json: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("uuid: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("store already open for writing")]
    AlreadyOpen,
    #[error("out of order append: ts_ms={ts_ms} < last={last}")]
    OutOfOrder { ts_ms: i64, last: i64 },
    #[error("partition not found: {0}")]
    PartitionNotFound(uuid::Uuid),
    #[error("invalid range: {from} > {to}")]
    InvalidRange { from: i64, to: i64 },
    #[error("missing required file: {path}")]
    MissingFile { path: PathBuf },
    #[error("partition is read-only")]
    ReadOnly,
    #[error("invalid snapshot: {reason}")]
    InvalidSnapshot { reason: String },
    #[error("error: {reason}")]
    Other { reason: String },
}
