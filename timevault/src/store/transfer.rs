use serde::{Deserialize, Serialize};
use crate::errors::Result;
use crate::store::disk::manifest::ManifestLine;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestDownload {
    pub partition_id: Uuid,
    pub lines: Vec<ManifestLine>,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TransferRange {
    pub start: u64,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDownload {
    pub partition_id: Uuid,
    pub chunk_id: u64,
    pub requested_range: TransferRange,
    pub bytes: Vec<u8>,
    pub remote_len: u64,
    pub version: Option<String>,
}

pub trait DataTransfer {
    fn download_manifest(&self, partition: Uuid) -> Result<ManifestDownload>;
    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> Result<FileDownload>;
    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> Result<FileDownload>;
}
