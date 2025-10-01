use reqwest::blocking::Client as HttpClient;
use std::path::PathBuf;
use timevault::errors::{Result as TvResult, TvError};
use timevault::store::paths;
use timevault::store::transfer::{DataTransfer, FileDownload, ManifestDownload, TransferRange};
use uuid::Uuid;

pub struct StoreTransferServer {
    pub root: PathBuf,
}

impl StoreTransferServer {
    fn read_range(path: &std::path::Path, range: TransferRange) -> TvResult<(Vec<u8>, u64)> {
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};

        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        let len = metadata.len();
        if range.start > len {
            return Err(TvError::InvalidRange { from: range.start as i64, to: len as i64 });
        }
        file.seek(SeekFrom::Start(range.start))?;
        let end = range.end.unwrap_or(len).min(len);
        if end < range.start {
            return Err(TvError::InvalidRange { from: range.start as i64, to: end as i64 });
        }
        let mut buf = vec![0u8; (end - range.start) as usize];
        file.read_exact(&mut buf)?;
        Ok((buf, len))
    }
}

impl DataTransfer for StoreTransferServer {
    fn download_manifest(&self, partition: Uuid) -> TvResult<ManifestDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let manifest_path = paths::partition_manifest(&part_dir);
        let lines = timevault::store::disk::manifest::load_manifest(&manifest_path)?;
        Ok(ManifestDownload {
            partition_id: partition,
            lines,
            version: None,
        })
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let chunks_dir = paths::chunks_dir(&part_dir);
        let path = paths::chunk_file(&chunks_dir, chunk_id);
        let (bytes, remote_len) = Self::read_range(&path, range)?;
        Ok(FileDownload {
            partition_id: partition,
            chunk_id,
            requested_range: range,
            bytes,
            remote_len,
            version: None,
        })
    }

    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let chunks_dir = paths::chunks_dir(&part_dir);
        let path = paths::index_file(&chunks_dir, chunk_id);
        let (bytes, remote_len) = Self::read_range(&path, range)?;
        Ok(FileDownload {
            partition_id: partition,
            chunk_id,
            requested_range: range,
            bytes,
            remote_len,
            version: None,
        })
    }
}

pub struct StoreTransferClient {
    base: String,
    client: HttpClient,
}

impl StoreTransferClient {
    pub fn new(base: String) -> Self {
        Self { base, client: HttpClient::new() }
    }
}

impl DataTransfer for StoreTransferClient {
    fn download_manifest(&self, partition: Uuid) -> TvResult<ManifestDownload> {
        let url = format!("{}/transfer/{}/manifest", self.base, partition);
        let resp = self
            .client
            .get(url)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json())
            .map_err(|e| TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let url = if let Some(end) = range.end {
            format!("{}/transfer/{}/chunk/{}?start={}&end={}", self.base, partition, chunk_id, range.start, end)
        } else {
            format!("{}/transfer/{}/chunk/{}?start={}", self.base, partition, chunk_id, range.start)
        };
        let resp = self
            .client
            .get(url)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json())
            .map_err(|e| TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }

    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let url = if let Some(end) = range.end {
            format!("{}/transfer/{}/index/{}?start={}&end={}", self.base, partition, chunk_id, range.start, end)
        } else {
            format!("{}/transfer/{}/index/{}?start={}", self.base, partition, chunk_id, range.start)
        };
        let resp = self
            .client
            .get(url)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json())
            .map_err(|e| TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }
}
