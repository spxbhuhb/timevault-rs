use actix_web::web::{Data, Json, Path, Query};
use actix_web::{Responder, get};
use serde::Deserialize;
use serde::Serialize;
use timevault::store::paths;
use timevault::store::transfer::{DataTransfer, FileDownload, ManifestDownload, TransferRange};
use uuid::Uuid;

use crate::app::App;

#[derive(Serialize)]
pub struct ManifestResponse {
    pub partition_id: Uuid,
    pub version: Option<String>,
    pub lines: Vec<timevault::store::disk::manifest::ManifestLine>,
}

#[derive(Serialize)]
pub struct RangeResponse {
    pub start: u64,
    pub end: Option<u64>,
}

#[derive(Serialize)]
pub struct FileResponse {
    pub partition_id: Uuid,
    pub chunk_id: u64,
    pub requested_range: RangeResponse,
    pub bytes: Vec<u8>,
    pub remote_len: u64,
    pub version: Option<String>,
}

impl From<FileDownload> for FileResponse {
    fn from(download: FileDownload) -> Self {
        Self {
            partition_id: download.partition_id,
            chunk_id: download.chunk_id,
            requested_range: RangeResponse { start: download.requested_range.start, end: download.requested_range.end },
            bytes: download.bytes,
            remote_len: download.remote_len,
            version: download.version,
        }
    }
}

impl From<ManifestDownload> for ManifestResponse {
    fn from(download: ManifestDownload) -> Self {
        Self { partition_id: download.partition_id, version: download.version, lines: download.lines }
    }
}

#[derive(Deserialize)]
pub struct RangeQuery {
    pub start: u64,
    pub end: Option<u64>,
}

#[get("/transfer/{partition}/manifest")]
pub async fn transfer_manifest(app: Data<App>, path: Path<(Uuid,)>) -> actix_web::Result<impl Responder> {
    let (partition,) = path.into_inner();
    let server = StoreTransferServer { root: app.root.clone() };
    let manifest = server.download_manifest(partition).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(ManifestResponse::from(manifest)))
}

#[get("/transfer/{partition}/chunk/{chunk_id}")]
pub async fn transfer_chunk(app: Data<App>, path: Path<(Uuid, u64)>, query: Query<RangeQuery>) -> actix_web::Result<impl Responder> {
    let (partition, chunk_id) = path.into_inner();
    let range = TransferRange { start: query.start, end: query.end };
    let server = StoreTransferServer { root: app.root.clone() };
    let download = server.download_chunk(partition, chunk_id, range).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(FileResponse::from(download)))
}

#[get("/transfer/{partition}/index/{chunk_id}")]
pub async fn transfer_index(app: Data<App>, path: Path<(Uuid, u64)>, query: Query<RangeQuery>) -> actix_web::Result<impl Responder> {
    let (partition, chunk_id) = path.into_inner();
    let range = TransferRange { start: query.start, end: query.end };
    let server = StoreTransferServer { root: app.root.clone() };
    let download = server.download_index(partition, chunk_id, range).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(FileResponse::from(download)))
}

#[get("/partitions")]
pub async fn partitions(app: Data<App>) -> actix_web::Result<impl Responder> {
    let ids = paths::list_partitions(&app.root).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(Ok::<_, crate::typ::RaftError>(ids)))
}

pub struct StoreTransferServer {
    pub root: std::path::PathBuf,
}

impl StoreTransferServer {
    fn read_range(path: &std::path::Path, range: TransferRange) -> timevault::errors::Result<(Vec<u8>, u64)> {
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};
        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        let len = metadata.len();
        if range.start > len {
            return Err(timevault::errors::TvError::InvalidRange { from: range.start as i64, to: len as i64 });
        }
        file.seek(SeekFrom::Start(range.start))?;
        let end = range.end.unwrap_or(len).min(len);
        if end < range.start {
            return Err(timevault::errors::TvError::InvalidRange { from: range.start as i64, to: end as i64 });
        }
        let mut buf = vec![0u8; (end - range.start) as usize];
        file.read_exact(&mut buf)?;
        Ok((buf, len))
    }
}

impl DataTransfer for StoreTransferServer {
    fn download_manifest(&self, partition: Uuid) -> timevault::errors::Result<ManifestDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let manifest_path = paths::partition_manifest(&part_dir);
        let lines = timevault::store::disk::manifest::load_manifest(&manifest_path)?;
        Ok(ManifestDownload { partition_id: partition, lines, version: None })
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> timevault::errors::Result<FileDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let chunks_dir = paths::chunks_dir(&part_dir);
        let path = paths::chunk_file(&chunks_dir, chunk_id);
        let (bytes, remote_len) = Self::read_range(&path, range)?;
        Ok(FileDownload { partition_id: partition, chunk_id, requested_range: range, bytes, remote_len, version: None })
    }

    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> timevault::errors::Result<FileDownload> {
        let part_dir = paths::partition_dir(&self.root, partition);
        let chunks_dir = paths::chunks_dir(&part_dir);
        let path = paths::index_file(&chunks_dir, chunk_id);
        let (bytes, remote_len) = Self::read_range(&path, range)?;
        Ok(FileDownload { partition_id: partition, chunk_id, requested_range: range, bytes, remote_len, version: None })
    }
}

pub struct StoreTransferClient {
    base: String,
    client: reqwest::blocking::Client,
}

impl StoreTransferClient {
    pub fn new(base: String) -> Self {
        Self { base, client: reqwest::blocking::Client::new() }
    }
}

impl DataTransfer for StoreTransferClient {
    fn download_manifest(&self, partition: Uuid) -> timevault::errors::Result<ManifestDownload> {
        let url = format!("{}/transfer/{}/manifest", self.base, partition);
        let resp = self
            .client
            .get(url)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json())
            .map_err(|e| timevault::errors::TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> timevault::errors::Result<FileDownload> {
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
            .map_err(|e| timevault::errors::TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }

    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> timevault::errors::Result<FileDownload> {
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
            .map_err(|e| timevault::errors::TvError::Other { reason: e.to_string() })?;
        Ok(resp)
    }
}

