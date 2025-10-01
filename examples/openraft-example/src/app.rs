use crate::PartitionHandle;
use crate::Raft;
use crate::TvrNodeId;
use crate::state::{DeviceStatus, SharedDeviceState};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use timevault::errors::{Result as TvResult, TvError};
use timevault::store::paths;
use timevault::store::transfer::{DataTransfer, FileDownload, ManifestDownload, TransferRange};
use tokio::sync::oneshot;
use uuid::Uuid;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: TvrNodeId,
    pub addr: String,
    pub raft: Raft,
    pub devices: SharedDeviceState,
    pub event_partition: PartitionHandle,
    pub shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl App {
    pub fn snapshot_devices(&self) -> Vec<DeviceStatus> {
        self.devices.read().values().cloned().collect()
    }

    pub fn take_shutdown_signal(&self) -> Option<oneshot::Sender<()>> {
        self.shutdown.lock().take()
    }

    fn ensure_partition(&self, partition: Uuid) -> TvResult<()> {
        if partition == self.event_partition.id() {
            Ok(())
        } else {
            Err(TvError::PartitionNotFound(partition))
        }
    }

    fn manifest_path(&self) -> std::path::PathBuf {
        let part_dir = paths::partition_dir(self.event_partition.root(), self.event_partition.id());
        paths::partition_manifest(&part_dir)
    }

    fn chunk_path(&self, chunk_id: u64) -> std::path::PathBuf {
        let part_dir = paths::partition_dir(self.event_partition.root(), self.event_partition.id());
        let chunks_dir = paths::chunks_dir(&part_dir);
        paths::chunk_file(&chunks_dir, chunk_id)
    }

    fn index_path(&self, chunk_id: u64) -> std::path::PathBuf {
        let part_dir = paths::partition_dir(self.event_partition.root(), self.event_partition.id());
        let chunks_dir = paths::chunks_dir(&part_dir);
        paths::index_file(&chunks_dir, chunk_id)
    }

    fn read_range(path: &std::path::Path, range: TransferRange) -> TvResult<(Vec<u8>, u64)> {
        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        let len = metadata.len();
        if range.start > len {
            return Err(TvError::InvalidRange { from: range.start as i64, to: len as i64 });
        }
        file.seek(SeekFrom::Start(range.start))?;
        let end = range.end.unwrap_or(len);
        let end = end.min(len);
        if end < range.start {
            return Err(TvError::InvalidRange { from: range.start as i64, to: end as i64 });
        }
        let mut buf = vec![0u8; (end - range.start) as usize];
        file.read_exact(&mut buf)?;
        Ok((buf, len))
    }
}

impl DataTransfer for App {
    fn download_manifest(&self, partition: Uuid) -> TvResult<ManifestDownload> {
        self.ensure_partition(partition)?;
        let manifest_path = self.manifest_path();
        let lines = timevault::store::disk::manifest::load_manifest(&manifest_path)?;
        Ok(ManifestDownload {
            partition_id: partition,
            lines,
            version: None,
        })
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        self.ensure_partition(partition)?;
        let path = self.chunk_path(chunk_id);
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
        self.ensure_partition(partition)?;
        let path = self.index_path(chunk_id);
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
