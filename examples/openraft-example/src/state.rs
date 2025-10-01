use std::collections::HashMap;
use std::sync::Arc;

use crate::network::transfer::StoreTransferClient;
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use timevault::PartitionHandle;
use timevault::raft::state::{SnapshotData, StateMachineData, StoredSnapshot};
use timevault::raft::{TvrNode, TvrNodeId};
use timevault::store::disk::atomic::atomic_write_json;
use timevault::store::disk::metadata::MetadataJson;
use timevault::store::partition::PartitionConfig;
use timevault::store::snapshot::{StoreSnapshot, build_store_snapshot_all, ensure_partitions_from_snapshot, install_store_snapshot};
use timevault::store::{Store, StoreConfig};
use uuid::Uuid;

use crate::ExampleConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExampleEvent {
    DeviceOffline { event_id: Uuid, device_id: Uuid, timestamp: i64, partition_id: Uuid },
    DeviceOnline { event_id: Uuid, device_id: Uuid, timestamp: i64, partition_id: Uuid },
}

impl ExampleEvent {
    pub fn device_id(&self) -> Uuid {
        match self {
            ExampleEvent::DeviceOffline { device_id, .. } => *device_id,
            ExampleEvent::DeviceOnline { device_id, .. } => *device_id,
        }
    }

    pub fn event_id(&self) -> Uuid {
        match self {
            ExampleEvent::DeviceOffline { event_id, .. } => *event_id,
            ExampleEvent::DeviceOnline { event_id, .. } => *event_id,
        }
    }

    pub fn timestamp(&self) -> i64 {
        match self {
            ExampleEvent::DeviceOffline { timestamp, .. } => *timestamp,
            ExampleEvent::DeviceOnline { timestamp, .. } => *timestamp,
        }
    }

    pub fn is_online(&self) -> bool {
        matches!(self, ExampleEvent::DeviceOnline { .. })
    }
    pub fn partition_id(&self) -> Uuid {
        match self {
            ExampleEvent::DeviceOffline { partition_id, .. } => *partition_id,
            ExampleEvent::DeviceOnline { partition_id, .. } => *partition_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleResponse {
    pub code: u16,
    pub message: Option<String>,
}

impl Default for ExampleResponse {
    fn default() -> Self {
        Self { code: 200, message: None }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeviceStatus {
    pub device_id: Uuid,
    pub is_online: bool,
    pub last_event_id: Uuid,
    pub last_timestamp: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SnapshotState {
    devices: HashMap<Uuid, DeviceStatus>,
    #[serde(default)]
    store: Option<StoreSnapshot>,
}

pub type SharedDeviceState = Arc<RwLock<HashMap<Uuid, DeviceStatus>>>;

pub fn new_shared_device_state() -> SharedDeviceState {
    Arc::new(RwLock::new(HashMap::new()))
}

fn storage_error<E>(subject: ErrorSubject<TvrNodeId>, verb: ErrorVerb, err: E) -> StorageError<TvrNodeId>
where
    E: ToString,
{
    StorageError::from(StorageIOError::new(subject, verb, AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))))
}

#[derive(Debug)]
pub struct ExampleStateMachine {
    store: Store,
    data: StateMachineData,
    snapshot_idx: u64,
    devices: SharedDeviceState,
}

// Do not implement Clone for ExampleStateMachine to avoid reopening the store

impl ExampleStateMachine {
    pub fn new(store: Store, devices: SharedDeviceState) -> Result<Self, StorageError<TvrNodeId>> {
        let mut sm = Self {
            store,
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
            },
            snapshot_idx: 0,
            devices,
        };

        if let Some(snapshot) = sm.get_current_snapshot_()? {
            sm.update_state_machine_(snapshot)?;
        }

        Ok(sm)
    }

    fn update_state_machine_(&mut self, snapshot: StoredSnapshot) -> Result<(), StorageError<TvrNodeId>> {
        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();

        let snapshot_state: SnapshotState = if snapshot.data.is_empty() {
            SnapshotState::default()
        } else {
            serde_json::from_slice(&snapshot.data).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))?
        };
        let mut guard = self.devices.write();
        guard.clear();
        guard.extend(snapshot_state.devices.into_iter());

        Ok(())
    }

    fn get_current_snapshot_(&self) -> Result<Option<StoredSnapshot>, StorageError<TvrNodeId>> {
        let path = self.state_file_path();
        match std::fs::File::open(&path) {
            Ok(file) => {
                let snapshot: Option<StoredSnapshot> = serde_json::from_reader(file).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))?;
                Ok(snapshot)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(storage_error(ErrorSubject::Store, ErrorVerb::Read, err)),
        }
    }

    fn set_current_snapshot_(&self, snapshot: StoredSnapshot) -> Result<(), StorageError<TvrNodeId>> {
        atomic_write_json(self.state_file_path(), &snapshot).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))
    }

    fn state_file_path(&self) -> std::path::PathBuf {
        let root = self.store.root_path().to_path_buf();
        root.join("raft_state.json")
    }

    fn apply_event(&self, event: &ExampleEvent) -> Result<(), StorageError<TvrNodeId>> {
        self.append_event(event)?;
        let mut guard = self.devices.write();
        let status = DeviceStatus {
            device_id: event.device_id(),
            is_online: event.is_online(),
            last_event_id: event.event_id(),
            last_timestamp: event.timestamp(),
        };
        guard.insert(status.device_id, status);
        Ok(())
    }
}

#[derive(Clone)]
pub struct ExampleSnapshotBuilder {
    store_snapshot: StoreSnapshot,
    devices: SharedDeviceState,
    last_applied_log: Option<LogId<TvrNodeId>>,
    last_membership: StoredMembership<TvrNodeId, TvrNode>,
    snapshot_idx: u64,
}

impl RaftSnapshotBuilder<ExampleConfig> for ExampleSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<ExampleConfig>, StorageError<TvrNodeId>> {
        let snapshot_id = if let Some(last) = self.last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx)
        } else {
            format!("--{}", self.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let snapshot_state = SnapshotState {
            devices: self.devices.read().clone(),
            store: Some(self.store_snapshot.clone()),
        };
        let data = serde_json::to_vec(&snapshot_state).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(std::io::Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<ExampleConfig> for ExampleStateMachine {
    type SnapshotBuilder = ExampleSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId<TvrNodeId>>, StoredMembership<TvrNodeId, TvrNode>), StorageError<TvrNodeId>> {
        Ok((self.data.last_applied_log_id, self.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ExampleResponse>, StorageError<TvrNodeId>>
    where
        I: IntoIterator<Item = Entry<ExampleConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for entry in entries {
            self.data.last_applied_log_id = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ExampleResponse::default(),
                EntryPayload::Normal(event) => {
                    self.apply_event(&event)?;
                    ExampleResponse::default()
                }
                EntryPayload::Membership(mem) => {
                    self.data.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    ExampleResponse::default()
                }
            };

            replies.push(response);
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        let store_snapshot = build_store_snapshot_all(&self.store)
            .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))
            .expect("build store snapshot");
        ExampleSnapshotBuilder {
            store_snapshot,
            devices: self.devices.clone(),
            last_applied_log: self.data.last_applied_log_id,
            last_membership: self.data.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<TvrNodeId>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<TvrNodeId, TvrNode>, snapshot: Box<SnapshotData>) -> Result<(), StorageError<TvrNodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // If snapshot includes a store snapshot, ensure local partitions exist and install data from leader.
        if let Ok(state) = serde_json::from_slice::<SnapshotState>(&new_snapshot.data) {
            if let Some(store) = state.store {
                let store_handle = Store::open(self.store.root_path(), StoreConfig { read_only: false }).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
                ensure_partitions_from_snapshot(&store_handle, &store).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
                // Perform full Option D: install store snapshot by downloading from leader.
                if let Some(last) = meta.last_log_id {
                    let leader_id = last.leader_id.node_id;
                    // Try to resolve leader address from last_membership
                    let leader_addr = meta.last_membership.membership().get_node(&leader_id).map(|n| n.addr.clone());
                    if let Some(addr) = leader_addr {
                        let base = format!("http://{}", addr);
                        let transfer = StoreTransferClient::new(base);
                        // Best-effort install; map errors to StorageError
                        install_store_snapshot(&store_handle, &transfer, &store).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
                    } else {
                        tracing::warn!(leader_id, "could not resolve leader address for snapshot install");
                    }
                }
                // Keep using the same store; partitions will be selected per event.
                self.store = store_handle;
            }
        }

        self.update_state_machine_(new_snapshot.clone())?;
        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<ExampleConfig>>, StorageError<TvrNodeId>> {
        let stored = self.get_current_snapshot_()?;
        Ok(stored.map(|snap| Snapshot {
            meta: snap.meta.clone(),
            snapshot: Box::new(std::io::Cursor::new(snap.data.clone())),
        }))
    }
}

impl ExampleStateMachine {
    pub fn devices(&self) -> SharedDeviceState {
        self.devices.clone()
    }

    fn ensure_event_partition(&self, id: Uuid) -> Result<PartitionHandle, StorageError<TvrNodeId>> {
        match self.store.open_partition(id) {
            Ok(h) => Ok(h),
            Err(e) => match e {
                timevault::errors::TvError::MissingFile { .. } | timevault::errors::TvError::PartitionNotFound(_) => {
                    let mut cfg = PartitionConfig::default();
                    cfg.format_plugin = "jsonl".to_string();
                    let meta = MetadataJson {
                        partition_id: id,
                        format_version: cfg.format_version,
                        format_plugin: cfg.format_plugin.clone(),
                        chunk_roll: cfg.chunk_roll.clone(),
                        index: cfg.index.clone(),
                        retention: cfg.retention.clone(),
                        key_is_timestamp: cfg.key_is_timestamp,
                        logical_purge: cfg.logical_purge,
                        last_purge_id: None,
                    };
                    let h = self.store.ensure_partition(&meta).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
                    Ok(h)
                }
                other => Err(storage_error(ErrorSubject::Store, ErrorVerb::Read, other)),
            },
        }
    }

    fn append_event(&self, event: &ExampleEvent) -> Result<(), StorageError<TvrNodeId>> {
        let pid = event.partition_id();
        let handle = self.ensure_event_partition(pid)?;
        let timestamp = event.timestamp();
        let order_key = timestamp.max(0) as u64;
        let mut record = serde_json::to_vec(event).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
        record.push(b'\n');
        handle.append(order_key, &record).map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
        Ok(())
    }
}
