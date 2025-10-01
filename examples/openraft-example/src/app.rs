use crate::Raft;
use crate::TvrNodeId;
use crate::state::{DeviceStatus, SharedDeviceState};
use parking_lot::Mutex;
use std::path::PathBuf;
use tokio::sync::oneshot;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: TvrNodeId,
    pub addr: String,
    pub raft: Raft,
    pub devices: SharedDeviceState,
    pub root: PathBuf,
    pub shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl App {
    pub fn snapshot_devices(&self) -> Vec<DeviceStatus> {
        self.devices.read().values().cloned().collect()
    }

    pub fn take_shutdown_signal(&self) -> Option<oneshot::Sender<()>> {
        self.shutdown.lock().take()
    }
}
