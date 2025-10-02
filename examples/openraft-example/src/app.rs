use crate::domain::DeviceStatus;
use crate::raft::Raft;
use crate::state_machine::SharedDeviceState;
use crate::TvrNodeId;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub ready: AtomicBool,
}

impl App {
    pub fn snapshot_devices(&self) -> Vec<DeviceStatus> {
        self.devices.read().values().cloned().collect()
    }

    pub fn take_shutdown_signal(&self) -> Option<oneshot::Sender<()>> {
        self.shutdown.lock().take()
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}
