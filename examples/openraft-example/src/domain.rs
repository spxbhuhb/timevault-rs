use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AppEvent {
    DeviceOffline { event_id: Uuid, device_id: Uuid, timestamp: i64, partition_id: Uuid },
    DeviceOnline { event_id: Uuid, device_id: Uuid, timestamp: i64, partition_id: Uuid },
}

impl AppEvent {
    pub fn device_id(&self) -> Uuid {
        match self {
            AppEvent::DeviceOffline { device_id, .. } => *device_id,
            AppEvent::DeviceOnline { device_id, .. } => *device_id,
        }
    }

    pub fn event_id(&self) -> Uuid {
        match self {
            AppEvent::DeviceOffline { event_id, .. } => *event_id,
            AppEvent::DeviceOnline { event_id, .. } => *event_id,
        }
    }

    pub fn timestamp(&self) -> i64 {
        match self {
            AppEvent::DeviceOffline { timestamp, .. } => *timestamp,
            AppEvent::DeviceOnline { timestamp, .. } => *timestamp,
        }
    }

    pub fn is_online(&self) -> bool {
        matches!(self, AppEvent::DeviceOnline { .. })
    }
    pub fn partition_id(&self) -> Uuid {
        match self {
            AppEvent::DeviceOffline { partition_id, .. } => *partition_id,
            AppEvent::DeviceOnline { partition_id, .. } => *partition_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppResponse {
    pub code: u16,
    pub message: Option<String>,
}

impl Default for AppResponse {
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

