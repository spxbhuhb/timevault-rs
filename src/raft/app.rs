use std::sync::Arc;

use crate::raft::TvRaft;
use openraft::Config;
use uuid::Uuid;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.

pub struct App {
    pub id: Uuid,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: TvRaft,
    pub config: Arc<Config>,
}