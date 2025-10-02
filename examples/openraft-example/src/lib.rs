#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

pub mod app;
pub mod client;
pub mod domain;
pub mod http;
pub mod node;
pub mod raft;
pub mod state_machine;
pub mod storage;

pub use domain::{AppEvent, AppResponse, DeviceStatus};
pub use node::start_app_node;

pub use timevault::raft::TvrNodeId;

pub mod typ {
    use crate::raft::AppConfig;
    use openraft::BasicNode;
    use timevault::raft::TvrNodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TvrNodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TvrNodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TvrNodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TvrNodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TvrNodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<TvrNodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<AppConfig>;
}
