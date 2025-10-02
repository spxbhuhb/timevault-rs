use crate::domain::{AppEvent, AppResponse};
use timevault::raft::log::TvrLogAdapter;
use timevault::raft::TvrConfig;

pub type AppConfig = TvrConfig<AppEvent, AppResponse>;
pub type AppLogStore = TvrLogAdapter<AppEvent, AppResponse>;
pub type Raft = openraft::Raft<AppConfig>;

pub mod network_impl;

