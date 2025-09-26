#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::sync::Arc;

use crate::app::App;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::network::Network;
use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::HttpServer;
use openraft::Config;
use serde::{Deserialize, Serialize};
use timevault::store::partition::PartitionConfig;
use timevault::raft::log::TvrLogAdapter;
use timevault::raft::state::TvrPartitionStateMachine;
use timevault::raft::{TvrConfig, TvrNodeId};
use timevault::PartitionHandle;
use uuid::Uuid;

pub mod app;
pub mod client;
pub mod network;

pub type ValueRequest = serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueResponse(pub Result<serde_json::Value, serde_json::Value>);

impl Default for ValueResponse {
    fn default() -> Self {
        Self(Ok(serde_json::Value::Null))
    }
}

pub type ValueConfig = TvrConfig<ValueRequest, ValueResponse>;
pub type ValueLogStore = TvrLogAdapter<ValueRequest, ValueResponse>;
pub type ValueStateMachine = TvrPartitionStateMachine<ValueRequest, ValueResponse>;

pub type Raft = openraft::Raft<ValueConfig>;

pub mod typ {
    use crate::ValueConfig;
    use openraft::BasicNode;
    use timevault::raft::TvrNodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TvrNodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TvrNodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TvrNodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TvrNodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TvrNodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<TvrNodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<ValueConfig>;
}

pub async fn start_example_raft_node(node_id: TvrNodeId, root : &str, http_addr: String) -> anyhow::Result<()> {
    // Create a partition for storing raft logs and state machine snapshots.
    let root = std::path::PathBuf::from(&root);
    std::fs::create_dir_all(&root)?;

    let part_id = Uuid::now_v7();

    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();

    let part = PartitionHandle::create(root.clone(), part_id, cfg)?;

    // Build storage components using timevault adapters.
    let log_store = ValueLogStore::new(part.clone(), node_id);
    let state_machine_store = ValueStateMachine::new(part.clone())?;

    // Build openraft Config
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    let config = Arc::new(config.validate()?);

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Network {};

    // Create a local raft instance.
    let raft = Raft::new(
        node_id,
        config,
        network,
        log_store,
        state_machine_store,
    )
    .await?;

    // Create an application that will store all the instances created above, this will
    // later be used on the actix-web services.
    let app_data = Data::new(App {
        id: node_id,
        addr: http_addr.clone(),
        raft
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // application API
            .service(api::write)
            .service(api::read)
            //.service(api::consistent_read)
    });

    let x = server.bind(http_addr)?;

    x.run().await.expect("run failed");
    Ok(())
}
