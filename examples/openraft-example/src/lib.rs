#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::sync::Arc;

use crate::app::App;
use crate::network::Network;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::state::{ExampleStateMachine, SharedDeviceState, new_shared_device_state};
use actix_web::HttpServer;
use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use openraft::{Config, SnapshotPolicy};
use parking_lot::Mutex;
use timevault::PartitionHandle;
use timevault::raft::log::TvrLogAdapter;
use timevault::raft::{TvrConfig, TvrNodeId};
use timevault::store::partition::PartitionConfig;
use tokio::sync::oneshot;
use uuid::Uuid;

pub mod app;
pub mod client;
pub mod network;
pub mod state;

pub use state::{ExampleEvent, ExampleResponse};

pub type ExampleConfig = TvrConfig<ExampleEvent, ExampleResponse>;
pub type ExampleLogStore = TvrLogAdapter<ExampleEvent, ExampleResponse>;

pub type Raft = openraft::Raft<ExampleConfig>;

pub mod typ {
    use crate::ExampleConfig;
    use openraft::BasicNode;
    use timevault::raft::TvrNodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TvrNodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TvrNodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TvrNodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TvrNodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TvrNodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<TvrNodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<ExampleConfig>;
}

pub async fn start_example_raft_node(node_id: TvrNodeId, root: &str, http_addr: String) -> anyhow::Result<()> {
    // Create a partition for storing raft logs and state machine snapshots.
    let root = std::path::PathBuf::from(&root);
    std::fs::create_dir_all(&root)?;

    let log_part_id = Uuid::now_v7();
    let event_part_id = Uuid::now_v7();

    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();

    let log_part = PartitionHandle::create(root.clone(), log_part_id, cfg.clone())?;
    let event_part = PartitionHandle::create(root.clone(), event_part_id, cfg)?;

    // Build storage components using timevault adapters.
    let log_store = ExampleLogStore::new(log_part, node_id);

    let devices: SharedDeviceState = new_shared_device_state();
    let state_machine_store = ExampleStateMachine::new(event_part.clone(), devices.clone())?;

    // Build openraft Config
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(40),
        ..Default::default()
    };
    let config = Arc::new(config.validate()?);

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Network {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config, network, log_store, state_machine_store).await?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Create an application that will store all the instances created above, this will
    // later be used on the actix-web services.
    let app_data = Data::new(App {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        devices,
        event_partition: event_part,
        shutdown: Mutex::new(Some(shutdown_tx)),
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
            .service(api::transfer_manifest)
            .service(api::transfer_chunk)
            .service(api::transfer_index)
            .service(api::shutdown)
        //.service(api::consistent_read)
    });

    let server = server.bind(http_addr)?.run();
    let handle = server.handle();
    tokio::pin!(server);

    tokio::select! {
        res = &mut server => {
            res?;
            Ok(())
        }
        _ = shutdown_rx => {
            tracing::info!(node_id, "shutdown signal received");
            let _ = handle.stop(false);
            tracing::info!(node_id, "actix stop requested");
            Ok(())
        }
    }
}
