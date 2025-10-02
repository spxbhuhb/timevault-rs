use std::sync::Arc;

use actix_web::HttpServer;
use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use openraft::{Config, SnapshotPolicy};
use parking_lot::Mutex;

use crate::TvrNodeId;
use crate::app::App;
use crate::http;
use crate::raft::{self, Raft};
use crate::state_machine::{AppStateMachine, SharedDeviceState, new_shared_device_state};
use crate::storage::{open_or_create_log_store, open_state_store};

pub async fn start_app_node(node_id: TvrNodeId, root: &str, http_addr: String) -> anyhow::Result<()> {
    // Prepare storage roots
    let root = std::path::PathBuf::from(&root);
    std::fs::create_dir_all(&root)?;

    // Open Raft log store and state machine store
    let log_store = open_or_create_log_store(&root, node_id)?;

    let devices: SharedDeviceState = new_shared_device_state();
    let store_for_sm = open_state_store(&root)?;
    let state_machine_store = AppStateMachine::new(store_for_sm, devices.clone())?;

    // Build openraft Config
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(40),
        ..Default::default()
    };
    let config = Arc::new(config.validate()?);

    // Create the network layer
    let network = raft::network_impl::Network {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config, network, log_store, state_machine_store).await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let app_data = Data::new(App {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        devices,
        root,
        shutdown: Mutex::new(Some(shutdown_tx)),
    });

    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(http::raft::append)
            .service(http::raft::snapshot)
            .service(http::raft::vote)
            // admin API
            .service(http::admin::init)
            .service(http::admin::add_learner)
            .service(http::admin::change_membership)
            .service(http::admin::metrics)
            // store transfer API
            .service(http::transfer::partitions)
            .service(http::transfer::transfer_manifest)
            .service(http::transfer::transfer_chunk)
            .service(http::transfer::transfer_index)
            // application API
            .service(http::app::write)
            .service(http::app::read)
            .service(http::health::ready)
            .service(http::health::live)
            .service(http::app::shutdown)
    });

    let server = server.bind(http_addr)?.run();
    let handle = server.handle();
    tokio::pin!(server);
    let mut server_stopped = false;

    tokio::select! {
        res = &mut server => {
            res?;
            server_stopped = true;
        }
        _ = shutdown_rx => {
            tracing::info!(node_id, "shutdown signal received");
            let _ = handle.stop(false);
            tracing::info!(node_id, "actix stop requested");
        }
    }

    if !server_stopped {
        server.await?;
    }

    Ok(())
}
