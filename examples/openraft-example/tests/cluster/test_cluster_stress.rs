use std::backtrace::Backtrace;
use std::collections::HashMap;
#[allow(deprecated)]
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::thread;
use std::time::{Duration, Instant};

use actix_web::App as ActixApp;
use actix_web::HttpServer;
use actix_web::web::Data;
use maplit::btreeset;
use openraft::Config;
use openraft::LogId;
use openraft::SnapshotPolicy;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

use openraft_example::app::App;
use openraft_example::client::ExampleClient;
use openraft_example::network::{Network, api, management, raft};
use openraft_example::state::{DeviceStatus, ExampleEvent, ExampleStateMachine, new_shared_device_state};
use openraft_example::{ExampleLogStore, Raft};
use timevault::PartitionHandle;
use timevault::raft::TvrNodeId;
use timevault::store::partition::PartitionConfig;
use timevault::store::paths;
use uuid::Uuid;

#[allow(deprecated)]
pub fn log_panic(panic: &PanicInfo) {
    let backtrace = {
        format!("{:?}", Backtrace::force_capture())
        // #[cfg(feature = "bt")]
        // {
        //     format!("{:?}", Backtrace::force_capture())
        // }
        //
        // #[cfg(not(feature = "bt"))]
        // {
        //     "backtrace is disabled without --features 'bt'".to_string()
        // }
    };

    eprintln!("{}", panic);

    if let Some(location) = panic.location() {
        tracing::error!(
            message = %panic,
            backtrace = %backtrace,
            panic.file = location.file(),
            panic.line = location.line(),
            panic.column = location.column(),
        );
        eprintln!("{}:{}:{}", location.file(), location.line(), location.column());
    } else {
        tracing::error!(message = %panic, backtrace = %backtrace);
    }

    eprintln!("{}", backtrace);
}

static TRACING_INIT: Once = Once::new();

fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true)
            .with_ansi(false)
            .with_env_filter(filter)
            .try_init();
    });
}

#[derive(Clone)]
struct NodeConfig {
    pub id: TvrNodeId,
    pub http_addr: String,
    pub log_part: Uuid,
    pub state_part: Uuid,
}

impl NodeConfig {
    fn new(id: TvrNodeId, http_addr: &str) -> Self {
        Self {
            id,
            http_addr: http_addr.to_string(),
            log_part: Uuid::now_v7(),
            state_part: Uuid::now_v7(),
        }
    }
}

struct NodeHandle {
    id: TvrNodeId,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<thread::JoinHandle<()>>,
}

impl NodeHandle {
    fn stop_inner(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }

    fn stop(mut self) {
        self.stop_inner();
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

fn spawn_node(root: PathBuf, config: NodeConfig) -> NodeHandle {
    let node_id = config.id.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = thread::spawn(move || {
        let runtime = Runtime::new().expect("create tokio runtime for node");
        let res = runtime.block_on(run_node(root, config, shutdown_rx));
        if let Err(err) = res {
            eprintln!("node terminated with error: {err:?}");
        }
    });

    NodeHandle {
        id : node_id,
        shutdown: Some(shutdown_tx),
        join: Some(handle),
    }
}

async fn run_node(root: PathBuf, config: NodeConfig, shutdown_rx: oneshot::Receiver<()>) -> anyhow::Result<()> {
    let NodeConfig { id, http_addr, log_part, state_part } = config;

    let mut part_cfg = PartitionConfig::default();
    part_cfg.format_plugin = "jsonl".to_string();

    let log_part = open_or_create_partition(&root, log_part, &part_cfg)?;
    let event_part = open_or_create_partition(&root, state_part, &part_cfg)?;

    let log_store = ExampleLogStore::new(log_part, id);
    let devices = new_shared_device_state();
    let state_machine_store = ExampleStateMachine::new(event_part.clone(), devices.clone())?;

    let mut config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(20);
    let config = Arc::new(config.validate()?);

    let network = Network {};
    let raft = Raft::new(id, config, network, log_store, state_machine_store).await?;

    let app_data = Data::new(App {
        id,
        addr: http_addr.clone(),
        raft,
        devices,
        event_partition: event_part,
    });

    let server = HttpServer::new(move || {
        ActixApp::new()
            .app_data(app_data.clone())
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            .service(api::write)
            .service(api::read)
            .service(api::transfer_manifest)
            .service(api::transfer_chunk)
            .service(api::transfer_index)
    })
    .bind(http_addr.clone())?
    .run();

    let handle = server.handle();
    tokio::pin!(server);

    tokio::select! {
        res = &mut server => {
            res?;
            Ok(())
        }
        _ = async {
            let _ = shutdown_rx.await;
        } => {
            handle.stop(true).await;
            Ok(())
        }
    }
}

fn open_or_create_partition(root: &PathBuf, id: Uuid, cfg: &PartitionConfig) -> anyhow::Result<PartitionHandle> {
    let part_dir = paths::partition_dir(root, id);
    let metadata_path = paths::partition_metadata(&part_dir);
    if metadata_path.exists() {
        Ok(PartitionHandle::open(root.clone(), id)?)
    } else {
        Ok(PartitionHandle::create(root.clone(), id, cfg.clone())?)
    }
}

async fn wait_for_leader(client: &ExampleClient, timeout: Duration) -> anyhow::Result<TvrNodeId> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(metrics) = client.metrics().await {
            if let Some(leader) = metrics.current_leader {
                return Ok(leader);
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for leader");
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_snapshot(client: &ExampleClient, min_index: u64, timeout: Duration) -> anyhow::Result<LogId<TvrNodeId>> {
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = client.metrics().await?;
        if let Some(snapshot) = metrics.snapshot {
            if snapshot.index >= min_index {
                return Ok(snapshot);
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for snapshot; last snapshot: {:?}", metrics.snapshot);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Setup a cluster of 3 nodes, flood it with events to force a snapshot, then
/// restart the cluster and verify the state is recovered from that snapshot.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_stress() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));

    init_tracing();

    let root = PathBuf::from(format!("./var/{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root)?;

    let node_configs = vec![
        NodeConfig::new(1, "127.0.0.1:21001"),
        NodeConfig::new(2, "127.0.0.1:21002"),
        NodeConfig::new(3, "127.0.0.1:21003"),
    ];

    let mut handles: Vec<NodeHandle> = node_configs.iter().cloned().map(|cfg| spawn_node(root.clone(), cfg)).collect();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_cfg = &node_configs[0];
    let client = ExampleClient::new(leader_cfg.id, leader_cfg.http_addr.clone());

    client.init().await?;
    client.add_learner((node_configs[1].id, node_configs[1].http_addr.clone())).await?;
    client.add_learner((node_configs[2].id, node_configs[2].http_addr.clone())).await?;
    client.change_membership(&btreeset! {1,2,3}).await?;

    wait_for_leader(&client, Duration::from_secs(10)).await?;

    tokio::time::sleep(Duration::from_secs(5000)).await;

    let base_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let device_ids: Vec<Uuid> = (0..128).map(|_| Uuid::now_v7()).collect();
    let mut expected_statuses: HashMap<Uuid, DeviceStatus> = HashMap::new();
    let total_events = 100u64;
    let snapshot_min_index = 40u64;

    for idx in 0..total_events {
        let device = device_ids[idx as usize % device_ids.len()];
        let event_id = Uuid::now_v7();
        let timestamp = base_timestamp + idx as i64;
        let is_online = idx % 3 != 0;
        let event = if is_online {
            ExampleEvent::DeviceOnline { event_id, device_id: device, timestamp }
        } else {
            ExampleEvent::DeviceOffline { event_id, device_id: device, timestamp }
        };
        expected_statuses.insert(
            device,
            DeviceStatus {
                device_id: device,
                is_online,
                last_event_id: event_id,
                last_timestamp: timestamp,
            },
        );
        client.write(&event).await?;
    }

    let snapshot_log = wait_for_snapshot(&client, snapshot_min_index, Duration::from_secs(60)).await?;
    assert!(snapshot_log.index >= snapshot_min_index);

    let snapshot_path = paths::partition_dir(&root, leader_cfg.state_part).join("raft_state.json");
    assert!(snapshot_path.exists(), "snapshot file should exist at {:?}", snapshot_path);

    for handle in handles.drain(..) {
        handle.stop();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut handles: Vec<NodeHandle> = node_configs.iter().cloned().map(|cfg| spawn_node(root.clone(), cfg)).collect();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ExampleClient::new(leader_cfg.id, leader_cfg.http_addr.clone());
    wait_for_leader(&client, Duration::from_secs(20)).await?;

    let statuses = client.read().await?;
    assert!(statuses.len() >= expected_statuses.len());
    for (device, expected) in &expected_statuses {
        let Some(found) = statuses.iter().find(|status| status.device_id == *device) else {
            panic!("device {:?} missing after restart", device);
        };
        assert_eq!(found.is_online, expected.is_online);
        assert_eq!(found.last_event_id, expected.last_event_id);
        assert_eq!(found.last_timestamp, expected.last_timestamp);
    }

    let verification_event = ExampleEvent::DeviceOnline {
        event_id: Uuid::now_v7(),
        device_id: device_ids[0],
        timestamp: base_timestamp + total_events as i64 + 1,
    };
    client.write(&verification_event).await?;

    let refreshed = client.read().await?;
    assert!(refreshed.iter().any(|status| status.device_id == device_ids[0] && status.is_online));

    for handle in handles.drain(..) {
        handle.stop();
    }

    // Clean up the temporary storage root so repeated test runs don't accumulate data.
    if let Err(err) = std::fs::remove_dir_all(&root) {
        tracing::warn!(?err, ?root, "failed to clean up temporary cluster directory");
    }

    Ok(())
}
