use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use openraft::LogId;
use openraft_example::client::ExampleClient;
use openraft_example::start_example_raft_node;
use reqwest::Client as HttpClient;
use timevault::raft::TvrNodeId;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Install a panic hook that logs a captured backtrace and location.
#[allow(deprecated)]
pub fn set_panic_hook() {
    std::panic::set_hook(Box::new(|panic| {
        let backtrace = format!("{:?}", Backtrace::force_capture());

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
    }));
}

static TRACING_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize tracing subscriber once for tests.
pub fn init_tracing() {
    TRACING_INIT.call_once(|| {
        // Start from env if provided; otherwise default to `info`
        let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        // Reduce noise from dependencies: suppress `info` from openraft and actix
        filter = filter
            .add_directive("openraft=info".parse().unwrap())
            .add_directive("actix_web=warn".parse().unwrap())
            .add_directive("actix_server=warn".parse().unwrap())
            .add_directive("actix_http=warn".parse().unwrap());

        let _ = tracing_subscriber::fmt()
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true)
            .with_ansi(false)
            .with_env_filter(filter)
            .try_init();
    });
}

/// Create a unique per-test directory under the workspace `target/test` root.
pub fn unique_test_root(test_name: &str) -> PathBuf {
    let id = Uuid::now_v7();
    test_utils::test_dir(test_name, id)
}

/// Allocate loopback HTTP addresses with ephemeral ports for the given node IDs.
pub fn allocate_node_addrs<I>(node_ids: I) -> BTreeMap<u64, String>
where
    I: IntoIterator<Item = u64>,
{
    node_ids
        .into_iter()
        .map(|node_id| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap_or_else(|err| panic!("failed to allocate port for node {node_id}: {err}"));
            let port = listener.local_addr().unwrap_or_else(|err| panic!("failed to read local addr for node {node_id}: {err}")).port();
            drop(listener);
            (node_id, format!("127.0.0.1:{port}"))
        })
        .collect()
}

/// Resolve address for a node ID.
pub fn get_addr(node_addrs: &BTreeMap<u64, String>, node_id: u64) -> anyhow::Result<String> {
    node_addrs.get(&node_id).cloned().ok_or_else(|| anyhow::anyhow!("node {} not found", node_id))
}

/// Spawn one async task per node that runs `start_example_raft_node`.
pub async fn spawn_nodes(root: &PathBuf, node_addrs: &BTreeMap<u64, String>) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::with_capacity(node_addrs.len());
    for (&node_id, addr) in node_addrs {
        let addr = addr.clone();
        let node_root = root.join(format!("node-{node_id}"));
        let node_root = node_root.to_string_lossy().to_string();
        handles.push(tokio::spawn(async move {
            if let Err(err) = start_example_raft_node(node_id, &node_root, addr).await {
                panic!("node {node_id} failed: {err:?}");
            }
        }));
    }
    handles
}

/// Post shutdown to all nodes and await their join handles with a timeout.
pub async fn shutdown_nodes(node_addrs: &BTreeMap<u64, String>, handles: Vec<JoinHandle<()>>) -> anyhow::Result<()> {
    let http = HttpClient::new();
    for (&node_id, addr) in node_addrs {
        let url = format!("http://{}/shutdown", addr);
        let resp = http.post(url).send().await.with_context(|| format!("sending shutdown to node {node_id}"))?;
        resp.error_for_status().with_context(|| format!("node {node_id} returned error status on shutdown"))?;
    }

    for handle in handles {
        let join_result = tokio::time::timeout(Duration::from_secs(10), handle).await.context("node task did not shutdown in time")?;
        join_result.expect("node task should shutdown cleanly");
    }
    Ok(())
}

/// Build an ExampleClient for the given leader id using the provided address map.
pub fn client_for(node_addrs: &BTreeMap<u64, String>, leader_id: u64) -> anyhow::Result<ExampleClient> {
    let addr = get_addr(node_addrs, leader_id)?;
    Ok(ExampleClient::new(leader_id, addr))
}

/// Poll metrics until a leader is present or timeout occurs.
pub async fn wait_for_leader(client: &ExampleClient, timeout: Duration) -> anyhow::Result<TvrNodeId> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(metrics) = client.metrics().await {
            if let Some(leader) = metrics.current_leader {
                return Ok(leader);
            }
        }

        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for leader");
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll metrics until a snapshot with at least `min_index` is observed or timeout occurs.
pub async fn wait_for_snapshot(client: &ExampleClient, min_index: u64, timeout: Duration) -> anyhow::Result<LogId<TvrNodeId>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let metrics = client.metrics().await?;
        if let Some(snapshot) = metrics.snapshot {
            if snapshot.index >= min_index {
                return Ok(snapshot);
            }
        }

        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for snapshot; last snapshot: {:?}", metrics.snapshot);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
