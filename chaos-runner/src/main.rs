use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use openraft_example::client::AppClient;
use openraft_example::domain::{AppEvent, DeviceStatus};
use parking_lot::Mutex;
use rand::prelude::*;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};
use uuid::Uuid;

#[derive(Parser, Debug, Clone)]
struct Opt {
    #[clap(long, default_value_t = 3)]
    nodes: u64,
    #[clap(long, default_value_t = 10_000)]
    events: u64,
    #[clap(long, default_value_t = 1_000)]
    snapshot_every: u64,
    #[clap(long, default_value_t = 300)]
    devices: u64,
    #[clap(long, default_value_t = 5)]
    partitions: u64,
    #[clap(long, default_value_t = 21000)]
    base_port: u16,
    #[clap(long, default_value = "target/chaos-run")] 
    run_root: String,
    #[clap(long, default_value = "target/debug/openraft-example")]
    app_bin: Option<PathBuf>,
    #[clap(long)]
    rate_per_sec: Option<u64>,
    #[clap(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Clone)]
struct Node { id: u64, addr: String, root: PathBuf }

struct ProcNode { meta: Node, child: Child }

struct Cluster { nodes: HashMap<u64, ProcNode> }

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let run_root = PathBuf::from(&opt.run_root);

    // Clean up any stale data from previous runs to avoid inconsistent state
    if run_root.exists() {
        eprintln!("[chaos] cleaning up stale data directory: {}", run_root.display());
        std::fs::remove_dir_all(&run_root)?;
    }
    std::fs::create_dir_all(&run_root)?;

    let app_bin = resolve_app_bin(opt.app_bin.clone())?;
    println!("Using app bin: {}", app_bin.display());

    // Build nodes
    let mut nodes_meta = Vec::new();
    for i in 0..opt.nodes {
        let id = i + 1;
        let addr = format!("127.0.0.1:{}", opt.base_port + i as u16);
        let root = run_root.join(format!("node-{}", id));
        nodes_meta.push(Node { id, addr, root });
    }

    // Start all nodes
    let cluster = std::sync::Arc::new(Mutex::new(Cluster { nodes: HashMap::new() }));
    for nm in nodes_meta.clone() { start_node(&mut cluster.lock(), &app_bin, nm.clone(), Some(opt.snapshot_every))?; }
    wait_ready_all(&cluster.lock(), Duration::from_secs(5)).await?;

    // Bootstrap cluster via client 1
    let client = AppClient::new(1, nodes_meta[0].addr.clone());
    client.init().await.context("init cluster")?;
    for nm in nodes_meta.iter().skip(1) { client.add_learner((nm.id as u64, nm.addr.clone())).await.context("add learner")?; }
    let ids = nodes_meta.iter().map(|n| n.id as u64).collect::<std::collections::BTreeSet<_>>();
    client.change_membership(&ids).await.context("change membership")?;

    // Workload setup
    let mut rng = StdRng::seed_from_u64(opt.seed);
    let partitions: Vec<Uuid> = (0..opt.partitions).map(|_| Uuid::now_v7()).collect();
    let devices: Vec<Uuid> = (0..opt.devices).map(|_| Uuid::now_v7()).collect();
    let expected = std::sync::Arc::new(Mutex::new(HashMap::<Uuid, DeviceStatus>::new()));

    // Give the cluster a brief warm-up before chaos
    sleep(Duration::from_millis(1500)).await;

    // Chaos controller task
    let cluster_c = cluster.clone();
    let nodes_meta_c = nodes_meta.clone();
    let app_bin_c = app_bin.clone();
    let mut rng_chaos = StdRng::seed_from_u64(opt.seed ^ 0xDEADBEEF);
    let chaos = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(1500 + rng_chaos.gen_range(0..1200))).await;
            let flip: u8 = rng_chaos.gen_range(0..100);
            if flip < 40 {
                if let Ok(metrics) = AppClient::new(nodes_meta_c[0].id as u64, nodes_meta_c[0].addr.clone()).metrics().await {
                    if let Some(leader) = metrics.current_leader {
                        eprintln!("[chaos] killing leader {}", leader);
                        // SIGTERM then SIGKILL if needed without holding lock across awaits
                        {
                            let mut cl = cluster_c.lock();
                            let _ = term_node(&mut cl, leader as u64);
                        }
                        if let Some(nm) = nodes_meta_c.iter().find(|n| n.id == leader as u64) { let _ = wait_ready_one(&nm.addr, Duration::from_secs(5)).await; }
                        sleep(Duration::from_millis(300)).await;
                        {
                            let mut cl = cluster_c.lock();
                            let _ = kill_node_now(&mut cl, leader as u64);
                            if let Some(nm) = nodes_meta_c.iter().find(|n| n.id == leader as u64) { let _ = restart_node(&mut cl, nm.clone(), app_bin_c.clone(), Some(1000)); }
                        }
                    }
                }
            } else if flip < 80 {
                let pick = rng_chaos.gen_range(0..nodes_meta_c.len());
                let id = nodes_meta_c[pick].id;
                eprintln!("[chaos] killing follower {}", id);
                { let mut cl = cluster_c.lock(); let _ = term_node(&mut cl, id); }
                sleep(Duration::from_millis(300)).await;
                { let mut cl = cluster_c.lock(); let _ = kill_node_now(&mut cl, id); let _ = restart_node(&mut cl, nodes_meta_c[pick].clone(), app_bin_c.clone(), Some(1000)); }
                let _ = wait_ready_one(&nodes_meta_c[pick].addr, Duration::from_secs(5)).await;
            } else {
                eprintln!("[chaos] restart all nodes");
                for nm in nodes_meta_c.iter() { let mut cl = cluster_c.lock(); let _ = term_node(&mut cl, nm.id); }
                sleep(Duration::from_millis(500)).await;
                for nm in nodes_meta_c.iter() { let mut cl = cluster_c.lock(); let _ = kill_node_now(&mut cl, nm.id); let _ = restart_node(&mut cl, nm.clone(), app_bin_c.clone(), Some(1000)); }
                // drop lock before awaits
                { let _ = (); }
                let addrs: Vec<String> = nodes_meta_c.iter().map(|n| n.addr.clone()).collect();
                if let Ok(_) = wait_ready_addrs(&addrs, Duration::from_secs(10)).await { let _ = wait_for_leader(&nodes_meta_c[0].addr).await; }
            }
        }
    });

    // Event write loop
    let mut ts = now_ms();
    let mut writes = 0u64;
    let mut failed_writes = 0u64;
    let start = Instant::now();
    while writes < opt.events {
        let device = devices[rng.gen_range(0..devices.len())];
        let part = partitions[rng.gen_range(0..partitions.len())];
        ts += 1;
        let is_online = rng.r#gen::<bool>();
        let event_id = Uuid::now_v7();
        let event = if is_online { AppEvent::DeviceOnline { event_id, device_id: device, timestamp: ts as i64, partition_id: part } } else { AppEvent::DeviceOffline { event_id, device_id: device, timestamp: ts as i64, partition_id: part } };

        // Retry failed writes until successful
        loop {
            match client.write(&event).await {
                Ok(_) => {
                    let mut map = expected.lock();
                    map.insert(device, DeviceStatus { device_id: device, is_online, last_event_id: event_id, last_timestamp: ts as i64 });
                    writes += 1;
                    if writes % 1000 == 0 { eprintln!("[chaos] progress: {} writes (failed: {})", writes, failed_writes); }
                    break;
                }
                Err(e) => {
                    failed_writes += 1;
                    if failed_writes % 100 == 0 {
                        eprintln!("[chaos] write failed (total failures: {}): {:?}", failed_writes, e);
                    }
                    // Short delay before retry to avoid tight loop
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }

        if let Some(rps) = opt.rate_per_sec { if rps > 0 { sleep(Duration::from_millis((1000 / rps).max(1))).await; } }
        if writes % 2000 == 0 { eprintln!("[chaos] checkpoint verify at {}", writes); verify_state(&client, &expected).await?; }
    }
    println!("Wrote {} events in {:?} (total failures: {}, failure rate: {:.2}%)", writes, start.elapsed(), failed_writes, (failed_writes as f64 / (writes + failed_writes) as f64) * 100.0);

    // Final verify
    verify_state(&client, &expected).await?;
    verify_partitions(&nodes_meta[0].root, &expected).await?;

    chaos.abort();
    let mut cl = cluster.lock();
    for (_, pn) in cl.nodes.drain() { let _ = shutdown_child(pn.child).await; }
    Ok(())
}

fn resolve_app_bin(p: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = p { return Ok(p); }
    let mut here = std::env::current_dir()?;
    while here.parent().is_some() {
        let cand = here.join("target").join("debug").join(if cfg!(windows) { "openraft-example.exe" } else { "openraft-example" });
        if cand.exists() { return Ok(cand); }
        here = here.parent().unwrap().to_path_buf();
    }
    anyhow::bail!("could not resolve openraft-example binary; pass --app-bin")
}

fn start_node(cluster: &mut Cluster, app_bin: &Path, meta: Node, snapshot_every: Option<u64>) -> Result<()> {
    std::fs::create_dir_all(&meta.root)?;
    let mut cmd = Command::new(app_bin);
    // Prepare per-node log files
    let stdout_path = meta.root.join("stdout.log");
    let stderr_path = meta.root.join("stderr.log");
    let stdout_file = std::fs::OpenOptions::new().create(true).append(true).open(&stdout_path)?;
    let stderr_file = std::fs::OpenOptions::new().create(true).append(true).open(&stderr_path)?;

    cmd.arg("--id")
        .arg(meta.id.to_string())
        .arg("--http-addr")
        .arg(&meta.addr)
        .arg("--data-root")
        .arg(meta.root.display().to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    if let Some(n) = snapshot_every { cmd.env("OPENRAFT_SNAPSHOT_LOGS_SINCE_LAST", n.to_string()); }
    let child = cmd.spawn().with_context(|| format!("spawn node {}", meta.id))?;
    cluster.nodes.insert(meta.id, ProcNode { meta, child });
    Ok(())
}

async fn wait_ready_all(cluster: &Cluster, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    for (_, pn) in cluster.nodes.iter() {
        let url = format!("http://{}/ready", pn.meta.addr);
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline { anyhow::bail!("timed out waiting for ready {}", pn.meta.id); }
            match client.get(&url).send().await { Ok(resp) if resp.status().is_success() => break, _ => sleep(Duration::from_millis(100)).await }
        }
    }
    Ok(())
}

async fn wait_ready_addrs(addrs: &[String], timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    for addr in addrs.iter() {
        let url = format!("http://{}/ready", addr);
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline { anyhow::bail!("timed out waiting for ready {}", addr); }
            match client.get(&url).send().await { Ok(resp) if resp.status().is_success() => break, _ => sleep(Duration::from_millis(100)).await }
        }
    }
    Ok(())
}

async fn wait_ready_one(addr: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://{}/ready", addr);
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() > deadline { anyhow::bail!("timed out waiting for ready {}", addr); }
        match client.get(&url).send().await { Ok(resp) if resp.status().is_success() => break, _ => sleep(Duration::from_millis(100)).await }
    }
    Ok(())
}

async fn wait_for_leader(addr: &str) -> Result<()> {
    let client = AppClient::new(1, addr.to_string());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(m) = client.metrics().await { if m.current_leader.is_some() { return Ok(()); } }
        if Instant::now() > deadline { anyhow::bail!("timed out waiting for leader via {}", addr); }
        sleep(Duration::from_millis(200)).await;
    }
}

fn term_node(cluster: &mut Cluster, id: u64) -> Result<()> {
    if let Some(pn) = cluster.nodes.get_mut(&id) { if let Some(pid) = pn.child.id() { let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM); } }
    Ok(())
}

fn kill_node_now(cluster: &mut Cluster, id: u64) -> Result<()> {
    if let Some(pn) = cluster.nodes.get_mut(&id) { if let Some(pid) = pn.child.id() { let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL); } }
    Ok(())
}

fn restart_node(cluster: &mut Cluster, meta: Node, app_bin: PathBuf, snapshot_every: Option<u64>) -> Result<()> {
    cluster.nodes.remove(&meta.id);
    start_node(cluster, &app_bin, meta, snapshot_every)?;
    Ok(())
}

async fn shutdown_child(mut child: Child) -> Result<()> { let _ = child.kill().await; Ok(()) }

async fn verify_state(client: &AppClient, expected: &std::sync::Arc<Mutex<HashMap<Uuid, DeviceStatus>>>) -> Result<()> {
    let statuses = client.read().await.context("read statuses")?;
    let exp = expected.lock();
    for (dev, exp_st) in exp.iter() {
        match statuses.iter().find(|s| &s.device_id == dev) {
            Some(found) => {
                if !(found.is_online == exp_st.is_online && found.last_event_id == exp_st.last_event_id && found.last_timestamp == exp_st.last_timestamp) {
                    eprintln!(
                        "[verify] mismatch dev={} expected={{online:{}, id:{}, ts:{}}} found={{online:{}, id:{}, ts:{}}}",
                        dev,
                        exp_st.is_online,
                        exp_st.last_event_id,
                        exp_st.last_timestamp,
                        found.is_online,
                        found.last_event_id,
                        found.last_timestamp
                    );
                    if let Ok(m) = client.metrics().await {
                        eprintln!(
                            "[verify] metrics: leader={:?} last_log_index={:?} last_applied={:?}",
                            m.current_leader, m.last_log_index, m.last_applied
                        );
                    }
                    anyhow::bail!("mismatch for device {dev}");
                }
            }
            None => {
                eprintln!("[verify] missing device {}", dev);
                if let Ok(m) = client.metrics().await {
                    eprintln!(
                        "[verify] metrics: leader={:?} last_log_index={:?} last_applied={:?}",
                        m.current_leader, m.last_log_index, m.last_applied
                    );
                }
                anyhow::bail!("missing device {dev}");
            }
        }
    }
    Ok(())
}

async fn verify_partitions(node_root: &Path, _expected: &std::sync::Arc<Mutex<HashMap<Uuid, DeviceStatus>>>) -> Result<()> {
    let parts_root = node_root.join("partitions");
    anyhow::ensure!(parts_root.exists(), "partitions dir missing at {}", parts_root.display());
    let mut found_any = false;
    if let Ok(mut rd) = std::fs::read_dir(&parts_root) {
        if let Some(_e) = rd.next() { found_any = true; }
    }
    anyhow::ensure!(found_any, "no partitions present under {}", parts_root.display());
    Ok(())
}

fn now_ms() -> u128 { use std::time::{SystemTime, UNIX_EPOCH}; SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() }
