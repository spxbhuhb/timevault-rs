use maplit::btreeset;
use openraft_example::client::ExampleClient;
use openraft_example::start_example_raft_node;
use openraft_example::state::{DeviceStatus, ExampleEvent};
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use example_test_utils::{allocate_node_addrs, client_for, get_addr, init_tracing, set_panic_hook, shutdown_node, shutdown_nodes, spawn_nodes, unique_test_root, wait_for_http_ready, wait_for_leader};

#[ignore] // not working yet
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_follower_replacement() -> anyhow::Result<()> {
    set_panic_hook();
    init_tracing();

    let root = unique_test_root("test_cluster_follower_replacement");

    let mut node_addrs: BTreeMap<u64, String> = allocate_node_addrs([1, 2, 3]);
    let handles = spawn_nodes(&root, &node_addrs).await;
    wait_for_http_ready(&node_addrs, Duration::from_secs(5)).await?;

    let mut handle_map: BTreeMap<u64, tokio::task::JoinHandle<()>> = node_addrs.keys().cloned().zip(handles.into_iter()).collect();

    let client = client_for(&node_addrs, 1)?;

    client.init().await?;
    client.add_learner((2, get_addr(&node_addrs, 2)?)).await?;
    client.add_learner((3, get_addr(&node_addrs, 3)?)).await?;
    client.change_membership(&btreeset! {1,2,3}).await?;
    wait_for_leader(&client, Duration::from_secs(10)).await?;

    let partition_id = Uuid::now_v7();
    let base_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let first_event = ExampleEvent::DeviceOnline {
        event_id: Uuid::now_v7(),
        device_id: Uuid::now_v7(),
        timestamp: base_timestamp,
        partition_id,
    };
    client.write(&first_event).await?;

    // Simulate follower crash by shutting down node 2.
    if let Some(handle) = handle_map.remove(&2) {
        let addr = node_addrs.get(&2).cloned().expect("address for node 2");
        shutdown_node(2, &addr, handle).await?;
        node_addrs.remove(&2);
    } else {
        anyhow::bail!("missing handle for node 2");
    }

    client.change_membership(&btreeset! {1,3}).await?;
    wait_for_leader(&client, Duration::from_secs(10)).await?;

    // Add a new node (id 4) as replacement follower.
    let new_addr_map = allocate_node_addrs([4]);
    let new_addr = new_addr_map.get(&4).cloned().expect("address for node 4");
    let node_root = root.join("node-4");
    let node_root_str = node_root.to_string_lossy().to_string();
    let addr_for_task = new_addr.clone();
    let handle4 = tokio::spawn(async move {
        if let Err(err) = start_example_raft_node(4, &node_root_str, addr_for_task).await {
            panic!("node 4 failed: {err:?}");
        }
    });

    node_addrs.insert(4, new_addr.clone());
    handle_map.insert(4, handle4);

    let mut single = BTreeMap::new();
    single.insert(4, new_addr.clone());
    wait_for_http_ready(&single, Duration::from_secs(5)).await?;

    client.add_learner((4, get_addr(&node_addrs, 4)?)).await?;
    client.change_membership(&btreeset! {1,3,4}).await?;
    wait_for_leader(&client, Duration::from_secs(10)).await?;

    // Write another event and ensure the new follower sees it.
    let replacement_event = ExampleEvent::DeviceOffline {
        event_id: Uuid::now_v7(),
        device_id: Uuid::now_v7(),
        timestamp: base_timestamp + 1,
        partition_id,
    };
    client.write(&replacement_event).await?;

    let statuses = ExampleClient::new(4, get_addr(&node_addrs, 4)?).read().await?;
    assert!(
        statuses
            .iter()
            .any(|status: &DeviceStatus| status.device_id == replacement_event.device_id() && status.last_event_id == replacement_event.event_id())
    );

    shutdown_nodes(&node_addrs, handle_map.into_values().collect()).await?;

    Ok(())
}

trait EventExt {
    fn device_id(&self) -> Uuid;
    fn event_id(&self) -> Uuid;
}

impl EventExt for ExampleEvent {
    fn device_id(&self) -> Uuid {
        match self {
            ExampleEvent::DeviceOnline { device_id, .. } | ExampleEvent::DeviceOffline { device_id, .. } => *device_id,
        }
    }

    fn event_id(&self) -> Uuid {
        match self {
            ExampleEvent::DeviceOnline { event_id, .. } | ExampleEvent::DeviceOffline { event_id, .. } => *event_id,
        }
    }
}
