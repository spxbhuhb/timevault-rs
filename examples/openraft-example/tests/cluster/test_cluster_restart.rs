use maplit::btreeset;
use openraft_example::state::{DeviceStatus, ExampleEvent};
use std::collections::HashMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use example_test_utils::{
    allocate_node_addrs, client_for, get_addr, init_tracing, set_panic_hook, shutdown_nodes, spawn_nodes, unique_test_root, wait_for_http_ready, wait_for_leader, wait_for_snapshot,
};

// wait_for_leader and wait_for_snapshot moved to example-test-utils

/// Setup a cluster of 3 nodes, flood it with events to force a snapshot,
/// and verify state via reads and metrics.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_restart() -> anyhow::Result<()> {
    set_panic_hook();
    init_tracing();

    let root = unique_test_root("test_cluster_restart");

    let node_addrs = allocate_node_addrs([1, 2, 3]);
    let handles = spawn_nodes(&root, &node_addrs).await;
    wait_for_http_ready(&node_addrs, Duration::from_secs(5)).await?;

    let client = client_for(&node_addrs, 1)?;
    client.init().await?;
    client.add_learner((2, get_addr(&node_addrs, 2)?)).await?;
    client.add_learner((3, get_addr(&node_addrs, 3)?)).await?;
    client.change_membership(&btreeset! {1,2,3}).await?;

    wait_for_leader(&client, Duration::from_secs(10)).await?;

    let base_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let partition_id = Uuid::now_v7();
    let device_ids: Vec<Uuid> = (0..128).map(|_| Uuid::now_v7()).collect();
    let mut expected_statuses: HashMap<Uuid, DeviceStatus> = HashMap::new();
    let total_events = 100;
    let snapshot_min_index = 40;

    for idx in 0..total_events {
        let device = device_ids[idx as usize % device_ids.len()];
        let event_id = Uuid::now_v7();
        let timestamp = base_timestamp + idx;
        let is_online = idx % 3 != 0;
        let event = if is_online {
            ExampleEvent::DeviceOnline {
                event_id,
                device_id: device,
                timestamp,
                partition_id,
            }
        } else {
            ExampleEvent::DeviceOffline {
                event_id,
                device_id: device,
                timestamp,
                partition_id,
            }
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
        timestamp: base_timestamp + total_events + 1,
        partition_id,
    };
    client.write(&verification_event).await?;
    let (verification_event_id, verification_timestamp) = match &verification_event {
        ExampleEvent::DeviceOnline { event_id, timestamp, .. } | ExampleEvent::DeviceOffline { event_id, timestamp, .. } => (*event_id, *timestamp),
    };
    expected_statuses.insert(
        device_ids[0],
        DeviceStatus {
            device_id: device_ids[0],
            is_online: true,
            last_event_id: verification_event_id,
            last_timestamp: verification_timestamp,
        },
    );

    let refreshed = client.read().await?;
    assert!(refreshed.iter().any(|status| status.device_id == device_ids[0] && status.is_online));

    shutdown_nodes(&node_addrs, handles).await?;

    // --- Restart the cluster with the same root and same ports
    let handles2 = spawn_nodes(&root, &node_addrs).await;
    wait_for_http_ready(&node_addrs, Duration::from_secs(5)).await?;

    let client = client_for(&node_addrs, 1)?;
    let leader = wait_for_leader(&client, Duration::from_secs(10)).await?;
    tracing::info!(leader, "leader detected after restart");

    // The cluster should retain its membership configuration from the previous run.
    let metrics_after = client.metrics().await?;
    tracing::info!(?metrics_after, "metrics after restart");
    assert_eq!(&vec![btreeset![1, 2, 3]], metrics_after.membership_config.membership().get_joint_config());

    // Existing replicated state should still be readable.
    let statuses_after_restart = client.read().await?;
    for (device, expected) in &expected_statuses {
        let Some(found) = statuses_after_restart.iter().find(|status| status.device_id == *device) else {
            panic!("device {:?} missing after restart", device);
        };
        assert_eq!(found.is_online, expected.is_online);
        assert_eq!(found.last_event_id, expected.last_event_id);
        assert_eq!(found.last_timestamp, expected.last_timestamp);
    }

    // Basic operation check after restart
    let device_after = Uuid::now_v7();
    let event_after = ExampleEvent::DeviceOnline {
        event_id: Uuid::now_v7(),
        device_id: device_after,
        timestamp: base_timestamp + total_events + 2,
        partition_id,
    };
    client.write(&event_after).await?;
    let refreshed_after = client.read().await?;
    assert!(refreshed_after.iter().any(|status| status.device_id == device_after && status.is_online));

    shutdown_nodes(&node_addrs, handles2).await?;

    Ok(())
}
