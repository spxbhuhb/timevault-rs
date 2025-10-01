use std::collections::BTreeMap;
use std::time::Duration;

use maplit::btreeset;
use openraft::BasicNode;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use openraft_example::client::ExampleClient;
use openraft_example::state::{DeviceStatus, ExampleEvent};

use example_test_utils::{allocate_node_addrs, client_for, init_tracing, set_panic_hook, shutdown_nodes, spawn_nodes, unique_test_root};

/// Setup a cluster of 3 nodes.
/// Write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster_simple() -> anyhow::Result<()> {
    // --- The client itself does not store addresses for all nodes, but just node id.
    //     Thus we need a supporting component to provide mapping from node id to node address.
    //     This is only used by the client. A raft node in this example stores node addresses in its
    // store.

    set_panic_hook();

    init_tracing();

    let root = unique_test_root("test_cluster_simple");

    let node_addrs: BTreeMap<u64, String> = allocate_node_addrs([1_u64, 2, 3]);

    // Helper to mirror old closure behavior for address lookup
    let get_addr = |node_id| example_test_utils::get_addr(&node_addrs, node_id);

    // --- Start 3 raft nodes in 3 threads.
    let node_handles = spawn_nodes(&root, &node_addrs).await;

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Create a client to the first node, as a control handle to the cluster.

    let client = client_for(&node_addrs, 1)?;

    // --- 1. Initialize the target node as a cluster of only one node.
    //        After init(), the single node cluster will be fully functional.

    info!("=== init single node cluster");
    client.init().await?;

    info!("=== metrics after init");
    let _x = client.metrics().await?;

    // --- 2. Add node 2 and 3 to the cluster as `Learner`, to let them start to receive log replication
    // from the        leader.

    info!("=== add-learner 2");
    let _x = client.add_learner((2, get_addr(2)?)).await?;

    info!("=== add-learner 3");
    let _x = client.add_learner((3, get_addr(3)?)).await?;

    info!("=== metrics after add-learner");
    let x = client.metrics().await?;

    assert_eq!(&vec![btreeset![1]], x.membership_config.membership().get_joint_config());

    let nodes_in_cluster = x.membership_config.nodes().map(|(nid, node)| (*nid, node.clone())).collect::<BTreeMap<_, _>>();
    let expected_nodes = node_addrs
        .iter()
        .map(|(node_id, addr)| (*node_id, BasicNode::new(addr.clone())))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(expected_nodes, nodes_in_cluster);

    // --- 3. Turn the two learners to members. A member node can vote or elect itself as leader.

    info!("=== change-membership to 1,2,3");
    let _x = client.change_membership(&btreeset! {1,2,3}).await?;

    // --- After change-membership, some cluster state will be seen in the metrics.
    //
    // ```text
    // metrics: RaftMetrics {
    //   current_leader: Some(1),
    //   membership_config: EffectiveMembership {
    //        log_id: LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 },
    //        membership: Membership { learners: {}, configs: [{1, 2, 3}] }
    //   },
    //   leader_metrics: Some(LeaderMetrics { replication: {
    //     2: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 7 }) },
    //     3: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 }) }} })
    // }
    // ```

    info!("=== metrics after change-member");
    let x = client.metrics().await?;
    assert_eq!(&vec![btreeset![1, 2, 3]], x.membership_config.membership().get_joint_config());

    // --- Try to write some application data through the leader.

    info!("=== write device online event");
    let device_id = uuid::Uuid::now_v7();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let event = ExampleEvent::DeviceOnline {
        event_id: uuid::Uuid::now_v7(),
        device_id,
        timestamp,
    };
    let _x = client.write(&event).await?;

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Read it on every node.

    info!("=== read statuses on node 1");
    let statuses = client.read().await?;
    assert!(statuses.iter().any(|s: &DeviceStatus| s.device_id == device_id && s.is_online));

    info!("=== read statuses on node 2");
    let client2 = ExampleClient::new(2, get_addr(2)?);
    let statuses = client2.read().await?;
    assert!(statuses.iter().any(|s: &DeviceStatus| s.device_id == device_id && s.is_online));

    info!("=== read statuses on node 3");
    let client3 = ExampleClient::new(3, get_addr(3)?);
    let statuses = client3.read().await?;
    assert!(statuses.iter().any(|s: &DeviceStatus| s.device_id == device_id && s.is_online));

    // --- Remove node 1,2 from the cluster.

    info!("=== change-membership to 3, ");
    let _x = client.change_membership(&btreeset! {3}).await?;
    
    info!("=== metrics after change-membership to {{3}}");
    let x = client.metrics().await?;
    assert_eq!(&vec![btreeset![3]], x.membership_config.membership().get_joint_config());

    shutdown_nodes(&node_addrs, node_handles).await?;

    Ok(())
}
