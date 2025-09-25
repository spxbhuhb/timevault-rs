# Timevault partitions as OpenRaft states

When using a timevault partition as part of an OpenRaft state, special
care must be taken as:

- partition chunks must have identical chunk ids and content on all replicas
- operations such as truncate and purge must be executed through raft
- changing configuration of a partition must be executed through raft

Identical chunk ids are ensured by default, see:

- [File naming](design.md#22-file-naming)
- [Chunk Rolling](design.md#42-chunk-rolling)
- [Invariants](design.md#11-correctness-invariants)

## State snapshots

As partitions may contain large amounts of data, they should not be included in the
state snapshots directly.

Instead:

- the partition data should be pulled before the node is added to the cluster
- during snapshot installation, the differences between pulled data and the installed state should be applied

Instead, the state snapshot should include only:

- partition id
- last chunk id
- last chunk size
- last order key

1. The state can start applying operations as soon as the data in the last chunk is available.
