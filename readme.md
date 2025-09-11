# TimeVault

An embedded Rust library for persistently storing timestamped records (events, IoT measurements, alarms, etc.) in
files, grouped by partitions (UUIDs). Optimized for high throughput appends, inexpensive sequential reads by time range.

- In-scope: on-disk layout, directory structure, file formats, IDs, append semantics, indexing, retention, observability.
- Out-of-scope: Write-ahead logging (WAL), consensus/replication, and crash recovery.

TimeVault is intended to be used as a `RaftStateMachine` for [OpenRaft](https://github.com/databendlabs/openraft).

## Status

🚧 Early experimental. On-disk format may change. 
