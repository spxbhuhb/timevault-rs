# Timevault partitions as OpenRaft log storage

To use timevault partitions as OpenRaft log storage, we need an adapter that provides
the necessary mapping.

- targeted minimum OpenRaft version is: 0.9.21
- one OpenRaft node per timevault partition

- The adapter should implement `RaftLogStorage` and `RaftLogReader` with the following details.
- The adapter should not implement `RaftStateStorage`.

## Mapping

- Key choice:
  - Use Raft log index as the partition `order_key`.
  - Configure the partition with `key_is_timestamp = false` to avoid time-based rolling and indexing logic.
  
## Format plugin

Timevault partitions store opaque bytes. To handle OpenRaft specific information,
a format plugin is required.

- Serialization format:
  - Use the existing `jsonl` plugin (newline-delimited JSON). Each log record should contain at least:
    - `log_id`: `{ index: u64, term: u64, node_id: uuid::Uuid }`
    - `payload`: the OpenRaft entry payload (serde-serializable)
    - Optionally, any metadata OpenRaft requires to reconstruct `LogId` reliably
  - The adapter’s `encode` should ensure a trailing newline to match the scanner.

## Implementation details

- Module name: `raft`
- Required feature: none, the module is included by default
- Adapter struct name: `RaftLogAdapter`
  - implements both `RaftLogStorage` and `RaftLogReader`
  - contains all necessary data for the adapter to operate
- Define `RaftTypeConfig` with these types:
  - `D`: `Request(serde_json::Value)` (define the struct Request)
  - `R`: `Response(Result<serde_json::Value, serde_json::Value>)` (define the struct Response)
  - `NodeId`: `uuid::Uuid`
  - `Node`: `BasicNode`
  - `SnapshotData`: `Cursor<Vec<u8>>`
  - `AsyncRuntime` : `TokioRuntime`
- for `LogFlushed` use `openraft::storage::LogFlushed`

## Synchronization

- The adapter uses a channel and a background thread to synchronize all operations.
- The thread starts when the adapter is created and stops when the adapter is dropped.

All operations follow the same pattern:

- an instance of RaftLogOperation is created
- this instance is put into the channel that belongs to the adapter
- the background thread reads from the channel and performs the operation
- in case of `append`:
  - the original call to the adapter returns without waiting for the operation to complete and without calling the callback
  - the callback provided by OpenRaft is called by the background thread when the operation completes
- in case of other operations
  - the original call to the adapter waits until the operation completes, then returns

Implementation notes:

- The operations are represented by an enum and use `std::sync::mpsc` channels.
- The background thread is a `std::thread`.
- There is a graceful shutdown operation as well.

## `append`

Append simply calls the `append` method of the timevault partition for each log record.

The OpenRaft requirement of log visibility is provided by the synchronization mechanism described above.
As all operations are queued into one queue and executed from that queue one-by-one, the actual log
visibility is guaranteed.

## `try_get_log_entries`

- Calls `read_range_blocks` of the timevault partition.
- Uses the format plugin to deserialize the log records.
- Filters out the log records that are not in the range.
- Returns the log records.

## `get_log_state`

### Last deleted log ID

1. `get_log_state` of `RaftLogStorage` returns with the last log ID and the last deleted log ID.
2. Log ID in OpenRaft consists of an index, a term and a node ID.
3. Timevault handles opaque bytes; it does not know the meaning of index, term and node ID.
4. Timevault partition purge stores the last deleted order_key which is the index of the last deleted log.

Timevault does not store the term and node ID required by `RaftLogStorage`, these should be persisted by the adapter.

When the adapter performs a purge, it must:

- persist the last deleted log ID
- call timevault partition `purge`
- return with success

During recovery, the adapter should check if the last deleted log ID persisted contains the same index
as the `last_purge_id` of the timevault partition.

1. If so, no recovery is required.
2. Otherwise, the adapter should call `purge` with the last deleted log ID persisted.

### Last log ID

1. `get_log_state` of `RaftLogStorage` returns with the last log ID.
2. Timevault handles opaque bytes; it does not know the meaning of index, term and node ID.
3. Moreover, the last log ID is something inside the log record, depending on whatever OpenRaft decides it is.

During adapter recovery, the adapter should use a format plugin to recover the last record
of from the timevault partition. Then decode that record and get the last log ID from it.

> [!NOTE]
> This is basically how the `rockstore` example of OpenRaft works. It indexes log records with the index 
> only (without the term and the node ID). When the `get_log_state` is called, it reads the last log record
> from the table, deserializes it and returns the last log ID.

If there is no log record in the timevault partition, the adapter should return the last deleted log ID.

## `save_vote`

`save_vote` should simply persist the vote into a file called `raft_vote.json` into the partition directory.
Writing the file must be durable (same guarantees as for timevault partition metadata).

## `read_vote`

`read_vote` should read the vote from the file called `raft_vote.json` into the partition directory.

## Purge

The adapter should:

1. Persist the last deleted log ID in a file called `raft_purge.json` into the partition directory 
   (same durability guarantees as for timevault partition metadata).
2. Should call the `purge` method of the timevault partition with the index of the passed log ID.

As purge is inclusive, it already matches OpenRaft's definition.

## Truncate

The adapter should call the `truncate` method of the timevault partition with the `index` of the
passed log ID.

As truncate is inclusive, it already matches OpenRaft's definition.