# Timevault partitions as OpenRaft log storage

To use timevault partitions as OpenRaft log storage, we need an adapter that provides
the necessary mapping.

The adapter should implement `RaftLogStorage` with the following details.

## Format plugin

Timevault partitions store opaque bytes. To handle OpenRaft specific information,
a format plugin is required.

The format plugin should store data in JSON format, simply serialized with serde.

Also, the format plugin must be type-dependent as OpenRaft log records are type-dependent.

## Log state

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

## Vote

`save_vote` should simply persist the vote into a file called `raft_vote.json` into the partition directory.
`read_vote` should read the vote from the file.

## Append

The adapter should serialize the log records and call the `append` method of the timevault partition.

## Purge

The adapter should:

1. Persist the last deleted log ID in a file called `raft_purge.json` into the partition directory.
2. Should call the `purge` method of the timevault partition with the index of the passed log ID.

## Truncate

The adapter should call the `truncate` method of the timevault partition with the `index` of the
passed log ID.

## Synchronization

All write operations of the adapter must be serialized and keep the order they are called. 