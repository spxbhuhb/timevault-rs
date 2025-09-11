# TimeVault

## 0. Purpose & Scope

An embedded Rust library for persistently storing timestamped records (events, IoT measurements, alarms, etc.) in
files, grouped by partitions (UUIDs). Optimized for high-throughput appends, inexpensive sequential reads by time range.

- In-scope: on-disk layout, directory structure, file formats, IDs, append semantics, indexing, retention, observability.
- Out-of-scope (provided elsewhere): Write-ahead logging (WAL), consensus/replication, and crash recovery are handled
  by OpenRaft (upper layer).

### Expected use patterns

Main use cases:

1. Many small (one record) writes into different partitions (e.g., IoT measurements).
2. Bulk uploads (after a network outage).
3. Range queries between two timestamps.

Maximum record size is configurable (default: 1 MB).

## 1. Terminology & Data Model

- Partition: Logical stream of records identified by a UUID (partition id is exactly the UUID you provide).
- Record: Opaque bytes and timestamp (required). Library does not interpret the payload.
- Format plugin: User-supplied logic to parse, validate, compress, uncompress etc. a record.
- Chunk: Append-only file containing time-ordered records for a partition. Identified by UUIDv7.
- Index (per-chunk): Sparse mapping from timestamp ranges to byte ranges in its chunk to accelerate seeks.
- Metadata: Per-partition static info and rolling state (format versions, knobs).
- Manifest: Per-partition list of live chunks (ordered by time), their IDs, min/max timestamps, and file paths.
- Timestamps: i64 milliseconds since Unix epoch.

## 2. On-Disk Layout

### 2.1 Root & Sharding

```text
/store-root/
  partitions/
    <shard_hi>/<shard_lo>/<partition-uuid>/
```

- Sharding: Use the last two bytes of the partition UUID as hex nybbles:
  - For partition `13f98d87-e036-4804-829a-bcb965a9fa87` → `87/fa/13f98d87-e036-4804-829a-bcb965a9fa87/`
- Each partition directory contains:

```text
/<partition>/
  metadata.json
  manifest.jsonl
  chunks/
    <chunk-uuidv7>.chunk
    <chunk-uuidv7>.index
  tmp/                      (atomic rename staging)
  gc/                       (quarantine for deletion)
```

### 2.2 File Naming

- Chunk ID: 
  - UUIDv7 (time-ordered, simplifies ops & debugging).
  - The timestamp in the UUID is the timestamp of the first record in the chunk (minted from record timestamp).
- Filenames:
  - Chunk: <chunk-uuidv7>.chunk
  - Index: <chunk-uuidv7>.index

Chunk and index files are created as the first record in the chunk is appended, this ensures that the chunk ID 
is monotonic and known at the time of creation.

## 3. File Formats (v1)

### 3.1 Chunk file (*.chunk)

Chunk files are simple list of records. The library does not interpret the payload and uses the format plugin to 
parse, validate, compress, uncompress etc. records.

Record format is opaque. It is the responsibility of the caller to serialize/deserialize, determine length, 
timestamp, validate, compress, etc.

The manifest provides information about the chunk files, the library uses information from the manifest to route reads.

### 3.2 Index file (*.index) - Per Chunk

Sparse index entries to enable timestamp → offset seeks.

- File format is JSONL where each line represents one block (the example below is broken into multiple lines for clarity).
- The server loads the entire index into memory and uses binary search to find the block containing the timestamp.

```json
{
  "block_min_ms": 1609459200000,
  "block_min_iso": "2021-01-01T00:00:00.000Z",
  "block_max_ms": 1609545600000,
  "block_max_iso": "2021-01-02T00:00:00.000Z",
  "file_offset_bytes": 1234,
  "block_len_bytes": 5678
}
```

- Index cadence: one entry every `index.max_records` or when `Δt` ≥ `index.max_hours` (both configurable).
- Rebuild: possible from the chunk if index is missing/corrupt (uses format plugin).

### 3.3 Partition Metadata (metadata.json)

Stable per-partition info and knobs (human-friendly JSON, durations are in appropriate units).

```json
{
  "partition_id": "<uuid>",
  "format_version": 1,
  "format_plugin": "my-format-plugin",
  "chunk_roll": { "max_bytes": 256000000, "max_hours": 100 },
  "index": { "max_records": 1440, "max_hours": 24 },
  "retention": { "max_days": 365 }
}
```

### 3.4 Manifest (manifest.jsonl)

Ordered list of live chunks for range routing ("chunks" may contain more entries).

File format is JSONL where each line represents one chunk (the examples below is broken into multiple lines for clarity).

For rolled chunks the manifest contains `max_ts_ms` and `max_ts` fields. For open chunks it does not.

Open chunk manifest entry:

```json
{ 
  "chunk_id": "<uuidv7>", 
  "min_ts_ms": 1609459200000,
  "min_ts": "2021-01-01T00:00:00.000Z"
}
```

Rolled chunk manifest entry:

```json
{ 
  "chunk_id": "<uuidv7>", 
  "min_ts_ms": 1609459200000,
  "min_ts": "2021-01-01T00:00:00.000Z",
  "max_ts_ms": 1609545600000,
  "max_ts": "2021-01-02T00:00:00.000Z"
}
```

Notes:

- File paths can be easily constructed from chunk id.
- Manifest is updated when a chunk is opened, rolled, or removed by retention.
- Manifest update is atomic (write new + fsync(file) + rename + fsync(dir)).

## 3.5 Format plugin

```rust
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct RecordMeta {
    pub offset: u64,       // start of record in the chunk file
    pub len: u32,          // exact byte length of the record
    pub ts_ms: i64,        // event timestamp extracted by the plugin
}

/// Read-only view into a chunk during maintenance operations.
/// Implementations may use internal buffering; they must be robust to partial tails.
pub trait ChunkScanner {
    /// Return the next complete record meta starting at or after the current position.
    /// Should gracefully return Ok(None) on clean EOF and ignore any trailing garbage/partial record at the tail.
    fn next(&mut self) -> io::Result<Option<RecordMeta>>;

    /// Optionally locate the last complete record quickly (used for tail recovery or dedup).
    /// Default impl may be a linear scan; optimized plugins can implement a backward scan.
    fn last(&mut self) -> io::Result<Option<RecordMeta>> {
        // default: scan to end, remember last
        let mut last = None;
        while let Some(m) = self.next()? { last = Some(m); }
        Ok(last)
    }
}

/// A format plugin is registered under a short, stable id (e.g., "jsonl", "protobuf", "postcard").
/// It is used ONLY for maintenance flows (index rebuild, tail recovery, optional dedup).
pub trait FormatPlugin: Send + Sync + 'static {
    /// Stable identifier to match `metadata.format_plugin`.
    fn id(&self) -> &'static str;

    /// Create a forward scanner over a chunk file. The store opens the file and passes a Read+Seek.
    /// Implementations must:
    ///  - Extract record boundaries and timestamps without panicking.
    ///  - Treat trailing partial bytes as corruption of the last record and stop before them.
    fn scanner<'a>(
        &self,
        file: &'a mut (dyn Read + Seek)
    ) -> io::Result<Box<dyn ChunkScanner + 'a>>;

    /// Optional fast tail recovery: return the byte length of the longest valid prefix.
    /// If provided, the store may truncate a torn tail to `valid_len` during recovery.
    fn valid_prefix_len(
        &self,
        file: &mut (dyn Read + Seek)
    ) -> io::Result<Option<u64>> { Ok(None) }

    /// Optional dedup comparator for last-record optimization on equal timestamps.
    /// Given two record byte slices (new append vs last record), return whether they are identical semantically.
    /// Default: false (no dedup).
    fn dedup_eq(&self, _a: &[u8], _b: &[u8]) -> bool { false }
}
```

## 4. Operations & Semantics

For each partition, the library maintains in-memory runtime data (`partition.runtime`).
This is used to route reads and avoids reading the manifest for every read.

```rust
struct PartitionRuntime {
    last_chunk_id: Option<Uuid>,
    last_chunk_min_ts_ms: i64,
    last_chunk_max_ts_ms: i64,
    last_chunk_size_bytes: u64,
    last_index_block_min_ts_ms: i64,
    last_index_block_max_ts_ms: i64,
    last_index_block_record_count: u64,
    last_index_block_size_bytes: u64,
    last_record_bytes: Option<Vec<u8>>
}
```

### 4.1 Append (Normal / In-Order)

Input: (partition_id, timestamp_ms, payload_bytes)

Route:

1. Acquire write lock (process level, per partition).
2. Get cached partition data (`partition.runtime`).
3. If there is no partition, create a new partition.
4. If there is no last chunk or roll condition met (size/time), create a new chunk (new UUIDv7).
5. Append to the chunk file; update in-memory partition cache data
6. Append index entries to the chunk’s index file if needed
7. Release write lock.

Durability: WAL is guaranteed by OpenRaft; at the store layer call fsync at policy checkpoints (configurable)
to bound data loss window if used standalone.

- When `timestamp_ms < partition.runtime.last_chunk_max_ts_ms` the library should reject the operation with an error. 
- No grace period is provided, appends must be in order.
- When `timestamp_ms == partition.runtime.last_chunk_max_ts_ms`:
  - if the record is the same (raw bytes) as the last record in the chunk (from the cache)
    - ignore the record and return with success status
  - otherwise, append the record to the chunk as per normal appending

### 4.2 Chunk Rolling

Roll when a record arrives that would cause the chunk to exceed the configured size or time AND
the record timestamp != `partition.runtime.last_chunk_max_ts_ms`. This ensures that records with the
same timestamp are always appended to the same chunk.

- `record byte count + cached.partition.last_chunk_size_bytes` ≥ `chunk_roll.max_bytes` OR
- `record timestamp - cached.partition.last_chunk_min_ts_ms` ≥ `chunk_roll.max_hours`

Rolling:

- closes the current chunk
- opens a new chunk (UUIDv7)
- updates the manifest

### 4.3 Retention & Deletion

- Time-based: drop rolled chunks where `max_ts_ms` < `(now - retention.max_days)`.
- Deletions are at chunk granularity.
- Deletion steps: move to /gc, then unlink asynchronously.

### 4.4 Process restart

- On restart, the library:
  - for each partition
    - reloads the manifest
    - loads the index of the last chunk
    - if the file size is larger than the last indexed block
      - scans the remaining bytes to find the last record
      - truncates the chunk file to the last valid record
    - Recover the metadata of the last index block from the open chunk using the format plugin
      - set `last_index_block_record_count` in the cache to the recovered record count 
      - set `last_index_block_size_bytes` in the cache to the recovered bytes 
    - Recover the bytes of the last record from the open chunk using the format plugin
      - set `last_record_bytes` in the cache to the recovered bytes 
    - adds the metadata to the cache

## 5. Indexing & Queries

**Ranges are closed.**

* Rolled chunk covers **[chunk_min_ms, chunk_max_ms]** (from the manifest).
* Open chunk covers **[chunk_min_ms, +∞)** (no max yet).

**Index blocks.**
Each index line describes a block:
`{ block_min_ms, block_max_ms, file_offset_bytes, block_len_bytes }` (sorted by time).

**read_range(partition, t_from, t_to)**

1. **Choose chunks**

  * **Rolled:** include if `chunk_min_ms ≤ t_to` **and** `chunk_max_ms ≥ t_from`.
  * **Open:** include if `chunk_min_ms ≤ t_to`.

2. **Whole-chunk fast path (rolled only)**

  * If `chunk_min_ms ≥ t_from` **and** `chunk_max_ms ≤ t_to`, return the entire file as one range:
    `(offset = 0, len = file_size_bytes)`.
    Skip the index for this chunk.

3. **Block selection (otherwise)**

  * Load the chunk’s index.
  * Find the **first** block with `block_max_ms ≥ t_from`.
  * Return that block and every following block **until** a block has `block_min_ms > t_to`.
  * For each returned block, emit the byte range `(file_offset_bytes, block_len_bytes)` as-is. The client filters individual records.

4. **Boundary rules**

  * Records with timestamp equal to `t_from` or `t_to` are included (closed interval).
  * Adjacent chunks must not share a boundary timestamp; rolling ensures `next.chunk_min_ms > prev.chunk_max_ms`.

5. **Closeness**

  * Only the **first** and **last** returned blocks can include bytes outside `[t_from, t_to]`.
  * All blocks in between lie fully inside the requested range.

## 6. Concurrency, Atomicity & Recovery

### 6.1 Concurrency

- Per-partition locking for writers; concurrent readers allowed.

- Lock file path: "<store_root>/.timevault.write.lock"
- When opening store in write mode:
  1. open(O_RDWR | O_CREAT | O_CLOEXEC) the lock file.
  2. Try non-blocking exclusive lock.
    - Success → proceed; keep the FD open for the lifetime of Store.
    - Failure → return AlreadyOpen error.
  3. Overwrite the file contents with `pid=<PID>`
  4. fsync(file) once after writing the identity info (not required for locking; useful if you want the file to show up 
     immediately on crash inspection).
- When opening store in read-only mode: do not acquire any lock (reads are allowed concurrently with a writer).
- When closing: drop the Store, which closes the FD → OS releases the lock.

### 6.2 Atomic File Ops

- Chunks and index files are append-only.
- Chunks and index file creation fsyncs directory.
- Manifests/metadata use write-then-rename with durable directory fsync.

### 6.3 Crash Recovery

- Primary: OpenRaft WAL replays write to restore append intent.
- Secondary (local):
  - Validate manifest-chunk consistency on open.
  - If index missing/corrupt → rebuild from chunk.

## 7. Compression & Integrity

- There is no compression.

## 8. Configuration (per-partition defaults; global overrides allowed)

- `chunk_roll.max_bytes` (e.g., 256 MB)
- `chunk_roll.max_hours` (e.g., 24 h)
- `index.max_records` (e.g., every 128 records)
- `index.max_hours` (e.g., 24 hours)
- `fsync_policy`: `none|interval(ms)|on_roll|on_manifest_update`
- `retention.max_days`

## 9. Observability & Admin

Metrics (per partition & global):

- Appends/sec, bytes/sec
- Current chunk size, age
- Index hit ratio (seek distance vs actual)
- Open file descriptors per partition
- Retention deletions

Health checks:
- Manifest ↔ files parity
- Index rebuild-needed flag
- Disk space threshold alarms

Introspection APIs:

- Dump manifest
- Rebuild index for chunk
- Force roll
- Dry-run retention

## 10. Library Surface (Rust) — High-Level

```rust
struct Store { /* ... */ }

impl Store {
    fn open(root: &Path, cfg: StoreConfig) -> Result<Store> {}
    fn open_partition(&self, partition: Uuid) -> Result<PartitionHandle> {}
    fn list_partitions(&self) -> Result<Vec<Uuid>> {}
}

struct PartitionHandle { /** ... */ }

impl PartitionHandle {
    fn append(&self, ts_ms: i64, payload: &[u8]) -> Result<AppendAck> {}
    fn read_range(&self, from_ms: i64, to_ms: i64) -> Result<Vec<u8>> {}
    fn force_roll(&self) -> Result<()> {}
    fn stats(&self) -> PartitionStats {}
    fn set_config(&self, delta: PartitionConfigDelta) -> Result<()> {}
}
```

## 11. Correctness Invariants

1. Monotonicity within a chunk: records in a chunk are non-decreasing by timestamp.
2. Chunk coverage: Manifest’s chunk list is strictly ordered and non-overlapping in time.
3. Append-only: No in-place mutations to chunk files.
4. Durable manifests: A manifest rename is the sole source of truth for routing reads.

## 12. Performance Notes & Limits (Initial Targets)

- Write path (in-order): sequential disk IO; single fsync per roll/manifest flush (policy-dependent).
- Chunk size: aim for 16–128 MB.
- Index density: tune to keep index size ~0.5–1.5% of chunk size.

## 13. Code structure

```text
timevault/
  src/
    lib.rs
    store/
      mod.rs
      open.rs
      paths.rs
      locks.rs
      fsync.rs
    partition/
      mod.rs
      append.rs
      read.rs
      roll.rs
      retention.rs
      recovery.rs
    disk/
      manifest.rs
      index.rs
      chunk.rs
      atomic.rs
    plugins/
      mod.rs
      jsonl.rs        // reference FormatPlugin
    admin/
      stats.rs
      health.rs
      introspect.rs
    errors.rs
    types.rs
    config.rs
```

Dependencies:

- uuid (v7)
- serde
- serde_json
- thiserror
- fs2 (file locks)
- parking_lot (fast Mutex/RwLock)
- chrono
- tempfile