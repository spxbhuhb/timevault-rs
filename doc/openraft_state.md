# Timevault store in OpenRaft states

## 1. Purpose & Scope

Guidelines and procedures for using a timevault store as part of an OpenRaft **state**:

* Ensures identical chunk IDs and contents across replicas.
* Defines live operation rules (append, truncate, purge, configuration changes).
* Defines snapshot format and installation.

### 1.1 Terminology

* state snapshot: Data that represents the state of the application as per OpenRaft.
* store snapshot: Part of state snapshot, represents the state of a timevault store. Contains a list of partition snapshots.
* partition snapshot: Part of store snapshot, the minimized state of one partition.
* snapshot installation: An OpenRaft-initiated process that aims to bring the state of the application to a well known point defined by a given state snapshot.

---

## 2. Live Operation Rules

1. **Appends**

   * All appends to partitions must be executed through OpenRaft.

2. **Truncate & Purge**

   * Must be initiated and committed through OpenRaft.

3. **Configuration Changes**

   * Any change to per-partition configuration (e.g., `chunk_roll`, `index`, `retention`, `key_is_timestamp`, `logical_purge`) must be performed through OpenRaft.

4. **Consistency**

   * Divergence in chunk IDs or chunk contents across replicas is a design error. It cannot be fixed automatically.

---

## 3. Snapshot Model

A state snapshot contains a **store snapshot** which in turn contains a list of **partition snapshots**. Large data files (chunks) are not embedded; only a compact metadata set is included.

### 3.1 Partition Snapshot Contents

* `partition_metadata`
* `last_chunk_id`
* `last_chunk_size`
* `last_order_key`

Partition metadata is stored verbatim to preserve the exact configuration active at snapshot time.

---

## 4. Snapshot Installation

Snapshot installation brings the local store into alignment with a given state snapshot. The procedure runs **per partition** and can also create partitions if they do not exist locally.

### 4.1 Partition Creation (Optional)

* If the partition is missing locally, create it using the `partition_metadata` from the snapshot:

   * Create partition directory structure.
   * Persist `metadata.json` exactly as provided.
   * Initialize an empty `manifest.jsonl`.

### 4.2 Manifest Rebase

1. Download the manifest from the leader.
2. Drop every entry after `last_chunk_id` from the snapshot.
3. If the last kept manifest entry is closed, remove its `max_order_key` to make it open.
4. Persist the updated manifest durably.

### 4.3 Chunk & Index Synchronization (Closed Chunks)

* Download and persist any **closed chunks** and their **index files** referenced by the rebased manifest that are missing or outdated locally.

### 4.4 Open Chunk Preparation

* Download the final (open) chunk referenced by the manifest **up to** `last_chunk_size`.
* Download the **index** of the final chunk **up to** the block that precedes `last_chunk_size`.
* Ensure a writable open chunk exists locally with the correct chunk id and current size `last_chunk_size`.

### 4.5 Verification

* Verify that chunk IDs are strictly increasing and that no overlapping coverage exists across manifest entries.
* Confirm index coverage aligns with file extents for all synchronized chunks.

### 4.6 Transfer Integrity & Retries

All transfers must be **resumable**, **atomic on completion**, and **validated** before use.

* **Temporary files & atomic rename**

   * Download into `tmp/` with a deterministic name.
   * `fsync` file, then `rename` into place and `fsync` parent directory.

* **Resumable downloads**

   * Support byte-range requests. If a temp file exists, resume from its current size.
   * On retry, request `[current_len .. remote_len)`.

* **Closed chunks & indices**

   * Validate final byte length equals the leader’s reported length.
   * Parse the index to ensure it is well-formed and offsets lie within the chunk length.

* **Open chunk**

   * Target length is `last_chunk_size` from the snapshot.
   * If local length > `last_chunk_size`, **truncate** to `last_chunk_size`.
   * If local length < `last_chunk_size`, resume download until lengths match.
   * For the open chunk’s index, ensure the last parsed block end offset `≤ last_chunk_size`.

* **Retry policy**

   * Use bounded retries with backoff (e.g., 3 attempts). On repeated mismatch or parse error, abort snapshot installation for the partition.

* **Idempotence**

   * Re-running the step may reuse partially downloaded temp files and will yield the same final on-disk state upon success.

---

## 5. Invariants and Determinism (State Context)

* **Monotonic chunk IDs per partition.**
* **Non-overlapping manifest coverage.**
* **Equal-key grouping** within a chunk; rolling only occurs between different order keys.
* **Append-only files**; no in-place mutation.
* **Durable manifests** via atomic write-then-rename.

These invariants must hold identically across all replicas at all times.

---

## 6. Operational Notes

* **Retention**: Execute retention only via OpenRaft-coordinated changes to avoid diverging data sets.
* **Recovery**: On process restart, rely on the store’s recovery to revalidate manifest↔file parity and rebuild missing indexes before resuming participation.
* **Observability**: Track per-partition metrics (appends/sec, bytes/sec, current chunk size/age) and health (manifest↔files parity, space thresholds).

---

## 7. API Integration Points (State)

* **Data Transfer API**: The store must provide endpoints or functions that allow:

   * Downloading a partition manifest.
   * Downloading chunk files up to a specified size (supports byte-range and resume; returns size/version metadata).
   * Downloading corresponding index files (supports byte-range and resume; returns size/version metadata).
* **Snapshot API:** The store must provide functions that can be integrated into an application dependent `RaftSnapshotBuilder` and `RaftStateMachine` (as per OpenRaft APIs).
   * `get_snapshot_builder`
   * `build_snapshot`
   * `begin_receiving_snapshot`
   * `install_snapshot`
* **Mutation Hooks**: Route append/purge/truncate/config updates through OpenRaft; the store is mutated only via replicated operations.

---

## 8. Edge Cases

* **Empty Partitions**: A partition may have no chunks; snapshot records metadata with `last_chunk_id = None`.
* **Single Oversized Record**: If the open chunk is empty and an oversized record is needed, a single-record chunk may be created; subsequent rolling rules still apply.
* **Idempotence**: Running snapshot installation steps multiple times with the same inputs yields the same on-disk state.

---

## 9. Summary

Use OpenRaft as the single authority for all state mutations and configuration changes. Keep replica manifests and chunk contents identical by installing snapshots via **Snapshot Installation**. Treat any divergence as a fatal design error requiring operator intervention.
