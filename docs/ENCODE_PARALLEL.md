# Pack Encode Parallel Computation Architecture

## Overview

The pack encoder turns a stream of Git objects (blobs, trees, commits, tags) into a valid
[Git pack file][pack-format] with optional delta compression. All CPU-intensive work —
zlib compression, delta generation, similarity heuristics — is offloaded from the async
Tokio runtime to dedicated thread pools so that I/O and other async tasks stay responsive.

[pack-format]: https://github.com/git/git/blob/master/Documentation/technical/pack-format.adoc

This document describes the parallel computation strategy used at each stage, the
rationale for using Rayon (rather than manual `std::thread` pools), and how to configure
thread counts.

## Two Encoding Paths

The encoder selects its parallel strategy based on `window_size`:

| `window_size` | Path | Parallel strategy | File |
|--------------|------|-------------------|------|
| `0` | No-delta (independent) | Rayon `par_iter` over object batches | `parallel.rs` |
| `> 0` | Delta (sliding-window) | Rayon `into_par_iter` over work items | `mod.rs` → `inner_encode` |

Both paths share the same wire-format encoding helpers (`header.rs`), entry ordering
(`sort.rs`), and output adapters (`output.rs`).

### Router (`mod.rs:145–165`)

```rust
pub async fn encode(&mut self, entry_rx: Receiver<...>) -> Result<(), GitError> {
    if self.window_size == 0 {
        self.parallel_encode(entry_rx).await   // no-delta
    } else {
        self.inner_encode(entry_rx, ...).await  // delta
    }
}
```

---

## Path 1: Independent Encoding (`window_size == 0`)

**File:** `src/internal/pack/encode/parallel.rs`

When delta compression is disabled, every object is encoded independently.
This is an *embarrassingly parallel* workload — each object's header construction
and zlib compression share no state with any other object.

### Algorithm

1. **Batch read.** Objects arrive via an async `mpsc::Receiver`. They are accumulated into
   bounded batches (`batch_size = max(1000, channel_capacity / 10)`) to balance memory usage
   against parallelism.

2. **Parallel compress.** Each batch is handed to Rayon's `par_iter`:

   ```rust
   let batch_result: Vec<Result<(Vec<u8>, IndexEntry), GitError>> = batch_entries
       .par_iter()
       .map(|entry| encode_one_object(entry, None).map(|e| (e, IndexEntry::new(entry, 0))))
       .collect();
   ```

   - Every Rayon worker thread picks entries from the batch independently.
   - `par_iter` preserves input order in the output `Vec`, so pack layout is
     deterministic regardless of which worker finishes first.
   - No shared mutable state — each `encode_one_object` call is a pure function of
     its `Entry`.

3. **Serial write.** Encoded chunks are written in batch order so that `inner_offset`
   (the running pack byte position) and `inner_hash` (the running SHA-1/SHA-256) stay
   correct. Index records (`IndexEntry`) are collected for later `.idx` file generation.

4. **Checksum trailer.** After all batches are processed, the final pack checksum is
   appended and the output channel is closed.

### Parallelism Notes

- Rayon's global thread pool provides work-stealing across batches.
- No `tokio::spawn_blocking` wrapper is needed because the unit of work
  (`encode_one_object`) is short-lived (zlib of a single object). Rayon's
  `par_iter().collect()` blocks the calling thread, but `parallel_encode` is
  an `async fn` that may run on a Tokio worker. In practice the caller wraps
  encoding in `tokio::spawn` (via `encode_async`) or runs it from
  `encode_and_output_to_files` which is called from a dedicated task.

---

## Path 2: Delta Encoding (`window_size > 0`)

**Files:**
- Orchestration: `src/internal/pack/encode/mod.rs` (`inner_encode`)
- Sliding-window search: `src/internal/pack/encode/delta_search.rs` (`try_as_offset_delta`)
- Entry ordering: `src/internal/pack/encode/sort.rs` (`magic_sort`, similarity helpers)

When delta compression is enabled, the encoder groups related objects and, for each
target, searches a backwards sliding window for a suitable base. Delta generation is
CPU-intensive and involves mutable state (the window), so parallelism is applied
*across independent buckets*, not within a single bucket's window.

### Phase 1: Partition by Type (`mod.rs:206–235`)

Entries are drained from the input channel and partitioned into four `Vec`s:

| Type | Typical count (relative) | Delta window behavior |
|------|--------------------------|-----------------------|
| Commit | Small | One work item |
| Tree | Medium | One work item |
| Blob | **Dominant** (>90%) | Split into *N* contiguous chunks |
| Tag | Tiny | One work item |

Delta bases must have the same Git object type, so separating types makes that
invariant explicit. Unsupported types (AI objects, ref-deltas) are rejected.

### Phase 2: Sort for Locality (`mod.rs:233–244`)

Each type's vector is sorted with `magic_sort` (`sort.rs`):

1. **Path-aware entries first**, grouped by parent directory and Git's `pack_name_hash`.
2. **Within a directory**, descending by payload size — larger objects first so they
   can serve as bases for smaller ones.
3. **Pointer tie-breaker** for deterministic ordering.

The goal is to maximize the chance that a delta base and its target are neighbors in the
sorted list. When they are, the sliding window (size 10) will contain the base when the
target is processed.

### Phase 3: Build Parallel Work Items (`mod.rs:258–304`)

Sorted entries are packaged into a flat `Vec<WorkItem>`:

```rust
struct WorkItem {
    order: usize,       // position in final pack output
    entries: Vec<Entry>, // contiguous, sorted, same-type entries
}
```

| Work item | `order` | Contents |
|-----------|---------|----------|
| `work_items[0]` | 0 | All commits (one chunk) |
| `work_items[1]` | 1 | All trees (one chunk) |
| `work_items[2..2+N]` | 2..2+N | Blob chunks (N chunks) |
| `work_items[2+N]` | 2+N | All tags (one chunk) |

**Blob chunking.** Blobs dominate pack size, so they are split into *contiguous*
chunks. Contiguity is essential — splitting arbitrarily would destroy the
`magic_sort` locality that makes delta search effective.

Chunk count heuristic (`mod.rs:274–282`):

```rust
let num_threads = rayon::current_num_threads();
let chunks_per_thread = 20;
let blob_chunk_count = if num_threads > 1 && total_blob_entries > (num_threads * 20) {
    num_threads * chunks_per_thread  // e.g. 10 threads × 20 = 200 chunks
} else {
    1  // too few entries to benefit from splitting
};
```

The 20× multiplier creates far more chunks than workers. Rayon's work-stealing
scheduler then distributes them across available threads, automatically balancing
load when some chunks contain larger objects than others.

### Phase 4: Parallel Delta Search (`mod.rs:313–356`)

All work items are dispatched to Rayon in a single `into_par_iter()` call,
wrapped in one `tokio::task::spawn_blocking`:

```rust
let run_delta_search = move || -> Vec<ChunkResult> {
    work_items
        .into_par_iter()
        .map(|item| {
            (item.order, Self::try_as_offset_delta(item.entries, 10, ez, er, dp))
        })
        .collect()
};

let chunk_results = tokio::task::spawn_blocking(run_delta_search).await?;
```

**Why one `spawn_blocking` task instead of many?**

| Before (removed) | After |
|------------------|-------|
| 3 × `spawn_blocking` (commit, tree, tag) | 1 × `spawn_blocking` (all types) |
| N × `std::thread::spawn` (blob workers) | Rayon work-stealing (within the task) |
| `Arc<Mutex<Vec<BlobChunk>>>` shared queue | No shared mutable state |
| `Arc<Mutex<Vec<ChunkResult>>>` shared results | `par_iter().collect()` returns ordered `Vec` |

The single `spawn_blocking` moves all CPU work off the async runtime. Inside,
Rayon's global pool (with work-stealing) distributes the individual work items
across cores. This avoids:

- Mixing Tokio's blocking pool with manually-spawned OS threads.
- Manual queue management and mutex contention.
- Thread-join error handling boilerplate.

**`PACK_THREADS` support.** When the `PACK_THREADS` environment variable is set,
a dedicated `rayon::ThreadPool` is built with the requested thread count and the
delta search runs on that pool instead of the global one:

```rust
if let Some(n) = std::env::var("PACK_THREADS").ok().and_then(|s| s.parse().ok()) {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(n).build()?;
    tokio::task::spawn_blocking(move || pool.install(run_delta_search)).await?
} else {
    tokio::task::spawn_blocking(run_delta_search).await?
}
```

When `PACK_THREADS` is not set, the global Rayon pool is used, which can be
controlled with the standard `RAYON_NUM_THREADS` environment variable.

### Phase 5: Ordered Assembly (`mod.rs:358–386`)

Rayon workers may finish in any order. The `order` field in each result restores
the deterministic pack layout:

```rust
chunk_results.sort_by_key(|(order, _)| *order);   // commits → trees → blobs → tags

let mut all_res = Vec::new();
for (_order, res) in chunk_results {
    all_res.push(res?);  // propagate encoding errors
}
```

Encoded chunks are then written serially — `write_all_and_update` updates both
`inner_offset` (absolute pack position) and `inner_hash` (running checksum).
OFS_DELTA offsets computed in Phase 4 are *bucket-local*; the absolute pack
offset is assigned during this serial write step, and the relative distance
between an entry and its base remains valid regardless of where the bucket is
placed.

### Phase 6: Trailer & Cleanup (`mod.rs:380–386`)

The final pack checksum (SHA-1 or SHA-256) is computed from the running hash,
appended to the output stream, and the pack sender channel is dropped to signal
end-of-stream.

---

## Internal Parallelism: The Sliding Window (`delta_search.rs`)

Within a single work item (one call to `try_as_offset_delta`), entries are
processed **sequentially**. Each target entry needs to observe the window state
produced by all previous entries in the same bucket.

However, the **candidate evaluation** step within the window uses additional
parallelism depending on the delta engine:

### Rabin Path (`#[cfg(feature = "diff_rabin")]` — default)

Candidates are filtered sequentially but scored with lazy Rabin indexing:

1. Pre-filter candidates (type match, chain depth, size ratio, `multi_point_similar`).
2. Build a Rabin delta index once per candidate while it remains in the window.
3. Score survivors by actual encoded delta size.
4. Select the most profitable base (must save ≥ 50% of target size).

The Rabin path stores an `Arc<[u8]>` and a `RabinDeltaIndex` on each
`DeltaWindowEntry`, amortizing index construction across multiple targets
that may use the same base.

### Myers / Patience Path (`#[cfg(not(feature = "diff_rabin"))]`)

Candidates are evaluated **in parallel** within the window using Rayon:

```rust
let candidates: Vec<_> = window
    .par_iter()
    .with_min_len(3)
    .filter_map(|try_base| {
        // similarity check + heuristic_encode_rate_parallel(...)
    })
    .collect();
```

`with_min_len(3)` ensures each Rayon worker gets at least 3 candidates
to amortize scheduling overhead. The `heuristic_encode_rate_parallel`
function (`src/delta/mod.rs`) splits buffers into stepped chunks and
counts matching chunks in parallel, adapting the step size to the
input length.

---

## Thread Pool Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Async Layer (Tokio)                         │
│                                                                  │
│  ┌──────────┐   ┌──────────────┐   ┌──────────────────────────┐ │
│  │ I/O task │   │ encode_async │   │ encode_and_output_to_files│ │
│  │ (drain   │   │ (tokio::spawn)│   │ (output.rs)              │ │
│  │  channel)│   └──────┬───────┘   └──────────┬───────────────┘ │
│  └──────────┘          │                       │                 │
│                        │ await                 │ await           │
│                        ▼                       ▼                 │
│               ┌────────────────────────────────┐                │
│               │         encode()               │                │
│               │  (window_size == 0 ?           │                │
│               │   parallel_encode :            │                │
│               │   inner_encode)                │                │
│               └───────────┬────────────────────┘                │
│                           │                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │ spawn_blocking
┌───────────────────────────┼─────────────────────────────────────┐
│                Blocking Layer (Rayon)                            │
│                           ▼                                      │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              Rayon Global Thread Pool                       │ │
│  │                                                            │ │
│  │  Worker 0  Worker 1  Worker 2  ...  Worker N-1             │ │
│  │     │         │         │              │                   │ │
│  │     └─────────┼─────────┼──────────────┘                   │ │
│  │               │         │  (work-stealing)                  │ │
│  │               ▼         ▼                                   │ │
│  │  ┌──────────────────────────────────────┐                  │ │
│  │  │  into_par_iter() over WorkItem[]     │                  │ │
│  │  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐│                  │ │
│  │  │  │commit│ │ tree │ │blob 0│ │blob N││ ...              │ │
│  │  │  │ (1)  │ │ (1)  │ │(200) │ │(200) ││                  │ │
│  │  │  └──────┘ └──────┘ └──────┘ └──────┘│                  │ │
│  │  └──────────────────────────────────────┘                  │ │
│  │                                                            │ │
│  │  Each work item → try_as_offset_delta()                    │ │
│  │    └→ sliding-window (sequential within bucket)             │ │
│  │       └→ candidate scoring (Rabin: sequential,              │ │
│  │                              Myers: par_iter within window) │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Optional: PACK_THREADS=N → dedicated rayon::ThreadPool(N)       │
└──────────────────────────────────────────────────────────────────┘
```

### Thread Pool Separation Rationale

| Pool | Purpose | Why |
|------|---------|-----|
| **Tokio async runtime** | I/O, channel operations, `spawn_blocking` dispatch | Non-blocking async I/O |
| **Rayon global pool** | `par_iter` for independent encoding, work-stealing for delta search | CPU-bound work with automatic load balancing |
| **`tokio::spawn_blocking`** | Bridge: moves a Rayon `install` / `collect` call off the async executor | Prevents CPU work from starving async tasks |

---

## Configuration

### Thread Count

| Variable | Scope | Default |
|----------|-------|---------|
| `RAYON_NUM_THREADS` | All Rayon operations (global pool) | `available_parallelism()` |
| `PACK_THREADS` | Delta search only (dedicated pool) | Not set → uses global pool |

When `PACK_THREADS` is set, a one-shot `rayon::ThreadPool` is created for the
duration of `inner_encode`. This pool is independent of the global Rayon pool
and allows fine-grained control over delta-search parallelism without affecting
other Rayon users in the same process.

### Delta Window Size

The sliding window is currently fixed at **10** entries per bucket (the same
default used by C Git). The window size in `PackEncoder::new()` controls
whether delta encoding is enabled (`> 0`) or not (`0`), but the per-bucket
window depth is hard-coded in `try_as_offset_delta`.

### Minimum Delta Savings

A delta must save at least **50%** of the target payload to be selected
(`MIN_DELTA_RATE = 0.5` in `delta_search.rs`). This prevents the encoder
from paying the delta decode cost when the savings are marginal.

---

## Error Propagation

Parallel errors are collected and propagated through the `Result` chain:

1. **`try_as_offset_delta`** returns `Result<Vec<(Vec<u8>, IndexEntry)>, GitError>`.
   Errors from zstdelta, delta encoding, or wire-format encoding are captured here.

2. **Rayon `map`** preserves the `Result` — a failed work item produces
   `(order, Err(...))`, not a panic.

3. **`chunk_results` iteration** unwraps each `Result`, returning the first
   error to the caller.

4. **`spawn_blocking` panic** is caught by Tokio and converted to `GitError`
   via `.map_err()`.

---

## Summary: Why Rayon?

The encoder previously used a mixed approach:

- `tokio::spawn_blocking` for commits, trees, and tags (3 tasks).
- `std::thread::spawn` with an `Arc<Mutex<Vec<BlobChunk>>>` work queue for blobs.
- `rayon::par_iter` for independent encoding and non-Rabin candidate scoring.

This mixed three thread pools (Tokio blocking, manual OS threads, Rayon global)
and required ~50 lines of manual queue management. The unified Rayon approach:

1. **Eliminates manual thread management.** `into_par_iter()` replaces
   `Arc<Mutex<Vec>>` queues, manual `thread::spawn`, and `JoinHandle` loops.

2. **Provides work-stealing.** If one blob chunk contains 500 large objects
   and another contains 500 small ones, Rayon automatically redistributes
   remaining chunks from the idle worker to the busy one.

3. **Is consistent.** Both encoding paths now use Rayon as their sole
   CPU-parallelism mechanism.

4. **Preserves `PACK_THREADS`.** Users who need per-encoder thread control
   get a dedicated pool; everyone else gets the global pool controlled by
   `RAYON_NUM_THREADS`.

5. **Does not change compression.** The delta search algorithm
   (`try_as_offset_delta`) is unchanged. Only the *dispatch* mechanism
   changed — the same chunks, in the same order, produce the same deltas.
