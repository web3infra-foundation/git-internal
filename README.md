## Git Internal Module

Git-Internal is a high-performance Rust library for encoding and decoding Git internal objects and Pack files. It provides comprehensive support for Git's internal object storage format with advanced features like delta compression, memory management, and concurrent processing.

## Overview

This module is designed to handle Git internal objects and Pack files efficiently, supporting both reading and writing operations with optimized memory usage and multi-threaded processing capabilities. The library implements the complete Git Pack format specification with additional optimizations for large-scale Git operations.

## Quickstart

Decode a pack (offline):

```rust
use std::{fs::File, io::BufReader};
use git_internal::internal::pack::Pack;
use git_internal::hash::{set_hash_kind, HashKind};
fn main() -> Result<(), Box<dyn std::error::Error>> {
   // set once at startup, this is usually configured in the upper-level repository, not set directly; it's only shown here for demonstration purposes.
    set_hash_kind(HashKind::Sha1);
    let f = File::open("tests/data/packs/small-sha1.pack")?;
    let mut reader = BufReader::new(f);
    let mut pack = Pack::new(None, Some(64 * 1024 * 1024), None, true);

    pack.decode(&mut reader, |_entry| {
        // Process each decoded object here (MetaAttached<Entry, EntryMeta>).
        // For example, index it, persist it, or feed it into your build pipeline.
    }, None::<fn(git_internal::hash::ObjectHash)>)?;
    Ok(())
}
```

## Modules at a glance

- `hash.rs`: object IDs and hash algorithm selection (thread-local), set once by your app.
- `internal/object` / `internal/index` / `internal/metadata`: object parse/serialize, .git/index IO, path/offset/CRC metadata.
- `delta` / `zstdelta` / `diff.rs`: delta compression, zstd dictionary delta, line-level diff.
- `internal/pack`: pack decode/encode, waitlist, cache, idx building.
- `protocol/*`: smart protocol + HTTP/SSH adapters, wrapping info-refs/upload-pack/receive-pack.
- Docs: [docs/ARCHITECTURE.md (architecture)](docs/ARCHITECTURE.md), [docs/GIT_OBJECTS.md (objects)](docs/GIT_OBJECTS.md), [docs/GIT_PROTOCOL_GUIDE.md (protocol)](docs/GIT_PROTOCOL_GUIDE.md).

## Key Features

### 1. Multi-threaded Processing

- Configurable thread pool for parallel object processing
- Concurrent delta resolution with dependency management
- Asynchronous I/O operations for improved performance

### 2. Advanced Memory Management

- LRU-based memory cache with configurable limits
- Automatic disk spillover for large objects
- Memory usage tracking and optimization
- Heap size calculation for accurate memory accounting

### 3. Delta Compression Support

- Offset Delta : References objects by pack file offset
- Hash Delta : References objects by SHA-1 hash
- Zstd Delta : Enhanced compression using Zstandard algorithm
- Intelligent delta chain resolution

### 4. Streaming Support

- Stream-based pack file processing
- Memory-efficient handling of large pack files
- Support for network streams and file streams

## Core Algorithms

### Pack Decoding Algorithm

1. Read and validate pack header (PACK signature, version, object count)
2. For each object in the pack:
   a. Parse object header (type, size)
   b. Handle based on object type:
      - Base objects: Decompress and store directly
      - Delta objects: Add to waitlist until base is available
   c. Resolve delta chains when base objects become available
3. Verify pack checksum

### Delta Resolution Strategy

- Waitlist Management : Delta objects wait for their base objects
- Dependency Tracking : Maintains offset and hash-based dependency maps
- Chain Resolution : Recursively applies delta operations
- Memory Optimization : Calculates expanded object sizes to prevent OOM

### Cache Management

- Two-tier Caching : Memory cache with disk spillover
- LRU Eviction : Least recently used objects are evicted first
- Size-based Limits : Configurable memory limits with accurate tracking
- Async Persistence : Background threads handle disk operations

### Object Processing Pipeline

```
Input Stream → Header Parsing → Object Decoding → Delta Resolution → Cache Storage → Output
                     ↓              ↓              ↓              ↓
                Validation    Decompression   Waitlist Mgmt   Memory Mgmt
```

## Performance Optimizations

### Memory Allocator Recommendations

> [!TIP]
> Here are some performance tips that you can use to significantly improve performance when using `git-internal` crates as a dependency.

In certain versions of Rust, using `HashMap` on Windows can lead to performance issues. This is due to the allocation strategy of the internal heap memory allocator. To mitigate these performance issues on Windows, you can use [mimalloc](https://github.com/microsoft/mimalloc). (See [this issue](https://github.com/rust-lang/rust/issues/121747) for more details.)

On other platforms, you can also experiment with [jemalloc](https://github.com/jemalloc/jemalloc) or [mimalloc](https://github.com/microsoft/mimalloc) to potentially improve performance.

A simple approach:

1. Change Cargo.toml to use mimalloc on Windows and jemalloc on other platforms.

   ```toml
   [target.'cfg(not(windows))'.dependencies]
   jemallocator = "0.5.4"

   [target.'cfg(windows)'.dependencies]
   mimalloc = "0.1.43"
   ```

2. Add `#[global_allocator]` to the main.rs file of the program to specify the allocator.

   ```rust
   #[cfg(not(target_os = "windows"))]
   #[global_allocator]
   static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

   #[cfg(target_os = "windows")]
   #[global_allocator]
   static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
   ```

### Concurrent Processing

- Configurable thread pools for CPU-intensive operations
- Lock-free data structures where possible (DashMap for waitlists)
- Parallel delta application using Rayon

### 3. I/O Optimization

- Buffered reading with configurable buffer sizes
- Asynchronous file operations for cache persistence
- Stream-based processing to minimize memory footprint

### Benchmark

**TODO**
