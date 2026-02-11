//! Git-Internal: a high-performance Rust library for Git objects and pack files—encode/decode, delta (ref/offset/zstd),
//! caching, streaming, and sync/async pipelines.
//!
//! Goals
//! - Provide high-performance parsing and generation of Git pack format and Git objects.
//! - Support both file-based and streaming inputs of Git Pack.
//! - Offer synchronous and asynchronous APIs for multi-threaded and Tokio runtimes.
//!
//! Core Capabilities
//! - Decoding: base objects and delta objects (Ref-Delta, Offset-Delta, ZstdDelta).
//! - Encoding: serial and parallel pipelines; offset deltas and Zstd-based deltas.
//! - Streaming: `decode_stream` for `Stream<Bytes>`; `decode_async` decodes in a new thread and sends entries.
//! - Caching & memory: LRU-based cache; `MemSizeRecorder` tracks heap usage; optional `mem_limit` to bound memory.
//! - Utilities: Hash-stream helpers, zlib, delta, zstdelta toolkits.
//!
//! Modules
//! - `internal::pack`: decode/encode, caches, waitlists, parallel pipelines, helpers.
//! - `internal::object`: Blob/Tree/Commit/Tag/Note objects, type enum, object trait.
//! - `internal::zlib`: compression/decompression stream utilities.
//! - `delta` and `zstdelta`: delta algorithms and rebuild helpers.
//! - `errors`: unified error types.
//! - `hash`: Hash helpers.
//! - `utils`: common utilities (e.g., `CountingReader`).
//!
//! Typical Usage
//! - Offline large-file decode: `Pack::decode_async(reader, sender)` decodes in a thread and sends `Entry`s.
//! - Stream decode: `Pack::decode_stream(stream, sender)` consumes `ReaderStream` under Tokio.
//! - Parallel encode: `encode_async` / `parallel_encode` build packs from many objects.
//!
//! Test Data
//! - Located under `tests/data/`, includes real pack files and object sets.

mod delta;
pub mod diff;
pub mod errors;
pub mod hash;
pub mod internal;
pub mod protocol;
pub mod utils;
mod zstdelta;

// Core traits and types that external users need to implement/use
pub use diff::{Diff, DiffItem};
pub use protocol::{
    AuthenticationService, GitProtocol, ProtocolError, RepositoryAccess, ServiceType,
};
