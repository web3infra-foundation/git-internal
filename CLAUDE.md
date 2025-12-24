# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Is

git-internal is a high-performance Rust library for encoding/decoding Git internal objects and Pack files. It supports large monorepo-scale repositories with delta compression, multi-pack indexing, streaming I/O, and both sync/async APIs.

## Build & Test Commands

```bash
# Build
cargo build
cargo build --release

# Test
cargo test
cargo test <test_name>           # Run specific test
cargo test -- --nocapture        # Show output

# Lint & Format
cargo fmt                        # Format code
cargo clippy                     # Lint (treat warnings as errors for new code)

# Check all targets compile
cargo build --all-targets
```

## Architecture Overview

```
protocol/* (smart/http/ssh)
        ⇅ pkt-line & pack encode/decode
internal/pack (encode/decode/waitlist/cache/idx)
        ⇅ consumes/produces Entry+Meta
        ⇅ internal/object/index/metadata
        ⇅ delta / zstdelta / diff

hash.rs / utils.rs / errors.rs  (shared infrastructure)
```

**Core hub**: `internal/pack` - decodes/encodes packs, manages cache/waitlist/idx, exchanges data with protocol layer and object/delta modules.

**Protocol layer**: `protocol/*` - drives info-refs/upload-pack/receive-pack via pkt-line, uses app-provided `RepositoryAccess` and `AuthenticationService` traits.

**Object model**: `internal/object` - Blob/Tree/Commit/Tag/Note parsing/serialization with `ObjectTrait`.

**Delta/compression**: `delta/` and `zstdelta/` - delta encoding/decoding, zstd dictionary compression.

## Key Data Flows

**Pack Decode**: `Pack::decode(reader, callback)` or `Pack::decode_stream(stream, sender)` for async
- Validates PACK header → loops objects → inflates zlib → resolves delta chains via waitlist → emits `MetaAttached<Entry, EntryMeta>`

**Pack Encode**: `PackEncoder::encode()` or `encode_and_output_to_files()`
- Accepts Entry+Meta → optional delta compression within window → zlib compress → async write pack/idx → rename by hash

**Protocol**: `SmartProtocol` handles Git smart protocol
- upload-pack: parse want/have → `PackGenerator` builds pack stream
- receive-pack: parse commands → decode pack → store via `RepositoryAccess`

## Coding Conventions

- **Language**: Rust Edition 2024, async/await with tokio, tracing for observability
- **Errors**: `thiserror` for library errors, `anyhow` for binaries/tests
- **Style**: rustfmt defaults, clippy warnings as errors for new code
- **Safety**: Avoid `unwrap()`/`expect()` in library code; return `Result<_, _>`
- **Performance**: Use iterators, streaming I/O, bounded allocations in hot paths
- **FFI/unsafe**: Only when required, with `// SAFETY:` comment and tests

## Hash Algorithm

Supports both SHA-1 and SHA-256. Configure via `set_hash_kind(HashKind::Sha1)` at startup. Thread-local setting - set once per application context.

```rust
use git_internal::hash::{set_hash_kind, HashKind};
set_hash_kind(HashKind::Sha1);  // or HashKind::Sha256
```

## Concurrency Model

- **ThreadPool**: parallel inflate and delta rebuild during pack decode
- **Tokio**: streaming decode (`decode_stream`), async file writes
- **DashMap**: lock-free waitlist for delta dependencies
- **Rayon**: parallel delta application
- **Cache**: LRU memory + disk spill, 80% of `mem_limit` for object cache

## Key Types to Know

- `Pack` - main pack decoder/encoder entry point
- `Entry` / `EntryMeta` - decoded object with metadata (offset, CRC, path)
- `ObjectHash` - SHA-1 or SHA-256 object identifier
- `ObjectType` - Blob/Tree/Commit/Tag enum
- `RepositoryAccess` - trait for storage backend integration
- `GitProtocol` / `SmartProtocol` - protocol handling traits

## Test Data

Real pack files in `tests/data/packs/` (e.g., `small-sha1.pack`). Use for decode/encode roundtrip testing.
