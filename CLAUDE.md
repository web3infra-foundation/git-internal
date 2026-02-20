# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Is

git-internal is a high-performance Rust library for encoding/decoding Git internal objects, Pack files, and AI-assisted development objects. It supports large monorepo-scale repositories with delta compression, multi-pack indexing, streaming I/O, and both sync/async APIs. Beyond the standard Git object model (Blob, Tree, Commit, Tag), it provides a structured AI object model (Intent, Plan, Task, Run, PatchSet, Evidence, Decision, etc.) that captures the full lifecycle of AI-driven code changes.

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
cargo +nightly fmt               # Format code (requires nightly)
cargo +nightly fmt --check       # Check formatting without modifying
cargo clippy                     # Lint (treat warnings as errors for new code)

# Check all targets compile
cargo build --all-targets
```

## Git Commands

```bash
git commit -a -s -S -m"" # Commit 
git push --force
```

## Architecture Overview

```
protocol/* (smart/http/ssh)
        ⇅ pkt-line & pack encode/decode
internal/pack (encode/decode/waitlist/cache/idx)
        ⇅ consumes/produces Entry+Meta
        ⇅ internal/object/index/metadata
        ⇅ delta / zstdelta / diff

internal/object
  ├── Standard: blob, tree, commit, tag, note
  ├── AI objects: intent, plan, task, run, patchset,
  │   evidence, decision, provenance, tool, context, pipeline
  └── Shared: types (Header, ActorRef, ObjectType), integrity, signature

hash.rs / utils.rs / errors.rs  (shared infrastructure)
```

**Core hub**: `internal/pack` - decodes/encodes packs, manages cache/waitlist/idx, exchanges data with protocol layer and object/delta modules.

**Protocol layer**: `protocol/*` - drives info-refs/upload-pack/receive-pack via pkt-line, uses app-provided `RepositoryAccess` and `AuthenticationService` traits.

**Object model**: `internal/object` - standard Git objects (Blob/Tree/Commit/Tag/Note) and AI objects, all implementing `ObjectTrait` for unified serialization.

**Delta/compression**: `delta/` and `zstdelta/` - delta encoding/decoding, zstd dictionary compression.

## AI Object Model

The AI object model lives in `src/internal/object/` alongside standard Git objects. All AI objects implement `ObjectTrait`, share a common `Header` (UUID v7, timestamps, creator `ActorRef`), and are serialized as JSON.

### End-to-End Flow

```
 ①  User input
      ▼
 ②  Intent (Draft → Active → Completed)
      ▼
 ③  Plan (steps + ContextPipeline)
      ▼
 ④  Task (constraints + acceptance criteria)
      ▼
 ⑤  Run (baseline commit + environment)
      ├── Provenance (LLM config, 1:1)
      ├── ContextSnapshot (static context, optional)
      ├── ⑥ ToolInvocation (action log, 1:N)
      ├── ⑦ PatchSet (candidate diff)
      ├── ⑧ Evidence (test/lint/build, 1:N)
      ▼
 ⑨  Decision (commit / retry / abandon / rollback)
      ▼
 ⑩  Intent (Completed, commit recorded)
```

### AI Object Files

| File | Object(s) | Role |
|------|-----------|------|
| `intent.rs` | `Intent`, `IntentStatus` | User prompt + AI interpretation; workflow entry/exit |
| `plan.rs` | `Plan`, `PlanStep`, `StepStatus` | Ordered steps from an Intent; revision chain via `previous` |
| `task.rs` | `Task`, `TaskStatus`, `GoalType` | Unit of work with constraints and acceptance criteria |
| `run.rs` | `Run`, `RunStatus`, `Environment` | Single execution attempt; accumulates artifacts |
| `tool.rs` | `ToolInvocation`, `IoFootprint` | Per-tool-call action log with file I/O tracking |
| `patchset.rs` | `PatchSet`, `PatchSetStatus` | Candidate unified diff with touched-file summary |
| `evidence.rs` | `Evidence`, `EvidenceKind` | Validation output (test, lint, build) |
| `decision.rs` | `Decision`, `Verdict` | Terminal verdict on a Run |
| `provenance.rs` | `Provenance`, `TokenUsage` | LLM model config and token metrics |
| `context.rs` | `ContextSnapshot`, `ContextItem` | Static file/URL/snippet capture at Run start |
| `pipeline.rs` | `ContextPipeline`, `ContextFrame` | Dynamic sliding-window context during planning |

### Shared Types (`types.rs`)

- `Header` — common header for all AI objects (UUID v7 `object_id`, `object_type`, `created_at`, `updated_at`, `created_by`)
- `ActorRef` — actor identity with kind (`agent`, `human`, `system`, `tool`) and name
- `ArtifactRef` — reference to an external artifact (kind + locator)
- `ObjectType` — enum covering both standard Git types and AI types
- `IntegrityHash` — SHA-256 content hash for commit references in AI objects (in `integrity.rs`)

### Key Patterns

- **Append-only history**: `Intent.statuses`, `PlanStep.statuses`, `Task.runs`, `Run.patchsets` — append-only vectors that preserve full history.
- **Snapshot references**: `Run.plan` records the Plan version at execution time and never changes; `Intent.plan` always points to the latest revision.
- **Revision chains**: `Plan.previous` links to the prior Plan version, forming an immutable chain.
- **Recursive decomposition**: `PlanStep.task` can reference a sub-Task with its own Run/Plan lifecycle; `Task.parent` provides the reverse link.
- **Context separation**: `ContextSnapshot` (static, at Run start) vs `ContextPipeline` (dynamic, accumulated during planning with frame eviction).
- **Serde conventions**: `#[serde(default)]` + `skip_serializing_if` on optional/empty fields; `rename_all = "snake_case"` on enums; `#[serde(alias = "...")]` for backward-compatible renames.

### Documentation

Full AI object lifecycle, field-level docs, and usage examples: `docs/ai.md`.

## Key Data Flows

**Pack Decode**: `Pack::decode(reader, callback)` or `Pack::decode_stream(stream, sender)` for async
- Validates PACK header → loops objects → inflates zlib → resolves delta chains via waitlist → emits `MetaAttached<Entry, EntryMeta>`

**Pack Encode**: `PackEncoder::encode()` or `encode_and_output_to_files()`
- Accepts Entry+Meta → optional delta compression within window → zlib compress → async write pack/idx → rename by hash

**Protocol**: `SmartProtocol` handles Git smart protocol
- upload-pack: parse want/have → `PackGenerator` builds pack stream
- receive-pack: parse commands → decode pack → store via `RepositoryAccess`

**AI Object Persistence**: AI objects are stored as content-addressed JSON blobs in the Git object database using their own `ObjectType` discriminator. They are excluded from pack encode/decode paths (rejected at the pack layer boundary).

## Coding Conventions

- **Language**: Rust Edition 2024, async/await with tokio, tracing for observability
- **Errors**: `thiserror` for library errors, `anyhow` for binaries/tests
- **Style**: rustfmt defaults (nightly), clippy warnings as errors for new code
- **Safety**: Avoid `unwrap()`/`expect()` in library code; return `Result<_, _>`
- **Performance**: Use iterators, streaming I/O, bounded allocations in hot paths
- **FFI/unsafe**: Only when required, with `// SAFETY:` comment and tests
- **AI objects**: JSON serialization via serde; `ObjectTrait` implementation with `from_bytes`/`to_data`/`get_type`/`get_size`; doc comments follow the pattern: module-level Position in Lifecycle diagram, Relationships table, Purpose section, field-level docs

## Hash Algorithm

Supports both SHA-1 and SHA-256. Configure via `set_hash_kind(HashKind::Sha1)` at startup. Thread-local setting - set once per application context.

```rust
use git_internal::hash::{set_hash_kind, HashKind};
set_hash_kind(HashKind::Sha1);  // or HashKind::Sha256
```

AI objects use `IntegrityHash` (always SHA-256) for commit references, independent of the repository's hash algorithm.

## Concurrency Model

- **ThreadPool**: parallel inflate and delta rebuild during pack decode
- **Tokio**: streaming decode (`decode_stream`), async file writes
- **DashMap**: lock-free waitlist for delta dependencies
- **Rayon**: parallel delta application
- **Cache**: LRU memory + disk spill, 80% of `mem_limit` for object cache

## Key Types to Know

**Standard Git**:
- `Pack` - main pack decoder/encoder entry point
- `Entry` / `EntryMeta` - decoded object with metadata (offset, CRC, path)
- `ObjectHash` - SHA-1 or SHA-256 object identifier
- `ObjectType` - Blob/Tree/Commit/Tag + AI type variants
- `RepositoryAccess` - trait for storage backend integration
- `GitProtocol` / `SmartProtocol` - protocol handling traits

**AI Objects**:
- `Intent` - workflow entry point; user prompt + AI interpretation
- `Plan` / `PlanStep` - planning artifact with ordered steps
- `Task` - stable work identity with acceptance criteria
- `Run` - execution attempt; records baseline commit and environment
- `PatchSet` - candidate diff artifact
- `Evidence` - validation result (test/lint/build)
- `Decision` - terminal verdict (Commit/Retry/Abandon/Rollback)
- `Provenance` - LLM configuration and token usage
- `ContextSnapshot` / `ContextPipeline` - static and dynamic context
- `ToolInvocation` - per-tool-call action log
- `Header` / `ActorRef` - shared metadata types

## Test Data

Real pack files in `tests/data/packs/` (e.g., `small-sha1.pack`). Use for decode/encode roundtrip testing. AI object unit tests are inline in each module file.

## Documentation

- `docs/ARCHITECTURE.md` - overall library architecture
- `docs/GIT_OBJECTS.md` - standard Git object format reference
- `docs/GIT_PROTOCOL_GUIDE.md` - Git smart protocol guide
- `docs/ai.md` - AI object model: lifecycle, fields, and usage examples
