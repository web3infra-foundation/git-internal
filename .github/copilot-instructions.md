# Git-Internal Repository Custom Instructions for GitHub Copilot

## What this repository is

This repository (git-internal) houses an advanced, internal rewrite / extension of the Git object model, transport, and packfile layer — intended to support very large repositories (monorepo scale), content-addressed storage, delta chains, multi‐pack indexing, and integration with next-generation build systems (for example Bazel, Buck2). The target language is primarily Rust; the goal is high-performance Git internals re-architected for modern workflows.

## Languages & defaults

- Prefer Rust (Edition 2021+). Use async/await, tokio, tracing for async/observability.
- Serialization: serde; errors: thiserror for libraries, anyhow for binaries/tests.
- FFI or unsafe only when absolutely required — include // SAFETY: rationale and tests.
- Scripting utilities (build/test) may use Python or Bash, but core logic should be Rust.

## Build & run

- Use cargo for local iteration (cargo build, cargo test, cargo bench) but the canonical build is through the monorepo tooling (Buck2/Bazel) when applicable.
- For CI: ensure “clean workspace” builds succeed, e.g., buck2 build //git-internal/... or similar.
- Testing incremental git pack/objects: provide reproducible scripts in scripts/ and reference them in CI workflows.

## Workspace layout & major components (mental map)

- git-internal/ root holds multiple crates:
  - engine/ – core Git object engine (object lookup, packfile, loose objects)
  - transport/ – network/transport layer (Git clone/fetch/push)
  - delta/ – delta chain rewrite, multi-pack index support
  - fs/ – filesystem overlay for large repos (e.g., using FUSE)
  - tools/ – CLI utilities, benchmarks, interactive tools
- Root Cargo.toml defines workspace; shared common/ crate for cross-cutting utilities.
- Config/tests/benchmarks at tests/ and benches/.

## Coding style & quality

- Run rustfmt with defaults; new code should compile without warnings under cargo build --all-targets.
- Treat clippy warnings as errors for new/changed code (#![deny(warnings)] on new crates).
- Avoid unwrap()/expect() in library code; prefer returning Result<_, _>.
- Use iterators, slices, streaming I/O, bounded allocations — especially in hot paths.
- For performance-critical code, add criterion benches and document expected throughput/alloc.

## Performance & memory considerations

- Designed for very large repos: focus on streaming, O(n) algorithms, minimal copying.
- Packfile handling: consider fan-out tables, delta chain depth, SHA-1 vs SHA-256 object IDs.
- Object layout: support migration from SHA-1 → SHA-256; avoid hard-coding SHA-1 assumptions.
- Bench and regression test for pack size, object count, clone time under large scale.

## Git compatibility & hashing

- Must interoperate with Git on disk and on network: support both SHA-1 and SHA-256 object IDs.
- On examples or docs, always mention dual-stack context (SHA-1 legacy vs new SHA-256).

## Testing


## Provide clear invariants

- Content-addressed: objects are identified by their content hash (SHA-1 or SHA-256).
- Idempotent: same input always produces same output.
- Backward/forward compatibility: old clients can fetch new packs, new clients can fetch old packs. 

## Testing

- Include unit tests, integration tests, property tests (proptest) for object graph correctness.
- Snapshot tests (insta) for textual/log output when appropriate.
- Concurrency tests for transport and fs layers (using tokio::test).
- Include fuzz tests where feasible for pack extraction/rewriting.

## Observability & errors

- Use tracing spans and structured fields; avoid logging sensitive data.
- Use anyhow::Context or similar for rich error messages; prefer actionable errors.
- Benchmarks should emit meaningful diagnostics (alloc counts, time, RAM) not just “works”.

## Documentation

- Public APIs must have /// docs; complex subsystems must have module-level //! docs.
- Provide architectural overview docs (flowcharts, sequence diagrams) for pack rewriting, multi-pack-index, transport.
- Prefer English for broad audience; Chinese comments allowed for internal/internationalization notes.

## Git workflow & PRs

- Use Trunk-Based Development; commit messages follow Conventional Commits (feat: …, fix: …, perf: …).
- In PRs: include description of problem, design chosen, benchmarks (before/after) if relevant.
- Use CHANGELOG.md in crates where public APIs change (semver rules apply).

## How Copilot should assist

- When the user asks for code: emit Rust code first, then if needed integration snippet (bench, test, CI).
- When the user asks for design options: list trade-offs (performance, memory, compatibility, complexity).
- Prefer minimal, composable abstractions; avoid large global abstractions without modularity.
- Always mention relevant Git-internal context when generating suggestions (object ID, packfile, fan-out, delta chain, clone time).

## Non-goals

- Do not design a new VCS from scratch (unless explicitly requested).
- Do not propose to abandon Git backwards compatibility without explicit scope.
- Do not rewrite core build system (Buck2/Bazel) itself — focus remains on Git internals.