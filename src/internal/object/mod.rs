//! Object model definitions for Git blobs, trees, commits, tags, and
//! supporting traits that let the pack/zlib layers create strongly typed
//! values from raw bytes.
//!
//! AI objects are also defined here, as they are a fundamental part of
//! the system and need to be accessible across multiple modules without
//! circular dependencies.
//!
//! # AI Object End-to-End Flow
//!
//! ```text
//!  ①  User input
//!       │
//!       ▼
//!  ②  Intent (Draft → Active)
//!       │
//!       ├──▶ ContextPipeline  ← seeded with IntentAnalysis frame
//!       │
//!       ▼
//!  ③  Plan (pipeline, fwindow, steps)
//!       │
//!       ├─ PlanStep₀ (inline)
//!       ├─ PlanStep₁ ──task──▶ sub-Task (recursive)
//!       └─ PlanStep₂ (inline)
//!       │
//!       ▼
//!  ④  Task (Draft → Running)
//!       │
//!       ▼
//!  ⑤  Run (Created → Patching → Validating → Completed/Failed)
//!       │
//!       ├──▶ Provenance (1:1, LLM config + token usage)
//!       ├──▶ ContextSnapshot (optional, static context at start)
//!       │
//!       │  ┌─── agent execution loop ───┐
//!       │  │                            │
//!       │  │  ⑥ ToolInvocation (1:N)    │  ← action log
//!       │  │       │                    │
//!       │  │       ▼                    │
//!       │  │  ⑦ PatchSet (Proposed)     │  ← candidate diff
//!       │  │       │                    │
//!       │  │       ▼                    │
//!       │  │  ⑧ Evidence (1:N)          │  ← test/lint/build
//!       │  │       │                    │
//!       │  │       ├─ pass ──────────────┘
//!       │  │       └─ fail → new PatchSet (retry within Run)
//!       │  └─────────────────────────────┘
//!       │
//!       ▼
//!  ⑨  Decision (terminal verdict)
//!       │
//!       ├─ Commit    → apply PatchSet, record result_commit
//!       ├─ Retry     → create new Run ⑤ for same Task
//!       ├─ Abandon   → mark Task as Failed
//!       ├─ Checkpoint → save state, resume later
//!       └─ Rollback  → revert applied PatchSet
//!       │
//!       ▼
//!  ⑩  Intent (Completed) ← commit recorded
//! ```
//!
//! ## Steps
//!
//! 1. **User input** — the user provides a natural-language request.
//!
//! 2. **[`Intent`](intent::Intent)** — captures the raw prompt and the
//!    AI's structured interpretation. Status transitions from `Draft`
//!    (prompt only) to `Active` (analysis complete). Supports
//!    conversational refinement via `parent` chain.
//!
//! 3. **[`Plan`](plan::Plan)** — a sequence of
//!    [`PlanStep`](plan::PlanStep)s derived from the Intent. References
//!    a [`ContextPipeline`](pipeline::ContextPipeline) and records the
//!    visible frame range (`fwindow`). Steps track consumed/produced
//!    frames by stable ID (`iframes`/`oframes`). A step may spawn a sub-Task for
//!    recursive decomposition. Plans form a revision chain via
//!    `previous`.
//!
//! 4. **[`Task`](task::Task)** — a unit of work with title, constraints,
//!    and acceptance criteria. May link back to its originating Intent.
//!    Accumulates Runs in `runs` (chronological execution history).
//!
//! 5. **[`Run`](run::Run)** — a single execution attempt of a Task.
//!    Records the baseline `commit`, the Plan version being executed
//!    (snapshot reference), and the host `environment`. A
//!    [`Provenance`](provenance::Provenance) (1:1) captures the LLM
//!    configuration and token usage.
//!
//! 6. **[`ToolInvocation`](tool::ToolInvocation)** — the finest-grained
//!    record: one per tool call (read file, run command, etc.). Forms
//!    a chronological action log for the Run. Tracks file I/O via
//!    `io_footprint`.
//!
//! 7. **[`PatchSet`](patchset::PatchSet)** — a candidate diff generated
//!    by the agent. Contains the diff `artifact`, file-level `touched`
//!    summary, and `rationale`. Starts as `Proposed`; transitions to
//!    `Applied` or `Rejected`. Ordering is by position in
//!    `Run.patchsets`.
//!
//! 8. **[`Evidence`](evidence::Evidence)** — output of a validation tool
//!    (test, lint, build) run against a PatchSet. One per tool
//!    invocation. Carries `exit_code`, `summary`, and
//!    `report_artifacts`. Feeds into the Decision.
//!
//! 9. **[`Decision`](decision::Decision)** — the terminal verdict of a
//!    Run. Selects a PatchSet to apply (`Commit`), retries the Task
//!    (`Retry`), gives up (`Abandon`), saves progress (`Checkpoint`),
//!    or reverts (`Rollback`). Records `rationale` and
//!    `result_commit_sha`.
//!
//! 10. **Intent completed** — the orchestrator records the final git
//!     commit in `Intent.commit` and transitions status to `Completed`.
//!
//! ## Object Relationship Summary
//!
//! | From | Field | To | Cardinality |
//! |------|-------|----|-------------|
//! | Intent | `parent` | Intent | 0..1 |
//! | Intent | `plan` | Plan | 0..1 |
//! | Plan | `previous` | Plan | 0..1 |
//! | Plan | `pipeline` | ContextPipeline | 0..1 |
//! | PlanStep | `task` | Task | 0..1 |
//! | Task | `parent` | Task | 0..1 |
//! | Task | `intent` | Intent | 0..1 |
//! | Task | `runs` | Run | 0..N |
//! | Task | `dependencies` | Task | 0..N |
//! | Run | `task` | Task | 1 |
//! | Run | `plan` | Plan | 0..1 |
//! | Run | `snapshot` | ContextSnapshot | 0..1 |
//! | Run | `patchsets` | PatchSet | 0..N |
//! | PatchSet | `run` | Run | 1 |
//! | Evidence | `run_id` | Run | 1 |
//! | Evidence | `patchset_id` | PatchSet | 0..1 |
//! | Decision | `run_id` | Run | 1 |
//! | Decision | `chosen_patchset_id` | PatchSet | 0..1 |
//! | Provenance | `run_id` | Run | 1 |
//! | ToolInvocation | `run_id` | Run | 1 |
//!
pub mod blob;
pub mod commit;
pub mod context;
pub mod decision;
pub mod evidence;
pub mod integrity;
pub mod intent;
pub mod note;
pub mod patchset;
pub mod pipeline;
pub mod plan;
pub mod provenance;
pub mod run;
pub mod signature;
pub mod tag;
pub mod task;
pub mod tool;
pub mod tree;
pub mod types;
pub mod utils;

use std::{
    fmt::Display,
    io::{BufRead, Read},
};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{object::types::ObjectType, zlib::stream::inflate::ReadBoxed},
};

/// **The Object Trait**
/// Defines the common interface for all Git object types, including blobs, trees, commits, and tags.
pub trait ObjectTrait: Send + Sync + Display {
    /// Creates a new object from a byte slice.
    fn from_bytes(data: &[u8], hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized;

    /// Generate a new Object from a `ReadBoxed<BufRead>`.
    /// the input size,is only for new a vec with directive space allocation
    /// the input data stream and output object should be plain base object .
    fn from_buf_read<R: BufRead>(read: &mut ReadBoxed<R>, size: usize) -> Self
    where
        Self: Sized,
    {
        let mut content: Vec<u8> = Vec::with_capacity(size);
        read.read_to_end(&mut content).unwrap();
        let digest = read.hash.clone().finalize();
        let hash = ObjectHash::from_bytes(&digest).unwrap();
        Self::from_bytes(&content, hash).unwrap()
    }

    /// Returns the type of the object.
    fn get_type(&self) -> ObjectType;

    fn get_size(&self) -> usize;

    fn to_data(&self) -> Result<Vec<u8>, GitError>;

    /// Computes the object hash from serialized data.
    ///
    /// Default implementation serializes the object and computes the hash from that data.
    /// Override only if you need custom hash computation or caching.
    fn object_hash(&self) -> Result<ObjectHash, GitError> {
        let data = self.to_data()?;
        Ok(ObjectHash::from_type_and_data(self.get_type(), &data))
    }
}
