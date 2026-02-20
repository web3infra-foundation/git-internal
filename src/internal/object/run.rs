//! AI Run Definition
//!
//! A [`Run`] is a single execution attempt of a
//! [`Task`](super::task::Task). It captures the execution context
//! (baseline commit, environment, Plan version) and accumulates
//! artifacts ([`PatchSet`](super::patchset::PatchSet)s,
//! [`Evidence`](super::evidence::Evidence),
//! [`ToolInvocation`](super::tool::ToolInvocation)s) during execution.
//! The Run is step ⑤ in the end-to-end flow described in
//! [`mod.rs`](super).
//!
//! # Position in Lifecycle
//!
//! ```text
//!  ④  Task ──runs──▶ [Run₀, Run₁, ...]
//!                        │
//!                        ▼
//!  ⑤  Run (Created → Patching → Validating → Completed/Failed)
//!       │
//!       ├──task──▶ Task          (mandatory, 1:1)
//!       ├──plan──▶ Plan          (snapshot reference)
//!       ├──snapshot──▶ ContextSnapshot  (optional)
//!       │
//!       │  ┌─── agent execution loop ───┐
//!       │  │                            │
//!       │  │  ⑥ ToolInvocation (1:N)    │
//!       │  │       │                    │
//!       │  │       ▼                    │
//!       │  │  ⑦ PatchSet (Proposed)     │
//!       │  │       │                    │
//!       │  │       ▼                    │
//!       │  │  ⑧ Evidence (1:N)          │
//!       │  │       │                    │
//!       │  │       ├─ pass ─────────────┘
//!       │  │       └─ fail → new PatchSet
//!       │  └────────────────────────────┘
//!       │
//!       ▼
//!  ⑨  Decision (terminal verdict)
//! ```
//!
//! # Status Transitions
//!
//! ```text
//! Created ──▶ Patching ──▶ Validating ──▶ Completed
//!                │              │
//!                └──────────────┴──▶ Failed
//! ```
//!
//! # Relationships
//!
//! | Field | Target | Cardinality | Notes |
//! |-------|--------|-------------|-------|
//! | `task` | Task | 1 | Mandatory owning Task |
//! | `plan` | Plan | 0..1 | Snapshot reference (frozen at Run start) |
//! | `snapshot` | ContextSnapshot | 0..1 | Static context at Run start |
//! | `patchsets` | PatchSet | 0..N | Candidate diffs, chronological |
//!
//! Reverse references (by `run_id`):
//! - `Provenance.run_id` → this Run (1:1, LLM config)
//! - `ToolInvocation.run_id` → this Run (1:N, action log)
//! - `Evidence.run_id` → this Run (1:N, validation results)
//! - `Decision.run_id` → this Run (1:1, terminal verdict)
//!
//! # Purpose
//!
//! - **Execution Context**: Records the baseline `commit`, host
//!   `environment`, and Plan version so that results can be
//!   reproduced.
//! - **Artifact Collection**: Accumulates PatchSets (candidate diffs)
//!   during the agent execution loop.
//! - **Isolation**: Each Run is independent — a retry creates a new
//!   Run with potentially different parameters, without mutating the
//!   previous Run's state.

use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        integrity::IntegrityHash,
        types::{ActorRef, Header, ObjectType},
    },
};

/// Lifecycle status of a [`Run`].
///
/// See module docs for the status transition diagram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Run has been created but the agent has not started execution.
    /// Environment and baseline commit are captured at this point.
    Created,
    /// Agent is actively generating code changes. One or more
    /// [`ToolInvocation`](super::tool::ToolInvocation)s are being
    /// produced.
    Patching,
    /// Agent has produced a candidate
    /// [`PatchSet`](super::patchset::PatchSet) and is running
    /// validation tools (tests, lint, build). One or more
    /// [`Evidence`](super::evidence::Evidence) objects are being
    /// produced.
    Validating,
    /// Agent has finished successfully. A
    /// [`Decision`](super::decision::Decision) has been created.
    Completed,
    /// Agent encountered an unrecoverable error. `Run.error` should
    /// contain the error message.
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Created => "created",
            RunStatus::Patching => "patching",
            RunStatus::Validating => "validating",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        }
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Host environment snapshot captured at Run creation time.
///
/// Records the OS, CPU architecture, and working directory so that
/// results can be correlated with the execution environment. The
/// `extra` map allows capturing additional environment details
/// (e.g. tool versions, environment variables) without schema changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    /// Operating system identifier (e.g. "macos", "linux", "windows").
    pub os: String,
    /// CPU architecture (e.g. "aarch64", "x86_64").
    pub arch: String,
    /// Current working directory at Run creation time.
    pub cwd: String,
    /// Additional environment details (tool versions, etc.).
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Environment {
    /// Create a new environment object from the current system environment
    pub fn capture() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cwd: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|e| {
                    tracing::warn!("Failed to get current directory: {}", e);
                    "unknown".to_string()
                }),
            extra: HashMap::new(),
        }
    }
}

/// A single execution attempt of a [`Task`](super::task::Task).
///
/// A Run captures the execution context and accumulates artifacts
/// during the agent's work. It is step ⑤ in the end-to-end flow.
/// See module documentation for lifecycle, relationships, and
/// status transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Task`](super::task::Task) this Run belongs to.
    ///
    /// Mandatory — every Run is an execution attempt of exactly one
    /// Task. `Task.runs` holds the reverse reference. This field is
    /// set at creation and never changes.
    task: Uuid,
    /// The [`Plan`](super::plan::Plan) this Run is executing.
    ///
    /// This is a **snapshot reference**: it records the specific Plan
    /// version that was active when this Run started. After
    /// replanning, existing Runs keep their original `plan` unchanged
    /// — only new Runs reference the revised Plan.
    /// `Intent.plan` always points to the latest revision, but a Run
    /// may be executing an older version. `None` when no Plan was
    /// associated (e.g. ad-hoc execution without formal planning).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan: Option<Uuid>,
    /// Git commit hash of the working tree when this Run started.
    ///
    /// Serves as the baseline for all code changes: the agent reads
    /// files at this commit, and the resulting
    /// [`PatchSet`](super::patchset::PatchSet) diffs are relative to
    /// it. If the Run fails and a new Run is created, the new Run
    /// may start from a different commit (e.g. after upstream changes
    /// are pulled).
    commit: IntegrityHash,
    /// Current lifecycle status.
    ///
    /// Transitions follow the sequence:
    /// `Created → Patching → Validating → Completed` (happy path),
    /// or `→ Failed` from any active state. The orchestrator advances
    /// the status as the agent progresses through execution phases.
    status: RunStatus,
    /// Optional [`ContextSnapshot`](super::context::ContextSnapshot)
    /// captured at Run creation time.
    ///
    /// Records the file tree, documentation fragments, and other
    /// static context the agent observed when the Run began. Used
    /// for reproducibility: given the same snapshot and Plan, the
    /// agent should produce equivalent results. `None` when no
    /// snapshot was captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot: Option<Uuid>,
    /// Chronological list of [`PatchSet`](super::patchset::PatchSet)
    /// IDs generated during this Run.
    ///
    /// Append-only — each new PatchSet is pushed to the end. The
    /// last entry is the most recent candidate. A Run may produce
    /// multiple PatchSets when the agent iterates on validation
    /// failures (step ⑦ → ⑧ retry loop). Empty when no PatchSet
    /// has been generated yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    patchsets: Vec<Uuid>,
    /// Execution metrics (token usage, timing, etc.).
    ///
    /// Free-form JSON for metrics not captured by
    /// [`Provenance`](super::provenance::Provenance). For example,
    /// wall-clock duration, number of tool calls, or retry count.
    /// `None` when no metrics are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metrics: Option<serde_json::Value>,
    /// Error message if the Run failed.
    ///
    /// Set when `status` transitions to `Failed`. Contains a
    /// human-readable description of what went wrong. `None` while
    /// the Run is in progress or completed successfully.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Host [`Environment`] snapshot captured at Run creation time.
    ///
    /// Automatically populated by [`Run::new`] via
    /// [`Environment::capture`]. Records OS, architecture, and
    /// working directory for reproducibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    environment: Option<Environment>,
}

impl Run {
    /// Create a new Run.
    ///
    /// # Arguments
    /// * `created_by` - Actor (usually the Orchestrator)
    /// * `task` - The Task this run belongs to
    /// * `commit` - The Git commit hash of the checkout
    pub fn new(created_by: ActorRef, task: Uuid, commit: impl AsRef<str>) -> Result<Self, String> {
        let commit = commit.as_ref().parse()?;
        Ok(Self {
            header: Header::new(ObjectType::Run, created_by)?,
            task,
            plan: None,
            commit,
            status: RunStatus::Created,
            snapshot: None,
            patchsets: Vec::new(),
            metrics: None,
            error: None,
            environment: Some(Environment::capture()),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn task(&self) -> Uuid {
        self.task
    }

    /// Returns the Plan this Run is executing, if set.
    pub fn plan(&self) -> Option<Uuid> {
        self.plan
    }

    /// Sets the Plan this Run will execute.
    pub fn set_plan(&mut self, plan: Option<Uuid>) {
        self.plan = plan;
    }

    pub fn commit(&self) -> &IntegrityHash {
        &self.commit
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    pub fn snapshot(&self) -> Option<Uuid> {
        self.snapshot
    }

    /// Returns the chronological list of PatchSet IDs generated during this Run.
    pub fn patchsets(&self) -> &[Uuid] {
        &self.patchsets
    }

    pub fn metrics(&self) -> Option<&serde_json::Value> {
        self.metrics.as_ref()
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn environment(&self) -> Option<&Environment> {
        self.environment.as_ref()
    }

    pub fn set_status(&mut self, status: RunStatus) {
        self.status = status;
    }

    pub fn set_snapshot(&mut self, snapshot: Option<Uuid>) {
        self.snapshot = snapshot;
    }

    /// Appends a PatchSet ID to this Run's generation history.
    pub fn add_patchset(&mut self, patchset_id: Uuid) {
        self.patchsets.push(patchset_id);
    }

    pub fn set_metrics(&mut self, metrics: Option<serde_json::Value>) {
        self.metrics = metrics;
    }

    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }
}

impl fmt::Display for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Run: {}", self.header.object_id())
    }
}

impl ObjectTrait for Run {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Run
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Run size: {}", e);
                0
            }
        }
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_hash_hex() -> String {
        IntegrityHash::compute(b"ai-process-test").to_hex()
    }

    #[test]
    fn test_new_objects_creation() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let base_hash = test_hash_hex();

        // Run with environment (auto captured)
        let run = Run::new(actor.clone(), Uuid::from_u128(0x1), &base_hash).expect("run");

        let env = run.environment().unwrap();
        // Check if it captured real values (assuming we are running on some OS)
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
        assert!(!env.cwd.is_empty());
    }
}
