//! AI Task Definition
//!
//! A [`Task`] is a unit of work to be performed by an AI agent. It is
//! step ④ in the end-to-end flow described in [`mod.rs`](super) — the
//! stable identity for a piece of work, independent of how many times
//! it is attempted (Runs) or how the strategy evolves (Plan revisions).
//!
//! # Position in Lifecycle
//!
//! ```text
//!  ③  Plan ──steps──▶ [PlanStep₀, PlanStep₁, ...]
//!                          │
//!                          ├─ inline (no task)
//!                          └─ task ──▶ ④ Task
//!                                        │
//!                                        ├──▶ Run₀ ──plan──▶ Plan_v1
//!                                        ├──▶ Run₁ ──plan──▶ Plan_v2
//!                                        │
//!                                        ▼
//!                                    ⑤  Run (execution)
//! ```
//!
//! # Status Transitions
//!
//! ```text
//! Draft ──▶ Running ──▶ Done
//!   │          │
//!   ├──────────┴──▶ Failed
//!   └──────────────▶ Cancelled
//! ```
//!
//! # Relationships
//!
//! | Field | Target | Cardinality | Notes |
//! |-------|--------|-------------|-------|
//! | `parent` | Task | 0..1 | Back-reference to parent Task for sub-Tasks |
//! | `intent` | Intent | 0..1 | Originating user request |
//! | `runs` | Run | 0..N | Chronological execution history |
//! | `dependencies` | Task | 0..N | Must complete before this Task starts |
//!
//! Reverse references:
//! - `PlanStep.task` → this Task (forward link from Plan)
//! - `Run.task` → this Task (each Run knows its owner)
//!
//! # Replanning
//!
//! When a Run fails or the agent determines the plan needs revision,
//! a new [`Plan`](super::plan::Plan) revision is created. The **Task
//! stays the same** — it is the stable identity for the work. Only
//! the strategy (Plan) evolves:
//!
//! ```text
//! Task (constant)                Intent (constant, plan updated)
//!   │                              └─ plan ──▶ Plan_v2 (latest)
//!   └─ runs:
//!        Run₀ ──plan──▶ Plan_v1   (snapshot: original plan)
//!        Run₁ ──plan──▶ Plan_v2   (snapshot: revised plan)
//! ```
//!
//! # Purpose
//!
//! - **Stable Identity**: The Task persists across retries and
//!   replanning. All Runs, regardless of which Plan version they
//!   executed, belong to the same Task.
//! - **Scope Definition**: `constraints` and `acceptance_criteria`
//!   define what the agent must and must not do, and how success is
//!   measured.
//! - **Hierarchy**: `parent` enables recursive decomposition — a
//!   PlanStep can spawn a sub-Task, which in turn has its own Plan
//!   and Runs.
//! - **Dependency Management**: `dependencies` enables ordering
//!   between sibling Tasks (e.g. "implement API before writing
//!   tests").

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        types::{ActorRef, Header, ObjectType},
    },
};

/// Lifecycle status of a [`Task`].
///
/// See module docs for the status transition diagram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Initial state. Task definition is in progress — title,
    /// constraints, and acceptance criteria may still be changing.
    Draft,
    /// An agent (via a [`Run`](super::run::Run)) is actively working
    /// on this Task. At least one Run in `Task.runs` is active.
    Running,
    /// Task completed successfully. All acceptance criteria met and
    /// the final PatchSet has been committed.
    Done,
    /// Task failed to complete after all retry attempts. The
    /// [`Decision`](super::decision::Decision) of the last Run
    /// explains the failure.
    Failed,
    /// Task was cancelled by the user or orchestrator before
    /// completion (e.g. timeout, budget exceeded, user interrupt).
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Draft => "draft",
            TaskStatus::Running => "running",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Classification of the work a [`Task`] aims to accomplish.
///
/// Helps agents choose appropriate strategies and tools. For example,
/// a `Bugfix` task might prioritize reading test output, while a
/// `Refactor` task might focus on code structure analysis.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GoalType {
    Feature,
    Bugfix,
    Refactor,
    Docs,
    Perf,
    Test,
    Chore,
    Build,
    Ci,
    Style,
    /// Catch-all for goal categories not covered by the predefined
    /// variants. The inner string is the custom category name.
    Other(String),
}

impl GoalType {
    pub fn as_str(&self) -> &str {
        match self {
            GoalType::Feature => "feature",
            GoalType::Bugfix => "bugfix",
            GoalType::Refactor => "refactor",
            GoalType::Docs => "docs",
            GoalType::Perf => "perf",
            GoalType::Test => "test",
            GoalType::Chore => "chore",
            GoalType::Build => "build",
            GoalType::Ci => "ci",
            GoalType::Style => "style",
            GoalType::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for GoalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for GoalType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "feature" => Ok(GoalType::Feature),
            "bugfix" => Ok(GoalType::Bugfix),
            "refactor" => Ok(GoalType::Refactor),
            "docs" => Ok(GoalType::Docs),
            "perf" => Ok(GoalType::Perf),
            "test" => Ok(GoalType::Test),
            "chore" => Ok(GoalType::Chore),
            "build" => Ok(GoalType::Build),
            "ci" => Ok(GoalType::Ci),
            "style" => Ok(GoalType::Style),
            _ => Ok(GoalType::Other(value.to_string())),
        }
    }
}

/// A unit of work with constraints and success criteria.
///
/// A Task can be **top-level** (created directly from a user request)
/// or a **sub-Task** (spawned by a [`PlanStep`](super::plan::PlanStep)
/// for recursive decomposition). It is step ④ in the end-to-end flow.
/// See module documentation for lifecycle, relationships, and
/// replanning semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// Short human-readable summary of the work to be done.
    ///
    /// Analogous to a git commit subject line or a Jira ticket title.
    /// Should be concise (under 100 characters) and describe the
    /// desired outcome, not the method. Set once at creation.
    title: String,
    /// Extended description providing additional context.
    ///
    /// May include background information, links to relevant docs or
    /// issues, and any details that don't fit in `title`. `None` when
    /// the title is self-explanatory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Classification of the work (Feature, Bugfix, Refactor, etc.).
    ///
    /// Helps agents choose appropriate strategies. For example, a
    /// `Bugfix` task might prioritize reading test output, while a
    /// `Docs` task focuses on documentation files. `None` when the
    /// category is unclear or not relevant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal: Option<GoalType>,
    /// Hard constraints the solution must satisfy.
    ///
    /// Each entry is a natural-language rule (e.g. "Must use JWT",
    /// "No breaking API changes", "Keep backward compatibility with
    /// v2"). The agent must verify all constraints are met before
    /// marking the Task as `Done`. Empty when there are no constraints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    constraints: Vec<String>,
    /// Criteria that must be met for the Task to be considered done.
    ///
    /// Each entry is a testable condition (e.g. "All tests pass",
    /// "Coverage >= 80%", "No clippy warnings"). The
    /// [`Evidence`](super::evidence::Evidence) produced during a Run
    /// should demonstrate that these criteria are satisfied. Empty
    /// when success is implied (e.g. "just do it").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    acceptance_criteria: Vec<String>,
    /// The actor who requested this work.
    ///
    /// May differ from `created_by` in the header when an agent
    /// creates a Task on behalf of a user. For example, the
    /// orchestrator (`created_by = system`) might create a Task
    /// requested by a human (`requester = human`). `None` when the
    /// requester is the same as the creator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requester: Option<ActorRef>,
    /// Parent Task that spawned this sub-Task.
    ///
    /// Provides O(1) reverse navigation from a sub-Task back to its
    /// parent. Set when a [`PlanStep`](super::plan::PlanStep) creates
    /// a sub-Task via `PlanStep.task`. `None` for top-level Tasks.
    ///
    /// The forward direction is `PlanStep.task → sub-Task`; this field
    /// is the corresponding back-reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Uuid>,
    /// Back-reference to the [`Intent`](super::intent::Intent) that
    /// motivated this Task.
    ///
    /// Provides O(1) reverse navigation from work unit to the
    /// originating user request:
    /// - **Top-level Task**: points to the root Intent.
    /// - **Sub-Task with own analysis**: points to a new sub-Intent.
    /// - **Sub-Task (pure delegation)**: `None` — context is already
    ///   captured in the parent PlanStep's `iframes`/`oframes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    intent: Option<Uuid>,
    /// Chronological list of [`Run`](super::run::Run) IDs that have
    /// executed (or are executing) this Task.
    ///
    /// Append-only — each new Run is pushed to the end. The last
    /// entry is the most recent attempt. A Task may have multiple
    /// Runs due to retries (after a `Decision::Retry`) or parallel
    /// execution experiments. Empty when no Run has been created yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    runs: Vec<Uuid>,
    /// Other Tasks that must complete before this one can start.
    ///
    /// Used for ordering sibling Tasks within a Plan (e.g. "implement
    /// API" must complete before "write integration tests"). Empty
    /// when there are no ordering constraints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependencies: Vec<Uuid>,
    /// Current lifecycle status.
    ///
    /// Updated by the orchestrator as the Task progresses. See
    /// [`TaskStatus`] for valid transitions and semantics.
    status: TaskStatus,
}

impl Task {
    /// Create a new Task.
    ///
    /// # Arguments
    /// * `created_by` - Actor creating the task
    /// * `title` - Short summary of the task
    /// * `goal` - Optional classification (Feature, Bugfix, etc.)
    pub fn new(
        created_by: ActorRef,
        title: impl Into<String>,
        goal: Option<GoalType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Task, created_by)?,
            title: title.into(),
            description: None,
            goal,
            constraints: Vec::new(),
            acceptance_criteria: Vec::new(),
            requester: None,
            parent: None,
            intent: None,
            runs: Vec::new(),
            dependencies: Vec::new(),
            status: TaskStatus::Draft,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn goal(&self) -> Option<&GoalType> {
        self.goal.as_ref()
    }

    pub fn constraints(&self) -> &[String] {
        &self.constraints
    }

    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    pub fn requester(&self) -> Option<&ActorRef> {
        self.requester.as_ref()
    }

    /// Returns the parent Task ID, if this is a sub-Task.
    pub fn parent(&self) -> Option<Uuid> {
        self.parent
    }

    pub fn intent(&self) -> Option<Uuid> {
        self.intent
    }

    /// Returns the chronological list of Run IDs for this task.
    pub fn runs(&self) -> &[Uuid] {
        &self.runs
    }

    pub fn dependencies(&self) -> &[Uuid] {
        &self.dependencies
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    pub fn add_constraint(&mut self, constraint: impl Into<String>) {
        self.constraints.push(constraint.into());
    }

    pub fn add_acceptance_criterion(&mut self, criterion: impl Into<String>) {
        self.acceptance_criteria.push(criterion.into());
    }

    pub fn set_requester(&mut self, requester: Option<ActorRef>) {
        self.requester = requester;
    }

    pub fn set_parent(&mut self, parent: Option<Uuid>) {
        self.parent = parent;
    }

    pub fn set_intent(&mut self, intent: Option<Uuid>) {
        self.intent = intent;
    }

    /// Appends a Run ID to the execution history.
    pub fn add_run(&mut self, run_id: Uuid) {
        self.runs.push(run_id);
    }

    pub fn add_dependency(&mut self, task_id: Uuid) {
        self.dependencies.push(task_id);
    }

    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task: {}", self.header.object_id())
    }
}

impl ObjectTrait for Task {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Task
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Task size: {}", e);
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
    use crate::internal::object::types::ActorKind;

    #[test]
    fn test_task_creation() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task = Task::new(actor, "Fix bug", Some(GoalType::Bugfix)).expect("task");

        // Test dependencies
        let dep_id = Uuid::from_u128(0x00000000000000000000000000000001);
        task.add_dependency(dep_id);

        assert_eq!(task.header().object_type(), &ObjectType::Task);
        assert_eq!(task.status(), &TaskStatus::Draft);
        assert_eq!(task.goal(), Some(&GoalType::Bugfix));
        assert_eq!(task.dependencies().len(), 1);
        assert_eq!(task.dependencies()[0], dep_id);
        assert!(task.intent().is_none());
    }

    #[test]
    fn test_task_goal_optional() {
        let actor = ActorRef::human("jackie").expect("actor");
        let task = Task::new(actor, "Write docs", None).expect("task");

        assert!(task.goal().is_none());
    }

    #[test]
    fn test_task_requester() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task = Task::new(actor.clone(), "Fix bug", Some(GoalType::Bugfix)).expect("task");

        task.set_requester(Some(ActorRef::mcp_client("vscode-client").expect("actor")));

        assert!(task.requester().is_some());
        assert_eq!(task.requester().unwrap().kind(), &ActorKind::McpClient);
    }

    #[test]
    fn test_task_runs() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task = Task::new(actor, "Fix bug", Some(GoalType::Bugfix)).expect("task");

        assert!(task.runs().is_empty());

        let run1 = Uuid::from_u128(0x10);
        let run2 = Uuid::from_u128(0x20);
        task.add_run(run1);
        task.add_run(run2);

        assert_eq!(task.runs(), &[run1, run2]);
    }

    #[test]
    fn test_task_from_bytes_without_header_version() {
        // Old format data without header_version — should still parse
        let json = serde_json::json!({
            "object_id": "01234567-89ab-cdef-0123-456789abcdef",
            "object_type": "task",
            "schema_version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "created_by": {"kind": "human", "id": "jackie"},
            "visibility": "private",
            "title": "old task",
            "status": "draft"
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let task =
            Task::from_bytes(&bytes, ObjectHash::default()).expect("should parse old format");
        assert_eq!(task.title(), "old task");
        assert_eq!(task.header().header_version(), 1);
    }

    #[test]
    fn test_task_serialization_includes_header_version() {
        let actor = ActorRef::human("jackie").expect("actor");
        let task = Task::new(actor, "New task", None).expect("task");
        let data = task.to_data().expect("serialize");
        let value: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(
            value["header_version"],
            crate::internal::object::types::CURRENT_HEADER_VERSION
        );
    }
}
