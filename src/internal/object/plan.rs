//! AI Plan Definition
//!
//! A [`Plan`] is a sequence of [`PlanStep`]s derived from an
//! [`Intent`](super::intent::Intent)'s analyzed content. It defines
//! *what* to do — the strategy and decomposition — while
//! [`Run`](super::run::Run) handles *how* to execute it. The Plan is
//! step ③ in the end-to-end flow described in [`mod.rs`](super).
//!
//! # Position in Lifecycle
//!
//! ```text
//!  ②  Intent (Active)       ← content analyzed
//!       │
//!       ├──▶ ContextPipeline ← seeded with IntentAnalysis frame
//!       │
//!       ▼
//!  ③  Plan (pipeline, fwindow, steps)
//!       │
//!       ├─ PlanStep₀ (inline)
//!       ├─ PlanStep₁ ──task──▶ sub-Task (recursive)
//!       └─ PlanStep₂ (inline)
//!       │
//!       ▼
//!  ④  Task ──runs──▶ Run ──plan──▶ Plan (snapshot reference)
//! ```
//!
//! # Revision Chain
//!
//! When the agent encounters obstacles or learns new information, it
//! creates a revised Plan via [`new_revision`](Plan::new_revision).
//! Each revision links back to its predecessor via `previous`, forming
//! a singly-linked revision chain. The [`Intent`](super::intent::Intent)
//! always points to the **latest** revision:
//!
//! ```text
//! Intent.plan ──▶ Plan_v3 (latest)
//!                   │ previous
//!                   ▼
//!                 Plan_v2
//!                   │ previous
//!                   ▼
//!                 Plan_v1 (original, previous = None)
//! ```
//!
//! Each [`Run`](super::run::Run) records the specific Plan version it
//! executed via a **snapshot reference** (`Run.plan`), which never
//! changes after creation.
//!
//! # Context Range
//!
//! A Plan references a [`ContextPipeline`](super::pipeline::ContextPipeline)
//! via `pipeline` and records the visible frame range `fwindow = (start,
//! end)` — the half-open range `[start..end)` of frames that were
//! visible when this Plan was created. This enables retrospective
//! analysis: given the context the agent saw, was the plan a reasonable
//! decomposition?
//!
//! ```text
//! ContextPipeline.frames:  [F₀, F₁, F₂, F₃, F₄, F₅, ...]
//!                           ^^^^^^^^^^^^^^^^
//!                           fwindow = (0, 4)
//! ```
//!
//! When replanning occurs, a new Plan is created with an updated frame
//! range that includes frames accumulated since the previous plan.
//!
//! # Steps
//!
//! Each [`PlanStep`] has a `description` (what to do) and a status
//! history (`statuses`) tracking every lifecycle transition with
//! timestamps and optional reasons, following the same append-only
//! pattern used by [`Intent`](super::intent::Intent).
//!
//! ## Step Context Tracking
//!
//! Each step tracks its relationship to pipeline frames via two index
//! vectors:
//!
//! - `iframes` — indices of frames the step **consumed** as context.
//! - `oframes` — indices of frames the step **produced** (e.g.
//!   `StepSummary`, `CodeChange`).
//!
//! All context association is owned by the step side;
//! [`ContextFrame`](super::pipeline::ContextFrame) itself is a passive
//! data record with no back-references.
//!
//! ```text
//! ContextPipeline.frames:  [F₀, F₁, F₂, F₃, F₄, F₅]
//!                            │    │         ▲
//!                            ╰────╯         │
//!                            iframes       oframes
//!                              ╰── Step₀ ──╯
//! ```
//!
//! ## Recursive Decomposition
//!
//! A step can optionally spawn a sub-[`Task`](super::task::Task) via
//! its `task` field. When set, the step delegates execution to an
//! independent Task with its own Run / Intent / Plan lifecycle,
//! enabling recursive work breakdown:
//!
//! ```text
//! Plan
//!  ├─ Step₀  (inline — executed by current Run)
//!  ├─ Step₁  ──task──▶ Task₁
//!  │                     └─ Run → Plan
//!  │                          ├─ Step₁₋₀
//!  │                          └─ Step₁₋₁
//!  └─ Step₂  (inline)
//! ```
//!
//! # Purpose
//!
//! - **Decomposition**: Breaks a complex Intent into manageable,
//!   ordered steps that an agent can execute sequentially.
//! - **Context Scoping**: `pipeline` + `fwindow` record exactly what
//!   context the Plan was derived from. Step-level `iframes`/`oframes`
//!   track fine-grained context flow.
//! - **Versioning**: The `previous` revision chain preserves the full
//!   planning history, enabling comparison of strategies across
//!   attempts.
//! - **Recursive Delegation**: Steps can spawn sub-Tasks for complex
//!   sub-problems, enabling divide-and-conquer workflows.

use std::fmt;

use chrono::{DateTime, Utc};
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

/// Lifecycle status of a [`PlanStep`].
///
/// Valid transitions:
/// ```text
/// Pending ──▶ Progressing ──▶ Completed
///   │             │
///   ├─────────────┴──▶ Failed
///   └──────────────────▶ Skipped
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Step is waiting to be executed. Initial state.
    Pending,
    /// Step is currently being executed by the agent.
    Progressing,
    /// Step finished successfully. Outputs and `oframes` should be set.
    Completed,
    /// Step encountered an unrecoverable error. A reason should be
    /// recorded in the [`StepStatusEntry`] that carries this status.
    Failed,
    /// Step was skipped (e.g. no longer necessary after replanning,
    /// or pre-condition not met). Not an error — the Plan continues.
    Skipped,
}

impl StepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            StepStatus::Pending => "pending",
            StepStatus::Progressing => "progressing",
            StepStatus::Completed => "completed",
            StepStatus::Failed => "failed",
            StepStatus::Skipped => "skipped",
        }
    }
}

impl fmt::Display for StepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single entry in a step's status history.
///
/// Mirrors [`StatusEntry`](super::intent::StatusEntry) in Intent.
/// Each transition appends a new entry; entries are never removed
/// or mutated, forming an append-only audit log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepStatusEntry {
    /// The [`StepStatus`] that was entered by this transition.
    status: StepStatus,
    /// UTC timestamp of when this transition occurred.
    changed_at: DateTime<Utc>,
    /// Optional human-readable reason for the transition.
    ///
    /// Recommended for `Failed` (error details) and `Skipped`
    /// (why the step was deemed unnecessary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl StepStatusEntry {
    pub fn new(status: StepStatus, reason: Option<String>) -> Self {
        Self {
            status,
            changed_at: Utc::now(),
            reason,
        }
    }

    pub fn status(&self) -> &StepStatus {
        &self.status
    }

    pub fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// Default for [`PlanStep::statuses`] when deserializing legacy data
/// that lacks the `statuses` field.
fn default_step_statuses() -> Vec<StepStatusEntry> {
    vec![StepStatusEntry::new(StepStatus::Pending, None)]
}

/// A single step within a [`Plan`], describing one unit of work.
///
/// Steps are executed in order by the agent. Each step can be either
/// **inline** (executed directly by the current Run) or **delegated**
/// (spawning a sub-Task via the `task` field). See module documentation
/// for context tracking and recursive decomposition details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanStep {
    /// Human-readable description of what this step should accomplish.
    ///
    /// Set once at creation. The `alias = "intent"` supports legacy
    /// serialized data where this field was named `intent`.
    #[serde(alias = "intent")]
    description: String,
    /// Expected inputs for this step as a JSON value.
    ///
    /// Schema is step-dependent. For example, a "refactor" step might
    /// list `{"files": ["src/auth.rs"]}`. `None` when the step has no
    /// explicit inputs (e.g. a discovery step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inputs: Option<serde_json::Value>,
    /// Expected outputs for this step as a JSON value.
    ///
    /// Populated after execution completes. For example,
    /// `{"files_modified": ["src/auth.rs", "src/lib.rs"]}`. `None`
    /// while the step is `Pending` or `Progressing`, or when the step
    /// produces no structured output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputs: Option<serde_json::Value>,
    /// Validation criteria for this step as a JSON value.
    ///
    /// Defines what must pass for the step to be considered successful.
    /// For example, `{"tests": "cargo test", "lint": "cargo clippy"}`.
    /// `None` when no explicit checks are defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checks: Option<serde_json::Value>,
    /// Indices into the pipeline's frame list that this step **consumed**
    /// as input context.
    ///
    /// Set when the step begins execution. The indices reference
    /// [`ContextFrame`](super::pipeline::ContextFrame)s in the
    /// [`ContextPipeline`](super::pipeline::ContextPipeline) that the
    /// Plan references. Empty when no prior context was consumed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    iframes: Vec<u32>,
    /// Indices into the pipeline's frame list that this step **produced**
    /// as output context.
    ///
    /// Set after the step completes. The step pushes new frames (e.g.
    /// `StepSummary`, `CodeChange`) to the pipeline and records their
    /// indices here. Empty when the step produced no context frames.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    oframes: Vec<u32>,
    /// Optional sub-[`Task`](super::task::Task) spawned for this step.
    ///
    /// When set, the step delegates execution to an independent Task
    /// with its own Run / Intent / Plan lifecycle (recursive
    /// decomposition). The sub-Task's `parent` field points back to
    /// the owning Task. When `None`, the step is executed inline by
    /// the current Run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task: Option<Uuid>,
    /// Append-only chronological history of status transitions.
    ///
    /// Initialized with a single `Pending` entry at creation. The
    /// current status is always `statuses.last().status`.
    ///
    /// `#[serde(default)]` ensures backward compatibility with the
    /// legacy schema that used a single `status: PlanStatus` field.
    /// When deserializing old data that lacks `statuses`, the default
    /// produces a single `Pending` entry.
    #[serde(default = "default_step_statuses")]
    statuses: Vec<StepStatusEntry>,
}

impl PlanStep {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            inputs: None,
            outputs: None,
            checks: None,
            iframes: Vec::new(),
            oframes: Vec::new(),
            task: None,
            statuses: vec![StepStatusEntry::new(StepStatus::Pending, None)],
        }
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn inputs(&self) -> Option<&serde_json::Value> {
        self.inputs.as_ref()
    }

    pub fn outputs(&self) -> Option<&serde_json::Value> {
        self.outputs.as_ref()
    }

    pub fn checks(&self) -> Option<&serde_json::Value> {
        self.checks.as_ref()
    }

    /// Returns the current step status (last entry in the history).
    pub fn status(&self) -> &StepStatus {
        &self
            .statuses
            .last()
            .expect("statuses is never empty")
            .status
    }

    /// Returns the full chronological status history.
    pub fn statuses(&self) -> &[StepStatusEntry] {
        &self.statuses
    }

    /// Transitions the step to a new status, appending to the history.
    pub fn set_status(&mut self, status: StepStatus) {
        self.statuses.push(StepStatusEntry::new(status, None));
    }

    /// Transitions the step to a new status with a reason.
    pub fn set_status_with_reason(&mut self, status: StepStatus, reason: impl Into<String>) {
        self.statuses
            .push(StepStatusEntry::new(status, Some(reason.into())));
    }

    pub fn set_inputs(&mut self, inputs: Option<serde_json::Value>) {
        self.inputs = inputs;
    }

    pub fn set_outputs(&mut self, outputs: Option<serde_json::Value>) {
        self.outputs = outputs;
    }

    pub fn set_checks(&mut self, checks: Option<serde_json::Value>) {
        self.checks = checks;
    }

    /// Returns the pipeline frame indices this step consumed as input context.
    pub fn iframes(&self) -> &[u32] {
        &self.iframes
    }

    /// Returns the pipeline frame indices this step produced as output context.
    pub fn oframes(&self) -> &[u32] {
        &self.oframes
    }

    /// Records the pipeline frame indices this step consumed as input.
    pub fn set_iframes(&mut self, indices: Vec<u32>) {
        self.iframes = indices;
    }

    /// Records the pipeline frame indices this step produced as output.
    pub fn set_oframes(&mut self, indices: Vec<u32>) {
        self.oframes = indices;
    }

    /// Returns the sub-Task ID if this step has been elevated to an
    /// independent Task.
    pub fn task(&self) -> Option<Uuid> {
        self.task
    }

    /// Elevates this step to an independent sub-Task, or clears the
    /// association by passing `None`.
    pub fn set_task(&mut self, task: Option<Uuid>) {
        self.task = task;
    }
}

/// A sequence of steps derived from an Intent's analyzed content.
///
/// A Plan is a pure planning artifact — it defines *what* to do, not
/// *how* to execute. It is step ③ in the end-to-end flow. A
/// [`Run`](super::run::Run) then references the Plan via `plan` to
/// execute it. See module documentation for revision chain, context
/// range, and recursive decomposition details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// Link to the predecessor Plan in the revision chain.
    ///
    /// Forms a singly-linked list from newest to oldest: each revised
    /// Plan points to the Plan it supersedes. `None` for the initial
    /// (first) Plan. The [`Intent`](super::intent::Intent) always
    /// points to the latest revision via `Intent.plan`.
    ///
    /// Use [`new_revision`](Plan::new_revision) to create a successor
    /// that automatically sets this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous: Option<Uuid>,
    /// The [`ContextPipeline`](super::pipeline::ContextPipeline) that
    /// served as the context basis for this Plan.
    ///
    /// Set when the Plan is created from an Intent's analyzed content.
    /// The pipeline contains the [`ContextFrame`](super::pipeline::ContextFrame)s
    /// that informed this Plan's decomposition. `None` when no pipeline
    /// was used (e.g. a manually created Plan).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pipeline: Option<Uuid>,
    /// Frame visibility window `(start, end)`.
    ///
    /// A half-open range `[start..end)` into the pipeline's frame list
    /// that was visible when this Plan was created. Enables
    /// retrospective analysis: given the context the agent saw, was the
    /// decomposition reasonable? `None` when `pipeline` is not set or
    /// when the entire pipeline was visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fwindow: Option<(u32, u32)>,
    /// Ordered sequence of steps to execute.
    ///
    /// Steps are executed in order (index 0 first). Each step can be
    /// inline (executed by the current Run) or delegated (spawning a
    /// sub-Task via `PlanStep.task`). Empty when the Plan has just been
    /// created and steps haven't been added yet.
    #[serde(default)]
    steps: Vec<PlanStep>,
}

impl Plan {
    /// Create a new initial plan (no predecessor).
    pub fn new(created_by: ActorRef) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Plan, created_by)?,
            previous: None,
            pipeline: None,
            fwindow: None,
            steps: Vec::new(),
        })
    }

    /// Create a revised plan that links back to this one.
    pub fn new_revision(&self, created_by: ActorRef) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Plan, created_by)?,
            previous: Some(self.header.object_id()),
            pipeline: None,
            fwindow: None,
            steps: Vec::new(),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn previous(&self) -> Option<Uuid> {
        self.previous
    }

    pub fn steps(&self) -> &[PlanStep] {
        &self.steps
    }

    pub fn add_step(&mut self, step: PlanStep) {
        self.steps.push(step);
    }

    pub fn set_previous(&mut self, previous: Option<Uuid>) {
        self.previous = previous;
    }

    /// Returns the pipeline that served as context basis for this plan.
    pub fn pipeline(&self) -> Option<Uuid> {
        self.pipeline
    }

    /// Sets the pipeline that serves as the context basis for this plan.
    pub fn set_pipeline(&mut self, pipeline: Option<Uuid>) {
        self.pipeline = pipeline;
    }

    /// Returns the frame window `(start, end)` — the half-open range
    /// `[start..end)` of pipeline frames visible when this plan was created.
    pub fn fwindow(&self) -> Option<(u32, u32)> {
        self.fwindow
    }

    /// Sets the frame window `(start, end)` — the half-open range
    /// `[start..end)` of pipeline frames visible when this plan was created.
    pub fn set_fwindow(&mut self, fwindow: Option<(u32, u32)>) {
        self.fwindow = fwindow;
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Plan: {}", self.header.object_id())
    }
}

impl ObjectTrait for Plan {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Plan
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Plan size: {}", e);
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
    use serde_json::json;

    use super::*;

    #[test]
    fn test_plan_revision_chain() {
        let actor = ActorRef::human("jackie").expect("actor");

        let plan_v1 = Plan::new(actor.clone()).expect("plan");
        let plan_v2 = plan_v1.new_revision(actor.clone()).expect("plan");
        let plan_v3 = plan_v2.new_revision(actor.clone()).expect("plan");

        // Initial plan has no predecessor
        assert!(plan_v1.previous().is_none());

        // Revision chain links back correctly
        assert_eq!(plan_v2.previous(), Some(plan_v1.header().object_id()));
        assert_eq!(plan_v3.previous(), Some(plan_v2.header().object_id()));

        // Chronological ordering via header timestamps
        assert!(plan_v2.header().created_at() >= plan_v1.header().created_at());
        assert!(plan_v3.header().created_at() >= plan_v2.header().created_at());
    }

    #[test]
    fn test_plan_pipeline_and_fwindow() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut plan = Plan::new(actor).expect("plan");

        assert!(plan.pipeline().is_none());
        assert!(plan.fwindow().is_none());

        let pipeline_id = Uuid::from_u128(0x42);
        plan.set_pipeline(Some(pipeline_id));
        plan.set_fwindow(Some((0, 3)));

        assert_eq!(plan.pipeline(), Some(pipeline_id));
        assert_eq!(plan.fwindow(), Some((0, 3)));
    }

    #[test]
    fn test_plan_step_statuses() {
        let mut step = PlanStep::new("run tests");

        // Initial state: one Pending entry
        assert_eq!(step.statuses().len(), 1);
        assert_eq!(step.status(), &StepStatus::Pending);

        // Transition to Progressing
        step.set_status(StepStatus::Progressing);
        assert_eq!(step.status(), &StepStatus::Progressing);
        assert_eq!(step.statuses().len(), 2);

        // Transition to Completed with reason
        step.set_status_with_reason(StepStatus::Completed, "all checks passed");
        assert_eq!(step.status(), &StepStatus::Completed);
        assert_eq!(step.statuses().len(), 3);

        // Verify full history
        let history = step.statuses();
        assert_eq!(history[0].status(), &StepStatus::Pending);
        assert!(history[0].reason().is_none());
        assert_eq!(history[1].status(), &StepStatus::Progressing);
        assert!(history[1].reason().is_none());
        assert_eq!(history[2].status(), &StepStatus::Completed);
        assert_eq!(history[2].reason(), Some("all checks passed"));

        // Timestamps are ordered
        assert!(history[1].changed_at() >= history[0].changed_at());
        assert!(history[2].changed_at() >= history[1].changed_at());
    }

    #[test]
    fn test_plan_step_deserializes_legacy_intent_field() {
        let step: PlanStep = serde_json::from_value(json!({
            "intent": "run tests",
            "statuses": [{"status": "pending", "changed_at": "2026-01-01T00:00:00Z"}]
        }))
        .expect("deserialize legacy step");

        assert_eq!(step.description(), "run tests");
    }

    #[test]
    fn test_plan_step_serializes_description_field() {
        let step = PlanStep::new("run tests");
        let value = serde_json::to_value(&step).expect("serialize step");

        assert_eq!(
            value.get("description").and_then(|v| v.as_str()),
            Some("run tests")
        );
        assert!(value.get("intent").is_none());
    }

    #[test]
    fn test_plan_step_context_frames() {
        let mut step = PlanStep::new("refactor auth module");

        // Initially empty
        assert!(step.iframes().is_empty());
        assert!(step.oframes().is_empty());

        // Step consumed frames 0 and 1 as input context
        step.set_iframes(vec![0, 1]);
        // Step produced frame 2 as output
        step.set_oframes(vec![2]);

        assert_eq!(step.iframes(), &[0, 1]);
        assert_eq!(step.oframes(), &[2]);
    }

    #[test]
    fn test_plan_step_context_frames_serde_roundtrip() {
        let mut step = PlanStep::new("deploy");
        step.set_iframes(vec![0, 3]);
        step.set_oframes(vec![4, 5]);

        let value = serde_json::to_value(&step).expect("serialize");
        let restored: PlanStep = serde_json::from_value(value).expect("deserialize");

        assert_eq!(restored.iframes(), &[0, 3]);
        assert_eq!(restored.oframes(), &[4, 5]);
    }

    #[test]
    fn test_plan_step_empty_frames_omitted_in_json() {
        let step = PlanStep::new("noop");
        let value = serde_json::to_value(&step).expect("serialize");

        // Empty vecs should be omitted (skip_serializing_if = "Vec::is_empty")
        assert!(value.get("iframes").is_none());
        assert!(value.get("oframes").is_none());
    }

    #[test]
    fn test_plan_fwindow_serde_roundtrip() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut plan = Plan::new(actor).expect("plan");
        plan.set_pipeline(Some(Uuid::from_u128(0x99)));
        plan.set_fwindow(Some((2, 7)));

        let mut step = PlanStep::new("step 0");
        step.set_iframes(vec![2, 3]);
        step.set_oframes(vec![7]);
        plan.add_step(step);

        let data = plan.to_data().expect("serialize");
        let restored = Plan::from_bytes(&data, ObjectHash::default()).expect("deserialize");

        assert_eq!(restored.fwindow(), Some((2, 7)));
        assert_eq!(restored.steps()[0].iframes(), &[2, 3]);
        assert_eq!(restored.steps()[0].oframes(), &[7]);
    }

    #[test]
    fn test_plan_step_subtask() {
        let mut step = PlanStep::new("design OAuth flow");

        // Initially no sub-task
        assert!(step.task().is_none());

        // Elevate to independent sub-Task
        let sub_task_id = Uuid::from_u128(0xAB);
        step.set_task(Some(sub_task_id));
        assert_eq!(step.task(), Some(sub_task_id));

        // Clear association
        step.set_task(None);
        assert!(step.task().is_none());
    }

    #[test]
    fn test_plan_step_subtask_serde_roundtrip() {
        let mut step = PlanStep::new("implement auth module");
        let sub_task_id = Uuid::from_u128(0xCD);
        step.set_task(Some(sub_task_id));

        let value = serde_json::to_value(&step).expect("serialize");
        assert!(value.get("task").is_some());

        let restored: PlanStep = serde_json::from_value(value).expect("deserialize");
        assert_eq!(restored.task(), Some(sub_task_id));
    }

    #[test]
    fn test_plan_step_no_subtask_omitted_in_json() {
        let step = PlanStep::new("inline step");
        let value = serde_json::to_value(&step).expect("serialize");

        // None task should be omitted (skip_serializing_if)
        assert!(value.get("task").is_none());
    }
}
