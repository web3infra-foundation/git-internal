//! AI Intent definition and lifecycle contract.
//!
//! This module defines the `Intent` object used by the AI workflow and
//! keeps the raw user request anchored to all downstream objects. It
//! follows the same step chain in `docs/agent-workflow.md`:
//! `User Query -> Intent -> Plan -> Task -> Run -> (ToolInvocation, PatchSet,
//! Evidence) -> Decision -> Intent complete`.
//!
//! The workflow-level structured understanding is captured as `IntentSpec`
//! (`goal`, `constraints`, `risk_level`) and serialized in the `spec`
//! field. The full historical state is an append-only `statuses` vector;
//! each transition writes a new [`StatusEntry`] with an immutable timestamp.
//!
//! # Position in the AI workflow
//!
//! ```text
//! User Query
//!    │
//!    ▼
//! ② Intent (Draft → Active)
//!    │
//!    ├─ Draft: prompt captured, `spec=None`
//!    └─ Active: AI has produced `IntentSpec`
//!         └─ Direct link to ContextPipeline (`context_pipeline`)
//!         
//!         ▼
//! ③ Plan (workflow decomposition)
//!    │
//!    ▼
//! ④ Task
//!    │
//!    ▼
//! ⑤ Run(s)
//!    ├─ ⑥ ToolInvocation*
//!    ├─ ⑦ PatchSet*
//!    ├─ ⑧ Evidence*
//!    └─ ⑨ Decision
//!         │
//!         ▼
//! ⑩ Intent terminal state (Completed / Cancelled)
//! ```
//!
//! ## Conversational Refinement
//!
//! `Intent` objects can form a parent chain to represent iterative user
//! refinement. Each follow-up `Intent` stores a `parent` UUID and does not
//! mutate its predecessor.
//! ```text
//! Intent_N -> parent -> Intent_(N-1) -> ... -> Intent_0
//! ```
//!
//! # Purpose
//!
//! - **Traceability**: links the original request to every downstream
//!   object.
//! - **Reproducibility**: keeps both raw `prompt` and structured analysis in
//!   `spec` for re-analysis with different models/parameters.
//! - **Risk-aware execution**: `risk_level` in `IntentSpec` is used by the
//!   workflow to drive review thresholds and automation gates.
//! - **Completion closing**: `commit` records the exact repository commit
//!   that satisfies the request when status reaches `Completed`.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        integrity::IntegrityHash,
        pipeline::ContextPipeline,
        types::{ActorRef, Header, ObjectType},
    },
};

/// Status of an Intent through its lifecycle.
///
/// Valid transitions (see module docs for diagram):
///
/// - `Draft` → `Active`: AI has analyzed the prompt and filled `spec`.
/// - `Active` → `Completed`: All downstream Tasks finished successfully
///   and the result commit has been recorded in `Intent.commit`.
/// - `Draft` → `Cancelled`: User abandoned the request before AI analysis.
/// - `Active` → `Cancelled`: User or orchestrator cancelled during
///   planning/execution (e.g. timeout, user interrupt, budget exceeded).
///
/// Reverse transitions (e.g. `Active` → `Draft`) are not expected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    /// Initial state. The `prompt` has been captured but the AI has not
    /// yet analyzed it — `Intent.spec` is `None`.
    Draft,
    /// AI interpretation is available in `Intent.spec`. Downstream
    /// objects (Plan, Tasks, Runs) may be in progress.
    Active,
    /// The Intent has been fully satisfied. `Intent.commit` should
    /// contain the SHA of the git commit that fulfils the request.
    Completed,
    /// The Intent was abandoned before completion. A reason should be
    /// recorded in the [`StatusEntry`] that carries this status.
    Cancelled,
}

impl IntentStatus {
    /// Returns the snake_case string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            IntentStatus::Draft => "draft",
            IntentStatus::Active => "active",
            IntentStatus::Completed => "completed",
            IntentStatus::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for IntentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single entry in the Intent's status history.
///
/// Each status transition appends a new `StatusEntry` to
/// `Intent.statuses`. The entries are never removed or mutated,
/// forming an append-only audit log. The current status is always
/// `statuses.last().status`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StatusEntry {
    /// The [`IntentStatus`] that was entered by this transition.
    status: IntentStatus,
    /// UTC timestamp of when this transition occurred.
    ///
    /// Automatically set to `Utc::now()` by [`StatusEntry::new`].
    /// Timestamps across entries in the same Intent are monotonically
    /// non-decreasing.
    changed_at: DateTime<Utc>,
    /// Optional human-readable reason for the transition.
    ///
    /// Recommended for `Cancelled` (why the request was abandoned) and
    /// `Completed` (summary of what was achieved). May be `None` for
    /// routine transitions like `Draft` → `Active`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl StatusEntry {
    /// Creates a new status entry timestamped to now.
    pub fn new(status: IntentStatus, reason: Option<String>) -> Self {
        Self {
            status,
            changed_at: Utc::now(),
            reason,
        }
    }

    /// The status that was entered.
    pub fn status(&self) -> &IntentStatus {
        &self.status
    }

    /// When the transition occurred.
    pub fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    /// Optional reason for the transition.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// The entry point of every AI-assisted workflow.
///
/// An `Intent` captures both the verbatim user input (`prompt`) and the
/// AI's structured understanding of that input (`spec`). It is
/// created at step ② and completed at step ⑩ of the end-to-end flow.
/// See module documentation for lifecycle position, status transitions,
/// and conversational refinement.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(transparent)]
pub struct IntentSpec(pub serde_json::Value);

impl From<String> for IntentSpec {
    fn from(value: String) -> Self {
        Self(serde_json::Value::String(value))
    }
}

impl From<&str> for IntentSpec {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// Verbatim natural-language request from the user.
    ///
    /// This is the unmodified input exactly as the user typed it (e.g.
    /// "Add pagination to the user list API"). It is set once at
    /// creation and never changed, preserving the original request for
    /// auditing and potential re-analysis with a different model.
    prompt: String,
    /// AI-analyzed structured interpretation of `prompt`.
    ///
    /// `None` while the Intent is in `Draft` status — the AI has not
    /// yet processed the prompt. Set to `Some(...)` when the AI
    /// completes its analysis, at which point the status should
    /// transition to `Active`. The spec typically includes:
    /// - Disambiguated requirements
    /// - Identified scope (which files, modules, APIs are affected)
    /// - Inferred constraints or acceptance criteria
    ///
    /// Unlike `prompt`, `spec` is the AI's output and may be
    /// regenerated if the analysis is re-run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spec: Option<IntentSpec>,
    /// Link to a predecessor Intent for conversational refinement.
    ///
    /// Forms a singly-linked list from newest to oldest: each
    /// follow-up Intent points to the Intent it refines. `None` for
    /// the first Intent in a conversation. The orchestrator can walk
    /// the `parent` chain to reconstruct the full conversational
    /// history and provide prior context to the AI.
    ///
    /// Example chain: Intent₂ → Intent₁ → Intent₀ (root, parent=None).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Uuid>,
    /// Git commit hash recorded when this Intent is fulfilled.
    ///
    /// Set by the orchestrator at step ⑩ after the
    /// [`Decision`](super::decision::Decision) applies the final
    /// PatchSet. `None` while the Intent is in progress (`Draft` or
    /// `Active`) or if it was `Cancelled`. When set, the Intent's
    /// status should be `Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commit: Option<IntegrityHash>,
    /// Link to the [`ContextPipeline`](super::pipeline::ContextPipeline)
    /// reserved for this Intent.
    ///
    /// `Intent::new` creates the pipeline eagerly and stores its ID
    /// here for one-step traversal from Intent → ContextPipeline.
    context_pipeline: Uuid,
    /// Link to the [`Plan`](super::plan::Plan) derived from this
    /// Intent.
    ///
    /// Set after the AI analyzes `spec` and produces a Plan at
    /// step ③. Always points to the **latest** Plan revision — if
    /// the Plan is revised (via `Plan.previous` chain), this field
    /// is updated to the newest version. `None` while no Plan has
    /// been created yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan: Option<Uuid>,
    /// Append-only chronological history of status transitions.
    ///
    /// Initialized with a single `Draft` entry at creation. Each call
    /// to [`set_status`](Intent::set_status) or
    /// [`set_status_with_reason`](Intent::set_status_with_reason)
    /// pushes a new [`StatusEntry`]. The current status is always
    /// `statuses.last().status`. Entries are never removed or mutated.
    ///
    /// This design preserves the full transition timeline with
    /// timestamps and optional reasons, enabling audit and duration
    /// analysis (e.g. time spent in `Active` before `Completed`).
    statuses: Vec<StatusEntry>,
}

impl Intent {
    /// Create a new intent in `Draft` status from a raw user prompt.
    ///
    /// A [`ContextPipeline`](super::pipeline::ContextPipeline) is
    /// created first, then linked into the returned Intent.
    ///
    /// The `spec` field is initially `None` — call [`set_spec`](Intent::set_spec)
    /// after the AI has analyzed the prompt.
    pub fn new(
        created_by: ActorRef,
        prompt: impl Into<String>,
    ) -> Result<(Self, ContextPipeline), String> {
        let pipeline = ContextPipeline::new(created_by.clone())?;
        let pipeline_id = pipeline.header().object_id();

        Ok((
            Self {
                header: Header::new(ObjectType::Intent, created_by)?,
                prompt: prompt.into(),
                spec: None,
                parent: None,
                commit: None,
                context_pipeline: pipeline_id,
                plan: None,
                statuses: vec![StatusEntry::new(IntentStatus::Draft, None)],
            },
            pipeline,
        ))
    }

    /// Returns a reference to the common header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns the raw user prompt.
    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    /// Returns the AI-analyzed spec, if available.
    pub fn spec(&self) -> Option<&IntentSpec> {
        self.spec.as_ref()
    }

    /// Sets the AI-analyzed spec.
    pub fn set_spec(&mut self, spec: Option<IntentSpec>) {
        let is_draft = matches!(self.status(), Some(&IntentStatus::Draft));
        self.spec = spec;
        if self.spec.is_some() && is_draft {
            self.set_status(IntentStatus::Active);
        }
    }

    /// Returns the parent intent ID, if this is part of a refinement chain.
    pub fn parent(&self) -> Option<Uuid> {
        self.parent
    }

    /// Returns the result commit SHA, if the intent has been fulfilled.
    pub fn commit(&self) -> Option<&IntegrityHash> {
        self.commit.as_ref()
    }

    /// Returns the linked ContextPipeline ID.
    pub fn context_pipeline(&self) -> Uuid {
        self.context_pipeline
    }

    /// Returns the current lifecycle status (the last entry in the history).
    ///
    /// Returns `None` only if `statuses` is empty, which should not
    /// happen for objects created via [`Intent::new`] (seeds with
    /// `Draft`), but may occur for malformed deserialized data.
    pub fn status(&self) -> Option<&IntentStatus> {
        self.statuses.last().map(|e| &e.status)
    }

    /// Returns the full chronological status history.
    pub fn statuses(&self) -> &[StatusEntry] {
        &self.statuses
    }

    /// Links this intent to a parent intent for conversational refinement.
    pub fn set_parent(&mut self, parent: Option<Uuid>) {
        self.parent = parent;
    }

    /// Records the git commit SHA that fulfilled this intent.
    pub fn set_commit(&mut self, sha: Option<IntegrityHash>) {
        self.commit = sha;
    }

    /// Returns the associated Plan ID, if a Plan has been derived from this intent.
    pub fn plan(&self) -> Option<Uuid> {
        self.plan
    }

    /// Sets the linked ContextPipeline ID.
    pub fn set_context_pipeline(&mut self, context_pipeline: Uuid) {
        self.context_pipeline = context_pipeline;
    }

    /// Associates this intent with a [`Plan`](super::plan::Plan).
    pub fn set_plan(&mut self, plan: Option<Uuid>) {
        self.plan = plan;
    }

    /// Transitions the intent to a new lifecycle status, appending to the history.
    pub fn set_status(&mut self, status: IntentStatus) {
        self.statuses.push(StatusEntry::new(status, None));
    }

    /// Transitions the intent to a new lifecycle status with a reason.
    pub fn set_status_with_reason(&mut self, status: IntentStatus, reason: impl Into<String>) {
        self.statuses
            .push(StatusEntry::new(status, Some(reason.into())));
    }
}

impl fmt::Display for Intent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Intent: {}", self.header.object_id())
    }
}

impl ObjectTrait for Intent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidIntentObject(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Intent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Intent size: {}", e);
                0
            }
        }
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidIntentObject(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intent_creation() {
        let actor = ActorRef::human("jackie").expect("actor");
        let (mut intent, pipeline) = Intent::new(actor, "Refactor login flow").expect("intent");

        assert_eq!(intent.header().object_type(), &ObjectType::Intent);
        assert_eq!(intent.prompt(), "Refactor login flow");
        assert!(intent.spec().is_none());
        assert_eq!(intent.status(), Some(&IntentStatus::Draft));
        assert!(intent.parent().is_none());
        assert_eq!(intent.context_pipeline(), pipeline.header().object_id());
        assert!(intent.plan().is_none());

        let spec = IntentSpec(serde_json::json!({
            "objective": "Restructure the authentication module",
            "scope": ["auth module"],
            "constraints": ["keep API contract stable"]
        }));
        intent.set_spec(Some(spec.clone()));
        assert_eq!(intent.spec(), Some(&spec));
        assert_eq!(intent.status(), Some(&IntentStatus::Active));

        // After spec is analyzed, a Plan can be linked
        let plan_id = Uuid::from_u128(0x42);
        intent.set_plan(Some(plan_id));
        assert_eq!(intent.plan(), Some(plan_id));
    }

    #[test]
    fn test_statuses() {
        let actor = ActorRef::human("jackie").expect("actor");
        let (mut intent, _) = Intent::new(actor, "Fix bug").expect("intent");

        // Initial state: one Draft entry
        assert_eq!(intent.statuses().len(), 1);
        assert_eq!(intent.status(), Some(&IntentStatus::Draft));

        // Transition to Active
        intent.set_status(IntentStatus::Active);
        assert_eq!(intent.status(), Some(&IntentStatus::Active));
        assert_eq!(intent.statuses().len(), 2);

        // Transition to Completed with reason
        intent.set_status_with_reason(IntentStatus::Completed, "All tasks done");
        assert_eq!(intent.status(), Some(&IntentStatus::Completed));
        assert_eq!(intent.statuses().len(), 3);

        // Verify full history
        let history = intent.statuses();
        assert_eq!(history[0].status(), &IntentStatus::Draft);
        assert!(history[0].reason().is_none());
        assert_eq!(history[1].status(), &IntentStatus::Active);
        assert!(history[1].reason().is_none());
        assert_eq!(history[2].status(), &IntentStatus::Completed);
        assert_eq!(history[2].reason(), Some("All tasks done"));

        // Timestamps are ordered
        assert!(history[1].changed_at() >= history[0].changed_at());
        assert!(history[2].changed_at() >= history[1].changed_at());
    }
}
