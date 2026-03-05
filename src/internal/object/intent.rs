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
//! `Intent` objects can form a parent DAG to represent iterative user
//! refinement. Each follow-up `Intent` stores one or more parent `Intent`
//! IDs (for merges/branching) and does not mutate its predecessors.
//! ```text
//! Intent_N -> parent/parents -> Intent_(N-1) -> ... -> Intent_0
//! ```
//!
//! ## Headless example: fork and walk a chain
//!
//! From one intent, create multiple follow-up intents, and query all
//! descendants from a root intent.
//!
//! ```rust,ignore
//! use git_internal::internal::object::intent::Intent;
//! use git_internal::internal::object::types::ActorRef;
//! use std::collections::HashSet;
//! use uuid::Uuid;
//!
//! let actor = ActorRef::human("agent").unwrap();
//! let (root, _) = Intent::new(actor.clone(), "Support new tracing mode").unwrap();
//!
//! // 1) Fork: multiple Intents from the same parent.
//! let (opt_a, _) = Intent::new_revision_chain(
//!     actor.clone(),
//!     "Add event-level tracing",
//!     &[&root],
//! )
//! .unwrap();
//! let (opt_b, _) = Intent::new_revision_chain(
//!     actor.clone(),
//!     "Add span-level tracing",
//!     &[&root],
//! )
//! .unwrap();
//! let (merged, _) = Intent::new_revision_chain(
//!     actor,
//!     "Merge event + span design",
//!     &[&opt_a, &opt_b],
//! )
//! .unwrap();
//!
//! // 2) Query: find every descendant under the root intent.
//! let all = vec![root, opt_a, opt_b, merged];
//! let mut frontier = vec![all[0].header().object_id()];
//! let mut visited: HashSet<Uuid> = HashSet::new();
//! let mut descendants = Vec::new();
//!
//! while let Some(current) = frontier.pop() {
//!     for candidate in all.iter() {
//!         let candidate_id = candidate.header().object_id();
//!         if visited.contains(&candidate_id) {
//!             continue;
//!         }
//!         if candidate.parents().iter().any(|p| *p == current) {
//!             descendants.push(candidate_id);
//!             frontier.push(candidate_id);
//!             visited.insert(candidate_id);
//!         }
//!     }
//! }
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
//!
//! # Thread draft (for full conversation records)
//!
//! To preserve complete conversational records, a Thread aggregate can be
//! introduced as a lightweight container object. This draft is intentionally
//! versioned separately from `Intent`, so migration can be rolled out safely.
//!
//! ## Thread object fields (draft)
//!
//! | Field | Type | Description |
//! |---|---|---|
//! | `header` | `Header` | Thread metadata (id/type/timestamps/creator). |
//! | `title` | `Option<String>` | Human-readable conversation title. |
//! | `owner` | `ActorRef` | Conversation owner / initiator. |
//! | `participants` | `Vec<ActorRef>` | Optional actor list for collaboration. |
//! | `intent_ids` | `Vec<Uuid>` | All intents under this thread, ordered by creation. |
//! | `head_intent_ids` | `Vec<Uuid>` | Current non-finalized branch tips (one or more). |
//! | `metadata` | `Option<serde_json::Value>` | Optional strategy, routing, policy tags. |
//! | `archived` | `bool` | Marks historical threads as read-only for UI. |
//! | `latest_intent_id` | `Option<Uuid>` | Latest materialized intent in canonical path. |
//!
//! ## Draft relation graph
//!
//! ```text
//! Thread ──intent_ids──→ Intent₀
//!        ├─intent_ids──→ Intent₁ ──parents──→ Intent₀
//!        ├─intent_ids──→ Intent₂ ──parents──→ Intent₁
//!        └─intent_ids──→ Intent₃ ──parents──→ Intent₀/Intent₁ (merge)
//! Intent.thread_id             (1:1 link once introduced)
//! ```
//!
//! ## Migration sketch
//!
//! 1. Add `Thread` object definition and add optional `thread_id` field to
//!    `Intent` payload.
//! 2. Add `Thread` on demand: when a new root intent arrives, create one thread
//!    and append the intent to `intent_ids`.
//! 3. Backfill existing intents in batches:
//!    - group intents by `prompt`/request source correlation,
//!    - create one thread per group,
//!    - set `Intent.thread_id` accordingly,
//!    - populate `intent_ids`, `head_intent_ids`, and `latest_intent_id`.
//! 4. Keep parent chain resolution (`parents`) intact as fallback for objects
//!    created before migration.
//! 5. For future writes, require `thread_id` while keeping reads backward
//!    compatible with unset field.

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

trait ParentLike {
    fn parent_id(&self) -> Uuid;
}

impl ParentLike for Uuid {
    fn parent_id(&self) -> Uuid {
        *self
    }
}

impl ParentLike for &Intent {
    fn parent_id(&self) -> Uuid {
        self.header.object_id()
    }
}

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
    /// Links to predecessor Intents for conversational refinement.
    ///
    /// Acts like commit parents: one `Intent` can point to multiple
    /// predecessors. The orchestrator can walk the parent set recursively
    /// to reconstruct the full refinement graph/history.
    ///
    /// Example: Intent₃ can reference Intent₁ and Intent₂ if both provide
    /// usable context for follow-up synthesis.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parents: Vec<Uuid>,
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
    /// Git commit hash recorded when this Intent is fulfilled.
    ///
    /// Set by the orchestrator at step ⑩ after the
    /// [`Decision`](super::decision::Decision) applies the final
    /// PatchSet. `None` while the Intent is in progress (`Draft` or
    /// `Active`) or if it was `Cancelled`. When set, the Intent's
    /// status should be `Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commit: Option<IntegrityHash>,
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
                parents: Vec::new(),
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

    /// Create a new intent as a revision from an existing intent.
    ///
    /// This keeps a single-element parent chain by default (`parent_id`).
    /// Additional parents can be added with [`add_parent`](Intent::add_parent)
    /// for merge-style refinements.
    pub fn new_revision_from(
        created_by: ActorRef,
        prompt: impl Into<String>,
        parent: &Self,
    ) -> Result<(Self, ContextPipeline), String> {
        Self::new_revision_chain(created_by, prompt, &[parent])
    }

    /// Create a new revision intent with multiple parents in one step.
    ///
    /// The function accepts either ID-based or intent-based parent inputs
    /// and automatically deduplicates parents.
    ///
    /// - For ID-based input: `Intent::new_revision_chain(actor, prompt, &[id1, id2])`
    /// - For intent-based input: `Intent::new_revision_chain(actor, prompt, &[&intent1, &intent2])`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use git_internal::internal::object::intent::Intent;
    /// use git_internal::internal::object::types::ActorRef;
    /// use uuid::Uuid;
    ///
    /// let actor = ActorRef::human("agent").expect("actor");
    /// let (base, _) = Intent::new(actor.clone(), "Build API rate-limiting").expect("base");
    ///
    /// // 1) single parent (same as `new_revision_from`)
    /// let (r1, _) = Intent::new_revision_chain(
    ///     actor.clone(),
    ///     "Split into middleware + config",
    ///     &[&base],
    /// ).expect("revision");
    ///
    /// // 2) multi-parent merge from existing intents
    /// let extra_parent = Uuid::from_u128(0x1001);
    /// let (_merged, _) = Intent::new_revision_chain(
    ///     actor.clone(),
    ///     "Merge constraints from policy + implementation feedback",
    ///     &[&base, &r1],
    /// ).expect("merge");
    /// ```
    ///
    /// # Chained updates
    ///
    /// ```rust
    /// use git_internal::internal::object::intent::Intent;
    /// use git_internal::internal::object::types::ActorRef;
    /// use uuid::Uuid;
    ///
    /// let actor = ActorRef::human("agent").expect("actor");
    /// let (first, _) = Intent::new(actor.clone(), "A").expect("first");
    /// let (second, _) = Intent::new_revision_chain(
    ///     actor.clone(),
    ///     "B",
    ///     &[Uuid::from_u128(0x2001)],
    /// ).expect("second");
    /// let (_third, _) = Intent::new_revision_chain(
    ///     actor,
    ///     "C",
    ///     &[&first, &second],
    /// ).expect("third");
    /// ```
    #[allow(private_bounds)]
    pub fn new_revision_chain<P: ParentLike>(
        created_by: ActorRef,
        prompt: impl Into<String>,
        parents: &[P],
    ) -> Result<(Self, ContextPipeline), String> {
        let (mut intent, pipeline) = Self::new(created_by, prompt)?;
        for p in parents {
            intent.add_parent(p.parent_id());
        }
        Ok((intent, pipeline))
    }

    /// Returns the parent Intent IDs, if this is part of a refinement graph.
    pub fn parents(&self) -> &[Uuid] {
        &self.parents
    }

    /// Add a parent Intent ID with dedupe and self-parent guard.
    ///
    /// Duplicate parent IDs are ignored to keep the parent vector
    /// canonical for deterministic serialization and simple diffs.
    pub fn add_parent(&mut self, parent_id: Uuid) {
        if parent_id == self.header.object_id() {
            return;
        }
        if !self.parents.contains(&parent_id) {
            self.parents.push(parent_id);
        }
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

    /// Sets the parent Intent IDs for conversational refinement.
    pub fn set_parents(&mut self, parents: Vec<Uuid>) {
        self.parents = parents;
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
        let (mut intent, pipeline) =
            Intent::new(actor.clone(), "Refactor login flow").expect("intent");

        assert_eq!(intent.header().object_type(), &ObjectType::Intent);
        assert_eq!(intent.prompt(), "Refactor login flow");
        assert!(intent.spec().is_none());
        assert_eq!(intent.status(), Some(&IntentStatus::Draft));
        assert!(intent.parents().is_empty());
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

        // Multiple parents are supported for merge-style refinement.
        let mut intent = intent;
        intent.set_parents(vec![Uuid::from_u128(0x43), Uuid::from_u128(0x44)]);
        assert_eq!(intent.parents().len(), 2);
        assert_eq!(intent.parents()[0], Uuid::from_u128(0x43));
        assert_eq!(intent.parents()[1], Uuid::from_u128(0x44));

        // new_revision_from automatically links the previous intent.
        let (followup, _) = Intent::new_revision_from(
            actor.clone(),
            "Refactor login flow with stricter constraints",
            &intent,
        )
        .expect("intent revision");
        assert_eq!(followup.parents(), &[intent.header().object_id()]);

        // new_revision_chain accepts either parent IDs...
        let merge_parent_a = Uuid::from_u128(0x201);
        let merge_parent_b = Uuid::from_u128(0x202);
        let (merge_from_ids, _) = Intent::new_revision_chain(
            actor.clone(),
            "Refactor login flow with merged context",
            &[merge_parent_a, merge_parent_b, merge_parent_a],
        )
        .expect("intent revision by ids");
        assert_eq!(merge_from_ids.parents(), &[merge_parent_a, merge_parent_b]);

        // ...or parent intent references...
        let (merge_from_intents, _) = Intent::new_revision_chain(
            actor.clone(),
            "Refactor login flow with intent merge",
            &[&intent, &followup],
        )
        .expect("intent revision by intents");
        assert_eq!(
            merge_from_intents.parents(),
            &[intent.header().object_id(), followup.header().object_id()]
        );

        // add_parent deduplicates and ignores self-parent.
        let (mut with_helpers, _) =
            Intent::new(actor.clone(), "parent merge helper").expect("intent helper");
        let p1 = Uuid::from_u128(0x101);
        let p2 = Uuid::from_u128(0x102);
        with_helpers.add_parent(p1);
        with_helpers.add_parent(p1);
        with_helpers.add_parent(p2);
        with_helpers.add_parent(with_helpers.header().object_id());
        assert_eq!(with_helpers.parents(), &[p1, p2]);
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
