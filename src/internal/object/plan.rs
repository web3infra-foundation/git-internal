//! AI Plan snapshot.
//!
//! `Plan` stores one immutable planning revision for an `Intent`. It
//! records the chosen strategy, the stable step structure, and the
//! frozen planning context used to derive that strategy.
//!
//! # How to use this object
//!
//! - Create a base `Plan` after analyzing an `Intent`.
//! - Create a new `Plan` revision when replanning is needed.
//! - Use multi-parent revisions to represent merged planning branches.
//! - Freeze planning-time context through `context_frames`.
//!
//! # How it works with other objects
//!
//! - `Intent` is the canonical owner via `Plan.intent`.
//! - `Task.origin_step_id` points back to the logical step that spawned
//!   delegated work.
//! - `PlanStepEvent` records runtime step status, produced context,
//!   outputs, and spawned tasks.
//! - `ContextFrame` stores incremental context facts referenced by the
//!   plan or step events.
//!
//! # How Libra should call it
//!
//! Libra should write a new `Plan` whenever the strategy itself changes.
//! Libra should not mutate a stored plan to reflect execution progress;
//! instead it should append `PlanStepEvent` objects and keep the active
//! plan head in scheduler state.

use std::fmt;

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

/// Immutable step definition inside a `Plan`.
///
/// `PlanStep` describes what a logical step is supposed to do. Runtime
/// facts for that step belong to `PlanStepEvent`, not to this struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    /// Stable logical step identity across Plan revisions.
    ///
    /// `step_id` is the cross-revision identity for the logical step.
    /// One concrete stored step snapshot is identified by the pair
    /// `(Plan.header.object_id(), step_id)`.
    step_id: Uuid,
    /// Human-readable description of what this step is supposed to do.
    description: String,
    /// Optional structured inputs expected by this step definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inputs: Option<serde_json::Value>,
    /// Optional structured checks or completion criteria for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checks: Option<serde_json::Value>,
}

impl PlanStep {
    /// Create a new logical plan step with a fresh stable step id.
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            step_id: Uuid::now_v7(),
            description: description.into(),
            inputs: None,
            checks: None,
        }
    }

    /// Return the stable logical step id.
    pub fn step_id(&self) -> Uuid {
        self.step_id
    }

    /// Return the human-readable step description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Return the structured input contract for this step, if present.
    pub fn inputs(&self) -> Option<&serde_json::Value> {
        self.inputs.as_ref()
    }

    /// Return the structured checks for this step, if present.
    pub fn checks(&self) -> Option<&serde_json::Value> {
        self.checks.as_ref()
    }

    /// Set or clear the structured inputs for this in-memory step.
    pub fn set_inputs(&mut self, inputs: Option<serde_json::Value>) {
        self.inputs = inputs;
    }

    /// Set or clear the structured checks for this in-memory step.
    pub fn set_checks(&mut self, checks: Option<serde_json::Value>) {
        self.checks = checks;
    }
}

/// Immutable planning revision for one `Intent`.
///
/// A `Plan` may form a DAG through `parents`, allowing Libra to model
/// linear replanning as well as multi-branch plan merges without losing
/// history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical owning intent for this planning revision.
    intent: Uuid,
    /// Parent plan revisions from which this plan directly derives.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parents: Vec<Uuid>,
    /// Immutable context-frame snapshot used for plan derivation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    context_frames: Vec<Uuid>,
    /// Immutable step structure chosen for this plan revision.
    #[serde(default)]
    steps: Vec<PlanStep>,
}

impl Plan {
    /// Create a new root plan revision for the given intent.
    pub fn new(created_by: ActorRef, intent: Uuid) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Plan, created_by)?,
            intent,
            parents: Vec::new(),
            context_frames: Vec::new(),
            steps: Vec::new(),
        })
    }

    /// Create a new child plan revision from this plan as the only
    /// parent.
    pub fn new_revision(&self, created_by: ActorRef) -> Result<Self, String> {
        Self::new_revision_chain(created_by, &[self])
    }

    /// Create a new plan revision from a single explicit parent.
    pub fn new_revision_from(created_by: ActorRef, parent: &Self) -> Result<Self, String> {
        Self::new_revision_chain(created_by, &[parent])
    }

    /// Create a new plan revision from multiple parents.
    ///
    /// All parents must belong to the same intent.
    pub fn new_revision_chain(created_by: ActorRef, parents: &[&Self]) -> Result<Self, String> {
        let first_parent = parents
            .first()
            .ok_or_else(|| "plan revision chain requires at least one parent".to_string())?;
        let mut plan = Self::new(created_by, first_parent.intent)?;
        for parent in parents {
            if parent.intent != first_parent.intent {
                return Err("plan parents must belong to the same intent".to_string());
            }
            plan.add_parent(parent.header.object_id());
        }
        Ok(plan)
    }

    /// Return the immutable header for this plan revision.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical owning intent id.
    pub fn intent(&self) -> Uuid {
        self.intent
    }

    /// Return the direct parent plan ids.
    pub fn parents(&self) -> &[Uuid] {
        &self.parents
    }

    /// Add one parent link if it is not already present and is not self.
    pub fn add_parent(&mut self, parent_id: Uuid) {
        if parent_id == self.header.object_id() {
            return;
        }
        if !self.parents.contains(&parent_id) {
            self.parents.push(parent_id);
        }
    }

    /// Replace the parent set for this in-memory plan revision.
    pub fn set_parents(&mut self, parents: Vec<Uuid>) {
        self.parents = parents;
    }

    /// Return the planning-time context frame ids frozen into this plan.
    pub fn context_frames(&self) -> &[Uuid] {
        &self.context_frames
    }

    /// Replace the planning-time context frame set for this in-memory
    /// plan revision.
    pub fn set_context_frames(&mut self, context_frames: Vec<Uuid>) {
        self.context_frames = context_frames;
    }

    /// Return the immutable step definitions stored in this plan.
    pub fn steps(&self) -> &[PlanStep] {
        &self.steps
    }

    /// Append one logical step definition to this in-memory plan.
    pub fn add_step(&mut self, step: PlanStep) {
        self.steps.push(step);
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

    // Coverage:
    // - single-parent and multi-parent plan revision DAG behaviour
    // - parent deduplication and self-link rejection
    // - mixed-intent merge rejection
    // - plan-level frozen context frame assignment
    // - serde compatibility for step descriptions

    #[test]
    fn test_plan_revision_graph() {
        let actor = ActorRef::human("jackie").expect("actor");
        let intent_id = Uuid::from_u128(0x10);
        let plan_v1 = Plan::new(actor.clone(), intent_id).expect("plan");
        let plan_v2 = plan_v1.new_revision(actor.clone()).expect("plan");
        let plan_v2b = Plan::new_revision_from(actor.clone(), &plan_v1).expect("plan");
        let plan_v3 = Plan::new_revision_chain(actor, &[&plan_v2, &plan_v2b]).expect("plan");

        assert!(plan_v1.parents().is_empty());
        assert_eq!(plan_v2.parents(), &[plan_v1.header().object_id()]);
        assert_eq!(
            plan_v3.parents(),
            &[plan_v2.header().object_id(), plan_v2b.header().object_id()]
        );
        assert_eq!(plan_v3.intent(), intent_id);
    }

    #[test]
    fn test_plan_add_parent_dedupes_and_ignores_self() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut plan = Plan::new(actor, Uuid::from_u128(0x11)).expect("plan");
        let parent_a = Uuid::from_u128(0x41);
        let parent_b = Uuid::from_u128(0x42);

        plan.add_parent(parent_a);
        plan.add_parent(parent_a);
        plan.add_parent(parent_b);
        plan.add_parent(plan.header().object_id());

        assert_eq!(plan.parents(), &[parent_a, parent_b]);
    }

    #[test]
    fn test_plan_revision_chain_rejects_mixed_intents() {
        let actor = ActorRef::human("jackie").expect("actor");
        let plan_a = Plan::new(actor.clone(), Uuid::from_u128(0x100)).expect("plan");
        let plan_b = Plan::new(actor, Uuid::from_u128(0x200)).expect("plan");

        let err = Plan::new_revision_chain(
            ActorRef::human("jackie").expect("actor"),
            &[&plan_a, &plan_b],
        )
        .expect_err("mixed intents should fail");

        assert!(err.contains("same intent"));
    }

    #[test]
    fn test_plan_context_frames() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut plan = Plan::new(actor, Uuid::from_u128(0x12)).expect("plan");
        let frame_a = Uuid::from_u128(0x51);
        let frame_b = Uuid::from_u128(0x52);

        plan.set_context_frames(vec![frame_a, frame_b]);
        assert_eq!(plan.context_frames(), &[frame_a, frame_b]);
    }

    #[test]
    fn test_plan_step_serializes_description_field() {
        let step = PlanStep::new("run tests");
        let value = serde_json::to_value(&step).expect("serialize step");

        assert_eq!(
            value.get("description").and_then(|v| v.as_str()),
            Some("run tests")
        );
        assert!(value.get("step_id").is_some());
    }

    #[test]
    fn test_plan_step_deserializes_description_field() {
        let step_id = Uuid::from_u128(0x501);
        let step: PlanStep = serde_json::from_value(json!({
            "step_id": step_id,
            "description": "run tests"
        }))
        .expect("deserialize step");

        assert_eq!(step.step_id(), step_id);
        assert_eq!(step.description(), "run tests");
    }
}
