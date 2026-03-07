//! Plan-step execution event.
//!
//! `PlanStepEvent` is the runtime bridge between immutable planning
//! structure and actual execution.
//!
//! # How to use this object
//!
//! - Append a new event whenever a logical plan step changes execution
//!   state inside a run.
//! - Use `consumed_frames` and `produced_frames` to document context
//!   flow.
//! - Set `spawned_task_id` when the step delegates work to a durable
//!   `Task`.
//! - Set `outputs` when the step produced structured runtime output.
//!
//! # How it works with other objects
//!
//! - `plan_id` points to the immutable `Plan` revision.
//! - `step_id` points to the stable `PlanStep.step_id`.
//! - `run_id` ties the execution fact to the specific attempt.
//! - `ContextFrame` IDs describe what context the step consumed and
//!   produced.
//!
//! # How Libra should call it
//!
//! Libra should reconstruct current step state from the ordered
//! `PlanStepEvent` stream rather than mutating `PlanStep` inside the
//! stored plan snapshot.

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus {
    Pending,
    Progressing,
    Completed,
    Failed,
    Skipped,
}

impl PlanStepStatus {
    /// Return the canonical snake_case storage/display form for the step
    /// status.
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanStepStatus::Pending => "pending",
            PlanStepStatus::Progressing => "progressing",
            PlanStepStatus::Completed => "completed",
            PlanStepStatus::Failed => "failed",
            PlanStepStatus::Skipped => "skipped",
        }
    }
}

impl fmt::Display for PlanStepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Append-only execution fact for one logical plan step in one `Run`.
///
/// The pair `(plan_id, step_id)` identifies the logical step revision,
/// while `run_id` identifies the execution attempt that produced this
/// event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStepEvent {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Immutable plan revision that owns the referenced logical step.
    plan_id: Uuid,
    /// Stable logical step id inside the owning plan family.
    step_id: Uuid,
    /// Concrete execution attempt that produced this step event.
    run_id: Uuid,
    /// Runtime status recorded for the step at this point in the run.
    status: PlanStepStatus,
    /// Optional human-readable explanation for this status transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// Context frame ids consumed while executing the step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    consumed_frames: Vec<Uuid>,
    /// Context frame ids produced while executing the step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    produced_frames: Vec<Uuid>,
    /// Optional durable task spawned from this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spawned_task_id: Option<Uuid>,
    /// Optional structured runtime outputs produced by the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outputs: Option<serde_json::Value>,
}

impl PlanStepEvent {
    /// Create a new execution event for one logical plan step inside one
    /// run.
    pub fn new(
        created_by: ActorRef,
        plan_id: Uuid,
        step_id: Uuid,
        run_id: Uuid,
        status: PlanStepStatus,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::PlanStepEvent, created_by)?,
            plan_id,
            step_id,
            run_id,
            status,
            reason: None,
            consumed_frames: Vec::new(),
            produced_frames: Vec::new(),
            spawned_task_id: None,
            outputs: None,
        })
    }

    /// Return the immutable header for this event.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the owning plan revision id.
    pub fn plan_id(&self) -> Uuid {
        self.plan_id
    }

    /// Return the stable logical step id.
    pub fn step_id(&self) -> Uuid {
        self.step_id
    }

    /// Return the concrete execution attempt id.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Return the recorded runtime status.
    pub fn status(&self) -> &PlanStepStatus {
        &self.status
    }

    /// Return the human-readable explanation, if present.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Return the context frame ids consumed by the step.
    pub fn consumed_frames(&self) -> &[Uuid] {
        &self.consumed_frames
    }

    /// Return the context frame ids produced by the step.
    pub fn produced_frames(&self) -> &[Uuid] {
        &self.produced_frames
    }

    /// Return the durable spawned task id, if present.
    pub fn spawned_task_id(&self) -> Option<Uuid> {
        self.spawned_task_id
    }

    /// Return the structured runtime outputs, if present.
    pub fn outputs(&self) -> Option<&serde_json::Value> {
        self.outputs.as_ref()
    }

    /// Set or clear the human-readable explanation.
    pub fn set_reason(&mut self, reason: Option<String>) {
        self.reason = reason;
    }

    /// Replace the consumed context frame set.
    pub fn set_consumed_frames(&mut self, consumed_frames: Vec<Uuid>) {
        self.consumed_frames = consumed_frames;
    }

    /// Replace the produced context frame set.
    pub fn set_produced_frames(&mut self, produced_frames: Vec<Uuid>) {
        self.produced_frames = produced_frames;
    }

    /// Set or clear the durable spawned task id.
    pub fn set_spawned_task_id(&mut self, spawned_task_id: Option<Uuid>) {
        self.spawned_task_id = spawned_task_id;
    }

    /// Set or clear the structured runtime outputs.
    pub fn set_outputs(&mut self, outputs: Option<serde_json::Value>) {
        self.outputs = outputs;
    }
}

impl fmt::Display for PlanStepEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlanStepEvent: {}", self.header.object_id())
    }
}

impl ObjectTrait for PlanStepEvent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::PlanStepEvent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute PlanStepEvent size: {}", e);
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

    // Coverage:
    // - completed step-event creation
    // - consumed/produced context frame flow
    // - spawned task linkage and structured outputs

    #[test]
    fn test_plan_step_event_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let mut event = PlanStepEvent::new(
            actor,
            Uuid::from_u128(0x1),
            Uuid::from_u128(0x2),
            Uuid::from_u128(0x3),
            PlanStepStatus::Completed,
        )
        .expect("event");
        let frame_a = Uuid::from_u128(0x10);
        let frame_b = Uuid::from_u128(0x11);
        let task_id = Uuid::from_u128(0x20);

        event.set_reason(Some("done".to_string()));
        event.set_consumed_frames(vec![frame_a]);
        event.set_produced_frames(vec![frame_b]);
        event.set_spawned_task_id(Some(task_id));
        event.set_outputs(Some(serde_json::json!({"files": ["src/lib.rs"]})));

        assert_eq!(event.status(), &PlanStepStatus::Completed);
        assert_eq!(event.reason(), Some("done"));
        assert_eq!(event.consumed_frames(), &[frame_a]);
        assert_eq!(event.produced_frames(), &[frame_b]);
        assert_eq!(event.spawned_task_id(), Some(task_id));
    }
}
