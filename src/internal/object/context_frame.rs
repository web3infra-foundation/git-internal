//! Immutable context-frame event object.
//!
//! `ContextFrame` stores one durable piece of incremental workflow
//! context.
//!
//! # How to use this object
//!
//! - Create one frame whenever an incremental context fact should
//!   survive history: intent analysis, step summary, code change,
//!   checkpoint, tool call, or recovery note.
//! - Attach `intent_id`, `run_id`, `plan_id`, and `step_id` when known
//!   so the frame can be joined back to analysis or execution history.
//! - Persist each frame independently instead of mutating a shared
//!   pipeline object.
//!
//! # How it works with other objects
//!
//! - `Intent.analysis_context_frames` freezes the analysis-time context
//!   set used to derive one `IntentSpec` revision.
//! - `Plan.context_frames` freezes the planning-time context set.
//! - `PlanStepEvent.consumed_frames` and `produced_frames` express
//!   runtime context flow.
//! - Libra's live context window is a projection over stored frame IDs.
//!
//! # How Libra should call it
//!
//! Libra should store every durable context increment as its own
//! `ContextFrame`, then maintain a separate in-memory or database-backed
//! window of which frame IDs are currently active.

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
pub enum FrameKind {
    IntentAnalysis,
    StepSummary,
    CodeChange,
    SystemState,
    ErrorRecovery,
    Checkpoint,
    ToolCall,
    Other(String),
}

impl FrameKind {
    /// Return the canonical snake_case storage/display form for the
    /// frame kind.
    pub fn as_str(&self) -> &str {
        match self {
            FrameKind::IntentAnalysis => "intent_analysis",
            FrameKind::StepSummary => "step_summary",
            FrameKind::CodeChange => "code_change",
            FrameKind::SystemState => "system_state",
            FrameKind::ErrorRecovery => "error_recovery",
            FrameKind::Checkpoint => "checkpoint",
            FrameKind::ToolCall => "tool_call",
            FrameKind::Other(value) => value.as_str(),
        }
    }
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Immutable incremental context record.
///
/// A `ContextFrame` is append-only history, not a mutable slot in a
/// buffer. Current visibility of frames is a Libra concern.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextFrame {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Optional intent revision that emitted or owns this frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    intent_id: Option<Uuid>,
    /// Optional run that emitted or owns this frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_id: Option<Uuid>,
    /// Optional plan revision associated with this frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan_id: Option<Uuid>,
    /// Optional stable logical plan-step id associated with this frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    step_id: Option<Uuid>,
    /// Coarse semantic kind of context carried by this frame.
    kind: FrameKind,
    /// Human-readable short description of the context increment.
    summary: String,
    /// Optional structured payload with additional frame details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    /// Optional approximate token footprint for budgeting and retrieval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_estimate: Option<u64>,
}

impl ContextFrame {
    /// Create a new incremental context frame with the given kind and
    /// summary.
    pub fn new(
        created_by: ActorRef,
        kind: FrameKind,
        summary: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::ContextFrame, created_by)?,
            intent_id: None,
            run_id: None,
            plan_id: None,
            step_id: None,
            kind,
            summary: summary.into(),
            data: None,
            token_estimate: None,
        })
    }

    /// Return the immutable header for this context frame.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the associated intent id, if present.
    pub fn intent_id(&self) -> Option<Uuid> {
        self.intent_id
    }

    /// Return the associated run id, if present.
    pub fn run_id(&self) -> Option<Uuid> {
        self.run_id
    }

    /// Return the associated plan id, if present.
    pub fn plan_id(&self) -> Option<Uuid> {
        self.plan_id
    }

    /// Return the associated stable plan-step id, if present.
    pub fn step_id(&self) -> Option<Uuid> {
        self.step_id
    }

    /// Return the semantic frame kind.
    pub fn kind(&self) -> &FrameKind {
        &self.kind
    }

    /// Return the short human-readable frame summary.
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Return the structured payload, if present.
    pub fn data(&self) -> Option<&serde_json::Value> {
        self.data.as_ref()
    }

    /// Return the approximate token footprint, if present.
    pub fn token_estimate(&self) -> Option<u64> {
        self.token_estimate
    }

    /// Set or clear the associated intent id.
    pub fn set_intent_id(&mut self, intent_id: Option<Uuid>) {
        self.intent_id = intent_id;
    }

    /// Set or clear the associated run id.
    pub fn set_run_id(&mut self, run_id: Option<Uuid>) {
        self.run_id = run_id;
    }

    /// Set or clear the associated plan id.
    pub fn set_plan_id(&mut self, plan_id: Option<Uuid>) {
        self.plan_id = plan_id;
    }

    /// Set or clear the associated stable plan-step id.
    pub fn set_step_id(&mut self, step_id: Option<Uuid>) {
        self.step_id = step_id;
    }

    /// Set or clear the structured payload.
    pub fn set_data(&mut self, data: Option<serde_json::Value>) {
        self.data = data;
    }

    /// Set or clear the approximate token footprint.
    pub fn set_token_estimate(&mut self, token_estimate: Option<u64>) {
        self.token_estimate = token_estimate;
    }
}

impl fmt::Display for ContextFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContextFrame: {}", self.header.object_id())
    }
}

impl ObjectTrait for ContextFrame {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidContextFrameObject(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::ContextFrame
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute ContextFrame size: {}", e);
                0
            }
        }
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidContextFrameObject(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Coverage:
    // - intent/run/plan/step association links
    // - frame payload storage
    // - token estimate capture

    #[test]
    fn test_context_frame_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let mut frame =
            ContextFrame::new(actor, FrameKind::StepSummary, "Updated API").expect("frame");
        let intent_id = Uuid::from_u128(0x0f);
        let run_id = Uuid::from_u128(0x10);
        let plan_id = Uuid::from_u128(0x11);
        let step_id = Uuid::from_u128(0x12);

        frame.set_intent_id(Some(intent_id));
        frame.set_run_id(Some(run_id));
        frame.set_plan_id(Some(plan_id));
        frame.set_step_id(Some(step_id));
        frame.set_data(Some(serde_json::json!({"files": ["src/lib.rs"]})));
        frame.set_token_estimate(Some(128));

        assert_eq!(frame.intent_id(), Some(intent_id));
        assert_eq!(frame.run_id(), Some(run_id));
        assert_eq!(frame.plan_id(), Some(plan_id));
        assert_eq!(frame.step_id(), Some(step_id));
        assert_eq!(frame.kind(), &FrameKind::StepSummary);
        assert_eq!(frame.token_estimate(), Some(128));
    }
}
