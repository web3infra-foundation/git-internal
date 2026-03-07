//! Run lifecycle event.
//!
//! `RunEvent` records append-only execution-phase facts for a `Run`.
//!
//! # How to use this object
//!
//! - Append events as a run moves through creation, patching,
//!   validation, checkpoints, completion, or failure.
//! - Attach `error`, `metrics`, and `patchset_id` when they belong to
//!   that phase transition.
//! - Do not mutate the `Run` snapshot to reflect phase changes.
//!
//! # How it works with other objects
//!
//! - `RunEvent.run_id` points at the execution envelope.
//! - `patchset_id` can associate a run-phase fact with a candidate
//!   patchset.
//! - `Decision` normally appears after the terminal run events.
//!
//! # How Libra should call it
//!
//! Libra should derive the current run phase from the latest events and
//! scheduler state, while treating `Run` itself as the immutable attempt
//! record.

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
pub enum RunEventKind {
    Created,
    Patching,
    Validating,
    Completed,
    Failed,
    Checkpointed,
}

/// Append-only execution-phase fact for one `Run`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical target run for this execution-phase fact.
    run_id: Uuid,
    /// Execution-phase transition kind being recorded.
    kind: RunEventKind,
    /// Optional human-readable explanation of the phase change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// Optional human-readable error summary for failure cases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Optional structured metrics captured for this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metrics: Option<serde_json::Value>,
    /// Optional patchset associated with this run-phase fact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    patchset_id: Option<Uuid>,
}

impl RunEvent {
    /// Create a new execution-phase event for the given run.
    pub fn new(created_by: ActorRef, run_id: Uuid, kind: RunEventKind) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::RunEvent, created_by)?,
            run_id,
            kind,
            reason: None,
            error: None,
            metrics: None,
            patchset_id: None,
        })
    }

    /// Return the immutable header for this event.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical target run id.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Return the execution-phase transition kind.
    pub fn kind(&self) -> &RunEventKind {
        &self.kind
    }

    /// Return the human-readable reason, if present.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Return the human-readable error message, if present.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Return structured metrics, if present.
    pub fn metrics(&self) -> Option<&serde_json::Value> {
        self.metrics.as_ref()
    }

    /// Return the associated patchset id, if present.
    pub fn patchset_id(&self) -> Option<Uuid> {
        self.patchset_id
    }

    /// Set or clear the human-readable reason.
    pub fn set_reason(&mut self, reason: Option<String>) {
        self.reason = reason;
    }

    /// Set or clear the human-readable error message.
    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }

    /// Set or clear the structured metrics payload.
    pub fn set_metrics(&mut self, metrics: Option<serde_json::Value>) {
        self.metrics = metrics;
    }

    /// Set or clear the associated patchset id.
    pub fn set_patchset_id(&mut self, patchset_id: Option<Uuid>) {
        self.patchset_id = patchset_id;
    }
}

impl fmt::Display for RunEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RunEvent: {}", self.header.object_id())
    }
}

impl ObjectTrait for RunEvent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::RunEvent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute RunEvent size: {}", e);
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
    // - failed run-event creation
    // - rationale, error message, metrics, and patchset association

    #[test]
    fn test_run_event_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let mut event =
            RunEvent::new(actor, Uuid::from_u128(0x1), RunEventKind::Failed).expect("event");
        let patchset_id = Uuid::from_u128(0x2);
        event.set_reason(Some("validation failed".to_string()));
        event.set_error(Some("cargo test failed".to_string()));
        event.set_metrics(Some(serde_json::json!({"duration_ms": 1200})));
        event.set_patchset_id(Some(patchset_id));

        assert_eq!(event.kind(), &RunEventKind::Failed);
        assert_eq!(event.reason(), Some("validation failed"));
        assert_eq!(event.error(), Some("cargo test failed"));
        assert_eq!(event.patchset_id(), Some(patchset_id));
    }
}
