//! Task lifecycle event.
//!
//! `TaskEvent` records append-only lifecycle changes for a `Task`.
//!
//! # How to use this object
//!
//! - Append events as the scheduler creates, starts, blocks, finishes,
//!   fails, or cancels task execution.
//! - Set `run_id` when the event is associated with a concrete `Run`.
//! - Keep the `Task` snapshot itself stable.
//!
//! # How it works with other objects
//!
//! - `TaskEvent.task_id` points at the durable task definition.
//! - `run_id` optionally links the state change to a specific execution
//!   attempt.
//!
//! # How Libra should call it
//!
//! Libra should reconstruct current task status from event history and
//! scheduler state instead of storing mutable task status inside
//! `Task`.

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
pub enum TaskEventKind {
    Created,
    Running,
    Blocked,
    Done,
    Failed,
    Cancelled,
}

/// Append-only lifecycle fact for one `Task`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEvent {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical target task for this lifecycle fact.
    task_id: Uuid,
    /// Lifecycle transition kind being recorded.
    kind: TaskEventKind,
    /// Optional human-readable explanation for the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// Optional run associated with the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_id: Option<Uuid>,
}

impl TaskEvent {
    /// Create a new lifecycle event for the given task.
    pub fn new(created_by: ActorRef, task_id: Uuid, kind: TaskEventKind) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::TaskEvent, created_by)?,
            task_id,
            kind,
            reason: None,
            run_id: None,
        })
    }

    /// Return the immutable header for this event.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical target task id.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// Return the lifecycle transition kind.
    pub fn kind(&self) -> &TaskEventKind {
        &self.kind
    }

    /// Return the human-readable explanation, if present.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Return the associated run id, if present.
    pub fn run_id(&self) -> Option<Uuid> {
        self.run_id
    }

    /// Set or clear the human-readable explanation.
    pub fn set_reason(&mut self, reason: Option<String>) {
        self.reason = reason;
    }

    /// Set or clear the associated run id.
    pub fn set_run_id(&mut self, run_id: Option<Uuid>) {
        self.run_id = run_id;
    }
}

impl fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaskEvent: {}", self.header.object_id())
    }
}

impl ObjectTrait for TaskEvent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::TaskEvent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute TaskEvent size: {}", e);
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
    // - task-event creation for running state
    // - optional rationale and associated run linking

    #[test]
    fn test_task_event_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let mut event =
            TaskEvent::new(actor, Uuid::from_u128(0x1), TaskEventKind::Running).expect("event");
        let run_id = Uuid::from_u128(0x2);
        event.set_reason(Some("agent started".to_string()));
        event.set_run_id(Some(run_id));

        assert_eq!(event.kind(), &TaskEventKind::Running);
        assert_eq!(event.reason(), Some("agent started"));
        assert_eq!(event.run_id(), Some(run_id));
    }
}
