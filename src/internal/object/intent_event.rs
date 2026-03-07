//! Intent lifecycle event.
//!
//! `IntentEvent` records append-only lifecycle facts for an `Intent`.
//!
//! # How to use this object
//!
//! - Append an event when an intent is analyzed, completed, or
//!   cancelled.
//! - Include `result_commit` only when the lifecycle transition produced
//!   a repository commit.
//! - Keep the `Intent` snapshot immutable; lifecycle belongs here.
//!
//! # How it works with other objects
//!
//! - `IntentEvent.intent_id` attaches the event to an `Intent`.
//! - `Decision` and final repository actions may feed data such as
//!   `result_commit`.
//!
//! # How Libra should call it
//!
//! Libra should derive the current intent lifecycle state from the most
//! recent relevant `IntentEvent`, not by mutating the `Intent` object.

use std::fmt;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntentEventKind {
    Analyzed,
    Completed,
    Cancelled,
}

/// Append-only lifecycle fact for one `Intent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentEvent {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical target intent for this lifecycle fact.
    intent_id: Uuid,
    /// Lifecycle transition kind being recorded.
    kind: IntentEventKind,
    /// Optional human-readable explanation for the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// Optional resulting repository commit associated with the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_commit: Option<IntegrityHash>,
}

impl IntentEvent {
    /// Create a new lifecycle event for the given intent.
    pub fn new(
        created_by: ActorRef,
        intent_id: Uuid,
        kind: IntentEventKind,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::IntentEvent, created_by)?,
            intent_id,
            kind,
            reason: None,
            result_commit: None,
        })
    }

    /// Return the immutable header for this event.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical target intent id.
    pub fn intent_id(&self) -> Uuid {
        self.intent_id
    }

    /// Return the lifecycle transition kind.
    pub fn kind(&self) -> &IntentEventKind {
        &self.kind
    }

    /// Return the human-readable explanation, if present.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Return the resulting repository commit, if present.
    pub fn result_commit(&self) -> Option<&IntegrityHash> {
        self.result_commit.as_ref()
    }

    /// Set or clear the human-readable explanation.
    pub fn set_reason(&mut self, reason: Option<String>) {
        self.reason = reason;
    }

    /// Set or clear the resulting repository commit.
    pub fn set_result_commit(&mut self, result_commit: Option<IntegrityHash>) {
        self.result_commit = result_commit;
    }
}

impl fmt::Display for IntentEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntentEvent: {}", self.header.object_id())
    }
}

impl ObjectTrait for IntentEvent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::IntentEvent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute IntentEvent size: {}", e);
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
    // - completed intent event construction
    // - optional rationale and result-commit attachment

    #[test]
    fn test_intent_event_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let mut event = IntentEvent::new(actor, Uuid::from_u128(0x1), IntentEventKind::Completed)
            .expect("event");
        let hash = IntegrityHash::compute(b"commit");
        event.set_reason(Some("done".to_string()));
        event.set_result_commit(Some(hash));

        assert_eq!(event.kind(), &IntentEventKind::Completed);
        assert_eq!(event.reason(), Some("done"));
        assert_eq!(event.result_commit(), Some(&hash));
    }
}
