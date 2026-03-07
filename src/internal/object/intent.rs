//! AI Intent snapshot.
//!
//! `Intent` is the immutable entry point of the agent workflow. It
//! captures one revision of the user's request plus the optional
//! analyzed `IntentSpec`.
//!
//! # How to use this object
//!
//! - Create a root `Intent` when Libra accepts a new user request.
//! - Create a new `Intent` revision when the request is refined,
//!   branched, or merged; link earlier revisions through `parents`.
//! - Fill `spec` before persistence if analysis has already produced a
//!   structured request.
//!
//! # How it works with other objects
//!
//! - `Plan.intent` points back to the `Intent` that the plan belongs to.
//! - `Task.intent` may point back to the originating `Intent`.
//! - `IntentEvent` records lifecycle facts such as analyzed /
//!   completed / cancelled.
//!
//! # How Libra should call it
//!
//! Libra should persist a new `Intent` for every semantic revision of
//! the request, then keep "current thread head", "selected plan", and
//! other mutable session state in Libra projections rather than on the
//! `Intent` object itself.

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

/// Structured request payload derived from the free-form prompt.
///
/// `IntentSpec` remains intentionally schema-agnostic at the storage
/// layer. Libra can impose additional application-level conventions on
/// top of the raw JSON payload.
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

/// Immutable request/spec revision.
///
/// One stored `Intent` answers "what request revision existed here?".
/// It does not answer "what is the current thread head?" or "which plan
/// is currently selected?" because those are Libra projection concerns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Parent intent revisions that this revision directly derives from.
    ///
    /// Multiple parents allow merge-style intent history similar to a
    /// commit DAG.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parents: Vec<Uuid>,
    /// Original free-form user request captured for this revision.
    prompt: String,
    /// Structured interpretation of `prompt`, when Libra or an agent has
    /// already produced one at persistence time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spec: Option<IntentSpec>,
}

impl Intent {
    /// Create a new root intent revision from a free-form user prompt.
    pub fn new(created_by: ActorRef, prompt: impl Into<String>) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Intent, created_by)?,
            parents: Vec::new(),
            prompt: prompt.into(),
            spec: None,
        })
    }

    /// Create a new intent revision from a single parent intent.
    ///
    /// This is the common helper for linear refinement.
    pub fn new_revision_from(
        created_by: ActorRef,
        prompt: impl Into<String>,
        parent: &Self,
    ) -> Result<Self, String> {
        Self::new_revision_chain(created_by, prompt, &[parent.header.object_id()])
    }

    /// Create a new intent revision from multiple parent intents.
    ///
    /// Use this when Libra merges several prior intent branches into a
    /// new request/spec revision.
    pub fn new_revision_chain(
        created_by: ActorRef,
        prompt: impl Into<String>,
        parent_ids: &[Uuid],
    ) -> Result<Self, String> {
        let mut intent = Self::new(created_by, prompt)?;
        for id in parent_ids {
            intent.add_parent(*id);
        }
        Ok(intent)
    }

    /// Return the immutable header for this intent revision.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the direct parent intent ids of this revision.
    pub fn parents(&self) -> &[Uuid] {
        &self.parents
    }

    /// Return the original user prompt stored on this revision.
    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    /// Return the structured request payload, if one was stored.
    pub fn spec(&self) -> Option<&IntentSpec> {
        self.spec.as_ref()
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

    /// Replace the parent set for this in-memory revision before
    /// persistence.
    pub fn set_parents(&mut self, parents: Vec<Uuid>) {
        self.parents = parents;
    }

    /// Set or clear the structured spec for this in-memory revision.
    pub fn set_spec(&mut self, spec: Option<IntentSpec>) {
        self.spec = spec;
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

    // Coverage:
    // - root intent construction defaults
    // - revision graph creation for single-parent and multi-parent flows
    // - structured spec assignment before persistence

    #[test]
    fn test_intent_creation() {
        let actor = ActorRef::human("jackie").expect("actor");
        let intent = Intent::new(actor, "Add pagination").expect("intent");

        assert_eq!(intent.prompt(), "Add pagination");
        assert!(intent.parents().is_empty());
        assert!(intent.spec().is_none());
    }

    #[test]
    fn test_intent_revision_graph() {
        let actor = ActorRef::human("jackie").expect("actor");
        let root = Intent::new(actor.clone(), "A").expect("intent");
        let branch_a = Intent::new_revision_from(actor.clone(), "B", &root).expect("intent");
        let branch_b = Intent::new_revision_chain(
            actor,
            "C",
            &[root.header().object_id(), branch_a.header().object_id()],
        )
        .expect("intent");

        assert_eq!(branch_a.parents(), &[root.header().object_id()]);
        assert_eq!(
            branch_b.parents(),
            &[root.header().object_id(), branch_a.header().object_id()]
        );
    }

    #[test]
    fn test_spec_assignment() {
        let actor = ActorRef::human("jackie").expect("actor");
        let mut intent = Intent::new(actor, "A").expect("intent");
        intent.set_spec(Some("structured spec".into()));
        assert_eq!(intent.spec(), Some(&IntentSpec::from("structured spec")));
    }
}
