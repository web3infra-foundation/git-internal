//! AI Decision Definition
//!
//! A `Decision` is the **terminal verdict** of a [`Run`](super::run::Run).
//! After the agent finishes generating and validating PatchSets, the
//! orchestrator (or the agent itself) creates a Decision to record what
//! should happen next.
//!
//! # Position in Lifecycle
//!
//! ```text
//! Task ──runs──▶ Run ──patchsets──▶ [PatchSet₀, PatchSet₁, ...]
//!                  │
//!                  └──(terminal)──▶ Decision
//!                                     ├── chosen_patchset ──▶ PatchSet
//!                                     └── evidence (via Evidence.decision)
//! ```
//!
//! A Decision is created **once per Run**, at the end of execution.
//! It selects which PatchSet (if any) to apply and records the
//! resulting commit hash. [`Evidence`](super::evidence::Evidence)
//! objects may reference the Decision to provide supporting data
//! (test results, lint reports) that justified the verdict.
//!
//! # Decision Types
//!
//! - **`Commit`**: Accept the chosen PatchSet and apply it to the
//!   repository. `chosen_patchset` and `result_commit` should be set.
//! - **`Checkpoint`**: Save intermediate progress without finishing.
//!   The Run may continue or be resumed later. `checkpoint_id`
//!   identifies the saved state.
//! - **`Abandon`**: Give up on the Task. The goal is deemed impossible
//!   or not worth pursuing. No PatchSet is applied.
//! - **`Retry`**: The current attempt failed but the Task is still
//!   viable. The orchestrator should create a new Run to try again,
//!   potentially with different parameters or prompts.
//! - **`Rollback`**: Revert previously applied changes. Used when a
//!   committed PatchSet is later found to be incorrect.
//!
//! # Flow
//!
//! ```text
//!   Run completes
//!        │
//!        ▼
//!   Orchestrator creates Decision
//!        │
//!        ├─ Commit ──▶ apply PatchSet, record result_commit
//!        ├─ Checkpoint ──▶ save state, record checkpoint_id
//!        ├─ Abandon ──▶ mark Task as Failed
//!        ├─ Retry ──▶ create new Run for same Task
//!        └─ Rollback ──▶ revert applied PatchSet
//! ```

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

/// Type of decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DecisionType {
    /// Approve and commit changes.
    Commit,
    /// Save intermediate progress.
    Checkpoint,
    /// Give up on the task.
    Abandon,
    /// Try again (re-run).
    Retry,
    /// Revert applied changes.
    Rollback,
    #[serde(untagged)]
    Other(String),
}

impl fmt::Display for DecisionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecisionType::Commit => write!(f, "commit"),
            DecisionType::Checkpoint => write!(f, "checkpoint"),
            DecisionType::Abandon => write!(f, "abandon"),
            DecisionType::Retry => write!(f, "retry"),
            DecisionType::Rollback => write!(f, "rollback"),
            DecisionType::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<String> for DecisionType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "commit" => DecisionType::Commit,
            "checkpoint" => DecisionType::Checkpoint,
            "abandon" => DecisionType::Abandon,
            "retry" => DecisionType::Retry,
            "rollback" => DecisionType::Rollback,
            _ => DecisionType::Other(s),
        }
    }
}

impl From<&str> for DecisionType {
    fn from(s: &str) -> Self {
        match s {
            "commit" => DecisionType::Commit,
            "checkpoint" => DecisionType::Checkpoint,
            "abandon" => DecisionType::Abandon,
            "retry" => DecisionType::Retry,
            "rollback" => DecisionType::Rollback,
            _ => DecisionType::Other(s.to_string()),
        }
    }
}

/// Terminal verdict of a [`Run`](super::run::Run).
///
/// Created once per Run at the end of execution. See module
/// documentation for lifecycle position and decision type semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Run`](super::run::Run) this Decision concludes.
    ///
    /// Every Decision belongs to exactly one Run. The Run does not
    /// store a back-reference; lookup is done by scanning or indexing.
    run_id: Uuid,
    /// The verdict: what should happen as a result of this Run.
    ///
    /// See [`DecisionType`] variants for semantics. The orchestrator
    /// inspects this field to determine the next action (apply patch,
    /// retry, abandon, etc.).
    decision_type: DecisionType,
    /// The [`PatchSet`](super::patchset::PatchSet) selected for
    /// application.
    ///
    /// Set when `decision_type` is `Commit` — identifies which
    /// PatchSet from `Run.patchsets` was chosen. `None` for
    /// `Abandon`, `Retry`, `Rollback`, or when no suitable PatchSet
    /// exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chosen_patchset_id: Option<Uuid>,
    /// Git commit hash produced after applying the chosen PatchSet.
    ///
    /// Set by the orchestrator after a successful `git commit`.
    /// `None` until the PatchSet is actually committed, or when the
    /// decision does not involve applying changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_commit_sha: Option<IntegrityHash>,
    /// Opaque identifier for a saved checkpoint.
    ///
    /// Set when `decision_type` is `Checkpoint`. The format and
    /// resolution of the ID are defined by the orchestrator (e.g.
    /// a snapshot name, a storage key). `None` for all other
    /// decision types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_id: Option<String>,
    /// Human-readable explanation of why this decision was made.
    ///
    /// Written by the agent or orchestrator to justify the verdict.
    /// For `Commit`: summarises why the chosen PatchSet is correct.
    /// For `Abandon`/`Retry`: explains what went wrong.
    /// For `Rollback`: describes the defect that triggered reversion.
    /// `None` if no explanation was provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rationale: Option<String>,
}

impl Decision {
    /// Create a new decision object
    pub fn new(
        created_by: ActorRef,
        run_id: Uuid,
        decision_type: impl Into<DecisionType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Decision, created_by)?,
            run_id,
            decision_type: decision_type.into(),
            chosen_patchset_id: None,
            result_commit_sha: None,
            checkpoint_id: None,
            rationale: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn decision_type(&self) -> &DecisionType {
        &self.decision_type
    }

    pub fn chosen_patchset_id(&self) -> Option<Uuid> {
        self.chosen_patchset_id
    }

    pub fn result_commit_sha(&self) -> Option<&IntegrityHash> {
        self.result_commit_sha.as_ref()
    }

    pub fn checkpoint_id(&self) -> Option<&str> {
        self.checkpoint_id.as_deref()
    }

    pub fn rationale(&self) -> Option<&str> {
        self.rationale.as_deref()
    }

    pub fn set_chosen_patchset_id(&mut self, chosen_patchset_id: Option<Uuid>) {
        self.chosen_patchset_id = chosen_patchset_id;
    }

    pub fn set_result_commit_sha(&mut self, result_commit_sha: Option<IntegrityHash>) {
        self.result_commit_sha = result_commit_sha;
    }

    pub fn set_checkpoint_id(&mut self, checkpoint_id: Option<String>) {
        self.checkpoint_id = checkpoint_id;
    }

    pub fn set_rationale(&mut self, rationale: Option<String>) {
        self.rationale = rationale;
    }
}

impl fmt::Display for Decision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decision: {}", self.header.object_id())
    }
}

impl ObjectTrait for Decision {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Decision
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Decision size: {}", e);
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

    #[test]
    fn test_decision_fields() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);
        let patchset_id = Uuid::from_u128(0x2);
        let expected_hash = IntegrityHash::compute(b"decision-hash");

        let mut decision = Decision::new(actor, run_id, "commit").expect("decision");
        decision.set_chosen_patchset_id(Some(patchset_id));
        decision.set_result_commit_sha(Some(expected_hash));
        decision.set_rationale(Some("tests passed".to_string()));

        assert_eq!(decision.chosen_patchset_id(), Some(patchset_id));
        assert_eq!(decision.result_commit_sha(), Some(&expected_hash));
        assert_eq!(decision.rationale(), Some("tests passed"));
        assert_eq!(decision.decision_type(), &DecisionType::Commit);
    }
}
