use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ai_hash::IntegrityHash,
    ai_header::{ActorRef, AiObjectType, Header},
};

/// Type of decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DecisionType {
    Commit,
    Checkpoint,
    Abandon,
    Retry,
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

/// Decision object linking process to outcomes.
/// Records the final outcome of a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    decision_type: DecisionType,
    chosen_patchset_id: Option<Uuid>,
    result_commit_sha: Option<IntegrityHash>,
    checkpoint_id: Option<String>,
    rationale: Option<String>,
}

impl Decision {
    /// Create a new decision object
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        decision_type: impl Into<DecisionType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(AiObjectType::Decision, repo_id, created_by)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decision_fields() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);
        let patchset_id = Uuid::from_u128(0x2);
        let expected_hash = IntegrityHash::compute(b"decision-hash");

        let mut decision = Decision::new(repo_id, actor, run_id, "commit").expect("decision");
        decision.set_chosen_patchset_id(Some(patchset_id));
        decision.set_result_commit_sha(Some(expected_hash));
        decision.set_rationale(Some("tests passed".to_string()));

        assert_eq!(decision.chosen_patchset_id(), Some(patchset_id));
        assert_eq!(decision.result_commit_sha(), Some(&expected_hash));
        assert_eq!(decision.rationale(), Some("tests passed"));
        assert_eq!(decision.decision_type(), &DecisionType::Commit);
    }
}
