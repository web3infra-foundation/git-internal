//! AI Evidence Definition
//!
//! `Evidence` represents the result of a validation or quality assurance step, such as
//! running tests, linting code, or building artifacts.
//!
//! # Purpose
//!
//! - **Validation**: Proves that a patchset works as expected.
//! - **Feedback**: Provides error messages and logs to the agent so it can fix issues.
//! - **Decision Support**: Used by the `Decision` object to justify committing or rejecting changes.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ai_header::{ActorRef, AiObjectType, ArtifactRef, Header};

/// Kind of evidence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Unit, integration, or e2e tests.
    Test,
    /// Static analysis results.
    Lint,
    /// Compilation or build results.
    Build,
    #[serde(untagged)]
    Other(String),
}

impl fmt::Display for EvidenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvidenceKind::Test => write!(f, "test"),
            EvidenceKind::Lint => write!(f, "lint"),
            EvidenceKind::Build => write!(f, "build"),
            EvidenceKind::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<String> for EvidenceKind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "test" => EvidenceKind::Test,
            "lint" => EvidenceKind::Lint,
            "build" => EvidenceKind::Build,
            _ => EvidenceKind::Other(s),
        }
    }
}

impl From<&str> for EvidenceKind {
    fn from(s: &str) -> Self {
        match s {
            "test" => EvidenceKind::Test,
            "lint" => EvidenceKind::Lint,
            "build" => EvidenceKind::Build,
            _ => EvidenceKind::Other(s.to_string()),
        }
    }
}

/// Evidence object for test/lint/build results.
/// Links tooling output back to a run or patchset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    patchset_id: Option<Uuid>,
    kind: EvidenceKind,
    tool: String,
    command: Option<String>,
    exit_code: Option<i32>,
    summary: Option<String>, // passed/failed, error signature
    #[serde(default)]
    report_artifacts: Vec<ArtifactRef>,
}

impl Evidence {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        kind: impl Into<EvidenceKind>,
        tool: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(AiObjectType::Evidence, repo_id, created_by)?,
            run_id,
            patchset_id: None,
            kind: kind.into(),
            tool: tool.into(),
            command: None,
            exit_code: None,
            summary: None,
            report_artifacts: Vec::new(),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn patchset_id(&self) -> Option<Uuid> {
        self.patchset_id
    }

    pub fn kind(&self) -> &EvidenceKind {
        &self.kind
    }

    pub fn tool(&self) -> &str {
        &self.tool
    }

    pub fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    pub fn report_artifacts(&self) -> &[ArtifactRef] {
        &self.report_artifacts
    }

    pub fn set_patchset_id(&mut self, patchset_id: Option<Uuid>) {
        self.patchset_id = patchset_id;
    }

    pub fn set_command(&mut self, command: Option<String>) {
        self.command = command;
    }

    pub fn set_exit_code(&mut self, exit_code: Option<i32>) {
        self.exit_code = exit_code;
    }

    pub fn set_summary(&mut self, summary: Option<String>) {
        self.summary = summary;
    }

    pub fn add_report_artifact(&mut self, artifact: ArtifactRef) {
        self.report_artifacts.push(artifact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evidence_fields() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);
        let patchset_id = Uuid::from_u128(0x2);

        let mut evidence =
            Evidence::new(repo_id, actor, run_id, "test", "cargo").expect("evidence");
        evidence.set_patchset_id(Some(patchset_id));
        evidence.set_exit_code(Some(1));
        evidence.add_report_artifact(ArtifactRef::new("local", "log.txt").expect("artifact"));

        assert_eq!(evidence.patchset_id(), Some(patchset_id));
        assert_eq!(evidence.exit_code(), Some(1));
        assert_eq!(evidence.report_artifacts().len(), 1);
        assert_eq!(evidence.kind(), &EvidenceKind::Test);
    }
}
