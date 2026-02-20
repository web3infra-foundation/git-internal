//! AI Evidence Definition
//!
//! An `Evidence` captures the output of a single validation or quality
//! assurance step — running tests, linting code, compiling the project,
//! etc. It is the objective data that supports (or contradicts) the
//! agent's proposed changes.
//!
//! # Position in Lifecycle
//!
//! ```text
//! Run ──patchsets──▶ [PatchSet₀, PatchSet₁, ...]
//!  │                       │
//!  │                       ▼
//!  └──────────────▶ Evidence (run_id + optional patchset_id)
//!                       │
//!                       ▼
//!                   Decision (uses Evidence to justify verdict)
//! ```
//!
//! Evidence is produced **during** a Run, typically after a PatchSet is
//! generated. The orchestrator runs validation tools against the
//! PatchSet and creates one Evidence per tool invocation. A single
//! PatchSet may have multiple Evidence objects (e.g. test + lint +
//! build). Evidence that is not tied to a specific PatchSet (e.g. a
//! pre-run environment check) sets `patchset_id` to `None`.
//!
//! # Purpose
//!
//! - **Validation**: Proves that a PatchSet works as expected (tests
//!   pass, code compiles, lint clean).
//! - **Feedback**: Provides error messages, logs, and exit codes to the
//!   agent so it can fix issues and produce a better PatchSet.
//! - **Decision Support**: The [`Decision`](super::decision::Decision)
//!   references Evidence to justify committing or rejecting changes.
//!   Reviewers can inspect Evidence to understand why a verdict was made.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        types::{ActorRef, ArtifactRef, Header, ObjectType},
    },
};

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

/// Output of a single validation step (test, lint, build, etc.).
///
/// One Evidence per tool invocation. Multiple Evidence objects may
/// exist for the same PatchSet (one per validation tool). See module
/// documentation for lifecycle position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Run`](super::run::Run) during which this validation was
    /// performed. Every Evidence belongs to exactly one Run.
    run_id: Uuid,
    /// The [`PatchSet`](super::patchset::PatchSet) being validated.
    ///
    /// `None` for run-level checks that are not specific to any
    /// PatchSet (e.g. environment health check before patching starts).
    /// When set, the Evidence applies to that specific PatchSet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    patchset_id: Option<Uuid>,
    /// Category of validation performed.
    ///
    /// `Test` for unit/integration/e2e tests, `Lint` for static
    /// analysis, `Build` for compilation. `Other(String)` for
    /// categories not covered by the predefined variants.
    kind: EvidenceKind,
    /// Name of the tool that produced this evidence (e.g. "cargo",
    /// "eslint", "pytest"). Used for display and filtering.
    tool: String,
    /// Full command line that was executed (e.g. "cargo test --release").
    ///
    /// `None` if the tool was invoked programmatically without a
    /// shell command. Useful for reproducibility — a reviewer can
    /// re-run the exact same command locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    /// Process exit code returned by the tool.
    ///
    /// `0` typically means success; non-zero means failure. `None` if
    /// the tool did not produce an exit code (e.g. an in-process check).
    /// The orchestrator uses this as a quick pass/fail signal before
    /// parsing the full report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    /// Short human-readable summary of the result.
    ///
    /// Typically a one-liner like "42 tests passed", "3 lint errors",
    /// or an error signature extracted from the output. `None` if no
    /// summary was produced. For full output, see `report_artifacts`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    /// References to full report files in object storage.
    ///
    /// May include log files, HTML coverage reports, JUnit XML, etc.
    /// Each [`ArtifactRef`] points to one stored file. The list is
    /// empty when the tool produced no persistent output, or when the
    /// output is captured entirely in `summary`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    report_artifacts: Vec<ArtifactRef>,
}

impl Evidence {
    pub fn new(
        created_by: ActorRef,
        run_id: Uuid,
        kind: impl Into<EvidenceKind>,
        tool: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Evidence, created_by)?,
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

impl fmt::Display for Evidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Evidence: {}", self.header.object_id())
    }
}

impl ObjectTrait for Evidence {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Evidence
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Evidence size: {}", e);
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
    fn test_evidence_fields() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);
        let patchset_id = Uuid::from_u128(0x2);

        let mut evidence = Evidence::new(actor, run_id, "test", "cargo").expect("evidence");
        evidence.set_patchset_id(Some(patchset_id));
        evidence.set_exit_code(Some(1));
        evidence.add_report_artifact(ArtifactRef::new("local", "log.txt").expect("artifact"));

        assert_eq!(evidence.patchset_id(), Some(patchset_id));
        assert_eq!(evidence.exit_code(), Some(1));
        assert_eq!(evidence.report_artifacts().len(), 1);
        assert_eq!(evidence.kind(), &EvidenceKind::Test);
    }
}
