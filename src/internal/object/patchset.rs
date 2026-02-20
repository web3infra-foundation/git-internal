//! AI PatchSet Definition
//!
//! A `PatchSet` represents a proposed set of code changes (diffs) generated
//! by an agent during a [`Run`](super::run::Run). It is the atomic unit of
//! code modification in the AI workflow — every change the agent wants to
//! make to the repository is packaged as a PatchSet.
//!
//! # Relationships
//!
//! ```text
//! Run ──patchsets──▶ [PatchSet₀, PatchSet₁, ...]
//!                        │
//!                        └──run──▶ Run  (back-reference)
//! ```
//!
//! - **Run** (bidirectional): `Run.patchsets` holds the forward reference
//!   (chronological generation history), `PatchSet.run` is the back-reference.
//!
//! # Lifecycle
//!
//! ```text
//!   ┌──────────┐   agent produces diff   ┌──────────┐
//!   │ (created)│ ───────────────────────▶ │ Proposed │
//!   └──────────┘                          └────┬─────┘
//!                                              │
//!                          ┌───────────────────┼───────────────────┐
//!                          │ validation/review  │                   │
//!                          ▼ passes             ▼ fails             │
//!                     ┌─────────┐          ┌──────────┐            │
//!                     │ Applied │          │ Rejected │            │
//!                     └─────────┘          └────┬─────┘            │
//!                                               │                  │
//!                                               ▼                  │
//!                                  agent generates new PatchSet     │
//!                                  appended to Run.patchsets        │
//! ```
//!
//! 1. **Creation**: The orchestrator calls `PatchSet::new()`, which sets
//!    `apply_status` to `Proposed`. At this point `artifact` is `None`
//!    and `touched` is empty.
//! 2. **Diff generation**: The agent produces a diff against `commit`
//!    (the baseline Git commit). It sets `artifact` to point to the
//!    stored diff content, populates `touched` with a file-level
//!    summary, writes a `rationale`, and records the `format`.
//! 3. **Review / validation**: The orchestrator or a human reviewer
//!    inspects the PatchSet. Automated checks (tests, linting) may run.
//! 4. **Applied**: If the diff passes, the orchestrator commits it to
//!    the repository and transitions `apply_status` to `Applied`.
//! 5. **Rejected**: If the diff fails validation or is rejected by a
//!    reviewer, `apply_status` becomes `Rejected`. The agent may then
//!    generate a new PatchSet appended to `Run.patchsets`.
//!
//! # Ordering
//!
//! PatchSet ordering is determined by position in `Run.patchsets`. If a
//! PatchSet is rejected, the agent generates a new PatchSet and appends
//! it to the Vec. The last entry is always the most recent attempt.
//!
//! # Content
//!
//! The actual diff content is stored as an [`ArtifactRef`] (via the
//! `artifact` field), while [`TouchedFile`] (via the `touched` field)
//! provides a lightweight file-level summary for UI and indexing.
//! The `format` field indicates how to parse the artifact content
//! (unified diff or git diff). The `rationale` field carries the
//! agent's explanation of what was changed and why.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        integrity::IntegrityHash,
        types::{ActorRef, ArtifactRef, Header, ObjectType},
    },
};

/// Patch application status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    /// Patch is generated but not yet applied to the repo.
    Proposed,
    /// Patch has been applied (committed) to the repo.
    Applied,
    /// Patch was rejected by validation or user.
    Rejected,
}

impl ApplyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApplyStatus::Proposed => "proposed",
            ApplyStatus::Applied => "applied",
            ApplyStatus::Rejected => "rejected",
        }
    }
}

impl fmt::Display for ApplyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Diff format for patch content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiffFormat {
    /// Standard unified diff format.
    UnifiedDiff,
    /// Git-specific diff format (with binary support etc).
    GitDiff,
}

/// Type of change for a file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Add,
    Modify,
    Delete,
    Rename,
    Copy,
}

/// Touched file summary in a patchset.
///
/// Provides a quick overview of what files are modified without parsing the full diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TouchedFile {
    pub path: String,
    pub change_type: ChangeType,
    pub lines_added: u32,
    pub lines_deleted: u32,
}

impl TouchedFile {
    pub fn new(
        path: impl Into<String>,
        change_type: ChangeType,
        lines_added: u32,
        lines_deleted: u32,
    ) -> Result<Self, String> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err("path cannot be empty".to_string());
        }
        Ok(Self {
            path,
            change_type,
            lines_added,
            lines_deleted,
        })
    }
}

/// PatchSet object containing a candidate diff.
///
/// Ordering between PatchSets is determined by their position in
/// [`Run.patchsets`](super::run::Run). The PatchSet itself does not
/// carry a generation number or supersession list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchSet {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Run`](super::run::Run) that generated this PatchSet.
    /// `Run.patchsets` holds the forward reference and ordering.
    run: Uuid,
    /// Git commit hash the diff is based on.
    commit: IntegrityHash,
    /// Diff format used for the patch content (e.g. unified diff, git diff).
    ///
    /// Determines how the diff stored in `artifact` should be parsed.
    /// `UnifiedDiff` is the standard format produced by `diff -u`;
    /// `GitDiff` extends it with binary file support, rename detection,
    /// and mode-change headers. The orchestrator sets this at creation
    /// time based on the tool that generated the diff.
    #[serde(alias = "diff_format")]
    format: DiffFormat,
    /// Reference to the actual diff content in object storage.
    ///
    /// Points to an [`ArtifactRef`] whose payload contains the full
    /// diff text (or binary patch) in the encoding described by `format`.
    /// `None` while the diff is still being generated; set once the
    /// agent finishes producing the patch. Consumers fetch the artifact,
    /// then interpret it according to `format`.
    #[serde(alias = "diff_artifact")]
    artifact: Option<ArtifactRef>,
    /// Lightweight summary of files modified in this PatchSet.
    ///
    /// Each [`TouchedFile`] records a path, change type (add/modify/
    /// delete/rename/copy), and line-count deltas. This allows UIs and
    /// indexing pipelines to display a file-level overview without
    /// downloading or parsing the full diff artifact. The list is
    /// populated incrementally as the agent produces changes and should
    /// be consistent with the actual diff content.
    #[serde(default, alias = "touched_files")]
    touched: Vec<TouchedFile>,
    /// Human-readable explanation of the changes in this PatchSet.
    ///
    /// Serves a role analogous to a commit message or PR description,
    /// bridging the gap between the high-level goal (Task/Plan) and
    /// the raw diff (artifact).
    ///
    /// **Primary author**: the agent executing the Run. After producing
    /// the diff, the agent summarises **what was changed and why** and
    /// writes it here. A human reviewer may later overwrite or refine
    /// the text via `set_rationale()` if the agent's explanation is
    /// insufficient.
    ///
    /// When a Run produces multiple PatchSets (successive attempts),
    /// each rationale captures the reasoning behind that specific
    /// attempt, e.g.:
    ///
    /// - PatchSet₀: "Replaced session auth with JWT — breaks backward compat"
    /// - PatchSet₁: "Gradual migration: accept both auth schemes"
    ///
    /// `None` only when the PatchSet is still being generated or the
    /// agent did not provide an explanation. Reviewers should treat a
    /// missing rationale as a signal to inspect the diff more carefully.
    rationale: Option<String>,
    /// Current application status of this PatchSet.
    ///
    /// Tracks whether the diff has been applied to the repository:
    ///
    /// - **`Proposed`** (initial): The diff has been generated but not
    ///   yet committed. The orchestrator or a human reviewer can inspect
    ///   the artifact, run validation, and decide whether to apply.
    /// - **`Applied`**: The diff has been committed to the repository.
    ///   Once applied, the PatchSet is immutable — further changes
    ///   require a new PatchSet in the same Run.
    /// - **`Rejected`**: The diff was rejected by automated validation
    ///   (e.g. tests failed) or by a human reviewer. The agent may
    ///   generate a new PatchSet appended to `Run.patchsets` to retry.
    ///
    /// Transitions: `Proposed → Applied` or `Proposed → Rejected`.
    /// No other transitions are valid.
    apply_status: ApplyStatus,
}

impl PatchSet {
    /// Create a new patchset object.
    pub fn new(created_by: ActorRef, run: Uuid, commit: impl AsRef<str>) -> Result<Self, String> {
        let commit = commit.as_ref().parse()?;
        Ok(Self {
            header: Header::new(ObjectType::PatchSet, created_by)?,
            run,
            commit,
            format: DiffFormat::UnifiedDiff,
            artifact: None,
            touched: Vec::new(),
            rationale: None,
            apply_status: ApplyStatus::Proposed,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run(&self) -> Uuid {
        self.run
    }

    pub fn commit(&self) -> &IntegrityHash {
        &self.commit
    }

    pub fn format(&self) -> &DiffFormat {
        &self.format
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }

    pub fn touched(&self) -> &[TouchedFile] {
        &self.touched
    }

    pub fn rationale(&self) -> Option<&str> {
        self.rationale.as_deref()
    }

    pub fn apply_status(&self) -> &ApplyStatus {
        &self.apply_status
    }

    pub fn set_artifact(&mut self, artifact: Option<ArtifactRef>) {
        self.artifact = artifact;
    }

    pub fn add_touched(&mut self, file: TouchedFile) {
        self.touched.push(file);
    }

    pub fn set_rationale(&mut self, rationale: Option<String>) {
        self.rationale = rationale;
    }

    pub fn set_apply_status(&mut self, apply_status: ApplyStatus) {
        self.apply_status = apply_status;
    }
}

impl fmt::Display for PatchSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PatchSet: {}", self.header.object_id())
    }
}

impl ObjectTrait for PatchSet {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::PatchSet
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute PatchSet size: {}", e);
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

    fn test_hash_hex() -> String {
        IntegrityHash::compute(b"ai-process-test").to_hex()
    }

    #[test]
    fn test_patchset_creation() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run = Uuid::from_u128(0x1);
        let base_hash = test_hash_hex();

        let patchset = PatchSet::new(actor, run, &base_hash).expect("patchset");

        assert_eq!(patchset.header().object_type(), &ObjectType::PatchSet);
        assert_eq!(patchset.run(), run);
        assert_eq!(patchset.format(), &DiffFormat::UnifiedDiff);
        assert_eq!(patchset.apply_status(), &ApplyStatus::Proposed);
        assert!(patchset.touched().is_empty());
    }
}
