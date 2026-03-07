//! AI PatchSet snapshot.
//!
//! `PatchSet` stores one immutable candidate diff produced during a
//! `Run`.
//!
//! # How to use this object
//!
//! - Create one `PatchSet` per candidate diff worth retaining.
//! - Use `sequence` to preserve ordering between multiple candidates in
//!   the same run.
//! - Attach diff artifacts, touched files, and rationale before
//!   persistence.
//!
//! # How it works with other objects
//!
//! - `Run` is the canonical owner through `PatchSet.run`.
//! - `Evidence` may validate a specific patchset via `patchset_id`.
//! - `Decision` selects the chosen patchset, if any.
//!
//! # How Libra should call it
//!
//! Libra should use `PatchSet` as immutable staging history. Acceptance,
//! rejection, or promotion to repository commit should be represented by
//! `Decision` and Libra projections rather than by mutating the
//! `PatchSet`.

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiffFormat {
    UnifiedDiff,
    GitDiff,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Add,
    Modify,
    Delete,
    Rename,
    Copy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TouchedFile {
    /// Repository-relative path affected by the candidate diff.
    pub path: String,
    /// Coarse change category for the touched file.
    pub change_type: ChangeType,
    /// Number of added lines attributed to this file in the patch.
    pub lines_added: u32,
    /// Number of deleted lines attributed to this file in the patch.
    pub lines_deleted: u32,
}

impl TouchedFile {
    /// Create one touched-file summary entry for a patchset.
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

/// Immutable candidate diff snapshot for one `Run`.
///
/// A `PatchSet` stores the proposed change and its metadata, while the
/// higher-level verdict about whether that change is accepted lives
/// elsewhere.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchSet {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical owning run for this candidate diff.
    run: Uuid,
    /// Ordering of this candidate among patchsets produced by the same
    /// run.
    sequence: u32,
    /// Repository integrity hash representing the diff baseline or
    /// associated commit context.
    commit: IntegrityHash,
    /// Diff serialization format used for the stored patch candidate.
    format: DiffFormat,
    /// Optional artifact pointer to the full diff payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact: Option<ArtifactRef>,
    /// File-level summary of paths touched by the candidate diff.
    #[serde(default)]
    touched: Vec<TouchedFile>,
    /// Optional human-readable rationale for why this candidate was
    /// generated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rationale: Option<String>,
}

impl PatchSet {
    /// Create a new patchset candidate for the given run.
    pub fn new(created_by: ActorRef, run: Uuid, commit: impl AsRef<str>) -> Result<Self, String> {
        let commit = commit.as_ref().parse()?;
        Ok(Self {
            header: Header::new(ObjectType::PatchSet, created_by)?,
            run,
            sequence: 0,
            commit,
            format: DiffFormat::UnifiedDiff,
            artifact: None,
            touched: Vec::new(),
            rationale: None,
        })
    }

    /// Return the immutable header for this patchset.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical owning run id.
    pub fn run(&self) -> Uuid {
        self.run
    }

    /// Return the patchset ordering number within the run.
    pub fn sequence(&self) -> u32 {
        self.sequence
    }

    /// Set the patchset ordering number before persistence.
    pub fn set_sequence(&mut self, sequence: u32) {
        self.sequence = sequence;
    }

    /// Return the associated integrity hash.
    pub fn commit(&self) -> &IntegrityHash {
        &self.commit
    }

    /// Return the diff serialization format.
    pub fn format(&self) -> &DiffFormat {
        &self.format
    }

    /// Return the diff artifact pointer, if present.
    pub fn artifact(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }

    /// Return the touched-file summary entries.
    pub fn touched(&self) -> &[TouchedFile] {
        &self.touched
    }

    /// Return the human-readable patch rationale, if present.
    pub fn rationale(&self) -> Option<&str> {
        self.rationale.as_deref()
    }

    /// Set or clear the diff artifact pointer.
    pub fn set_artifact(&mut self, artifact: Option<ArtifactRef>) {
        self.artifact = artifact;
    }

    /// Append one touched-file summary entry.
    pub fn add_touched(&mut self, file: TouchedFile) {
        self.touched.push(file);
    }

    /// Set or clear the human-readable rationale.
    pub fn set_rationale(&mut self, rationale: Option<String>) {
        self.rationale = rationale;
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

    // Coverage:
    // - patchset creation defaults
    // - canonical run link, ordering default, and diff-format default

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
        assert_eq!(patchset.sequence(), 0);
        assert_eq!(patchset.format(), &DiffFormat::UnifiedDiff);
        assert!(patchset.touched().is_empty());
    }
}
