//! AI Context Snapshot Definition
//!
//! A `ContextSnapshot` represents the state of the codebase and external resources
//! that an agent uses to perform its task.
//!
//! # Selection Strategy
//!
//! - **Explicit**: User manually selected files.
//! - **Heuristic**: Agent automatically selected files based on relevance.
//!
//! # Integrity
//!
//! Each item in the snapshot has a content hash (`IntegrityHash`).
//! This ensures that if the file changes on disk, we know the snapshot is stale or refers to an older version.

use std::fmt::Display;

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

/// Selection strategy for context snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    /// Files explicitly chosen by the user.
    Explicit,
    /// Files automatically selected by the agent/system.
    Heuristic,
}

/// Context item kind.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextItemKind {
    /// A regular file in the repository.
    File,
    /// A URL (web page, API endpoint, etc.).
    Url,
    /// A free-form text snippet (e.g. doc fragment, note).
    Snippet,
    /// Command or terminal output.
    Command,
    /// Image or other binary visual content.
    Image,
    Other(String),
}

/// Context item describing a single input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextItem {
    pub kind: ContextItemKind,
    pub path: String,
    pub content_id: IntegrityHash,
    #[serde(default)]
    pub content_preview: Option<String>,
}

impl ContextItem {
    pub fn new(
        kind: ContextItemKind,
        path: impl Into<String>,
        content_id: IntegrityHash,
    ) -> Result<Self, String> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err("path cannot be empty".to_string());
        }
        Ok(Self {
            kind,
            path,
            content_id,
            content_preview: None,
        })
    }
}

/// Context snapshot describing selected inputs.
/// Captures the selection strategy and content identifiers used by a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSnapshot {
    #[serde(flatten)]
    header: Header,
    base_commit_sha: IntegrityHash,
    selection_strategy: SelectionStrategy,
    #[serde(default)]
    items: Vec<ContextItem>,
    summary: Option<String>,
}

impl ContextSnapshot {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        base_commit_sha: impl AsRef<str>,
        selection_strategy: SelectionStrategy,
    ) -> Result<Self, String> {
        let base_commit_sha = base_commit_sha.as_ref().parse()?;
        Ok(Self {
            header: Header::new(ObjectType::ContextSnapshot, repo_id, created_by)?,
            base_commit_sha,
            selection_strategy,
            items: Vec::new(),
            summary: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn base_commit_sha(&self) -> &IntegrityHash {
        &self.base_commit_sha
    }

    pub fn selection_strategy(&self) -> &SelectionStrategy {
        &self.selection_strategy
    }

    pub fn items(&self) -> &[ContextItem] {
        &self.items
    }

    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    pub fn add_item(&mut self, item: ContextItem) {
        self.items.push(item);
    }

    pub fn set_summary(&mut self, summary: Option<String>) {
        self.summary = summary;
    }
}

impl Display for ContextSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ContextSnapshot: {}", self.header.object_id())
    }
}

impl ObjectTrait for ContextSnapshot {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::ContextSnapshot
    }

    fn get_size(&self) -> usize {
        serde_json::to_vec(self).map(|v| v.len()).unwrap_or(0)
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_snapshot_accessors_and_mutators() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("coder").expect("actor");
        let mut snapshot = ContextSnapshot::new(
            repo_id,
            actor,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            SelectionStrategy::Heuristic,
        )
        .expect("snapshot");

        assert_eq!(snapshot.selection_strategy(), &SelectionStrategy::Heuristic);
        assert!(snapshot.items().is_empty());
        assert!(snapshot.summary().is_none());

        let item = ContextItem::new(
            ContextItemKind::File,
            "src/main.rs",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .expect("hash"),
        )
        .expect("item");
        snapshot.add_item(item);
        snapshot.set_summary(Some("selected by relevance".to_string()));

        assert_eq!(snapshot.items().len(), 1);
        assert_eq!(snapshot.summary(), Some("selected by relevance"));
        assert_eq!(snapshot.base_commit_sha().to_hex().len(), 64);
    }
}
