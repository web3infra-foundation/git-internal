use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ai_hash::IntegrityHash,
    ai_header::{ActorRef, AiObjectType, Header},
};

/// Selection strategy for context snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    Explicit,
    Heuristic,
}

/// Context item kind.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextItemKind {
    File,
}

/// Context item describing a single input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextItem {
    pub kind: ContextItemKind,
    pub path: String,
    pub content_id: IntegrityHash,
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
            header: Header::new(AiObjectType::ContextSnapshot, repo_id, created_by)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_hash_hex() -> String {
        IntegrityHash::compute(b"ai-process-test").to_hex()
    }

    #[test]
    fn test_context_snapshot_fields() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let base_hash = test_hash_hex();

        let mut snapshot =
            ContextSnapshot::new(repo_id, actor, &base_hash, SelectionStrategy::Explicit)
                .expect("snapshot");
        snapshot.set_summary(Some("core files".to_string()));

        snapshot.add_item(
            ContextItem::new(
                ContextItemKind::File,
                "src/lib.rs",
                IntegrityHash::compute(b"context-item"),
            )
            .expect("context item"),
        );

        assert_eq!(snapshot.items().len(), 1);
        assert_eq!(snapshot.items()[0].path, "src/lib.rs");
        assert_eq!(snapshot.summary(), Some("core files"));
    }
}
