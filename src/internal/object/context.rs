//! AI Context Snapshot Definition
//!
//! A [`ContextSnapshot`] is an optional static capture of the codebase
//! and external resources that an agent observed when a
//! [`Run`](super::run::Run) began. Unlike the dynamic
//! [`ContextPipeline`](super::pipeline::ContextPipeline) (which
//! accumulates frames during execution), a ContextSnapshot is a
//! **point-in-time** record that does not change after creation.
//!
//! # Position in Lifecycle
//!
//! ```text
//!  ⑤  Run ──snapshot──▶ ContextSnapshot (optional, static)
//!       │
//!       ├──▶ ContextPipeline (dynamic, via Plan.pipeline)
//!       │
//!       ▼
//!  ⑥  ToolInvocations ...
//! ```
//!
//! A ContextSnapshot is created at step ⑤ when the Run is initialized.
//! It complements the ContextPipeline: the snapshot captures the
//! **initial** state (what files, URLs, snippets the agent sees at
//! start), while the pipeline tracks **incremental** context changes
//! during execution.
//!
//! # Items
//!
//! Each [`ContextItem`] has three layers:
//!
//! - **`path`** — human-readable locator (repo path, URL, command,
//!   label).
//! - **`blob`** — Git blob hash pointing to the **full content** at
//!   capture time.
//! - **`preview`** — truncated text for quick display without reading
//!   the blob.
//!
//! All item kinds use `blob` as the unified content reference:
//!
//! | Kind | `path` example | `blob` content |
//! |---|---|---|
//! | `File` | `src/main.rs` | Same blob in git tree (zero extra storage) |
//! | `Url` | `https://docs.rs/...` | Fetched page content stored as blob |
//! | `Snippet` | `"design notes"` | Snippet text stored as blob |
//! | `Command` | `cargo test` | Command output stored as blob |
//! | `Image` | `screenshot.png` | Image binary stored as blob |
//!
//! `blob` is `Option` because it may be `None` during the
//! draft/collection phase; by the time the snapshot is finalized,
//! items should have their blob set.
//!
//! # Blob Retention
//!
//! Standard `git gc` only considers objects reachable from
//! refs → commits → trees → blobs. A blob referenced solely by an AI
//! object's JSON payload is **not** reachable in git's graph and
//! **will be pruned** after `gc.pruneExpire` (default 2 weeks).
//!
//! For `File` items this is not a concern — the blob is already
//! reachable through the commit tree. For all other kinds,
//! applications must choose a retention strategy:
//!
//! | Strategy | Pros | Cons |
//! |---|---|---|
//! | **Ref anchoring** (`refs/ai/blobs/<hex>`) | Simple, works with stock git | Ref namespace pollution |
//! | **Orphan commit** (`refs/ai/uploads`) | Standard reachability; packable | Extra commit/tree overhead |
//! | **Keep pack** (`.keep` marker) | Zero ref management | Must repack manually |
//! | **Custom GC mark** (scan AI objects) | Cleanest long-term | Requires custom gc |
//!
//! This library does **not** enforce any particular strategy — the
//! consuming application is responsible for ensuring referenced blobs
//! remain reachable.
//!
//! # Purpose
//!
//! - **Reproducibility**: Given the same ContextSnapshot and Plan, an
//!   agent should produce equivalent results.
//! - **Auditing**: Reviewers can inspect exactly what context the agent
//!   had access to when making decisions.
//! - **Content Deduplication**: Using Git blob hashes means identical
//!   file content is stored only once, regardless of how many snapshots
//!   reference it.

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        types::{ActorRef, Header, ObjectType},
    },
};

/// How the items in a [`ContextSnapshot`] were selected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    /// Items were explicitly chosen by the user (e.g. "look at these
    /// files"). The agent should treat these as authoritative context.
    Explicit,
    /// Items were automatically selected by the agent or system based
    /// on relevance heuristics (e.g. file dependency analysis, search
    /// results). The agent may decide to fetch additional context.
    Heuristic,
}

/// The kind of content a [`ContextItem`] represents.
///
/// Determines how `path` and `blob` should be interpreted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextItemKind {
    /// A regular file in the repository. `path` is a repo-relative
    /// path (e.g. `src/main.rs`). `blob` is the same object already
    /// in the git tree (zero extra storage).
    File,
    /// A URL (web page, API docs, etc.). `path` is the full URL.
    /// `blob` contains the fetched page content.
    Url,
    /// A free-form text snippet (e.g. design note, doc fragment).
    /// `path` is a descriptive label. `blob` contains the snippet text.
    Snippet,
    /// Command or terminal output. `path` is the command that was run
    /// (e.g. `cargo test`). `blob` contains the captured output.
    Command,
    /// Image or other binary visual content. `path` is the file name.
    /// `blob` contains the raw binary data.
    Image,
    /// Application-defined kind not covered by the variants above.
    Other(String),
}

/// A single input item within a [`ContextSnapshot`].
///
/// Represents one piece of context the agent has access to — a source
/// file, a URL, a text snippet, command output, or an image. See
/// module documentation for the three-layer design (`path` / `blob` /
/// `preview`) and blob retention strategies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextItem {
    /// The kind of content this item represents. Determines how
    /// `path` and `blob` should be interpreted.
    pub kind: ContextItemKind,
    /// Human-readable locator for this item.
    ///
    /// Meaning depends on `kind`: repo-relative path for `File`,
    /// full URL for `Url`, descriptive label for `Snippet`, shell
    /// command for `Command`, file name for `Image`.
    pub path: String,
    /// Truncated preview of the content for quick display.
    ///
    /// Should be kept under 500 characters. `None` when no preview
    /// is available (e.g. binary content, very short items where
    /// the full content fits in `path`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    /// Git blob hash referencing the **full content** at capture time.
    ///
    /// For `File` items, this is the same blob already in the git
    /// tree (zero extra storage due to content-addressing). For
    /// other kinds (Url, Snippet, Command, Image), the content is
    /// stored as a new blob — see module-level docs for retention
    /// strategies. `None` during the draft/collection phase; should
    /// be set before the snapshot is finalized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<ObjectHash>,
}

impl ContextItem {
    pub fn new(kind: ContextItemKind, path: impl Into<String>) -> Result<Self, String> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err("path cannot be empty".to_string());
        }
        Ok(Self {
            kind,
            path,
            preview: None,
            blob: None,
        })
    }

    pub fn set_blob(&mut self, blob: Option<ObjectHash>) {
        self.blob = blob;
    }
}

/// A static capture of the context an agent observed at Run start.
///
/// Created once per Run (optional). Records which files, URLs,
/// snippets, etc. the agent had access to. See module documentation
/// for lifecycle position, item design, and blob retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSnapshot {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// How the items were selected — by the user (`Explicit`) or
    /// by the agent/system (`Heuristic`).
    selection_strategy: SelectionStrategy,
    /// The context items included in this snapshot.
    ///
    /// Each item references a piece of content (file, URL, snippet,
    /// etc.) via its `blob` field. Items are ordered as added; no
    /// implicit ordering is guaranteed. Empty when the snapshot has
    /// just been created and items haven't been added yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    items: Vec<ContextItem>,
    /// Aggregated human-readable summary of all items.
    ///
    /// A brief description of the overall context (e.g. "3 source
    /// files + API docs for /users endpoint"). `None` when no
    /// summary has been provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
}

impl ContextSnapshot {
    pub fn new(
        created_by: ActorRef,
        selection_strategy: SelectionStrategy,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::ContextSnapshot, created_by)?,
            selection_strategy,
            items: Vec::new(),
            summary: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
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
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute ContextSnapshot size: {}", e);
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
    fn test_context_snapshot_accessors_and_mutators() {
        let actor = ActorRef::agent("coder").expect("actor");
        let mut snapshot =
            ContextSnapshot::new(actor, SelectionStrategy::Heuristic).expect("snapshot");

        assert_eq!(snapshot.selection_strategy(), &SelectionStrategy::Heuristic);
        assert!(snapshot.items().is_empty());
        assert!(snapshot.summary().is_none());

        let item = ContextItem::new(ContextItemKind::File, "src/main.rs").expect("item");
        snapshot.add_item(item);
        snapshot.set_summary(Some("selected by relevance".to_string()));

        assert_eq!(snapshot.items().len(), 1);
        assert_eq!(snapshot.summary(), Some("selected by relevance"));
    }
}
