//! AI Tool Invocation Definition
//!
//! A `ToolInvocation` records a single action taken by an agent during
//! a [`Run`](super::run::Run) — reading a file, running a shell
//! command, calling an API, etc. It is the finest-grained unit of
//! agent activity, capturing *what* was done, *with which arguments*,
//! and *what happened*.
//!
//! # Position in Lifecycle
//!
//! ```text
//! Run ──patchsets──▶ [PatchSet₀, ...]
//!  │
//!  ├── evidence ──▶ [Evidence₀, ...]
//!  │
//!  └── tool invocations ──▶ [ToolInvocation₀, ToolInvocation₁, ...]
//!                                  │
//!                                  └── io_footprint (paths read/written)
//! ```
//!
//! ToolInvocations are produced **throughout** a Run, one per tool
//! call. They form a chronological log of every action the agent
//! performed. Unlike Evidence (which validates a PatchSet) or
//! Decision (which concludes a Run), ToolInvocations are low-level
//! operational records.
//!
//! # Purpose
//!
//! - **Audit Trail**: Allows reconstructing exactly what the agent did
//!   step by step, including arguments and results.
//! - **Dependency Tracking**: `io_footprint` records which files were
//!   read or written, enabling incremental re-runs and cache
//!   invalidation.
//! - **Debugging**: When a Run produces unexpected results, reviewing
//!   the ToolInvocation sequence reveals the agent's reasoning path.

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

/// Tool invocation status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    /// Tool executed successfully.
    Ok,
    /// Tool execution failed (returned error).
    Error,
}

impl ToolStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ToolStatus::Ok => "ok",
            ToolStatus::Error => "error",
        }
    }
}

impl fmt::Display for ToolStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// File-level I/O footprint of a tool invocation.
///
/// Records which files were read and written during the tool call.
/// Used for dependency tracking (which inputs influenced which
/// outputs) and for cache invalidation on incremental re-runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoFootprint {
    /// Paths the tool read during execution (e.g. source files,
    /// config files). Relative to the repository root.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths_read: Vec<String>,
    /// Paths the tool wrote or modified (e.g. generated files,
    /// patch output). Relative to the repository root.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths_written: Vec<String>,
}

/// Record of a single tool call made by an agent during a Run.
///
/// One ToolInvocation per tool call. The chronological sequence of
/// ToolInvocations within a Run forms the agent's action log. See
/// module documentation for lifecycle position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvocation {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Run`](super::run::Run) during which this tool was called.
    ///
    /// Every ToolInvocation belongs to exactly one Run. The Run does
    /// not store a back-reference; the full invocation log is
    /// reconstructed by querying all ToolInvocations with the same
    /// `run_id`, ordered by `created_at`.
    run_id: Uuid,
    /// Identifier of the tool that was called (e.g. "read_file",
    /// "bash", "search_code").
    ///
    /// This is the tool's registered name in the agent's tool
    /// catalogue, not a human-readable label.
    tool_name: String,
    /// Files read and written during this tool call.
    ///
    /// `None` when the tool has no file-system side effects (e.g. a
    /// pure computation or API call). See [`IoFootprint`] for details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    io_footprint: Option<IoFootprint>,
    /// Arguments passed to the tool, as a JSON value.
    ///
    /// The schema depends on the tool. For example, `read_file` might
    /// have `{"path": "src/main.rs"}`, while `bash` might have
    /// `{"command": "cargo test"}`. `Null` when the tool takes no
    /// arguments.
    #[serde(default)]
    args: serde_json::Value,
    /// Whether the tool call succeeded or failed.
    ///
    /// `Ok` means the tool returned a normal result; `Error` means it
    /// returned an error. The orchestrator may use this to decide
    /// whether to retry or abort the Run.
    status: ToolStatus,
    /// Short human-readable summary of the tool's output.
    ///
    /// For `read_file`: might be the file size or first few lines.
    /// For `bash`: might be the last line of stdout. For failed calls:
    /// the error message. `None` if no summary was captured. For full
    /// output, see `artifacts`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_summary: Option<String>,
    /// References to full output files in object storage.
    ///
    /// May include stdout/stderr logs, generated files, screenshots,
    /// etc. Each [`ArtifactRef`] points to one stored file. Empty
    /// when the tool produced no persistent output.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    artifacts: Vec<ArtifactRef>,
}

impl ToolInvocation {
    pub fn new(
        created_by: ActorRef,
        run_id: Uuid,
        tool_name: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::ToolInvocation, created_by)?,
            run_id,
            tool_name: tool_name.into(),
            io_footprint: None,
            args: serde_json::Value::Null,
            status: ToolStatus::Ok,
            result_summary: None,
            artifacts: Vec::new(),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    pub fn io_footprint(&self) -> Option<&IoFootprint> {
        self.io_footprint.as_ref()
    }

    pub fn args(&self) -> &serde_json::Value {
        &self.args
    }

    pub fn status(&self) -> &ToolStatus {
        &self.status
    }

    pub fn result_summary(&self) -> Option<&str> {
        self.result_summary.as_deref()
    }

    pub fn artifacts(&self) -> &[ArtifactRef] {
        &self.artifacts
    }

    pub fn set_io_footprint(&mut self, io_footprint: Option<IoFootprint>) {
        self.io_footprint = io_footprint;
    }

    pub fn set_args(&mut self, args: serde_json::Value) {
        self.args = args;
    }

    pub fn set_status(&mut self, status: ToolStatus) {
        self.status = status;
    }

    pub fn set_result_summary(&mut self, result_summary: Option<String>) {
        self.result_summary = result_summary;
    }

    pub fn add_artifact(&mut self, artifact: ArtifactRef) {
        self.artifacts.push(artifact);
    }
}

impl fmt::Display for ToolInvocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToolInvocation: {}", self.header.object_id())
    }
}

impl ObjectTrait for ToolInvocation {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::ToolInvocation
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute ToolInvocation size: {}", e);
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
    fn test_tool_invocation_io_footprint() {
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut tool_inv =
            ToolInvocation::new(actor, run_id, "read_file").expect("tool_invocation");

        let footprint = IoFootprint {
            paths_read: vec!["src/main.rs".to_string()],
            paths_written: vec![],
        };

        tool_inv.set_io_footprint(Some(footprint));

        assert_eq!(tool_inv.tool_name(), "read_file");
        assert!(tool_inv.io_footprint().is_some());
        assert_eq!(
            tool_inv.io_footprint().unwrap().paths_read[0],
            "src/main.rs"
        );
    }

    #[test]
    fn test_tool_invocation_fields() {
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut tool_inv =
            ToolInvocation::new(actor, run_id, "apply_patch").expect("tool_invocation");
        tool_inv.set_status(ToolStatus::Error);
        tool_inv.set_args(serde_json::json!({"path": "src/lib.rs"}));
        tool_inv.set_result_summary(Some("failed".to_string()));
        tool_inv.add_artifact(ArtifactRef::new("local", "artifact-key").expect("artifact"));

        assert_eq!(tool_inv.status(), &ToolStatus::Error);
        assert_eq!(tool_inv.artifacts().len(), 1);
        assert_eq!(tool_inv.args()["path"], "src/lib.rs");
    }
}
