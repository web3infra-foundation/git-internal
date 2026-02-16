//! AI Tool Invocation Definition
//!
//! A `ToolInvocation` records a specific action taken by an agent, such as reading a file,
//! running a command, or querying a search engine.
//!
//! # Purpose
//!
//! - **Audit Trail**: Allows reconstructing exactly what the agent did.
//! - **Cost Tracking**: Can be used to calculate token/resource usage.
//! - **Debugging**: Helps understand why an agent made a particular decision.
//!
//! # Fields
//!
//! - `tool_name`: The identifier of the tool (e.g., "read_file").
//! - `args`: JSON arguments passed to the tool.
//! - `io_footprint`: Files read/written during the operation (for dependency tracking).
//! - `status`: Whether the tool call succeeded or failed.

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

/// IO footprint of a tool invocation.
/// Tracks reads and writes for auditability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoFootprint {
    #[serde(default)]
    pub paths_read: Vec<String>,
    #[serde(default)]
    pub paths_written: Vec<String>,
}

/// Tool invocation record.
/// Records a single tool call within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvocation {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    tool_name: String,
    io_footprint: Option<IoFootprint>,
    #[serde(default)]
    args: serde_json::Value,
    status: ToolStatus,
    result_summary: Option<String>,
    #[serde(default)]
    artifacts: Vec<ArtifactRef>,
}

impl ToolInvocation {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        tool_name: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::ToolInvocation, repo_id, created_by)?,
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
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut tool_inv =
            ToolInvocation::new(repo_id, actor, run_id, "read_file").expect("tool_invocation");

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
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut tool_inv =
            ToolInvocation::new(repo_id, actor, run_id, "apply_patch").expect("tool_invocation");
        tool_inv.set_status(ToolStatus::Error);
        tool_inv.set_args(serde_json::json!({"path": "src/lib.rs"}));
        tool_inv.set_result_summary(Some("failed".to_string()));
        tool_inv.add_artifact(ArtifactRef::new("local", "artifact-key").expect("artifact"));

        assert_eq!(tool_inv.status(), &ToolStatus::Error);
        assert_eq!(tool_inv.artifacts().len(), 1);
        assert_eq!(tool_inv.args()["path"], "src/lib.rs");
    }
}
