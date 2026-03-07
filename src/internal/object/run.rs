//! AI Run snapshot.
//!
//! `Run` stores one immutable execution attempt for a `Task`.
//!
//! # How to use this object
//!
//! - Create a `Run` when Libra starts a new execution attempt.
//! - Set the selected `Plan`, optional `ContextSnapshot`, and runtime
//!   `Environment` before persistence.
//! - Create a fresh `Run` for retries instead of mutating a prior run.
//!
//! # How it works with other objects
//!
//! - `Provenance` records model/provider configuration for the run.
//! - `ToolInvocation`, `RunEvent`, `PlanStepEvent`, `Evidence`,
//!   `PatchSet`, `Decision`, and `RunUsage` all attach to `Run`.
//! - `Decision` is the terminal verdict for a run.
//!
//! # How Libra should call it
//!
//! Libra should treat `Run` as the execution envelope and keep "active
//! run", retries, and scheduling state in Libra. Execution progress,
//! metrics, and failures must be appended as event objects rather than
//! written back onto the run snapshot.

use std::{collections::HashMap, fmt};

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

/// Best-effort runtime environment capture for one `Run`.
///
/// This is a lightweight reproducibility aid. Libra may augment or
/// normalize these values before persistence if it needs stricter
/// environment tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    /// Operating system identifier captured for the run environment.
    pub os: String,
    /// CPU architecture identifier captured for the run environment.
    pub arch: String,
    /// Working directory from which the run was started.
    pub cwd: String,
    /// Additional application-defined environment metadata.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Environment {
    /// Capture a best-effort environment snapshot from the current
    /// process.
    pub fn capture() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cwd: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|e| {
                    tracing::warn!("Failed to get current directory: {}", e);
                    "unknown".to_string()
                }),
            extra: HashMap::new(),
        }
    }
}

/// Immutable execution-attempt envelope.
///
/// A stored `Run` says "this attempt existed against this task / plan /
/// commit baseline". It does not itself accumulate logs or status
/// transitions after persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical owning task for this execution attempt.
    task: Uuid,
    /// Optional selected plan revision used by this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan: Option<Uuid>,
    /// Baseline repository integrity hash from which execution started.
    commit: IntegrityHash,
    /// Optional static context snapshot captured at run start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot: Option<Uuid>,
    /// Optional execution environment metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    environment: Option<Environment>,
}

impl Run {
    /// Create a new execution attempt for the given task and commit
    /// baseline.
    pub fn new(created_by: ActorRef, task: Uuid, commit: impl AsRef<str>) -> Result<Self, String> {
        let commit = commit.as_ref().parse()?;
        Ok(Self {
            header: Header::new(ObjectType::Run, created_by)?,
            task,
            plan: None,
            commit,
            snapshot: None,
            environment: Some(Environment::capture()),
        })
    }

    /// Return the immutable header for this run.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical owning task id.
    pub fn task(&self) -> Uuid {
        self.task
    }

    /// Return the selected plan revision, if one was stored.
    pub fn plan(&self) -> Option<Uuid> {
        self.plan
    }

    /// Set or clear the selected plan revision for this in-memory run.
    pub fn set_plan(&mut self, plan: Option<Uuid>) {
        self.plan = plan;
    }

    /// Return the baseline repository integrity hash.
    pub fn commit(&self) -> &IntegrityHash {
        &self.commit
    }

    /// Return the static context snapshot id, if present.
    pub fn snapshot(&self) -> Option<Uuid> {
        self.snapshot
    }

    /// Set or clear the static context snapshot link for this in-memory
    /// run.
    pub fn set_snapshot(&mut self, snapshot: Option<Uuid>) {
        self.snapshot = snapshot;
    }

    /// Return the captured execution environment, if present.
    pub fn environment(&self) -> Option<&Environment> {
        self.environment.as_ref()
    }
}

impl fmt::Display for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Run: {}", self.header.object_id())
    }
}

impl ObjectTrait for Run {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Run
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Run size: {}", e);
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
    // - new run creation captures a non-empty environment snapshot
    // - plan and context snapshot links can be assigned before storage

    fn test_hash_hex() -> String {
        IntegrityHash::compute(b"ai-process-test").to_hex()
    }

    #[test]
    fn test_new_objects_creation() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let base_hash = test_hash_hex();
        let run = Run::new(actor, Uuid::from_u128(0x1), &base_hash).expect("run");

        let env = run.environment().expect("environment");
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
        assert!(!env.cwd.is_empty());
    }

    #[test]
    fn test_run_plan_and_snapshot() {
        let actor = ActorRef::agent("test-agent").expect("actor");
        let base_hash = test_hash_hex();
        let mut run = Run::new(actor, Uuid::from_u128(0x1), &base_hash).expect("run");
        let plan_id = Uuid::from_u128(0x10);
        let snapshot_id = Uuid::from_u128(0x20);

        run.set_plan(Some(plan_id));
        run.set_snapshot(Some(snapshot_id));

        assert_eq!(run.plan(), Some(plan_id));
        assert_eq!(run.snapshot(), Some(snapshot_id));
    }
}
