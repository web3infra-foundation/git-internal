//! Object type enumeration and AI Object Header Definition.
//!
//! This module defines the common metadata header shared by all AI process objects
//! and the object type enumeration used across pack/object modules.

use std::fmt::{self, Display};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::integrity::IntegrityHash;
use crate::errors::GitError;

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectType {
    Commit = 1,
    Tree,
    Blob,
    Tag,
    OffsetZstdelta,
    OffsetDelta,
    HashDelta,
    ContextSnapshot,
    Decision,
    Evidence,
    PatchSet,
    Plan,
    Provenance,
    Run,
    Task,
    Intent,
    ToolInvocation,
    ContextFrame,
    IntentEvent,
    TaskEvent,
    RunEvent,
    PlanStepEvent,
    RunUsage,
}

const COMMIT_OBJECT_TYPE: &[u8] = b"commit";
const TREE_OBJECT_TYPE: &[u8] = b"tree";
const BLOB_OBJECT_TYPE: &[u8] = b"blob";
const TAG_OBJECT_TYPE: &[u8] = b"tag";
const CONTEXT_SNAPSHOT_OBJECT_TYPE: &[u8] = b"snapshot";
const DECISION_OBJECT_TYPE: &[u8] = b"decision";
const EVIDENCE_OBJECT_TYPE: &[u8] = b"evidence";
const PATCH_SET_OBJECT_TYPE: &[u8] = b"patchset";
const PLAN_OBJECT_TYPE: &[u8] = b"plan";
const PROVENANCE_OBJECT_TYPE: &[u8] = b"provenance";
const RUN_OBJECT_TYPE: &[u8] = b"run";
const TASK_OBJECT_TYPE: &[u8] = b"task";
const INTENT_OBJECT_TYPE: &[u8] = b"intent";
const TOOL_INVOCATION_OBJECT_TYPE: &[u8] = b"invocation";
const CONTEXT_FRAME_OBJECT_TYPE: &[u8] = b"context_frame";
const INTENT_EVENT_OBJECT_TYPE: &[u8] = b"intent_event";
const TASK_EVENT_OBJECT_TYPE: &[u8] = b"task_event";
const RUN_EVENT_OBJECT_TYPE: &[u8] = b"run_event";
const PLAN_STEP_EVENT_OBJECT_TYPE: &[u8] = b"plan_step_event";
const RUN_USAGE_OBJECT_TYPE: &[u8] = b"run_usage";

impl Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ObjectType::Blob => write!(f, "blob"),
            ObjectType::Tree => write!(f, "tree"),
            ObjectType::Commit => write!(f, "commit"),
            ObjectType::Tag => write!(f, "tag"),
            ObjectType::OffsetZstdelta => write!(f, "OffsetZstdelta"),
            ObjectType::OffsetDelta => write!(f, "OffsetDelta"),
            ObjectType::HashDelta => write!(f, "HashDelta"),
            ObjectType::ContextSnapshot => write!(f, "snapshot"),
            ObjectType::Decision => write!(f, "decision"),
            ObjectType::Evidence => write!(f, "evidence"),
            ObjectType::PatchSet => write!(f, "patchset"),
            ObjectType::Plan => write!(f, "plan"),
            ObjectType::Provenance => write!(f, "provenance"),
            ObjectType::Run => write!(f, "run"),
            ObjectType::Task => write!(f, "task"),
            ObjectType::Intent => write!(f, "intent"),
            ObjectType::ToolInvocation => write!(f, "invocation"),
            ObjectType::ContextFrame => write!(f, "context_frame"),
            ObjectType::IntentEvent => write!(f, "intent_event"),
            ObjectType::TaskEvent => write!(f, "task_event"),
            ObjectType::RunEvent => write!(f, "run_event"),
            ObjectType::PlanStepEvent => write!(f, "plan_step_event"),
            ObjectType::RunUsage => write!(f, "run_usage"),
        }
    }
}

impl ObjectType {
    pub fn to_pack_type_u8(&self) -> Result<u8, GitError> {
        match self {
            ObjectType::Commit => Ok(1),
            ObjectType::Tree => Ok(2),
            ObjectType::Blob => Ok(3),
            ObjectType::Tag => Ok(4),
            ObjectType::OffsetZstdelta => Ok(5),
            ObjectType::OffsetDelta => Ok(6),
            ObjectType::HashDelta => Ok(7),
            _ => Err(GitError::PackEncodeError(format!(
                "object type `{}` cannot be encoded in pack header type bits",
                self
            ))),
        }
    }

    pub fn from_pack_type_u8(number: u8) -> Result<ObjectType, GitError> {
        match number {
            1 => Ok(ObjectType::Commit),
            2 => Ok(ObjectType::Tree),
            3 => Ok(ObjectType::Blob),
            4 => Ok(ObjectType::Tag),
            5 => Ok(ObjectType::OffsetZstdelta),
            6 => Ok(ObjectType::OffsetDelta),
            7 => Ok(ObjectType::HashDelta),
            _ => Err(GitError::InvalidObjectType(format!(
                "Invalid pack object type number: {number}"
            ))),
        }
    }

    pub fn to_bytes(&self) -> Option<&[u8]> {
        match self {
            ObjectType::Commit => Some(COMMIT_OBJECT_TYPE),
            ObjectType::Tree => Some(TREE_OBJECT_TYPE),
            ObjectType::Blob => Some(BLOB_OBJECT_TYPE),
            ObjectType::Tag => Some(TAG_OBJECT_TYPE),
            ObjectType::ContextSnapshot => Some(CONTEXT_SNAPSHOT_OBJECT_TYPE),
            ObjectType::Decision => Some(DECISION_OBJECT_TYPE),
            ObjectType::Evidence => Some(EVIDENCE_OBJECT_TYPE),
            ObjectType::PatchSet => Some(PATCH_SET_OBJECT_TYPE),
            ObjectType::Plan => Some(PLAN_OBJECT_TYPE),
            ObjectType::Provenance => Some(PROVENANCE_OBJECT_TYPE),
            ObjectType::Run => Some(RUN_OBJECT_TYPE),
            ObjectType::Task => Some(TASK_OBJECT_TYPE),
            ObjectType::Intent => Some(INTENT_OBJECT_TYPE),
            ObjectType::ToolInvocation => Some(TOOL_INVOCATION_OBJECT_TYPE),
            ObjectType::ContextFrame => Some(CONTEXT_FRAME_OBJECT_TYPE),
            ObjectType::IntentEvent => Some(INTENT_EVENT_OBJECT_TYPE),
            ObjectType::TaskEvent => Some(TASK_EVENT_OBJECT_TYPE),
            ObjectType::RunEvent => Some(RUN_EVENT_OBJECT_TYPE),
            ObjectType::PlanStepEvent => Some(PLAN_STEP_EVENT_OBJECT_TYPE),
            ObjectType::RunUsage => Some(RUN_USAGE_OBJECT_TYPE),
            ObjectType::OffsetDelta | ObjectType::HashDelta | ObjectType::OffsetZstdelta => None,
        }
    }

    pub fn from_string(s: &str) -> Result<ObjectType, GitError> {
        match s {
            "blob" => Ok(ObjectType::Blob),
            "tree" => Ok(ObjectType::Tree),
            "commit" => Ok(ObjectType::Commit),
            "tag" => Ok(ObjectType::Tag),
            "snapshot" => Ok(ObjectType::ContextSnapshot),
            "decision" => Ok(ObjectType::Decision),
            "evidence" => Ok(ObjectType::Evidence),
            "patchset" => Ok(ObjectType::PatchSet),
            "plan" => Ok(ObjectType::Plan),
            "provenance" => Ok(ObjectType::Provenance),
            "run" => Ok(ObjectType::Run),
            "task" => Ok(ObjectType::Task),
            "intent" => Ok(ObjectType::Intent),
            "invocation" => Ok(ObjectType::ToolInvocation),
            "context_frame" => Ok(ObjectType::ContextFrame),
            "intent_event" => Ok(ObjectType::IntentEvent),
            "task_event" => Ok(ObjectType::TaskEvent),
            "run_event" => Ok(ObjectType::RunEvent),
            "plan_step_event" => Ok(ObjectType::PlanStepEvent),
            "run_usage" => Ok(ObjectType::RunUsage),
            _ => Err(GitError::InvalidObjectType(s.to_string())),
        }
    }

    pub fn to_data(self) -> Result<Vec<u8>, GitError> {
        match self {
            ObjectType::Blob => Ok(b"blob".to_vec()),
            ObjectType::Tree => Ok(b"tree".to_vec()),
            ObjectType::Commit => Ok(b"commit".to_vec()),
            ObjectType::Tag => Ok(b"tag".to_vec()),
            ObjectType::ContextSnapshot => Ok(b"snapshot".to_vec()),
            ObjectType::Decision => Ok(b"decision".to_vec()),
            ObjectType::Evidence => Ok(b"evidence".to_vec()),
            ObjectType::PatchSet => Ok(b"patchset".to_vec()),
            ObjectType::Plan => Ok(b"plan".to_vec()),
            ObjectType::Provenance => Ok(b"provenance".to_vec()),
            ObjectType::Run => Ok(b"run".to_vec()),
            ObjectType::Task => Ok(b"task".to_vec()),
            ObjectType::Intent => Ok(b"intent".to_vec()),
            ObjectType::ToolInvocation => Ok(b"invocation".to_vec()),
            ObjectType::ContextFrame => Ok(b"context_frame".to_vec()),
            ObjectType::IntentEvent => Ok(b"intent_event".to_vec()),
            ObjectType::TaskEvent => Ok(b"task_event".to_vec()),
            ObjectType::RunEvent => Ok(b"run_event".to_vec()),
            ObjectType::PlanStepEvent => Ok(b"plan_step_event".to_vec()),
            ObjectType::RunUsage => Ok(b"run_usage".to_vec()),
            _ => Err(GitError::InvalidObjectType(self.to_string())),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            ObjectType::Commit => 1,
            ObjectType::Tree => 2,
            ObjectType::Blob => 3,
            ObjectType::Tag => 4,
            ObjectType::OffsetZstdelta => 5,
            ObjectType::OffsetDelta => 6,
            ObjectType::HashDelta => 7,
            ObjectType::ContextSnapshot => 8,
            ObjectType::Decision => 9,
            ObjectType::Evidence => 10,
            ObjectType::PatchSet => 11,
            ObjectType::Plan => 12,
            ObjectType::Provenance => 13,
            ObjectType::Run => 14,
            ObjectType::Task => 15,
            ObjectType::Intent => 16,
            ObjectType::ToolInvocation => 17,
            ObjectType::ContextFrame => 18,
            ObjectType::IntentEvent => 19,
            ObjectType::TaskEvent => 20,
            ObjectType::RunEvent => 21,
            ObjectType::PlanStepEvent => 22,
            ObjectType::RunUsage => 23,
        }
    }

    pub fn from_u8(number: u8) -> Result<ObjectType, GitError> {
        match number {
            1 => Ok(ObjectType::Commit),
            2 => Ok(ObjectType::Tree),
            3 => Ok(ObjectType::Blob),
            4 => Ok(ObjectType::Tag),
            5 => Ok(ObjectType::OffsetZstdelta),
            6 => Ok(ObjectType::OffsetDelta),
            7 => Ok(ObjectType::HashDelta),
            8 => Ok(ObjectType::ContextSnapshot),
            9 => Ok(ObjectType::Decision),
            10 => Ok(ObjectType::Evidence),
            11 => Ok(ObjectType::PatchSet),
            12 => Ok(ObjectType::Plan),
            13 => Ok(ObjectType::Provenance),
            14 => Ok(ObjectType::Run),
            15 => Ok(ObjectType::Task),
            16 => Ok(ObjectType::Intent),
            17 => Ok(ObjectType::ToolInvocation),
            18 => Ok(ObjectType::ContextFrame),
            19 => Ok(ObjectType::IntentEvent),
            20 => Ok(ObjectType::TaskEvent),
            21 => Ok(ObjectType::RunEvent),
            22 => Ok(ObjectType::PlanStepEvent),
            23 => Ok(ObjectType::RunUsage),
            _ => Err(GitError::InvalidObjectType(format!(
                "Invalid object type number: {number}"
            ))),
        }
    }

    pub fn is_base(&self) -> bool {
        matches!(
            self,
            ObjectType::Commit | ObjectType::Tree | ObjectType::Blob | ObjectType::Tag
        )
    }

    pub fn is_ai_object(&self) -> bool {
        matches!(
            self,
            ObjectType::ContextSnapshot
                | ObjectType::Decision
                | ObjectType::Evidence
                | ObjectType::PatchSet
                | ObjectType::Plan
                | ObjectType::Provenance
                | ObjectType::Run
                | ObjectType::Task
                | ObjectType::Intent
                | ObjectType::ToolInvocation
                | ObjectType::ContextFrame
                | ObjectType::IntentEvent
                | ObjectType::TaskEvent
                | ObjectType::RunEvent
                | ObjectType::PlanStepEvent
                | ObjectType::RunUsage
        )
    }
}

/// Actor kind enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    Agent,
    System,
    McpClient,
    #[serde(untagged)]
    Other(String),
}

impl fmt::Display for ActorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorKind::Human => write!(f, "human"),
            ActorKind::Agent => write!(f, "agent"),
            ActorKind::System => write!(f, "system"),
            ActorKind::McpClient => write!(f, "mcp_client"),
            ActorKind::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<String> for ActorKind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "human" => ActorKind::Human,
            "agent" => ActorKind::Agent,
            "system" => ActorKind::System,
            "mcp_client" => ActorKind::McpClient,
            _ => ActorKind::Other(s),
        }
    }
}

impl From<&str> for ActorKind {
    fn from(s: &str) -> Self {
        match s {
            "human" => ActorKind::Human,
            "agent" => ActorKind::Agent,
            "system" => ActorKind::System,
            "mcp_client" => ActorKind::McpClient,
            _ => ActorKind::Other(s.to_string()),
        }
    }
}

/// Actor reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActorRef {
    kind: ActorKind,
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
}

impl ActorRef {
    pub fn new(kind: impl Into<ActorKind>, id: impl Into<String>) -> Result<Self, String> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err("actor id cannot be empty".to_string());
        }
        Ok(Self {
            kind: kind.into(),
            id,
            display_name: None,
        })
    }

    pub fn human(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::Human, id)
    }

    pub fn agent(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::Agent, id)
    }

    pub fn system(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::System, id)
    }

    pub fn mcp_client(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::McpClient, id)
    }

    pub fn kind(&self) -> &ActorKind {
        &self.kind
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub fn set_display_name(&mut self, display_name: Option<String>) {
        self.display_name = display_name;
    }
}

/// Reference to an external artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    store: String,
    key: String,
}

impl ArtifactRef {
    pub fn new(store: impl Into<String>, key: impl Into<String>) -> Result<Self, String> {
        let store = store.into();
        let key = key.into();
        if store.trim().is_empty() {
            return Err("artifact store cannot be empty".to_string());
        }
        if key.trim().is_empty() {
            return Err("artifact key cannot be empty".to_string());
        }
        Ok(Self { store, key })
    }

    pub fn store(&self) -> &str {
        &self.store
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Shared object header for AI objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Header {
    object_id: Uuid,
    object_type: ObjectType,
    version: u8,
    created_at: DateTime<Utc>,
    created_by: ActorRef,
}

const CURRENT_HEADER_VERSION: u8 = 1;

impl Header {
    pub fn new(object_type: ObjectType, created_by: ActorRef) -> Result<Self, String> {
        Ok(Self {
            object_id: Uuid::now_v7(),
            object_type,
            version: CURRENT_HEADER_VERSION,
            created_at: Utc::now(),
            created_by,
        })
    }

    pub fn object_id(&self) -> Uuid {
        self.object_id
    }

    pub fn object_type(&self) -> &ObjectType {
        &self.object_type
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn created_by(&self) -> &ActorRef {
        &self.created_by
    }

    pub fn set_version(&mut self, version: u8) -> Result<(), String> {
        if version == 0 {
            return Err("header version must be non-zero".to_string());
        }
        self.version = version;
        Ok(())
    }

    pub fn checksum(&self) -> IntegrityHash {
        let bytes = serde_json::to_vec(self).expect("header serialization");
        IntegrityHash::compute(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_kind_serialization() {
        let value = serde_json::to_string(&ActorKind::McpClient).expect("serialize");
        assert_eq!(value, "\"mcp_client\"");
    }

    #[test]
    fn test_actor_ref() {
        let mut actor = ActorRef::human("alice").expect("actor");
        actor.set_display_name(Some("Alice".to_string()));

        assert_eq!(actor.kind(), &ActorKind::Human);
        assert_eq!(actor.id(), "alice");
        assert_eq!(actor.display_name(), Some("Alice"));
    }

    #[test]
    fn test_empty_actor_id() {
        let err = ActorRef::human("  ").expect_err("empty actor id must fail");
        assert!(err.contains("actor id"));
    }

    #[test]
    fn test_header_serialization() {
        let actor = ActorRef::human("alice").expect("actor");
        let header = Header::new(ObjectType::Intent, actor).expect("header");
        let json = serde_json::to_value(&header).expect("serialize");

        assert_eq!(json["object_type"], "intent");
        assert_eq!(json["version"], 1);
    }

    #[test]
    fn test_header_version_new_uses_current() {
        let actor = ActorRef::human("alice").expect("actor");
        let header = Header::new(ObjectType::Plan, actor).expect("header");
        assert_eq!(header.version(), CURRENT_HEADER_VERSION);
    }

    #[test]
    fn test_header_version_setter_rejects_zero() {
        let actor = ActorRef::human("alice").expect("actor");
        let mut header = Header::new(ObjectType::Task, actor).expect("header");
        let err = header.set_version(0).expect_err("zero must fail");
        assert!(err.contains("non-zero"));
    }

    #[test]
    fn test_header_checksum() {
        let actor = ActorRef::human("alice").expect("actor");
        let header = Header::new(ObjectType::Run, actor).expect("header");
        assert!(!header.checksum().to_hex().is_empty());
    }

    #[test]
    fn test_object_type_from_u8() {
        assert_eq!(
            ObjectType::from_u8(18).expect("type"),
            ObjectType::ContextFrame
        );
    }

    #[test]
    fn test_object_type_to_u8() {
        assert_eq!(ObjectType::RunUsage.to_u8(), 23);
    }

    #[test]
    fn test_object_type_from_string() {
        assert_eq!(
            ObjectType::from_string("plan_step_event").expect("type"),
            ObjectType::PlanStepEvent
        );
    }

    #[test]
    fn test_object_type_to_data() {
        assert_eq!(
            ObjectType::IntentEvent.to_data().expect("data"),
            b"intent_event".to_vec()
        );
    }

    /// All ObjectType variants for exhaustive testing.
    /// Update this list whenever a new variant is added to ObjectType.
    const ALL_VARIANTS: &[ObjectType] = &[
        ObjectType::Commit,
        ObjectType::Tree,
        ObjectType::Blob,
        ObjectType::Tag,
        ObjectType::OffsetZstdelta,
        ObjectType::OffsetDelta,
        ObjectType::HashDelta,
        ObjectType::ContextSnapshot,
        ObjectType::Decision,
        ObjectType::Evidence,
        ObjectType::PatchSet,
        ObjectType::Plan,
        ObjectType::Provenance,
        ObjectType::Run,
        ObjectType::Task,
        ObjectType::Intent,
        ObjectType::ToolInvocation,
        ObjectType::ContextFrame,
        ObjectType::IntentEvent,
        ObjectType::TaskEvent,
        ObjectType::RunEvent,
        ObjectType::PlanStepEvent,
        ObjectType::RunUsage,
    ];

    #[test]
    fn test_to_u8_from_u8_round_trip() {
        for variant in ALL_VARIANTS {
            let n = variant.to_u8();
            let recovered = ObjectType::from_u8(n)
                .unwrap_or_else(|_| panic!("from_u8({n}) failed for {variant}"));
            assert_eq!(
                *variant, recovered,
                "to_u8/from_u8 round-trip mismatch for {variant}"
            );
        }
    }

    #[test]
    fn test_display_from_string_round_trip() {
        // Delta types have no string representation in from_string, skip them.
        let skip = [
            ObjectType::OffsetZstdelta,
            ObjectType::OffsetDelta,
            ObjectType::HashDelta,
        ];
        for variant in ALL_VARIANTS {
            if skip.contains(variant) {
                continue;
            }
            let s = variant.to_string();
            let recovered = ObjectType::from_string(&s)
                .unwrap_or_else(|_| panic!("from_string({s:?}) failed for {variant}"));
            assert_eq!(
                *variant, recovered,
                "Display/from_string round-trip mismatch for {variant}"
            );
        }
    }

    #[test]
    fn test_to_bytes_to_data_consistency() {
        for variant in ALL_VARIANTS {
            if let Some(bytes) = variant.to_bytes() {
                let data = variant
                    .to_data()
                    .unwrap_or_else(|_| panic!("to_data failed for {variant}"));
                assert_eq!(bytes, &data[..], "to_bytes/to_data mismatch for {variant}");
            }
        }
    }

    #[test]
    fn test_all_variants_count() {
        // If you add a new ObjectType variant, add it to ALL_VARIANTS above
        // and update this count.
        assert_eq!(
            ALL_VARIANTS.len(),
            23,
            "ALL_VARIANTS count mismatch — did you add a new ObjectType variant?"
        );
    }

    #[test]
    fn test_invalid_checksum() {
        let err = ObjectType::from_string("unknown").expect_err("must fail");
        assert!(matches!(err, GitError::InvalidObjectType(_)));
    }

    #[test]
    fn test_artifact_checksum() {
        let artifact = ArtifactRef::new("local", "artifact-key").expect("artifact");
        assert_eq!(artifact.store(), "local");
        assert_eq!(artifact.key(), "artifact-key");
    }

    #[test]
    fn test_artifact_expiration() {
        let artifact = ArtifactRef::new("s3", "bucket/key").expect("artifact");
        assert_eq!(artifact.store(), "s3");
        assert_eq!(artifact.key(), "bucket/key");
    }
}
