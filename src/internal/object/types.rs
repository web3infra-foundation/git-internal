//! Object type enumeration and AI Object Header Definition.
//!
//! This module defines the common metadata header shared by all AI process objects
//! and the object type enumeration used across pack/object modules.

use std::{
    collections::HashMap,
    fmt::{self, Display},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::integrity::{IntegrityHash, compute_integrity_hash};
use crate::errors::GitError;

/// Visibility of an AI process object.
///
/// Determines whether the object is accessible only within the project (Private)
/// or can be shared externally (Public).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    Private,
    Public,
}

/// In Git, each object type is assigned a unique integer value, which is used to identify the
/// type of the object in Git repositories.
///
/// * `Blob` (1): A Git object that stores the content of a file.
/// * `Tree` (2): A Git object that represents a directory or a folder in a Git repository.
/// * `Commit` (3): A Git object that represents a commit in a Git repository, which contains
///   information such as the author, committer, commit message, and parent commits.
/// * `Tag` (4): A Git object that represents a tag in a Git repository, which is used to mark a
///   specific point in the Git history.
/// * `OffsetDelta` (6): A Git object that represents a delta between two objects, where the delta
///   is stored as an offset to the base object.
/// * `HashDelta` (7): A Git object that represents a delta between two objects, where the delta
///   is stored as a hash of the base object.
///
/// By assigning unique integer values to each Git object type, Git can easily and efficiently
/// identify the type of an object and perform the appropriate operations on it. when parsing a Git
/// repository, Git can use the integer value of an object's type to determine how to parse
/// the object's content.
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectType {
    Commit = 1,
    Tree,
    Blob,
    Tag,
    OffsetZstdelta, // Private extension for Zstandard-compressed delta objects
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

/// Display trait for Git objects type
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
        }
    }
}

/// Display trait for Git objects type
impl ObjectType {
    /// Convert object type to 3-bit pack header type id.
    ///
    /// Git pack headers only carry 3 type bits (values 0..=7). AI object
    /// types are not representable in this field and must not be written
    /// as regular base objects in a pack entry.
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

    /// Decode 3-bit pack header type id to object type.
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

    pub fn to_bytes(&self) -> &[u8] {
        match self {
            ObjectType::Commit => COMMIT_OBJECT_TYPE,
            ObjectType::Tree => TREE_OBJECT_TYPE,
            ObjectType::Blob => BLOB_OBJECT_TYPE,
            ObjectType::Tag => TAG_OBJECT_TYPE,
            ObjectType::ContextSnapshot => CONTEXT_SNAPSHOT_OBJECT_TYPE,
            ObjectType::Decision => DECISION_OBJECT_TYPE,
            ObjectType::Evidence => EVIDENCE_OBJECT_TYPE,
            ObjectType::PatchSet => PATCH_SET_OBJECT_TYPE,
            ObjectType::Plan => PLAN_OBJECT_TYPE,
            ObjectType::Provenance => PROVENANCE_OBJECT_TYPE,
            ObjectType::Run => RUN_OBJECT_TYPE,
            ObjectType::Task => TASK_OBJECT_TYPE,
            ObjectType::Intent => INTENT_OBJECT_TYPE,
            ObjectType::ToolInvocation => TOOL_INVOCATION_OBJECT_TYPE,
            _ => panic!("can put compute the delta hash value"),
        }
    }

    /// Parses a string representation of a Git object type and returns an ObjectType value
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
            _ => Err(GitError::InvalidObjectType(s.to_string())),
        }
    }

    /// Convert an object type to a byte array.
    pub fn to_data(self) -> Result<Vec<u8>, GitError> {
        match self {
            ObjectType::Blob => Ok(vec![0x62, 0x6c, 0x6f, 0x62]), // blob
            ObjectType::Tree => Ok(vec![0x74, 0x72, 0x65, 0x65]), // tree
            ObjectType::Commit => Ok(vec![0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74]), // commit
            ObjectType::Tag => Ok(vec![0x74, 0x61, 0x67]),        // tag
            ObjectType::ContextSnapshot => Ok(vec![0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74]), // snapshot
            ObjectType::Decision => Ok(vec![0x64, 0x65, 0x63, 0x69, 0x73, 0x69, 0x6f, 0x6e]), // decision
            ObjectType::Evidence => Ok(vec![0x65, 0x76, 0x69, 0x64, 0x65, 0x6e, 0x63, 0x65]), // evidence
            ObjectType::PatchSet => Ok(vec![0x70, 0x61, 0x74, 0x63, 0x68, 0x73, 0x65, 0x74]), // patchset
            ObjectType::Plan => Ok(vec![0x70, 0x6c, 0x61, 0x6e]), // plan
            ObjectType::Provenance => Ok(vec![
                0x70, 0x72, 0x6f, 0x76, 0x65, 0x6e, 0x61, 0x6e, 0x63, 0x65,
            ]), // provenance
            ObjectType::Run => Ok(vec![0x72, 0x75, 0x6e]),        // run
            ObjectType::Task => Ok(vec![0x74, 0x61, 0x73, 0x6b]), // task
            ObjectType::Intent => Ok(vec![0x69, 0x6e, 0x74, 0x65, 0x6e, 0x74]), // intent
            ObjectType::ToolInvocation => Ok(vec![
                0x69, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
            ]), // invocation
            _ => Err(GitError::InvalidObjectType(self.to_string())),
        }
    }

    /// Convert an object type to a number.
    pub fn to_u8(&self) -> u8 {
        match self {
            ObjectType::Commit => 1,
            ObjectType::Tree => 2,
            ObjectType::Blob => 3,
            ObjectType::Tag => 4,
            ObjectType::OffsetZstdelta => 5, // Type 5 is reserved in standard Git packs; we use it for Zstd delta objects.
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
        }
    }

    /// Convert a number to an object type.
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
            _ => Err(GitError::InvalidObjectType(format!(
                "Invalid object type number: {number}"
            ))),
        }
    }

    pub fn is_base(&self) -> bool {
        match self {
            ObjectType::Commit => true,
            ObjectType::Tree => true,
            ObjectType::Blob => true,
            ObjectType::Tag => true,
            ObjectType::HashDelta => false,
            ObjectType::OffsetZstdelta => false,
            ObjectType::OffsetDelta => false,
            ObjectType::ContextSnapshot => true,
            ObjectType::Decision => true,
            ObjectType::Evidence => true,
            ObjectType::PatchSet => true,
            ObjectType::Plan => true,
            ObjectType::Provenance => true,
            ObjectType::Run => true,
            ObjectType::Task => true,
            ObjectType::Intent => true,
            ObjectType::ToolInvocation => true,
        }
    }
}

/// Actor kind enum
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

/// Actor reference (who created/triggered).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorRef {
    /// Kind: human/agent/system/mcp_client
    kind: ActorKind,
    /// Subject ID (user/agent name or client ID)
    id: String,
    /// Display name (optional)
    display_name: Option<String>,
    /// Auth context (optional, Libra usually empty)
    auth_context: Option<String>,
}

impl ActorRef {
    /// Create a new ActorRef with validation.
    pub fn new(kind: impl Into<ActorKind>, id: impl Into<String>) -> Result<Self, String> {
        let id_str = id.into();
        if id_str.trim().is_empty() {
            return Err("Actor ID cannot be empty".to_string());
        }
        Ok(Self {
            kind: kind.into(),
            id: id_str,
            display_name: None,
            auth_context: None,
        })
    }

    /// Create an MCP client actor reference (MCP writes must use this).
    pub fn new_for_mcp(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::McpClient, id)
    }

    /// Validate that this actor is an MCP client.
    pub fn ensure_mcp_client(&self) -> Result<(), String> {
        if self.kind != ActorKind::McpClient {
            return Err("MCP writes must use mcp_client actor kind".to_string());
        }
        Ok(())
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

    pub fn auth_context(&self) -> Option<&str> {
        self.auth_context.as_deref()
    }

    pub fn set_display_name(&mut self, display_name: Option<String>) {
        self.display_name = display_name;
    }

    pub fn set_auth_context(&mut self, auth_context: Option<String>) {
        self.auth_context = auth_context;
    }

    /// Create a human actor reference.
    pub fn human(id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::Human, id)
    }

    /// Create an agent actor reference.
    pub fn agent(name: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::Agent, name)
    }

    /// Create a system component actor reference.
    pub fn system(component: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::System, component)
    }

    /// Create an MCP client actor reference.
    pub fn mcp_client(client_id: impl Into<String>) -> Result<Self, String> {
        Self::new(ActorKind::McpClient, client_id)
    }
}

/// Artifact reference (external content).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    /// Store type: local_fs/s3
    store: String,
    /// Storage key (e.g., path or object key)
    key: String,
    /// MIME type (optional)
    content_type: Option<String>,
    /// Size in bytes (optional)
    size_bytes: Option<u64>,
    /// Content hash (strongly recommended)
    hash: Option<IntegrityHash>,
    /// Expiration time (optional)
    expires_at: Option<DateTime<Utc>>,
}

impl ArtifactRef {
    pub fn new(store: impl Into<String>, key: impl Into<String>) -> Result<Self, String> {
        let store = store.into();
        let key = key.into();
        if store.trim().is_empty() {
            return Err("store cannot be empty".to_string());
        }
        if key.trim().is_empty() {
            return Err("key cannot be empty".to_string());
        }
        Ok(Self {
            store,
            key,
            content_type: None,
            size_bytes: None,
            hash: None,
            expires_at: None,
        })
    }

    pub fn store(&self) -> &str {
        &self.store
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    pub fn hash(&self) -> Option<&IntegrityHash> {
        self.hash.as_ref()
    }

    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }

    /// Calculate hash for the given content bytes.
    pub fn compute_hash(content: &[u8]) -> IntegrityHash {
        IntegrityHash::compute(content)
    }

    /// Set the hash directly.
    pub fn with_hash(mut self, hash: IntegrityHash) -> Self {
        self.hash = Some(hash);
        self
    }

    /// Set the hash from a hex string.
    pub fn with_hash_hex(mut self, hash: impl AsRef<str>) -> Result<Self, String> {
        let hash = hash.as_ref().parse()?;
        self.hash = Some(hash);
        Ok(self)
    }

    pub fn set_content_type(&mut self, content_type: Option<String>) {
        self.content_type = content_type;
    }

    pub fn set_size_bytes(&mut self, size_bytes: Option<u64>) {
        self.size_bytes = size_bytes;
    }

    pub fn set_expires_at(&mut self, expires_at: Option<DateTime<Utc>>) {
        self.expires_at = expires_at;
    }

    /// Verify if the provided content matches the stored checksum
    #[must_use = "handle integrity verification result"]
    pub fn verify_integrity(&self, content: &[u8]) -> Result<bool, String> {
        let stored_hash = self
            .hash
            .as_ref()
            .ok_or_else(|| "No hash stored in ArtifactRef".to_string())?;

        Ok(IntegrityHash::compute(content) == *stored_hash)
    }

    /// Check if two artifacts have the same content based on checksum
    #[must_use]
    pub fn content_eq(&self, other: &Self) -> Option<bool> {
        match (&self.hash, &other.hash) {
            (Some(a), Some(b)) => Some(a == b),
            _ => None,
        }
    }

    /// Check if the artifact has expired
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            expires_at < Utc::now()
        } else {
            false
        }
    }
}

fn default_updated_at() -> DateTime<Utc> {
    Utc::now()
}

/// Header shared by all AI Process Objects.
///
/// Contains standard metadata like ID, type, creator, and timestamps.
///
/// # Usage
///
/// Every AI object struct should flatten this header:
///
/// ```rust,ignore
/// #[derive(Serialize, Deserialize)]
/// pub struct MyObject {
///     #[serde(flatten)]
///     header: Header,
///     // specific fields...
/// }
/// ```

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Header {
    /// Global unique ID (UUID v7)
    object_id: Uuid,
    /// Object type (task/run/patchset/...)
    object_type: ObjectType,
    /// Model version
    schema_version: u32,
    /// Repository identifier
    repo_id: Uuid,
    /// Creation time
    created_at: DateTime<Utc>,
    #[serde(default = "default_updated_at")]
    updated_at: DateTime<Utc>,
    /// Creator
    created_by: ActorRef,
    /// Visibility (fixed to private for Libra)
    visibility: Visibility,
    /// Search tags
    #[serde(default)]
    tags: HashMap<String, String>,
    /// External ID mapping
    #[serde(default)]
    external_ids: HashMap<String, String>,
    /// Content checksum (optional)
    #[serde(default)]
    checksum: Option<IntegrityHash>,
}

impl Header {
    /// Create a new Header with default values.
    ///
    /// # Arguments
    ///
    /// * `object_type` - The specific type of the AI object.
    /// * `repo_id` - The UUID of the repository this object belongs to.
    /// * `created_by` - The actor (human/agent) creating this object.
    pub fn new(
        object_type: ObjectType,
        repo_id: Uuid,
        created_by: ActorRef,
    ) -> Result<Self, String> {
        let now = Utc::now();
        Ok(Self {
            object_id: Uuid::now_v7(),
            object_type,
            schema_version: 1,
            repo_id,
            created_at: now,
            updated_at: now,
            created_by,
            visibility: Visibility::Private,
            tags: HashMap::new(),
            external_ids: HashMap::new(),
            checksum: None,
        })
    }

    pub fn object_id(&self) -> Uuid {
        self.object_id
    }

    pub fn object_type(&self) -> &ObjectType {
        &self.object_type
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn repo_id(&self) -> Uuid {
        self.repo_id
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn created_by(&self) -> &ActorRef {
        &self.created_by
    }

    pub fn visibility(&self) -> &Visibility {
        &self.visibility
    }

    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    pub fn tags_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.tags
    }

    pub fn external_ids(&self) -> &HashMap<String, String> {
        &self.external_ids
    }

    pub fn external_ids_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.external_ids
    }

    pub fn set_object_id(&mut self, object_id: Uuid) {
        self.object_id = object_id;
    }

    pub fn set_object_type(&mut self, object_type: ObjectType) -> Result<(), String> {
        self.object_type = object_type;
        Ok(())
    }

    pub fn set_schema_version(&mut self, schema_version: u32) -> Result<(), String> {
        if schema_version == 0 {
            return Err("schema_version must be greater than 0".to_string());
        }
        self.schema_version = schema_version;
        Ok(())
    }

    pub fn set_created_at(&mut self, created_at: DateTime<Utc>) {
        self.created_at = created_at;
    }

    pub fn set_updated_at(&mut self, updated_at: DateTime<Utc>) {
        self.updated_at = updated_at;
    }

    pub fn set_visibility(&mut self, visibility: Visibility) {
        self.visibility = visibility;
    }

    /// Accessor for checksum
    pub fn checksum(&self) -> Option<&IntegrityHash> {
        self.checksum.as_ref()
    }

    /// Seal the header by calculating and setting the checksum of the provided object.
    /// The checksum field is temporarily cleared to keep sealing idempotent.
    ///
    /// This is typically called just before storing the object to ensure `checksum` matches content.
    pub fn seal<T: Serialize>(&mut self, object: &T) -> Result<(), serde_json::Error> {
        let previous_checksum = self.checksum.take();
        match compute_integrity_hash(object) {
            Ok(checksum) => {
                self.checksum = Some(checksum);
                self.updated_at = Utc::now();
                Ok(())
            }
            Err(err) => {
                self.checksum = previous_checksum;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use crate::internal::object::types::{
        ActorKind, ActorRef, ArtifactRef, Header, IntegrityHash, ObjectType,
    };

    /// Verify ObjectType::Blob converts to its ASCII byte representation "blob".
    #[test]
    fn test_object_type_to_data() {
        let blob = ObjectType::Blob;
        let blob_bytes = blob.to_data().unwrap();
        assert_eq!(blob_bytes, vec![0x62, 0x6c, 0x6f, 0x62]);
    }

    /// Verify parsing "tree" string returns ObjectType::Tree.
    #[test]
    fn test_object_type_from_string() {
        assert_eq!(ObjectType::from_string("blob").unwrap(), ObjectType::Blob);
        assert_eq!(ObjectType::from_string("tree").unwrap(), ObjectType::Tree);
        assert_eq!(
            ObjectType::from_string("commit").unwrap(),
            ObjectType::Commit
        );
        assert_eq!(ObjectType::from_string("tag").unwrap(), ObjectType::Tag);
        assert_eq!(
            ObjectType::from_string("snapshot").unwrap(),
            ObjectType::ContextSnapshot
        );
        assert_eq!(
            ObjectType::from_string("decision").unwrap(),
            ObjectType::Decision
        );
        assert_eq!(
            ObjectType::from_string("evidence").unwrap(),
            ObjectType::Evidence
        );
        assert_eq!(
            ObjectType::from_string("patchset").unwrap(),
            ObjectType::PatchSet
        );
        assert_eq!(ObjectType::from_string("plan").unwrap(), ObjectType::Plan);
        assert_eq!(
            ObjectType::from_string("provenance").unwrap(),
            ObjectType::Provenance
        );
        assert_eq!(ObjectType::from_string("run").unwrap(), ObjectType::Run);
        assert_eq!(ObjectType::from_string("task").unwrap(), ObjectType::Task);
        assert_eq!(
            ObjectType::from_string("invocation").unwrap(),
            ObjectType::ToolInvocation
        );

        assert!(ObjectType::from_string("invalid_type").is_err());
    }

    /// Verify ObjectType::Commit converts to pack type number 1.
    #[test]
    fn test_object_type_to_u8() {
        let commit = ObjectType::Commit;
        let commit_number = commit.to_u8();
        assert_eq!(commit_number, 1);
    }

    /// Verify pack type number 4 parses to ObjectType::Tag.
    #[test]
    fn test_object_type_from_u8() {
        let tag_number = 4;
        let tag = ObjectType::from_u8(tag_number).unwrap();
        assert_eq!(tag, ObjectType::Tag);
    }

    #[test]
    fn test_header_serialization() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let header = Header::new(ObjectType::Task, repo_id, actor).expect("header");

        let json = serde_json::to_string(&header).unwrap();
        let deserialized: Header = serde_json::from_str(&json).unwrap();

        assert_eq!(header.object_id(), deserialized.object_id());
        assert_eq!(header.object_type(), deserialized.object_type());
        assert_eq!(header.repo_id(), deserialized.repo_id());
    }

    #[test]
    fn test_actor_ref() {
        let actor = ActorRef::agent("coder").expect("actor");
        assert_eq!(actor.kind(), &ActorKind::Agent);
        assert_eq!(actor.id(), "coder");

        let sys = ActorRef::system("scheduler").expect("system");
        assert_eq!(sys.kind(), &ActorKind::System);

        let client = ActorRef::mcp_client("vscode").expect("client");
        assert_eq!(client.kind(), &ActorKind::McpClient);
        assert!(client.ensure_mcp_client().is_ok());

        let non_mcp = ActorRef::human("jackie").expect("actor");
        assert!(non_mcp.ensure_mcp_client().is_err());
    }

    #[test]
    fn test_actor_kind_serialization() {
        let k = ActorKind::McpClient;
        let s = serde_json::to_string(&k).unwrap();
        assert_eq!(s, "\"mcp_client\"");

        let k2: ActorKind = serde_json::from_str("\"system\"").unwrap();
        assert_eq!(k2, ActorKind::System);
    }

    #[test]
    fn test_header_checksum() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let mut header = Header::new(ObjectType::Task, repo_id, actor).expect("header");
        // Fix time for deterministic checksum
        header.set_created_at(
            DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        header.set_object_id(Uuid::from_u128(0x00000000000000000000000000000001));

        let checksum =
            crate::internal::object::integrity::compute_integrity_hash(&header).expect("checksum");
        assert_eq!(checksum.to_hex().len(), 64); // SHA256 length

        // Ensure changes change checksum
        header
            .set_object_type(ObjectType::Run)
            .expect("object_type");
        let checksum2 =
            crate::internal::object::integrity::compute_integrity_hash(&header).expect("checksum");
        assert_ne!(checksum, checksum2);
    }

    #[test]
    fn test_artifact_checksum() {
        let content = b"hello world";
        let hash = ArtifactRef::compute_hash(content);
        // echo -n "hello world" | shasum -a 256
        let expected_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        assert_eq!(hash.to_hex(), expected_str);

        let artifact = ArtifactRef::new("s3", "key")
            .expect("artifact")
            .with_hash(hash);
        assert_eq!(artifact.hash(), Some(&hash));

        // Integrity check
        assert!(artifact.verify_integrity(content).unwrap());
        assert!(!artifact.verify_integrity(b"wrong").unwrap());

        // Deduplication
        let artifact2 = ArtifactRef::new("local", "other/path")
            .expect("artifact")
            .with_hash(IntegrityHash::compute(content));
        assert_eq!(artifact.content_eq(&artifact2), Some(true));

        let artifact3 = ArtifactRef::new("s3", "diff")
            .expect("artifact")
            .with_hash(ArtifactRef::compute_hash(b"diff"));
        assert_eq!(artifact.content_eq(&artifact3), Some(false));
    }

    #[test]
    fn test_invalid_checksum() {
        let result = ArtifactRef::new("s3", "key")
            .expect("artifact")
            .with_hash_hex("bad_hash");
        assert!(result.is_err());
    }

    #[test]
    fn test_header_seal() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let mut header = Header::new(ObjectType::Task, repo_id, actor).expect("header");

        let content = serde_json::json!({"key": "value"});
        header.seal(&content).expect("seal");

        assert!(header.checksum().is_some());
        let expected =
            crate::internal::object::integrity::compute_integrity_hash(&content).expect("checksum");
        assert_eq!(header.checksum().expect("checksum"), &expected);
    }

    #[test]
    fn test_header_updated_at_on_seal() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let mut header = Header::new(ObjectType::Task, repo_id, actor).expect("header");

        let before = header.updated_at();
        let content = serde_json::json!({"key": "value"});

        header.seal(&content).expect("seal");

        let after = header.updated_at();
        assert!(after >= before);
    }

    #[test]
    fn test_empty_actor_id() {
        let result = ActorRef::new(ActorKind::Human, "  ");
        assert!(result.is_err());
    }

    #[test]
    fn test_artifact_expiration() {
        let mut artifact = ArtifactRef::new("s3", "key").expect("artifact");
        assert!(!artifact.is_expired());

        artifact.set_expires_at(Some(Utc::now() - chrono::Duration::hours(1)));
        assert!(artifact.is_expired());

        artifact.set_expires_at(Some(Utc::now() + chrono::Duration::hours(1)));
        assert!(!artifact.is_expired());
    }
}
