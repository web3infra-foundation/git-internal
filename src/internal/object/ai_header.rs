use std::{collections::HashMap, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ai_hash::{IntegrityHash, compute_integrity_hash};

/// Visibility of an AI process object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    Private,
    Public,
}

/// AI process object type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AiObjectType {
    Task,
    Run,
    PatchSet,
    ContextSnapshot,
    ToolInvocation,
    Plan,
    Evidence,
    Provenance,
    Decision,
}

impl AiObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AiObjectType::Task => "task",
            AiObjectType::Run => "run",
            AiObjectType::PatchSet => "patchset",
            AiObjectType::ContextSnapshot => "context_snapshot",
            AiObjectType::ToolInvocation => "tool_invocation",
            AiObjectType::Plan => "plan",
            AiObjectType::Evidence => "evidence",
            AiObjectType::Provenance => "provenance",
            AiObjectType::Decision => "decision",
        }
    }
}

impl fmt::Display for AiObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Header shared by all AI Process Objects.
///
/// Contains standard metadata like ID, type, creator, and timestamps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Header {
    /// Global unique ID (UUID v7)
    object_id: Uuid,
    /// Object type (task/run/patchset/...)
    object_type: AiObjectType,
    /// Model version
    schema_version: u32,
    /// Repository identifier
    repo_id: Uuid,
    /// Creation time
    created_at: DateTime<Utc>,
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
    pub fn new(
        object_type: AiObjectType,
        repo_id: Uuid,
        created_by: ActorRef,
    ) -> Result<Self, String> {
        Ok(Self {
            object_id: Uuid::now_v7(),
            object_type,
            schema_version: 1,
            repo_id,
            created_at: Utc::now(),
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

    pub fn object_type(&self) -> &AiObjectType {
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

    pub fn set_object_type(&mut self, object_type: AiObjectType) -> Result<(), String> {
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

    pub fn set_visibility(&mut self, visibility: Visibility) {
        self.visibility = visibility;
    }

    /// Accessor for checksum
    pub fn checksum(&self) -> Option<&IntegrityHash> {
        self.checksum.as_ref()
    }

    /// Seal the header by calculating and setting the checksum of the provided object.
    /// The checksum field is temporarily cleared to keep sealing idempotent.
    pub fn seal<T: Serialize>(&mut self, object: &T) -> Result<(), serde_json::Error> {
        let previous = self.checksum.take();
        match compute_integrity_hash(object) {
            Ok(checksum) => {
                self.checksum = Some(checksum);
                Ok(())
            }
            Err(err) => {
                self.checksum = previous;
                Err(err)
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_serialization() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let header = Header::new(AiObjectType::Task, repo_id, actor).expect("header");

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
        let mut header = Header::new(AiObjectType::Task, repo_id, actor).expect("header");
        // Fix time for deterministic checksum
        header.set_created_at(
            DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        header.set_object_id(Uuid::from_u128(0x00000000000000000000000000000001));

        let checksum = compute_integrity_hash(&header).expect("checksum");
        assert_eq!(checksum.to_hex().len(), 64); // SHA256 length

        // Ensure changes change checksum
        header
            .set_object_type(AiObjectType::Run)
            .expect("object_type");
        let checksum2 = compute_integrity_hash(&header).expect("checksum");
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
        let mut header = Header::new(AiObjectType::Task, repo_id, actor).expect("header");

        let content = serde_json::json!({"key": "value"});
        header.seal(&content).expect("seal");

        assert!(header.checksum().is_some());
        let expected = compute_integrity_hash(&content).expect("checksum");
        assert_eq!(header.checksum().expect("checksum"), &expected);
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
