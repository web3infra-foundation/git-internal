use std::{collections::HashMap, fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::checksum::Checksum;

/// Header shared by all AI Process Objects
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Header {
    /// Global unique ID (UUID v7)
    pub object_id: Uuid,
    /// Object type (task/run/patchset/...)
    pub object_type: String,
    /// Model version
    pub schema_version: u32,
    /// Repository identifier
    pub repo_id: Uuid,
    /// Creation time
    pub created_at: DateTime<Utc>,
    /// Creator
    pub created_by: ActorRef,
    /// Visibility (fixed to private for Libra)
    pub visibility: String,
    /// Search tags
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// External ID mapping
    #[serde(default)]
    pub external_ids: HashMap<String, String>,
    /// Content checksum (optional)
    #[serde(default)]
    pub checksum: Option<Checksum>,
}

impl Header {
    pub fn new(object_type: impl Into<String>, repo_id: Uuid, created_by: ActorRef) -> Self {
        Self {
            object_id: Uuid::now_v7(),
            object_type: object_type.into(),
            schema_version: 1,
            repo_id,
            created_at: Utc::now(),
            created_by,
            visibility: "private".to_string(),
            tags: HashMap::new(),
            external_ids: HashMap::new(),
            checksum: None,
        }
    }

    /// Accessor for checksum
    pub fn checksum(&self) -> Option<&Checksum> {
        self.checksum.as_ref()
    }

    /// Seal the header by calculating and setting the checksum of the provided object.
    /// This is typically called before persisting the object.
    pub fn seal<T: Serialize>(&mut self, object: &T) {
        self.checksum = Some(Checksum::compute_json(object));
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

impl FromStr for ActorKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "human" => Ok(ActorKind::Human),
            "agent" => Ok(ActorKind::Agent),
            "system" => Ok(ActorKind::System),
            "mcp_client" => Ok(ActorKind::McpClient),
            _ => Ok(ActorKind::Other(s.to_string())),
        }
    }
}

impl From<String> for ActorKind {
    fn from(s: String) -> Self {
        ActorKind::from_str(&s).unwrap()
    }
}

impl From<&str> for ActorKind {
    fn from(s: &str) -> Self {
        ActorKind::from_str(s).unwrap()
    }
}

/// Actor reference (who created/triggered)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorRef {
    /// Kind: human/agent/system/mcp_client
    pub kind: ActorKind,
    /// Subject ID (user/agent name or client ID)
    pub id: String,
    /// Display name (optional)
    pub display_name: Option<String>,
    /// Auth context (optional, Libra usually empty)
    pub auth_context: Option<String>,
}

impl ActorRef {
    /// Create a new ActorRef with validation.
    pub fn new(kind: impl Into<ActorKind>, id: impl Into<String>) -> Self {
        let id_str = id.into();
        if id_str.trim().is_empty() {
            panic!("Actor ID cannot be empty");
        }
        Self {
            kind: kind.into(),
            id: id_str,
            display_name: None,
            auth_context: None,
        }
    }

    /// Create a human actor reference.
    pub fn human(id: impl Into<String>) -> Self {
        Self::new(ActorKind::Human, id)
    }

    /// Create an agent actor reference.
    pub fn agent(name: impl Into<String>) -> Self {
        Self::new(ActorKind::Agent, name)
    }

    /// Create a system component actor reference.
    pub fn system(component: impl Into<String>) -> Self {
        Self::new(ActorKind::System, component)
    }

    /// Create an MCP client actor reference.
    pub fn mcp_client(client_id: impl Into<String>) -> Self {
        Self::new(ActorKind::McpClient, client_id)
    }
}

/// Artifact reference (external content)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    /// Store type: local_fs/s3
    pub store: String,
    /// Storage key (e.g., path or object key)
    pub key: String,
    /// MIME type (optional)
    pub content_type: Option<String>,
    /// Size in bytes (optional)
    pub size_bytes: Option<u64>,
    /// SHA256 checksum (strongly recommended)
    pub sha256: Option<Checksum>,
    /// Expiration time (optional)
    pub expires_at: Option<DateTime<Utc>>,
}

impl ArtifactRef {
    pub fn new(store: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            store: store.into(),
            key: key.into(),
            content_type: None,
            size_bytes: None,
            sha256: None,
            expires_at: None,
        }
    }

    /// Calculate SHA256 checksum for the given content bytes
    pub fn compute_sha256(content: &[u8]) -> Checksum {
        Checksum::compute(content)
    }

    /// Set the checksum directly with validation
    pub fn with_checksum(mut self, sha256: impl Into<String>) -> Self {
        self.sha256 = Some(Checksum::new(sha256).expect("Invalid checksum format"));
        self
    }

    /// Verify if the provided content matches the stored checksum
    pub fn verify_integrity(&self, content: &[u8]) -> Result<bool, String> {
        let stored_hash = self
            .sha256
            .as_ref()
            .ok_or_else(|| "No checksum stored in ArtifactRef".to_string())?;

        Ok(stored_hash.verify(content))
    }

    /// Check if two artifacts have the same content based on checksum
    pub fn content_eq(&self, other: &Self) -> Option<bool> {
        match (&self.sha256, &other.sha256) {
            (Some(a), Some(b)) => Some(a == b),
            _ => None,
        }
    }

    /// Check if the artifact has expired
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
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie");
        let header = Header::new("task", repo_id, actor);

        let json = serde_json::to_string(&header).unwrap();
        let deserialized: Header = serde_json::from_str(&json).unwrap();

        assert_eq!(header.object_id, deserialized.object_id);
        assert_eq!(header.object_type, deserialized.object_type);
        assert_eq!(header.repo_id, deserialized.repo_id);
    }

    #[test]
    fn test_actor_ref() {
        let actor = ActorRef::agent("coder");
        assert_eq!(actor.kind, ActorKind::Agent);
        assert_eq!(actor.id, "coder");

        let sys = ActorRef::system("scheduler");
        assert_eq!(sys.kind, ActorKind::System);

        let client = ActorRef::mcp_client("vscode");
        assert_eq!(client.kind, ActorKind::McpClient);
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
        let repo_id = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();
        let actor = ActorRef::human("jackie");
        let mut header = Header::new("task", repo_id, actor);
        // Fix time for deterministic checksum
        header.created_at = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        header.object_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();

        let checksum = Checksum::compute_json(&header);
        assert_eq!(checksum.as_str().len(), 64); // SHA256 length

        // Ensure changes change checksum
        header.object_type = "run".to_string();
        let checksum2 = Checksum::compute_json(&header);
        assert_ne!(checksum, checksum2);
    }

    #[test]
    fn test_artifact_checksum() {
        let content = b"hello world";
        let hash = ArtifactRef::compute_sha256(content);
        // echo -n "hello world" | shasum -a 256
        let expected_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        assert_eq!(hash.as_str(), expected_str);

        let artifact = ArtifactRef::new("s3", "key").with_checksum(hash.as_str());
        assert_eq!(artifact.sha256, Some(hash.clone()));

        // Integrity check
        assert!(artifact.verify_integrity(content).unwrap());
        assert!(!artifact.verify_integrity(b"wrong").unwrap());

        // Deduplication
        let artifact2 = ArtifactRef::new("local", "other/path").with_checksum(hash.as_str());
        assert_eq!(artifact.content_eq(&artifact2), Some(true));

        let artifact3 = ArtifactRef::new("s3", "diff")
            .with_checksum(ArtifactRef::compute_sha256(b"diff").as_str());
        assert_eq!(artifact.content_eq(&artifact3), Some(false));
    }

    #[test]
    #[should_panic(expected = "Invalid checksum format")]
    fn test_invalid_checksum() {
        ArtifactRef::new("s3", "key").with_checksum("bad_hash");
    }

    #[test]
    fn test_header_seal() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie");
        let mut header = Header::new("task", repo_id, actor);

        let content = serde_json::json!({"key": "value"});
        header.seal(&content);

        assert!(header.checksum.is_some());
        let expected = Checksum::compute_json(&content);
        assert_eq!(header.checksum.unwrap(), expected);
    }

    #[test]
    #[should_panic(expected = "Actor ID cannot be empty")]
    fn test_empty_actor_id() {
        ActorRef::new(ActorKind::Human, "  ");
    }

    #[test]
    fn test_artifact_expiration() {
        let mut artifact = ArtifactRef::new("s3", "key");
        assert!(!artifact.is_expired());

        artifact.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        assert!(artifact.is_expired());

        artifact.expires_at = Some(Utc::now() + chrono::Duration::hours(1));
        assert!(!artifact.is_expired());
    }
}
