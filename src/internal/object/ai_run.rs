use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ai_hash::IntegrityHash,
    ai_header::{ActorRef, AiObjectType, Header},
};

/// Run lifecycle status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Created,
    Patching,
    Validating,
    Completed,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Created => "created",
            RunStatus::Patching => "patching",
            RunStatus::Validating => "validating",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        }
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Environment snapshot of the run host.
/// Captured at run creation time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub os: String,   // e.g. "macos", "linux"
    pub arch: String, // e.g. "aarch64", "x86_64"
    pub cwd: String,  // Current working directory
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Environment {
    /// Create a new environment object from the current system environment
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

/// Agent instance participating in a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInstance {
    pub role: String,
    pub provider_route: Option<String>,
}

/// Run object for a single orchestration execution.
/// Links a task to execution state and environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    #[serde(flatten)]
    header: Header,
    task_id: Uuid,
    orchestrator_version: String,
    base_commit_sha: IntegrityHash,
    status: RunStatus,
    context_snapshot_id: Option<Uuid>,
    #[serde(default)]
    agent_instances: Vec<AgentInstance>,
    metrics: Option<serde_json::Value>,
    error: Option<String>,
    environment: Option<Environment>,
}

impl Run {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        task_id: Uuid,
        base_commit_sha: impl AsRef<str>,
    ) -> Result<Self, String> {
        let base_commit_sha = base_commit_sha.as_ref().parse()?;
        Ok(Self {
            header: Header::new(AiObjectType::Run, repo_id, created_by)?,
            task_id,
            orchestrator_version: "libra-builtin".to_string(),
            base_commit_sha,
            status: RunStatus::Created,
            context_snapshot_id: None,
            agent_instances: Vec::new(),
            metrics: None,
            error: None,
            environment: Some(Environment::capture()),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    pub fn orchestrator_version(&self) -> &str {
        &self.orchestrator_version
    }

    pub fn base_commit_sha(&self) -> &IntegrityHash {
        &self.base_commit_sha
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    pub fn context_snapshot_id(&self) -> Option<Uuid> {
        self.context_snapshot_id
    }

    pub fn agent_instances(&self) -> &[AgentInstance] {
        &self.agent_instances
    }

    pub fn metrics(&self) -> Option<&serde_json::Value> {
        self.metrics.as_ref()
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn environment(&self) -> Option<&Environment> {
        self.environment.as_ref()
    }

    pub fn set_status(&mut self, status: RunStatus) {
        self.status = status;
    }

    pub fn set_context_snapshot_id(&mut self, context_snapshot_id: Option<Uuid>) {
        self.context_snapshot_id = context_snapshot_id;
    }

    pub fn add_agent_instance(&mut self, instance: AgentInstance) {
        self.agent_instances.push(instance);
    }

    pub fn set_metrics(&mut self, metrics: Option<serde_json::Value>) {
        self.metrics = metrics;
    }

    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_hash_hex() -> String {
        IntegrityHash::compute(b"ai-process-test").to_hex()
    }

    #[test]
    fn test_new_objects_creation() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let base_hash = test_hash_hex();

        // Run with environment (auto captured)
        let run = Run::new(repo_id, actor.clone(), Uuid::from_u128(0x1), &base_hash).expect("run");

        let env = run.environment().unwrap();
        // Check if it captured real values (assuming we are running on some OS)
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
        assert!(!env.cwd.is_empty());
    }
}
