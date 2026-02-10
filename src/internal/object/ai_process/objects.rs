use std::{collections::HashMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::base::{ActorRef, ArtifactRef, Header};

/// Task lifecycle status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Draft,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Draft => "draft",
            TaskStatus::Running => "running",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Task goal category.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GoalType {
    Feature,
    Bugfix,
    Refactor,
    Docs,
    Perf,
    Test,
    Chore,
    Build,
    Ci,
    Style,
}

impl GoalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            GoalType::Feature => "feature",
            GoalType::Bugfix => "bugfix",
            GoalType::Refactor => "refactor",
            GoalType::Docs => "docs",
            GoalType::Perf => "perf",
            GoalType::Test => "test",
            GoalType::Chore => "chore",
            GoalType::Build => "build",
            GoalType::Ci => "ci",
            GoalType::Style => "style",
        }
    }
}

impl fmt::Display for GoalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for GoalType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "feature" => Ok(GoalType::Feature),
            "bugfix" => Ok(GoalType::Bugfix),
            "refactor" => Ok(GoalType::Refactor),
            "docs" => Ok(GoalType::Docs),
            "perf" => Ok(GoalType::Perf),
            "test" => Ok(GoalType::Test),
            "chore" => Ok(GoalType::Chore),
            "build" => Ok(GoalType::Build),
            "ci" => Ok(GoalType::Ci),
            "style" => Ok(GoalType::Style),
            _ => Err(format!("Invalid goal_type: {}", value)),
        }
    }
}

/// Task object describing intent and constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    #[serde(flatten)]
    pub header: Header,
    pub title: String,
    pub description: Option<String>, // Can be text or artifact ref serialized
    pub goal_type: Option<GoalType>, // feature/bugfix/refactor/docs/...
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    pub requested_by: Option<ActorRef>,
    #[serde(default)]
    pub dependencies: Vec<Uuid>,
    pub status: TaskStatus,
}

/// Task Object
impl Task {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        title: impl Into<String>,
        goal_type: Option<GoalType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("task", repo_id, created_by)?,
            title: title.into(),
            description: None,
            goal_type,
            constraints: Vec::new(),
            acceptance_criteria: Vec::new(),
            requested_by: None,
            dependencies: Vec::new(),
            status: TaskStatus::Draft,
        })
    }
}

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

/// Run object for a single orchestration execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    #[serde(flatten)]
    pub header: Header,
    pub task_id: Uuid,
    pub orchestrator_version: String,
    pub base_commit_sha: String,
    pub status: RunStatus,
    pub context_snapshot_id: Option<Uuid>,
    #[serde(default)]
    pub agent_instances: Vec<AgentInstance>,
    pub metrics: Option<serde_json::Value>,
    pub error: Option<String>,
    pub environment: Option<Environment>,
}

/// Environment snapshot of the run host.
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

/// Run Object
impl Run {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        task_id: Uuid,
        base_commit_sha: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("run", repo_id, created_by)?,
            task_id,
            orchestrator_version: "libra-builtin".to_string(),
            base_commit_sha: base_commit_sha.into(),
            status: RunStatus::Created,
            context_snapshot_id: None,
            agent_instances: Vec::new(),
            metrics: None,
            error: None,
            environment: Some(Environment::capture()),
        })
    }
}

/// Patch application status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    Proposed,
    Applied,
    Rejected,
    Superseded,
}

impl ApplyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApplyStatus::Proposed => "proposed",
            ApplyStatus::Applied => "applied",
            ApplyStatus::Rejected => "rejected",
            ApplyStatus::Superseded => "superseded",
        }
    }
}

impl fmt::Display for ApplyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Diff format for patch content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiffFormat {
    UnifiedDiff,
    GitDiff,
}

/// PatchSet object containing a candidate diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchSet {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    pub generation: u32,
    pub base_commit_sha: String,
    pub diff_format: DiffFormat,
    pub diff_artifact: Option<ArtifactRef>,
    #[serde(default)]
    pub touched_files: Vec<TouchedFile>,
    pub rationale: Option<String>,
    pub apply_status: ApplyStatus,
}

/// Touched file summary in a patchset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TouchedFile {
    pub path: String,
    pub change_type: String, // modify/add/delete
    pub lines_added: u32,
    pub lines_deleted: u32,
}

impl TouchedFile {
    pub fn new(
        path: impl Into<String>,
        change_type: impl Into<String>,
        lines_added: u32,
        lines_deleted: u32,
    ) -> Result<Self, String> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err("path cannot be empty".to_string());
        }
        Ok(Self {
            path,
            change_type: change_type.into(),
            lines_added,
            lines_deleted,
        })
    }
}

impl PatchSet {
    /// Create a new patchset object
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        base_commit_sha: impl Into<String>,
        generation: u32,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("patchset", repo_id, created_by)?,
            run_id,
            generation,
            base_commit_sha: base_commit_sha.into(),
            diff_format: DiffFormat::UnifiedDiff,
            diff_artifact: None,
            touched_files: Vec::new(),
            rationale: None,
            apply_status: ApplyStatus::Proposed,
        })
    }
}

/// Selection strategy for context snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    Explicit,
    Heuristic,
}

/// Context snapshot describing selected inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSnapshot {
    #[serde(flatten)]
    pub header: Header,
    pub base_commit_sha: String,
    pub selection_strategy: SelectionStrategy,
    #[serde(default)]
    pub items: Vec<ContextItem>,
    pub summary: Option<String>,
}

impl ContextSnapshot {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        base_commit_sha: impl Into<String>,
        selection_strategy: SelectionStrategy,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("context_snapshot", repo_id, created_by)?,
            base_commit_sha: base_commit_sha.into(),
            selection_strategy,
            items: Vec::new(),
            summary: None,
        })
    }
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
    pub content_hash: String,
}

impl ContextItem {
    pub fn new(
        kind: ContextItemKind,
        path: impl Into<String>,
        content_hash: impl Into<String>,
    ) -> Result<Self, String> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err("path cannot be empty".to_string());
        }
        Ok(Self {
            kind,
            path,
            content_hash: content_hash.into(),
        })
    }
}

/// Tool invocation status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    Ok,
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

/// Tool invocation record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvocation {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    pub tool_name: String,
    pub io_footprint: Option<IoFootprint>,
    #[serde(default)]
    pub args: serde_json::Value,
    pub status: ToolStatus,
    pub result_summary: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactRef>,
}

/// IO footprint of a tool invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoFootprint {
    #[serde(default)]
    pub paths_read: Vec<String>,
    #[serde(default)]
    pub paths_written: Vec<String>,
}

impl ToolInvocation {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        tool_name: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("tool_invocation", repo_id, created_by)?,
            run_id,
            tool_name: tool_name.into(),
            io_footprint: None,
            args: serde_json::Value::Null,
            status: ToolStatus::Ok,
            result_summary: None,
            artifacts: Vec::new(),
        })
    }
}

/// Plan step status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Skipped,
}

impl PlanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanStatus::Pending => "pending",
            PlanStatus::InProgress => "in_progress",
            PlanStatus::Completed => "completed",
            PlanStatus::Failed => "failed",
            PlanStatus::Skipped => "skipped",
        }
    }
}

impl fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Plan object for step decomposition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    /// Plan version starts at 1 and must increase by 1 for each update.
    pub plan_version: u32,
    #[serde(default)]
    pub steps: Vec<PlanStep>,
}

/// Plan step with inputs, outputs, and checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanStep {
    pub intent: String,
    pub inputs: Option<serde_json::Value>,
    pub outputs: Option<serde_json::Value>,
    pub checks: Option<serde_json::Value>,
    pub owner_role: Option<String>,
    pub status: PlanStatus,
}

impl Plan {
    /// Create a new plan object
    pub fn new(repo_id: Uuid, created_by: ActorRef, run_id: Uuid) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("plan", repo_id, created_by)?,
            run_id,
            plan_version: 1,
            steps: Vec::new(),
        })
    }

    pub fn new_next(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        previous_version: u32,
    ) -> Result<Self, String> {
        let next_version = previous_version
            .checked_add(1)
            .ok_or_else(|| "plan_version overflow".to_string())?;
        Ok(Self {
            header: Header::new("plan", repo_id, created_by)?,
            run_id,
            plan_version: next_version,
            steps: Vec::new(),
        })
    }
}

/// Evidence object for test/lint/build results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    pub patchset_id: Option<Uuid>,
    pub kind: String, // test/lint/build
    pub tool: String,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub summary: Option<String>, // passed/failed, error signature
    #[serde(default)]
    pub report_artifacts: Vec<ArtifactRef>,
}

impl Evidence {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        kind: impl Into<String>,
        tool: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("evidence", repo_id, created_by)?,
            run_id,
            patchset_id: None,
            kind: kind.into(),
            tool: tool.into(),
            command: None,
            exit_code: None,
            summary: None,
            report_artifacts: Vec::new(),
        })
    }
}

/// Provenance object for model/provider metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    pub provider: String,
    pub model: String,
    pub parameters: Option<serde_json::Value>,
    pub token_usage: Option<serde_json::Value>,
}

impl Provenance {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("provenance", repo_id, created_by)?,
            run_id,
            provider: provider.into(),
            model: model.into(),
            parameters: None,
            token_usage: None,
        })
    }
}

/// Decision object linking process to outcomes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    #[serde(flatten)]
    pub header: Header,
    pub run_id: Uuid,
    pub decision_type: String, // commit/checkpoint/abandon/retry/rollback
    pub chosen_patchset_id: Option<Uuid>,
    pub result_commit_sha: Option<String>,
    pub checkpoint_id: Option<String>,
    pub rationale: Option<String>,
}

impl Decision {
    /// Create a new decision object
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        run_id: Uuid,
        decision_type: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new("decision", repo_id, created_by)?,
            run_id,
            decision_type: decision_type.into(),
            chosen_patchset_id: None,
            result_commit_sha: None,
            checkpoint_id: None,
            rationale: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task = Task::new(repo_id, actor, "Fix bug", Some(GoalType::Bugfix)).expect("task");

        // Test dependencies
        let dep_id = Uuid::now_v7();
        task.dependencies.push(dep_id);

        assert_eq!(task.header.object_type(), "task");
        assert_eq!(task.status, TaskStatus::Draft);
        assert_eq!(task.goal_type, Some(GoalType::Bugfix));
        assert_eq!(task.dependencies.len(), 1);
        assert_eq!(task.dependencies[0], dep_id);
    }

    #[test]
    fn test_task_goal_type_optional() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let task = Task::new(repo_id, actor, "Write docs", None).expect("task");

        assert!(task.goal_type.is_none());
    }

    #[test]
    fn test_new_objects_creation() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::now_v7();

        // Run with environment (auto captured)
        let run = Run::new(repo_id, actor.clone(), Uuid::now_v7(), "sha123").expect("run");

        let env = run.environment.as_ref().unwrap();
        // Check if it captured real values (assuming we are running on some OS)
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
        assert!(!env.cwd.is_empty());

        // Plan with steps and status
        let mut plan = Plan::new(repo_id, actor.clone(), run_id).expect("plan");
        plan.steps.push(PlanStep {
            intent: "step1".to_string(),
            inputs: None,
            outputs: None,
            checks: None,
            owner_role: None,
            status: PlanStatus::Pending,
        });

        assert_eq!(plan.header.object_type(), "plan");
        assert_eq!(plan.plan_version, 1);
        assert_eq!(plan.steps[0].status, PlanStatus::Pending);

        // Evidence
        let evidence =
            Evidence::new(repo_id, actor.clone(), run_id, "test", "cargo").expect("evidence");
        assert_eq!(evidence.header.object_type(), "evidence");
        assert_eq!(evidence.kind, "test");

        // Provenance
        let provenance =
            Provenance::new(repo_id, actor.clone(), run_id, "openai", "gpt-4").expect("provenance");
        assert_eq!(provenance.header.object_type(), "provenance");
        assert_eq!(provenance.provider, "openai");

        // Decision
        let decision = Decision::new(repo_id, actor.clone(), run_id, "commit").expect("decision");
        assert_eq!(decision.header.object_type(), "decision");
        assert_eq!(decision.decision_type, "commit");
    }

    #[test]
    fn test_task_requested_by() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task =
            Task::new(repo_id, actor.clone(), "Fix bug", Some(GoalType::Bugfix)).expect("task");

        task.requested_by = Some(ActorRef::mcp_client("vscode-client").expect("actor"));

        assert!(task.requested_by.is_some());
        assert_eq!(
            task.requested_by.unwrap().kind(),
            &super::super::base::ActorKind::McpClient
        );
    }

    #[test]
    fn test_tool_invocation_io_footprint() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::now_v7();

        let mut tool_inv =
            ToolInvocation::new(repo_id, actor, run_id, "read_file").expect("tool_invocation");

        let footprint = IoFootprint {
            paths_read: vec!["src/main.rs".to_string()],
            paths_written: vec![],
        };

        tool_inv.io_footprint = Some(footprint);

        assert_eq!(tool_inv.tool_name, "read_file");
        assert!(tool_inv.io_footprint.is_some());
        assert_eq!(tool_inv.io_footprint.unwrap().paths_read[0], "src/main.rs");
    }

    #[test]
    fn test_patchset_creation() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::now_v7();

        let patchset = PatchSet::new(repo_id, actor, run_id, "sha123", 1).expect("patchset");

        assert_eq!(patchset.header.object_type(), "patchset");
        assert_eq!(patchset.generation, 1);
        assert_eq!(patchset.diff_format, DiffFormat::UnifiedDiff);
        assert_eq!(patchset.apply_status, ApplyStatus::Proposed);
        assert!(patchset.touched_files.is_empty());
    }

    #[test]
    fn test_context_snapshot_fields() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");

        let mut snapshot =
            ContextSnapshot::new(repo_id, actor, "sha123", SelectionStrategy::Explicit)
                .expect("snapshot");
        snapshot.summary = Some("core files".to_string());

        snapshot.items.push(
            ContextItem::new(ContextItemKind::File, "src/lib.rs", "abc").expect("context item"),
        );

        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(snapshot.items[0].path, "src/lib.rs");
        assert_eq!(snapshot.summary.as_deref(), Some("core files"));
    }

    #[test]
    fn test_tool_invocation_fields() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::now_v7();

        let mut tool_inv =
            ToolInvocation::new(repo_id, actor, run_id, "apply_patch").expect("tool_invocation");
        tool_inv.status = ToolStatus::Error;
        tool_inv.args = serde_json::json!({"path": "src/lib.rs"});
        tool_inv.result_summary = Some("failed".to_string());
        tool_inv
            .artifacts
            .push(ArtifactRef::new("local", "artifact-key").expect("artifact"));

        assert_eq!(tool_inv.status, ToolStatus::Error);
        assert_eq!(tool_inv.artifacts.len(), 1);
        assert_eq!(tool_inv.args["path"], "src/lib.rs");
    }

    #[test]
    fn test_evidence_fields() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::now_v7();
        let patchset_id = Uuid::now_v7();

        let mut evidence =
            Evidence::new(repo_id, actor, run_id, "test", "cargo").expect("evidence");
        evidence.patchset_id = Some(patchset_id);
        evidence.exit_code = Some(1);
        evidence
            .report_artifacts
            .push(ArtifactRef::new("local", "log.txt").expect("artifact"));

        assert_eq!(evidence.patchset_id, Some(patchset_id));
        assert_eq!(evidence.exit_code, Some(1));
        assert_eq!(evidence.report_artifacts.len(), 1);
    }

    #[test]
    fn test_provenance_fields() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::now_v7();

        let mut provenance =
            Provenance::new(repo_id, actor, run_id, "openai", "gpt-4").expect("provenance");
        provenance.parameters = Some(serde_json::json!({"temperature": 0.2}));
        provenance.token_usage = Some(serde_json::json!({"input": 10, "output": 5}));

        assert!(provenance.parameters.is_some());
        assert!(provenance.token_usage.is_some());
    }

    #[test]
    fn test_decision_fields() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::now_v7();
        let patchset_id = Uuid::now_v7();

        let mut decision = Decision::new(repo_id, actor, run_id, "commit").expect("decision");
        decision.chosen_patchset_id = Some(patchset_id);
        decision.result_commit_sha = Some("abc123".to_string());
        decision.rationale = Some("tests passed".to_string());

        assert_eq!(decision.chosen_patchset_id, Some(patchset_id));
        assert_eq!(decision.result_commit_sha.as_deref(), Some("abc123"));
        assert_eq!(decision.rationale.as_deref(), Some("tests passed"));
    }

    #[test]
    fn test_plan_version_ordering() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::now_v7();

        let plan_v1 = Plan::new(repo_id, actor.clone(), run_id).expect("plan");
        let plan_v2 =
            Plan::new_next(repo_id, actor.clone(), run_id, plan_v1.plan_version).expect("plan");
        let plan_v3 =
            Plan::new_next(repo_id, actor.clone(), run_id, plan_v2.plan_version).expect("plan");

        let mut plans = [plan_v2.clone(), plan_v1.clone(), plan_v3.clone()];
        plans.sort_by_key(|plan| plan.plan_version);

        assert_eq!(plans[0].plan_version, 1);
        assert_eq!(plans[1].plan_version, 2);
        assert_eq!(plans[2].plan_version, 3);

        assert!(plan_v3.plan_version > plan_v2.plan_version);
        assert!(plan_v2.plan_version > plan_v1.plan_version);
    }

    #[test]
    fn ai_process_tool_invocation_artifacts_default() {
        let repo_id = Uuid::now_v7();
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::now_v7();

        let tool_inv =
            ToolInvocation::new(repo_id, actor, run_id, "read_file").expect("tool_invocation");
        let mut value = serde_json::to_value(&tool_inv).unwrap();

        if let serde_json::Value::Object(ref mut map) = value {
            map.remove("artifacts");
        }

        let deserialized: ToolInvocation = serde_json::from_value(value).unwrap();
        assert!(deserialized.artifacts.is_empty());
    }
}
