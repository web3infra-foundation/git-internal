//! AI Task Definition
//!
//! A `Task` represents a unit of work to be performed by an AI agent.
//! It serves as the root of the AI workflow, defining intent, constraints, and success criteria.
//!
//! # Lifecycle
//!
//! 1. **Draft**: Initial state. Task is being defined.
//! 2. **Running**: An agent (via a `Run` object) has started working on it.
//! 3. **Done**: Work is completed and verified.
//! 4. **Failed**: Work could not be completed.
//! 5. **Cancelled**: User aborted the task.
//!
//! # Relationships
//!
//! - **Parent**: None (Root object).
//! - **Children**: `Run` (1-to-many). A task can have multiple runs (retries).
//! - **Dependencies**: Can depend on other Tasks via `dependencies`.
//!
//! # Example
//!
//! ```rust
//! use git_internal::internal::object::task::{Task, GoalType};
//! use git_internal::internal::object::types::ActorRef;
//! use uuid::Uuid;
//!
//! let repo_id = Uuid::new_v4();
//! let actor = ActorRef::human("user").unwrap();
//! let mut task = Task::new(repo_id, actor, "Refactor Login", Some(GoalType::Refactor)).unwrap();
//!
//! task.add_constraint("Must use JWT");
//! task.add_acceptance_criterion("All tests pass");
//! ```

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        types::{ActorRef, Header, ObjectType},
    },
};

/// Task lifecycle status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Initial state, definition in progress.
    Draft,
    /// Agent is actively working on this task.
    Running,
    /// Task completed successfully.
    Done,
    /// Task failed to complete.
    Failed,
    /// Task was cancelled by user.
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
///
/// Helps agents understand the nature of the work.
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
/// Typically created first, then referenced by Run objects.
///
/// See module documentation for lifecycle details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    #[serde(flatten)]
    header: Header,
    title: String,
    description: Option<String>,
    goal_type: Option<GoalType>,
    #[serde(default)]
    constraints: Vec<String>,
    #[serde(default)]
    acceptance_criteria: Vec<String>,
    requested_by: Option<ActorRef>,
    intent_id: Option<Uuid>,
    #[serde(default)]
    dependencies: Vec<Uuid>,
    status: TaskStatus,
}

impl Task {
    /// Create a new Task.
    ///
    /// # Arguments
    /// * `repo_id` - Repository UUID
    /// * `created_by` - Actor creating the task
    /// * `title` - Short summary of the task
    /// * `goal_type` - Optional classification (Feature, Bugfix, etc.)
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        title: impl Into<String>,
        goal_type: Option<GoalType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Task, repo_id, created_by)?,
            title: title.into(),
            description: None,
            goal_type,
            constraints: Vec::new(),
            acceptance_criteria: Vec::new(),
            requested_by: None,
            intent_id: None,
            dependencies: Vec::new(),
            status: TaskStatus::Draft,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn goal_type(&self) -> Option<&GoalType> {
        self.goal_type.as_ref()
    }

    pub fn constraints(&self) -> &[String] {
        &self.constraints
    }

    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    pub fn requested_by(&self) -> Option<&ActorRef> {
        self.requested_by.as_ref()
    }

    pub fn intent_id(&self) -> Option<Uuid> {
        self.intent_id
    }

    pub fn dependencies(&self) -> &[Uuid] {
        &self.dependencies
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    pub fn add_constraint(&mut self, constraint: impl Into<String>) {
        self.constraints.push(constraint.into());
    }

    pub fn add_acceptance_criterion(&mut self, criterion: impl Into<String>) {
        self.acceptance_criteria.push(criterion.into());
    }

    pub fn set_requested_by(&mut self, requested_by: Option<ActorRef>) {
        self.requested_by = requested_by;
    }

    pub fn set_intent_id(&mut self, intent_id: Option<Uuid>) {
        self.intent_id = intent_id;
    }

    pub fn add_dependency(&mut self, task_id: Uuid) {
        self.dependencies.push(task_id);
    }

    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task: {}", self.header.object_id())
    }
}

impl ObjectTrait for Task {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Task
    }

    fn get_size(&self) -> usize {
        serde_json::to_vec(self).map(|v| v.len()).unwrap_or(0)
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::object::types::ActorKind;

    #[test]
    fn test_task_creation() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task = Task::new(repo_id, actor, "Fix bug", Some(GoalType::Bugfix)).expect("task");

        // Test dependencies
        let dep_id = Uuid::from_u128(0x00000000000000000000000000000001);
        task.add_dependency(dep_id);

        assert_eq!(task.header().object_type(), &ObjectType::Task);
        assert_eq!(task.status(), &TaskStatus::Draft);
        assert_eq!(task.goal_type(), Some(&GoalType::Bugfix));
        assert_eq!(task.dependencies().len(), 1);
        assert_eq!(task.dependencies()[0], dep_id);
        assert!(task.intent_id().is_none());
    }

    #[test]
    fn test_task_goal_type_optional() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let task = Task::new(repo_id, actor, "Write docs", None).expect("task");

        assert!(task.goal_type().is_none());
    }

    #[test]
    fn test_task_requested_by() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let mut task =
            Task::new(repo_id, actor.clone(), "Fix bug", Some(GoalType::Bugfix)).expect("task");

        task.set_requested_by(Some(ActorRef::mcp_client("vscode-client").expect("actor")));

        assert!(task.requested_by().is_some());
        assert_eq!(task.requested_by().unwrap().kind(), &ActorKind::McpClient);
    }
}
