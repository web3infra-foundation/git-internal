//! AI Task snapshot.
//!
//! `Task` stores a stable unit of work derived from a plan or created by
//! Libra as a delegated work item.
//!
//! # How to use this object
//!
//! - Create a `Task` when a `PlanStep` needs its own durable execution
//!   unit.
//! - Fill `parent`, `intent`, `origin_step_id`, and `dependencies`
//!   before persistence if those provenance links are known.
//! - Keep the stored object stable; define a new task snapshot only when
//!   the work definition itself changes.
//!
//! # How it works with other objects
//!
//! - `Task.origin_step_id` links the task back to the stable
//!   `PlanStep.step_id`.
//! - `Run.task` links execution attempts to the task.
//! - `TaskEvent` records lifecycle changes such as running / blocked /
//!   done / failed.
//!
//! # How Libra should call it
//!
//! Libra should derive ready queues, dependency resolution, and current
//! task status from `Task` plus `TaskEvent` and `Run` history. Those
//! mutable scheduling views do not belong on the `Task` object itself.

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
    Other(String),
}

impl GoalType {
    /// Return the canonical snake_case storage/display form for the goal
    /// type.
    pub fn as_str(&self) -> &str {
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
            GoalType::Other(value) => value.as_str(),
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
            _ => Ok(GoalType::Other(value.to_string())),
        }
    }
}

/// Stable work definition used by the scheduler and execution layer.
///
/// `Task` answers "what work should be done?" rather than "what is the
/// current runtime status?". Current status is reconstructed from event
/// history in Libra.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Short task title suitable for queues and summaries.
    title: String,
    /// Optional longer-form explanation of the work item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Optional coarse work classification used by Libra and UI layers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal: Option<GoalType>,
    /// Explicit constraints that the executor must respect.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    constraints: Vec<String>,
    /// Concrete acceptance criteria that define task completion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    acceptance_criteria: Vec<String>,
    /// Optional actor on whose behalf the task was requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requester: Option<ActorRef>,
    /// Optional parent task id when the task was decomposed from another
    /// durable task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Uuid>,
    /// Optional originating intent id for cross-object provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    intent: Option<Uuid>,
    /// Optional stable plan-step id that originally spawned this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin_step_id: Option<Uuid>,
    /// Other task ids that must complete before this task becomes ready.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependencies: Vec<Uuid>,
}

impl Task {
    /// Create a new task definition with the given title and optional
    /// goal type.
    pub fn new(
        created_by: ActorRef,
        title: impl Into<String>,
        goal: Option<GoalType>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Task, created_by)?,
            title: title.into(),
            description: None,
            goal,
            constraints: Vec::new(),
            acceptance_criteria: Vec::new(),
            requester: None,
            parent: None,
            intent: None,
            origin_step_id: None,
            dependencies: Vec::new(),
        })
    }

    /// Return the immutable header for this task definition.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the short task title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Return the longer-form task description, if present.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Return the coarse work classification, if present.
    pub fn goal(&self) -> Option<&GoalType> {
        self.goal.as_ref()
    }

    /// Return the explicit task constraints.
    pub fn constraints(&self) -> &[String] {
        &self.constraints
    }

    /// Return the acceptance criteria for task completion.
    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    /// Return the requesting actor, if one was stored.
    pub fn requester(&self) -> Option<&ActorRef> {
        self.requester.as_ref()
    }

    /// Return the parent task id, if this task was derived from another
    /// task.
    pub fn parent(&self) -> Option<Uuid> {
        self.parent
    }

    /// Return the originating intent id, if present.
    pub fn intent(&self) -> Option<Uuid> {
        self.intent
    }

    /// Return the stable plan-step id that originally spawned this task,
    /// if present.
    pub fn origin_step_id(&self) -> Option<Uuid> {
        self.origin_step_id
    }

    /// Return the task dependency list.
    pub fn dependencies(&self) -> &[Uuid] {
        &self.dependencies
    }

    /// Set or clear the long-form task description.
    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    /// Append one execution constraint to this task definition.
    pub fn add_constraint(&mut self, constraint: impl Into<String>) {
        self.constraints.push(constraint.into());
    }

    /// Append one acceptance criterion to this task definition.
    pub fn add_acceptance_criterion(&mut self, criterion: impl Into<String>) {
        self.acceptance_criteria.push(criterion.into());
    }

    /// Set or clear the requesting actor for this task.
    pub fn set_requester(&mut self, requester: Option<ActorRef>) {
        self.requester = requester;
    }

    /// Set or clear the parent task link.
    pub fn set_parent(&mut self, parent: Option<Uuid>) {
        self.parent = parent;
    }

    /// Set or clear the originating intent link.
    pub fn set_intent(&mut self, intent: Option<Uuid>) {
        self.intent = intent;
    }

    /// Set or clear the originating stable plan-step link.
    pub fn set_origin_step_id(&mut self, origin_step_id: Option<Uuid>) {
        self.origin_step_id = origin_step_id;
    }

    /// Append one prerequisite task id to the dependency list.
    pub fn add_dependency(&mut self, task_id: Uuid) {
        self.dependencies.push(task_id);
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
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Task size: {}", e);
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
    fn test_task_creation() {
        let actor = ActorRef::agent("worker").expect("actor");
        let task = Task::new(actor, "Implement pagination", Some(GoalType::Feature)).expect("task");

        assert_eq!(task.title(), "Implement pagination");
        assert_eq!(task.goal(), Some(&GoalType::Feature));
        assert!(task.origin_step_id().is_none());
    }

    #[test]
    fn test_task_requester() {
        let actor = ActorRef::agent("worker").expect("actor");
        let requester = ActorRef::human("alice").expect("requester");
        let mut task = Task::new(actor, "Implement pagination", None).expect("task");
        task.set_requester(Some(requester.clone()));
        assert_eq!(task.requester(), Some(&requester));
    }

    #[test]
    fn test_task_goal_optional() {
        let actor = ActorRef::agent("worker").expect("actor");
        let task = Task::new(actor, "Investigate", None).expect("task");
        assert!(task.goal().is_none());
    }

    #[test]
    fn test_task_origin_step_id() {
        let actor = ActorRef::agent("worker").expect("actor");
        let mut task = Task::new(actor, "Implement pagination", None).expect("task");
        let step_id = Uuid::from_u128(0x1234);
        task.set_origin_step_id(Some(step_id));
        assert_eq!(task.origin_step_id(), Some(step_id));
    }

    #[test]
    fn test_task_dependencies() {
        let actor = ActorRef::agent("worker").expect("actor");
        let mut task = Task::new(actor, "Implement pagination", None).expect("task");
        let dep = Uuid::from_u128(0xAA);
        task.add_dependency(dep);
        assert_eq!(task.dependencies(), &[dep]);
    }
}
