//! AI Plan Definition
//!
//! A `Plan` represents a sequence of steps that an agent intends to execute to complete a task.
//!
//! # Versioning
//!
//! Plans are versioned monotonically. As the agent learns more or encounters obstacles,
//! it may update the plan. Each update creates a new `Plan` object with `plan_version = previous + 1`.
//!
//! # Steps
//!
//! Each step has an `intent` (what to do) and a `status` (pending/in_progress/done).
//! Steps can also define expected inputs/outputs for better chain-of-thought tracking.

use std::fmt;

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

/// Plan step status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    /// Step is waiting to be executed.
    Pending,
    /// Step is currently being executed.
    InProgress,
    /// Step finished successfully.
    Completed,
    /// Step failed.
    Failed,
    /// Step was skipped (e.g. no longer necessary).
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

/// Plan object for step decomposition.
/// New versions are created via `new_next` with monotonic versioning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    /// Plan version starts at 1 and must increase by 1 for each update.
    plan_version: u32,
    #[serde(default)]
    previous_plan_id: Option<Uuid>,
    #[serde(default)]
    steps: Vec<PlanStep>,
}

impl Plan {
    /// Create a new plan object (version 1)
    pub fn new(repo_id: Uuid, created_by: ActorRef, run_id: Uuid) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Plan, repo_id, created_by)?,
            run_id,
            plan_version: 1,
            previous_plan_id: None,
            steps: Vec::new(),
        })
    }

    /// Create the next version of a plan.
    ///
    /// # Arguments
    /// * `previous_version` - The version number of the plan being updated.
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
            header: Header::new(ObjectType::Plan, repo_id, created_by)?,
            run_id,
            plan_version: next_version,
            previous_plan_id: None,
            steps: Vec::new(),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn plan_version(&self) -> u32 {
        self.plan_version
    }

    pub fn previous_plan_id(&self) -> Option<Uuid> {
        self.previous_plan_id
    }

    pub fn steps(&self) -> &[PlanStep] {
        &self.steps
    }

    pub fn add_step(&mut self, step: PlanStep) {
        self.steps.push(step);
    }

    pub fn set_previous_plan_id(&mut self, previous_plan_id: Option<Uuid>) {
        self.previous_plan_id = previous_plan_id;
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Plan: {}", self.header.object_id())
    }
}

impl ObjectTrait for Plan {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Plan
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

    #[test]
    fn test_plan_version_ordering() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let plan_v1 = Plan::new(repo_id, actor.clone(), run_id).expect("plan");
        let plan_v2 =
            Plan::new_next(repo_id, actor.clone(), run_id, plan_v1.plan_version()).expect("plan");
        let plan_v3 =
            Plan::new_next(repo_id, actor.clone(), run_id, plan_v2.plan_version()).expect("plan");

        let mut plans = [plan_v2.clone(), plan_v1.clone(), plan_v3.clone()];
        plans.sort_by_key(|plan| plan.plan_version());

        assert_eq!(plans[0].plan_version(), 1);
        assert_eq!(plans[1].plan_version(), 2);
        assert_eq!(plans[2].plan_version(), 3);

        assert!(plan_v3.plan_version() > plan_v2.plan_version());
        assert!(plan_v2.plan_version() > plan_v1.plan_version());

        assert!(plan_v1.previous_plan_id().is_none());
        assert!(plan_v2.previous_plan_id().is_none());
        assert!(plan_v3.previous_plan_id().is_none());
    }
}
