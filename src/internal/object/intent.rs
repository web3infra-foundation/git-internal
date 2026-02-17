use std::fmt;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    Draft,
    Active,
    Completed,
    Cancelled,
}

impl IntentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            IntentStatus::Draft => "draft",
            IntentStatus::Active => "active",
            IntentStatus::Completed => "completed",
            IntentStatus::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for IntentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    #[serde(flatten)]
    header: Header,
    content: String,
    parent_id: Option<Uuid>,
    root_id: Option<Uuid>,
    task_id: Option<Uuid>,
    result_commit_sha: Option<IntegrityHash>,
    status: IntentStatus,
}

impl Intent {
    pub fn new(
        repo_id: Uuid,
        created_by: ActorRef,
        content: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Intent, repo_id, created_by)?,
            content: content.into(),
            parent_id: None,
            root_id: None,
            task_id: None,
            result_commit_sha: None,
            status: IntentStatus::Draft,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn parent_id(&self) -> Option<Uuid> {
        self.parent_id
    }

    pub fn root_id(&self) -> Option<Uuid> {
        self.root_id
    }

    pub fn task_id(&self) -> Option<Uuid> {
        self.task_id
    }

    pub fn result_commit_sha(&self) -> Option<&IntegrityHash> {
        self.result_commit_sha.as_ref()
    }

    pub fn status(&self) -> &IntentStatus {
        &self.status
    }

    pub fn set_parent_id(&mut self, parent_id: Option<Uuid>) {
        self.parent_id = parent_id;
    }

    pub fn set_root_id(&mut self, root_id: Option<Uuid>) {
        self.root_id = root_id;
    }

    pub fn set_task_id(&mut self, task_id: Option<Uuid>) {
        self.task_id = task_id;
    }

    pub fn set_result_commit_sha(&mut self, sha: Option<IntegrityHash>) {
        self.result_commit_sha = sha;
    }

    pub fn set_status(&mut self, status: IntentStatus) {
        self.status = status;
    }
}

impl fmt::Display for Intent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Intent: {}", self.header.object_id())
    }
}

impl ObjectTrait for Intent {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidIntentObject(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Intent
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Intent size: {}", e);
                0
            }
        }
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidIntentObject(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intent_creation() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::human("jackie").expect("actor");
        let intent = Intent::new(repo_id, actor, "Refactor login flow").expect("intent");

        assert_eq!(intent.header().object_type(), &ObjectType::Intent);
        assert_eq!(intent.status(), &IntentStatus::Draft);
        assert!(intent.parent_id().is_none());
        assert!(intent.root_id().is_none());
        assert!(intent.task_id().is_none());
    }
}
