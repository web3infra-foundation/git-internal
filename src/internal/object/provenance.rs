//! AI Provenance Definition
//!
//! `Provenance` captures metadata about *how* a run was executed, specifically focusing on
//! the model (LLM) and provider configuration.
//!
//! # Usage
//!
//! This is critical for:
//! - **Reproducibility**: Knowing which model version produced a result.
//! - **Cost Accounting**: Tracking token usage per run.
//! - **Optimization**: Comparing performance across different models or parameters.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::header::{ActorRef, AiObjectType, Header};

/// Provenance object for model/provider metadata.
/// Captures model/provider settings and usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    provider: String,
    model: String,
    parameters: Option<serde_json::Value>,
    token_usage: Option<serde_json::Value>,
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
            header: Header::new(AiObjectType::Provenance, repo_id, created_by)?,
            run_id,
            provider: provider.into(),
            model: model.into(),
            parameters: None,
            token_usage: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn parameters(&self) -> Option<&serde_json::Value> {
        self.parameters.as_ref()
    }

    pub fn token_usage(&self) -> Option<&serde_json::Value> {
        self.token_usage.as_ref()
    }

    pub fn set_parameters(&mut self, parameters: Option<serde_json::Value>) {
        self.parameters = parameters;
    }

    pub fn set_token_usage(&mut self, token_usage: Option<serde_json::Value>) {
        self.token_usage = token_usage;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provenance_fields() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut provenance =
            Provenance::new(repo_id, actor, run_id, "openai", "gpt-4").expect("provenance");
        provenance.set_parameters(Some(serde_json::json!({"temperature": 0.2})));
        provenance.set_token_usage(Some(serde_json::json!({"input": 10, "output": 5})));

        assert!(provenance.parameters().is_some());
        assert!(provenance.token_usage().is_some());
    }
}
