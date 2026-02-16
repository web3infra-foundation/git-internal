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

/// Normalized token usage across providers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cost_usd: Option<f64>,
}

/// Provenance object for model/provider metadata.
/// Captures model/provider settings and usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    #[serde(flatten)]
    header: Header,
    run_id: Uuid,
    provider: String,
    model: String,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    token_usage: Option<TokenUsage>,
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
            header: Header::new(ObjectType::Provenance, repo_id, created_by)?,
            run_id,
            provider: provider.into(),
            model: model.into(),
            parameters: None,
            temperature: None,
            max_tokens: None,
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

    /// Provider-specific raw parameters payload.
    pub fn parameters(&self) -> Option<&serde_json::Value> {
        self.parameters.as_ref()
    }

    /// Normalized temperature if available.
    pub fn temperature(&self) -> Option<f64> {
        self.temperature.or_else(|| {
            self.parameters
                .as_ref()
                .and_then(|p| p.get("temperature"))
                .and_then(|v| v.as_f64())
        })
    }

    /// Normalized max_tokens if available.
    pub fn max_tokens(&self) -> Option<u64> {
        self.max_tokens.or_else(|| {
            self.parameters
                .as_ref()
                .and_then(|p| p.get("max_tokens"))
                .and_then(|v| v.as_u64())
        })
    }

    pub fn token_usage(&self) -> Option<&TokenUsage> {
        self.token_usage.as_ref()
    }

    pub fn set_parameters(&mut self, parameters: Option<serde_json::Value>) {
        self.parameters = parameters;
    }

    pub fn set_temperature(&mut self, temperature: Option<f64>) {
        self.temperature = temperature;
    }

    pub fn set_max_tokens(&mut self, max_tokens: Option<u64>) {
        self.max_tokens = max_tokens;
    }

    pub fn set_token_usage(&mut self, token_usage: Option<TokenUsage>) {
        self.token_usage = token_usage;
    }
}

impl fmt::Display for Provenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Provenance: {}", self.header.object_id())
    }
}

impl ObjectTrait for Provenance {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::Provenance
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute Provenance size: {}", e);
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
    fn test_provenance_fields() {
        let repo_id = Uuid::from_u128(0x0123456789abcdef0123456789abcdef);
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut provenance =
            Provenance::new(repo_id, actor, run_id, "openai", "gpt-4").expect("provenance");
        provenance.set_parameters(Some(
            serde_json::json!({"temperature": 0.2, "max_tokens": 128}),
        ));
        provenance.set_temperature(Some(0.2));
        provenance.set_max_tokens(Some(128));
        provenance.set_token_usage(Some(TokenUsage {
            input_tokens: 10,
            output_tokens: 5,
            total_tokens: 15,
            cost_usd: Some(0.001),
        }));

        assert!(provenance.parameters().is_some());
        assert_eq!(provenance.temperature(), Some(0.2));
        assert_eq!(provenance.max_tokens(), Some(128));
        let usage = provenance.token_usage().expect("token usage");
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
        assert_eq!(usage.cost_usd, Some(0.001));
    }
}
