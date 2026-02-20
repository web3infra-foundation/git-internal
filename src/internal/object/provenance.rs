//! AI Provenance Definition
//!
//! A `Provenance` records **how** a [`Run`](super::run::Run) was executed:
//! which LLM provider, model, and parameters were used, and how many
//! tokens were consumed. It is the "lab notebook" for AI execution —
//! capturing the exact configuration so results can be reproduced,
//! compared, and accounted for.
//!
//! # Position in Lifecycle
//!
//! ```text
//! Run ──(1:1)──▶ Provenance
//!  │
//!  ├── patchsets ──▶ [PatchSet₀, ...]
//!  ├── evidence  ──▶ [Evidence₀, ...]
//!  └── decision  ──▶ Decision
//! ```
//!
//! A Provenance is created **once per Run**, typically at run start
//! when the orchestrator selects the model and provider. Token usage
//! (`token_usage`) is populated after the Run completes. The
//! Provenance is a sibling of PatchSet, Evidence, and Decision —
//! all attached to the same Run but serving different purposes.
//!
//! # Purpose
//!
//! - **Reproducibility**: Given the same model, parameters, and
//!   [`ContextSnapshot`](super::context::ContextSnapshot), the agent
//!   should produce equivalent results.
//! - **Cost Accounting**: `token_usage.cost_usd` enables per-Run and
//!   per-Task cost tracking and budgeting.
//! - **Optimization**: Comparing Provenance across Runs of the same
//!   Task reveals which model/parameter combinations yield better
//!   results or lower cost.

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
///
/// All fields use a provider-neutral representation so that usage
/// from different LLM providers (OpenAI, Anthropic, etc.) can be
/// compared directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenUsage {
    /// Number of tokens in the prompt / input.
    pub input_tokens: u64,
    /// Number of tokens in the completion / output.
    pub output_tokens: u64,
    /// `input_tokens + output_tokens`. Stored explicitly for quick
    /// aggregation; [`is_consistent`](TokenUsage::is_consistent)
    /// verifies the invariant.
    pub total_tokens: u64,
    /// Estimated cost in USD for this usage, if the provider reports
    /// pricing. `None` when pricing data is unavailable.
    pub cost_usd: Option<f64>,
}

impl TokenUsage {
    pub fn is_consistent(&self) -> bool {
        self.total_tokens == self.input_tokens + self.output_tokens
    }

    pub fn cost_per_token(&self) -> Option<f64> {
        if self.total_tokens == 0 {
            return None;
        }
        self.cost_usd.map(|cost| cost / self.total_tokens as f64)
    }
}

/// LLM provider/model configuration and usage for a single Run.
///
/// Created once per Run. See module documentation for lifecycle
/// position and purpose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// The [`Run`](super::run::Run) this Provenance describes.
    ///
    /// Every Provenance belongs to exactly one Run. The Run does not
    /// store a back-reference; lookup is done by scanning or indexing.
    run_id: Uuid,
    /// LLM provider identifier (e.g. "openai", "anthropic", "local").
    ///
    /// Used together with `model` to fully identify the AI backend.
    /// The value is a free-form string; no enum is imposed because
    /// new providers appear frequently.
    provider: String,
    /// Model identifier as returned by the provider (e.g.
    /// "gpt-4", "claude-opus-4-20250514", "llama-3-70b").
    ///
    /// Should match the provider's official model ID so that results
    /// can be correlated with the provider's documentation and pricing.
    model: String,
    /// Provider-specific raw parameters payload.
    ///
    /// A catch-all JSON object for parameters that don't have
    /// dedicated fields (e.g. `top_p`, `frequency_penalty`, custom
    /// system prompts). `None` when no extra parameters were set.
    /// `temperature` and `max_tokens` are extracted into dedicated
    /// fields for convenience but may also appear here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parameters: Option<serde_json::Value>,
    /// Sampling temperature used for generation.
    ///
    /// `0.0` = deterministic, higher = more creative. `None` if the
    /// provider default was used. The getter falls back to
    /// `parameters.temperature` when this field is not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    /// Maximum number of tokens the model was allowed to generate.
    ///
    /// `None` if the provider default was used. The getter falls back
    /// to `parameters.max_tokens` when this field is not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u64>,
    /// Token consumption and cost for this Run.
    ///
    /// Populated after the Run completes. `None` while the Run is
    /// still in progress or if the provider does not report usage.
    /// See [`TokenUsage`] for field details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_usage: Option<TokenUsage>,
}

impl Provenance {
    pub fn new(
        created_by: ActorRef,
        run_id: Uuid,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::Provenance, created_by)?,
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
        let actor = ActorRef::agent("test-agent").expect("actor");
        let run_id = Uuid::from_u128(0x1);

        let mut provenance = Provenance::new(actor, run_id, "openai", "gpt-4").expect("provenance");
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
