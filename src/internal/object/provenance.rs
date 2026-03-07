//! AI Provenance snapshot.
//!
//! `Provenance` records the immutable model/provider configuration used
//! for a `Run`.
//!
//! # How to use this object
//!
//! - Create `Provenance` when Libra has chosen the provider, model, and
//!   generation parameters for a run.
//! - Populate optional sampling and parameter fields before
//!   persistence.
//! - Keep it immutable after writing; usage and cost belong elsewhere.
//!
//! # How it works with other objects
//!
//! - `Run` is the canonical owner via `run_id`.
//! - `RunUsage` stores tokens and cost for the same run.
//!
//! # How Libra should call it
//!
//! Libra should write `Provenance` once near run start, then later write
//! `RunUsage` when consumption totals are known. Do not backfill usage
//! onto the provenance snapshot.

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

/// Immutable provider/model configuration for one execution attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical owning run for this provider/model configuration.
    run_id: Uuid,
    /// Provider identifier, such as `openai`.
    provider: String,
    /// Model identifier, such as `gpt-5`.
    model: String,
    /// Provider-specific structured parameters captured as raw JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parameters: Option<serde_json::Value>,
    /// Optional top-level temperature convenience field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    /// Optional top-level max token convenience field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u64>,
}

impl Provenance {
    /// Create a new provider/model configuration record for one run.
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
        })
    }

    /// Return the immutable header for this provenance record.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical owning run id.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Return the provider identifier.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Return the model identifier.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Return the raw structured parameters, if present.
    pub fn parameters(&self) -> Option<&serde_json::Value> {
        self.parameters.as_ref()
    }

    /// Return the effective temperature, checking the explicit field
    /// first and the raw parameters second.
    pub fn temperature(&self) -> Option<f64> {
        self.temperature.or_else(|| {
            self.parameters
                .as_ref()
                .and_then(|p| p.get("temperature"))
                .and_then(|v| v.as_f64())
        })
    }

    /// Return the effective max token limit, checking the explicit field
    /// first and the raw parameters second.
    pub fn max_tokens(&self) -> Option<u64> {
        self.max_tokens.or_else(|| {
            self.parameters
                .as_ref()
                .and_then(|p| p.get("max_tokens"))
                .and_then(|v| v.as_u64())
        })
    }

    /// Set or clear the raw structured provider parameters.
    pub fn set_parameters(&mut self, parameters: Option<serde_json::Value>) {
        self.parameters = parameters;
    }

    /// Set or clear the top-level temperature field.
    pub fn set_temperature(&mut self, temperature: Option<f64>) {
        self.temperature = temperature;
    }

    /// Set or clear the top-level max token field.
    pub fn set_max_tokens(&mut self, max_tokens: Option<u64>) {
        self.max_tokens = max_tokens;
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

    // Coverage:
    // - canonical run/provider/model storage
    // - fallback lookup of temperature and max_tokens from parameters

    #[test]
    fn test_provenance_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let run_id = Uuid::from_u128(0x42);
        let mut provenance = Provenance::new(actor, run_id, "openai", "gpt-5").expect("prov");

        provenance.set_parameters(Some(
            serde_json::json!({"temperature": 0.2, "max_tokens": 2048}),
        ));

        assert_eq!(provenance.run_id(), run_id);
        assert_eq!(provenance.provider(), "openai");
        assert_eq!(provenance.model(), "gpt-5");
        assert_eq!(provenance.temperature(), Some(0.2));
        assert_eq!(provenance.max_tokens(), Some(2048));
    }
}
