//! Run usage / cost event.
//!
//! `RunUsage` stores immutable usage totals for a `Run`.
//!
//! # How to use this object
//!
//! - Create it after the run, model call batch, or accounting phase has
//!   produced stable token totals.
//! - Keep it append-only; if Libra needs additional rollups, compute
//!   them in projections.
//!
//! # How it works with other objects
//!
//! - `run_id` links usage to the owning `Run`.
//! - `Provenance` supplies the corresponding provider/model
//!   configuration.
//!
//! # How Libra should call it
//!
//! Libra should aggregate analytics, quotas, and billing views from
//! stored `RunUsage` records instead of backfilling usage into
//! `Provenance` or `Run`.

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

/// Immutable token / cost summary for one `Run`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunUsage {
    /// Common object header carrying the immutable object id, type,
    /// creator, and timestamps.
    #[serde(flatten)]
    header: Header,
    /// Canonical owning run for this usage summary.
    run_id: Uuid,
    /// Input tokens consumed by the run or model-call batch.
    input_tokens: u64,
    /// Output tokens produced by the run or model-call batch.
    output_tokens: u64,
    /// Precomputed total tokens for quick reads and validation.
    total_tokens: u64,
    /// Optional billing estimate in USD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cost_usd: Option<f64>,
}

impl RunUsage {
    /// Create a new immutable usage summary for one run.
    pub fn new(
        created_by: ActorRef,
        run_id: Uuid,
        input_tokens: u64,
        output_tokens: u64,
        cost_usd: Option<f64>,
    ) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::RunUsage, created_by)?,
            run_id,
            input_tokens,
            output_tokens,
            total_tokens: input_tokens + output_tokens,
            cost_usd,
        })
    }

    /// Return the immutable header for this usage record.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Return the canonical owning run id.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Return the input token count.
    pub fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// Return the output token count.
    pub fn output_tokens(&self) -> u64 {
        self.output_tokens
    }

    /// Return the total token count.
    pub fn total_tokens(&self) -> u64 {
        self.total_tokens
    }

    /// Return the billing estimate in USD, if present.
    pub fn cost_usd(&self) -> Option<f64> {
        self.cost_usd
    }

    /// Validate that the stored total matches input plus output.
    pub fn is_consistent(&self) -> bool {
        self.total_tokens == self.input_tokens + self.output_tokens
    }
}

impl fmt::Display for RunUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RunUsage: {}", self.header.object_id())
    }
}

impl ObjectTrait for RunUsage {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data).map_err(|e| GitError::InvalidObjectInfo(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::RunUsage
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute RunUsage size: {}", e);
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
    // - usage summary totals
    // - consistency check
    // - optional billing estimate storage

    #[test]
    fn test_run_usage_fields() {
        let actor = ActorRef::agent("planner").expect("actor");
        let usage = RunUsage::new(actor, Uuid::from_u128(0x1), 100, 40, Some(0.12)).expect("usage");

        assert_eq!(usage.input_tokens(), 100);
        assert_eq!(usage.output_tokens(), 40);
        assert_eq!(usage.total_tokens(), 140);
        assert!(usage.is_consistent());
        assert_eq!(usage.cost_usd(), Some(0.12));
    }
}
