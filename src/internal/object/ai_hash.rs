//! Hash implementation for AI process objects.
//!
//! This module defines `IntegrityHash`, which is used for integrity verification
//! and deduplication of AI objects (Artifacts, Headers, etc.).
//!
//! # Why not `ObjectHash`?
//!
//! We avoid using `git_internal::hash::ObjectHash` here because:
//! 1. `ObjectHash` implies Git content addressing (SHA-1 or SHA-256 depending on repo config).
//! 2. `IntegrityHash` always uses SHA-256 for consistent integrity checks regardless of the
//!    underlying Git repository format.
//! 3. This separation prevents accidental usage of integrity checksums as Git object IDs.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// A SHA-256 hash used for integrity verification.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IntegrityHash([u8; 32]);

impl IntegrityHash {
    /// Create a new hash from raw bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Compute hash from content bytes.
    pub fn compute(content: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let result = hasher.finalize();
        Self(result.into())
    }

    /// Return the hex string representation.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for IntegrityHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntegrityHash({})", self.to_hex())
    }
}

impl fmt::Display for IntegrityHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for IntegrityHash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(format!("Invalid hash length: expected 64, got {}", s.len()));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes).map_err(|e| e.to_string())?;
        Ok(Self(bytes))
    }
}

impl Serialize for IntegrityHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for IntegrityHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Compute canonical JSON hash.
pub fn compute_integrity_hash<T: Serialize>(
    object: &T,
) -> Result<IntegrityHash, serde_json::Error> {
    let mut value = serde_json::to_value(object)?;
    canonicalize_json(&mut value);
    let content = serde_json::to_vec(&value)?;
    Ok(IntegrityHash::compute(&content))
}

fn canonicalize_json(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                canonicalize_json(item);
            }
        }
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = std::mem::take(map).into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut sorted = serde_json::Map::with_capacity(entries.len());
            for (key, mut value) in entries {
                canonicalize_json(&mut value);
                sorted.insert(key, value);
            }
            *map = sorted;
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Serialize)]
    struct MapWrapper {
        map: HashMap<String, String>,
    }

    #[test]
    fn test_integrity_hash_deterministic() {
        let mut map_a = HashMap::new();
        map_a.insert("b".to_string(), "2".to_string());
        map_a.insert("a".to_string(), "1".to_string());

        let mut map_b = HashMap::new();
        map_b.insert("a".to_string(), "1".to_string());
        map_b.insert("b".to_string(), "2".to_string());

        let hash_a = compute_integrity_hash(&MapWrapper { map: map_a }).expect("checksum");
        let hash_b = compute_integrity_hash(&MapWrapper { map: map_b }).expect("checksum");

        assert_eq!(hash_a, hash_b);
        assert_eq!(hash_a.to_hex().len(), 64);
    }
}
