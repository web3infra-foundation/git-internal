use std::fmt;

use ring::digest;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Unified SHA256 Checksum handler
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Checksum(String);

impl Checksum {
    /// Calculate checksum from bytes
    pub fn compute(content: &[u8]) -> Self {
        let digest = digest::digest(&digest::SHA256, content);
        Self(hex::encode(digest.as_ref()))
    }

    /// Calculate checksum from a serializable object (deterministic JSON)
    pub fn compute_json<T: Serialize>(object: &T) -> Self {
        let mut value = serde_json::to_value(object).unwrap_or(Value::Null);
        canonicalize_json(&mut value);
        let content = serde_json::to_vec(&value).unwrap_or_default();
        Self::compute(&content)
    }

    /// Create from existing hash string with validation
    pub fn new(hash: impl Into<String>) -> Result<Self, String> {
        let hash = hash.into();
        if !Self::is_valid(&hash) {
            return Err(format!("Invalid SHA256 hash format: {}", hash));
        }
        Ok(Self(hash))
    }

    /// Verify if content matches this checksum
    pub fn verify(&self, content: &[u8]) -> bool {
        Self::compute(content) == *self
    }

    /// Check valid format (64 hex chars)
    pub fn is_valid(hash: &str) -> bool {
        hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Get inner string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Checksum> for String {
    fn from(c: Checksum) -> Self {
        c.0
    }
}

impl Serialize for Checksum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Checksum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
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
    fn ai_process_checksum_deterministic_map() {
        let mut map_a = HashMap::new();
        map_a.insert("b".to_string(), "2".to_string());
        map_a.insert("a".to_string(), "1".to_string());

        let mut map_b = HashMap::new();
        map_b.insert("a".to_string(), "1".to_string());
        map_b.insert("b".to_string(), "2".to_string());

        let hash_a = Checksum::compute_json(&MapWrapper { map: map_a });
        let hash_b = Checksum::compute_json(&MapWrapper { map: map_b });

        assert_eq!(hash_a, hash_b);
    }
}
