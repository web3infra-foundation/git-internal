use serde::Serialize;
use serde_json::Value;

use crate::hash::sha256_hex;

/// Calculate SHA256 hex string from bytes.
pub fn compute_sha256_hex(content: &[u8]) -> String {
    sha256_hex(content)
}

/// Calculate SHA256 hex string from a serializable object (deterministic JSON).
pub fn compute_json_sha256_hex<T: Serialize>(object: &T) -> Result<String, serde_json::Error> {
    let mut value = serde_json::to_value(object)?;
    canonicalize_json(&mut value);
    let content = serde_json::to_vec(&value)?;
    Ok(sha256_hex(&content))
}

/// Check valid format (64 hex chars).
pub fn is_valid_sha256_hex(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// Verify content against the expected SHA256 hex string.
pub fn verify_sha256_hex(expected: &str, content: &[u8]) -> bool {
    compute_sha256_hex(content) == expected
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

        let hash_a = compute_json_sha256_hex(&MapWrapper { map: map_a }).expect("checksum");
        let hash_b = compute_json_sha256_hex(&MapWrapper { map: map_b }).expect("checksum");

        assert_eq!(hash_a, hash_b);
    }
}
