use std::str::FromStr;

use serde::Serialize;
use serde_json::Value;

use crate::hash::{ObjectHash, get_hash_kind};

/// Calculate hash from bytes using the repository hash kind.
pub fn compute_hash(content: &[u8]) -> ObjectHash {
    ObjectHash::new(content)
}

/// Calculate hash from a serializable object (deterministic JSON).
pub fn compute_json_hash<T: Serialize>(object: &T) -> Result<ObjectHash, serde_json::Error> {
    let mut value = serde_json::to_value(object)?;
    canonicalize_json(&mut value);
    let content = serde_json::to_vec(&value)?;
    Ok(ObjectHash::new(&content))
}

/// Parse a hex string into `ObjectHash` for the current repository hash kind.
pub fn parse_object_hash(value: &str) -> Result<ObjectHash, String> {
    if value.len() != get_hash_kind().hex_len() {
        return Err(format!(
            "Invalid hash hex string length (expected {})",
            get_hash_kind().hex_len()
        ));
    }
    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("Invalid hash hex string".to_string());
    }
    ObjectHash::from_str(value)
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

        let hash_a = compute_json_hash(&MapWrapper { map: map_a }).expect("checksum");
        let hash_b = compute_json_hash(&MapWrapper { map: map_b }).expect("checksum");

        assert_eq!(hash_a, hash_b);
    }
}
