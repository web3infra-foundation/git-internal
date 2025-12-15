//! Lightweight representation of a decoded Git object coming out of a pack stream, with helpers to
//! convert to/from strongly typed objects.

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::{
    hash::ObjectHash,
    internal::object::{
        ObjectTrait, blob::Blob, commit::Commit, tag::Tag, tree::Tree, types::ObjectType,
    },
};

///
/// Git object data from pack file
///
#[derive(Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub obj_type: ObjectType,
    pub data: Vec<u8>,
    pub hash: ObjectHash,
    pub chain_len: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        // hash is enough to compare, right?
        self.obj_type == other.obj_type && self.hash == other.hash
    }
}

impl Hash for Entry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.obj_type.hash(state);
        self.hash.hash(state);
    }
}

impl From<Blob> for Entry {
    fn from(value: Blob) -> Self {
        Self {
            obj_type: ObjectType::Blob,
            data: value.data,
            hash: value.id,
            chain_len: 0,
        }
    }
}

impl From<Commit> for Entry {
    fn from(value: Commit) -> Self {
        Self {
            obj_type: ObjectType::Commit,
            data: value.to_data().unwrap(),
            hash: value.id,
            chain_len: 0,
        }
    }
}

impl From<Tree> for Entry {
    fn from(value: Tree) -> Self {
        Self {
            obj_type: ObjectType::Tree,
            data: value.to_data().unwrap(),
            hash: value.id,
            chain_len: 0,
        }
    }
}

impl From<Tag> for Entry {
    fn from(value: Tag) -> Self {
        Self {
            obj_type: ObjectType::Tag,
            data: value.to_data().unwrap(),
            hash: value.id,
            chain_len: 0,
        }
    }
}
