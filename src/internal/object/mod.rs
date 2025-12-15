//! Object model definitions for Git blobs, trees, commits, tags, and supporting traits that let the
//! pack/zlib layers create strongly typed values from raw bytes.

pub mod blob;
pub mod commit;
pub mod note;
pub mod signature;
pub mod tag;
pub mod tree;
pub mod types;
pub mod utils;

use std::{
    fmt::Display,
    io::{BufRead, Read},
    str::FromStr,
};

use sha1::Digest;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{object::types::ObjectType, zlib::stream::inflate::ReadBoxed},
};

pub trait ObjectTrait: Send + Sync + Display {
    /// Creates a new object from a byte slice.
    fn from_bytes(data: &[u8], hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized;

    /// Generate a new Object from a `ReadBoxed<BufRead>`.
    /// the input size,is only for new a vec with directive space allocation
    /// the input data stream and output object should be plain base object .
    fn from_buf_read<R: BufRead>(read: &mut ReadBoxed<R>, size: usize) -> Self
    where
        Self: Sized,
    {
        let mut content: Vec<u8> = Vec::with_capacity(size);
        read.read_to_end(&mut content).unwrap();
        let h = read.hash.clone();
        let hash_str = h.finalize();
        Self::from_bytes(
            &content,
            ObjectHash::from_str(&format!("{hash_str:x}")).unwrap(),
        )
        .unwrap()
    }

    /// Returns the type of the object.
    fn get_type(&self) -> ObjectType;

    fn get_size(&self) -> usize;

    fn to_data(&self) -> Result<Vec<u8>, GitError>;
}
