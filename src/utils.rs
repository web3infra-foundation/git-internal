//! Shared I/O utilities for Git-internal including buffered readers, SHA abstractions, and helpers
//! for reading pack/file bytes while tracking stream progress.

use std::{
    io,
    io::{BufRead, Read},
};

use sha1::{Digest, Sha1};

use crate::hash::{HashKind, ObjectHash, get_hash_kind};
pub fn read_bytes(file: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_sha(file: &mut impl Read) -> io::Result<ObjectHash> {
    ObjectHash::from_stream(file)
}

/// A lightweight wrapper that counts bytes read from the underlying reader.
/// replace deflate.intotal() in decompress_data
pub struct CountingReader<R> {
    pub inner: R,
    pub bytes_read: u64,
}

impl<R> CountingReader<R> {
    /// Creates a new `CountingReader` wrapping the given reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            bytes_read: 0,
        }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes_read += n as u64;
        Ok(n)
    }
}

impl<R: BufRead> BufRead for CountingReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.bytes_read += amt as u64;
        self.inner.consume(amt);
    }
}
/// a hash abstraction to support both SHA1 and SHA256
/// which for stream hashing handle use (e.g. Sha1::new())
/// `std::io::Write` trait to update the hash state
#[derive(Clone)]
pub enum HashAlgorithm {
    Sha1(Sha1),
    Sha256(sha2::Sha256),
    // Future: support other hash algorithms
}
impl HashAlgorithm {
    /// Update hash with data
    pub fn update(&mut self, data: &[u8]) {
        match self {
            HashAlgorithm::Sha1(hasher) => hasher.update(data),
            HashAlgorithm::Sha256(hasher) => hasher.update(data),
        }
    }
    /// Finalize and get hash result
    pub fn finalize(self) -> Vec<u8> {
        match self {
            HashAlgorithm::Sha1(hasher) => hasher.finalize().to_vec(),
            HashAlgorithm::Sha256(hasher) => hasher.finalize().to_vec(),
        }
    }
    pub fn new() -> Self {
        match get_hash_kind() {
            HashKind::Sha1 => HashAlgorithm::Sha1(Sha1::new()),
            HashKind::Sha256 => HashAlgorithm::Sha256(sha2::Sha256::new()),
        }
    }
}
impl std::io::Write for HashAlgorithm {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Default for HashAlgorithm {
    fn default() -> Self {
        Self::new()
    }
}
