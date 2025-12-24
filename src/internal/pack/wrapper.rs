//! Reader wrapper that tracks how many bytes of a pack have been consumed while keeping a running
//! SHA-1/SHA-256 hash for trailer verification.

use std::io::{self, BufRead, Read};

use sha1::{Digest, Sha1};

use crate::{
    hash::{HashKind, ObjectHash, get_hash_kind},
    utils::HashAlgorithm,
};
/// [`Wrapper`] is a wrapper around a reader that also computes the SHA1/ SHA256 hash of the data read.
///
/// It is designed to work with any reader that implements `BufRead`.
///
/// Fields:
/// * `inner`: The inner reader.
/// * `hash`: The  hash state.
/// * `count_hash`: A flag to indicate whether to compute the hash while reading.
///
pub struct Wrapper<R> {
    inner: R,
    hash: HashAlgorithm,
    bytes_read: usize,
}

impl<R> Wrapper<R>
where
    R: BufRead,
{
    /// Constructs a new [`Wrapper`] with the given reader and a flag to enable or disable hashing.
    ///
    /// # Parameters
    /// * `inner`: The reader to wrap.
    /// * `count_hash`: If `true`, the hash is computed while reading; otherwise, it is not.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hash: match get_hash_kind() {
                HashKind::Sha1 => HashAlgorithm::Sha1(Sha1::new()),
                HashKind::Sha256 => HashAlgorithm::Sha256(sha2::Sha256::new()),
            }, // Initialize a new SHA1/ SHA256 hasher
            bytes_read: 0,
        }
    }

    /// Returns the number of bytes read so far.
    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    /// Returns the final SHA1/ SHA256 hash of the data read so far.
    ///
    /// This is a clone of the internal hash state finalized into a SHA1/ SHA256 hash.
    pub fn final_hash(&self) -> ObjectHash {
        match &self.hash.clone() {
            HashAlgorithm::Sha1(hasher) => {
                let re: [u8; 20] = hasher.clone().finalize().into(); // Clone, finalize, and convert the hash into bytes
                ObjectHash::from_bytes(&re).unwrap()
            }
            HashAlgorithm::Sha256(hasher) => {
                let re: [u8; 32] = hasher.clone().finalize().into(); // Clone, finalize, and convert the hash into bytes
                ObjectHash::from_bytes(&re).unwrap()
            }
        }
    }
}

impl<R> BufRead for Wrapper<R>
where
    R: BufRead,
{
    /// Provides access to the internal buffer of the wrapped reader without consuming it.
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf() // Delegate to the inner reader
    }

    /// Consumes data from the buffer and updates the hash if `count_hash` is true.
    ///
    /// # Parameters
    /// * `amt`: The amount of data to consume from the buffer.
    fn consume(&mut self, amt: usize) {
        let buffer = self.inner.fill_buf().expect("Failed to fill buffer");
        match &mut self.hash {
            HashAlgorithm::Sha1(hasher) => hasher.update(&buffer[..amt]), // Update SHA1 hash with the data being consumed
            HashAlgorithm::Sha256(hasher) => hasher.update(&buffer[..amt]), // Update SHA256 hash with the data being consumed
        }
        self.inner.consume(amt); // Consume the data from the inner reader
        self.bytes_read += amt;
    }
}

impl<R> Read for Wrapper<R>
where
    R: BufRead,
{
    /// Reads data into the provided buffer and updates the hash if `count_hash` is true.
    /// <br> [Read::read_exact] calls it internally.
    ///
    /// # Parameters
    /// * `buf`: The buffer to read data into.
    ///
    /// # Returns
    /// Returns the number of bytes read.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let o = self.inner.read(buf)?; // Read data into the buffer
        match &mut self.hash {
            HashAlgorithm::Sha1(hasher) => hasher.update(&buf[..o]), // Update SHA1 hash with the data being read
            HashAlgorithm::Sha256(hasher) => hasher.update(&buf[..o]), // Update SHA256 hash with the data being read
        }
        self.bytes_read += o;
        Ok(o) // Return the number of bytes read
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader, Cursor, Read};

    use sha1::{Digest, Sha1};

    use crate::{
        hash::{HashKind, ObjectHash, set_hash_kind},
        internal::pack::wrapper::Wrapper,
    };

    /// Helper function to test wrapper read functionality for different hash kinds.
    fn wrapper_read(kind: HashKind) {
        let _guard = set_hash_kind(kind);
        let data = b"Hello, world!"; // Sample data
        let cursor = Cursor::new(data.as_ref());
        let buf_reader = BufReader::new(cursor);
        let mut wrapper = Wrapper::new(buf_reader);

        let mut buffer = vec![0; data.len()];
        wrapper.read_exact(&mut buffer).unwrap();

        assert_eq!(buffer, data);
    }
    #[test]
    fn test_wrapper_read() {
        wrapper_read(HashKind::Sha1);
        wrapper_read(HashKind::Sha256);
    }

    /// Helper function to test wrapper hash functionality for different hash kinds.
    fn wrapper_hash_with_kind(kind: HashKind) -> io::Result<()> {
        let _guard = set_hash_kind(kind);
        let data = b"Hello, world!";
        let cursor = Cursor::new(data.as_ref());
        let buf_reader = BufReader::new(cursor);
        let mut wrapper = Wrapper::new(buf_reader);

        let mut buffer = vec![0; data.len()];
        wrapper.read_exact(&mut buffer)?;

        let hash_result = wrapper.final_hash();
        let expected_hash = match kind {
            HashKind::Sha1 => ObjectHash::from_bytes(&Sha1::digest(data)).unwrap(),
            HashKind::Sha256 => ObjectHash::from_bytes(&sha2::Sha256::digest(data)).unwrap(),
        };

        assert_eq!(hash_result, expected_hash);
        Ok(())
    }
    #[test]
    fn test_wrapper_hash() -> io::Result<()> {
        wrapper_hash_with_kind(HashKind::Sha1)?;
        wrapper_hash_with_kind(HashKind::Sha256)?;
        Ok(())
    }
}
