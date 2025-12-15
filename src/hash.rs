//! In Git, the SHA-1 hash algorithm is widely used to generate unique identifiers for Git objects.
//! Each Git object corresponds to a unique SHA-1 hash value, which is used to identify the object's
//! location in the Git internal and mega database.
//!

use std::{cell::RefCell, fmt::Display, hash::Hash, io, str::FromStr};

use bincode::{Decode, Encode};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use sha1::Digest;

use crate::internal::object::types::ObjectType;
pub type SHA1 = ObjectHash;
/// The [`SHA1`] struct, encapsulating a `[u8; 20]` array, is specifically designed to represent Git hash IDs.
/// In Git's context, these IDs are 40-character hexadecimal strings generated via the SHA-1 algorithm.
/// Each Git object receives a unique hash ID based on its content, serving as an identifier for its location
/// within the Git internal database. Utilizing a dedicated struct for these hash IDs enhances code readability and
/// maintainability by providing a clear, structured format for their manipulation and storage.
///
/// The [`HashKind`] enum represents different types of hash algorithms supported in Git,
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
    Deserialize,
    Serialize,
    Encode,
    Decode,
)]
pub enum HashKind {
    #[default]
    Sha1,
    Sha256,
}
/// Implementation of methods for the [`HashKind`] enum.
impl HashKind {
    pub const fn size(&self) -> usize {
        match self {
            HashKind::Sha1 => 20,
            HashKind::Sha256 => 32,
            // Add more hash kinds here as needed
        }
    }
    pub const fn hex_len(&self) -> usize {
        match self {
            HashKind::Sha1 => 40,
            HashKind::Sha256 => 64,
        }
    }
    pub const fn as_str(&self) -> &'static str {
        match self {
            HashKind::Sha1 => "sha1",
            HashKind::Sha256 => "sha256",
        }
    }
}
impl std::fmt::Display for HashKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl std::str::FromStr for HashKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "sha1" => Ok(HashKind::Sha1),
            "sha256" => Ok(HashKind::Sha256),
            _ => Err("Invalid hash kind".to_string()),
        }
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Encode, Decode,
)]
pub enum ObjectHash {
    Sha1([u8; 20]),
    Sha256([u8; 32]),
}
impl Default for ObjectHash {
    fn default() -> Self {
        ObjectHash::Sha1([0u8; 20])
    }
}
impl Display for ObjectHash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.as_ref()))
    }
}
impl AsRef<[u8]> for ObjectHash {
    fn as_ref(&self) -> &[u8] {
        match self {
            ObjectHash::Sha1(bytes) => bytes.as_slice(),
            ObjectHash::Sha256(bytes) => bytes.as_slice(),
        }
    }
}
/// Implementation of the [`std::str::FromStr`] trait for the [`ObjectHash`] enum.
/// To effectively use the `from_str` method for converting a string to an `ObjectHash` object, consider the following:
///   1. The input string `s` should be a pre-calculated hexadecimal string, either 40 characters in length for SHA1 or 64 characters for SHA256.
///      This string represents a hash and should conform to the standard hash format.
///   2. It is necessary to explicitly import the `FromStr` trait to utilize the `from_str` method. Include the import
///      statement `use std::str::FromStr;` in your code before invoking the `from_str` function. This import ensures
impl FromStr for ObjectHash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.len() {
            40 => {
                let mut h = [0u8; 20];
                let bytes = hex::decode(s).map_err(|e| e.to_string())?;
                h.copy_from_slice(bytes.as_slice());
                Ok(ObjectHash::Sha1(h))
            }
            64 => {
                let mut h = [0u8; 32];
                let bytes = hex::decode(s).map_err(|e| e.to_string())?;
                h.copy_from_slice(bytes.as_slice());
                Ok(ObjectHash::Sha256(h))
            }
            _ => Err("Invalid hash length".to_string()),
        }
    }
}

/// Implementation of methods for the [`ObjectHash`] enum.
/// 1. The `kind` method determines the type of hash (SHA1 or SHA256) based on the variant of the `ObjectHash` enum.
/// 2. The `size` method returns the size of the hash in bytes, utilizing the `kind` method to determine the appropriate size.
/// 3. The `new` method computes the hash of the provided data using the specified hash kind (SHA1 or SHA256) and returns
///    an `ObjectHash` instance containing the computed hash.
/// 4. `from` Prefix:Methods to create an `ObjectHash` from different sources:
///   - `from_type_and_data`: Constructs an `ObjectHash` from an object type and its associated data.
///  - `from_bytes`: Creates an `ObjectHash` from a byte slice, ensuring the length matches the expected hash size.
/// - `from_stream`: Reads bytes from a stream to create an `ObjectHash`, ensuring the correct number of bytes are read based on the hash kind.
/// 5. `to` Prefix:Methods to convert an `ObjectHash` to different formats:
///  - `to_color_str`: Converts the hash to a colored string representation for display purposes
/// - `to_data`: Converts the hash to a byte vector.
/// - `_to_string`: Converts the hash to a hexadecimal string representation.
///
impl ObjectHash {
    /// returns a zeroed hash value for the given hash kind
    pub fn zero_str(kind: HashKind) -> String {
        match kind {
            HashKind::Sha1 => "0000000000000000000000000000000000000000".to_string(),
            HashKind::Sha256 => {
                "0000000000000000000000000000000000000000000000000000000000000000".to_string()
            }
        }
    }

    /// returns the kind of hash
    pub fn kind(&self) -> HashKind {
        match self {
            ObjectHash::Sha1(_) => HashKind::Sha1,
            ObjectHash::Sha256(_) => HashKind::Sha256,
        }
    }
    /// returns the size of hash in bytes
    pub fn size(&self) -> usize {
        self.kind().size()
    }

    /// Calculates the hash of the given data using the specified hash kind.
    pub fn new(data: &[u8]) -> ObjectHash {
        match get_hash_kind() {
            HashKind::Sha1 => {
                let h = sha1::Sha1::digest(data);
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(h.as_ref());
                ObjectHash::Sha1(bytes)
            }
            HashKind::Sha256 => {
                let h = sha2::Sha256::digest(data);
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(h.as_ref());
                ObjectHash::Sha256(bytes)
            }
        }
    }
    /// Create ObjectHash from object type and data
    pub fn from_type_and_data(object_type: ObjectType, data: &[u8]) -> ObjectHash {
        let mut d: Vec<u8> = Vec::new();
        d.extend(object_type.to_data().unwrap());
        d.push(b' ');
        d.extend(data.len().to_string().as_bytes());
        d.push(b'\x00');
        d.extend(data);
        ObjectHash::new(&d)
    }
    /// Create ObjectHash from a byte slice
    pub fn from_bytes(bytes: &[u8]) -> Result<ObjectHash, String> {
        let expected_len = get_hash_kind().size();
        if bytes.len() != expected_len {
            return Err(format!(
                "Invalid byte length: got {}, expected {}",
                bytes.len(),
                expected_len
            ));
        }

        match get_hash_kind() {
            HashKind::Sha1 => {
                let mut h = [0u8; 20];
                h.copy_from_slice(bytes);
                Ok(ObjectHash::Sha1(h))
            }
            HashKind::Sha256 => {
                let mut h = [0u8; 32];
                h.copy_from_slice(bytes);
                Ok(ObjectHash::Sha256(h))
            }
        }
    }
    /// Create ObjectHash from a stream
    pub fn from_stream(data: &mut impl io::Read) -> io::Result<ObjectHash> {
        match get_hash_kind() {
            HashKind::Sha1 => {
                let mut h = [0u8; 20];
                data.read_exact(&mut h)?;
                Ok(ObjectHash::Sha1(h))
            }
            HashKind::Sha256 => {
                let mut h = [0u8; 32];
                data.read_exact(&mut h)?;
                Ok(ObjectHash::Sha256(h))
            }
        }
    }

    /// Export sha1 value to String with the color
    pub fn to_color_str(self) -> String {
        self.to_string().red().bold().to_string()
    }

    /// Export sha1 value to a byte array
    pub fn to_data(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    /// [`core::fmt::Display`] is somewhat expensive,
    /// use this hack to get a string more efficiently
    pub fn _to_string(&self) -> String {
        hex::encode(self.as_ref())
    }

    /// Get mutable hash as byte slice
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        match self {
            ObjectHash::Sha1(bytes) => bytes.as_mut_slice(),
            ObjectHash::Sha256(bytes) => bytes.as_mut_slice(),
        }
    }
}
thread_local! {
    /// Thread-local variable to store the current hash kind.
    /// This allows different threads to work with different hash algorithms concurrently
    /// without interfering with each other.
    static CURRENT_HASH_KIND: RefCell<HashKind> = RefCell::new(HashKind::default());
}
pub fn set_hash_kind(kind: HashKind) {
    CURRENT_HASH_KIND.with(|h| {
        *h.borrow_mut() = kind;
    });
}

/// Retrieves the hash kind for the current thread.
pub fn get_hash_kind() -> HashKind {
    CURRENT_HASH_KIND.with(|h| *h.borrow())
}
/// A guard to reset the hash kind after the test
pub struct HashKindGuard {
    prev: HashKind,
}
/// Implementation of the `Drop` trait for the `HashKindGuard` struct.
impl Drop for HashKindGuard {
    fn drop(&mut self) {
        set_hash_kind(self.prev);
    }
}
/// Sets the hash kind for the current thread and returns a guard to reset it later.
pub fn set_hash_kind_for_test(kind: HashKind) -> HashKindGuard {
    let prev = get_hash_kind();
    set_hash_kind(kind);
    HashKindGuard { prev }
}
#[cfg(test)]
mod tests {

    use std::{
        env,
        io::{BufReader, Read, Seek, SeekFrom},
        path::PathBuf,
        str::FromStr,
    };

    use crate::hash::{HashKind, ObjectHash, set_hash_kind_for_test};

    #[test]
    fn test_sha1_new() {
        // Set hash kind to SHA1 for this test
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        // Example input
        let data = "Hello, world!".as_bytes();

        // Generate SHA1 hash from the input data
        let sha1 = ObjectHash::new(data);

        // Known SHA1 hash for "Hello, world!"
        let expected_sha1_hash = "943a702d06f34599aee1f8da8ef9f7296031d699";

        assert_eq!(sha1.to_string(), expected_sha1_hash);
    }
    #[test]
    fn test_sha256_new() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let data = "Hello, world!".as_bytes();
        let sha256 = ObjectHash::new(data);
        let expected_sha256_hash =
            "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3";
        assert_eq!(sha256.to_string(), expected_sha256_hash);
    }

    #[test]
    fn test_signature_without_delta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/data/packs/small-sha1.pack");

        let f = std::fs::File::open(source).unwrap();
        let mut buffered = BufReader::new(f);

        buffered.seek(SeekFrom::End(-20)).unwrap();
        let mut buffer = vec![0; 20];
        buffered.read_exact(&mut buffer).unwrap();
        let signature = ObjectHash::from_bytes(buffer.as_ref()).unwrap();
        assert_eq!(signature.kind(), HashKind::Sha1);
    }
    #[test]
    fn test_signature_without_delta_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/data/packs/small-sha256.pack");

        let f = std::fs::File::open(source).unwrap();
        let mut buffered = BufReader::new(f);

        buffered.seek(SeekFrom::End(-32)).unwrap();
        let mut buffer = vec![0; 32];
        buffered.read_exact(&mut buffer).unwrap();
        let signature = ObjectHash::from_bytes(buffer.as_ref()).unwrap();
        assert_eq!(signature.kind(), HashKind::Sha256);
    }

    #[test]
    fn test_sha1_from_bytes() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let sha1 = ObjectHash::from_bytes(&[
            0x8a, 0xb6, 0x86, 0xea, 0xfe, 0xb1, 0xf4, 0x47, 0x02, 0x73, 0x8c, 0x8b, 0x0f, 0x24,
            0xf2, 0x56, 0x7c, 0x36, 0xda, 0x6d,
        ])
        .unwrap();

        assert_eq!(sha1.to_string(), "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
    }
    #[test]
    fn test_sha256_from_bytes() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        // Pre-calculated SHA256 hash for "abc"
        let sha256 = ObjectHash::from_bytes(&[
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ])
        .unwrap();

        assert_eq!(
            sha256.to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_from_stream() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let source = [
            0x8a, 0xb6, 0x86, 0xea, 0xfe, 0xb1, 0xf4, 0x47, 0x02, 0x73, 0x8c, 0x8b, 0x0f, 0x24,
            0xf2, 0x56, 0x7c, 0x36, 0xda, 0x6d,
        ];
        let mut reader = std::io::Cursor::new(source);
        let sha1 = ObjectHash::from_stream(&mut reader).unwrap();
        assert_eq!(sha1.to_string(), "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
    }
    #[test]
    fn test_sha256_from_stream() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let source = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        let mut reader = std::io::Cursor::new(source);
        let sha256 = ObjectHash::from_stream(&mut reader).unwrap();
        assert_eq!(
            sha256.to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
    #[test]
    fn test_sha1_from_str() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let hash_str = "8ab686eafeb1f44702738c8b0f24f2567c36da6d";

        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(hash.to_string(), "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
            }
            Err(e) => println!("Error: {e}"),
        }
    }
    #[test]
    fn test_sha256_from_str() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let hash_str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(
                    hash.to_string(),
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                );
            }
            Err(e) => println!("Error: {e}"),
        }
    }
    #[test]
    fn test_sha1_to_string() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let hash_str = "8ab686eafeb1f44702738c8b0f24f2567c36da6d";

        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(hash.to_string(), "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
            }
            Err(e) => println!("Error: {e}"),
        }
    }
    #[test]
    fn test_sha256_to_string() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let hash_str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(
                    hash.to_string(),
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                );
            }
            Err(e) => println!("Error: {e}"),
        }
    }
    #[test]
    fn test_sha1_to_data() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let hash_str = "8ab686eafeb1f44702738c8b0f24f2567c36da6d";

        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(
                    hash.to_data(),
                    vec![
                        0x8a, 0xb6, 0x86, 0xea, 0xfe, 0xb1, 0xf4, 0x47, 0x02, 0x73, 0x8c, 0x8b,
                        0x0f, 0x24, 0xf2, 0x56, 0x7c, 0x36, 0xda, 0x6d
                    ]
                );
            }
            Err(e) => println!("Error: {e}"),
        }
    }
    #[test]
    fn test_sha256_to_data() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let hash_str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        match ObjectHash::from_str(hash_str) {
            Ok(hash) => {
                assert_eq!(
                    hash.to_data(),
                    vec![
                        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde,
                        0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
                        0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
                    ]
                );
            }
            Err(e) => println!("Error: {e}"),
        }
    }
}
