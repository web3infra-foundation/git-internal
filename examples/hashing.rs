//! An example demonstrating how to use the hashing functionality in git-internal,
//! including how to switch between SHA-1 and SHA-256.

use git_internal::{
    hash::{HashKind, ObjectHash, set_hash_kind},
    internal::object::types::ObjectType,
};

fn main() {
    let data = b"This is some data to be hashed.";
    println!("Original data: \"{}\"", String::from_utf8_lossy(data));
    println!();

    // --- SHA-1 Hashing ---
    {
        // Set the hash kind for the current thread to SHA-1.
        // The guard ensures the hash kind is restored when it goes out of scope.
        set_hash_kind(HashKind::Sha1);
        println!("Using HashKind: {:?}", HashKind::Sha1);

        // Create a hash for a blob object. The library automatically prepends
        // the "blob <size>\0" header before hashing.
        let blob_hash = ObjectHash::from_type_and_data(ObjectType::Blob, data);
        println!("Blob hash (SHA-1): {}", blob_hash);

        // Manually verify with the `sha1` crate for correctness
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", data.len()).as_bytes());
        hasher.update(data);
        let expected_sha1_bytes: [u8; 20] = hasher.finalize().into();
        assert_eq!(
            blob_hash,
            ObjectHash::from_bytes(&expected_sha1_bytes).unwrap()
        );
        println!("Verified correctly against manual SHA-1 calculation.");
    }

    println!("\n----------------------------------------\n");

    // --- SHA-256 Hashing ---
    {
        // Set the hash kind for the current thread to SHA-256.
        set_hash_kind(HashKind::Sha256);
        println!("Using HashKind: {:?}", HashKind::Sha256);

        let blob_hash = ObjectHash::from_type_and_data(ObjectType::Blob, data);
        println!("Blob hash (SHA-256): {}", blob_hash);

        // Manually verify with the `sha2` crate
        use sha2::{Digest as Sha2Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(format!("blob {}\0", data.len()).as_bytes());
        hasher.update(data);
        let expected_sha256_bytes: [u8; 32] = hasher.finalize().into();
        assert_eq!(
            blob_hash,
            ObjectHash::from_bytes(&expected_sha256_bytes).unwrap()
        );
        println!("Verified correctly against manual SHA-256 calculation.");
    }
}
