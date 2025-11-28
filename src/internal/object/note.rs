//! Git Note object implementation
//!
//! Git Notes are a mechanism for adding metadata to existing Git objects (usually commits)
//! without modifying the original objects. Notes are commonly used for:
//!
//! - Adding review comments or approval metadata
//! - Storing CI/CD build status and code scan results  
//! - Attaching author signatures, annotations, or other metadata
//!
//! In Git's object model, Notes are stored as Blob objects, with the association between
//! notes and target objects managed through the refs/notes/* namespace.

use std::fmt::Display;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::errors::GitError;
use crate::hash::ObjectHash;
use crate::internal::object::ObjectTrait;
use crate::internal::object::ObjectType;

/// Git Note object structure
///
/// A Note represents additional metadata attached to a Git object (typically a commit).
/// The Note itself is stored as a Blob object in Git's object database, with the
/// association managed through Git's reference system.
#[derive(Eq, Debug, Clone, Serialize, Deserialize, Decode, Encode)]
pub struct Note {
    /// The ObjectHash of this Note object (same as the underlying Blob)
    pub id: ObjectHash,
    /// The ObjectHash of the object this Note annotates (usually a commit)
    pub target_object_id: ObjectHash,
    /// The textual content of the Note
    pub content: String,
}

impl PartialEq for Note {
    /// Two Notes are equal if they have the same ID
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Display for Note {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Note for object: {}", self.target_object_id)?;
        writeln!(f, "Content: {}", self.content)
    }
}

impl Note {
    /// Create a new Note for the specified target object with the given content
    ///
    /// # Arguments
    /// * `target_object_id` - The ObjectHash of the object to annotate
    /// * `content` - The textual content of the note
    ///
    /// # Returns
    /// A new Note instance with calculated ID based on the content
    pub fn new(target_object_id: ObjectHash, content: String) -> Self {
        // Calculate the SHA-1/ SHA-256 hash for this Note's content
        // Notes are stored as Blob objects in Git
        let id = ObjectHash::from_type_and_data(ObjectType::Blob, content.as_bytes());

        Self {
            id,
            target_object_id,
            content,
        }
    }

    /// Create a Note from content string, with default target object
    ///
    /// This is a convenience method for creating Notes when the target
    /// will be set later by the notes management system.
    ///
    /// # Arguments
    /// * `content` - The textual content of the note
    ///
    /// # Returns
    /// A new Note instance with default target object ID
    pub fn from_content(content: &str) -> Self {
        Self::new(ObjectHash::default(), content.to_string())
    }

    /// Get the size of the Note content in bytes
    pub fn content_size(&self) -> usize {
        self.content.len()
    }

    /// Check if the Note is empty (has no content)
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Update the target object ID for this Note
    ///
    /// This method allows changing which object this Note annotates
    /// without changing the Note's content or ID.
    ///
    /// # Arguments
    /// * `new_target` - The new target object SHA-1/ SHA-256 hash
    pub fn set_target(&mut self, new_target: ObjectHash) {
        self.target_object_id = new_target;
    }

    /// Create a Note object from raw bytes with explicit target object ID
    ///
    /// This is the preferred method when you know both the content and the target object,
    /// as it preserves the complete Note association information.
    ///
    /// # Arguments
    /// * `data` - The raw byte data (UTF-8 encoded text content)
    /// * `hash` - The SHA-1/ SHA-256 hash of this Note object
    /// * `target_object_id` - The SHA-1/ SHA-256 hash of the object this Note annotates
    ///
    /// # Returns
    /// A Result containing the Note object with complete association info
    pub fn from_bytes_with_target(
        data: &[u8],
        hash: ObjectHash,
        target_object_id: ObjectHash,
    ) -> Result<Self, GitError> {
        let content = String::from_utf8(data.to_vec())
            .map_err(|e| GitError::InvalidNoteObject(format!("Invalid UTF-8 content: {}", e)))?;

        Ok(Note {
            id: hash,
            target_object_id,
            content,
        })
    }

    /// Serialize a Note with its target association for external storage
    ///
    /// This method returns both the Git object data and the target object ID,
    /// which can be used by higher-level systems to manage the refs/notes/* references.
    ///
    /// # Returns
    /// A tuple of (object_data, target_object_id)
    pub fn to_data_with_target(&self) -> Result<(Vec<u8>, ObjectHash), GitError> {
        let data = self.to_data()?;
        Ok((data, self.target_object_id))
    }
}

impl ObjectTrait for Note {
    /// Create a Note object from raw bytes and hash
    ///
    /// # Arguments
    /// * `data` - The raw byte data (UTF-8 encoded text content)
    /// * `hash` - The SHA-1/ SHA-256 hash of this Note object
    ///
    /// # Returns
    /// A Result containing the Note object or an error
    fn from_bytes(data: &[u8], hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        // Convert bytes to UTF-8 string
        let content = String::from_utf8(data.to_vec())
            .map_err(|e| GitError::InvalidNoteObject(format!("Invalid UTF-8 content: {}", e)))?;

        Ok(Note {
            id: hash,
            target_object_id: ObjectHash::default(), // Target association managed externally
            content,
        })
    }

    /// Get the Git object type for Notes
    ///
    /// Notes are stored as Blob objects in Git's object database
    fn get_type(&self) -> ObjectType {
        ObjectType::Blob
    }

    /// Get the size of the Note content
    fn get_size(&self) -> usize {
        self.content.len()
    }

    /// Convert the Note to raw byte data for storage
    ///
    /// # Returns
    /// A Result containing the byte representation or an error
    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        Ok(self.content.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::{HashKind, ObjectHash, set_hash_kind_for_test};
    use std::str::FromStr;

    #[test]
    fn test_note_creation_and_serialization() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let target_id = ObjectHash::from_str("1234567890abcdef1234567890abcdef12345678").unwrap();
        let content = "This commit needs review".to_string();
        let note = Note::new(target_id, content.clone());

        assert_eq!(note.target_object_id, target_id);
        assert_eq!(note.content, content);
        assert_ne!(note.id, ObjectHash::default());
        assert_eq!(note.get_type(), ObjectType::Blob);

        // Test serialization
        let data = note.to_data().unwrap();
        assert_eq!(data, content.as_bytes());
        assert_eq!(note.get_size(), content.len());
    }
    #[test]
    fn test_note_creation_and_serialization_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let target_id = ObjectHash::from_str(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let content = "This commit needs review".to_string();
        let note = Note::new(target_id, content.clone());

        assert_eq!(note.target_object_id, target_id);
        assert_eq!(note.content, content);
        assert_ne!(note.id, ObjectHash::default());
        assert_eq!(note.get_type(), ObjectType::Blob);

        // Test serialization
        let data = note.to_data().unwrap();
        assert_eq!(data, content.as_bytes());
        assert_eq!(note.get_size(), content.len());
    }

    #[test]
    fn test_note_deserialization() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let content = "Deserialization test content";
        let hash = ObjectHash::from_str("fedcba0987654321fedcba0987654321fedcba09").unwrap();
        let target_id = ObjectHash::from_str("abcdef1234567890abcdef1234567890abcdef12").unwrap();

        // Test basic deserialization
        let note = Note::from_bytes(content.as_bytes(), hash).unwrap();
        assert_eq!(note.content, content);
        assert_eq!(note.id, hash);
        assert_eq!(note.target_object_id, ObjectHash::default());

        // Test deserialization with target
        let note_with_target =
            Note::from_bytes_with_target(content.as_bytes(), hash, target_id).unwrap();
        assert_eq!(note_with_target.content, content);
        assert_eq!(note_with_target.id, hash);
        assert_eq!(note_with_target.target_object_id, target_id);
    }
    #[test]
    fn test_note_deserialization_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let content = "Deserialization test content";
        let hash = ObjectHash::from_str(
            "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        )
        .unwrap();
        let target_id = ObjectHash::from_str(
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        )
        .unwrap();

        // Test basic deserialization
        let note = Note::from_bytes(content.as_bytes(), hash).unwrap();
        assert_eq!(note.content, content);
        assert_eq!(note.id, hash);
        assert_eq!(note.target_object_id, ObjectHash::default());

        // Test deserialization with target
        let note_with_target =
            Note::from_bytes_with_target(content.as_bytes(), hash, target_id).unwrap();
        assert_eq!(note_with_target.content, content);
        assert_eq!(note_with_target.id, hash);
        assert_eq!(note_with_target.target_object_id, target_id);
    }

    #[test]
    fn test_note_with_target_methods() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let target_id = ObjectHash::from_str("1234567890abcdef1234567890abcdef12345678").unwrap();
        let content = "Test note with target methods";
        let note = Note::new(target_id, content.to_string());

        // Test serialization with target
        let (data, returned_target) = note.to_data_with_target().unwrap();
        assert_eq!(data, content.as_bytes());
        assert_eq!(returned_target, target_id);

        // Test deserialization with target
        let restored_note = Note::from_bytes_with_target(&data, note.id, target_id).unwrap();
        assert_eq!(restored_note, note);
        assert_eq!(restored_note.target_object_id, target_id);
        assert_eq!(restored_note.content, content);
    }
    #[test]
    fn test_note_with_target_methods_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let target_id = ObjectHash::from_str(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let content = "Test note with target methods";
        let note = Note::new(target_id, content.to_string());

        // Test serialization with target
        let (data, returned_target) = note.to_data_with_target().unwrap();
        assert_eq!(data, content.as_bytes());
        assert_eq!(returned_target, target_id);

        // Test deserialization with target
        let restored_note = Note::from_bytes_with_target(&data, note.id, target_id).unwrap();
        assert_eq!(restored_note, note);
        assert_eq!(restored_note.target_object_id, target_id);
        assert_eq!(restored_note.content, content);
    }

    #[test]
    fn test_note_error_handling() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        // Test invalid UTF-8
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let hash = ObjectHash::from_str("3333333333333333333333333333333333333333").unwrap();
        let target = ObjectHash::from_str("4444444444444444444444444444444444444444").unwrap();
        let result = Note::from_bytes(&invalid_utf8, hash);
        assert!(result.is_err());

        let result_with_target = Note::from_bytes_with_target(&invalid_utf8, hash, target);
        assert!(result_with_target.is_err());
    }
    #[test]
    fn test_note_error_handling_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        // Test invalid UTF-8
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let hash = ObjectHash::from_str(
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();
        let target = ObjectHash::from_str(
            "4444444444444444444444444444444444444444444444444444444444444444",
        )
        .unwrap();
        let result = Note::from_bytes(&invalid_utf8, hash);
        assert!(result.is_err());

        let result_with_target = Note::from_bytes_with_target(&invalid_utf8, hash, target);
        assert!(result_with_target.is_err());
    }
    #[test]
    fn test_note_demo_functionality() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        // This is a demonstration test that shows the complete functionality
        // It's kept separate from unit tests for clarity
        println!("\n🚀 Git Note Object Demo - Best Practices");
        println!("==========================================");

        let commit_id = ObjectHash::from_str("a1b2c3d4e5f6789012345678901234567890abcd").unwrap();

        println!("\n1️⃣ Creating a new Note object:");
        let note = Note::new(
            commit_id,
            "Code review: LGTM! Great implementation.".to_string(),
        );
        println!("   Target Commit: {}", note.target_object_id);
        println!("   Note ID: {}", note.id);
        println!("   Content: {}", note.content);
        println!("   Size: {} bytes", note.get_size());

        println!("\n2️⃣ Serializing Note with target association:");
        let (serialized_data, target_id) = note.to_data_with_target().unwrap();
        println!("   Serialized size: {} bytes", serialized_data.len());
        println!("   Target object ID: {}", target_id);
        println!(
            "   Git object format: blob {}\\0<content>",
            note.content.len()
        );
        println!(
            "   Raw data preview: {:?}...",
            &serialized_data[..std::cmp::min(30, serialized_data.len())]
        );

        println!("\n3️⃣ Basic deserialization (ObjectTrait):");
        let basic_note = Note::from_bytes(&serialized_data, note.id).unwrap();
        println!("   Successfully deserialized!");
        println!(
            "   Target Commit: {} (default - target managed externally)",
            basic_note.target_object_id
        );
        println!("   Content: {}", basic_note.content);
        println!("   Content matches: {}", note.content == basic_note.content);

        println!("\n4️⃣ Best practice deserialization (with target):");
        let complete_note =
            Note::from_bytes_with_target(&serialized_data, note.id, target_id).unwrap();
        println!("   Successfully deserialized with target!");
        println!("   Target Commit: {}", complete_note.target_object_id);
        println!("   Content: {}", complete_note.content);
        println!("   Complete objects are equal: {}", note == complete_note);

        // Basic assertions to ensure demo works
        assert_eq!(note, complete_note);
        assert_eq!(target_id, commit_id);
    }
    #[test]
    fn test_note_demo_functionality_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        // This is a demonstration test that shows the complete functionality
        // It's kept separate from unit tests for clarity
        println!("\n🚀 Git Note Object Demo - Best Practices");
        println!("==========================================");

        let commit_id = ObjectHash::from_str(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        println!("\n1️⃣ Creating a new Note object:");
        let note = Note::new(
            commit_id,
            "Code review: LGTM! Great implementation.".to_string(),
        );
        println!("   Target Commit: {}", note.target_object_id);
        println!("   Note ID: {}", note.id);
        println!("   Content: {}", note.content);
        println!("   Size: {} bytes", note.get_size());

        println!("\n2️⃣ Serializing Note with target association:");
        let (serialized_data, target_id) = note.to_data_with_target().unwrap();
        println!("   Serialized size: {} bytes", serialized_data.len());
        println!("   Target object ID: {}", target_id);
        println!(
            "   Git object format: blob {}\\0<content>",
            note.content.len()
        );
        println!(
            "   Raw data preview: {:?}...",
            &serialized_data[..std::cmp::min(30, serialized_data.len())]
        );

        println!("\n3️⃣ Basic deserialization (ObjectTrait):");
        let basic_note = Note::from_bytes(&serialized_data, note.id).unwrap();
        println!("   Successfully deserialized!");
        println!(
            "   Target Commit: {} (default - target managed externally)",
            basic_note.target_object_id
        );
        println!("   Content: {}", basic_note.content);
        println!("   Content matches: {}", note.content == basic_note.content);

        println!("\n4️⃣ Best practice deserialization (with target):");
        let complete_note =
            Note::from_bytes_with_target(&serialized_data, note.id, target_id).unwrap();
        println!("   Successfully deserialized with target!");
        println!("   Target Commit: {}", complete_note.target_object_id);
        println!("   Content: {}", complete_note.content);
        println!("   Complete objects are equal: {}", note == complete_note);

        // Basic assertions to ensure demo works
        assert_eq!(note, complete_note);
        assert_eq!(target_id, commit_id);
    }
}
