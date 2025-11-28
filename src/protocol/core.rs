//! Core Git protocol implementation
//!
//! This module provides the main `GitProtocol` struct and `RepositoryAccess` trait
//! that form the core interface of the git-internal library.
use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;

use crate::hash::ObjectHash;
use crate::internal::object::ObjectTrait;

use crate::protocol::smart::SmartProtocol;
use crate::protocol::types::{ProtocolError, ProtocolStream, ServiceType};

/// Repository access trait for storage operations
///
/// This trait only handles storage-level operations, not Git protocol details.
/// The git-internal library handles all Git protocol formatting and parsing.
#[async_trait]
pub trait RepositoryAccess: Send + Sync + Clone {
    /// Get repository references as raw (name, hash) pairs
    async fn get_repository_refs(&self) -> Result<Vec<(String, String)>, ProtocolError>;

    /// Check if an object exists in the repository
    async fn has_object(&self, object_hash: &str) -> Result<bool, ProtocolError>;

    /// Get raw object data by hash
    async fn get_object(&self, object_hash: &str) -> Result<Vec<u8>, ProtocolError>;

    /// Store pack data in the repository
    async fn store_pack_data(&self, pack_data: &[u8]) -> Result<(), ProtocolError>;

    /// Update a single reference
    async fn update_reference(
        &self,
        ref_name: &str,
        old_hash: Option<&str>,
        new_hash: &str,
    ) -> Result<(), ProtocolError>;

    /// Get objects needed for pack generation
    async fn get_objects_for_pack(
        &self,
        wants: &[String],
        haves: &[String],
    ) -> Result<Vec<String>, ProtocolError>;

    /// Check if repository has a default branch
    async fn has_default_branch(&self) -> Result<bool, ProtocolError>;

    /// Post-receive hook after successful push
    async fn post_receive_hook(&self) -> Result<(), ProtocolError>;

    /// Get blob data by hash
    ///
    /// Default implementation parses the object data using the internal object module.
    /// Override this method if you need custom blob handling logic.
    async fn get_blob(
        &self,
        object_hash: &str,
    ) -> Result<crate::internal::object::blob::Blob, ProtocolError> {
        let data = self.get_object(object_hash).await?;
        let hash = ObjectHash::from_str(object_hash)
            .map_err(|e| ProtocolError::repository_error(format!("Invalid hash format: {}", e)))?;

        crate::internal::object::blob::Blob::from_bytes(&data, hash)
            .map_err(|e| ProtocolError::repository_error(format!("Failed to parse blob: {}", e)))
    }

    /// Get commit data by hash
    ///
    /// Default implementation parses the object data using the internal object module.
    /// Override this method if you need custom commit handling logic.
    async fn get_commit(
        &self,
        commit_hash: &str,
    ) -> Result<crate::internal::object::commit::Commit, ProtocolError> {
        let data = self.get_object(commit_hash).await?;
        let hash = ObjectHash::from_str(commit_hash)
            .map_err(|e| ProtocolError::repository_error(format!("Invalid hash format: {}", e)))?;

        crate::internal::object::commit::Commit::from_bytes(&data, hash)
            .map_err(|e| ProtocolError::repository_error(format!("Failed to parse commit: {}", e)))
    }

    /// Get tree data by hash
    ///
    /// Default implementation parses the object data using the internal object module.
    /// Override this method if you need custom tree handling logic.
    async fn get_tree(
        &self,
        tree_hash: &str,
    ) -> Result<crate::internal::object::tree::Tree, ProtocolError> {
        let data = self.get_object(tree_hash).await?;
        let hash = ObjectHash::from_str(tree_hash)
            .map_err(|e| ProtocolError::repository_error(format!("Invalid hash format: {}", e)))?;

        crate::internal::object::tree::Tree::from_bytes(&data, hash)
            .map_err(|e| ProtocolError::repository_error(format!("Failed to parse tree: {}", e)))
    }

    /// Check if a commit exists
    ///
    /// Default implementation checks object existence and validates it's a commit.
    /// Override this method if you have more efficient commit existence checking.
    async fn commit_exists(&self, commit_hash: &str) -> Result<bool, ProtocolError> {
        match self.has_object(commit_hash).await {
            Ok(exists) => {
                if !exists {
                    return Ok(false);
                }

                // Verify it's actually a commit by trying to parse it
                match self.get_commit(commit_hash).await {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false), // Object exists but is not a valid commit
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Handle pack objects after unpacking
    ///
    /// Default implementation stores each object individually using store_pack_data.
    /// Override this method if you need batch processing or custom storage logic.
    async fn handle_pack_objects(
        &self,
        commits: Vec<crate::internal::object::commit::Commit>,
        trees: Vec<crate::internal::object::tree::Tree>,
        blobs: Vec<crate::internal::object::blob::Blob>,
    ) -> Result<(), ProtocolError> {
        // Store blobs
        for blob in blobs {
            let data = blob.to_data().map_err(|e| {
                ProtocolError::repository_error(format!("Failed to serialize blob: {}", e))
            })?;
            self.store_pack_data(&data).await.map_err(|e| {
                ProtocolError::repository_error(format!("Failed to store blob {}: {}", blob.id, e))
            })?;
        }

        // Store trees
        for tree in trees {
            let data = tree.to_data().map_err(|e| {
                ProtocolError::repository_error(format!("Failed to serialize tree: {}", e))
            })?;
            self.store_pack_data(&data).await.map_err(|e| {
                ProtocolError::repository_error(format!("Failed to store tree {}: {}", tree.id, e))
            })?;
        }

        // Store commits
        for commit in commits {
            let data = commit.to_data().map_err(|e| {
                ProtocolError::repository_error(format!("Failed to serialize commit: {}", e))
            })?;
            self.store_pack_data(&data).await.map_err(|e| {
                ProtocolError::repository_error(format!(
                    "Failed to store commit {}: {}",
                    commit.id, e
                ))
            })?;
        }

        Ok(())
    }
}

/// Authentication service trait
#[async_trait]
pub trait AuthenticationService: Send + Sync {
    /// Authenticate HTTP request
    async fn authenticate_http(
        &self,
        headers: &std::collections::HashMap<String, String>,
    ) -> Result<(), ProtocolError>;

    /// Authenticate SSH public key
    async fn authenticate_ssh(
        &self,
        username: &str,
        public_key: &[u8],
    ) -> Result<(), ProtocolError>;
}

/// Transport-agnostic Git smart protocol handler
/// Main Git protocol handler
///
/// This struct provides the core Git protocol implementation that works
/// across HTTP, SSH, and other transports. It uses SmartProtocol internally
/// to handle all Git protocol details.
pub struct GitProtocol<R: RepositoryAccess, A: AuthenticationService> {
    smart_protocol: SmartProtocol<R, A>,
}

impl<R: RepositoryAccess, A: AuthenticationService> GitProtocol<R, A> {
    /// Create a new GitProtocol instance
    pub fn new(repo_access: R, auth_service: A) -> Self {
        Self {
            smart_protocol: SmartProtocol::new(
                super::types::TransportProtocol::Http,
                repo_access,
                auth_service,
            ),
        }
    }

    /// Authenticate HTTP request before serving Git operations
    pub async fn authenticate_http(
        &self,
        headers: &HashMap<String, String>,
    ) -> Result<(), ProtocolError> {
        self.smart_protocol.authenticate_http(headers).await
    }

    /// Authenticate SSH session before serving Git operations
    pub async fn authenticate_ssh(
        &self,
        username: &str,
        public_key: &[u8],
    ) -> Result<(), ProtocolError> {
        self.smart_protocol
            .authenticate_ssh(username, public_key)
            .await
    }

    /// Set transport protocol (Http, Ssh, etc.)
    pub fn set_transport(&mut self, protocol: super::types::TransportProtocol) {
        self.smart_protocol.set_transport_protocol(protocol);
    }

    /// Handle git info-refs request
    pub async fn info_refs(&self, service: &str) -> Result<Vec<u8>, ProtocolError> {
        let service_type = match service {
            "git-upload-pack" => ServiceType::UploadPack,
            "git-receive-pack" => ServiceType::ReceivePack,
            _ => return Err(ProtocolError::invalid_service(service)),
        };

        let bytes = self.smart_protocol.git_info_refs(service_type).await?;
        Ok(bytes.to_vec())
    }

    /// Handle git-upload-pack request (for clone/fetch)
    pub async fn upload_pack(
        &mut self,
        request_data: &[u8],
    ) -> Result<ProtocolStream, ProtocolError> {
        let request_bytes = bytes::Bytes::from(request_data.to_vec());
        let (stream, _) = self.smart_protocol.git_upload_pack(request_bytes).await?;
        Ok(Box::pin(stream.map(|data| Ok(Bytes::from(data)))))
    }

    /// Handle git-receive-pack request (for push)
    pub async fn receive_pack(
        &mut self,
        request_stream: ProtocolStream,
    ) -> Result<ProtocolStream, ProtocolError> {
        let result_bytes = self
            .smart_protocol
            .git_receive_pack_stream(request_stream)
            .await?;
        // Return the report status as a single-chunk stream
        Ok(Box::pin(futures::stream::once(async { Ok(result_bytes) })))
    }
}
