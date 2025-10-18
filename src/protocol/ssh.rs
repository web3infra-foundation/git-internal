/// SSH transport adapter for Git protocol
///
/// This module provides SSH-specific handling for Git smart protocol operations.
/// It's a thin wrapper around the core GitProtocol that handles SSH command
/// execution and data streaming.
use super::core::{AuthenticationService, GitProtocol, RepositoryAccess};
use super::types::ProtocolError;
use bytes::Bytes;
use futures::stream::Stream;
use std::pin::Pin;

/// SSH Git protocol handler
pub struct SshGitHandler<R: RepositoryAccess, A: AuthenticationService> {
    protocol: GitProtocol<R, A>,
}

impl<R: RepositoryAccess, A: AuthenticationService> SshGitHandler<R, A> {
    /// Create a new SSH Git handler
    pub fn new(repo_access: R, auth_service: A) -> Self {
        let mut protocol = GitProtocol::new(repo_access, auth_service);
        protocol.set_transport(super::types::TransportProtocol::Ssh);
        Self { protocol }
    }

    /// Authenticate SSH session using username and public key
    /// Call this once after SSH handshake, before running Git commands
    pub async fn authenticate_ssh(
        &self,
        username: &str,
        public_key: &[u8],
    ) -> Result<(), ProtocolError> {
        self.protocol.authenticate_ssh(username, public_key).await
    }

    /// Handle git-upload-pack command (for clone/fetch)
    pub async fn handle_upload_pack(
        &mut self,
        repo_path: &str,
        request_data: &[u8],
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, ProtocolError>> + Send>>, ProtocolError>
    {
        self.protocol.upload_pack(repo_path, request_data).await
    }

    /// Handle git-receive-pack command (for push)
    pub async fn handle_receive_pack(
        &mut self,
        repo_path: &str,
        request_stream: Pin<Box<dyn Stream<Item = Result<Bytes, ProtocolError>> + Send>>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, ProtocolError>> + Send>>, ProtocolError>
    {
        self.protocol.receive_pack(repo_path, request_stream).await
    }

    /// Handle info/refs request for SSH
    pub async fn handle_info_refs(
        &mut self,
        repo_path: &str,
        service: &str,
    ) -> Result<Vec<u8>, ProtocolError> {
        self.protocol.info_refs(repo_path, service).await
    }
}

/// SSH-specific utility functions
/// Parse SSH command line into command and arguments
pub fn parse_ssh_command(command_line: &str) -> Option<(String, Vec<String>)> {
    let parts: Vec<&str> = command_line.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let command = parts[0].to_string();
    let args = parts[1..].iter().map(|s| s.to_string()).collect();

    Some((command, args))
}

/// Check if command is a valid Git SSH command
pub fn is_git_ssh_command(command: &str) -> bool {
    matches!(command, "git-upload-pack" | "git-receive-pack")
}

/// Extract repository path from SSH command arguments
pub fn extract_repo_path_from_args(args: &[String]) -> Option<&str> {
    args.first().map(|s| s.as_str())
}
