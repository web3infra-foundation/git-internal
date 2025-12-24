//! SSH transport adapter for the Git smart protocol. Wraps the shared `GitProtocol` core with
//! helpers for authenticating connections, parsing SSH commands, and serving upload/receive-pack
//! requests over interactive channels.

use super::{
    core::{AuthenticationService, GitProtocol, RepositoryAccess},
    types::{ProtocolError, ProtocolStream},
};

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
        request_data: &[u8],
    ) -> Result<ProtocolStream, ProtocolError> {
        self.protocol.upload_pack(request_data).await
    }

    /// Handle git-receive-pack command (for push)
    pub async fn handle_receive_pack(
        &mut self,
        request_stream: ProtocolStream,
    ) -> Result<ProtocolStream, ProtocolError> {
        self.protocol.receive_pack(request_stream).await
    }

    /// Handle info/refs request for SSH
    pub async fn handle_info_refs(&mut self, service: &str) -> Result<Vec<u8>, ProtocolError> {
        self.protocol.info_refs(service).await
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

#[cfg(test)]
mod tests {
    use super::*;

    /// parse_ssh_command should split the command and arguments when present.
    #[test]
    fn parse_command_with_args() {
        let input = "git-upload-pack /repos/demo.git";
        let parsed = parse_ssh_command(input).expect("command should parse");
        assert_eq!(parsed.0, "git-upload-pack");
        assert_eq!(parsed.1, vec!["/repos/demo.git".to_string()]);
    }

    /// parse_ssh_command should return None for empty input.
    #[test]
    fn parse_command_empty_returns_none() {
        assert!(parse_ssh_command("").is_none());
        assert!(parse_ssh_command("   ").is_none());
    }

    /// is_git_ssh_command identifies upload-pack and receive-pack only.
    #[test]
    fn validate_git_ssh_commands() {
        assert!(is_git_ssh_command("git-upload-pack"));
        assert!(is_git_ssh_command("git-receive-pack"));
        assert!(!is_git_ssh_command("git-upload-archive"));
        assert!(!is_git_ssh_command("other"));
    }

    /// extract_repo_path_from_args returns the first argument if present.
    #[test]
    fn extract_repo_path_from_first_arg() {
        let args = vec!["/repos/demo.git".to_string(), "--stateless-rpc".to_string()];
        assert_eq!(extract_repo_path_from_args(&args), Some("/repos/demo.git"));
        let empty: Vec<String> = vec![];
        assert_eq!(extract_repo_path_from_args(&empty), None);
    }
}
