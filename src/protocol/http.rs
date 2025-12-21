//! HTTP transport adapter that wires request parsing and content negotiation onto the generic
//! `GitProtocol`, exposing helpers for info/refs, upload-pack, and receive-pack endpoints.

use std::collections::HashMap;

/// HTTP transport adapter for Git protocol
///
/// This module provides HTTP-specific handling for Git smart protocol operations.
/// It's a thin wrapper around the core GitProtocol that handles HTTP-specific
/// request/response formatting and uses the utility functions for proper HTTP handling.
use serde::Deserialize;

use super::{
    core::{AuthenticationService, GitProtocol, RepositoryAccess},
    types::{ProtocolError, ProtocolStream},
};

/// HTTP Git protocol handler
pub struct HttpGitHandler<R: RepositoryAccess, A: AuthenticationService> {
    protocol: GitProtocol<R, A>,
}

impl<R: RepositoryAccess, A: AuthenticationService> HttpGitHandler<R, A> {
    /// Create a new HTTP Git handler
    pub fn new(repo_access: R, auth_service: A) -> Self {
        let mut protocol = GitProtocol::new(repo_access, auth_service);
        protocol.set_transport(super::types::TransportProtocol::Http);
        Self { protocol }
    }

    /// Authenticate the HTTP request using provided headers
    /// Call this before invoking handle_* methods if your server requires auth
    pub async fn authenticate_http(
        &self,
        headers: &HashMap<String, String>,
    ) -> Result<(), ProtocolError> {
        self.protocol.authenticate_http(headers).await
    }

    /// Handle HTTP info/refs request
    ///
    /// Processes GET requests to /{repo}/info/refs?service=git-{service}
    /// Uses extract_repo_path and get_service_from_query for proper parsing
    pub async fn handle_info_refs(
        &mut self,
        request_path: &str,
        query: &str,
    ) -> Result<(Vec<u8>, &'static str), ProtocolError> {
        // Validate repository path exists in request
        extract_repo_path(request_path)
            .ok_or_else(|| ProtocolError::InvalidRequest("Invalid repository path".to_string()))?;

        // Get service from query parameters
        let service = get_service_from_query(query).ok_or_else(|| {
            ProtocolError::InvalidRequest("Missing service parameter".to_string())
        })?;

        // Validate it's a Git request
        if !is_git_request(request_path) {
            return Err(ProtocolError::InvalidRequest(
                "Not a Git request".to_string(),
            ));
        }

        let response_data = self.protocol.info_refs(service).await?;
        let content_type = get_advertisement_content_type(service);

        Ok((response_data, content_type))
    }

    /// Handle HTTP upload-pack request
    ///
    /// Processes POST requests to /{repo}/git-upload-pack
    pub async fn handle_upload_pack(
        &mut self,
        request_path: &str,
        request_body: &[u8],
    ) -> Result<(ProtocolStream, &'static str), ProtocolError> {
        // Validate repository path exists in request
        extract_repo_path(request_path)
            .ok_or_else(|| ProtocolError::InvalidRequest("Invalid repository path".to_string()))?;

        // Validate it's a Git request
        if !is_git_request(request_path) {
            return Err(ProtocolError::InvalidRequest(
                "Not a Git request".to_string(),
            ));
        }

        let response_stream = self.protocol.upload_pack(request_body).await?;
        let content_type = get_content_type("git-upload-pack");

        Ok((response_stream, content_type))
    }

    /// Handle HTTP receive-pack request
    ///
    /// Processes POST requests to /{repo}/git-receive-pack
    pub async fn handle_receive_pack(
        &mut self,
        request_path: &str,
        request_stream: ProtocolStream,
    ) -> Result<(ProtocolStream, &'static str), ProtocolError> {
        // Validate repository path exists in request
        extract_repo_path(request_path)
            .ok_or_else(|| ProtocolError::InvalidRequest("Invalid repository path".to_string()))?;

        // Validate it's a Git request
        if !is_git_request(request_path) {
            return Err(ProtocolError::InvalidRequest(
                "Not a Git request".to_string(),
            ));
        }

        let response_stream = self.protocol.receive_pack(request_stream).await?;
        let content_type = get_content_type("git-receive-pack");

        Ok((response_stream, content_type))
    }
}

/// HTTP-specific utility functions
/// Get content type for Git HTTP responses
pub fn get_content_type(service: &str) -> &'static str {
    match service {
        "git-upload-pack" => "application/x-git-upload-pack-result",
        "git-receive-pack" => "application/x-git-receive-pack-result",
        _ => "application/x-git-upload-pack-advertisement",
    }
}

/// Get content type for Git HTTP info/refs advertisement
pub fn get_advertisement_content_type(service: &str) -> &'static str {
    match service {
        "git-upload-pack" => "application/x-git-upload-pack-advertisement",
        "git-receive-pack" => "application/x-git-receive-pack-advertisement",
        _ => "application/x-git-upload-pack-advertisement",
    }
}

/// Check if request is a Git smart protocol request
pub fn is_git_request(path: &str) -> bool {
    path.ends_with("/info/refs")
        || path.ends_with("/git-upload-pack")
        || path.ends_with("/git-receive-pack")
}

/// Extract repository path from HTTP request path
pub fn extract_repo_path(path: &str) -> Option<&str> {
    if let Some(pos) = path.rfind("/info/refs") {
        Some(&path[..pos])
    } else if let Some(pos) = path.rfind("/git-upload-pack") {
        Some(&path[..pos])
    } else if let Some(pos) = path.rfind("/git-receive-pack") {
        Some(&path[..pos])
    } else {
        None
    }
}

/// Get Git service from query parameters
pub fn get_service_from_query(query: &str) -> Option<&str> {
    for param in query.split('&') {
        if let Some(("service", value)) = param.split_once('=') {
            return Some(value);
        }
    }
    None
}

/// Parameters for git info-refs request
#[derive(Debug, Deserialize)]
pub struct InfoRefsParams {
    pub service: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::core::{AuthenticationService, RepositoryAccess};
    use async_trait::async_trait;

    /// Mock repository access for testing
    #[derive(Clone)]
    struct MockRepo;

    #[async_trait]
    impl RepositoryAccess for MockRepo {
        async fn get_repository_refs(&self) -> Result<Vec<(String, String)>, ProtocolError> {
            Ok(vec![("refs/heads/main".into(), "0".repeat(40))])
        }
        async fn has_object(&self, _object_hash: &str) -> Result<bool, ProtocolError> {
            Ok(false)
        }
        async fn get_object(&self, _object_hash: &str) -> Result<Vec<u8>, ProtocolError> {
            Ok(Vec::new())
        }
        async fn store_pack_data(&self, _pack_data: &[u8]) -> Result<(), ProtocolError> {
            Ok(())
        }
        async fn update_reference(
            &self,
            _ref_name: &str,
            _old_hash: Option<&str>,
            _new_hash: &str,
        ) -> Result<(), ProtocolError> {
            Ok(())
        }
        async fn get_objects_for_pack(
            &self,
            _wants: &[String],
            _haves: &[String],
        ) -> Result<Vec<String>, ProtocolError> {
            Ok(Vec::new())
        }
        async fn has_default_branch(&self) -> Result<bool, ProtocolError> {
            Ok(false)
        }
        async fn post_receive_hook(&self) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    struct MockAuth;
    #[async_trait]
    impl AuthenticationService for MockAuth {
        async fn authenticate_http(
            &self,
            _headers: &std::collections::HashMap<String, String>,
        ) -> Result<(), ProtocolError> {
            Ok(())
        }
        async fn authenticate_ssh(
            &self,
            _username: &str,
            _public_key: &[u8],
        ) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    /// Helper to create HttpGitHandler with mock repo and auth
    fn make_handler() -> HttpGitHandler<MockRepo, MockAuth> {
        HttpGitHandler::new(MockRepo, MockAuth)
    }

    /// extract_repo_path should strip known suffixes.
    #[test]
    fn extract_repo_path_variants() {
        assert_eq!(extract_repo_path("/repo/info/refs"), Some("/repo"));
        assert_eq!(extract_repo_path("/repo/git-upload-pack"), Some("/repo"));
        assert_eq!(extract_repo_path("/repo/git-receive-pack"), Some("/repo"));
        assert!(extract_repo_path("/repo/other").is_none());
    }

    /// get_service_from_query should return value when present.
    #[test]
    fn parse_service_from_query() {
        assert_eq!(get_service_from_query("service=git-upload-pack"), Some("git-upload-pack"));
        assert_eq!(get_service_from_query("foo=bar&service=git-receive-pack"), Some("git-receive-pack"));
        assert!(get_service_from_query("foo=bar").is_none());
    }

    /// is_git_request should recognize Git smart protocol endpoints.
    #[test]
    fn detect_git_request() {
        assert!(is_git_request("/repo/info/refs"));
        assert!(is_git_request("/repo/git-upload-pack"));
        assert!(is_git_request("/repo/git-receive-pack"));
        assert!(!is_git_request("/repo/other"));
    }

    /// handle_info_refs should succeed on valid path/service and return content-type.
    #[tokio::test]
    async fn handle_info_refs_ok() {
        let mut handler = make_handler();
        let (data, content_type) = handler
            .handle_info_refs("/repo/info/refs", "service=git-upload-pack")
            .await
            .expect("info_refs");
        assert!(!data.is_empty());
        assert_eq!(content_type, get_advertisement_content_type("git-upload-pack"));
    }

    /// Missing service param should error.
    #[tokio::test]
    async fn handle_info_refs_missing_service_errors() {
        let mut handler = make_handler();
        let err = handler
            .handle_info_refs("/repo/info/refs", "")
            .await
            .unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidRequest(_)));
    }

    /// Non-git paths should error.
    #[tokio::test]
    async fn handle_info_refs_non_git_path_errors() {
        let mut handler = make_handler();
        let err = handler
            .handle_info_refs("/repo/other", "service=git-upload-pack")
            .await
            .unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidRequest(_)));
    }
}
