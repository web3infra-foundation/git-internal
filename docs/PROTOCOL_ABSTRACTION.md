## Git Protocol Abstraction in git-internal

The git-internal library provides a clean, transport-agnostic abstraction for Git smart protocol operations. This document outlines how the protocol abstraction works and how it separates concerns between transport layers, business logic, and Git protocol handling.

### 1. Architecture Overview

The git-internal library implements a layered architecture that cleanly separates different concerns:

    •	**Transport Layer**: HTTP and SSH adapters that handle transport-specific details
    •	**Protocol Layer**: Core Git smart protocol implementation that is transport-agnostic
    •	**Business Logic Layer**: Trait-based abstractions for repository access and authentication
    •	**Storage Layer**: Internal Git object handling and pack file operations

This separation allows the same Git protocol logic to work across different transports and integrate with any storage backend or business system.

### 2. Core Abstractions

#### (1) RepositoryAccess Trait

The `RepositoryAccess` trait provides a clean interface for storage operations without exposing Git protocol details:

```rust
#[async_trait]
pub trait RepositoryAccess: Send + Sync + Clone {
    // Basic repository operations
    async fn get_repository_refs(&self, repo_path: &str) -> Result<Vec<(String, String)>, ProtocolError>;
    async fn has_object(&self, repo_path: &str, object_hash: &str) -> Result<bool, ProtocolError>;
    async fn get_object(&self, repo_path: &str, object_hash: &str) -> Result<Vec<u8>, ProtocolError>;
    async fn store_pack_data(&self, repo_path: &str, pack_data: &[u8]) -> Result<(), ProtocolError>;

    // Reference management
    async fn update_reference(&self, repo_path: &str, ref_name: &str, old_hash: Option<&str>, new_hash: &str) -> Result<(), ProtocolError>;

    // Pack operations
    async fn get_objects_for_pack(&self, repo_path: &str, wants: &[String], haves: &[String]) -> Result<Vec<String>, ProtocolError>;

    // Repo-specific utilities
    async fn has_default_branch(&self, repo_path: &str) -> Result<bool, ProtocolError>;
    async fn post_receive_hook(&self, repo_path: &str) -> Result<(), ProtocolError>;

    // Optional helpers with default impls (can be overridden):
    // - get_blob/get_commit/get_tree
    // - commit_exists
    // - handle_pack_objects(repo_path, commits, trees, blobs)
}
```

This trait abstracts away all storage implementation details, allowing integration with any backend (filesystem, database, cloud storage, etc.).

#### (2) AuthenticationService Trait

The `AuthenticationService` trait handles authentication for both HTTP and SSH transports:

```rust
#[async_trait]
pub trait AuthenticationService: Send + Sync {
    /// Authenticate HTTP request using headers
    async fn authenticate_http(
        &self,
        headers: &std::collections::HashMap<String, String>
    ) -> Result<(), ProtocolError>;

    /// Authenticate SSH connection using public key
    async fn authenticate_ssh(&self, username: &str, public_key: &[u8]) -> Result<(), ProtocolError>;
}
```

This allows for flexible authentication strategies while keeping the protocol layer agnostic to authentication details.

### 3. Transport Layer Abstraction

#### (1) HTTP Transport Handler

The `HttpGitHandler` provides HTTP-specific request handling:

```rust
pub struct HttpGitHandler<R: RepositoryAccess, A: AuthenticationService> {
    protocol: GitProtocol<R, A>,
}

impl<R: RepositoryAccess, A: AuthenticationService> HttpGitHandler<R, A> {
    // Handle HTTP info/refs requests
    pub async fn handle_info_refs(&mut self, request_path: &str, query: &str) -> Result<(Vec<u8>, &'static str), ProtocolError>;

    // Handle HTTP upload-pack requests (clone/fetch)
    pub async fn handle_upload_pack(
        &mut self,
        request_path: &str,
        request_body: &[u8],
    ) -> Result<
        (
            std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>,
            &'static str,
        ),
        ProtocolError,
    >;

    // Handle HTTP receive-pack requests (push)
    pub async fn handle_receive_pack(
        &mut self,
        request_path: &str,
        request_stream: std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>,
    ) -> Result<
        (
            std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>,
            &'static str,
        ),
        ProtocolError,
    >;

    // Authenticate HTTP request using headers (call before handle_*)
    pub async fn authenticate_http(&self, headers: &std::collections::HashMap<String, String>) -> Result<(), ProtocolError>;
}
```

The HTTP handler includes utility functions for:

- Extracting repository paths from URLs
- Parsing query parameters
- Setting appropriate content types
- Validating Git requests

#### (2) SSH Transport Handler

The `SshGitHandler` provides SSH-specific command handling:

```rust
pub struct SshGitHandler<R: RepositoryAccess, A: AuthenticationService> {
    protocol: GitProtocol<R, A>,
}

impl<R: RepositoryAccess, A: AuthenticationService> SshGitHandler<R, A> {
    // Handle git-upload-pack command
    pub async fn handle_upload_pack(&mut self, repo_path: &str, request_data: &[u8]) -> Result<std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>, ProtocolError>;

    // Handle git-receive-pack command
    pub async fn handle_receive_pack(
        &mut self,
        repo_path: &str,
        request_stream: std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>,
    ) -> Result<std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ProtocolError>> + Send>>, ProtocolError>;

    // Handle info/refs for SSH
    pub async fn handle_info_refs(&mut self, repo_path: &str, service: &str) -> Result<Vec<u8>, ProtocolError>;

    // Authenticate SSH session (call once after handshake)
    pub async fn authenticate_ssh(&self, username: &str, public_key: &[u8]) -> Result<(), ProtocolError>;
}
```

The SSH handler includes utility functions for:

- Parsing SSH command lines
- Validating Git SSH commands
- Extracting repository paths from arguments

### 4. Protocol Layer Implementation

#### (1) GitProtocol Core

The `GitProtocol` struct provides the main transport-agnostic interface:

```rust
pub struct GitProtocol<R: RepositoryAccess, A: AuthenticationService> {
    smart_protocol: SmartProtocol<R, A>,
}
```

This struct delegates to `SmartProtocol` for the actual Git protocol implementation while providing a clean public API.

Authentication helpers are also exposed to keep adapters minimal:

```rust
impl<R: RepositoryAccess, A: AuthenticationService> GitProtocol<R, A> {
    pub async fn authenticate_http(&self, headers: &std::collections::HashMap<String, String>) -> Result<(), ProtocolError>;
    pub async fn authenticate_ssh(&self, username: &str, public_key: &[u8]) -> Result<(), ProtocolError>;
}
```

#### (2) SmartProtocol Implementation

The `SmartProtocol` handles all Git smart protocol details:

- **info/refs**: Advertises repository capabilities and references
- **upload-pack**: Handles fetch/clone operations with pack generation
- **receive-pack**: Handles push operations with pack unpacking

The protocol implementation is completely transport-agnostic and uses the trait abstractions for all external dependencies.

### 5. Pack File Handling

The library includes comprehensive pack file support:

#### (1) PackGenerator

Handles pack file generation for upload-pack operations:

```rust
pub struct PackGenerator<'a, R: RepositoryAccess> {
    repo_access: &'a R,
}
```

Features:

- Full pack generation for initial clones
- Incremental pack generation for fetches
- Efficient object traversal and collection
- Streaming pack output

#### (2) Pack Decoding

Built on the internal pack module for robust pack file handling:

- Delta object reconstruction
- Streaming pack decoding
- Memory-efficient processing
- Error recovery and validation

### 6. Integration Benefits

#### (1) Transport Independence

The same Git protocol logic works across:

- HTTP/HTTPS web servers
- SSH servers
- Custom transport protocols
- Local file system access

#### (2) Business Logic Separation

The trait-based design allows:

- Integration with any storage backend
- Custom authentication strategies
- Flexible repository management
- Easy testing and mocking

#### (3) Framework Agnostic

The library doesn't depend on:

- Specific web frameworks (Axum, Warp, etc.)
- SSH libraries
- Database systems
- Authentication providers

### 7. Usage Example

```rust
use git_internal::{GitProtocol, RepositoryAccess, AuthenticationService};

// Implement traits for your specific backend
struct MyRepository;
struct MyAuth;

#[async_trait]
impl RepositoryAccess for MyRepository {
    // Implement storage operations...
}

#[async_trait]
impl AuthenticationService for MyAuth {
    // Implement authentication...
}

// Use with HTTP
let http_handler = HttpGitHandler::new(MyRepository, MyAuth);
// Enforce auth at the entry (framework provides headers)
http_handler.authenticate_http(&headers).await?;
let response = http_handler.handle_info_refs("/repo.git/info/refs", "service=git-upload-pack").await?;

// Use with SSH
let ssh_handler = SshGitHandler::new(MyRepository, MyAuth);
// Authenticate once after SSH handshake
ssh_handler.authenticate_ssh(username, public_key).await?;
let response = ssh_handler.handle_upload_pack("repo.git", &request_data).await?;

// Error handling
// On authentication failure, methods return ProtocolError::Unauthorized("...")
```

### 8. Advantages

The abstraction provides significant benefits:

    •    **Reduced Coupling**: Business logic is separated from Git protocol details
    •    **Improved Testability**: Each layer can be tested independently
    •    **Enhanced Reusability**: The same protocol logic works across different systems
    •    **Simplified Maintenance**: Changes to transport or storage don't affect protocol logic
    •    **Performance-Oriented**: Optimized pack handling and streaming operations

### 9. Design Notes

- Removed the default `update_ref` method from `RepositoryAccess` to prevent misuse. Callers should always use `update_reference(repo_path, ...)` with an explicit repository path.
- Corrected examples to match the implementation:
  - `AuthenticationService::{authenticate_http, authenticate_ssh}` return `Result<(), ProtocolError>`.
  - `RepositoryAccess` includes `has_default_branch` and `post_receive_hook` utilities.
  - Optional helpers exist with default implementations: `get_blob`, `get_commit`, `get_tree`, `commit_exists`, and `handle_pack_objects(repo_path, ...)`.

## 10. Capabilities & Feature Matrix

This section summarizes supported capabilities and how they map to transport handlers:

- Capability advertisement: `info/refs` exposes negotiated capability lists (`COMMON_CAP_LIST`, `UPLOAD_CAP_LIST`, `RECEIVE_CAP_LIST`).
- LFS advertisement: `COMMON_CAP_LIST` includes `lfs`; negotiation is supported, but object transfer/storage is application-layer (proxy/pass-through only).
- Side-band channels: multiplexed streams supported via `SmartProtocol::build_side_band_format`.
- Pack features: thin-pack, ofs-delta, shallow clones handled by pack encode/decode modules.
- Reference updates: atomic update semantics enforced via `update_reference(repo_path, old, new)`.
- Authentication gate: handlers call `authenticate_http` / `authenticate_ssh` prior to serving operations.

Supported capability constants:

```rust
pub const RECEIVE_CAP_LIST: &str =
    "report-status report-status-v2 delete-refs quiet atomic no-thin ";
pub const COMMON_CAP_LIST: &str =
    "side-band-64k ofs-delta lfs agent=git-internal/0.1.0";
pub const UPLOAD_CAP_LIST: &str =
    "multi_ack_detailed no-done include-tag ";
```

Note: trailing spaces in `RECEIVE_CAP_LIST` and `UPLOAD_CAP_LIST` are intentional; they concatenate with `COMMON_CAP_LIST` in `info/refs` advertisement.

## 11. Error Handling & Mapping

Protocol errors are framework-agnostic and live under `protocol/types.rs`:

- Errors represent Git protocol semantics (e.g., `InvalidService`, `RepositoryNotFound`, `InvalidRequest`).
- Storage and parsing failures are wrapped using helpers like `repository_error(...)`.
- Transport adapters are responsible for mapping errors to framework responses.

Example adapter-side mapping (do not couple the library to HTTP frameworks):

```rust
// In application layer (e.g. HTTP framework adapter)
fn map_protocol_error_to_http(e: ProtocolError) -> http::StatusCode {
    use http::StatusCode;
    match e {
        ProtocolError::InvalidService(_) => StatusCode::BAD_REQUEST,
        ProtocolError::RepositoryNotFound(_) => StatusCode::NOT_FOUND,
        ProtocolError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        ProtocolError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        ProtocolError::RepositoryError(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
```

## 12. Security & Performance Considerations

- Input validation: strict parsing for service/query/path; reject non-Git endpoints early.
- Resource limits: stream-based pack processing to avoid large memory spikes; consider timeouts and rate limiting at transport layer.
- DoS resilience: cap negotiation and bounded buffering to prevent unbounded allocations.
- Authentication: perform auth before any expensive operations; prefer fail-fast.
- Logging: use `tracing` for structured logs; avoid leaking sensitive data in errors.

## 13. Testing Strategy & CI

- Unit tests: cover pkt-line parsing, capability negotiation, ref command parsing.
- Property tests: object parsing and hash invariants using `quickcheck` where applicable.
- Integration tests: pack encode/decode streams on curated datasets (can be long-running).
- CI pipeline: GitHub Actions runs `cargo check`, `cargo clippy -- -D warnings`, and `cargo test`. Long-running tests may be split or marked for scheduled runs if needed.

## 14. Adoption Guide (Trait-based Integration)

Steps to integrate with any business system:

- Implement `RepositoryAccess` for your storage backend:
  - Provide refs/object IO, pack storage, ref updates, and optional helpers.
- Implement `AuthenticationService` for your auth strategy:
  - Validate HTTP headers or SSH public keys.
- Wire transport adapters:
  - HTTP: construct `HttpGitHandler` and route `info/refs`, `upload-pack`, `receive-pack`.
  - SSH: construct `SshGitHandler` and dispatch `git-upload-pack` / `git-receive-pack`.
- Keep error mapping in the application layer to stay framework-agnostic.

Example skeleton:

```rust
let repo = MyRepoBackend::new();
let auth = MyAuthService::new();
let mut http = HttpGitHandler::new(repo.clone(), auth.clone());
http.authenticate_http(&headers).await?;
let (bytes, content_type) = http.handle_info_refs("/repo.git/info/refs", "service=git-upload-pack").await?;

let mut ssh = SshGitHandler::new(repo, auth);
ssh.authenticate_ssh(user, key).await?;
let stream = ssh.handle_upload_pack("repo.git", &request).await?;
```
