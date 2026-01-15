//! Git smart HTTP server using git-internal protocol handlers.
//!
//! This example serves repositories from `GIT_REPO_ROOT` and uses the local `git`
//! binary for repository plumbing (show-ref, cat-file, update-ref).
//! Repo names map to:
//! - `<root>/<name>` if it is a bare repo (contains `objects/`)
//! - `<root>/<name>.git` if bare with `.git` suffix
//! - `<root>/<name>/.git` for non-bare repos
//!
//! Quick test (two terminals):
//! A) Prepare a bare repo and start the server:
//! ```bash
//! mkdir -p /tmp/git-http-demo && git init --bare /tmp/git-http-demo/demo.git
//! GIT_REPO_ROOT=/tmp/git-http-demo cargo run --example http_server
//! ```
//! This creates a server-side repo and starts the HTTP server on port 3000.
//!
//! B) Verify info/refs, push, then clone:
//! ```bash
//! curl -i "http://127.0.0.1:3000/demo.git/info/refs?service=git-receive-pack"
//! mkdir -p /tmp/demo-src && cd /tmp/demo-src
//! git init
//! git config user.name demo
//! git config user.email demo@example.com
//! echo hello > README.md
//! git add README.md
//! git commit -m "init"
//! git remote add origin http://127.0.0.1:3000/demo.git
//! git push -u origin main
//! git clone http://127.0.0.1:3000/demo.git /tmp/demo-clone
//! ```
//! - The curl call checks the Git smart HTTP advertisement.
//! - The push exercises `receive-pack`; replace `main` with `master` if needed.
//! - The clone exercises `upload-pack`.
//!
//! C) Test with SHA-256 repository:
//! ```bash
//! mkdir -p /tmp/git-http-sha256 && git init --bare --object-format=sha256 /tmp/git-http-sha256/demo-sha256.git
//! GIT_REPO_ROOT=/tmp/git-http-sha256 cargo run --example http_server
//! # Then push/clone from a SHA-256 client repository
//! ```
//! The server automatically detects each repository's object format by reading
//! `extensions.objectformat` from the repository config before handling requests.

use std::{
    collections::HashMap,
    io::Write,
    path::{Path as StdPath, PathBuf},
    str::FromStr,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use flate2::{Compression, write::ZlibEncoder};
use futures::StreamExt;
use git_internal::{
    hash::{HashKind, ObjectHash, get_hash_kind, set_hash_kind},
    internal::object::{
        ObjectTrait,
        blob::Blob,
        commit::Commit,
        tree::{Tree, TreeItem, TreeItemMode},
        types::ObjectType,
    },
    protocol::{
        core::{AuthenticationService, RepositoryAccess},
        http::HttpGitHandler,
        types::{ProtocolError, ProtocolStream},
    },
};
use tokio::process::Command;

static GLOBAL_HASH_KIND: OnceLock<HashKind> = OnceLock::new();

/// Repository Access
#[derive(Clone)]
struct FsRepository {
    git_dir: Arc<PathBuf>,
}

impl FsRepository {
    /// Create a new FsRepository for the given git directory.
    fn new(git_dir: PathBuf) -> Self {
        Self {
            git_dir: Arc::new(git_dir),
        }
    }
    /// Create a git command with the appropriate git-dir.
    fn git_cmd(&self) -> Command {
        let mut cmd = Command::new("git");
        cmd.arg("--git-dir").arg(&*self.git_dir);
        cmd
    }
    /// Run a git command with the given arguments.
    async fn run_git<I, S>(&self, args: I) -> Result<std::process::Output, ProtocolError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.git_cmd()
            .args(args)
            .output()
            .await
            .map_err(ProtocolError::Io)
    }
    /// Get the path to the objects directory.
    fn objects_dir(&self) -> PathBuf {
        self.git_dir.join("objects")
    }
    /// Detect the repository's hash algorithm and configure the global hash kind once.
    /// Reads `extensions.objectformat` from the repository config.
    /// If not set, defaults to SHA-1 for backward compatibility.
    async fn detect_and_configure_hash_kind(&self) -> Result<(), ProtocolError> {
        if let Some(kind) = GLOBAL_HASH_KIND.get() {
            set_hash_kind(*kind);
            return Ok(());
        }

        let output = self
            .run_git(["config", "--get", "extensions.objectformat"])
            .await?;

        let detected = if output.status.success() {
            let format = String::from_utf8_lossy(&output.stdout)
                .trim()
                .to_ascii_lowercase();
            match format.as_str() {
                "sha256" => HashKind::Sha256,
                _ => HashKind::Sha1,
            }
        } else {
            // No extensions.objectformat means SHA-1 (default)
            HashKind::Sha1
        };

        match GLOBAL_HASH_KIND.set(detected) {
            Ok(()) => {
                set_hash_kind(detected);
                Ok(())
            }
            Err(_) => {
                if let Some(existing) = GLOBAL_HASH_KIND.get() {
                    if *existing != detected {
                        return Err(ProtocolError::repository_error(format!(
                            "Mixed repository object formats are not supported: server initialized with {existing}, but repository at {:?} uses {detected}",
                            self.git_dir
                        )));
                    }
                    set_hash_kind(*existing);
                    Ok(())
                } else {
                    set_hash_kind(detected);
                    Ok(())
                }
            }
        }
    }
    /// Write a loose object to the objects directory.
    fn write_loose_object(
        &self,
        obj_type: ObjectType,
        data: &[u8],
    ) -> Result<ObjectHash, ProtocolError> {
        let hash = ObjectHash::from_type_and_data(obj_type, data);
        let hex = hash.to_string();
        let (dir, file) = hex.split_at(2);
        let obj_dir = self.objects_dir().join(dir);
        let obj_path = obj_dir.join(file);

        if obj_path.exists() {
            return Ok(hash);
        }

        std::fs::create_dir_all(&obj_dir).map_err(ProtocolError::Io)?;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        let header = format!("{obj_type} {}\0", data.len());
        encoder
            .write_all(header.as_bytes())
            .map_err(ProtocolError::Io)?;
        encoder.write_all(data).map_err(ProtocolError::Io)?;
        let compressed = encoder.finish().map_err(ProtocolError::Io)?;

        std::fs::write(&obj_path, compressed).map_err(ProtocolError::Io)?;
        Ok(hash)
    }
    /// Parse a raw tree listing into TreeItems.
    fn parse_tree_listing(&self, raw: &[u8]) -> Result<Vec<TreeItem>, ProtocolError> {
        let mut items = Vec::new();
        let text = String::from_utf8_lossy(raw);
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let (meta, name) = line
                .split_once('\t')
                .ok_or_else(|| ProtocolError::invalid_request("Invalid tree entry"))?;
            let mut parts = meta.split_whitespace();
            let mode_raw = parts
                .next()
                .ok_or_else(|| ProtocolError::invalid_request("Missing tree mode"))?;
            let _kind = parts
                .next()
                .ok_or_else(|| ProtocolError::invalid_request("Missing tree kind"))?;
            let hash_str = parts
                .next()
                .ok_or_else(|| ProtocolError::invalid_request("Missing tree hash"))?;

            let mode_norm = mode_raw.trim_start_matches('0');
            let mode_bytes = if mode_norm.is_empty() {
                b"0"
            } else {
                mode_norm.as_bytes()
            };
            let mode = TreeItemMode::tree_item_type_from_bytes(mode_bytes)
                .map_err(|e| ProtocolError::repository_error(e.to_string()))?;
            let id = ObjectHash::from_str(hash_str).map_err(ProtocolError::repository_error)?;

            items.push(TreeItem::new(mode, id, name.to_string()));
        }
        Ok(items)
    }
}

#[async_trait]
impl RepositoryAccess for FsRepository {
    /// Get all refs in the repository.
    async fn get_repository_refs(&self) -> Result<Vec<(String, String)>, ProtocolError> {
        let output = self.run_git(["show-ref", "--head"]).await?;
        if !output.status.success() && output.stdout.is_empty() {
            return Ok(Vec::new());
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let mut refs = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let hash = parts.next().unwrap_or_default().to_string();
            let name = parts.next().unwrap_or_default().to_string();
            if !hash.is_empty() && !name.is_empty() {
                refs.push((name, hash));
            }
        }
        Ok(refs)
    }

    /// Check if an object exists by hash.
    async fn has_object(&self, object_hash: &str) -> Result<bool, ProtocolError> {
        let status = self
            .git_cmd()
            .arg("cat-file")
            .arg("-e")
            .arg(object_hash)
            .status()
            .await
            .map_err(ProtocolError::Io)?;
        Ok(status.success())
    }

    /// Get raw object data by hash.
    async fn get_object(&self, object_hash: &str) -> Result<Vec<u8>, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", object_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(object_hash.to_string()));
        }
        Ok(output.stdout)
    }

    /// Store pack data (not implemented in this example).
    async fn store_pack_data(&self, _pack_data: &[u8]) -> Result<(), ProtocolError> {
        Ok(())
    }

    /// Update a reference to point to a new hash.
    async fn update_reference(
        &self,
        ref_name: &str,
        old_hash: Option<&str>,
        new_hash: &str,
    ) -> Result<(), ProtocolError> {
        let zero = ObjectHash::zero_str(get_hash_kind());
        if new_hash == zero {
            let mut cmd = self.git_cmd();
            cmd.arg("update-ref").arg("-d").arg(ref_name);
            if let Some(old) = old_hash {
                cmd.arg(old);
            }
            let out = cmd.output().await.map_err(ProtocolError::Io)?;
            if !out.status.success() {
                return Err(ProtocolError::repository_error(
                    String::from_utf8_lossy(&out.stderr).to_string(),
                ));
            }
            return Ok(());
        }

        let mut cmd = self.git_cmd();
        cmd.arg("update-ref").arg(ref_name).arg(new_hash);
        if let Some(old) = old_hash {
            cmd.arg(old);
        }
        let out = cmd.output().await.map_err(ProtocolError::Io)?;
        if !out.status.success() {
            return Err(ProtocolError::repository_error(
                String::from_utf8_lossy(&out.stderr).to_string(),
            ));
        }
        Ok(())
    }

    /// Get objects needed for a pack (not implemented in this example).
    async fn get_objects_for_pack(
        &self,
        _wants: &[String],
        _haves: &[String],
    ) -> Result<Vec<String>, ProtocolError> {
        Ok(Vec::new())
    }

    /// Check if the repository has a default branch (main or master).
    async fn has_default_branch(&self) -> Result<bool, ProtocolError> {
        let refs = self.get_repository_refs().await?;
        Ok(refs
            .iter()
            .any(|(name, _)| name == "refs/heads/main" || name == "refs/heads/master"))
    }

    /// Post-receive hook (not implemented in this example).
    async fn post_receive_hook(&self) -> Result<(), ProtocolError> {
        Ok(())
    }

    /// Get a Commit object by hash.
    async fn get_commit(&self, commit_hash: &str) -> Result<Commit, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", commit_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(commit_hash.to_string()));
        }
        let hash = ObjectHash::from_str(commit_hash).map_err(ProtocolError::repository_error)?;
        Commit::from_bytes(&output.stdout, hash)
            .map_err(|e| ProtocolError::repository_error(e.to_string()))
    }

    /// Get a Tree object by hash.
    async fn get_tree(&self, tree_hash: &str) -> Result<Tree, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", tree_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(tree_hash.to_string()));
        }
        let id = ObjectHash::from_str(tree_hash).map_err(ProtocolError::repository_error)?;
        let items = self.parse_tree_listing(&output.stdout)?;
        if items.is_empty() {
            return Ok(Tree {
                id,
                tree_items: Vec::new(),
            });
        }
        Tree::from_tree_items(items).map_err(|e| ProtocolError::repository_error(e.to_string()))
    }

    /// Get a Blob object by hash.
    async fn get_blob(&self, blob_hash: &str) -> Result<Blob, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", blob_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(blob_hash.to_string()));
        }
        let hash = ObjectHash::from_str(blob_hash).map_err(ProtocolError::repository_error)?;
        Blob::from_bytes(&output.stdout, hash)
            .map_err(|e| ProtocolError::repository_error(e.to_string()))
    }

    /// Handle unpacking of received pack objects.
    async fn handle_pack_objects(
        &self,
        commits: Vec<Commit>,
        trees: Vec<Tree>,
        blobs: Vec<Blob>,
    ) -> Result<(), ProtocolError> {
        // Store unpacked objects as loose objects (enough for this example server).
        for blob in blobs {
            let data = blob
                .to_data()
                .map_err(|e| ProtocolError::repository_error(format!("serialize blob: {e}")))?;
            self.write_loose_object(ObjectType::Blob, &data)?;
        }
        for tree in trees {
            let data = tree
                .to_data()
                .map_err(|e| ProtocolError::repository_error(format!("serialize tree: {e}")))?;
            self.write_loose_object(ObjectType::Tree, &data)?;
        }
        for commit in commits {
            let data = commit
                .to_data()
                .map_err(|e| ProtocolError::repository_error(format!("serialize commit: {e}")))?;
            self.write_loose_object(ObjectType::Commit, &data)?;
        }
        Ok(())
    }
}

/// Authentication Service that allows all requests.
#[derive(Clone)]
struct AllowAllAuth;

#[async_trait]
impl AuthenticationService for AllowAllAuth {
    async fn authenticate_http(
        &self,
        _headers: &HashMap<String, String>,
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

/// GitHTTP Handlers
#[derive(Clone)]
struct AppState {
    repo_root: PathBuf,
    auth: AllowAllAuth,
}

#[tokio::main]
async fn main() {
    // Use GIT_REPO_ROOT to locate repositories on disk.
    let repo_root = std::env::var("GIT_REPO_ROOT").unwrap_or_else(|_| "./repos".to_string());

    let state = AppState {
        repo_root: PathBuf::from(repo_root),
        auth: AllowAllAuth,
    };

    // Routes match Git smart HTTP endpoints.
    let app = Router::new()
        .route("/{repo}/info/refs", get(info_refs))
        .route("/{repo}/git-upload-pack", post(upload_pack))
        .route("/{repo}/git-receive-pack", post(receive_pack))
        .with_state(Arc::new(state));

    let addr = "0.0.0.0:3000";
    println!("HTTP Git server on http://{addr}");
    println!(
        "Repo root: {}",
        std::env::var("GIT_REPO_ROOT").unwrap_or_else(|_| "./repos".into())
    );

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn info_refs(
    State(state): State<Arc<AppState>>,
    Path(repo_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    // info/refs advertises refs + capabilities for the requested service.
    let Some(service) = params.get("service") else {
        return (StatusCode::BAD_REQUEST, "missing service").into_response();
    };

    let git_dir = match resolve_repo_path(&state.repo_root, &repo_name) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let repo = FsRepository::new(git_dir);

    // Configure hash kind before any object operations
    if let Err(e) = repo.detect_and_configure_hash_kind().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to detect hash kind: {}", e),
        )
            .into_response();
    }

    let mut handler = HttpGitHandler::new(repo, state.auth.clone());

    let request_path = format!("/{}/info/refs", repo_name);
    let query = format!("service={}", service);

    if let Err(e) = handler.authenticate_http(&headers_to_map(&headers)).await {
        return (StatusCode::UNAUTHORIZED, e.to_string()).into_response();
    }

    match handler.handle_info_refs(&request_path, &query).await {
        Ok((data, content_type)) => {
            ([(axum::http::header::CONTENT_TYPE, content_type)], data).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn upload_pack(
    State(state): State<Arc<AppState>>,
    Path(repo_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let git_dir = match resolve_repo_path(&state.repo_root, &repo_name) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let repo = FsRepository::new(git_dir);

    // Configure hash kind before any object operations
    if let Err(e) = repo.detect_and_configure_hash_kind().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to detect hash kind: {}", e),
        )
            .into_response();
    }

    let mut handler = HttpGitHandler::new(repo, state.auth.clone());
    let request_path = format!("/{}/git-upload-pack", repo_name);

    if let Err(e) = handler.authenticate_http(&headers_to_map(&headers)).await {
        return (StatusCode::UNAUTHORIZED, e.to_string()).into_response();
    }

    // upload-pack returns a stream (pack data) so we stream it back to the client.
    match handler.handle_upload_pack(&request_path, &body).await {
        Ok((stream, content_type)) => {
            let body = Body::from_stream(stream);
            ([(axum::http::header::CONTENT_TYPE, content_type)], body).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn receive_pack(
    State(state): State<Arc<AppState>>,
    Path(repo_name): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let git_dir = match resolve_repo_path(&state.repo_root, &repo_name) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let repo = FsRepository::new(git_dir);

    // Configure hash kind before any object operations
    if let Err(e) = repo.detect_and_configure_hash_kind().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to detect hash kind: {}", e),
        )
            .into_response();
    }

    let mut handler = HttpGitHandler::new(repo, state.auth.clone());
    let request_path = format!("/{}/git-receive-pack", repo_name);

    if let Err(e) = handler.authenticate_http(&headers_to_map(&headers)).await {
        return (StatusCode::UNAUTHORIZED, e.to_string()).into_response();
    }

    // Convert Axum body into ProtocolStream for git-internal.
    let stream: ProtocolStream = Box::pin(
        body.into_data_stream()
            .map(|r| r.map_err(|e| ProtocolError::Io(std::io::Error::other(e)))),
    );

    match handler.handle_receive_pack(&request_path, stream).await {
        Ok((stream, content_type)) => {
            let body = Body::from_stream(stream);
            ([(axum::http::header::CONTENT_TYPE, content_type)], body).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn resolve_repo_path(repo_root: &StdPath, repo: &str) -> Result<PathBuf, Box<Response>> {
    // Reject traversal and map repo name to bare or non-bare layout.
    if repo.is_empty() || repo.contains("..") || repo.contains('\\') || repo.contains('/') {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "invalid repo").into_response(),
        ));
    }

    let direct = repo_root.join(repo);
    let bare = repo_root.join(format!("{repo}.git"));
    let non_bare = repo_root.join(repo).join(".git");

    if direct.is_dir() && direct.join("objects").exists() {
        return Ok(direct);
    }
    if bare.is_dir() && bare.join("objects").exists() {
        return Ok(bare);
    }
    if non_bare.is_dir() && non_bare.join("objects").exists() {
        return Ok(non_bare);
    }

    Err(Box::new(
        (StatusCode::NOT_FOUND, "repo not found").into_response(),
    ))
}

fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect()
}
