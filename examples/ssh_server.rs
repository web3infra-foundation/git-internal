//! SSH exec handler for git-upload-pack / git-receive-pack.
//!
//! This example uses git-internal's `SshGitHandler` and the same `FsRepository`
//! as the HTTP example. It relies on an external SSH transport (OpenSSH or a
//! local wrapper) to provide stdin/stdout.
//!
//! Quick test without real SSHD (two terminals):
//!
//! Server side:
//! ```bash
//! cargo build --example ssh_server --manifest-path git-internal/Cargo.toml
//! rm -rf /tmp/git-ssh-demo
//! mkdir -p /tmp/git-ssh-demo
//! git init --bare /tmp/git-ssh-demo/demo.git
//! cat > /tmp/git-ssh-wrapper <<'EOF'
//! #!/bin/sh
//! shift
//! export SSH_ORIGINAL_COMMAND="$*"
//! export GIT_REPO_ROOT=/tmp/git-ssh-demo
//! exec /path/to/git-internal/target/debug/examples/ssh_server
//! EOF
//! chmod +x /tmp/git-ssh-wrapper
//! ```
//! - Builds the example binary, prepares a bare repo, and creates a wrapper
//!   that simulates an SSH server.
//!
//! Client side (push + clone):
//! ```bash
//! rm -rf /tmp/ssh-src
//! mkdir -p /tmp/ssh-src && cd /tmp/ssh-src
//! git init
//! git config user.name demo
//! git config user.email demo@example.com
//! echo hello > README.md
//! git add README.md
//! git commit -m "init"
//! git remote add origin ssh://dummy@dummy/demo.git
//! GIT_SSH_COMMAND=/tmp/git-ssh-wrapper git push -u origin main
//! rm -rf /tmp/ssh-clone
//! GIT_SSH_COMMAND=/tmp/git-ssh-wrapper git clone ssh://dummy@dummy/demo.git /tmp/ssh-clone
//! ```
//! - `GIT_SSH_COMMAND` points Git at the wrapper instead of a real ssh binary.
//! - `main` can be replaced with `master` depending on your default branch.
//!
//! OpenSSH usage (real server):
//! - Install the binary on the server and wire it in `~/.ssh/authorized_keys`:
//!   `command="/path/to/ssh_server" ssh-ed25519 AAAA...`
//! - Then clients can run: `git clone ssh://user@host/demo.git`.
//!
//! SHA-256 repository test:
//! ```bash
//! rm -rf /tmp/git-ssh-sha256
//! mkdir -p /tmp/git-ssh-sha256
//! git init --bare --object-format=sha256 /tmp/git-ssh-sha256/demo-sha256.git
//! # Update GIT_REPO_ROOT in /tmp/git-ssh-wrapper to /tmp/git-ssh-sha256
//! # Then push/clone from a SHA-256 client repository
//! ```
//! The server automatically detects each repository's object format by reading
//! `extensions.objectformat` from the repository config before handling requests.

use std::{
    collections::HashMap,
    io::Write,
    path::{Component, Path as StdPath, PathBuf},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
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
        ssh::{SshGitHandler, parse_ssh_command},
        types::{ProtocolError, ProtocolStream},
        utils::{read_pkt_line, read_until_white_space},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tokio_util::io::ReaderStream;

/// Repo implementation (same as HTTP example)
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

    /// Detect the repository's hash algorithm and configure the thread-local hash kind.
    /// Reads `extensions.objectformat` from the repository config.
    /// If not set, defaults to SHA-1 for backward compatibility.
    async fn detect_and_configure_hash_kind(&self) -> Result<(), ProtocolError> {
        let output = self
            .run_git(["config", "--get", "extensions.objectformat"])
            .await?;

        let hash_kind = if output.status.success() {
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

        set_hash_kind(hash_kind);
        Ok(())
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

    async fn get_object(&self, object_hash: &str) -> Result<Vec<u8>, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", object_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(object_hash.to_string()));
        }
        Ok(output.stdout)
    }

    async fn store_pack_data(&self, _pack_data: &[u8]) -> Result<(), ProtocolError> {
        Ok(())
    }

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

    async fn get_objects_for_pack(
        &self,
        _wants: &[String],
        _haves: &[String],
    ) -> Result<Vec<String>, ProtocolError> {
        Ok(Vec::new())
    }

    async fn has_default_branch(&self) -> Result<bool, ProtocolError> {
        let refs = self.get_repository_refs().await?;
        Ok(refs
            .iter()
            .any(|(name, _)| name == "refs/heads/main" || name == "refs/heads/master"))
    }

    async fn post_receive_hook(&self) -> Result<(), ProtocolError> {
        Ok(())
    }

    async fn get_commit(&self, commit_hash: &str) -> Result<Commit, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", commit_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(commit_hash.to_string()));
        }
        let hash = ObjectHash::from_str(commit_hash).map_err(ProtocolError::repository_error)?;
        Commit::from_bytes(&output.stdout, hash)
            .map_err(|e| ProtocolError::repository_error(e.to_string()))
    }

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

    async fn get_blob(&self, blob_hash: &str) -> Result<Blob, ProtocolError> {
        let output = self.run_git(["cat-file", "-p", blob_hash]).await?;
        if !output.status.success() {
            return Err(ProtocolError::ObjectNotFound(blob_hash.to_string()));
        }
        let hash = ObjectHash::from_str(blob_hash).map_err(ProtocolError::repository_error)?;
        Blob::from_bytes(&output.stdout, hash)
            .map_err(|e| ProtocolError::repository_error(e.to_string()))
    }

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

/// Auth
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

/// SSH Exec Handler
#[tokio::main]
async fn main() {
    // Repo root is the base directory for served repositories.
    let repo_root = std::env::var("GIT_REPO_ROOT").unwrap_or_else(|_| "./repos".to_string());
    // SSH daemon passes the requested command here.
    let command_line = std::env::var("SSH_ORIGINAL_COMMAND")
        .ok()
        .or_else(|| {
            let args = std::env::args().skip(1).collect::<Vec<_>>();
            if args.is_empty() {
                None
            } else {
                Some(args.join(" "))
            }
        })
        .unwrap_or_else(|| {
            eprintln!("missing SSH_ORIGINAL_COMMAND");
            std::process::exit(1);
        });

    let Some((command, args)) = parse_ssh_command(&command_line) else {
        eprintln!("invalid command");
        std::process::exit(1);
    };

    // Extract repo argument (first non-flag token).
    let repo_arg = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(|s| s.trim_matches('\'').trim_matches('"').to_string())
        .unwrap_or_else(|| {
            eprintln!("missing repo argument");
            std::process::exit(1);
        });

    let git_dir = match resolve_repo_path(&PathBuf::from(repo_root), &repo_arg) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
    };

    let repo = FsRepository::new(git_dir);

    // Configure hash kind before any object operations
    if let Err(e) = repo.detect_and_configure_hash_kind().await {
        eprintln!("Failed to detect hash kind: {e}");
        std::process::exit(1);
    }

    let auth = AllowAllAuth;
    let mut handler = SshGitHandler::new(repo, auth);

    let username = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    if let Err(e) = handler.authenticate_ssh(&username, &[]).await {
        eprintln!("auth failed: {e}");
        std::process::exit(1);
    }

    let mut stdout = tokio::io::stdout();
    let service = match command.as_str() {
        "git-upload-pack" => "git-upload-pack",
        "git-receive-pack" => "git-receive-pack",
        _ => {
            eprintln!("unsupported command: {command}");
            std::process::exit(1);
        }
    };

    // SSH protocol starts by advertising refs/capabilities.
    let info_refs = handler.handle_info_refs(service).await.unwrap();
    stdout.write_all(&info_refs).await.unwrap();
    stdout.flush().await.unwrap();

    let output: ProtocolStream = match command.as_str() {
        "git-upload-pack" => {
            // Read request until "done" or flush to avoid blocking.
            let request = read_upload_pack_request().await.unwrap();
            handler.handle_upload_pack(&request).await.unwrap()
        }
        "git-receive-pack" => {
            // Stream pack data directly from stdin.
            let stream = ReaderStream::new(tokio::io::stdin())
                .map(|result| result.map_err(ProtocolError::Io));
            let stream: ProtocolStream = Box::pin(stream);
            handler.handle_receive_pack(stream).await.unwrap()
        }
        _ => unreachable!("unsupported command: {command}"),
    };

    let mut stream = output;
    while let Some(chunk) = stream.next().await {
        let data = chunk.unwrap();
        stdout.write_all(&data).await.unwrap();
    }
    if let Err(err) = stdout.flush().await {
        eprintln!("failed to flush stdout to client: {err}");
        std::process::exit(1);
    }
}

fn upload_pack_request_complete(buf: &BytesMut) -> bool {
    // Scan pkt-lines; request ends at flush or "done".
    let mut view = buf.clone().freeze();
    loop {
        let (consumed, pkt_line) = read_pkt_line(&mut view);
        if consumed == 0 {
            return false;
        }
        if pkt_line.is_empty() {
            return true;
        }
        let mut pkt_line = pkt_line;
        let command = read_until_white_space(&mut pkt_line);
        if command == "done" {
            return true;
        }
    }
}

async fn read_upload_pack_request() -> Result<Bytes, ProtocolError> {
    // Read stdin until the upload-pack request is complete.
    let mut stdin = tokio::io::stdin();
    let mut buf = BytesMut::with_capacity(8192);
    let mut temp = [0u8; 8192];

    loop {
        let n = stdin.read(&mut temp).await.map_err(ProtocolError::Io)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&temp[..n]);
        if upload_pack_request_complete(&buf) {
            break;
        }
    }

    Ok(buf.freeze())
}

fn resolve_repo_path(repo_root: &StdPath, repo: &str) -> Result<PathBuf, String> {
    // Reject traversal and map repo name to bare or non-bare layout.
    let repo = repo.trim_start_matches('/');
    if repo.is_empty() || repo.contains('\\') {
        return Err("invalid repo".to_string());
    }

    let repo_path = StdPath::new(repo);
    if repo_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err("invalid repo".to_string());
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

    Err("repo not found".to_string())
}
