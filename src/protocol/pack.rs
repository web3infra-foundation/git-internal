use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use std::io::Cursor;
use tokio;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::core::RepositoryAccess;
use super::types::ProtocolError;
use crate::internal::object::types::ObjectType;
use crate::internal::object::{ObjectTrait, blob::Blob, commit::Commit, tree::Tree};
use crate::internal::pack::{Pack, encode::PackEncoder, entry::Entry};

/// Pack generation service for Git protocol operations
///
/// This handles the core Git pack generation logic internally within git-internal,
/// using the RepositoryAccess trait only for data access.
pub struct PackGenerator<'a, R>
where
    R: RepositoryAccess,
{
    repo_access: &'a R,
    repo_path: &'a str,
}

impl<'a, R> PackGenerator<'a, R>
where
    R: RepositoryAccess,
{
    pub fn new(repo_access: &'a R, repo_path: &'a str) -> Self {
        Self {
            repo_access,
            repo_path,
        }
    }

    /// Generate a full pack containing all requested objects
    pub async fn generate_full_pack(
        &self,
        want: Vec<String>,
    ) -> Result<ReceiverStream<Vec<u8>>, ProtocolError> {
        let (tx, rx) = mpsc::channel(1024);

        // Collect all objects needed for the wanted commits
        let all_objects = self.collect_all_objects(want).await?;

        // Generate pack data
        tokio::spawn(async move {
            if let Err(e) = Self::generate_pack_stream(all_objects, tx).await {
                tracing::error!("Failed to generate pack stream: {}", e);
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Generate an incremental pack containing only objects not in 'have'
    pub async fn generate_incremental_pack(
        &self,
        want: Vec<String>,
        have: Vec<String>,
    ) -> Result<ReceiverStream<Vec<u8>>, ProtocolError> {
        let (tx, rx) = mpsc::channel(1024);

        // Collect objects for wanted commits
        let wanted_objects = self.collect_all_objects(want).await?;

        // Collect objects for have commits (to exclude)
        let have_objects = self.collect_all_objects(have).await?;

        // Filter out objects that are already in 'have'
        let incremental_objects = Self::filter_objects(wanted_objects, have_objects);

        // Generate pack data
        tokio::spawn(async move {
            if let Err(e) = Self::generate_pack_stream(incremental_objects, tx).await {
                tracing::error!("Failed to generate incremental pack stream: {}", e);
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Unpack incoming pack stream and extract objects
    pub async fn unpack_stream(
        &self,
        pack_data: Bytes,
    ) -> Result<(Vec<Commit>, Vec<Tree>, Vec<Blob>), ProtocolError> {
        use std::sync::{Arc, Mutex};

        let commits = Arc::new(Mutex::new(Vec::new()));
        let trees = Arc::new(Mutex::new(Vec::new()));
        let blobs = Arc::new(Mutex::new(Vec::new()));

        let commits_clone = commits.clone();
        let trees_clone = trees.clone();
        let blobs_clone = blobs.clone();

        // Create a Pack instance for decoding
        let mut pack = Pack::new(None, None, None, true);
        let mut cursor = Cursor::new(pack_data.to_vec());

        // Decode the pack and collect entries
        pack.decode(
            &mut cursor,
            move |entry: Entry, _offset: usize| match entry.obj_type {
                ObjectType::Commit => {
                    if let Ok(commit) = Commit::from_bytes(&entry.data, entry.hash) {
                        commits_clone.lock().unwrap().push(commit);
                    } else {
                        tracing::warn!("Failed to parse commit from pack entry");
                    }
                }
                ObjectType::Tree => {
                    if let Ok(tree) = Tree::from_bytes(&entry.data, entry.hash) {
                        trees_clone.lock().unwrap().push(tree);
                    } else {
                        tracing::warn!("Failed to parse tree from pack entry");
                    }
                }
                ObjectType::Blob => {
                    if let Ok(blob) = Blob::from_bytes(&entry.data, entry.hash) {
                        blobs_clone.lock().unwrap().push(blob);
                    } else {
                        tracing::warn!("Failed to parse blob from pack entry");
                    }
                }
                _ => {
                    tracing::warn!("Unknown object type in pack: {:?}", entry.obj_type);
                }
            },
        )
        .map_err(|e| ProtocolError::invalid_request(&format!("Failed to decode pack: {}", e)))?;

        // Extract the results
        let commits_result = Arc::try_unwrap(commits).unwrap().into_inner().unwrap();
        let trees_result = Arc::try_unwrap(trees).unwrap().into_inner().unwrap();
        let blobs_result = Arc::try_unwrap(blobs).unwrap().into_inner().unwrap();

        Ok((commits_result, trees_result, blobs_result))
    }

    /// Collect all objects reachable from the given commit hashes
    async fn collect_all_objects(
        &self,
        commit_hashes: Vec<String>,
    ) -> Result<(Vec<Commit>, Vec<Tree>, Vec<Blob>), ProtocolError> {
        let mut commits = Vec::new();
        let mut trees = Vec::new();
        let mut blobs = Vec::new();

        let mut visited_commits = HashSet::new();
        let mut visited_trees = HashSet::new();
        let mut visited_blobs = HashSet::new();

        let mut commit_queue = VecDeque::from(commit_hashes);

        // BFS traversal of commit graph
        while let Some(commit_hash) = commit_queue.pop_front() {
            if visited_commits.contains(&commit_hash) {
                continue;
            }
            visited_commits.insert(commit_hash.clone());

            // Get commit object
            let commit = self
                .repo_access
                .get_commit(self.repo_path, &commit_hash)
                .await
                .map_err(|e| {
                    ProtocolError::repository_error(format!(
                        "Failed to get commit {}: {}",
                        commit_hash, e
                    ))
                })?;

            // Add parent commits to queue
            for parent in &commit.parent_commit_ids {
                let parent_str = parent.to_string();
                if !visited_commits.contains(&parent_str) {
                    commit_queue.push_back(parent_str);
                }
            }

            // Collect tree objects
            Box::pin(self.collect_tree_objects(
                &commit.tree_id.to_string(),
                &mut trees,
                &mut blobs,
                &mut visited_trees,
                &mut visited_blobs,
            ))
            .await?;

            commits.push(commit);
        }

        Ok((commits, trees, blobs))
    }

    /// Recursively collect tree and blob objects
    async fn collect_tree_objects(
        &self,
        tree_hash: &str,
        trees: &mut Vec<Tree>,
        blobs: &mut Vec<Blob>,
        visited_trees: &mut HashSet<String>,
        visited_blobs: &mut HashSet<String>,
    ) -> Result<(), ProtocolError> {
        if visited_trees.contains(tree_hash) {
            return Ok(());
        }
        visited_trees.insert(tree_hash.to_string());

        let tree = self
            .repo_access
            .get_tree(self.repo_path, tree_hash)
            .await
            .map_err(|e| {
                ProtocolError::repository_error(format!("Failed to get tree {}: {}", tree_hash, e))
            })?;

        for entry in &tree.tree_items {
            let entry_hash = entry.id.to_string();
            match entry.mode {
                crate::internal::object::tree::TreeItemMode::Tree => {
                    Box::pin(self.collect_tree_objects(
                        &entry_hash,
                        trees,
                        blobs,
                        visited_trees,
                        visited_blobs,
                    ))
                    .await?;
                }
                crate::internal::object::tree::TreeItemMode::Blob
                | crate::internal::object::tree::TreeItemMode::BlobExecutable => {
                    if !visited_blobs.contains(&entry_hash) {
                        visited_blobs.insert(entry_hash.clone());
                        let blob = self
                            .repo_access
                            .get_blob(self.repo_path, &entry_hash)
                            .await
                            .map_err(|e| {
                                ProtocolError::repository_error(format!(
                                    "Failed to get blob {}: {}",
                                    entry_hash, e
                                ))
                            })?;
                        blobs.push(blob);
                    }
                }
                _ => {}
            }
        }

        trees.push(tree);
        Ok(())
    }

    /// Filter objects to exclude those already in 'have'
    fn filter_objects(
        wanted: (Vec<Commit>, Vec<Tree>, Vec<Blob>),
        have: (Vec<Commit>, Vec<Tree>, Vec<Blob>),
    ) -> (Vec<Commit>, Vec<Tree>, Vec<Blob>) {
        let (wanted_commits, wanted_trees, wanted_blobs) = wanted;
        let (have_commits, have_trees, have_blobs) = have;

        // Create hash sets for efficient lookup
        let have_commit_hashes: HashSet<String> =
            have_commits.iter().map(|c| c.id.to_string()).collect();
        let have_tree_hashes: HashSet<String> =
            have_trees.iter().map(|t| t.id.to_string()).collect();
        let have_blob_hashes: HashSet<String> =
            have_blobs.iter().map(|b| b.id.to_string()).collect();

        // Filter out objects that are in 'have'
        let filtered_commits: Vec<Commit> = wanted_commits
            .into_iter()
            .filter(|c| !have_commit_hashes.contains(&c.id.to_string()))
            .collect();

        let filtered_trees: Vec<Tree> = wanted_trees
            .into_iter()
            .filter(|t| !have_tree_hashes.contains(&t.id.to_string()))
            .collect();

        let filtered_blobs: Vec<Blob> = wanted_blobs
            .into_iter()
            .filter(|b| !have_blob_hashes.contains(&b.id.to_string()))
            .collect();

        (filtered_commits, filtered_trees, filtered_blobs)
    }

    /// Generate pack stream from objects
    async fn generate_pack_stream(
        objects: (Vec<Commit>, Vec<Tree>, Vec<Blob>),
        tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<(), ProtocolError> {
        let (commits, trees, blobs) = objects;

        // Convert objects to entries
        let mut entries = Vec::new();

        for commit in commits {
            entries.push(Entry::from(commit));
        }

        for tree in trees {
            entries.push(Entry::from(tree));
        }

        for blob in blobs {
            entries.push(Entry::from(blob));
        }

        // Create PackEncoder and encode entries
        let (pack_tx, mut pack_rx) = mpsc::channel(1024);
        let (entry_tx, entry_rx) = mpsc::channel(1024);
        let mut encoder = PackEncoder::new(entries.len(), 10, pack_tx); // window_size = 10

        // Spawn encoding task
        tokio::spawn(async move {
            if let Err(e) = encoder.encode(entry_rx).await {
                tracing::error!("Failed to encode pack: {}", e);
            }
        });

        // Send entries to encoder
        tokio::spawn(async move {
            for entry in entries {
                if entry_tx.send(entry).await.is_err() {
                    break; // Receiver dropped
                }
            }
            // Drop sender to signal end of entries
        });

        // Forward pack data to output channel
        while let Some(chunk) = pack_rx.recv().await {
            if tx.send(chunk).await.is_err() {
                break; // Receiver dropped
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::object::blob::Blob;
    use crate::internal::object::commit::Commit;
    use crate::internal::object::signature::{Signature, SignatureType};
    use crate::internal::object::tree::{Tree, TreeItem, TreeItemMode};
    use async_trait::async_trait;
    use bytes::Bytes;

    #[derive(Clone)]
    struct DummyRepoAccess;

    #[async_trait]
    impl RepositoryAccess for DummyRepoAccess {
        async fn get_repository_refs(
            &self,
            _repo_path: &str,
        ) -> Result<Vec<(String, String)>, ProtocolError> {
            Ok(vec![])
        }
        async fn has_object(
            &self,
            _repo_path: &str,
            _object_hash: &str,
        ) -> Result<bool, ProtocolError> {
            Ok(false)
        }
        async fn get_object(
            &self,
            _repo_path: &str,
            _object_hash: &str,
        ) -> Result<Vec<u8>, ProtocolError> {
            Err(ProtocolError::repository_error(
                "not implemented".to_string(),
            ))
        }
        async fn store_pack_data(
            &self,
            _repo_path: &str,
            _pack_data: &[u8],
        ) -> Result<(), ProtocolError> {
            Ok(())
        }
        async fn update_reference(
            &self,
            _repo_path: &str,
            _ref_name: &str,
            _old_hash: Option<&str>,
            _new_hash: &str,
        ) -> Result<(), ProtocolError> {
            Ok(())
        }
        async fn get_objects_for_pack(
            &self,
            _repo_path: &str,
            _wants: &[String],
            _haves: &[String],
        ) -> Result<Vec<String>, ProtocolError> {
            Ok(vec![])
        }
        async fn has_default_branch(&self, _repo_path: &str) -> Result<bool, ProtocolError> {
            Ok(false)
        }
        async fn post_receive_hook(&self, _repo_path: &str) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_pack_roundtrip_encode_decode() {
        // 构造两个 Blob
        let blob1 = Blob::from_content("hello");
        let blob2 = Blob::from_content("world");

        // 构造一个 Tree，包含两个文件项
        let item1 = TreeItem::new(TreeItemMode::Blob, blob1.id, "hello.txt".to_string());
        let item2 = TreeItem::new(TreeItemMode::Blob, blob2.id, "world.txt".to_string());
        let tree = Tree::from_tree_items(vec![item1, item2]).unwrap();

        // 构造一个 Commit 指向该 Tree
        let author = Signature::new(
            SignatureType::Author,
            "tester".to_string(),
            "tester@example.com".to_string(),
        );
        let committer = Signature::new(
            SignatureType::Committer,
            "tester".to_string(),
            "tester@example.com".to_string(),
        );
        let commit = Commit::new(author, committer, tree.id, vec![], "init commit");

        // 生成 pack 流
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
        PackGenerator::<DummyRepoAccess>::generate_pack_stream(
            (
                vec![commit.clone()],
                vec![tree.clone()],
                vec![blob1.clone(), blob2.clone()],
            ),
            tx,
        )
        .await
        .unwrap();

        let mut pack_bytes: Vec<u8> = Vec::new();
        while let Some(chunk) = rx.recv().await {
            pack_bytes.extend_from_slice(&chunk);
        }
        println!("Encoded pack size: {} bytes", pack_bytes.len());

        // 解包 pack 流
        let dummy = DummyRepoAccess;
        let generator = PackGenerator::new(&dummy, "test-repo-path");
        let (decoded_commits, decoded_trees, decoded_blobs) = generator
            .unpack_stream(Bytes::from(pack_bytes))
            .await
            .unwrap();

        println!(
            "Decoded commits: {:?}",
            decoded_commits
                .iter()
                .map(|c| c.id.to_string())
                .collect::<Vec<_>>()
        );
        println!(
            "Decoded trees:   {:?}",
            decoded_trees
                .iter()
                .map(|t| t.id.to_string())
                .collect::<Vec<_>>()
        );
        println!(
            "Decoded blobs:   {:?}",
            decoded_blobs
                .iter()
                .map(|b| b.id.to_string())
                .collect::<Vec<_>>()
        );

        // 验证对象 ID 往返一致
        assert_eq!(decoded_commits.len(), 1);
        assert_eq!(decoded_trees.len(), 1);
        assert_eq!(decoded_blobs.len(), 2);

        assert_eq!(decoded_commits[0].id, commit.id);
        assert_eq!(decoded_trees[0].id, tree.id);

        let mut orig_blob_ids = vec![blob1.id.to_string(), blob2.id.to_string()];
        orig_blob_ids.sort();
        let mut decoded_blob_ids = decoded_blobs
            .iter()
            .map(|b| b.id.to_string())
            .collect::<Vec<_>>();
        decoded_blob_ids.sort();
        assert_eq!(orig_blob_ids, decoded_blob_ids);
    }
}
