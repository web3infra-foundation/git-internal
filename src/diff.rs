//! Unified diff generation utilities that compare blobs/trees, map deltas back to line numbers,
//! and emit Myers-based unified diffs for Git objects while guarding against pathological inputs.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Write,
    path::{Path, PathBuf},
};

use path_absolutize::Absolutize;
use similar::{Algorithm, ChangeTag, TextDiff};

use crate::hash::ObjectHash;

/// Result item for a single file diff:
/// - `path`: logical file path
/// - `data`: unified diff text or a large-file marker
#[derive(Debug, Clone)]
pub struct DiffItem {
    /// The file path being diffed.
    pub path: String,
    /// The complete unified diff output string for that file, or a large-file marker if the file is too large to diff.
    pub data: String,
}

/// Unified diff generator and helpers.
pub struct Diff;

/// Diff line operation types primarily used by blame computation to map parent/child lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffOperation {
    Insert { line: usize, content: String },
    Delete { line: usize },
    Equal { old_line: usize, new_line: usize },
}

/// Internal representation of diff lines used while assembling unified hunks.
#[derive(Debug, Clone, Copy)]
enum EditLine<'a> {
    // old_line, new_line, text
    Context(Option<usize>, Option<usize>, &'a str),
    // old_line, text
    Delete(usize, &'a str),
    // new_line, text
    Insert(usize, &'a str),
}

impl Diff {
    /// Compute Myers line-level operations (equal/insert/delete) for blame/line mapping.
    fn compute_line_operations(old_lines: &[String], new_lines: &[String]) -> Vec<DiffOperation> {
        if old_lines.is_empty() && new_lines.is_empty() {
            return Vec::new();
        }

        let old_refs: Vec<&str> = old_lines.iter().map(|s| s.as_str()).collect();
        let new_refs: Vec<&str> = new_lines.iter().map(|s| s.as_str()).collect();

        let diff = TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_slices(&old_refs, &new_refs);

        let mut operations = Vec::with_capacity(old_lines.len() + new_lines.len());
        let mut old_line_no = 1usize;
        let mut new_line_no = 1usize;

        for change in diff.iter_all_changes() {
            match change.tag() {
                ChangeTag::Equal => {
                    operations.push(DiffOperation::Equal {
                        old_line: old_line_no,
                        new_line: new_line_no,
                    });
                    old_line_no += 1;
                    new_line_no += 1;
                }
                ChangeTag::Delete => {
                    operations.push(DiffOperation::Delete { line: old_line_no });
                    old_line_no += 1;
                }
                ChangeTag::Insert => {
                    operations.push(DiffOperation::Insert {
                        line: new_line_no,
                        content: change.value().to_string(),
                    });
                    new_line_no += 1;
                }
            }
        }

        operations
    }

    const MAX_DIFF_LINES: usize = 10_000; // safety cap for pathological inputs
    const LARGE_FILE_MARKER: &'static str = "<LargeFile>";
    const LARGE_FILE_END: &'static str = "</LargeFile>";
    const SHORT_HASH_LEN: usize = 7;

    /// Compute diffs for a set of files, honoring an optional filter and emitting unified diffs.
    pub fn diff<F>(
        old_blobs: Vec<(PathBuf, ObjectHash)>,
        new_blobs: Vec<(PathBuf, ObjectHash)>,
        filter: Vec<PathBuf>,
        read_content: F,
    ) -> Vec<DiffItem>
    where
        F: Fn(&PathBuf, &ObjectHash) -> Vec<u8>,
    {
        let (processed_files, old_blobs_map, new_blobs_map) =
            Self::prepare_diff_data(old_blobs, new_blobs, &filter);

        let mut diff_results: Vec<DiffItem> = Vec::with_capacity(processed_files.len());
        for file in processed_files {
            // Read bytes once per file to avoid duplicate IO and conversions.
            let old_hash = old_blobs_map.get(&file);
            let new_hash = new_blobs_map.get(&file);
            let old_bytes = old_hash.map_or_else(Vec::new, |h| read_content(&file, h));
            let new_bytes = new_hash.map_or_else(Vec::new, |h| read_content(&file, h));

            if let Some(large_file_marker) =
                Self::is_large_file_bytes(&file, &old_bytes, &new_bytes)
            {
                diff_results.push(DiffItem {
                    path: file.to_string_lossy().to_string(),
                    data: large_file_marker,
                });
            } else {
                let diff = Self::diff_for_file_preloaded(
                    &file, old_hash, new_hash, &old_bytes, &new_bytes,
                );
                diff_results.push(DiffItem {
                    path: file.to_string_lossy().to_string(),
                    data: diff,
                });
            }
        }

        diff_results
    }

    /// Large-file detection without re-reading: counts lines from already-loaded bytes.
    fn is_large_file_bytes(file: &Path, old_bytes: &[u8], new_bytes: &[u8]) -> Option<String> {
        let old_lines = String::from_utf8_lossy(old_bytes).lines().count();
        let new_lines = String::from_utf8_lossy(new_bytes).lines().count();
        let total_lines = old_lines + new_lines;
        if total_lines > Self::MAX_DIFF_LINES {
            Some(format!(
                "{}{}:{}:{}{}\n",
                Self::LARGE_FILE_MARKER,
                file.display(),
                total_lines,
                Self::MAX_DIFF_LINES,
                Self::LARGE_FILE_END
            ))
        } else {
            None
        }
    }

    /// Build maps, union file set, and apply filter/path checks.
    fn prepare_diff_data(
        old_blobs: Vec<(PathBuf, ObjectHash)>,
        new_blobs: Vec<(PathBuf, ObjectHash)>,
        filter: &[PathBuf],
    ) -> (
        Vec<PathBuf>,
        HashMap<PathBuf, ObjectHash>,
        HashMap<PathBuf, ObjectHash>,
    ) {
        let old_blobs_map: HashMap<PathBuf, ObjectHash> = old_blobs.into_iter().collect();
        let new_blobs_map: HashMap<PathBuf, ObjectHash> = new_blobs.into_iter().collect();
        // union set
        let union_files: HashSet<PathBuf> = old_blobs_map
            .keys()
            .chain(new_blobs_map.keys())
            .cloned()
            .collect();

        // filter files that should be processed
        let processed_files: Vec<PathBuf> = union_files
            .into_iter()
            .filter(|file| Self::should_process(file, filter, &old_blobs_map, &new_blobs_map))
            .collect();

        (processed_files, old_blobs_map, new_blobs_map)
    }

    /// Filter by path and hash equality; only process differing or unmatched files.
    fn should_process(
        file: &PathBuf,
        filter: &[PathBuf],
        old_blobs: &HashMap<PathBuf, ObjectHash>,
        new_blobs: &HashMap<PathBuf, ObjectHash>,
    ) -> bool {
        if !filter.is_empty()
            && !filter
                .iter()
                .any(|path| Self::sub_of(file, path).unwrap_or(false))
        {
            return false;
        }

        old_blobs.get(file) != new_blobs.get(file)
    }

    /// Check whether `path` is under `parent` (absolutized).
    fn sub_of(path: &PathBuf, parent: &PathBuf) -> Result<bool, std::io::Error> {
        let path_abs: PathBuf = path.absolutize()?.to_path_buf();
        let parent_abs: PathBuf = parent.absolutize()?.to_path_buf();
        Ok(path_abs.starts_with(parent_abs))
    }

    /// Shorten hash to 7 chars for diff headers; return zeros if missing.
    fn short_hash(hash: Option<&ObjectHash>) -> String {
        hash.map(|h| {
            let hex = h.to_string();
            let take = Self::SHORT_HASH_LEN.min(hex.len());
            hex[..take].to_string()
        })
        .unwrap_or_else(|| "0".repeat(Self::SHORT_HASH_LEN))
    }

    /// Format a single file's unified diff string.
    pub fn diff_for_file_string(
        file: &PathBuf,
        old_blobs: &HashMap<PathBuf, ObjectHash>,
        new_blobs: &HashMap<PathBuf, ObjectHash>,
        read_content: &dyn Fn(&PathBuf, &ObjectHash) -> Vec<u8>,
    ) -> String {
        let new_hash = new_blobs.get(file);
        let old_hash = old_blobs.get(file);
        let old_bytes = old_hash.map_or_else(Vec::new, |h| read_content(file, h));
        let new_bytes = new_hash.map_or_else(Vec::new, |h| read_content(file, h));

        Self::diff_for_file_preloaded(file, old_hash, new_hash, &old_bytes, &new_bytes)
    }

    /// Format a single file's unified diff using preloaded bytes to avoid re-reading.
    fn diff_for_file_preloaded(
        file: &Path,
        old_hash: Option<&ObjectHash>,
        new_hash: Option<&ObjectHash>,
        old_bytes: &[u8],
        new_bytes: &[u8],
    ) -> String {
        let mut out = String::new();

        // It's safe to ignore the Result when writing into a String; allocation errors panic elsewhere.
        let _ = writeln!(out, "diff --git a/{} b/{}", file.display(), file.display());

        if old_hash.is_none() {
            let _ = writeln!(out, "new file mode 100644");
        } else if new_hash.is_none() {
            let _ = writeln!(out, "deleted file mode 100644");
        }

        let old_index = Self::short_hash(old_hash);
        let new_index = Self::short_hash(new_hash);
        let _ = writeln!(out, "index {old_index}..{new_index} 100644");

        match (
            std::str::from_utf8(old_bytes),
            std::str::from_utf8(new_bytes),
        ) {
            (Ok(old_text), Ok(new_text)) => {
                let (old_pref, new_pref) = if old_text.is_empty() {
                    ("/dev/null".to_string(), format!("b/{}", file.display()))
                } else if new_text.is_empty() {
                    (format!("a/{}", file.display()), "/dev/null".to_string())
                } else {
                    (
                        format!("a/{}", file.display()),
                        format!("b/{}", file.display()),
                    )
                };

                let _ = writeln!(out, "--- {old_pref}");
                let _ = writeln!(out, "+++ {new_pref}");

                let unified = Self::compute_unified_diff(old_text, new_text, 3);
                out.push_str(&unified);
            }
            _ => {
                let _ = writeln!(out, "Binary files differ");
            }
        }

        out
    }

    /// Streaming unified diff that minimizes allocations by borrowing lines
    fn compute_unified_diff(old_text: &str, new_text: &str, context: usize) -> String {
        // Myers line diff
        let diff = TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_lines(old_text, new_text);

        // Reserve capacity heuristic to reduce allocations
        let mut out = String::with_capacity(((old_text.len() + new_text.len()) / 16).max(4096));

        // Rolling prefix context (last `context` equal lines when outside a hunk)
        let mut prefix_ctx: VecDeque<EditLine> = VecDeque::with_capacity(context);
        let mut cur_hunk: Vec<EditLine> = Vec::new();
        let mut eq_run: Vec<EditLine> = Vec::new(); // accumulating equal lines while in hunk
        let mut in_hunk = false;

        let mut last_old_seen = 0usize;
        let mut last_new_seen = 0usize;
        let mut old_line_no = 1usize;
        let mut new_line_no = 1usize;

        for change in diff.iter_all_changes() {
            let line = change.value().trim_end_matches(['\r', '\n']);
            match change.tag() {
                ChangeTag::Equal => {
                    let entry = EditLine::Context(Some(old_line_no), Some(new_line_no), line);
                    old_line_no += 1;
                    new_line_no += 1;
                    if in_hunk {
                        eq_run.push(entry);
                        // Flush once trailing equal lines exceed 2*context
                        if eq_run.len() > context * 2 {
                            Self::flush_hunk_to_out(
                                &mut out,
                                &mut cur_hunk,
                                &mut eq_run,
                                &mut prefix_ctx,
                                context,
                                &mut last_old_seen,
                                &mut last_new_seen,
                            );
                            in_hunk = false;
                        }
                    } else {
                        if prefix_ctx.len() == context {
                            prefix_ctx.pop_front();
                        }
                        prefix_ctx.push_back(entry);
                    }
                }
                ChangeTag::Delete => {
                    let entry = EditLine::Delete(old_line_no, line);
                    old_line_no += 1;
                    if !in_hunk {
                        cur_hunk.extend(prefix_ctx.iter().copied());
                        prefix_ctx.clear();
                        in_hunk = true;
                    }
                    if !eq_run.is_empty() {
                        cur_hunk.append(&mut eq_run);
                    }
                    cur_hunk.push(entry);
                }
                ChangeTag::Insert => {
                    let entry = EditLine::Insert(new_line_no, line);
                    new_line_no += 1;
                    if !in_hunk {
                        cur_hunk.extend(prefix_ctx.iter().copied());
                        prefix_ctx.clear();
                        in_hunk = true;
                    }
                    if !eq_run.is_empty() {
                        cur_hunk.append(&mut eq_run);
                    }
                    cur_hunk.push(entry);
                }
            }
        }

        if in_hunk {
            Self::flush_hunk_to_out(
                &mut out,
                &mut cur_hunk,
                &mut eq_run,
                &mut prefix_ctx,
                context,
                &mut last_old_seen,
                &mut last_new_seen,
            );
        }

        out
    }

    // Flush the current hunk into the output; trailing context is in `eq_run`
    fn flush_hunk_to_out<'a>(
        out: &mut String,
        cur_hunk: &mut Vec<EditLine<'a>>,
        eq_run: &mut Vec<EditLine<'a>>,
        prefix_ctx: &mut VecDeque<EditLine<'a>>,
        context: usize,
        last_old_seen: &mut usize,
        last_new_seen: &mut usize,
    ) {
        // 1. Append up to `context` trailing equal lines to the current hunk.
        let trail_to_take = eq_run.len().min(context);
        for entry in eq_run.iter().take(trail_to_take) {
            cur_hunk.push(*entry);
        }

        // 2. Compute header numbers (line ranges/counts) by scanning the hunk.
        let mut old_first: Option<usize> = None;
        let mut old_count: usize = 0;
        let mut new_first: Option<usize> = None;
        let mut new_count: usize = 0;

        for e in cur_hunk.iter() {
            match *e {
                EditLine::Context(o, n, _) => {
                    if let Some(o) = o {
                        if old_first.is_none() {
                            old_first = Some(o);
                        }
                        old_count += 1;
                    }
                    if let Some(n) = n {
                        if new_first.is_none() {
                            new_first = Some(n);
                        }
                        new_count += 1;
                    }
                }
                EditLine::Delete(o, _) => {
                    if old_first.is_none() {
                        old_first = Some(o);
                    }
                    old_count += 1;
                }
                EditLine::Insert(n, _) => {
                    if new_first.is_none() {
                        new_first = Some(n);
                    }
                    new_count += 1;
                }
            }
        }

        if old_count == 0 && new_count == 0 {
            cur_hunk.clear();
            eq_run.clear();
            return;
        }

        let old_start = old_first.unwrap_or(*last_old_seen + 1);
        let new_start = new_first.unwrap_or(*last_new_seen + 1);

        let _ = writeln!(
            out,
            "@@ -{old_start},{old_count} +{new_start},{new_count} @@"
        );

        // 3. Output the hunk according to Myers change order
        for &e in cur_hunk.iter() {
            match e {
                EditLine::Context(o, n, txt) => {
                    let _ = writeln!(out, " {txt}");
                    if let Some(o) = o {
                        *last_old_seen = (*last_old_seen).max(o);
                    }
                    if let Some(n) = n {
                        *last_new_seen = (*last_new_seen).max(n);
                    }
                }
                EditLine::Delete(o, txt) => {
                    let _ = writeln!(out, "-{txt}");
                    *last_old_seen = (*last_old_seen).max(o);
                }
                EditLine::Insert(n, txt) => {
                    let _ = writeln!(out, "+{txt}");
                    *last_new_seen = (*last_new_seen).max(n);
                }
            }
        }

        // 4. Preserve last `context` equal lines from eq_run for prefix of next hunk.
        prefix_ctx.clear();
        if context > 0 {
            let keep_start = eq_run.len().saturating_sub(context);
            for entry in eq_run.iter().skip(keep_start) {
                prefix_ctx.push_back(*entry);
            }
        }

        cur_hunk.clear();
        eq_run.clear();
    }
}

/// Compute Myers diff operations for blame/line-mapping scenarios.
pub fn compute_diff(old_lines: &[String], new_lines: &[String]) -> Vec<DiffOperation> {
    Diff::compute_line_operations(old_lines, new_lines)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::PathBuf, process::Command};

    use tempfile::tempdir;

    use super::{Diff, DiffOperation, compute_diff};
    use crate::hash::{HashKind, ObjectHash, set_hash_kind_for_test};

    /// Helper: run our diff on in-memory blobs and return diff text plus their hashes.
    fn run_diff(
        logical_path: &str,
        old_bytes: &[u8],
        new_bytes: &[u8],
    ) -> (String, ObjectHash, ObjectHash) {
        let file = PathBuf::from(logical_path);
        let old_hash = ObjectHash::new(old_bytes);
        let new_hash = ObjectHash::new(new_bytes);

        let mut blob_store: HashMap<ObjectHash, Vec<u8>> = HashMap::new();
        blob_store.insert(old_hash, old_bytes.to_vec());
        blob_store.insert(new_hash, new_bytes.to_vec());

        let mut old_map = HashMap::new();
        let mut new_map = HashMap::new();
        old_map.insert(file.clone(), old_hash);
        new_map.insert(file.clone(), new_hash);

        let reader = |_: &PathBuf, h: &ObjectHash| -> Vec<u8> {
            blob_store.get(h).cloned().unwrap_or_default()
        };

        let diff = Diff::diff_for_file_string(&file, &old_map, &new_map, &reader);
        (diff, old_hash, new_hash)
    }

    /// Helper: shorten hash to 7 chars for diff header normalization.
    fn short_hash(hash: &ObjectHash) -> String {
        hash.to_string().chars().take(7).collect()
    }

    /// Helper: run `git diff --no-index` on temp files and normalize headers for comparison.
    fn normalized_git_diff(
        logical_path: &str,
        old_bytes: &[u8],
        new_bytes: &[u8],
        old_hash: &ObjectHash,
        new_hash: &ObjectHash,
    ) -> Option<String> {
        let temp_dir = tempdir().ok()?;
        let old_file = temp_dir.path().join("old.txt");
        let new_file = temp_dir.path().join("new.txt");

        fs::write(&old_file, old_bytes).ok()?;
        fs::write(&new_file, new_bytes).ok()?;

        let output = Command::new("git")
            .current_dir(temp_dir.path())
            .args(["diff", "--no-index", "--unified=3", "old.txt", "new.txt"])
            .output()
            .ok()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.is_empty() {
            return None;
        }

        let short_old = short_hash(old_hash);
        let short_new = short_hash(new_hash);

        let mut normalized = Vec::new();
        for line in stdout.lines() {
            let rewritten = if line.starts_with("diff --git ") {
                format!("diff --git a/{logical_path} b/{logical_path}")
            } else if line.starts_with("index ") {
                format!("index {short_old}..{short_new} 100644")
            } else if line.starts_with("--- ") {
                format!("--- a/{logical_path}")
            } else if line.starts_with("+++ ") {
                format!("+++ b/{logical_path}")
            } else if line.starts_with("@@") {
                match line.rfind("@@") {
                    Some(pos) if pos + 2 <= line.len() => line[..pos + 2].to_string(),
                    _ => line.to_string(),
                }
            } else {
                line.to_string()
            };
            normalized.push(rewritten);
        }

        Some(normalized.join("\n") + "\n")
    }

    /// Basic text diff should include headers and expected +/- markers.
    #[test]
    fn unified_diff_basic_changes() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let old = b"a\nb\nc\n" as &[u8];
        let new = b"a\nB\nc\nd\n" as &[u8];
        let (diff, _, _) = run_diff("foo.txt", old, new);

        assert!(diff.contains("diff --git a/foo.txt b/foo.txt"));
        assert!(diff.contains("index "));
        assert!(diff.contains("--- a/foo.txt"));
        assert!(diff.contains("+++ b/foo.txt"));
        assert!(diff.contains("@@"));
        assert!(diff.contains("-b"));
        assert!(diff.contains("+B"));
        assert!(diff.contains("+d"));
    }

    /// Non-text inputs should yield a binary files notice.
    #[test]
    fn binary_files_detection() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let old_bytes = vec![0u8, 159, 146, 150];
        let new_bytes = vec![0xFF, 0x00, 0x01];
        let (diff, _, _) = run_diff("bin.dat", &old_bytes, &new_bytes);
        assert!(diff.contains("Binary files differ"));
    }

    /// Fixture diff should match git's inserted/deleted lines.
    #[test]
    fn diff_matches_git_for_fixture() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256); //use it to test SHA1/SHA-256 diffs as well
        let base: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "diff"]
            .iter()
            .collect();
        let old_bytes = fs::read(base.join("old.txt")).expect("read old.txt");
        let new_bytes = fs::read(base.join("new.txt")).expect("read new.txt");

        let (diff_output, old_hash, new_hash) = run_diff("fixture.txt", &old_bytes, &new_bytes);
        let git_output =
            normalized_git_diff("fixture.txt", &old_bytes, &new_bytes, &old_hash, &new_hash)
                .expect("git diff output");

        fn collect(s: &str, prefix: char) -> Vec<String> {
            s.lines()
                .filter(|l| l.starts_with(prefix))
                .map(|l| l.to_string())
                .collect()
        }
        let ours_del = collect(&diff_output, '-');
        let ours_ins = collect(&diff_output, '+');
        let git_del = collect(&git_output, '-');
        let git_ins = collect(&git_output, '+');

        use std::collections::HashSet;
        let ours_del_set: HashSet<_> = ours_del.iter().collect();
        let git_del_set: HashSet<_> = git_del.iter().collect();
        let ours_ins_set: HashSet<_> = ours_ins.iter().collect();
        let git_ins_set: HashSet<_> = git_ins.iter().collect();

        assert_eq!(
            ours_del_set, git_del_set,
            "deleted lines differ from git output"
        );
        assert_eq!(
            ours_ins_set, git_ins_set,
            "inserted lines differ from git output"
        );
    }

    /// Large input should still match git's inserted/deleted sets.
    #[test]
    fn diff_matches_git_for_large_change() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let old_lines: Vec<String> = (0..5_000).map(|i| format!("line {i}")).collect();
        let mut new_lines = old_lines.clone();
        for idx in [10, 499, 1_234, 3_210, 4_999] {
            new_lines[idx] = format!("updated line {idx}");
        }
        new_lines.insert(2_500, "inserted middle line".into());
        new_lines.push("new tail line".into());

        let old_text = old_lines.join("\n") + "\n";
        let new_text = new_lines.join("\n") + "\n";

        let (diff_output, old_hash, new_hash) = run_diff(
            "large_fixture.txt",
            old_text.as_bytes(),
            new_text.as_bytes(),
        );
        let git_output = normalized_git_diff(
            "large_fixture.txt",
            old_text.as_bytes(),
            new_text.as_bytes(),
            &old_hash,
            &new_hash,
        )
        .expect("git diff output");

        fn collect(s: &str, prefix: char) -> Vec<String> {
            s.lines()
                .filter(|l| l.starts_with(prefix))
                .map(|l| l.to_string())
                .collect()
        }
        use std::collections::HashSet;
        let ours_del: HashSet<_> = collect(&diff_output, '-').into_iter().collect();
        let ours_ins: HashSet<_> = collect(&diff_output, '+').into_iter().collect();
        let git_del: HashSet<_> = collect(&git_output, '-').into_iter().collect();
        let git_ins: HashSet<_> = collect(&git_output, '+').into_iter().collect();
        assert_eq!(ours_del, git_del, "deleted lines differ from git output");
        assert_eq!(ours_ins, git_ins, "inserted lines differ from git output");
    }

    /// Line mapping operations should match expected Equal/Delete/Insert sequence.
    #[test]
    fn compute_diff_operations_basic_mapping() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let old_lines = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let new_lines = vec![
            "a".to_string(),
            "B".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        let ops = compute_diff(&old_lines, &new_lines);

        let expected = vec![
            DiffOperation::Equal {
                old_line: 1,
                new_line: 1,
            },
            DiffOperation::Delete { line: 2 },
            DiffOperation::Insert {
                line: 2,
                content: "B".to_string(),
            },
            DiffOperation::Equal {
                old_line: 3,
                new_line: 3,
            },
            DiffOperation::Insert {
                line: 4,
                content: "d".to_string(),
            },
        ];

        assert_eq!(ops, expected);
    }
}
