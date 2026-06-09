//! Measure the compression rate of the git-internal pack encoder on an
//! existing Git repository.
//!
//! Usage:
//!
//! ```bash
//! cargo run --release --example grading_bot_encode_pack_bench -- /data/rk8s-dev/rk8s/.git
//! # Optional second arg: delta window size (default: 10, 0 disables delta).
//! cargo run --release --example grading_bot_encode_pack_bench -- /data/rk8s-dev/rk8s/.git 50
//! # Optional --rabin flag: use Rabin fingerprint delta (requires diff_rabin feature).
//! cargo run --release --features diff_rabin --example grading_bot_encode_pack_bench -- --rabin /data/rk8s-dev/rk8s/.git
//! ```
//!
//! The example walks both loose objects (`.git/objects/<aa>/<38-hex>`) and
//! existing pack files (`.git/objects/pack/pack-*.pack`), deduplicates them
//! by `ObjectHash`, re-encodes the unified set with the library's
//! `encode_and_output_to_files`, and prints the raw-vs-pack byte ratio.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use flate2::read::ZlibDecoder;
#[cfg(feature = "diff_rabin")]
use git_internal::internal::pack::encode::encode_and_output_to_files_with_rabin;
#[cfg(feature = "diff_rabin")]
use git_internal::internal::pack::encode::encode_and_output_to_files_with_rabin_no_prefilter;
use git_internal::{
    hash::{HashKind, ObjectHash, set_hash_kind},
    internal::{
        metadata::{EntryMeta, MetaAttached},
        object::types::ObjectType,
        pack::{Pack, encode::encode_and_output_to_files, entry::Entry},
    },
};
use tokio::sync::mpsc;

// ── grading correctness fingerprint ──────────────────────────────────────
// Order-independent fingerprint over a multiset of object hashes. Kept
// byte-for-byte identical to the copy in the decode bench and the reference
// oracle (examples/grading_bot_ref_decode.rs). The encode bench prints the
// fingerprint of the *input* object set (what was fed to the encoder); the
// grading bot then has the reference implementation decode the produced pack
// and compares its fingerprint to this one — equal fingerprints prove the
// student's pack round-trips to the same object multiset.
#[derive(Default)]
struct GradingFingerprint {
    count: u64,
    xor: [u8; 32],
    sum: u128,
}

impl GradingFingerprint {
    fn add(&mut self, hash_bytes: &[u8]) {
        self.count += 1;
        for (i, b) in hash_bytes.iter().enumerate() {
            self.xor[i % 32] ^= *b;
        }
        let mut v: u128 = 0x6c62272e07bb0142_62b821756295c58d;
        for b in hash_bytes {
            v = (v ^ *b as u128).wrapping_mul(0x0000000001000000_000000000000013b);
        }
        self.sum = self.sum.wrapping_add(v);
    }

    fn finish(&self) -> String {
        let mut hex = String::with_capacity(64);
        for b in &self.xor {
            hex.push_str(&format!("{b:02x}"));
        }
        format!("count={} xor={} sum={:032x}", self.count, hex, self.sum)
    }
}

/// Read VmHWM/VmRSS from /proc/self/status (Linux). Returns bytes.
#[cfg(target_os = "linux")]
fn read_proc_status_kib(key: &str) -> Option<usize> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            // Format: "VmHWM:\t  123456 kB"
            let rest = rest.trim_start_matches(':').trim();
            let num = rest.split_whitespace().next()?;
            let kib = num.parse::<usize>().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_proc_status_kib(_key: &str) -> Option<usize> {
    None
}

fn format_bytes(b: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let bf = b as f64;
    if bf >= GB {
        format!("{:.2} GiB", bf / GB)
    } else if bf >= MB {
        format!("{:.2} MiB", bf / MB)
    } else if bf >= KB {
        format!("{:.2} KiB", bf / KB)
    } else {
        format!("{b} B")
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- 1. Parse arguments -------------------------------------------------
    let args: Vec<String> = std::env::args().collect();

    // Parse flags and positional args
    let mut positional: Vec<&str> = Vec::new();
    let mut use_rabin = false;
    let mut no_prefilter = false;
    for arg in args.iter().skip(1) {
        if arg == "--rabin" {
            use_rabin = true;
        } else if arg == "--no-prefilter" {
            no_prefilter = true;
        } else if arg.starts_with("--") {
            eprintln!("unknown flag: {arg}");
            std::process::exit(2);
        } else {
            positional.push(arg.as_str());
        }
    }

    if positional.is_empty() || positional.len() > 2 {
        eprintln!(
            "usage: {} [--rabin] [--no-prefilter] <path-to-.git> [window_size]\n  --rabin         Use Rabin fingerprint delta (requires diff_rabin feature)\n  --no-prefilter  Disable similarity pre-filter (only with --rabin)",
            args.first().map(String::as_str).unwrap_or("grading_bot_encode_pack_bench")
        );
        std::process::exit(2);
    }
    let git_dir = PathBuf::from(positional[0]);
    let window_size: usize = if positional.len() == 2 {
        positional[1]
            .parse()
            .map_err(|e| format!("invalid window_size {:?}: {e}", positional[1]))?
    } else {
        10
    };

    #[cfg(not(feature = "diff_rabin"))]
    if use_rabin {
        eprintln!("warning: --rabin requires the `diff_rabin` feature; falling back to default delta");
        use_rabin = false;
    }

    if !git_dir.join("objects").is_dir() {
        return Err(format!(
            "{} does not look like a .git directory (no objects/ subdir)",
            git_dir.display()
        )
        .into());
    }

    // GitHub repos are SHA-1 by default.
    set_hash_kind(HashKind::Sha1);

    println!("Repository : {}", git_dir.display());
    println!("Hash kind  : sha1");
    println!("Window     : {window_size}");
    println!();

    // --- 2. Collect every object --------------------------------------------
    let objects_dir = git_dir.join("objects");

    let (mut entries, loose_count) = read_loose_objects(&objects_dir)?;
    println!("Loose objects scanned    : {loose_count}");

    let packed_count = read_packed_objects(&objects_dir, &mut entries)?;
    println!("Objects read from packs  : {packed_count}");

    let unique = entries.len();
    println!("Unique objects (deduped) : {unique}");

    if unique == 0 {
        return Err("no objects found in repository".into());
    }

    // Sum of decompressed payload bytes — what the pack encoder has to compress.
    let raw_bytes: u64 = entries.values().map(|e| e.data.len() as u64).sum();
    println!("Raw object bytes         : {}", format_bytes(raw_bytes as usize));

    // Fingerprint of the *input* object set (what we feed the encoder), folded
    // before the entries are consumed below. The grading bot compares this
    // against the reference implementation's fingerprint over the pack this
    // run produces — equal fingerprints prove the student's pack round-trips
    // to the same object multiset it was built from.
    let mut fp = GradingFingerprint::default();
    for hash in entries.keys() {
        fp.add(hash.as_ref());
    }
    let input_fingerprint = fp.finish();

    // --- 3. Re-encode into a fresh pack -------------------------------------
    let out_dir = tempfile::tempdir()?;
    let out_path = out_dir.path().to_path_buf();
    let (tx, rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1024);

    let n = unique;
    let win = window_size;
    let out_path_for_task = out_path.clone();
    let algorithm_name = if use_rabin && no_prefilter {
        "rabin-no-prefilter"
    } else if use_rabin {
        "rabin"
    } else {
        "myers"
    };
    let no_pf = no_prefilter;
    let encode_handle = tokio::spawn(async move {
        if use_rabin && no_pf {
            #[cfg(feature = "diff_rabin")]
            {
                encode_and_output_to_files_with_rabin_no_prefilter(rx, n, out_path_for_task, win).await
            }
            #[cfg(not(feature = "diff_rabin"))]
            {
                let _ = (rx, n, out_path_for_task, win);
                unreachable!("--rabin --no-prefilter requires diff_rabin feature")
            }
        } else if use_rabin {
            #[cfg(feature = "diff_rabin")]
            {
                encode_and_output_to_files_with_rabin(rx, n, out_path_for_task, win).await
            }
            #[cfg(not(feature = "diff_rabin"))]
            {
                let _ = (rx, n, out_path_for_task, win);
                unreachable!("--rabin requires diff_rabin feature")
            }
        } else {
            encode_and_output_to_files(rx, n, out_path_for_task, win).await
        }
    });

    println!();
    println!("Encoding pack ({} objects, window={window_size}, algorithm={algorithm_name}) ...", n);

    // Capture baseline memory before encoding starts.
    let baseline_rss = read_proc_status_kib("VmRSS").unwrap_or(0);
    let baseline_hwm = read_proc_status_kib("VmHWM").unwrap_or(0);

    let encode_start = std::time::Instant::now();

    for entry in entries.into_values() {
        tx.send(MetaAttached {
            inner: entry,
            meta: EntryMeta::new(),
        })
        .await
        .map_err(|e| format!("send entry failed: {e}"))?;
    }
    drop(tx);

    encode_handle.await??;
    let encode_elapsed = encode_start.elapsed();

    // --- 4. Locate the produced .pack and report ----------------------------
    let pack_path = find_single_pack(&out_path)?;
    let pack_bytes = fs::metadata(&pack_path)?.len();

    // Capture final memory after encoding completes.
    let final_rss = read_proc_status_kib("VmRSS").unwrap_or(0);
    let peak_hwm = read_proc_status_kib("VmHWM").unwrap_or(0);

    // --- 5. Print detailed stats in decode-bench format --------------------
    println!("------------------------------------------------------------");
    println!("mode:          encode");
    println!("input:         {}", git_dir.display());
    println!("objects:       {unique}");
    println!("window:        {window_size}");
    println!("raw bytes:     {} ({} bytes)", format_bytes(raw_bytes as usize), raw_bytes);
    println!("wall:          {:.3} s", encode_elapsed.as_secs_f64());
    let throughput = if encode_elapsed.as_secs_f64() > 0.0 {
        raw_bytes as f64 / encode_elapsed.as_secs_f64() / (1024.0 * 1024.0)
    } else {
        0.0
    };
    println!("throughput:    {throughput:.2} MiB/s (raw input)");
    println!("baseline RSS:  {}", format_bytes(baseline_rss));
    println!("final RSS:     {}", format_bytes(final_rss));
    println!(
        "peak RSS:      {} (delta vs baseline: {})",
        format_bytes(peak_hwm),
        format_bytes(peak_hwm.saturating_sub(baseline_hwm))
    );
    println!("pack written:  {}", pack_path.display());
    println!("pack size:     {} ({} bytes)", format_bytes(pack_bytes as usize), pack_bytes);

    // --- 6. Compression ratio ----------------------------------------------
    let ratio_pct = if raw_bytes > 0 {
        (pack_bytes as f64 / raw_bytes as f64) * 100.0
    } else {
        0.0
    };
    let inverse = if pack_bytes > 0 {
        raw_bytes as f64 / pack_bytes as f64
    } else {
        0.0
    };
    println!("compression:   {ratio_pct:.2}% (pack/raw), {inverse:.2}x (raw/pack)");

    // Correctness round-trip support. Emit the input fingerprint, and — when
    // the grading bot hands us a persistent output directory via
    // GRADING_ENCODE_OUTPUT_DIR — copy the produced pack there so the bot can
    // have the reference implementation decode it after this process exits
    // (the tempdir above is removed on drop). Best-effort: a copy failure is
    // reported but does not fail the bench, since the timing/ratio numbers
    // above are still valid.
    println!("GRADING_FINGERPRINT_BEGIN");
    println!("{input_fingerprint}");
    println!("GRADING_FINGERPRINT_END");
    if let Ok(dst_dir) = std::env::var("GRADING_ENCODE_OUTPUT_DIR") {
        let dst_dir = PathBuf::from(dst_dir);
        match fs::create_dir_all(&dst_dir)
            .and_then(|()| fs::copy(&pack_path, dst_dir.join("grading_encoded.pack")))
        {
            Ok(_) => println!(
                "GRADING_ENCODE_PACK: {}",
                dst_dir.join("grading_encoded.pack").display()
            ),
            Err(e) => eprintln!("warning: copying produced pack to {}: {e}", dst_dir.display()),
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Loose objects
// ---------------------------------------------------------------------------

/// Walk `.git/objects/<aa>/<38-hex>` and decode every loose object.
///
/// Returns the deduplicated entry map (keyed by `ObjectHash`) and the count of
/// loose files actually decoded.
fn read_loose_objects(
    objects_dir: &Path,
) -> Result<(HashMap<ObjectHash, Entry>, usize), Box<dyn std::error::Error>> {
    let mut map: HashMap<ObjectHash, Entry> = HashMap::new();
    let mut count = 0usize;

    let read_dir = match fs::read_dir(objects_dir) {
        Ok(r) => r,
        Err(e) => return Err(format!("cannot read {}: {e}", objects_dir.display()).into()),
    };

    for sub in read_dir {
        let sub = sub?;
        let name = sub.file_name();
        let name_str = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        // Only 2-hex-char directories hold loose objects.
        if name_str.len() != 2 || !name_str.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if !sub.file_type()?.is_dir() {
            continue;
        }

        for file in fs::read_dir(sub.path())? {
            let file = file?;
            let fname = file.file_name();
            let fname_str = match fname.to_str() {
                Some(s) => s,
                None => continue,
            };
            // Loose object filename is 38 hex chars (SHA-1: 2+38=40).
            if fname_str.len() != 38 || !fname_str.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            let hex = format!("{name_str}{fname_str}");
            let hash = ObjectHash::from_str(&hex)
                .map_err(|e| format!("bad hash {hex}: {e}"))?;

            let entry = match decode_loose_file(&file.path(), hash.clone()) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!(
                        "warning: skipping loose object {}: {e}",
                        file.path().display()
                    );
                    continue;
                }
            };
            map.insert(hash, entry);
            count += 1;
        }
    }

    Ok((map, count))
}

/// Decompress a single loose-object file and parse its header.
fn decode_loose_file(path: &Path, hash: ObjectHash) -> Result<Entry, Box<dyn std::error::Error>> {
    let f = File::open(path)?;
    let mut z = ZlibDecoder::new(BufReader::new(f));
    let mut buf = Vec::new();
    z.read_to_end(&mut buf)?;

    // Header: "<type> <size>\0<content>"
    let nul = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or("loose object: missing nul terminator in header")?;
    let header = std::str::from_utf8(&buf[..nul])?;
    let mut parts = header.splitn(2, ' ');
    let kind = parts.next().ok_or("loose header: missing type")?;
    let _size = parts.next().ok_or("loose header: missing size")?;

    let obj_type = match kind {
        "blob" => ObjectType::Blob,
        "tree" => ObjectType::Tree,
        "commit" => ObjectType::Commit,
        "tag" => ObjectType::Tag,
        other => return Err(format!("unexpected loose object type {other:?}").into()),
    };

    Ok(Entry {
        obj_type,
        data: buf[nul + 1..].to_vec(),
        hash,
        chain_len: 0,
    })
}

// ---------------------------------------------------------------------------
// Existing pack files
// ---------------------------------------------------------------------------

/// Decode every `.git/objects/pack/pack-*.pack` and merge its entries into
/// `map`. Returns the number of objects decoded (before dedup).
fn read_packed_objects(
    objects_dir: &Path,
    map: &mut HashMap<ObjectHash, Entry>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let pack_dir = objects_dir.join("pack");
    if !pack_dir.is_dir() {
        return Ok(0);
    }

    // Collect into a shared map so the Send+Sync+'static callback can update it.
    let shared: Arc<Mutex<HashMap<ObjectHash, Entry>>> = Arc::new(Mutex::new(std::mem::take(map)));
    let mut total = 0usize;

    for entry in fs::read_dir(&pack_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("pack") {
            continue;
        }
        println!("  decoding pack: {}", path.display());

        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut pack = Pack::new(None, None, None, true);

        let cb_map = Arc::clone(&shared);
        pack.decode(
            &mut reader,
            move |entry| {
                let mut guard = cb_map.lock().expect("pack decode map poisoned");
                guard.insert(entry.inner.hash.clone(), entry.inner);
            },
            None::<fn(ObjectHash)>,
        )?;

        total += pack.number;
    }

    *map = Arc::try_unwrap(shared)
        .map_err(|_| "internal: outstanding refs to pack-decode map")?
        .into_inner()
        .map_err(|e| format!("internal: map mutex poisoned: {e}"))?;

    Ok(total)
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

fn find_single_pack(dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut found = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("pack") {
            if found.is_some() {
                return Err(format!(
                    "more than one .pack file in {}",
                    dir.display()
                )
                .into());
            }
            found = Some(p);
        }
    }
    found.ok_or_else(|| format!("no .pack file produced in {}", dir.display()).into())
}
