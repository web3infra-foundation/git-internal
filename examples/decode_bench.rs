//! Decode benchmark harness.
//!
//! Example:
//! cargo run --release --features decode_profile --example decode_bench -- \
//!   --pack tests/data/packs/medium-sha1.pack --hash sha1 --threads 2 \
//!   --mem-limit-mb 1024 --warmups 1 --runs 5 --out target/decode-bench.jsonl

use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use git_internal::{
    hash::{HashKind, ObjectHash, set_hash_kind},
    internal::pack::Pack,
};
use serde_json::json;

#[cfg(not(feature = "decode_profile"))]
#[derive(Clone, Copy, Debug, Default)]
struct DecodeProfileSnapshot {
    cache_hits: usize,
    cache_misses: usize,
    fallback_loads: usize,
    base_objects: usize,
    delta_objects: usize,
    waitlist_inserts: usize,
    waitlist_takes: usize,
    delta_rebuilds: usize,
    peak_internal_memory_bytes: usize,
}

#[cfg(feature = "decode_profile")]
fn profile_reset() {
    git_internal::internal::pack::profile::reset();
}

#[cfg(not(feature = "decode_profile"))]
fn profile_reset() {}

#[cfg(feature = "decode_profile")]
fn profile_snapshot() -> git_internal::internal::pack::profile::DecodeProfileSnapshot {
    git_internal::internal::pack::profile::snapshot()
}

#[cfg(not(feature = "decode_profile"))]
fn profile_snapshot() -> DecodeProfileSnapshot {
    DecodeProfileSnapshot::default()
}

#[derive(Debug)]
struct Config {
    pack: PathBuf,
    hash: HashKind,
    threads: usize,
    mem_limit_mb: Option<usize>,
    warmups: usize,
    runs: usize,
    out: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    set_hash_kind(config.hash);

    let mut writer: Box<dyn Write> = match &config.out {
        Some(path) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            Box::new(File::create(path)?)
        }
        None => Box::new(io::stdout().lock()),
    };

    for run_index in 0..(config.warmups + config.runs) {
        let is_warmup = run_index < config.warmups;
        let measured_run_index = run_index.saturating_sub(config.warmups);
        let result = run_decode(&config, run_index)?;

        if !is_warmup {
            let record = result.to_json(&config, measured_run_index);
            serde_json::to_writer(&mut writer, &record)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct RunResult {
    elapsed_ms: u128,
    object_count: usize,
    callback_count: usize,
    pack_hash: String,
    cache_hits: usize,
    cache_misses: usize,
    fallback_loads: usize,
    base_objects: usize,
    delta_objects: usize,
    waitlist_inserts: usize,
    waitlist_takes: usize,
    delta_rebuilds: usize,
    peak_internal_memory_bytes: usize,
}

impl RunResult {
    fn to_json(&self, config: &Config, run_index: usize) -> serde_json::Value {
        let cache_lookups = self.cache_hits + self.cache_misses;
        let cache_hit_ratio = if cache_lookups == 0 {
            0.0
        } else {
            self.cache_hits as f64 / cache_lookups as f64
        };

        json!({
            "run": run_index,
            "pack": config.pack.display().to_string(),
            "hash": config.hash.as_str(),
            "threads": config.threads,
            "mem_limit_mb": config.mem_limit_mb,
            "decode_profile_enabled": cfg!(feature = "decode_profile"),
            "elapsed_ms": self.elapsed_ms,
            "object_count": self.object_count,
            "callback_count": self.callback_count,
            "pack_hash": self.pack_hash,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_ratio": cache_hit_ratio,
            "fallback_loads": self.fallback_loads,
            "base_objects": self.base_objects,
            "delta_objects": self.delta_objects,
            "waitlist_inserts": self.waitlist_inserts,
            "waitlist_takes": self.waitlist_takes,
            "delta_rebuilds": self.delta_rebuilds,
            "peak_internal_memory_bytes": self.peak_internal_memory_bytes,
        })
    }
}

fn run_decode(config: &Config, run_index: usize) -> Result<RunResult, Box<dyn std::error::Error>> {
    profile_reset();
    set_hash_kind(config.hash);

    let file = OpenOptions::new().read(true).open(&config.pack)?;
    let mut reader = BufReader::new(file);
    let temp_path = std::env::temp_dir().join(format!(
        "git-internal-decode-bench-{}-{run_index}",
        std::process::id()
    ));
    let mem_limit = config.mem_limit_mb.map(|mb| mb * 1024 * 1024);
    let mut pack = Pack::new(Some(config.threads), mem_limit, Some(temp_path), true);

    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_for_cb = callback_count.clone();
    let started = Instant::now();
    pack.decode(
        &mut reader,
        move |_| {
            callback_count_for_cb.fetch_add(1, Ordering::Relaxed);
        },
        None::<fn(ObjectHash)>,
    )?;
    let elapsed_ms = started.elapsed().as_millis();
    let snapshot = profile_snapshot();

    Ok(RunResult {
        elapsed_ms,
        object_count: pack.number,
        callback_count: callback_count.load(Ordering::Relaxed),
        pack_hash: pack.signature.to_string(),
        cache_hits: snapshot.cache_hits,
        cache_misses: snapshot.cache_misses,
        fallback_loads: snapshot.fallback_loads,
        base_objects: snapshot.base_objects,
        delta_objects: snapshot.delta_objects,
        waitlist_inserts: snapshot.waitlist_inserts,
        waitlist_takes: snapshot.waitlist_takes,
        delta_rebuilds: snapshot.delta_rebuilds,
        peak_internal_memory_bytes: snapshot.peak_internal_memory_bytes,
    })
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut pack = None;
        let mut hash = None;
        let mut threads = None;
        let mut mem_limit_mb = None;
        let mut warmups = 0usize;
        let mut runs = 1usize;
        let mut out = None;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--pack" => pack = Some(PathBuf::from(take_value(&mut args, "--pack")?)),
                "--hash" => {
                    let value = take_value(&mut args, "--hash")?;
                    hash = Some(value.parse::<HashKind>()?);
                }
                "--threads" => {
                    threads = Some(parse_usize(
                        &take_value(&mut args, "--threads")?,
                        "--threads",
                    )?)
                }
                "--mem-limit-mb" => {
                    let value = take_value(&mut args, "--mem-limit-mb")?;
                    mem_limit_mb = if value == "none" {
                        Some(None)
                    } else {
                        Some(Some(parse_usize(&value, "--mem-limit-mb")?))
                    };
                }
                "--warmups" => {
                    warmups = parse_usize(&take_value(&mut args, "--warmups")?, "--warmups")?
                }
                "--runs" => runs = parse_usize(&take_value(&mut args, "--runs")?, "--runs")?,
                "--out" => {
                    let value = take_value(&mut args, "--out")?;
                    if value != "-" {
                        out = Some(PathBuf::from(value));
                    }
                }
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument `{other}`\n{}", usage())),
            }
        }

        let threads = threads.ok_or_else(|| format!("missing --threads\n{}", usage()))?;
        if threads == 0 {
            return Err("--threads must be greater than 0".to_string());
        }
        if runs == 0 {
            return Err("--runs must be greater than 0".to_string());
        }

        Ok(Config {
            pack: pack.ok_or_else(|| format!("missing --pack\n{}", usage()))?,
            hash: hash.ok_or_else(|| format!("missing --hash\n{}", usage()))?,
            threads,
            mem_limit_mb: mem_limit_mb.unwrap_or(None),
            warmups,
            runs,
            out,
        })
    }
}

fn take_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for `{name}`\n{}", usage()))
}

fn parse_usize(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid value for `{name}`: `{value}`"))
}

fn usage() -> String {
    "usage: decode_bench --pack <path> --hash sha1|sha256 --threads <n> --mem-limit-mb <n|none> --warmups <n> --runs <n> [--out <path|->]".to_string()
}
