use std::{env, path::PathBuf, process::ExitCode, str::FromStr};

use git_internal::{
    hash::{HashKind, set_hash_kind},
    internal::pack::stats::collect_pack_stats,
};

/// Runs the pack statistics example and returns a process exit code.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// Parses CLI arguments, collects pack statistics, and prints the report.
///
/// # Returns
/// `Ok(())` when the report is printed successfully, otherwise a user-facing error string.
///
/// # Side effects
/// Reads a pack file from disk, updates the process hash-kind setting, and writes to stdout.
fn run() -> Result<(), String> {
    // 1. Parse an optional hash-kind flag before the pack path.
    // 2. Configure the global hash kind so trailer and ref-delta hashes decode correctly.
    // 3. Collect and print the pack statistics through PackStats::Display.
    let mut args = env::args().skip(1);
    let first_arg = args.next().ok_or_else(usage)?;
    let pack_path = if first_arg == "--hash" {
        let hash_kind = args
            .next()
            .ok_or_else(|| "missing value after --hash\n\n".to_string() + &usage())?;
        set_hash_kind(HashKind::from_str(&hash_kind)?);
        PathBuf::from(args.next().ok_or_else(usage)?)
    } else {
        PathBuf::from(first_arg)
    };

    if args.next().is_some() {
        return Err("too many arguments\n\n".to_string() + &usage());
    }

    let stats = collect_pack_stats(pack_path).map_err(|error| error.to_string())?;
    println!("{stats}");
    Ok(())
}

/// Returns command-line usage text for the example program.
fn usage() -> String {
    "usage: cargo run --example pack_stats -- [--hash sha1|sha256] <pack-path>".to_string()
}
