//! An example demonstrating how to decode a Git pack file using git-internal crate.
//! This example reads a pack file from disk, decodes it, and prints out the
//! decoded objects' information.
//! Make sure to replace the `pack_path` variable with the path to your own pack file.
//! The example assumes the pack file uses SHA-1 hashing.
use std::{fs::File, io::BufReader, path::Path};

use git_internal::{hash::ObjectHash, internal::pack::Pack};

fn main() {
    // replace with the path to your pack file
    let pack_path = "tests/data/packs/small-sha1.pack";
    if !Path::new(pack_path).exists() {
        println!("Pack file not found: {}, skipping example.", pack_path);
        return;
    }

    let f = File::open(pack_path).expect("Failed to open pack file");
    // Pack decode requires a reader that implements BufRead + Send
    let mut reader = BufReader::new(f);

    // Initialize Pack
    // Parameters:
    // 1. thread_num: None (automatically use the number of CPU cores)
    // 2. mem_limit: None (no memory limit)
    // 3. temp_path: None (use default temporary directory ./.cache_temp)
    // 4. clean_tmp: true (automatically clean temporary files on Drop)
    let mut pack = Pack::new(None, None, None, true);

    println!("Starting to decode pack file...");

    // Start decoding
    pack.decode(
        &mut reader,
        |entry| {
            // Callback function: process each decoded object (Entry)
            // entry.inner contains the actual data, entry.meta contains metadata
            println!(
                "Decoded object: {} | Type: {:?}",
                entry.inner.hash, entry.inner.obj_type
            );
        },
        None::<fn(ObjectHash)>, // Optional: callback for the overall Pack file Hash
    )
    .expect("Failed to decode pack");

    println!("Decode finished. Total objects processed: {}", pack.number);
}
