//! An example demonstrating how to encode Git objects into a pack file using git-internal crate.
//! This example creates several Blob objects from string data, encodes them into a pack file,
//! and writes the resulting pack file to disk.
//! 
//! Make sure to check the output directory for the generated pack file after running this example.
//! The example assumes SHA-1 hashing for simplicity.

use git_internal::internal::metadata::{EntryMeta, MetaAttached};
use git_internal::internal::object::blob::Blob;
use git_internal::internal::pack::encode::encode_and_output_to_files;
use git_internal::internal::pack::entry::Entry;
use std::fs;
use std::path::PathBuf;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    // 1. prepare data to encode
    let contents = vec!["Hello World", "Rust is awesome", "Git internals are fun"];

    let object_number = contents.len();
    let window_size = 10; // Delta compression window size, 0 means no Delta compression
    let output_dir = PathBuf::from("examples/output_packs");

    if !output_dir.exists() {
        fs::create_dir(&output_dir).expect("Failed to create output directory");
    }

    println!("Preparing to encode {} objects...", object_number);

    // 2. Create a channel to send Entry
    // Buffer size can be adjusted based on memory conditions
    let (entry_tx, entry_rx) = mpsc::channel(100);

    // 3. Start encoding task
    // encode_and_output_to_files will process received Entry in the background and write to files
    let encode_handle = tokio::spawn(async move {
        encode_and_output_to_files(entry_rx, object_number, output_dir, window_size).await
    });

    // 4. Send data
    for content in contents {
        // Convert string to Blob, then to Entry
        let blob = Blob::from_content(content);
        let entry: Entry = blob.into();

        // Wrap metadata
        let meta_entry = MetaAttached {
            inner: entry,
            meta: EntryMeta::new(),
        };

        entry_tx
            .send(meta_entry)
            .await
            .expect("Failed to send entry");
    }

    // 5. Close the sender to notify the encoder that data sending is complete
    drop(entry_tx);

    // 6. wait for encoding to complete
    match encode_handle.await.unwrap() {
        Ok(_) => println!("Pack encoding successful! Check 'output_packs' directory."),
        Err(e) => eprintln!("Pack encoding failed: {}", e),
    }
}
