//! An example demonstrating how to create a Git Tree object from Blob hashes and display its contents.
//! This example assumes you have Blob hashes available.
//! In a real scenario, you would obtain these hashes by creating Blob objects first.

use git_internal::hash::ObjectHash;
use git_internal::internal::object::tree::{Tree, TreeItem, TreeItemMode};
use std::str::FromStr;

fn main() {
    // mock Blob hashes (replace with actual Blob hashes as needed)
    let blob_hash_1 = ObjectHash::from_str("8ab686eafeb1f44702738c8b0f24f2567c36da6d").unwrap();
    let blob_hash_2 =
        ObjectHash::from_str("2cf8d83d9ee29543b34a87727421fdecb7e3f3a183d337639025de576db9ebb4")
            .unwrap();

    println!("Building a Tree object...");

    // create TreeItems for each Blob
    let item1 = TreeItem::new(
        TreeItemMode::Blob, // file mode (100644)
        blob_hash_1,
        "README.md".to_string(),
    );

    let item2 = TreeItem::new(TreeItemMode::Blob, blob_hash_2, "main.rs".to_string());

    // build Tree object from the list of TreeItems
    let tree_items = vec![item1, item2];
    let tree = Tree::from_tree_items(tree_items).expect("Failed to create tree");

    // output results
    println!("Created Tree Object successfully.");
    println!("Tree Hash ID: {}", tree.id);
    println!("Tree Contents:");
    for item in tree.tree_items {
        // Output format: <mode> <hash> <filename>
        println!(" - {} {} {}", item.mode, item.id, item.name);
    }
}
