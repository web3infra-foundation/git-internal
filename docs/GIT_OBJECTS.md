# Git Object Reference

This document summarizes the object formats supported by git-internal, how IDs are hashed, and how they map to canonical Git formats, based on the implementations in `src/internal/object`.

## Common Format and Hashing

- Storage format: `<type> <size>\0<raw-bytes>`, where `type` is `blob/tree/commit/tag` and `size` is the raw data length (decimal string).
- Hashing: `ObjectHash::from_type_and_data(ObjectType, data)` produces an ID using the current thread hash algorithm (Now support SHA1 and SHA256); switch via `set_hash_kind` / `set_hash_kind_for_test`.
- Types: `ObjectType` offers `to_string`/`to_u8`/`from_u8`/`from_string`, covering base objects (Commit/Tree/Blob/Tag) and delta objects (OffsetDelta/HashDelta/OffsetZstdelta—extension).
- Serialization: Each object’s `to_data` returns `<type><size>\0<body>`; `ObjectHash::to_string()` emits hex, `to_data()` returns raw bytes.

## Blob

- Location: `object/blob.rs`.
- Meaning: File content snapshot, no path/permission (those live in Tree).
- Structure: `Blob { id: ObjectHash, data: Vec<u8> }`.
- Build: `Blob::from_content` / `from_content_bytes` auto-compute the hash; `from_bytes(data, hash)` parses with a known hash.
- Serialize: `to_data()` returns raw content (header is implied when hashing with `ObjectHash::from_type_and_data`).

## Tree and TreeItem

- Location: `object/tree.rs`.
- TreeItem format: `"<mode> <name>\0<id-bytes>"`; modes include `100644`/`100755`/`120000`/`160000`/`40000` (gitlink).
- Structure: `TreeItem { mode: TreeItemMode, id: ObjectHash, name: String }`; `Tree { id, tree_items: Vec<TreeItem> }`.
- Build: `Tree::from_tree_items(items)` computes the tree hash; `rehash` recomputes after modifications.
- Parse: `Tree::from_bytes(data, hash)` splits IDs using current hash length (20/32 bytes); TreeItem parsing has a GBK fallback for non-UTF-8 names.

## Commit

- Location: `object/commit.rs`.
- Field order: `tree <tree-id>`, zero or more `parent <parent-id>`, `author <signature>`, `committer <signature>`, blank line, then message (may include signatures).
- Structure: `Commit { id, tree_id, parent_commit_ids, author, committer, message }`.
- Build: `Commit::new` (explicit signatures) or `from_tree_id` (convenience with current-time signatures); both use `ObjectHash::from_type_and_data` to derive the ID.
- Parse: `from_bytes` splits lines and uses `Signature::from_data` for author/committer.
- Helper: `format_message` skips PGP signature blocks or returns the first non-empty line.

## Tag (Annotated)

- Location: `object/tag.rs`.
- Format:  
  `object <object-hash>`  
  `type <object-type>`  
  `tag <tag-name>`  
  `tagger <name> <email> <timestamp> <tz>`  
  `<message>` (after a blank line)
- Structure: `Tag { id, object_hash, object_type, tag_name, tagger, message }`.
- Build: `Tag::new(object_hash, object_type, tag_name, tagger, message)`; hash is computed from serialized content.
- Parse: `from_bytes` validates UTF-8 and errors on invalid fields; `to_data` emits the format above.

## Note

- Location: `object/note.rs`.
- Meaning: An annotation attached to an object; internally treated as a Blob (`get_type` returns Blob), and hashed using Blob rules.
- Build/Parse: `Note::from_content(note_on, note_author, note_message)` computes the hash; `from_bytes` reads back fields.

## Signature

- Location: `object/signature.rs`.
- Layout: `<role> <name> <email> <timestamp> <tz>`, where `role` is `author`/`committer`/`tagger` (`SignatureType`).
- Functions: `Signature::from_data` parses a byte sequence; `to_data` serializes; `new` creates a signature with a given role/name/email (caller supplies time).

## Helpers and Common Types

- Location: `object/utils.rs`.
- Contents: Currently minimal; most shared I/O/hash helpers live in top-level `utils.rs`.

## Pack/Protocol Integration

- Pack decode yields `Entry` with `obj_type`, `hash`, `data`; these can be parsed by the object modules above.
- Pack encode expects `ObjectHash` plus raw data; `PackEncoder` uses `ObjectType` to craft headers and validate hashes.
- Protocol (upload-pack/receive-pack) cares about object ID/type consistency; content parsing is left to the caller or higher layers as needed.
