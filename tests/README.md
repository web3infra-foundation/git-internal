# Tests

## Running Tests

```bash
# Run all tests (unit + integration)
cargo test

# Run specific test by name
cargo test idx_offsets_match_sha1

# Run tests with output
cargo test -- --nocapture

# Run only integration tests
cargo test --test decode-index-pack

# Run only unit tests (in src/)
cargo test --lib
```

## Test Structure

```
tests/
├── decode-index-pack.rs      # Integration tests for pack decode/idx roundtrip
├── data/
│   ├── packs/                # Pack files for testing
│   │   ├── small-sha1.pack/.idx
│   │   ├── small-sha256.pack/.idx
│   │   ├── medium-sha1.pack/.idx
│   │   ├── medium-sha256.pack/.idx
│   │   ├── ref-delta-sha1.pack/.idx
│   │   ├── ref-delta-sha256.pack/.idx
│   │   ├── encode-test-sha1.pack/.idx
│   │   └── encode-test-sha256.pack/.idx
│   ├── objects/              # Loose Git objects for object parsing tests
│   └── index/                # .git/index files for index parsing tests
│       ├── index-2           # 2 entries
│       ├── index-760         # 760 entries
│       └── index-9-256       # SHA-256 format
├── diff/                     # Diff test fixtures
│   ├── old.txt / new.txt     # Sample files for diff testing
│   └── *.blob                # Expected diff output blobs
└── refs/                     # Reference test data
```

## Integration Tests

### decode-index-pack.rs

Tests pack decode and idx file generation roundtrip for both SHA-1 and SHA-256:

| Test | Description |
|------|-------------|
| `idx_offsets_match_sha1_small` | Decode small-sha1.pack, rebuild idx, verify offsets |
| `idx_offsets_match_sha1_delta` | Decode ref-delta-sha1.pack with delta objects |
| `idx_offsets_match_sha256_small` | Decode small-sha256.pack (SHA-256 format) |
| `idx_offsets_match_sha256_delta` | Decode ref-delta-sha256.pack with delta objects |

## Unit Tests

Unit tests are located in `src/` modules under `#[cfg(test)]` blocks:

| Module | Coverage |
|--------|----------|
| `hash.rs` | Hash algorithm, ObjectHash operations |
| `diff.rs` | Unified diff generation |
| `delta/*` | Delta encode/decode, similarity heuristics |
| `zstdelta/` | Zstd dictionary delta compression |
| `internal/object/*` | Blob/Tree/Commit/Tag/Note parsing |
| `internal/pack/*` | Pack decode/encode, cache, waitlist, idx |
| `internal/index.rs` | .git/index file parsing |
| `protocol/*` | Smart protocol, HTTP/SSH adapters |

## Test Data

### Pack Files

- **small-***: Minimal packs for basic decode testing
- **medium-***: Larger packs for performance and edge cases
- **ref-delta-***: Packs containing REF_DELTA objects for delta chain testing
- **encode-test-***: Packs for encode/decode roundtrip verification

All packs have both SHA-1 and SHA-256 variants (suffix indicates hash type).

### Loose Objects

Located in `data/objects/` using Git's standard `xx/xxxxxx...` directory structure. Used for object parsing unit tests.

### Index Files

Located in `data/index/`:
- `index-2`: Small index with 2 entries
- `index-760`: Larger index for parsing validation
- `index-9-256`: SHA-256 object format index

## Hash Algorithm in Tests

Use `set_hash_kind_for_test()` to temporarily set hash algorithm within a test scope:

```rust
use git_internal::hash::{HashKind, set_hash_kind_for_test};

#[test]
fn my_sha256_test() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);
    // Test code here - hash kind reverts when _guard drops
}
```

## Adding New Test Data

1. Place pack files in `tests/data/packs/` with naming convention: `<name>-<sha1|sha256>.pack`
2. Include corresponding `.idx` file for roundtrip verification
3. Loose objects go in `tests/data/objects/<first-2-hex>/<remaining-hex>`
4. Update this README if adding new test categories
