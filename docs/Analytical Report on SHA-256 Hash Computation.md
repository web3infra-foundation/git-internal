# Background and Problem Overview
Since its inception, Git has relied on **SHA-1** as its core hash algorithm for naming all objects (commits, trees, blobs, and tags). The 160-bit hash values are represented as 40-character hexadecimal strings and are deeply embedded in repository storage, index structures, network protocols, and a wide range of tooling.

However, practical collision attacks against **SHA-1** have already been demonstrated, and its security no longer meets long-term requirements. The Git community has outlined a migration path from **SHA-1** to **SHA-256** in the [_hash-function-transition_](https://git-scm.com/docs/hash-function-transition) document, and introduced experimental features such as `--object-format=sha256` in the 2.x series, with the plan to gradually adopt **SHA-256** as the default object format for newly created repositories in Git 3.0.

In the current technology stack, the low-level library **git-internal**, the Git client **Libra**, and the monolithic repository **Mega** all hard-code the **SHA-1** algorithm, which leads to a concrete requirement for supporting multiple hash algorithms.

Consequently, the current issues can be summarized as follows:

1. **git-internal** contains extensive **hard-coded SHA-1** logic (fixed 20-byte / 40-character hashes) and lacks a unified abstraction layer for hash functions.

2. **Libra** only implements **pack index v1**, and its protocol layer implicitly assumes “40-character hashes” during fetch/push discovery, making it incompatible with **SHA-256** and unable to negotiate multiple hash algorithms.

3. As a monolithic repository, **Mega** historically treats its hash strategy as an implicit constant. The chosen hash algorithm is not explicitly expressed in the context, and there is no clear “initialize once, use globally” policy for hash configuration.

# Project Objectives
The overall objective of this project is:
> To introduce systematic **SHA-256** support and a multi-hash mechanism for **git-internal**, **Libra**, and **Mega**—**without introducing major breaking changes to existing APIs**—thereby laying an extensible foundation for the Git 3.0 era.

Concretely, the goals can be decomposed as follows:

**git-internal Layer**
   - Evolve from “hard-coded **SHA-1**” to a unified hash abstraction that supports at least **SHA-1** and **SHA-256**, including:

     - Abstracting `HashKind` (hash algorithm type) and `ObjectHash` (concrete hash value type);

     - Providing unified APIs for hash computation, encoding, and decoding;

     - Replacing low-level assumptions such as fixed `[u8; 20]` layouts.

**Libra Client Layer**
   - Enable **Libra** to become a Git client that **understands multiple hash algorithms and interoperates with both SHA-1 and SHA-256 repositories**:

     - Initialize `HashKind` in command entry points based on `core.objectFormat`, `extensions.objectFormat`, and `compatObjectFormat`;

     - Remove the implicit 40-character hash assumption in the protocol layer (fetch/push discovery), and instead parse hashes via `HashKind::hex_len()` together with `ObjectHash::from_str`;

     - Evolve the index side from `build_index_v1` to **pack index v3/v2**, supporting concurrent indexing of SHA-1 and SHA-256 and laying the groundwork for bidirectional mappings (sha1 <-> sha256).

**Mega Monolithic Repository Layer**
   - Make Mega’s hash policy explicit as **“a single `objectFormat` chosen at repository creation time”**:

     - Do not implement cross-algorithm conversion (no need for `compatObjectFormat`);

     - During application initialization, set `hash_kind` once via `AppContext`, and ensure that all subsequent business logic accesses hash information exclusively through the shared context.

**Compatibility and Evolution**

   - Preserve the ability to access existing **SHA-1** repositories;

   - Reserve extension points for potential future hash algorithms (such as BLAKE3), without over-engineering the design.

# Design Overview
## Current State of git-internal
In the current **git-internal** implementation, the hash algorithm is essentially hard-coded as **SHA-1 with a fixed 20-byte representation**. For example:
```rust
fn from_bytes(data: &[u8], hash: SHA1) -> Result<Self, GitError>
```
This leads to several issues:
1. **Algorithm Cannot Be Switched**

   All object IDs are statically bound to **SHA-1**. As soon as we want to support **SHA-256**, a large number of type signatures must be modified across the codebase.

2. **Lack of a Unified Abstraction Layer**

   Different core modules in the library (such as commits, blobs, indexes, and protocol-related components) each make their own assumptions about hash formats and handle them in ad-hoc ways, without a single unified entry point or abstraction.

3. **Difficult to Support “Multiple Algorithms Coexisting”**

   In the short term, a repository may indeed use only a single hash algorithm. However, according to Git upstream’s `hash-function-transition` design, Git needs to understand both **SHA-1** and **SHA-256** in certain scenarios (for example, maintaining bidirectional mapping tables between the two)
### Usage Scenarios: Inferable vs Non-Inferable Hash Types

Before designing the refactoring plan, we need to distinguish between two categories of usage scenarios, as this directly affects how the APIs should be designed.
#### 1. Non-Inferable Types — Must Be Explicitly Passed or Provided via Context (e.g., `commit.rs`)

```rust
pub fn new(
    tree_id: SHA1,
    parent_commit_ids: Vec<SHA1>,
    message: &str,
) -> Commit {
    let mut commit = Commit {
        id: SHA1::default(),
        // ...
    };
    let hash = SHA1::from_type_and_data(ObjectType::Commit, &commit.to_data().unwrap());
    commit.id = hash;
    commit
}```
Here, Commit::new has no knowledge of whether the _current repository_ is using **SHA-1** or **SHA-256**. All it knows is:
- It needs to construct a commit object;
    
- It needs to assign an ID to that commit.

If we simply replace SHA1 with an enum like HashKind or a generic ObjectHash, but still do not provide any contextual information, the function has **no way to decide which concrete hash algorithm to use**.
Possible approaches include:
1. Add a hash_kind: HashKind parameter to all relevant functions – but this would propagate a large number of additional parameters across the codebase and hurt API simplicity.
    
2. Introduce a “repository context” (such as a Repository object or a global configuration) that provides the currently active hash algorithm.

For the low-level **git-internal** library, a more reasonable solution is to introduce an accessible “current hash algorithm” state (see the global configuration approach discussed later), so that we avoid an explosion of function signature changes.
#### **2. Inferable Types — Can Be Deduced from Input Length (e.g.,**  **core.rs**)
```rust
async fn get_blob(
    &self,
    object_hash: &str,
) -> Result<crate::internal::object::blob::Blob, ProtocolError> {
    let data = self.get_object(object_hash).await?;
    let hash = SHA1::from_str(object_hash)
        .map_err(|e| ProtocolError::repository_error(format!("Invalid hash format: {}", e)))?;
    crate::internal::object::blob::Blob::from_bytes(&data, hash)
        .map_err(|e| ProtocolError::repository_error(format!("Failed to parse blob: {}", e)))
}
```
In scenarios like this, object_hash is a hexadecimal string, and we can infer the hash algorithm based on its length:
- Length 40 → treat it as **SHA-1**;
    
- Length 64 → treat it as **SHA-256**.

With this approach, the function does not need an additional “algorithm” parameter. Instead, it only needs to inspect the length of object_hash, then delegate to a unified ObjectHash::from_str implementation.

For this class of use cases, we can keep the existing API surface largely unchanged and encapsulate all branching logic inside hash.rs.

### Design Principles: Abstraction Inspired by the C Implementation
In Git’s [C implementation](https://github.com/git/git), there is a core structure:
```c
struct git_hash_algo {
    const char *name;       //  ("sha1", "sha256")
    uint32_t format_id;     // format ID
    size_t rawsz;           // hash_len (20 or 32)
    size_t hexsz;           // hex_len (40 or 64)
    size_t blksz;           // block_size

    void (*init_fn)(struct git_hash_ctx *);  
    void (*clone_fn)(...);                  
    void (*update_fn)(...);                  
    void (*final_fn)(...);                   
    void (*final_oid_fn)(...);              
};
```
The core idea behind this design can be summarized as:
- Use a **single struct to describe both “algorithm metadata” and “algorithm interface”**;
    
- Each algorithm provides its own git_hash_algo instance;
    
- The rest of the code accesses the active algorithm solely via a pointer like struct git_hash_algo *the_hash_algo.

In Rust, there are two typical abstraction approaches:
1. **Trait + dynamic dispatch** (e.g. dyn HashAlgo with Box)
    
2. **Enum +** **match** based static dispatch

Given that:
- The number of algorithms is small (currently only **SHA-1** and **SHA-256**, and likely only a few more in the future);
    
- We want to avoid virtual tables and heap allocations;
    
- The low-level library targets performance-sensitive scenarios, and match branches are easily optimized into constant-time paths by the compiler;

it is more appropriate to adopt a design based on **“a couple of enums plus helper functions”** rather than traits with dynamic dispatch.

### Hash Abstraction Design: `HashKind` and `ObjectHash`

Here, we deliberately separate **“algorithm kind”** from **“concrete hash value”** into two enums:
- `HashKind`
  - Only concerns *which* algorithm is currently selected;
  - Used for configuration, branching logic, retrieving length information, etc.

- `ObjectHash`
  - The structure that actually stores hash values, and the one most function parameters will use;
  - Can hold either a `[u8; 20]` or a `[u8; 32]` representation in a single type.

#### `HashKind`: Describing the Active Algorithm Type

```rust
#[derive(Debug, Clone, Copy, Default)]
pub enum HashKind {
    #[default]
    Sha1,
    Sha256,
}

impl HashKind {
    /// Returns the raw hash length in bytes (20 or 32).
    pub const fn size(&self) -> usize {
        match self {
            HashKind::Sha1 => 20,
            HashKind::Sha256 => 32,
        }
    }

    /// Returns the hex string length (40 or 64).
    pub const fn hex_len(&self) -> usize {
        match self {
            HashKind::Sha1 => 40,
            HashKind::Sha256 => 64,
        }
    }
}```
- **Single responsibility**: it only encodes “which algorithm is being used” and does not hold any actual hash bytes;
    
- Provides basic algorithm-related metadata (byte length / hex length) for other structures to consume;
    
- In most cases, it can serve as a configuration enum for “current repository settings”, initialized from configuration files or environment variables.

#### **ObjectHash: Enum Carrying Actual Object IDs**
```rust
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum ObjectHash {
    Sha1([u8; 20]),
    Sha256([u8; 32]),
}

impl Default for ObjectHash {
    fn default() -> Self {
        ObjectHash::Sha1([0u8; 20])
    }
}

impl ObjectHash {
    /// Returns the algorithm kind corresponding to this hash.
    pub fn kind(&self) -> HashKind {
        match self {
            ObjectHash::Sha1(_) => HashKind::Sha1,
            ObjectHash::Sha256(_) => HashKind::Sha256,
        }
    }

    /// Returns the hash length in bytes.
    pub fn size(&self) -> usize {
        self.kind().size()
    }

    /// Computes a hash for the given data based on the global configuration.
    pub fn new(data: &[u8]) -> ObjectHash {
        match get_hash_kind() {
            HashKind::Sha1 => {
                let h = sha1::Sha1::digest(data);
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(h.as_ref());
                ObjectHash::Sha1(bytes)
            }
            HashKind::Sha256 => {
                let h = sha2::Sha256::digest(data);
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(h.as_ref());
                ObjectHash::Sha256(bytes)
            }
        }
    }

    /// Parses a hash from a hex string (auto-detecting the algorithm by length).
    pub fn from_hex(s: &str) -> Result<Self, GitError> {
        // Pseudocode: branch on length + decode
        // if len == 40 => Sha1, if len == 64 => Sha256, otherwise return error
        #![allow(unused)]
        unimplemented!()
    }

    /// Converts the hash into a hex string.
    pub fn to_hex(&self) -> String {
        // Pseudocode: encode the internal byte array
        #![allow(unused)]
        unimplemented!()
    }
}
```
**Benefits of this design**
1. **Clear type semantics**
    
    - Seeing HashKind immediately signals that it does not contain an actual hash, but only represents a policy choice;
        
    - Seeing ObjectHash makes it clear that this is “the real hash value” that can be attached to commits, trees, blobs, and other objects.
    
2. **Supports coexistence of multiple algorithms**
    
    The same process can simultaneously hold ObjectHash::Sha1 and ObjectHash::Sha256 values—for example, when reading objects of different formats from remote repositories. This is aligned with upstream Git’s design for maintaining **bidirectional mapping tables** between SHA-1 and SHA-256.
    
3. **Zero or near-zero runtime overhead**
    
    The compiler can efficiently optimize match statements, avoiding virtual tables and dynamic dispatch. As a result, this abstraction is suitable for performance-sensitive low-level code.
### Global Configuration: Selecting the Algorithm Without Changing APIs

In order to let low-level code know *“which hash algorithm the current repository is using”* **without** adding a `hash_kind: HashKind` parameter everywhere, we can follow upstream Git’s approach of using a global pointer and introduce a **global configuration / thread-local configuration** in Rust. For example:

```rust
/// Sets the hash kind for the current thread.
pub fn set_hash_kind(kind: HashKind) {
    HASH_KIND.with(|h| {
        *h.borrow_mut() = kind;
    });
}

/// Retrieves the hash kind for the current thread.
pub fn get_hash_kind() -> HashKind {
    HASH_KIND.with(|h| *h.borrow())
}```
The call flow is as follows:

1. Upper layers (for example Repository::open or a CLI command entry point) parse .git/config or other configuration sources:
    
    - If extensions.objectFormat = sha256, then set HashKind::Sha256;
        
    - Otherwise, fall back to the default HashKind::Sha1.
        
    
2. The chosen value is written into the global / contextual state.
    
3. All functions like ObjectHash::new(data) then only depend on this **“current global algorithm”**, and do not require an extra parameter.

This approach satisfies the requirement of **“keeping the APIs as stable as possible”**, while ensuring that all low-level components **read the active hash algorithm from a single, unified source**.

### Concrete Refactoring Suggestions for git-internal Core Modules

#### Index Writing Logic (`git-internal/src/internal/index.rs`)

```rust
pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), GitError> {
    // ...
    let mut hash = Sha1::new();
    // ...
    entry_bytes.write_all(&entry.hash.0)?;
    // ...
    let padding = 8 - ((22 + entry.name.len()) % 8);
    entry_bytes.write_all(&vec![0; padding])?;
    file.write_all(&entry_bytes)?;
    hash.update(&entry_bytes);

    // Extensions
    // check sum
    let file_hash: [u8; 20] = hash.finalize().into();
    file.write_all(&file_hash)?;
    Ok(())
}```
1. **Streaming hash computation depends on the concrete algorithm type**
    
    - The current implementation hard-codes Sha1::new(). This needs to be changed to select either SHA-1 or SHA-256 based on get_hash_kind(), so that the streaming hash computation respects the active algorithm.
        
    
2. **Hash-length-dependent padding and trailer size**
    
    - The expression padding = 8 - ((22 + entry.name.len()) % 8) currently bakes in assumptions about the fixed layout, such as “20-byte hash + 2-byte flags”, which only holds for SHA-1. This logic must be adjusted so that it works correctly when the hash length changes for SHA-256.
        
    - The final checksum file_hash: [u8; 20] must also be updated to derive its size dynamically from HashKind::size(), rather than assuming a fixed 20-byte hash.
#### Object Modules (`src/internal/object/blob.rs`, etc.)

```rust
fn from_bytes(data: &[u8], hash: SHA1) -> Result<Self, GitError>
where
    Self: Sized,
{
    Ok(Blob {
        id: hash,
        data: data.to_vec(),
    })
}```
For functions of this kind, the logic itself is very simple: they simply assign the hash to a field in the struct. The refactoring approach is straightforward:

- Replace the parameter type **SHA-1** with ObjectHash;
    
- Keep the rest of the logic almost unchanged;
    
- For certain helper functions that are only used internally, we may optionally add additional where-constraints (for example, enforcing that ObjectHash matches a specific encoding requirement).

These changes are essentially **type-level substitutions**. While the risk is relatively low for each individual call site, the impact is broad: the refactor requires a systematic sweep over all places that hard-code **SHA-1** as a type.

## ## Impact Analysis on Libra

#### 1. Global Initialization of `HashKind` and Refactoring of Command Entry Points
The official `hash-function-transition` documentation provides a typical configuration for a SHA-256 repository:
```ini
[core]
    repositoryFormatVersion = 1
[extensions]
    objectFormat       = sha256
    compatObjectFormat = sha1
```
In Libra, we should introduce a unified initialization routine at the command entry layer:
- Read the current repository’s core.repositoryFormatVersion, extensions.objectFormat, and extensions.compatObjectFormat;
    
- Set the global HashKind in **git-internal** based on objectFormat;
    
- Propagate compatObjectFormat (used for cross-algorithm compatibility) down into lower layers, where it can later be leveraged to build an ObjectIdMap (for example, backed by a dedicated SQLite table).

**Additional Note:**
Any code that **generates or parses hashes** must only run _after_ this initialization step has completed. This logic should be wrapped in a helper function (e.g., an initialization guard) to avoid duplication.

In the initial refactor, we only introduced a single global variable in **git-internal** to represent the active objectFormat. However, to support compatibility between different hash algorithms, we now need to extend this design and introduce additional state (e.g., a separate variable to represent compatObjectFormat), as will be discussed in later sections.
#### 2. Support for Pack Index v3 and `ObjectIdMap`
According to Git’s official [hash-function-transition](https://git-scm.com/docs/hash-function-transition) documentation, the pack index v3 (`.idx`) format for multiple hash functions introduces an explicit header that describes the index version, object count, and the number of supported hash algorithms:
```ini
- 4 bytes: pack index signature `\377t0c`
- 4 bytes: version number (e.g., `3`)
- 4 bytes: header size (including signature and version)
- 4 bytes: “number of objects in the pack”
- 4 bytes: “number of object formats (hash functions) in this index” (for example, `2` if both SHA-1 and SHA-256 are present)
```
The header is followed by per-algorithm tables for object IDs, offsets, and related structures. Each hash function gets its own set of tables, allowing **the same object to be referenced by multiple hash algorithms within a single index**. The core purpose is to let **one `.idx` file store names for multiple hash algorithms**, providing a **standard on-disk structure for algorithm migration and compatibility**.

However, there is currently no readily available reference implementation of v3 in our codebase. We need to implement it manually based on the documentation, or first finish v1/v2 support and then extend it to v3.

In the **Libra** repository, only a single-hash implementation exists today (`libra/src/command/index_pack.rs`):

```rust
/// Build index file for pack file, version 1
///
/// [pack-format](https://git-scm.com/docs/pack-format)
pub fn build_index_v1(pack_file: &str, index_file: &str) -> Result<(), GitError> {
    let pack_path = PathBuf::from(pack_file);
    let tmp_path = pack_path.parent().unwrap();
    // ...
}```
Once Git 3.0 introduces **SHA-256** and cross-algorithm compatibility, **Libra must evolve to support pack index v3** in order to manage **both SHA-1 and SHA-256 object names in a single index file**.

Within **Libra** itself (since pack indexing and object rewriting logic currently live inside Libra), the following evolutions are required:
1. Add a build_index_v3 implementation inside Libra:
    
    - Emit the v3 header (signature, version, object count, number of hash algorithms);
        
    - For the hash algorithm indicated by the global configuration, as well as for the compatible algorithm (e.g., SHA-1 and SHA-256), maintain a dedicated set of tables for each, as specified in the [pack index](https://git-scm.com/docs/hash-function-transition)![Attachment.tiff](file:///Attachment.tiff) documentation;
        
    - Ensure that, for each object in the pack, **both algorithm-specific IDs** are recorded in the index.
        
2. Introduce an ObjectIdMap abstraction so that the **Libra** process can efficiently perform bidirectional conversions:
```rust
struct ObjectIdMap {
    by_sha1:   HashMap<ObjectHash, ObjectHash>, // sha1   -> sha256
    by_sha256: HashMap<ObjectHash, ObjectHash>, // sha256 -> sha1
}
```
The contents of this map can be populated from parsed index data or queried from the database (since **Libra** replaces some of the original internal storage with SQLite). In-memory maps should ideally only hold mappings relevant to the current operation, while the full mapping set should be persisted in the database.
#### 3. Protocol Layer: Fetch as an Example
Consider the following scenario:

> Local **Libra** repository: `objectFormat = sha256`, `compatObjectFormat = sha1`  
> (primary storage algorithm is SHA-256, with SHA-1 as a compatibility format)  
> Remote Git server: legacy SHA-1 repository that only knows 40-character SHA-1 IDs.

In the current architecture, **Libra implements `index-pack` and related object-processing logic by itself**, so the following steps all happen inside **Libra**, without depending on external **git-internal** helpers. Precisely because of this, **git-internal** can remain relatively minimal and only provide the basic primitives.
##### 1. Session Initialization: Determining Three `HashKind`s
When a `fetch` command starts, Libra needs to:

1. Open the local repository and read `.git/config`:

   - Parse `extensions.objectFormat` and set `PrimaryHashKind` (used for local object storage and ID computation);
   - Parse `extensions.compatObjectFormat` and set `CompatHashKind` (if present, this describes the hash type stored in the translation table).

2. Based on the remote capability negotiation, determine the **`WireHashKind` (the hash type used on the wire for transmission and parsing)**:

   - Legacy servers that only support **SHA-1** → `WireHashKind = Sha1`;
   - In the future, servers may support **SHA-256**, in which case `WireHashKind` might also be `Sha256`.

In most transition scenarios, the local repository is **SHA-256**-based, while the remote is still **SHA-1**-only. In that case we typically have:

- `PrimaryHashKind  = Sha256`  
- `CompatHashKind   = Some(Sha1)`  
- `WireHashKind     = Sha1`

##### 2. Protocol Phase
In the current implementation, Libra’s `internal/protocol` code parses remote references by slicing fixed-length ID segments, which obviously only works for **SHA-1**.

Once **SHA-256** is supported, we must:

1. Use `WireHashKind::hex_len()` to determine the length of IDs on the wire, instead of hard-coding `40`;
2. Serialize all `have` / `want` lines according to `WireHashKind`, ensuring that on-the-wire IDs are fully compatible with the remote’s expectations;
3. Whenever Libra needs to map a remote ID to a local object, it should go through `ObjectIdMap` and dedicated conversion helpers.

##### 3. Pack-Processing Phase
According to Git’s [hash-function-transition](https://git-scm.com/docs/hash-function-transition), when the local repository is **SHA-256** and the remote repository is **SHA-1**, the key steps during `fetch` are:
1. **Receive a SHA-1 Pack**

   - Libra receives a “pure SHA-1” pack from the server in the traditional way, where all object IDs, delta references, etc. are expressed in **SHA-1**.

2. **Run `index-pack` and Perform Topological Sorting (Inside Libra)**

   - In `libra/src/command/index_pack.rs` or related modules:
     - Run `index-pack` over the SHA-1 pack to obtain all objects and their **SHA-1** IDs;
     - Perform a **topological sort** over commits / trees / tags so that “referenced objects come before their referrers”;
     - `blob` objects can be handled separately, since they do not reference other objects.

3. **Rewrite Object Contents into SHA-256 Form**

   In the current Libra architecture, this step also happens inside Libra itself (not in git-internal):

   - For **blob** objects:
     - The “SHA-1 content” and “SHA-256 content” are **byte-for-byte identical**; only the hash function differs;
     - Libra can compute both SHA-1 and SHA-256 over the same object body and store the mapping in the translation table.

   - For **tree / commit / tag** objects:
     - First obtain the “SHA-1 content”, which includes **SHA-1** references to other objects;
     - Use `ObjectIdMap` to replace all internal **SHA-1** references with their corresponding **SHA-256** IDs, thereby constructing the **SHA-256** version of the content;
     - Prepend the appropriate object header to this SHA-256 content and compute the **SHA-256** object ID;
     - Write the rewritten object into a new local **SHA-256** pack;
     - At the same time, record the mapping `sha1 <-> sha256` both in the v3 index and in the in-memory `ObjectIdMap`.

4. **Write Pack Index v3 and `loose-object-idx`**

   - After all objects have been processed, Libra invokes its own `build_index_v3` implementation to:
     - Build fan-out / ID / offset tables for both the primary and compatibility algorithms;
     - Write the `.idx` file and update the `loose-object-idx` index for unpacked (loose) objects.

5. **Clean Up the Temporary SHA-1 Pack**

   - The **SHA-1** pack is only an intermediate format and can be removed once the rewrite process is complete;
   - Locally, we only keep the **SHA-256** pack, the v3 index, and the translation table.
## Mega: Fixed Hash Policy and Context Design for a Monolithic Repository
The situation for **Mega** is different from **Libra**:
- It is a **monolithic repository**, and can internally adopt a single hash algorithm (preferably **SHA-256**) in a uniform way;
- It does not need to interoperate with **SHA-1** repositories, nor act as a “general-purpose Git client”;
- Therefore, a simpler and more explicit strategy is acceptable and even preferable.

Under these conditions, repeating Libra’s pattern of “reading configuration and propagating `HashKind` everywhere” becomes unnecessarily heavy. Typical issues include:

- Re-reading configuration in every low-level operation (writing objects, computing diffs, creating commits);
- Polluting API signatures with extra `hash_kind: HashKind` parameters, increasing cognitive load;
- In multi-threaded scenarios, repeatedly accessing configuration can introduce additional locking and runtime overhead.

### One-Time `hash_kind` Initialization in `AppContext`
A more appropriate approach for **Mega** is to introduce a **global application context** (`Context`) that is initialized only once when the application or worker thread starts. In the `context` module, there is already a struct:

```rust
pub struct AppContext {
    pub storage: jupiter::storage::Storage,
    pub vault: vault::integration::vault_core::VaultCore,
    pub config: Arc<common::config::Config>,
}```
We propose refactoring it as follows:
- In AppContext::new (or the main initialization entry point):
    
    - Read objectFormat from configuration or repository metadata (defaulting to sha256);
        
    - Call git-internal’s set_hash_kind(HashKind::Sha256), and store the chosen hash_kind as a field inside AppContext.

The conventions are:
- Business logic must **not** independently read configuration files to determine the hash algorithm;
    
- All hash-related logic obtains the algorithm either from ctx.hash_kind or via the global get_hash_kind() provided by **git-internal**;
    
- The context is shared across threads as Arc< AppContext> and treated as immutable.
# Project Roadmap

The project can be advanced in the following phases:

1. **Phase 1: Refactoring Abstractions in git-internal**
   - Implement the `HashKind` / `ObjectHash` abstraction;
   - Introduce global / thread-local `set_hash_kind` / `get_hash_kind`;
   - Refactor core modules (objects, index, protocol primitives) to remove `[u8; 20]` and hard-coded 40-byte assumptions.

2. **Phase 2: Baseline Adaptation in Libra**
   - Initialize `HashKind` in a unified way at all command entry points;
   - Refactor protocol parsing based on `hex_len` and `ObjectHash::from_str`;
   - Verify that interactions with existing SHA-1 repositories behave correctly.

3. **Phase 3: Pack Index v3 and Compatibility Support**
   - Implement pack index v3 in Libra;
   - Introduce `ObjectIdMap` and enable SHA-1 ↔ SHA-256 mapping end to end;
   - Complete cross-algorithm `fetch` / `push` flows within Libra.

4. **Phase 4: Context Refactoring in Mega**
   - Extend `AppContext` to include `hash_kind` and initialize it once during startup;
   - Remove all direct “hash algorithm detection” logic inside Mega and route everything through the shared context;
   - Validate performance and behavioral stability under a “single SHA-256 strategy”.

# Expected Benefits

1. **Improved Security**
   - Add support for SHA-256, eliminating practical SHA-1 collision risks and aligning with the evolution direction of Git 3.0.

2. **Enhanced Architectural Evolvability**
   - By introducing the `HashKind` / `ObjectHash` abstraction, the cost of adding new hash algorithms in the future is constrained to a manageable scope.

3. **Clearer Separation of Responsibilities**
   - **git-internal**: provides low-level multi-hash abstractions and indexing capabilities;
   - **Libra**: implements multi-hash interoperability as a general-purpose Git client;
   - **Mega**: chooses a hash policy once at the business boundary and keeps internal logic simple and stable.

4. **Alignment with Upstream Community**
   - Designs such as pack index v3 and `compatObjectFormat` remain compatible with upstream Git, laying the groundwork for integrating additional community tools and future protocol extensions.

# References

- [Git 2.45: Preliminary Support for SHA-1 and SHA-256 Interoperability](https://github.blog/open-source/git/highlights-from-git-2-45/#preliminary-support-for-sha-1-and-sha-256-interoperability)
- [hash-function-transition](https://git-scm.com/docs/hash-function-transition)  
- [git-init](https://git-scm.com/docs/git-init/2.38.0)  
- [BreakingChanges](https://git-scm.com/docs/BreakingChanges)  
- [Git Source](https://github.com/git/git)  
- [gitoxide](https://github.com/GitoxideLabs/gitoxide)  
- [libgit2: SHA-256 Discussion](https://github.com/libgit2/libgit2/discussions/5840)  
- [pack-format](https://git-scm.com/docs/pack-format)  
- [Why Google Stores Billions of Lines of Code in a Single Repository](https://cacm.acm.org/research/why-google-stores-billions-of-lines-of-code-in-a-single-repository/)  
- [protocol capabilities](https://git-scm.com/docs/protocol-capabilities)  
- [protocol v2](https://git-scm.com/docs/protocol-v2)
