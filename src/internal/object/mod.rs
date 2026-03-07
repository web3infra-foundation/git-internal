//! Object model definitions for Git blobs, trees, commits, tags, and
//! AI workflow objects.
//!
//! This module is the storage-layer contract for `git-internal`.
//! Git-native objects (`Blob`, `Tree`, `Commit`, `Tag`) model repository
//! content, while the AI objects model immutable workflow history that
//! Libra orchestrates on top.
//!
//! # How Libra should use this module
//!
//! Libra should treat every AI object here as an immutable record:
//!
//! - construct the object in memory,
//! - populate optional fields before persistence,
//! - persist it once,
//! - derive current state later from object history plus Libra
//!   projections.
//!
//! Libra should not store scheduler state, selected heads, active UI
//! focus, or query caches in these objects. Those belong to Libra's own
//! runtime and index layer.
//!
//! AI workflow objects are split into three layers:
//!
//! - **Snapshot objects** in `git-internal` answer "what was the stored
//!   fact at this revision?"
//! - **Event objects** in `git-internal` answer "what happened later?"
//! - **Libra projections** answer "what is the system's current view?"
//!
//! # Relationship Design Standard
//!
//! Relationship fields follow a simple storage rule:
//!
//! - Store the canonical ownership edge on the child object when the
//!   relationship is a historical fact.
//! - Low-frequency, strongly aggregated relationships that benefit
//!   from fast parent-to-children traversal may additionally keep a
//!   reverse convenience link.
//! - High-frequency, high-cardinality, event-stream relationships
//!   should remain single-directional to avoid turning parent objects
//!   into rewrite hotspots.
//!
//! # Three-Layer Design
//!
//! ```text
//! +------------------------------------------------------------------+
//! | Libra projection / runtime                                       |
//! |------------------------------------------------------------------|
//! | thread heads / selected_plan_id / active_run / scheduler state   |
//! | live context window / UI focus / query indexes                   |
//! +--------------------------------+---------------------------------+
//!                                  |
//!                                  v
//! +------------------------------------------------------------------+
//! | git-internal event objects                                        |
//! |------------------------------------------------------------------|
//! | IntentEvent / TaskEvent / RunEvent / PlanStepEvent / RunUsage    |
//! | ToolInvocation / Evidence / Decision / ContextFrame              |
//! +--------------------------------+---------------------------------+
//!                                  |
//!                                  v
//! +------------------------------------------------------------------+
//! | git-internal snapshot objects                                     |
//! |------------------------------------------------------------------|
//! | Intent / Plan / Task / Run / PatchSet / ContextSnapshot          |
//! | Provenance                                                       |
//! +------------------------------------------------------------------+
//! ```
//!
//! # Main Object Relationships
//!
//! ```text
//! Snapshot layer
//! ==============
//!
//! Intent --parents----------------------------> Intent
//! Intent --analysis_context_frames-----------> ContextFrame
//! Plan   --intent-----------------------------> Intent
//! Plan   --context_frames---------------------> ContextFrame
//! Plan   --parents----------------------------> Plan
//! Task   --intent?----------------------------> Intent
//! Task   --parent?----------------------------> Task
//! Task   --origin_step_id?-------------------> PlanStep.step_id
//! Run    --task-------------------------------> Task
//! Run    --plan?------------------------------> Plan
//! Run    --snapshot?--------------------------> ContextSnapshot
//! PatchSet   --run----------------------------> Run
//! Provenance --run_id-------------------------> Run
//!
//! Event layer
//! ===========
//!
//! IntentEvent   --intent_id-------------------> Intent
//! IntentEvent   --next_intent_id?-------------> Intent
//! ContextFrame  --intent_id?------------------> Intent
//! TaskEvent     --task_id---------------------> Task
//! RunEvent      --run_id----------------------> Run
//! RunUsage      --run_id----------------------> Run
//! PlanStepEvent --plan_id + step_id + run_id-> Plan / Run / PlanStep
//! ToolInvocation--run_id----------------------> Run
//! Evidence      --run_id / patchset_id?-------> Run / PatchSet
//! Decision      --run_id / chosen_patchset_id?> Run / PatchSet
//! ContextFrame  --run_id? / plan_id? / step_id?> Run / Plan / PlanStep
//! ```
//!
//! # Libra read / write pattern
//!
//! A typical Libra call flow looks like this:
//!
//! 1. write snapshot objects when a new immutable revision is defined
//!    (`Intent`, `Plan`, `Task`, `Run`, `PatchSet`, `ContextSnapshot`,
//!    `Provenance`);
//! 2. append event objects as execution progresses
//!    (`IntentEvent`, `TaskEvent`, `RunEvent`, `PlanStepEvent`,
//!    `RunUsage`, `ToolInvocation`, `Evidence`, `Decision`,
//!    `ContextFrame`);
//! 3. rebuild current state in Libra from those immutable objects plus
//!    its own `Thread`, `Scheduler`, `UI`, and `Query Index`
//!    projections.
//!
//! ## Object Relationship Summary
//!
//! | From | Field | To | Cardinality |
//! |------|-------|----|-------------|
//! | Intent | `parents` | Intent | 0..N |
//! | Intent | `analysis_context_frames` | ContextFrame | 0..N |
//! | Plan | `intent` | Intent | 1 canonical |
//! | Plan | `parents` | Plan | 0..N |
//! | Plan | `context_frames` | ContextFrame | 0..N |
//! | Task | `parent` | Task | 0..1 |
//! | Task | `intent` | Intent | 0..1 |
//! | Task | `origin_step_id` | PlanStep.step_id | 0..1 |
//! | Task | `dependencies` | Task | 0..N |
//! | Run | `task` | Task | 1 |
//! | Run | `plan` | Plan | 0..1 |
//! | Run | `snapshot` | ContextSnapshot | 0..1 |
//! | PatchSet | `run` | Run | 1 |
//! | Provenance | `run_id` | Run | 1 |
//! | IntentEvent | `intent_id` | Intent | 1 |
//! | IntentEvent | `next_intent_id` | Intent | 0..1 recommended follow-up |
//! | ContextFrame | `intent_id` | Intent | 0..1 |
//! | TaskEvent | `task_id` | Task | 1 |
//! | RunEvent | `run_id` | Run | 1 |
//! | RunUsage | `run_id` | Run | 1 |
//! | PlanStepEvent | `plan_id` | Plan | 1 |
//! | PlanStepEvent | `step_id` | PlanStep.step_id | 1 |
//! | PlanStepEvent | `run_id` | Run | 1 |
//! | ToolInvocation | `run_id` | Run | 1 |
//! | Evidence | `run_id` | Run | 1 |
//! | Evidence | `patchset_id` | PatchSet | 0..1 |
//! | Decision | `run_id` | Run | 1 |
//! | Decision | `chosen_patchset_id` | PatchSet | 0..1 |
//! | ContextFrame | `run_id` | Run | 0..1 |
//! | ContextFrame | `plan_id` | Plan | 0..1 |
//! | ContextFrame | `step_id` | PlanStep.step_id | 0..1 |
//!
pub mod blob;
pub mod commit;
pub mod context;
pub mod context_frame;
pub mod decision;
pub mod evidence;
pub mod integrity;
pub mod intent;
pub mod intent_event;
pub mod note;
pub mod patchset;
pub mod plan;
pub mod plan_step_event;
pub mod provenance;
pub mod run;
pub mod run_event;
pub mod run_usage;
pub mod signature;
pub mod tag;
pub mod task;
pub mod task_event;
pub mod tool;
pub mod tree;
pub mod types;
pub mod utils;

use std::{
    fmt::Display,
    io::{BufRead, Read},
};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{object::types::ObjectType, zlib::stream::inflate::ReadBoxed},
};

/// **The Object Trait**
/// Defines the common interface for all Git object types, including blobs, trees, commits, and tags.
pub trait ObjectTrait: Send + Sync + Display {
    /// Creates a new object from a byte slice.
    fn from_bytes(data: &[u8], hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized;

    /// Generate a new Object from a `ReadBoxed<BufRead>`.
    /// the input size,is only for new a vec with directive space allocation
    /// the input data stream and output object should be plain base object .
    fn from_buf_read<R: BufRead>(read: &mut ReadBoxed<R>, size: usize) -> Self
    where
        Self: Sized,
    {
        let mut content: Vec<u8> = Vec::with_capacity(size);
        read.read_to_end(&mut content).unwrap();
        let digest = read.hash.clone().finalize();
        let hash = ObjectHash::from_bytes(&digest).unwrap();
        Self::from_bytes(&content, hash).unwrap()
    }

    /// Returns the type of the object.
    fn get_type(&self) -> ObjectType;

    fn get_size(&self) -> usize;

    fn to_data(&self) -> Result<Vec<u8>, GitError>;

    fn object_hash(&self) -> Result<ObjectHash, GitError> {
        let data = self.to_data()?;
        Ok(ObjectHash::from_type_and_data(self.get_type(), &data))
    }
}
