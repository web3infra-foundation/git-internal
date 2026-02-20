//! Dynamic Context Pipeline
//!
//! A [`ContextPipeline`] solves the context-forgetting problem in
//! long-running AI tasks. Instead of relying solely on a static
//! [`ContextSnapshot`](super::context::ContextSnapshot) captured at
//! Run start, a ContextPipeline accumulates incremental
//! [`ContextFrame`]s throughout the workflow.
//!
//! # Position in Lifecycle
//!
//! ```text
//!  ②  Intent (Active)         ← content analyzed
//!       │
//!       ▼
//!      ContextPipeline created ← seeded with IntentAnalysis frame
//!       │
//!       ▼
//!  ③  Plan (Plan.pipeline → Pipeline, Plan.fwindow = visible range)
//!       │  steps execute
//!       ▼
//!      Frames accumulate       ← StepSummary, CodeChange, ToolCall, ...
//!       │
//!       ▼
//!      Replan? → new Plan with updated fwindow
//! ```
//!
//! The pipeline is created *after* an Intent's content is analyzed
//! (step ②) but *before* a Plan exists. The initial
//! [`IntentAnalysis`](FrameKind::IntentAnalysis) frame captures the
//! AI's structured interpretation, which serves as the foundation
//! for Plan creation. The [`Plan`](super::plan::Plan) then references
//! this pipeline via `pipeline` and records the visible frame range
//! via `fwindow`. During execution, frames accumulate to track
//! step-by-step progress.
//!
//! # Relationship to Other Objects
//!
//! ```text
//! Intent ──plan──→ Plan ──pipeline──→ ContextPipeline
//!                   │                        │
//!              [PlanStep₀, ...]   [IntentAnalysis, StepSummary, ...]
//!                   │                        ▲
//!              iframes/oframes ──────────────┘
//! ```
//!
//! | From | Field | To | Notes |
//! |------|-------|----|-------|
//! | Plan | `pipeline` | ContextPipeline | 0..1 |
//! | PlanStep | `iframes` | ContextFrame indices | consumed context |
//! | PlanStep | `oframes` | ContextFrame indices | produced context |
//!
//! The pipeline itself has no back-references — it is a passive
//! container. [`PlanStep`](super::plan::PlanStep)s own the
//! association via `iframes` and `oframes`.
//!
//! # Eviction
//!
//! When `max_frames > 0` and the limit is exceeded, the oldest
//! evictable frame is removed. `IntentAnalysis` and `Checkpoint`
//! frames are **protected** from eviction — they always survive.
//!
//! # Purpose
//!
//! - **Context Continuity**: Maintains a rolling window of high-value
//!   context for the agent's working memory across Plan steps.
//! - **Incremental Updates**: Unlike the static ContextSnapshot, the
//!   pipeline grows as work progresses, capturing step summaries,
//!   code changes, and tool results.
//! - **Bounded Memory**: `max_frames` + eviction ensures the pipeline
//!   doesn't grow unboundedly in long-running workflows.
//! - **Replan Support**: When replanning occurs, a new Plan can
//!   reference the same pipeline with an updated `fwindow` that
//!   includes frames accumulated since the previous plan.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::object::{
        ObjectTrait,
        types::{ActorRef, Header, ObjectType},
    },
};

/// The kind of context captured in a [`ContextFrame`].
///
/// Determines how the frame's `summary` and `data` should be
/// interpreted. `IntentAnalysis` and `Checkpoint` are protected
/// from eviction when `max_frames` is exceeded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    /// Initial context derived from an Intent's analyzed content.
    ///
    /// Created when the AI fills in the `content` field on an Intent,
    /// serving as the foundation for subsequent Plan creation. This
    /// is the **seed frame** — always the first frame in a pipeline.
    /// **Protected from eviction.**
    IntentAnalysis,
    /// Summary produced after a [`PlanStep`](super::plan::PlanStep)
    /// completes. Captures what the step accomplished so that
    /// subsequent steps have context.
    StepSummary,
    /// Code change digest (e.g. files modified, diff stats).
    /// Typically produced alongside a
    /// [`PatchSet`](super::patchset::PatchSet).
    CodeChange,
    /// System or environment state snapshot (e.g. memory usage,
    /// disk space, running services).
    SystemState,
    /// Context captured during error recovery. Records what went
    /// wrong and what corrective action was taken, so that subsequent
    /// steps don't repeat the same mistakes.
    ErrorRecovery,
    /// Explicit save-point created by user or system.
    /// **Protected from eviction.** Used for long-running workflows
    /// where the agent may be paused and resumed.
    Checkpoint,
    /// Result of an external tool invocation (MCP service, function
    /// call, REST API, CLI command, etc.).
    ///
    /// Intentionally protocol-agnostic: MCP is one transport for
    /// tool calls, but agents may also invoke tools via direct
    /// function calls, HTTP APIs, or shell commands. Protocol-specific
    /// details (server name, tool name, arguments, result preview)
    /// belong in `ContextFrame.data`.
    ToolCall,
    /// Application-defined context type not covered by the variants
    /// above.
    Other(String),
}

impl FrameKind {
    pub fn as_str(&self) -> &str {
        match self {
            FrameKind::IntentAnalysis => "intent_analysis",
            FrameKind::StepSummary => "step_summary",
            FrameKind::CodeChange => "code_change",
            FrameKind::SystemState => "system_state",
            FrameKind::ErrorRecovery => "error_recovery",
            FrameKind::Checkpoint => "checkpoint",
            FrameKind::ToolCall => "tool_call",
            FrameKind::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single context frame — a compact summary captured at a point in
/// time during the AI workflow.
///
/// Frames are **passive data records**. They carry no back-references
/// to the [`PlanStep`](super::plan::PlanStep) that consumed or produced
/// them; that association is tracked on the step side via `iframes`
/// and `oframes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextFrame {
    /// The kind of context this frame captures.
    ///
    /// Determines how `summary` and `data` should be interpreted.
    /// Also affects eviction: `IntentAnalysis` and `Checkpoint`
    /// frames are protected.
    kind: FrameKind,
    /// Compact human-readable summary of this frame's content.
    ///
    /// Should be concise (a few sentences). For example:
    /// - IntentAnalysis: "Add pagination to GET /users with limit/offset"
    /// - StepSummary: "Refactored auth module, 3 files changed"
    /// - CodeChange: "Modified src/api.rs (+42 -15)"
    summary: String,
    /// Structured data payload for machine consumption.
    ///
    /// Schema depends on `kind`. For example:
    /// - CodeChange: `{"files": ["src/api.rs"], "insertions": 42, "deletions": 15}`
    /// - ToolCall: `{"tool": "search", "args": {...}, "result_preview": "..."}`
    ///
    /// `None` when the `summary` is sufficient and no structured
    /// data is needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    /// UTC timestamp of when this frame was created.
    ///
    /// Automatically set to `Utc::now()` by [`ContextFrame::new`].
    /// Frames within a pipeline are chronologically ordered.
    created_at: DateTime<Utc>,
    /// Estimated token count for context-window budgeting.
    ///
    /// Used by the orchestrator to decide how many frames fit in
    /// the LLM's context window. `None` when the estimate hasn't
    /// been computed. See
    /// [`ContextPipeline::total_token_estimate`] for aggregation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_estimate: Option<u64>,
}

impl ContextFrame {
    /// Create a new frame with the given kind and summary.
    pub fn new(kind: FrameKind, summary: impl Into<String>) -> Self {
        Self {
            kind,
            summary: summary.into(),
            data: None,
            created_at: Utc::now(),
            token_estimate: None,
        }
    }

    pub fn kind(&self) -> &FrameKind {
        &self.kind
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn data(&self) -> Option<&serde_json::Value> {
        self.data.as_ref()
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn token_estimate(&self) -> Option<u64> {
        self.token_estimate
    }

    pub fn set_data(&mut self, data: Option<serde_json::Value>) {
        self.data = data;
    }

    pub fn set_token_estimate(&mut self, token_estimate: Option<u64>) {
        self.token_estimate = token_estimate;
    }
}

/// A dynamic context pipeline that accumulates
/// [`ContextFrame`]s throughout an AI workflow.
///
/// Created when an [`Intent`](super::intent::Intent)'s content is
/// first analyzed, seeded with an
/// [`IntentAnalysis`](FrameKind::IntentAnalysis) frame. The
/// [`Plan`](super::plan::Plan) references this pipeline via
/// `pipeline` as its context basis. See module documentation for
/// lifecycle position, eviction rules, and purpose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPipeline {
    /// Common header (object ID, type, timestamps, creator, etc.).
    #[serde(flatten)]
    header: Header,
    /// Chronologically ordered context frames.
    ///
    /// New frames are appended via [`push_frame`](ContextPipeline::push_frame).
    /// If `max_frames > 0` and the limit is exceeded, the oldest
    /// evictable frame is removed (see eviction rules in module docs).
    /// [`PlanStep`](super::plan::PlanStep)s reference frames by index
    /// via `iframes` and `oframes`.
    #[serde(default)]
    frames: Vec<ContextFrame>,
    /// Maximum number of active frames before eviction kicks in.
    ///
    /// `0` means unlimited (no eviction). When the frame count
    /// exceeds this limit, the oldest non-protected frame is removed.
    /// `IntentAnalysis` and `Checkpoint` frames are protected and
    /// never evicted.
    #[serde(default)]
    max_frames: u32,
    /// Aggregated human-readable summary across all frames.
    ///
    /// Maintained by the orchestrator as a high-level overview of
    /// the pipeline's accumulated context. Useful for quickly
    /// understanding the overall progress without reading individual
    /// frames. `None` when no summary has been set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    global_summary: Option<String>,
}

impl ContextPipeline {
    /// Create a new empty pipeline.
    ///
    /// After creation, seed it with an [`IntentAnalysis`](FrameKind::IntentAnalysis)
    /// frame, then create a [`Plan`](super::plan::Plan) that references this
    /// pipeline via `pipeline`.
    pub fn new(created_by: ActorRef) -> Result<Self, String> {
        Ok(Self {
            header: Header::new(ObjectType::ContextPipeline, created_by)?,
            frames: Vec::new(),
            max_frames: 0,
            global_summary: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns all frames in chronological order.
    pub fn frames(&self) -> &[ContextFrame] {
        &self.frames
    }

    pub fn max_frames(&self) -> u32 {
        self.max_frames
    }

    pub fn global_summary(&self) -> Option<&str> {
        self.global_summary.as_deref()
    }

    pub fn set_max_frames(&mut self, max_frames: u32) {
        self.max_frames = max_frames;
    }

    pub fn set_global_summary(&mut self, summary: Option<String>) {
        self.global_summary = summary;
    }

    /// Append a frame. If `max_frames > 0` and the limit is exceeded,
    /// the oldest non-Checkpoint frame is removed to make room.
    pub fn push_frame(&mut self, frame: ContextFrame) {
        self.frames.push(frame);
        self.evict_if_needed();
    }

    /// Returns frames that contribute to the active context window
    /// (i.e. all current frames after any eviction has been applied).
    pub fn active_frames(&self) -> &[ContextFrame] {
        &self.frames
    }

    /// Total estimated tokens across all frames.
    pub fn total_token_estimate(&self) -> u64 {
        self.frames.iter().filter_map(|f| f.token_estimate).sum()
    }

    /// Evict the oldest evictable frame if over the limit.
    ///
    /// `IntentAnalysis` and `Checkpoint` frames are protected from eviction.
    fn evict_if_needed(&mut self) {
        if self.max_frames == 0 {
            return;
        }
        while self.frames.len() > self.max_frames as usize {
            // Find the first evictable frame (not IntentAnalysis or Checkpoint)
            if let Some(pos) = self.frames.iter().position(|f| {
                f.kind != FrameKind::Checkpoint && f.kind != FrameKind::IntentAnalysis
            }) {
                self.frames.remove(pos);
            } else {
                // All frames are protected — nothing to evict
                break;
            }
        }
    }
}

impl fmt::Display for ContextPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContextPipeline: {}", self.header.object_id())
    }
}

impl ObjectTrait for ContextPipeline {
    fn from_bytes(data: &[u8], _hash: ObjectHash) -> Result<Self, GitError>
    where
        Self: Sized,
    {
        serde_json::from_slice(data)
            .map_err(|e| GitError::InvalidContextPipelineObject(e.to_string()))
    }

    fn get_type(&self) -> ObjectType {
        ObjectType::ContextPipeline
    }

    fn get_size(&self) -> usize {
        match serde_json::to_vec(self) {
            Ok(v) => v.len(),
            Err(e) => {
                tracing::warn!("failed to compute ContextPipeline size: {}", e);
                0
            }
        }
    }

    fn to_data(&self) -> Result<Vec<u8>, GitError> {
        serde_json::to_vec(self).map_err(|e| GitError::InvalidContextPipelineObject(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pipeline() -> ContextPipeline {
        let actor = ActorRef::agent("orchestrator").expect("actor");
        ContextPipeline::new(actor).expect("pipeline")
    }

    #[test]
    fn test_pipeline_creation() {
        let pipeline = make_pipeline();

        assert_eq!(
            pipeline.header().object_type(),
            &ObjectType::ContextPipeline
        );
        assert!(pipeline.frames().is_empty());
        assert_eq!(pipeline.max_frames(), 0);
        assert!(pipeline.global_summary().is_none());
    }

    #[test]
    fn test_push_and_retrieve_frames() {
        let mut pipeline = make_pipeline();

        let mut f1 = ContextFrame::new(FrameKind::StepSummary, "Completed auth refactor");
        f1.set_token_estimate(Some(200));
        pipeline.push_frame(f1);

        let f2 = ContextFrame::new(FrameKind::CodeChange, "Modified 3 files, +120 -45 lines");
        pipeline.push_frame(f2);

        let mut f3 = ContextFrame::new(FrameKind::Checkpoint, "User save-point");
        f3.set_data(Some(serde_json::json!({"key": "value"})));
        pipeline.push_frame(f3);

        assert_eq!(pipeline.frames().len(), 3);
        assert_eq!(pipeline.frames()[0].kind(), &FrameKind::StepSummary);
        assert_eq!(pipeline.frames()[1].kind(), &FrameKind::CodeChange);
        assert_eq!(pipeline.frames()[2].kind(), &FrameKind::Checkpoint);
        assert!(pipeline.frames()[2].data().is_some());
    }

    #[test]
    fn test_max_frames_eviction() {
        let mut pipeline = make_pipeline();
        pipeline.set_max_frames(3);

        // Push a checkpoint (should survive eviction)
        pipeline.push_frame(ContextFrame::new(FrameKind::Checkpoint, "save-point"));
        // Push regular frames
        pipeline.push_frame(ContextFrame::new(FrameKind::StepSummary, "step 1"));
        pipeline.push_frame(ContextFrame::new(FrameKind::StepSummary, "step 2"));
        assert_eq!(pipeline.frames().len(), 3);

        // This push exceeds max_frames → oldest non-Checkpoint ("step 1") is evicted
        pipeline.push_frame(ContextFrame::new(FrameKind::CodeChange, "code change"));
        assert_eq!(pipeline.frames().len(), 3);

        // Checkpoint survived, "step 1" was evicted
        assert_eq!(pipeline.frames()[0].kind(), &FrameKind::Checkpoint);
        assert_eq!(pipeline.frames()[1].summary(), "step 2");
        assert_eq!(pipeline.frames()[2].summary(), "code change");
    }

    #[test]
    fn test_total_token_estimate() {
        let mut pipeline = make_pipeline();

        let mut f1 = ContextFrame::new(FrameKind::StepSummary, "s1");
        f1.set_token_estimate(Some(100));
        pipeline.push_frame(f1);

        let mut f2 = ContextFrame::new(FrameKind::StepSummary, "s2");
        f2.set_token_estimate(Some(250));
        pipeline.push_frame(f2);

        // Frame without token estimate
        pipeline.push_frame(ContextFrame::new(FrameKind::Checkpoint, "cp"));

        assert_eq!(pipeline.total_token_estimate(), 350);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut pipeline = make_pipeline();
        pipeline.set_global_summary(Some("Overall progress summary".to_string()));

        let mut frame = ContextFrame::new(FrameKind::StepSummary, "did stuff");
        frame.set_token_estimate(Some(150));
        frame.set_data(Some(serde_json::json!({"files": ["a.rs", "b.rs"]})));
        pipeline.push_frame(frame);

        let data = pipeline.to_data().expect("serialize");
        let restored =
            ContextPipeline::from_bytes(&data, ObjectHash::default()).expect("deserialize");

        assert_eq!(restored.frames().len(), 1);
        assert_eq!(restored.frames()[0].summary(), "did stuff");
        assert_eq!(restored.frames()[0].token_estimate(), Some(150));
        assert_eq!(restored.global_summary(), Some("Overall progress summary"));
    }

    #[test]
    fn test_intent_analysis_frame_survives_eviction() {
        let mut pipeline = make_pipeline();
        pipeline.set_max_frames(2);

        // Seed with IntentAnalysis (protected)
        pipeline.push_frame(ContextFrame::new(
            FrameKind::IntentAnalysis,
            "AI analysis of user intent",
        ));
        pipeline.push_frame(ContextFrame::new(FrameKind::StepSummary, "step 1"));
        assert_eq!(pipeline.frames().len(), 2);

        // Adding another frame should evict "step 1", not IntentAnalysis
        pipeline.push_frame(ContextFrame::new(FrameKind::CodeChange, "code change"));
        assert_eq!(pipeline.frames().len(), 2);
        assert_eq!(pipeline.frames()[0].kind(), &FrameKind::IntentAnalysis);
        assert_eq!(pipeline.frames()[1].summary(), "code change");
    }
}
