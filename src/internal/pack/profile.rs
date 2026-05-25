//! Optional decode profiling counters.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Snapshot of decode profiling counters.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct DecodeProfileSnapshot {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub fallback_loads: usize,
    pub base_objects: usize,
    pub delta_objects: usize,
    pub waitlist_inserts: usize,
    pub waitlist_takes: usize,
    pub delta_rebuilds: usize,
    pub peak_internal_memory_bytes: usize,
}

static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
static FALLBACK_LOADS: AtomicUsize = AtomicUsize::new(0);
static BASE_OBJECTS: AtomicUsize = AtomicUsize::new(0);
static DELTA_OBJECTS: AtomicUsize = AtomicUsize::new(0);
static WAITLIST_INSERTS: AtomicUsize = AtomicUsize::new(0);
static WAITLIST_TAKES: AtomicUsize = AtomicUsize::new(0);
static DELTA_REBUILDS: AtomicUsize = AtomicUsize::new(0);
static PEAK_INTERNAL_MEMORY_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Reset all profiling counters before a benchmark run.
pub fn reset() {
    CACHE_HITS.store(0, Ordering::Relaxed);
    CACHE_MISSES.store(0, Ordering::Relaxed);
    FALLBACK_LOADS.store(0, Ordering::Relaxed);
    BASE_OBJECTS.store(0, Ordering::Relaxed);
    DELTA_OBJECTS.store(0, Ordering::Relaxed);
    WAITLIST_INSERTS.store(0, Ordering::Relaxed);
    WAITLIST_TAKES.store(0, Ordering::Relaxed);
    DELTA_REBUILDS.store(0, Ordering::Relaxed);
    PEAK_INTERNAL_MEMORY_BYTES.store(0, Ordering::Relaxed);
}

/// Read the current profiling counters.
pub fn snapshot() -> DecodeProfileSnapshot {
    DecodeProfileSnapshot {
        cache_hits: CACHE_HITS.load(Ordering::Relaxed),
        cache_misses: CACHE_MISSES.load(Ordering::Relaxed),
        fallback_loads: FALLBACK_LOADS.load(Ordering::Relaxed),
        base_objects: BASE_OBJECTS.load(Ordering::Relaxed),
        delta_objects: DELTA_OBJECTS.load(Ordering::Relaxed),
        waitlist_inserts: WAITLIST_INSERTS.load(Ordering::Relaxed),
        waitlist_takes: WAITLIST_TAKES.load(Ordering::Relaxed),
        delta_rebuilds: DELTA_REBUILDS.load(Ordering::Relaxed),
        peak_internal_memory_bytes: PEAK_INTERNAL_MEMORY_BYTES.load(Ordering::Relaxed),
    }
}

pub(crate) fn cache_hit() {
    CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn cache_miss() {
    CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn fallback_load() {
    FALLBACK_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn base_object() {
    BASE_OBJECTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn delta_object() {
    DELTA_OBJECTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn waitlist_insert() {
    WAITLIST_INSERTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn waitlist_take(count: usize) {
    WAITLIST_TAKES.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn delta_rebuild() {
    DELTA_REBUILDS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn sample_peak_internal_memory(bytes: usize) {
    let mut current = PEAK_INTERNAL_MEMORY_BYTES.load(Ordering::Relaxed);
    while bytes > current {
        match PEAK_INTERNAL_MEMORY_BYTES.compare_exchange_weak(
            current,
            bytes,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}
