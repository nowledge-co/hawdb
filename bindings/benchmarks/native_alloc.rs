// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Local boundary qualification instrumentation, enabled only in explicitly
//! named profiling builds. Counts Rust allocator requests across every native
//! thread; it neither changes admission nor proves a total process-memory bound.
//! Non-Rust allocations, allocator rounding and foreign heaps need separate
//! measurements. Instrumented timings are separate from ordinary release runs.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct BoundaryAllocationSnapshot {
    pub allocation_calls: u64,
    pub allocated_bytes: u64,
    pub deallocation_calls: u64,
    pub deallocated_bytes: u64,
    pub reallocation_calls: u64,
    pub live_requested_bytes: u64,
    pub process_peak_requested_bytes: u64,
}

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn allocated(size: usize) {
    let size = size as u64;
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED.fetch_add(size, Ordering::Relaxed);
    let live = LIVE.fetch_add(size, Ordering::Relaxed).saturating_add(size);
    PEAK.fetch_max(live, Ordering::Relaxed);
}

fn deallocated(size: usize) {
    DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    DEALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
    LIVE.fetch_sub(size as u64, Ordering::Relaxed);
}

struct CountingAllocator;

// All raw memory operations preserve System's allocator contract. Counters
// never allocate, take locks, format strings, or call back into the allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        deallocated(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, new_size) };
        if !next.is_null() {
            REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            // Record a successful resize as one allocation/free pair. Count
            // requested traffic, not an unproven claim that realloc copied.
            deallocated(layout.size());
            allocated(new_size);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Counters are monotonic except live bytes. A snapshot during concurrent work
/// is an observation, not an atomic cross-field snapshot or a memory ledger.
pub fn snapshot() -> BoundaryAllocationSnapshot {
    BoundaryAllocationSnapshot {
        allocation_calls: ALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED.load(Ordering::Relaxed),
        deallocation_calls: DEALLOCATIONS.load(Ordering::Relaxed),
        deallocated_bytes: DEALLOCATED.load(Ordering::Relaxed),
        reallocation_calls: REALLOCATIONS.load(Ordering::Relaxed),
        live_requested_bytes: LIVE.load(Ordering::Relaxed),
        process_peak_requested_bytes: PEAK.load(Ordering::Relaxed),
    }
}
