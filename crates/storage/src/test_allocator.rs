// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native test-only allocation observations, scoped to the calling thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static OBSERVING: Cell<bool> = const { Cell::new(false) };
    static LARGE_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
    static DEALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static DEALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
    static MINIMUM_ALLOCATION_BYTES: Cell<usize> = const { Cell::new(64 * 1024) };
}

struct TestAllocator;

fn observe(size: usize, allocated: bool) {
    let _ = OBSERVING.try_with(|active| {
        let selected = MINIMUM_ALLOCATION_BYTES
            .try_with(|minimum| size > minimum.get())
            .unwrap_or(false);
        if active.get() && selected && allocated {
            let _ = LARGE_ALLOCATIONS.try_with(|count| {
                count.set(count.get().saturating_add(1));
            });
            let _ = ALLOCATED_BYTES.try_with(|bytes| {
                bytes.set(bytes.get().saturating_add(size));
            });
        } else if active.get() && selected {
            let _ = DEALLOCATIONS.try_with(|count| count.set(count.get().saturating_add(1)));
            let _ = DEALLOCATED_BYTES.try_with(|bytes| {
                bytes.set(bytes.get().saturating_add(size));
            });
        }
    });
}

// SAFETY: every operation delegates the original valid layout and pointer to
// System. Observations use allocation-free TLS cells and do not alter results.
unsafe impl GlobalAlloc for TestAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            observe(layout.size(), true);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            observe(layout.size(), true);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller supplies a live System allocation and valid size.
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            observe(layout.size(), false);
            observe(size, true);
        }
        result
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller supplies the original System pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        observe(layout.size(), false);
    }
}

#[global_allocator]
static ALLOCATOR: TestAllocator = TestAllocator;

// An observation must be finished/dropped on the thread that started it.
pub(crate) struct AllocationObservation(std::marker::PhantomData<std::rc::Rc<()>>);

/// Requested allocation traffic on the observing thread, including frees of
/// older allocations. This is not a process-wide live-memory or RSS counter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AllocationStats {
    pub allocations: usize,
    pub allocated_bytes: usize,
    pub deallocations: usize,
    pub deallocated_bytes: usize,
}

impl AllocationObservation {
    pub(crate) fn start() -> Self {
        Self::start_at(64 * 1024)
    }

    pub(crate) fn start_all() -> Self {
        Self::start_at(0)
    }

    fn start_at(minimum_bytes: usize) -> Self {
        OBSERVING.with(|active| assert!(!active.replace(true), "nested allocation observation"));
        LARGE_ALLOCATIONS.with(|count| count.set(0));
        ALLOCATED_BYTES.with(|bytes| bytes.set(0));
        DEALLOCATIONS.with(|count| count.set(0));
        DEALLOCATED_BYTES.with(|bytes| bytes.set(0));
        MINIMUM_ALLOCATION_BYTES.with(|minimum| minimum.set(minimum_bytes));
        Self(std::marker::PhantomData)
    }

    pub(crate) fn finish(self) -> usize {
        let result = LARGE_ALLOCATIONS.with(Cell::get);
        drop(self);
        result
    }

    pub(crate) fn finish_bytes(self) -> usize {
        let result = ALLOCATED_BYTES.with(Cell::get);
        drop(self);
        result
    }

    pub(crate) fn finish_stats(self) -> AllocationStats {
        let result = AllocationStats {
            allocations: LARGE_ALLOCATIONS.with(Cell::get),
            allocated_bytes: ALLOCATED_BYTES.with(Cell::get),
            deallocations: DEALLOCATIONS.with(Cell::get),
            deallocated_bytes: DEALLOCATED_BYTES.with(Cell::get),
        };
        drop(self);
        result
    }
}

impl Drop for AllocationObservation {
    fn drop(&mut self) {
        OBSERVING.with(|active| active.set(false));
    }
}
