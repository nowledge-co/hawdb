//! Native test-only allocation observations, scoped to the calling thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static OBSERVING: Cell<bool> = const { Cell::new(false) };
    static LARGE_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static MINIMUM_ALLOCATION_BYTES: Cell<usize> = const { Cell::new(64 * 1024) };
}

struct TestAllocator;

fn observe(size: usize, allocated: bool) {
    if allocated {
        let _ = OBSERVING.try_with(|active| {
            let selected = MINIMUM_ALLOCATION_BYTES
                .try_with(|minimum| size > minimum.get())
                .unwrap_or(false);
            if active.get() && selected {
                let _ = LARGE_ALLOCATIONS.try_with(|count| {
                    count.set(count.get().saturating_add(1));
                });
            }
        });
    }
}

// SAFETY: every operation delegates the original valid layout and pointer to
// System. Observations use allocation-free TLS cells and do not alter results.
unsafe impl GlobalAlloc for TestAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero layout.
        let pointer = unsafe { System.alloc(layout) };
        observe(layout.size(), !pointer.is_null());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        observe(layout.size(), !pointer.is_null());
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller supplies a live System allocation and valid size.
        let result = unsafe { System.realloc(pointer, layout, size) };
        observe(size, !result.is_null());
        result
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller supplies the original System pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: TestAllocator = TestAllocator;

// An observation must be finished/dropped on the thread that started it.
pub(crate) struct AllocationObservation(std::marker::PhantomData<std::rc::Rc<()>>);

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
        MINIMUM_ALLOCATION_BYTES.with(|minimum| minimum.set(minimum_bytes));
        Self(std::marker::PhantomData)
    }

    pub(crate) fn finish(self) -> usize {
        let result = LARGE_ALLOCATIONS.with(Cell::get);
        drop(self);
        result
    }
}

impl Drop for AllocationObservation {
    fn drop(&mut self) {
        OBSERVING.with(|active| active.set(false));
    }
}
