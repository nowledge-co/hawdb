//! Track requested live capacity for selected calls, including later TLS drops.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TrackingAllocator;

#[repr(C)]
struct Header {
    tracked: bool,
}

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
}

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocation_layout(layout: Layout) -> (Layout, usize) {
    let offset = layout.align().max(std::mem::size_of::<Header>());
    let size = layout.size().checked_add(offset).unwrap();
    (
        Layout::from_size_align(size, layout.align()).unwrap(),
        offset,
    )
}

fn added(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every pointer has a private prefix recording its tracking status.
// System receives the same extended layout for allocation and deallocation;
// the user pointer retains the requested alignment and usable size.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (extended, offset) = allocation_layout(layout);
        // SAFETY: extended is a valid layout with room for the header.
        let base = unsafe { System.alloc(extended) };
        if base.is_null() {
            return base;
        }
        let tracked = ENABLED.try_with(Cell::get).unwrap_or(false);
        // SAFETY: the header and payload are disjoint and inside the allocation.
        unsafe { base.cast::<Header>().write(Header { tracked }) };
        if tracked {
            added(layout.size());
        }
        // SAFETY: offset preserves alignment and leaves layout.size() bytes.
        unsafe { base.add(offset) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let (extended, offset) = allocation_layout(layout);
        // SAFETY: pointer was returned by alloc with this exact offset/layout.
        let base = unsafe { pointer.sub(offset) };
        // SAFETY: the initialized header precedes the live payload.
        let tracked = unsafe { (*base.cast::<Header>()).tracked };
        // SAFETY: base and extended reproduce the original System allocation.
        unsafe { System.dealloc(base, extended) };
        if tracked {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // Allocate before freeing to measure conservative replacement overlap,
        // independently of whether the system allocator could grow in place.
        let replacement = Layout::from_size_align(size, layout.align()).unwrap();
        // SAFETY: replacement is valid and obeys the GlobalAlloc contract.
        let next = unsafe { self.alloc(replacement) };
        if !next.is_null() {
            // SAFETY: allocations are disjoint and the copied range fits both.
            unsafe { std::ptr::copy_nonoverlapping(pointer, next, size.min(layout.size())) };
            // SAFETY: the old pointer remains live with its original layout.
            unsafe { self.dealloc(pointer, layout) };
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

pub(crate) fn measure<T>(work: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ENABLED.with(|enabled| enabled.set(false));
        }
    }
    ENABLED.with(|enabled| assert!(!enabled.replace(true)));
    PEAK.store(live(), Ordering::Relaxed);
    let reset = Reset;
    let value = work();
    let peak = PEAK.load(Ordering::Relaxed);
    drop(reset);
    (value, peak)
}

pub(crate) fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

// Serialize the complete lifetime of measured values within one test binary.
#[allow(dead_code)]
pub(crate) fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL.lock().unwrap_or_else(|error| error.into_inner())
}
