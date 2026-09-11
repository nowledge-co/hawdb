//! Isolate allocator instrumentation from the library and its other tests.

#[path = "../src/identifier.rs"]
mod identifier;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;

thread_local! {
    static REQUESTED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record(size: usize) {
    let _ = REQUESTED_BYTES.try_with(|bytes| {
        if let Some(total) = bytes.get() {
            bytes.set(Some(total.saturating_add(size)));
        }
    });
}

// SAFETY: every allocation and deallocation is forwarded unchanged to System.
// The thread-local counter does not allocate or touch the returned memory.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout retain the caller's GlobalAlloc contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        // SAFETY: ptr, layout, and new_size are forwarded without modification.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measure<T>(run: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            REQUESTED_BYTES.with(|bytes| bytes.set(None));
        }
    }
    REQUESTED_BYTES.with(|bytes| assert!(bytes.replace(Some(0)).is_none()));
    let reset = Reset;
    let result = run();
    let requested = REQUESTED_BYTES.with(|bytes| bytes.get().unwrap());
    drop(reset);
    (result, requested)
}

#[test]
fn splitter_removes_source_sized_character_scratch() {
    // Measure allocator-requested bytes, including reallocations, not RSS or
    // live memory. Construct the input outside the measurement window.
    for (name, raw) in [
        ("ascii", "a".repeat(1024 * 1024)),
        ("camel_digits", "HTTPServer42".repeat(8192)),
        ("unicode_expansion", "\u{130}\u{4e2d}\u{e9}".repeat(32768)),
    ] {
        let (expected, old_bytes) = measure(|| identifier::reference::identifier_parts(&raw));
        let (actual, new_bytes) = measure(|| identifier::identifier_parts(&raw));
        assert_eq!(actual, expected, "{name}");
        let char_bytes = raw.chars().count() * std::mem::size_of::<char>();
        assert!(
            new_bytes + char_bytes <= old_bytes,
            "{name}: old={old_bytes}, new={new_bytes}, character scratch={char_bytes}"
        );
        println!(
            "{name}: old={old_bytes}, new={new_bytes}, removed={}",
            old_bytes - new_bytes
        );
    }
}
