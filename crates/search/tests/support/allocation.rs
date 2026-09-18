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

pub(crate) fn measure<T>(run: impl FnOnce() -> T) -> (T, usize) {
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
