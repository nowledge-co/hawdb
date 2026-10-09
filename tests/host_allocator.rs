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

//! Allocator injection belongs to the final consumer on native and browser WASM.
use hawdb::{Database, Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

struct HostAllocator;

#[derive(Clone, Copy, Default)]
struct AllocationActivity {
    allocations: usize,
    deallocations: usize,
    allocated_bytes: usize,
    deallocated_bytes: usize,
}

thread_local! {
    // Thread-local observation excludes activity from concurrently running tests.
    static ACTIVITY: Cell<Option<AllocationActivity>> = const { Cell::new(None) };
}

fn record(allocation: bool, bytes: usize) {
    let _ = ACTIVITY.try_with(|activity| {
        if let Some(mut counts) = activity.get() {
            if allocation {
                counts.allocations = counts.allocations.saturating_add(1);
                counts.allocated_bytes = counts.allocated_bytes.saturating_add(bytes);
            } else {
                counts.deallocations = counts.deallocations.saturating_add(1);
                counts.deallocated_bytes = counts.deallocated_bytes.saturating_add(bytes);
            }
            activity.set(Some(counts));
        }
    });
}

// SAFETY: all operations preserve System's pointer/layout contracts. Recording
// uses only const-initialized thread-local cells and cannot allocate or unwind.
unsafe impl GlobalAlloc for HostAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid allocation layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(true, layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid allocation layout.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(true, layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's live System allocation and new size are unchanged.
        let replacement = unsafe { System.realloc(ptr, layout, new_size) };
        if !replacement.is_null() {
            record(false, layout.size());
            record(true, new_size);
        }
        replacement
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout describe the original System allocation.
        unsafe { System.dealloc(ptr, layout) };
        record(false, layout.size());
    }
}

#[global_allocator]
static ALLOCATOR: HostAllocator = HostAllocator;

fn observe<T>(run: impl FnOnce() -> T) -> (T, AllocationActivity) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVITY.with(|activity| activity.set(None));
        }
    }

    ACTIVITY.with(|activity| {
        assert!(activity
            .replace(Some(AllocationActivity::default()))
            .is_none());
    });
    let reset = Reset;
    let result = run();
    let activity = ACTIVITY.with(|activity| activity.get().unwrap());
    drop(reset);
    (result, activity)
}

#[test]
fn graph_and_sql_queries_use_the_host_allocator() {
    let mut db = Database::new();
    let parameters = BTreeMap::from([("title".into(), Value::String("Host allocator".into()))]);
    let (_, graph_write) = observe(|| {
        db.query_with_params("CREATE (:Memory {id: 1, title: $title})", &parameters)
            .unwrap()
    });
    assert!(graph_write.allocations > 0);
    let (graph, graph_read) = observe(|| {
        db.query("MATCH (m:Memory) RETURN m.title AS title")
            .unwrap()
    });
    assert!(graph_read.allocations > 0);
    assert_eq!(graph.rows.len(), 1);
    assert_eq!(
        graph.rows[0].get("title"),
        Some(&Value::String("Host allocator".into()))
    );

    let (_, sql_write) = observe(|| {
        db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
            .unwrap();
        db.query_sql("INSERT INTO items (id) VALUES (42)").unwrap()
    });
    assert!(sql_write.allocations > 0);
    let (sql, sql_read) = observe(|| db.query_sql("SELECT id FROM items").unwrap());
    assert!(sql_read.allocations > 0);
    assert_eq!(sql.rows.len(), 1);
    assert_eq!(sql.rows[0].get("id"), Some(&Value::Int(42)));
}

#[test]
fn database_and_result_teardown_use_the_host_allocator() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Owned by the host'})")
        .unwrap();
    let result = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    let (_, result_drop) = observe(|| drop(result));
    assert!(result_drop.deallocations > 0);
    let (_, database_drop) = observe(|| drop(db));
    assert!(database_drop.deallocations > 0);
}

#[test]
fn native_zstd_context_uses_the_host_for_allocation_and_release() {
    let (context, created) =
        observe(|| hawdb_storage::compression::DecompressionContext::new().unwrap());
    assert!(created.allocations > 0);
    assert!(created.allocated_bytes >= context.sizeof());
    let (_, released) = observe(|| drop(context));
    assert_eq!(created.allocations, released.deallocations);
    assert_eq!(created.allocated_bytes, released.deallocated_bytes);
}

#[test]
fn native_zstd_compression_workspace_uses_the_host_allocator() {
    let input = vec![42; 300_000];
    let (_, activity) = observe(|| {
        let mut encoder = hawdb_storage::compression::Encoder::new(std::io::sink(), 3).unwrap();
        std::io::Write::write_all(&mut encoder, &input).unwrap();
        encoder.finish().unwrap();
    });
    // A sink retains no encoded output. This exceeds the 32 KiB Rust I/O
    // buffer and proves that the native compression workspace is observed.
    assert!(activity.allocated_bytes > 1024 * 1024);
    assert_eq!(activity.allocations, activity.deallocations);
    assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
}
