// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Independent requested-allocation oracle for the pinned standard-library
//! layouts. Run this target again when an embedding host changes toolchains.

use hawdb_core::Value;
use hawdb_plan_cache::{parameterize_logical_plan, LfuCache, ParameterBindingTemplate};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
}
static LIVE: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        LIVE.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn deallocated(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        LIVE.fetch_sub(bytes, Ordering::Relaxed);
    }
}

struct CountingAllocator;

// SAFETY: every operation delegates unchanged pointer/layout arguments to
// System. Const TLS cells and atomic counters never allocate or panic. Only
// allocations on the test's thread are counted, excluding harness activity.
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

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            deallocated(layout.size());
            allocated(size);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Window;

impl Window {
    fn new() -> Self {
        LIVE.store(0, Ordering::Relaxed);
        TRACK.set(true);
        Self
    }

    fn retained(&self) -> usize {
        LIVE.load(Ordering::Relaxed)
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        TRACK.set(false);
    }
}

#[test]
fn charges_cover_actual_template_and_churn_allocations() {
    for candidates in [20, 100, 512] {
        let mut query = "MATCH (n:Memory) WHERE n.id IN $ids RETURN n.id AS id".to_owned();
        let mut parameters = BTreeMap::from([(
            "ids".into(),
            Value::List(
                (0..candidates)
                    .map(|index| Value::String(format!("memory-{index:08}")))
                    .collect(),
            ),
        )]);
        for field in 0..5 {
            query.push_str(", CASE n.id");
            for index in 0..candidates {
                query.push_str(&format!(" WHEN $id_{index} THEN $score_{field}_{index}"));
                parameters.insert(
                    format!("id_{index}"),
                    Value::String(format!("memory-{index:08}")),
                );
                parameters.insert(format!("score_{field}_{index}"), Value::Float(0.5));
            }
            query.push_str(&format!(" ELSE null END AS score_{field}"));
        }
        query.push_str(" ORDER BY score_0");
        let statement = hawdb_cypher::parse(&query).unwrap();
        let (retained, charged) = {
            let window = Window::new();
            let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
            let template = Arc::new(ParameterBindingTemplate::new(&plan, &parameters).unwrap());
            drop(plan);
            let retained = window.retained();
            let charged = template.retained_heap_bytes().unwrap();
            assert!(retained <= charged, "{candidates}: {retained} > {charged}");
            drop(template);
            assert_eq!(window.retained(), 0);
            (retained, charged)
        };
        println!("candidates={candidates} retained={retained} charged={charged}");
    }

    // General projection parameters retain exact guards. Include a large
    // nested map/list and vector, independently of the slot-marker fixtures.
    let properties = (0..512)
        .map(|index| (format!("property-{index}"), Value::String("x".repeat(4096))))
        .collect();
    let payload = Value::List(vec![
        Value::Map(properties),
        Value::Binary(vec![7; 16384]),
        Value::Vector(hawdb_core::Vector::new(&vec![0.5; 1024]).unwrap()),
    ]);
    let parameters = BTreeMap::from([("payload".into(), payload)]);
    let statement = hawdb_cypher::parse("MATCH (n:Memory) RETURN $payload AS payload").unwrap();
    let (retained, charged) = {
        let window = Window::new();
        let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
        assert_eq!(plan.exact_variant_count(), 1);
        let template = Arc::new(ParameterBindingTemplate::new(&plan, &parameters).unwrap());
        drop(plan);
        let retained = window.retained();
        let charged = template.retained_heap_bytes().unwrap();
        assert!(retained <= charged, "exact payload: {retained} > {charged}");
        drop(template);
        assert_eq!(window.retained(), 0);
        (retained, charged)
    };
    println!("exact_payload retained={retained} charged={charged}");

    let (maximum_retained, maximum_charged) = {
        let window = Window::new();
        let budget = 64 * 1024;
        let mut cache = LfuCache::with_byte_limit(Some(128), Some(budget));
        let mut maximum_retained = 0;
        let mut maximum_charged = 0;
        for index in 0..4096 {
            let key = format!("retention-probe-{index:08}");
            let value = vec![index as u8; 512];
            let bytes = key.capacity() + value.capacity();
            cache.insert_weighted(key, value, bytes);
            let retained = window.retained();
            let charged = cache.charged_bytes().unwrap();
            assert!(retained <= charged, "{index}: {retained} > {charged}");
            assert!(charged <= budget);
            maximum_retained = maximum_retained.max(retained);
            maximum_charged = maximum_charged.max(charged);
        }
        assert!(cache.stats().evictions > 0);
        drop(cache);
        assert_eq!(window.retained(), 0);
        (maximum_retained, maximum_charged)
    };
    println!("churn retained_max={maximum_retained} charged_max={maximum_charged} budget=65536");
}
