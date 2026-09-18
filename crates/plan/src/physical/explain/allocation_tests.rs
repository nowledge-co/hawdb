use crate::PhysicalPlan;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct MeasuringAllocator;

thread_local! {
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static REQUESTED_BYTES: Cell<usize> = const { Cell::new(0) };
}

fn record_request(bytes: usize) {
    let enabled = MEASURING.try_with(Cell::get).unwrap_or(false);
    if enabled {
        let _ = REQUESTED_BYTES.try_with(|count| count.set(count.get().saturating_add(bytes)));
    }
}

// SAFETY: All allocation operations delegate unchanged layouts and pointers to
// System. Accounting uses non-allocating thread-local cells and cannot recurse.
unsafe impl GlobalAlloc for MeasuringAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_request(layout.size());
        // SAFETY: The caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_request(layout.size());
        // SAFETY: The caller supplies a valid allocation layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: Allocations are created by System with this same layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_request(new_size);
        // SAFETY: The caller supplies the live System allocation and new size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: MeasuringAllocator = MeasuringAllocator;

struct Measurement;

impl Drop for Measurement {
    fn drop(&mut self) {
        MEASURING.set(false);
    }
}

#[test]
fn public_explain_allocation_is_bounded_by_output_size() {
    let mut measurements = Vec::new();
    for depth in [0, 8, 16, 32] {
        let mut plan = PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Memory".to_string(),
        };
        for _ in 0..depth {
            plan = PhysicalPlan::DistinctExec {
                input: Box::new(plan),
            };
        }
        let mut expected = String::new();
        for level in 0..depth {
            expected.push_str(&" ".repeat(level * 2));
            expected.push_str("DistinctExec\n");
        }
        expected.push_str(&" ".repeat(depth * 2));
        expected.push_str("SeqNodeScan variable=n label=Memory");

        REQUESTED_BYTES.set(0);
        assert!(!MEASURING.replace(true));
        let measurement = Measurement;
        let actual = plan.explain(0);
        drop(measurement);
        let requested_bytes = REQUESTED_BYTES.get();
        assert_eq!(actual, expected);
        eprintln!(
            "EXPLAIN depth {depth}: {} output bytes, {requested_bytes} requested bytes",
            actual.len()
        );
        measurements.push((depth, actual.len(), requested_bytes));
    }
    eprintln!("EXPLAIN allocation (depth, output bytes, requested bytes): {measurements:?}");
    for (depth, output_bytes, requested_bytes) in measurements {
        // Allow geometric final-buffer growth, one header/padding allocation
        // per operator and the traversal stack, but not repeated subtrees.
        assert!(
            requested_bytes <= output_bytes * 12 + 1024,
            "depth {depth}: requested {requested_bytes} bytes for {output_bytes} output bytes"
        );
    }
}

#[test]
fn public_explain_handles_deep_plans_on_the_normal_test_stack() {
    let mut plan = PhysicalPlan::EmptyExec;
    for _ in 0..128 {
        plan = PhysicalPlan::DistinctExec {
            input: Box::new(plan),
        };
    }
    let explanation = plan.explain(0);
    assert_eq!(explanation.lines().count(), 129);
    for (depth, line) in explanation.lines().enumerate() {
        let operator = if depth == 128 {
            "EmptyExec"
        } else {
            "DistinctExec"
        };
        assert_eq!(line, format!("{}{operator}", " ".repeat(depth * 2)));
    }
}
