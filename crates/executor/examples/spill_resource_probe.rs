// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual resource probe. Run identical source on both revisions in release mode.
//! Heap counters include all worker threads; obtain process peak RSS externally.

use hawdb_core::{Catalog, Result, RuntimeTaskContext, Value};
use hawdb_executor::binding::Binding;
use hawdb_executor::blocking::stream_sort_batches;
use hawdb_executor::observer::QueryExecutionObserver;
use hawdb_executor::pipeline::{
    BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
};
use hawdb_executor::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
use hawdb_plan_cypher::{PhysicalPlan, SortDirection, SortItem, SortKey};
use hawdb_qos::{
    IoConcurrencyBudget, ProcessMemoryCapabilities, ProcessMemoryPolicy, ProcessMemoryPolicyConfig,
    ProcessMemorySnapshot, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;
use std::time::Instant;

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

fn account(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

// SAFETY: Every operation delegates its unchanged layout and pointer to System.
// Counters neither allocate nor alter allocation ownership or alignment.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            account(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            account(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, bytes) };
        if !next.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            account(bytes);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Rows {
    count: usize,
    width: usize,
}
impl BindingBatchSource for Rows {
    fn execute(
        &mut self,
        _: &PhysicalPlan,
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        for index in 0..self.count {
            let binding = Binding::values(BTreeMap::from([
                ("key".into(), Value::Int(((index * 7919) % 97) as i64)),
                ("ordinal".into(), Value::Int(index as i64)),
                (
                    "payload".into(),
                    Value::List(vec![Value::Map(BTreeMap::from([(
                        "body".into(),
                        Value::String("x".repeat(self.width)),
                    )]))]),
                ),
            ]));
            if emit(vec![binding])? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }
}

#[derive(Default)]
struct Measurement {
    rows: usize,
    spill_bytes: u64,
    runs: usize,
    ledger_peak: usize,
    checksum: u64,
}

fn query(
    index: usize,
    rows: usize,
    width: usize,
    blocking: usize,
    barrier: &Barrier,
    governor: Option<&RuntimeGovernor>,
) -> Result<Measurement> {
    let directory =
        std::env::temp_dir().join(format!("hawdb-spill-probe-{}-{index}", std::process::id()));
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(blocking).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        max_spill_runs: NonZeroUsize::new(4096).unwrap(),
        max_total_spill_runs: NonZeroUsize::new(16384).unwrap(),
        max_spill_bytes: NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let permit = governor.map(|governor| {
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(16 * 1024 * 1024, 0))
            .unwrap()
    });
    let task = permit
        .as_ref()
        .map(|permit| permit.bind_task_context(RuntimeTaskContext::default()));
    let ledger = QueryMemoryLedger::for_query(&memory, task.as_ref())?;
    let observer = QueryExecutionObserver::default();
    let catalog = Catalog::default();
    let items = [SortItem {
        key: SortKey::Column("key".into()),
        direction: SortDirection::Asc,
    }];
    let mut source = Rows { count: rows, width };
    let mut measurement = Measurement::default();
    let mut previous = None;
    barrier.wait();
    barrier.wait();
    stream_sort_batches(
        &PhysicalPlan::EmptyExec,
        &items,
        &mut source,
        BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: task.as_ref(),
            observer: &observer,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            for binding in batch {
                let Value::Int(key) = binding.values["key"] else {
                    panic!("missing key")
                };
                let Value::Int(ordinal) = binding.values["ordinal"] else {
                    panic!("missing ordinal")
                };
                assert!(
                    previous.is_none_or(|previous| previous < (key, ordinal)),
                    "stable ordering changed"
                );
                previous = Some((key, ordinal));
                let Value::List(payload) = &binding.values["payload"] else {
                    panic!("missing payload")
                };
                let Value::Map(payload) = &payload[0] else {
                    panic!("missing payload map")
                };
                let Value::String(body) = &payload["body"] else {
                    panic!("missing body")
                };
                assert_eq!(body.len(), width);
                measurement.checksum = measurement.checksum.wrapping_add(ordinal as u64);
                measurement.rows += 1;
            }
            Ok(BatchControl::Continue)
        },
    )?;
    assert_eq!(measurement.rows, rows);
    assert_eq!(measurement.checksum, (rows as u64) * (rows as u64 - 1) / 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.spill_pool_snapshot()?.active_bytes, 0);
    measurement.ledger_peak = ledger.snapshot().peak_bytes;
    for report in observer.into_reports().blocking_memory {
        measurement.spill_bytes += report.spilled_bytes;
        measurement.runs += report.spill_run_count;
    }
    if directory.exists() {
        std::fs::remove_dir_all(directory).unwrap();
    }
    Ok(measurement)
}

fn shared_governor() -> RuntimeGovernor {
    let policy = ProcessMemoryPolicy::new(
        ProcessMemoryPolicyConfig::new(NonZeroU64::new(2 * 1024 * 1024 * 1024).unwrap())
            .with_sample_max_age(std::time::Duration::from_secs(60)),
    );
    // A fixed host sample isolates governed execution from an RSS sampler.
    policy.update(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 128 * 1024 * 1024,
        peak_resident_bytes: 128 * 1024 * 1024,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    RuntimeGovernor::new_with_process_memory_policy(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(512 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(16).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(4 << 30), Some(4 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(16, 1),
        policy,
    )
}

fn main() {
    let scenario = std::env::args().nth(1).unwrap_or_else(|| "wide".into());
    let (workers, rows, width, blocking) = match scenario.as_str() {
        "small" => (1, 32, 64, 128 * 1024),
        "wide" => (1, 2048, 4096, 128 * 1024),
        "many" => (1, 8192, 128, 8 * 1024),
        "concurrent" => (4, 2048, 4096, 128 * 1024),
        _ => panic!("use small, wide, many, or concurrent"),
    };
    let governed = std::env::args().nth(2).is_some_and(|option| {
        assert_eq!(option, "--governed");
        true
    });
    let governor = governed.then(shared_governor);
    let barrier = Barrier::new(workers + 1);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let barrier = &barrier;
                let governor = governor.as_ref();
                scope.spawn(move || query(worker, rows, width, blocking, barrier, governor))
            })
            .collect();
        barrier.wait();
        let baseline_live = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline_live, Ordering::Relaxed);
        let allocations = ALLOCATIONS.load(Ordering::Relaxed);
        let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
        let start = Instant::now();
        barrier.wait();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        println!("scenario={scenario} governed={} workers={workers} rows={} width={width} blocking_bytes={blocking} elapsed_us={} heap_peak_bytes={} heap_growth_bytes={} allocations={} allocated_bytes={} spill_bytes={} spill_runs={} ledger_peak_bytes={} checksum={}",
            usize::from(governed), rows * workers, start.elapsed().as_micros(), PEAK.load(Ordering::Relaxed),
            PEAK.load(Ordering::Relaxed).saturating_sub(baseline_live), ALLOCATIONS.load(Ordering::Relaxed) - allocations,
            ALLOCATED_BYTES.load(Ordering::Relaxed) - allocated_bytes,
            results.iter().map(|result| result.spill_bytes).sum::<u64>(), results.iter().map(|result| result.runs).sum::<usize>(),
            results.iter().map(|result| result.ledger_peak).max().unwrap(), results.iter().map(|result| result.checksum).sum::<u64>(),
        );
    });
    if let Some(governor) = governor {
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.admissions, workers as u64);
        assert_eq!(snapshot.completions, workers as u64);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }
}
