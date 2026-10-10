// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.
//!
//! Public transform and optimized-producer contracts with real owning sources.
//! Allocation observations cover execution on this test thread, after fixtures
//! and prebuilt plans have been constructed.

use hawdb_core::{Catalog, HawDBError, PropertyType, Result, TableKind, Value};
use hawdb_executor::batch::{execute_binding_batches, BatchReadContext};
use hawdb_executor::binding::{binding_memory_bytes, Binding};
use hawdb_executor::external::seed::BatchExternalRead;
use hawdb_executor::external::{
    TextSeedExecutionOutput, TextSeedExecutionRequest, VectorSeedExecutionOutput,
    VectorSeedExecutionRequest,
};
use hawdb_executor::observer::QueryExecutionObserver;
use hawdb_executor::pipeline::{
    BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
};
use hawdb_executor::transform::stream_projection_batches;
use hawdb_executor::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
use hawdb_plan_cypher::{
    NodeProjectionAccess, PhysicalPlan, Predicate, Projection, ProjectionExpression,
    VectorExecutionResourceProfile,
};
use hawdb_storage::store::GraphStore;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

thread_local! {
    static WATCH_SIZE: Cell<usize> = const { Cell::new(0) };
    static WATCH_COUNT: Cell<usize> = const { Cell::new(0) };
}

struct ProjectionAllocator;

fn observe(layout: Layout) {
    let _ = WATCH_SIZE.try_with(|size| {
        if size.get() != 0 && (size.get() == usize::MAX || size.get() == layout.size()) {
            let _ = WATCH_COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
        }
    });
}

// This isolated integration binary forwards every allocation unchanged.
// Thread-local observation is enabled only around the actual transform callback.
unsafe impl GlobalAlloc for ProjectionAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(layout);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, size: usize) -> *mut u8 {
        observe(Layout::from_size_align(size, old.align()).unwrap());
        unsafe { System.realloc(pointer, old, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: ProjectionAllocator = ProjectionAllocator;

struct AllocationWindow;

impl AllocationWindow {
    fn start(size: usize) -> Self {
        WATCH_COUNT.with(|count| count.set(0));
        WATCH_SIZE.with(|watch| {
            assert_eq!(watch.get(), 0, "nested allocation observation");
            watch.set(size);
        });
        Self
    }
    fn allocations(&self) -> usize {
        WATCH_COUNT.with(Cell::get)
    }
}

impl Drop for AllocationWindow {
    fn drop(&mut self) {
        let _ = WATCH_SIZE.try_with(|watch| watch.set(0));
    }
}

struct TextSource {
    external_id: String,
    calls: Cell<usize>,
}

impl BatchExternalRead for TextSource {
    fn execute_vector_seed(
        &self,
        _: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        panic!("unexpected vector source")
    }
    fn execute_text_seed(
        &self,
        request: TextSeedExecutionRequest<'_>,
    ) -> Result<TextSeedExecutionOutput> {
        self.calls.set(self.calls.get() + 1);
        let mut output =
            TextSeedExecutionOutput::new(request.result_account, request.resources.result)?;
        for _ in 0..request.resources.result.max_rows {
            output.push("document:a", Some(&self.external_id), 1.0)?;
        }
        Ok(output)
    }
}

struct RuntimeSource<'a> {
    context: BatchReadContext<'a>,
    watched_bytes: usize,
    allocations: &'a Cell<usize>,
}

impl BindingBatchSource for RuntimeSource<'_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        execute_binding_batches(input, self.context, limit, &mut |batch| {
            // The real child has already admitted/constructed its owned rows.
            // Fixture, plan, source and input allocation are outside this window.
            let window = AllocationWindow::start(self.watched_bytes);
            let result = emit(batch);
            self.allocations
                .set(self.allocations.get() + window.allocations());
            result
        })
    }
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn seed(window: usize) -> PhysicalPlan {
    PhysicalPlan::TextSeedScan {
        query_parameter: "text".into(),
        top_k: window,
        output_external_id: true,
        metadata_filters: BTreeMap::new(),
        resource_profile: VectorExecutionResourceProfile {
            priority: 1,
            max_parallelism: 1,
            max_working_memory_bytes: Some(16 * 1024),
        },
    }
}

fn run_refused(items: Vec<Projection>, external_id: String, watched_bytes: usize) {
    let catalog = Catalog::default();
    let store = GraphStore::default();
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(64 * 1024),
        blocking_operator_bytes: nz(16 * 1024),
        batch_payload_bytes: nz(16 * 1024),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let input = seed(1);
    let observer = QueryExecutionObserver::new(&input);
    let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
    let external = TextSource {
        external_id,
        calls: Cell::new(0),
    };
    let allocations = Cell::new(0);
    let mut source = RuntimeSource {
        context: BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        watched_bytes,
        allocations: &allocations,
    };
    let context = BatchExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &observer,
    };
    let callbacks = Cell::new(0);
    let result = stream_projection_batches(
        &items,
        &input,
        &mut source,
        context,
        ExecutionLimit::unlimited(),
        &mut |_| {
            callbacks.set(callbacks.get() + 1);
            Ok(BatchControl::Continue)
        },
    );
    assert_eq!(
        allocations.get(),
        0,
        "oversized projection copied payload before row admission"
    );
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(callbacks.get(), 0);
    assert_eq!(external.calls.get(), 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
}

#[test]
fn text_projection_refuses_large_literal_before_copying_payload() {
    let size = 1024 * 1024 + 137;
    run_refused(
        vec![Projection {
            name: "blob".into(),
            expression: ProjectionExpression::Literal(Value::String("x".repeat(size))),
        }],
        "a".into(),
        size,
    );
}

#[test]
fn text_projection_refuses_repeated_columns_before_copying_payload() {
    let size = 4 * 1024 + 137;
    run_refused(
        (0..5)
            .map(|alias| Projection {
                name: format!("blob{alias}"),
                expression: ProjectionExpression::Column("external_id".into()),
            })
            .collect(),
        "x".repeat(size),
        size,
    );
}

#[test]
fn text_projection_refuses_null_list_storage_before_copying() {
    let count = 131_073;
    run_refused(
        vec![Projection {
            name: "blob".into(),
            expression: ProjectionExpression::Literal(Value::List(vec![Value::Null; count])),
        }],
        "a".into(),
        count * std::mem::size_of::<Value>(),
    );
}

fn run_optimized_literal_refused(numeric: bool, typed: bool) {
    let size = 1024 * 1024 + 137;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "score",
            PropertyType::Int,
            false,
        )
        .unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "kind",
            PropertyType::String,
            false,
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("score".into(), Value::Int(1)),
                ("kind".into(), Value::String("fixture".into())),
            ]),
        )
        .unwrap();
    let mut items = vec![Projection {
        name: "blob".into(),
        expression: ProjectionExpression::Literal(Value::String("X".repeat(size))),
    }];
    let mut required_properties = Vec::new();
    if numeric {
        required_properties.push("score".into());
    }
    if numeric && !typed {
        required_properties.push("kind".into());
        items.push(Projection {
            name: "kind".into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: "kind".into(),
            },
        });
    }
    let plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "n".into(),
        label: "Memory".into(),
        access: NodeProjectionAccess::LabelScan,
        required_properties,
        predicate: numeric.then(|| Predicate::PropertyEq {
            variable: "n".into(),
            property: "score".into(),
            value: Value::Int(1),
        }),
        items,
    };
    let memory = ExecutionMemoryConfig {
        // Leave room for the real numeric morsel admission. The selected
        // output must still be refused by its independent 16 KiB row budget.
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(16 * 1024),
        batch_payload_bytes: nz(16 * 1024),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let context = BatchReadContext {
        catalog: &catalog,
        store: &store,
        parameters: &parameters,
        external: &external,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &observer,
        host_scorer: None,
    };
    let callbacks = Cell::new(0);
    // Schema, fixture, prebuilt plan and literal are outside observation.
    // This executes the actual optimized producer, including numeric selection.
    let window = AllocationWindow::start(size);
    let result = execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
        callbacks.set(callbacks.get() + 1);
        Ok(BatchControl::Continue)
    });
    assert_eq!(
        window.allocations(),
        0,
        "optimized projection copied payload before row admission"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(callbacks.get(), 0);
    assert_eq!(external.calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
}

#[test]
fn fused_node_projection_refuses_large_literal_before_copying() {
    run_optimized_literal_refused(false, false);
}

#[test]
fn numeric_lending_projection_refuses_large_literal_before_copying() {
    run_optimized_literal_refused(true, true);
}

#[test]
fn numeric_rows_projection_refuses_large_literal_before_copying() {
    run_optimized_literal_refused(true, false);
}

fn run_stored_property_projection(numeric: bool, requested: bool) {
    run_stored_property_projection_with_access(numeric, requested, NodeProjectionAccess::LabelScan);
}

struct StoredPropertyDirectory(std::path::PathBuf);

impl StoredPropertyDirectory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-projection-admission-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        assert!(!path.exists(), "fresh fixture already exists");
        Self(path)
    }
}

impl Drop for StoredPropertyDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run_stored_property_projection_with_access(
    numeric: bool,
    requested: bool,
    access: NodeProjectionAccess,
) {
    run_stored_property_projection_with_source(numeric, requested, access, false);
}

fn run_stored_property_projection_with_source(
    numeric: bool,
    requested: bool,
    access: NodeProjectionAccess,
    persisted: bool,
) {
    let size = 1024 * 1024 + 137;
    let directory = persisted.then(StoredPropertyDirectory::new);
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            hawdb_storage::store::WalReplayConfig {
                residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    store.create_node_table(&mut catalog, "Memory").unwrap();
    for (property, property_type) in [("score", PropertyType::Int), ("body", PropertyType::String)]
    {
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                "Memory",
                property,
                property_type,
                false,
            )
            .unwrap();
    }
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("score".into(), Value::Int(1)),
                ("body".into(), Value::String("X".repeat(size))),
            ]),
        )
        .unwrap();
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            hawdb_storage::store::WalReplayConfig {
                residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(store.is_out_of_core());
    }
    let mut required_properties = Vec::new();
    if numeric {
        required_properties.push("score".into());
    }
    if requested {
        required_properties.push("body".into());
    }
    let mut plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "n".into(),
        label: "Memory".into(),
        access,
        required_properties,
        predicate: numeric.then(|| Predicate::PropertyEq {
            variable: "n".into(),
            property: "score".into(),
            value: Value::Int(1),
        }),
        items: vec![Projection {
            name: "result".into(),
            expression: if requested {
                ProjectionExpression::Property {
                    variable: "n".into(),
                    property: "body".into(),
                }
            } else {
                ProjectionExpression::Literal(Value::Int(7))
            },
        }],
    };
    if numeric && persisted {
        // The fused out-of-core NodeProjectionScan deliberately declines the
        // numeric specialization. Its ordinary Project/Filter/Scan entry point
        // exercises the owned numeric and owned typed-numeric producers.
        let PhysicalPlan::NodeProjectionScanExec {
            predicate: Some(predicate),
            items,
            ..
        } = plan
        else {
            unreachable!("numeric fixture retains its selection predicate");
        };
        plan = PhysicalPlan::ProjectExec {
            items,
            input: Box::new(PhysicalPlan::FilterExec {
                predicate,
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".into(),
                    label: "Memory".into(),
                }),
            }),
        };
    }
    let memory = ExecutionMemoryConfig {
        // Numeric selection owns an admitted morsel; the ordinary fused
        // source must also obey its smaller query and working envelopes.
        query_memory_bytes: nz(if numeric { 16 * 1024 * 1024 } else { 64 * 1024 }),
        blocking_operator_bytes: nz(16 * 1024),
        batch_payload_bytes: nz(16 * 1024),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let context = BatchReadContext {
        catalog: &catalog,
        store: &store,
        parameters: &parameters,
        external: &external,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &observer,
        host_scorer: None,
    };
    let rows = Cell::new(0);
    // Store ownership and plan preparation precede the observation window.
    // Observe the actual producer, including property selection/hydration.
    let window = AllocationWindow::start(size);
    let result =
        execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |batch| {
            assert!(!requested, "oversized stored property reached the consumer");
            for row in batch {
                assert_eq!(row.values["result"], Value::Int(7));
                rows.set(rows.get() + 1);
            }
            Ok(BatchControl::Continue)
        });
    assert_eq!(
        window.allocations(),
        0,
        "stored property copied without admission or despite not being requested"
    );
    drop(window);
    if requested {
        assert!(matches!(result, Err(HawDBError::Execution(_))));
        assert_eq!(rows.get(), 0);
    } else {
        assert_eq!(result.unwrap(), BatchControl::Continue);
        assert_eq!(rows.get(), 1);
    }
    assert_eq!(external.calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
}

#[test]
fn fused_projection_refuses_large_stored_property_before_copying() {
    run_stored_property_projection(false, true);
}

fn run_pruning_budget(predicate: Predicate, count: usize, refusal: bool) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "kind",
            PropertyType::String,
            false,
        )
        .unwrap();
    for _ in 0..count {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("kind".into(), Value::String("a".into()))]),
            )
            .unwrap();
    }
    store
        .create_property_index(&mut catalog, "Memory", "kind")
        .unwrap();
    let label_id = catalog.label_id("Memory").unwrap();
    assert!(catalog.property_index_id(label_id, "kind").is_some());
    // Verify the fixture exercises index pruning before the budgeted run.
    let probe = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label_id),
        Some(&hawdb_storage::mutation::PropertyFilter::Eq {
            property: "kind".into(),
            value: Value::String("a".into()),
        }),
    );
    assert!(probe.report.pruned);
    assert_eq!(probe.report.candidate_count_before_filter, count);
    assert_eq!(probe.nodes.len(), count);
    drop(probe);
    let plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "n".into(),
        label: "Memory".into(),
        access: NodeProjectionAccess::LabelScan,
        required_properties: vec!["kind".into()],
        predicate: Some(predicate),
        items: vec![Projection {
            name: "result".into(),
            expression: ProjectionExpression::Literal(Value::Int(7)),
        }],
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(64 * 1024),
        blocking_operator_bytes: nz(4096),
        batch_payload_bytes: nz(4096),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let mut rows = 0;
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            for row in batch {
                assert_eq!(row.values["result"], Value::Int(7));
                rows += 1;
            }
            Ok(BatchControl::Continue)
        },
    );
    if refusal {
        assert!(
            matches!(result, Err(HawDBError::Execution(_))),
            "indexed candidate storage escaped its working budget: {result:?}"
        );
        assert_eq!(rows, 0, "refused pruning exposed a partial result");
    } else {
        assert_eq!(result.unwrap(), BatchControl::Continue);
        assert_eq!(rows, count);
    }
    assert_eq!(external.calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
}

fn pruning_equality() -> Predicate {
    Predicate::PropertyEq {
        variable: "n".into(),
        property: "kind".into(),
        value: Value::String("a".into()),
    }
}

#[test]
fn pruning_dense_index_candidates_refuse_before_result_delivery() {
    run_pruning_budget(pruning_equality(), 128, true);
}

#[test]
fn pruning_and_charges_simultaneous_candidate_sets() {
    run_pruning_budget(
        Predicate::And(vec![pruning_equality(), pruning_equality()]),
        32,
        true,
    );
}

#[test]
fn pruning_or_charges_simultaneous_candidate_sets() {
    run_pruning_budget(
        Predicate::Or(vec![pruning_equality(), pruning_equality()]),
        32,
        true,
    );
}

#[test]
fn pruning_single_candidate_set_within_budget_preserves_all_rows() {
    run_pruning_budget(pruning_equality(), 32, false);
}

#[test]
fn fused_projection_skips_unrequested_large_stored_property() {
    run_stored_property_projection(false, false);
}

#[test]
fn numeric_rows_projection_refuses_large_stored_property_before_copying() {
    run_stored_property_projection(true, true);
}

#[test]
fn property_access_projection_refuses_large_stored_property_before_copying() {
    run_stored_property_projection_with_access(
        false,
        true,
        NodeProjectionAccess::PropertyValues {
            property: "score".into(),
            values: vec![Value::Int(1)],
        },
    );
}

#[test]
fn property_access_projection_skips_unrequested_large_stored_property() {
    run_stored_property_projection_with_access(
        false,
        false,
        NodeProjectionAccess::PropertyValues {
            property: "score".into(),
            values: vec![Value::Int(1)],
        },
    );
}

#[test]
fn range_projection_refuses_large_selected_value_before_ownership() {
    run_stored_property_projection_with_source(
        false,
        true,
        NodeProjectionAccess::PropertyRange {
            property: "score".into(),
            lower: Some((Value::Int(1), true)),
            upper: Some((Value::Int(1), true)),
        },
        false,
    );
}

#[test]
fn range_projection_skips_unrequested_large_value() {
    run_stored_property_projection_with_source(
        false,
        false,
        NodeProjectionAccess::PropertyRange {
            property: "score".into(),
            lower: Some((Value::Int(1), true)),
            upper: Some((Value::Int(1), true)),
        },
        false,
    );
}

#[test]
fn composite_projection_refuses_large_selected_value_before_ownership() {
    run_stored_property_projection_with_source(
        false,
        true,
        NodeProjectionAccess::CompositeEquality {
            predicates: vec![("score".into(), Value::Int(1))],
        },
        false,
    );
}

#[test]
fn composite_projection_skips_unrequested_large_value() {
    run_stored_property_projection_with_source(
        false,
        false,
        NodeProjectionAccess::CompositeEquality {
            predicates: vec![("score".into(), Value::Int(1))],
        },
        false,
    );
}

#[test]
fn persisted_property_projection_refuses_large_selected_value_before_ownership() {
    run_stored_property_projection_with_source(
        false,
        true,
        NodeProjectionAccess::PropertyValues {
            property: "score".into(),
            values: vec![Value::Int(1)],
        },
        true,
    );
}

#[test]
fn persisted_property_projection_skips_unrequested_large_value() {
    run_stored_property_projection_with_source(
        false,
        false,
        NodeProjectionAccess::PropertyValues {
            property: "score".into(),
            values: vec![Value::Int(1)],
        },
        true,
    );
}

#[test]
fn persisted_label_projection_refuses_large_selected_value_before_ownership() {
    run_stored_property_projection_with_source(false, true, NodeProjectionAccess::LabelScan, true);
}

#[test]
fn persisted_label_projection_skips_unrequested_large_value() {
    run_stored_property_projection_with_source(false, false, NodeProjectionAccess::LabelScan, true);
}

#[test]
fn persisted_numeric_projection_refuses_large_selected_value_before_ownership() {
    run_stored_property_projection_with_source(true, true, NodeProjectionAccess::LabelScan, true);
}

#[test]
fn persisted_numeric_projection_skips_unrequested_large_value() {
    run_stored_property_projection_with_source(true, false, NodeProjectionAccess::LabelScan, true);
}

#[test]
fn text_projection_refuses_selected_nested_payloads_before_copying() {
    let size = 1024 * 1024 + 137;
    let blob = || ProjectionExpression::Literal(Value::String("X".repeat(size)));
    let expressions = vec![
        ProjectionExpression::Coalesce(vec![ProjectionExpression::Literal(Value::Null), blob()]),
        ProjectionExpression::Case {
            operand: None,
            branches: vec![(ProjectionExpression::Literal(Value::Bool(true)), blob())],
            otherwise: None,
        },
        ProjectionExpression::Lower(Box::new(blob())),
        ProjectionExpression::Left {
            expression: Box::new(blob()),
            length: size,
        },
        ProjectionExpression::Literal(Value::List(vec![Value::String("X".repeat(size))])),
        ProjectionExpression::Literal(Value::Map(BTreeMap::from([(
            "body".into(),
            Value::String("X".repeat(size)),
        )]))),
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column: "external_id".into(),
            empty: Value::Null,
            non_empty: Value::String("X".repeat(size)),
            null_or_empty: Value::Int(0),
        },
    ];
    for expression in expressions {
        run_refused(
            vec![Projection {
                name: "blob".into(),
                expression,
            }],
            "a".into(),
            size,
        );
    }
}

#[test]
fn text_projection_shrinks_borrowed_payload_and_skips_unused_branches() {
    let size = 1024 * 1024 + 137;
    let catalog = Catalog::default();
    let store = GraphStore::default();
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(64 * 1024),
        blocking_operator_bytes: nz(16 * 1024),
        batch_payload_bytes: nz(16 * 1024),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let input = seed(1);
    let observer = QueryExecutionObserver::new(&input);
    let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
    let external = TextSource {
        external_id: "a".into(),
        calls: Cell::new(0),
    };
    let allocations = Cell::new(0);
    let mut source = RuntimeSource {
        context: BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        watched_bytes: size,
        allocations: &allocations,
    };
    let blob = || ProjectionExpression::Literal(Value::String("X".repeat(size)));
    let items = vec![
        Projection {
            name: "short".into(),
            expression: ProjectionExpression::Left {
                expression: Box::new(blob()),
                length: 3,
            },
        },
        Projection {
            name: "case".into(),
            expression: ProjectionExpression::Case {
                operand: None,
                branches: vec![(ProjectionExpression::Literal(Value::Bool(false)), blob())],
                otherwise: Some(Box::new(ProjectionExpression::Literal(Value::Int(7)))),
            },
        },
        Projection {
            name: "coalesce".into(),
            expression: ProjectionExpression::Coalesce(vec![
                ProjectionExpression::Literal(Value::String("ok".into())),
                blob(),
            ]),
        },
    ];
    let context = BatchExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &observer,
    };
    let rows = Cell::new(0);
    let result = stream_projection_batches(
        &items,
        &input,
        &mut source,
        context,
        ExecutionLimit::unlimited(),
        &mut |batch| {
            for row in batch {
                assert_eq!(row.values["short"], Value::String("XXX".into()));
                assert_eq!(row.values["case"], Value::Int(7));
                assert_eq!(row.values["coalesce"], Value::String("ok".into()));
                rows.set(rows.get() + 1);
            }
            Ok(BatchControl::Continue)
        },
    );
    assert_eq!(result.unwrap(), BatchControl::Continue);
    assert_eq!(rows.get(), 1);
    assert_eq!(
        allocations.get(),
        0,
        "shrinking projection copied the unneeded full payload"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn text_projection_batches_actual_payload_with_exact_rows_and_stop() {
    for stop in [false, true] {
        let catalog = Catalog::default();
        let store = GraphStore::default();
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: nz(64 * 1024),
            blocking_operator_bytes: nz(16 * 1024),
            batch_payload_bytes: nz(16 * 1024),
            batch_rows: nz(8192),
            ..Default::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let input = seed(3);
        let observer = QueryExecutionObserver::new(&input);
        let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
        let external = TextSource {
            external_id: "x".repeat(1024),
            calls: Cell::new(0),
        };
        let allocations = Cell::new(0);
        let mut source = RuntimeSource {
            context: BatchReadContext {
                catalog: &catalog,
                store: &store,
                parameters: &parameters,
                external: &external,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &observer,
                host_scorer: None,
            },
            watched_bytes: 0,
            allocations: &allocations,
        };
        let items: Vec<_> = (0..8)
            .map(|alias| Projection {
                name: format!("blob{alias}"),
                expression: ProjectionExpression::Column("external_id".into()),
            })
            .collect();
        let context = BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
        };
        let rows = Cell::new(0);
        let result = stream_projection_batches(
            &items,
            &input,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                let bytes: usize = batch.iter().map(binding_memory_bytes).sum();
                assert!(
                    bytes <= memory.batch_payload_bytes.get(),
                    "projected batch actual payload {bytes} exceeds admitted {}",
                    memory.batch_payload_bytes
                );
                for Binding { values, .. } in &batch {
                    assert_eq!(values.len(), 8);
                    assert!(values
                        .values()
                        .all(|value| value == &Value::String(external.external_id.clone())));
                }
                rows.set(rows.get() + batch.len());
                Ok(if stop {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            },
        );
        assert_eq!(
            result.unwrap(),
            if stop {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            }
        );
        assert_eq!(rows.get(), if stop { 1 } else { 3 });
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[derive(Clone, Copy)]
enum NumericBufferCase {
    Stop,
    Error,
    Refusal,
    RetainedRows,
    TypedScratch,
}

fn run_persisted_numeric_buffer(case: NumericBufferCase) {
    let directory = StoredPropertyDirectory::new();
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut catalog,
        Default::default(),
        replay,
    )
    .unwrap();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    for (property, kind) in [("score", PropertyType::Int), ("body", PropertyType::String)] {
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                "Memory",
                property,
                kind,
                false,
            )
            .unwrap();
    }
    let count = if matches!(case, NumericBufferCase::RetainedRows) {
        40
    } else {
        2
    };
    let size = 1024 * 1024 + 137;
    let mut expected_ids = Vec::new();
    for index in 0..count {
        let body = if index == 1
            && matches!(
                case,
                NumericBufferCase::Stop | NumericBufferCase::Error | NumericBufferCase::Refusal
            ) {
            "X".repeat(size)
        } else {
            "a".repeat(1041)
        };
        expected_ids.push(
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("score".into(), Value::Int(1)),
                        ("body".into(), Value::String(body)),
                    ]),
                )
                .unwrap(),
        );
    }
    store.checkpoint(&catalog).unwrap();
    drop(store);
    catalog = Catalog::default();
    let store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut catalog,
        Default::default(),
        replay,
    )
    .unwrap();
    assert!(store.is_out_of_core());
    let typed = matches!(case, NumericBufferCase::TypedScratch);
    let plan = PhysicalPlan::ProjectExec {
        items: vec![
            Projection {
                name: "id".into(),
                expression: ProjectionExpression::Id {
                    variable: "n".into(),
                },
            },
            Projection {
                name: "result".into(),
                expression: if typed {
                    ProjectionExpression::Literal(Value::Int(7))
                } else {
                    ProjectionExpression::Property {
                        variable: "n".into(),
                        property: "body".into(),
                    }
                },
            },
        ],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyEq {
                variable: "n".into(),
                property: "score".into(),
                value: Value::Int(1),
            },
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".into(),
                label: "Memory".into(),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(8192),
        batch_payload_bytes: nz(4096),
        batch_rows: nz(if matches!(case, NumericBufferCase::RetainedRows) {
            1
        } else if typed {
            2
        } else {
            8192
        }),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let mut ids = Vec::new();
    let mut callbacks = 0;
    let window = AllocationWindow::start(size);
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            callbacks += 1;
            if matches!(
                case,
                NumericBufferCase::RetainedRows | NumericBufferCase::TypedScratch
            ) {
                let class = if typed {
                    hawdb_executor::QueryMemoryClass::PipelineBatch
                } else {
                    hawdb_executor::QueryMemoryClass::BlockingState
                };
                let retained = ledger
                    .snapshot()
                    .classes
                    .iter()
                    .find(|entry| entry.class == class)
                    .map_or(0, |entry| entry.used_bytes);
                let output_bytes = batch.iter().map(binding_memory_bytes).sum::<usize>();
                assert!(
                    retained >= if typed { output_bytes + 32 } else { 2065 },
                    "numeric buffer lost admission while its input remained owned: {retained}"
                );
            }
            for row in batch {
                ids.push(row.values["id"].clone());
                assert_eq!(
                    row.values["result"],
                    if typed {
                        Value::Int(7)
                    } else {
                        Value::String("a".repeat(1041))
                    }
                );
            }
            match case {
                NumericBufferCase::Stop => Ok(BatchControl::Stop),
                NumericBufferCase::Error => {
                    Err(HawDBError::Execution("numeric consumer failed".into()))
                }
                _ => Ok(BatchControl::Continue),
            }
        },
    );
    assert_eq!(
        window.allocations(),
        0,
        "numeric next payload was owned before Stop/refusal"
    );
    drop(window);
    match case {
        NumericBufferCase::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
        NumericBufferCase::Error => assert!(
            matches!(result, Err(HawDBError::Execution(message)) if message == "numeric consumer failed")
        ),
        NumericBufferCase::Refusal => assert!(matches!(result, Err(HawDBError::Execution(_)))),
        _ => assert_eq!(result.unwrap(), BatchControl::Continue),
    }
    let expected_count = if matches!(
        case,
        NumericBufferCase::RetainedRows | NumericBufferCase::TypedScratch
    ) {
        count
    } else {
        1
    };
    assert_eq!(
        ids,
        expected_ids[..expected_count]
            .iter()
            .map(|id| Value::Int(id.0 as i64))
            .collect::<Vec<_>>()
    );
    assert!(callbacks > 0);
    assert_eq!(external.calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    // Probe the same persisted source after caller Stop/refusal/error.
    assert_eq!(
        store
            .node_owned(expected_ids[0])
            .unwrap()
            .unwrap()
            .properties["body"],
        Value::String("a".repeat(1041))
    );
}

#[test]
fn owned_numeric_buffer_stops_before_next_large_payload() {
    run_persisted_numeric_buffer(NumericBufferCase::Stop);
}
#[test]
fn owned_numeric_buffer_preserves_consumer_error_without_next_payload() {
    run_persisted_numeric_buffer(NumericBufferCase::Error);
}
#[test]
fn owned_numeric_buffer_refuses_next_payload_and_releases_admission() {
    run_persisted_numeric_buffer(NumericBufferCase::Refusal);
}
#[test]
fn owned_numeric_buffer_retains_row_permits_during_consumption() {
    run_persisted_numeric_buffer(NumericBufferCase::RetainedRows);
}
#[test]
fn owned_numeric_buffer_retains_typed_scratch_during_consumption() {
    run_persisted_numeric_buffer(NumericBufferCase::TypedScratch);
}

#[test]
fn owned_numeric_buffer_refuses_typed_capacity_before_allocation() {
    use hawdb_executor::numeric::{
        stream_owned_typed_numeric_nodes, LendingNumericScan, NumericExecutionContext,
        NumericFragment,
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("score".into(), Value::Int(1))]),
        )
        .unwrap();
    let plan = PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Memory".into(),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(128),
        batch_payload_bytes: nz(4096),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let items = vec![Projection {
        name: "result".into(),
        expression: ProjectionExpression::Literal(Value::Int(7)),
    }];
    let window = AllocationWindow::start(32 * std::mem::size_of::<i64>());
    let result = stream_owned_typed_numeric_nodes(
        NumericFragment {
            label: "Memory",
            property: "score",
            property_type: PropertyType::Int,
            predicate: hawdb_executor::NumericPredicate::Eq,
            expected: hawdb_executor::NumericLiteral::Int(1),
            fused_operators: None,
        },
        &items,
        catalog.label_id("Memory").unwrap(),
        LendingNumericScan {
            batch_rows: 32,
            needs_node_ids: true,
        },
        NumericExecutionContext {
            catalog: &catalog,
            store: &store,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
        },
        ExecutionLimit::unlimited(),
        &mut |_| panic!("refused numeric capacity reached consumer"),
    );
    assert_eq!(
        window.allocations(),
        0,
        "typed numeric columns allocated before scratch admission"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn owned_numeric_buffer_refuses_small_validity_capacity_before_allocation() {
    let directory = StoredPropertyDirectory::new();
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut catalog,
        Default::default(),
        replay,
    )
    .unwrap();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "score",
            PropertyType::Int,
            true,
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("score".into(), Value::Null)]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    catalog = Catalog::default();
    let store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut catalog,
        Default::default(),
        replay,
    )
    .unwrap();
    assert!(store.is_out_of_core());
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "result".into(),
            expression: ProjectionExpression::Literal(Value::Int(7)),
        }],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyEq {
                variable: "n".into(),
                property: "score".into(),
                value: Value::Int(1),
            },
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".into(),
                label: "Memory".into(),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(8192),
        batch_payload_bytes: nz(32),
        batch_rows: nz(1),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    // The first lazy bitmap reserves four u64 slots. A 32-byte payload budget
    // cannot also retain its scalar column and selection storage.
    let window = AllocationWindow::start(4 * std::mem::size_of::<u64>());
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |_| panic!("NULL numeric predicate returned a row"),
    );
    assert_eq!(
        window.allocations(),
        0,
        "numeric validity bitmap allocated outside its scratch budget"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[derive(Clone, Copy)]
enum FullNodeSource {
    Scan,
    Index,
    Lookup(bool),
    IndexKey,
    LookupKey,
}

fn run_full_node_scan_admission(persisted: bool, materialized: bool) {
    run_full_node_source_admission(persisted, materialized, FullNodeSource::Scan)
}

fn run_full_node_source_admission(persisted: bool, materialized: bool, source: FullNodeSource) {
    let size = 1024 * 1024 + 137;
    let key = if matches!(source, FullNodeSource::IndexKey | FullNodeSource::LookupKey) {
        "K".repeat(size)
    } else {
        "target".into()
    };
    let directory = persisted.then(StoredPropertyDirectory::new);
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "body",
            PropertyType::String,
            false,
        )
        .unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "id",
            PropertyType::String,
            false,
        )
        .unwrap();
    store
        .create_property_index(&mut catalog, "Memory", "id")
        .unwrap();
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("body".into(), Value::String("X".repeat(size))),
                ("id".into(), Value::String(key.clone())),
            ]),
        )
        .unwrap();
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap();
    }
    assert_eq!(store.is_out_of_core(), persisted);
    let plan = if matches!(source, FullNodeSource::Index | FullNodeSource::IndexKey) {
        PhysicalPlan::IndexNodeSeek {
            variable: "n".into(),
            label: "Memory".into(),
            property: "id".into(),
            value: Value::String(key.clone()),
        }
    } else {
        PhysicalPlan::SeqNodeScan {
            variable: "n".into(),
            label: "Memory".into(),
        }
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(4096),
        batch_payload_bytes: nz(4096),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let rows = Cell::new(0);
    let predicate_calls = Cell::new(0);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let input_account = ledger.account(
        hawdb_executor::QueryMemoryClass::BlockingState,
        "full node input guard",
        memory.blocking_operator_bytes,
    );
    let batch_account = ledger.account(
        hawdb_executor::QueryMemoryClass::PipelineBatch,
        "full node batch guard",
        memory.batch_payload_bytes,
    );
    let lookup_input = vec![hawdb_executor::binding::Binding::values(BTreeMap::from([
        ("seed".into(), Value::String(key.clone())),
    ]))];
    let window = AllocationWindow::start(size);
    let result = if matches!(
        source,
        FullNodeSource::Lookup(_) | FullNodeSource::LookupKey
    ) {
        let indexed = !matches!(source, FullNodeSource::Lookup(false));
        hawdb_executor::scan::execute_node_column_lookup(
            hawdb_executor::scan::NodeColumnLookupSpec {
                variable: "n",
                label: if indexed { "Memory" } else { "" },
                property: "id",
                column: "seed",
                optional: false,
                node_visibility_predicate: None,
            },
            lookup_input,
            hawdb_executor::scan::NodeScanContext {
                catalog: &catalog,
                store: &store,
                execution_limit: ExecutionLimit::unlimited(),
                memory_budget: memory.blocking_operator_bytes,
                memory_account: &input_account,
                batch_memory_budget: memory.batch_payload_bytes,
                batch_memory_account: &batch_account,
                batch_rows: memory.batch_rows.get(),
                task_context: None,
            },
            &observer,
        )
        .map(|output| {
            rows.set(output.len());
            BatchControl::Continue
        })
    } else if materialized {
        hawdb_executor::scan::execute_node_scan(
            hawdb_executor::scan::NodeScanSpec {
                variable: "n",
                label: "Memory",
                property_filter: None,
            },
            hawdb_executor::scan::NodeScanContext {
                catalog: &catalog,
                store: &store,
                execution_limit: ExecutionLimit::unlimited(),
                memory_budget: memory.blocking_operator_bytes,
                memory_account: &input_account,
                batch_memory_budget: memory.batch_payload_bytes,
                batch_memory_account: &batch_account,
                batch_rows: memory.batch_rows.get(),
                task_context: None,
            },
            &mut |_| {
                predicate_calls.set(predicate_calls.get() + 1);
                Ok(true)
            },
            &observer,
        )
        .map(|output| {
            rows.set(output.len());
            BatchControl::Continue
        })
    } else {
        execute_binding_batches(
            &plan,
            BatchReadContext {
                catalog: &catalog,
                store: &store,
                parameters: &parameters,
                external: &external,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &observer,
                host_scorer: None,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                rows.set(rows.get() + batch.len());
                Ok(BatchControl::Continue)
            },
        )
    };
    assert_eq!(
        window.allocations(),
        0,
        "full node payload was copied before query admission"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(rows.get(), 0);
    assert_eq!(predicate_calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(external.calls.get(), 0);
    // Caller refusal must not mark the pinned canonical reader corrupt.
    assert_eq!(store.node_owned(id).unwrap().unwrap().id, id);
}

#[test]
fn full_node_scan_admits_resident_streaming_payload_before_clone() {
    run_full_node_scan_admission(false, false);
}

#[test]
fn full_node_scan_admits_persisted_streaming_payload_before_decode() {
    run_full_node_scan_admission(true, false);
}

#[test]
fn full_node_scan_admits_resident_materialized_payload_before_clone() {
    run_full_node_scan_admission(false, true);
}

#[test]
fn full_node_scan_admits_persisted_materialized_payload_before_decode() {
    run_full_node_scan_admission(true, true);
}

#[derive(Clone, Copy)]
enum FullNodeBufferCase {
    Stop,
    Error,
    Refusal,
    Retained,
}

fn run_full_node_scan_buffer(persisted: bool, case: FullNodeBufferCase) {
    let directory = persisted.then(StoredPropertyDirectory::new);
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "body",
            PropertyType::String,
            false,
        )
        .unwrap();
    let retained = matches!(case, FullNodeBufferCase::Retained);
    let size = 1024 * 1024 + 137;
    let mut expected = Vec::new();
    for index in 0..if retained { 40 } else { 2 } {
        let body = if !retained && index == 1 {
            "X".repeat(size)
        } else {
            "a".repeat(1041)
        };
        expected.push(
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([("body".into(), Value::String(body))]),
                )
                .unwrap(),
        );
    }
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap();
    }
    assert_eq!(store.is_out_of_core(), persisted);
    let plan = PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Memory".into(),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(8192),
        batch_payload_bytes: nz(4096),
        batch_rows: nz(if retained { 1 } else { 8192 }),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let mut delivered = Vec::new();
    let window = AllocationWindow::start(size);
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            let retained_bytes = ledger
                .snapshot()
                .classes
                .iter()
                .find(|entry| entry.class == hawdb_executor::QueryMemoryClass::BlockingState)
                .map_or(0, |class| class.used_bytes);
            for row in batch {
                assert_eq!(
                    row.nodes["n"].properties["body"],
                    Value::String("a".repeat(1041))
                );
                delivered.push(row.nodes["n"].id);
                if retained {
                    assert!(
                        retained_bytes >= 1041,
                        "buffered node lost its payload permit: {retained_bytes}"
                    );
                }
            }
            match case {
                FullNodeBufferCase::Stop => Ok(BatchControl::Stop),
                FullNodeBufferCase::Error => {
                    Err(HawDBError::Execution("full node consumer sentinel".into()))
                }
                FullNodeBufferCase::Refusal | FullNodeBufferCase::Retained => {
                    Ok(BatchControl::Continue)
                }
            }
        },
    );
    assert_eq!(
        window.allocations(),
        0,
        "full-node buffer owned the next large payload before terminal Stop/error/refusal"
    );
    drop(window);
    match case {
        FullNodeBufferCase::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
        FullNodeBufferCase::Error => assert!(
            matches!(result, Err(HawDBError::Execution(message)) if message == "full node consumer sentinel")
        ),
        FullNodeBufferCase::Refusal => assert!(matches!(result, Err(HawDBError::Execution(_)))),
        FullNodeBufferCase::Retained => assert_eq!(result.unwrap(), BatchControl::Continue),
    }
    assert_eq!(
        delivered.as_slice(),
        if retained {
            expected.as_slice()
        } else {
            &expected[..1]
        }
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(external.calls.get(), 0);
    assert_eq!(
        store.node_owned(expected[0]).unwrap().unwrap().id,
        expected[0]
    );
}

#[test]
fn full_node_scan_resident_buffer_stops_before_next_payload() {
    run_full_node_scan_buffer(false, FullNodeBufferCase::Stop);
}

#[test]
fn full_node_scan_persisted_buffer_stops_before_next_payload() {
    run_full_node_scan_buffer(true, FullNodeBufferCase::Stop);
}

#[test]
fn full_node_scan_resident_buffer_preserves_consumer_error() {
    run_full_node_scan_buffer(false, FullNodeBufferCase::Error);
}

#[test]
fn full_node_scan_persisted_buffer_preserves_consumer_error() {
    run_full_node_scan_buffer(true, FullNodeBufferCase::Error);
}

#[test]
fn full_node_scan_resident_buffer_refuses_without_next_payload() {
    run_full_node_scan_buffer(false, FullNodeBufferCase::Refusal);
}

#[test]
fn full_node_scan_persisted_buffer_refuses_without_next_payload() {
    run_full_node_scan_buffer(true, FullNodeBufferCase::Refusal);
}

#[test]
fn full_node_scan_resident_buffer_retains_payload_permit() {
    run_full_node_scan_buffer(false, FullNodeBufferCase::Retained);
}

#[test]
fn full_node_scan_persisted_buffer_retains_payload_permit() {
    run_full_node_scan_buffer(true, FullNodeBufferCase::Retained);
}

#[derive(Clone, Copy)]
enum PointReadCase {
    GraphImport,
    OneHop,
    Bounded,
}

fn run_point_read_admission(persisted: bool, case: PointReadCase) {
    let size = 1024 * 1024 + 137;
    let directory = persisted.then(StoredPropertyDirectory::new);
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    store.create_node_table(&mut catalog, "Memory").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Memory",
            "body",
            PropertyType::String,
            false,
        )
        .unwrap();
    let source = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("body".into(), Value::String(String::new()))]),
        )
        .unwrap();
    let target = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("body".into(), Value::String("X".repeat(size)))]),
        )
        .unwrap();
    store
        .create_relationship_table(&mut catalog, "LINKS")
        .unwrap();
    store
        .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
        .unwrap();
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap();
    }
    // The real text producer admits its small binding with a 4096-byte
    // envelope. Let that input reach MATCH without admitting the 1 MiB node.
    let point_budget = if matches!(case, PointReadCase::GraphImport) {
        16 * 1024
    } else {
        4096
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(point_budget),
        batch_payload_bytes: nz(point_budget),
        batch_rows: nz(1),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut input = seed(1);
    if let PhysicalPlan::TextSeedScan {
        resource_profile, ..
    } = &mut input
    {
        resource_profile.max_working_memory_bytes = Some(1024);
    }
    let plan = PhysicalPlan::GraphMatchExec {
        program: hawdb_plan_cypher::GraphMatchProgram {
            imports: vec![hawdb_plan_cypher::GraphBindingImport {
                variable: "n".into(),
                column: "entity".into(),
                kind: hawdb_plan_cypher::GraphEntityKind::Node,
            }],
            introduced: Vec::new(),
            steps: Vec::new(),
            predicate: None,
            optional: false,
        },
        input: Some(Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "entity".into(),
                expression: ProjectionExpression::Literal(Value::Map(BTreeMap::from([(
                    "_id".into(),
                    Value::Int(target.0 as i64),
                )]))),
            }],
            input: Box::new(input),
        })),
    };
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
    let external = TextSource {
        external_id: "small".into(),
        calls: Cell::new(0),
    };
    let point_account = ledger.account(
        hawdb_executor::QueryMemoryClass::BlockingState,
        "point read guard",
        memory.blocking_operator_bytes,
    );
    let rows = Cell::new(0);
    let label_ids = [catalog.label_id("Memory").unwrap()];
    let rel_type_id = catalog.rel_type_id("LINKS").unwrap();
    let rel_properties = BTreeMap::new();
    let window = AllocationWindow::start(size);
    let result = match case {
        PointReadCase::GraphImport => execute_binding_batches(
            &plan,
            BatchReadContext {
                catalog: &catalog,
                store: &store,
                parameters: &parameters,
                external: &external,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &observer,
                host_scorer: None,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                rows.set(rows.get() + batch.len());
                Ok(BatchControl::Continue)
            },
        ),
        PointReadCase::OneHop => {
            hawdb_executor::traversal::visit_one_hop_relationships_with_budget(
                &store,
                hawdb_executor::traversal::OneHopRelationshipSpec {
                    source,
                    rel_type_id: Some(rel_type_id),
                    target_label_ids: Some(&label_ids),
                    rel_properties: &rel_properties,
                    relationship_scan_filter: None,
                    direction: hawdb_core::RelationshipDirection::Outgoing,
                },
                hawdb_executor::store::AdjacencyReadMemory {
                    budget_bytes: memory.blocking_operator_bytes.get(),
                    account: Some(&point_account),
                },
                &observer,
                &mut |_, _| {
                    rows.set(rows.get() + 1);
                    Ok(hawdb_executor::store::ScanControl::Continue)
                },
            )
            .map(|_| BatchControl::Continue)
        }
        PointReadCase::Bounded => hawdb_executor::traversal::visit_bounded_expand_targets(
            &store,
            hawdb_executor::traversal::BoundedExpandSpec {
                source,
                rel_type_id,
                target_label_ids: Some(&label_ids),
                min_hops: 1,
                max_hops: 1,
            },
            hawdb_executor::store::AdjacencyReadMemory {
                budget_bytes: memory.blocking_operator_bytes.get(),
                account: Some(&point_account),
            },
            None,
            &mut |_, _| {
                rows.set(rows.get() + 1);
                Ok(hawdb_executor::store::ScanControl::Continue)
            },
        )
        .map(|_| BatchControl::Continue),
    };
    assert_eq!(
        window.allocations(),
        0,
        "graph point hydration copied its payload before admission"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(rows.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(store.node_owned(target).unwrap().unwrap().id, target);
}

#[test]
fn point_read_admits_resident_graph_import_before_clone() {
    run_point_read_admission(false, PointReadCase::GraphImport);
}

#[test]
fn point_read_admits_persisted_graph_import_before_decode() {
    run_point_read_admission(true, PointReadCase::GraphImport);
}

#[test]
fn point_read_admits_resident_one_hop_before_clone() {
    run_point_read_admission(false, PointReadCase::OneHop);
}

#[test]
fn point_read_admits_persisted_one_hop_before_decode() {
    run_point_read_admission(true, PointReadCase::OneHop);
}

#[test]
fn point_read_admits_resident_bounded_expand_before_clone() {
    run_point_read_admission(false, PointReadCase::Bounded);
}

#[test]
fn point_read_admits_persisted_bounded_expand_before_decode() {
    run_point_read_admission(true, PointReadCase::Bounded);
}

#[test]
fn numeric_graph_features_do_not_allocate_property_text() {
    use hawdb_executor::graph_seed::numeric_property;
    use hawdb_storage::{NodeId, NodeRecord};
    let numeric_text = format!("{}1.25", "0".repeat(4096 + 137));
    let node = NodeRecord {
        id: NodeId(42),
        labels: Default::default(),
        properties: BTreeMap::from([
            ("id".into(), Value::String(numeric_text.clone())),
            ("source_id".into(), Value::Null),
            (
                "thread_id".into(),
                Value::List(vec![Value::String(numeric_text.clone())]),
            ),
            ("space_id".into(), Value::String(numeric_text)),
            (
                "nested".into(),
                Value::List(vec![Value::List(vec![Value::Int(17)])]),
            ),
            ("binary".into(), Value::Binary(vec![17; 4096])),
            (
                "map".into(),
                Value::Map(BTreeMap::from([("x".repeat(4096), Value::Int(17))])),
            ),
            (
                "list".into(),
                Value::List(vec![Value::String("0".repeat(4096)), Value::Int(17)]),
            ),
        ]),
    };
    for (property, expected) in [
        ("external_id", Some(1.25)),
        ("source_id", Some(1.25)),
        ("space_id", Some(1.25)),
        ("nested", Some(17.0)),
        ("binary", None),
        ("map", None),
        ("list", None),
        ("missing", None),
    ] {
        // usize::MAX observes every allocation on this thread, excluding the
        // fixture and assertion machinery outside the feature read itself.
        let window = AllocationWindow::start(usize::MAX);
        let actual = numeric_property(&node, property);
        let allocations = window.allocations();
        drop(window);
        assert_eq!(actual, expected, "numeric semantics for {property}");
        assert_eq!(
            allocations, 0,
            "numeric feature {property} allocated property text"
        );
    }
}

#[derive(Clone, Copy)]
enum RelationshipReadCase {
    PointRefusal,
    OrderedRefusal,
    Retained,
    PointStop,
    OrderedStop,
}

fn run_relationship_owned_read(persisted: bool, case: RelationshipReadCase) {
    use hawdb_executor::store::{
        admit_graph_read, AdjacencyReadMemory, GraphExecutionRead, ScanControl,
    };
    use hawdb_storage::adjacency::AdjacencyDirection;
    use hawdb_storage::read_view::AdmittedRelationshipRead;
    let retained = matches!(case, RelationshipReadCase::Retained);
    let size = if retained {
        12 * 1024 + 137
    } else {
        64 * 1024 + 137
    };
    let directory = persisted.then(StoredPropertyDirectory::new);
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    store.create_node_table(&mut catalog, "Memory").unwrap();
    let source = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    store
        .create_relationship_table(&mut catalog, "LINKS")
        .unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Relationship,
            "LINKS",
            "body",
            PropertyType::String,
            false,
        )
        .unwrap();
    let relationship_id = store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "LINKS",
            BTreeMap::from([("body".into(), Value::String("X".repeat(size)))]),
        )
        .unwrap();
    if retained {
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "LINKS",
                BTreeMap::from([("body".into(), Value::String("Y".repeat(size)))]),
            )
            .unwrap();
    }
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap();
    }
    assert_eq!(store.is_out_of_core(), persisted);
    let limit = nz(if retained { 24 * 1024 } else { 8 * 1024 });
    let ledger = QueryMemoryLedger::new(limit);
    let account = ledger.account(
        hawdb_executor::QueryMemoryClass::BlockingState,
        "relationship source allocation regression",
        limit,
    );
    let stop = matches!(
        case,
        RelationshipReadCase::PointStop | RelationshipReadCase::OrderedStop
    );
    let mut admit = |bytes| {
        if stop && bytes > 0 {
            Ok(None)
        } else {
            admit_graph_read(&account, None, bytes).map(Some)
        }
    };
    let mut buffered = Vec::new();
    let window = AllocationWindow::start(size);
    let result: Result<()> = match case {
        RelationshipReadCase::PointRefusal | RelationshipReadCase::PointStop => {
            GraphExecutionRead::relationship_with_allocation(&store, relationship_id, &mut admit)
                .map(|read| {
                    if stop {
                        assert!(matches!(read, AdmittedRelationshipRead::Stopped));
                    }
                })
        }
        _ => GraphExecutionRead::visit_ordered_adjacent_relationships_with_allocation(
            &store,
            source,
            None,
            AdjacencyDirection::Outgoing,
            AdjacencyReadMemory {
                budget_bytes: limit.get(),
                account: Some(&account),
            },
            &mut admit,
            &mut |relationship| {
                buffered.push(relationship);
                Ok(ScanControl::Continue)
            },
        )
        .map(|control| {
            if stop {
                assert_eq!(control, ScanControl::Stop);
            }
        }),
    };
    let allocations = window.allocations();
    drop(window);
    assert_eq!(allocations, usize::from(retained), "relationship payload was copied before admission or after a buffered source lost its permit");
    if stop {
        assert!(result.is_ok());
    } else {
        assert!(matches!(result, Err(HawDBError::Execution(_))));
    }
    assert_eq!(buffered.len(), usize::from(retained));
    if retained {
        assert!(
            ledger.snapshot().used_bytes >= size,
            "buffered owned source lost its admission lease"
        );
    }
    drop(buffered);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= limit.get());
    let recovered = store.relationship_owned(relationship_id).unwrap().unwrap();
    assert!(
        matches!(recovered.properties.get("body"), Some(Value::String(body)) if body.len() == size)
    );
}

macro_rules! relationship_read_guard {
    ($name:ident, $persisted:expr, $case:ident) => {
        #[test]
        fn $name() {
            run_relationship_owned_read($persisted, RelationshipReadCase::$case);
        }
    };
}
relationship_read_guard!(
    relationship_owned_read_live_point_refuses_before_copy,
    false,
    PointRefusal
);
relationship_read_guard!(
    relationship_owned_read_canonical_point_refuses_before_copy,
    true,
    PointRefusal
);
relationship_read_guard!(
    relationship_owned_read_live_ordered_refuses_before_copy,
    false,
    OrderedRefusal
);
relationship_read_guard!(
    relationship_owned_read_canonical_ordered_refuses_before_copy,
    true,
    OrderedRefusal
);
relationship_read_guard!(
    relationship_owned_read_live_retains_buffered_source_permits,
    false,
    Retained
);
relationship_read_guard!(
    relationship_owned_read_canonical_retains_buffered_source_permits,
    true,
    Retained
);
relationship_read_guard!(
    relationship_owned_read_live_point_stops_before_copy,
    false,
    PointStop
);
relationship_read_guard!(
    relationship_owned_read_canonical_point_stops_before_copy,
    true,
    PointStop
);
relationship_read_guard!(
    relationship_owned_read_live_ordered_stops_before_copy,
    false,
    OrderedStop
);
relationship_read_guard!(
    relationship_owned_read_canonical_ordered_stops_before_copy,
    true,
    OrderedStop
);

#[test]
fn relationship_owned_read_typed_window_streams_without_degree_sized_key_buffer() {
    use hawdb_executor::store::{
        admit_graph_read, AdjacencyReadMemory, GraphExecutionRead, ScanControl,
    };
    use hawdb_storage::adjacency::AdjacencyDirection;
    for persisted in [false, true] {
        let directory = persisted.then(StoredPropertyDirectory::new);
        let replay = hawdb_storage::store::WalReplayConfig {
            residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
            ..Default::default()
        };
        let mut catalog = Catalog::default();
        let mut store = match &directory {
            Some(directory) => GraphStore::open_with_durability_and_replay_config(
                &directory.0,
                &mut catalog,
                Default::default(),
                replay,
            )
            .unwrap(),
            None => GraphStore::default(),
        };
        store.create_node_table(&mut catalog, "Memory").unwrap();
        store
            .create_relationship_table(&mut catalog, "LINKS")
            .unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let mut expected = Vec::new();
        for index in 0..1000 {
            let id = store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
            if index < 2 {
                expected.push(id);
            }
        }
        if let Some(directory) = &directory {
            store.checkpoint(&catalog).unwrap();
            drop(store);
            catalog = Catalog::default();
            store = GraphStore::open_with_durability_and_replay_config(
                &directory.0,
                &mut catalog,
                Default::default(),
                replay,
            )
            .unwrap();
            // A live tail must merge after the pinned canonical posting list.
            store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
        }
        let ledger = QueryMemoryLedger::new(nz(4096));
        let account = ledger.account(
            hawdb_executor::QueryMemoryClass::BlockingState,
            "typed adjacency window",
            nz(4096),
        );
        let mut selected = Vec::new();
        let control = GraphExecutionRead::visit_ordered_adjacent_relationships_with_allocation(
            &store,
            source,
            Some(catalog.rel_type_id("LINKS").unwrap()),
            AdjacencyDirection::Outgoing,
            AdjacencyReadMemory {
                budget_bytes: 0,
                account: Some(&account),
            },
            &mut |bytes| admit_graph_read(&account, None, bytes).map(Some),
            &mut |relationship| {
                selected.push(relationship);
                Ok(if selected.len() == 2 {
                    ScanControl::Stop
                } else {
                    ScanControl::Continue
                })
            },
        )
        .unwrap();
        assert_eq!(control, ScanControl::Stop);
        assert!(ledger.snapshot().used_bytes > 0);
        let actual: Vec<_> = selected
            .into_iter()
            .map(|row| row.into_parts().0.id)
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn relationship_owned_read_filtered_ordering_admits_before_native_or_indexed_copy() {
    use hawdb_executor::store::{AdjacencyReadMemory, GraphExecutionRead, ScanControl};
    use hawdb_storage::adjacency::AdjacencyDirection;
    use hawdb_storage::mutation::PropertyFilter;
    let size = 64 * 1024 + 151;
    for persisted in [false, true] {
        let directory = persisted.then(StoredPropertyDirectory::new);
        let replay = hawdb_storage::store::WalReplayConfig {
            residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
            ..Default::default()
        };
        let mut catalog = Catalog::default();
        let mut store = match &directory {
            Some(directory) => GraphStore::open_with_durability_and_replay_config(
                &directory.0,
                &mut catalog,
                Default::default(),
                replay,
            )
            .unwrap(),
            None => GraphStore::default(),
        };
        store.create_node_table(&mut catalog, "Memory").unwrap();
        store
            .create_relationship_table(&mut catalog, "LINKS")
            .unwrap();
        for property in ["body", "status"] {
            store
                .create_property_descriptor(
                    &mut catalog,
                    TableKind::Relationship,
                    "LINKS",
                    property,
                    PropertyType::String,
                    false,
                )
                .unwrap();
        }
        let source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let selected = store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "LINKS",
                BTreeMap::from([
                    ("body".into(), Value::String("X".repeat(size))),
                    ("status".into(), Value::String("active".into())),
                ]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "LINKS",
                BTreeMap::from([
                    ("body".into(), Value::String(String::new())),
                    ("status".into(), Value::String("inactive".into())),
                ]),
            )
            .unwrap();
        if let Some(directory) = &directory {
            store.checkpoint(&catalog).unwrap();
            drop(store);
            catalog = Catalog::default();
            store = GraphStore::open_with_durability_and_replay_config(
                &directory.0,
                &mut catalog,
                Default::default(),
                replay,
            )
            .unwrap();
        }
        let limit = nz(8 * 1024);
        let ledger = QueryMemoryLedger::new(limit);
        let account = ledger.account(
            hawdb_executor::QueryMemoryClass::BlockingState,
            "filtered relationship source",
            limit,
        );
        let filter = PropertyFilter::Eq {
            property: "status".into(),
            value: Value::String("active".into()),
        };
        let window = AllocationWindow::start(size);
        let result = GraphExecutionRead::visit_ordered_adjacent_relationships_with_filter_owned(
            &store,
            source,
            Some(catalog.rel_type_id("LINKS").unwrap()),
            AdjacencyDirection::Outgoing,
            &filter,
            AdjacencyReadMemory {
                budget_bytes: limit.get(),
                account: Some(&account),
            },
            &mut |_| panic!("oversized relationship must be refused before consumer"),
        );
        let copies = window.allocations();
        drop(window);
        assert_eq!(copies, 0, "filtered source copied before query admission");
        assert!(matches!(result, Err(HawDBError::Execution(_))));
        assert_eq!(ledger.snapshot().used_bytes, 0);
        // Refusal must preserve the canonical reader's health and index mapping.
        assert_eq!(
            store.relationship_owned(selected).unwrap().unwrap().id,
            selected
        );
        let generous = QueryMemoryLedger::new(nz(256 * 1024));
        let account = generous.account(
            hawdb_executor::QueryMemoryClass::BlockingState,
            "filtered recovery",
            nz(256 * 1024),
        );
        let mut ids = Vec::new();
        let (control, report) =
            GraphExecutionRead::visit_ordered_adjacent_relationships_with_filter_owned(
                &store,
                source,
                Some(catalog.rel_type_id("LINKS").unwrap()),
                AdjacencyDirection::Outgoing,
                &filter,
                AdjacencyReadMemory {
                    budget_bytes: 256 * 1024,
                    account: Some(&account),
                },
                &mut |row| {
                    ids.push(row.id);
                    Ok(ScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(control, ScanControl::Continue);
        assert_eq!(ids, vec![selected]);
        if persisted {
            assert!(report.is_some_and(|report| report.pruned));
        }
        assert_eq!(generous.snapshot().used_bytes, 0);
    }
}

#[test]
fn full_node_index_source_admits_native_and_cold_payload_before_clone() {
    for persisted in [false, true] {
        run_full_node_source_admission(persisted, false, FullNodeSource::Index);
    }
}

#[test]
fn full_node_lookup_sources_admit_native_and_cold_payload_before_clone() {
    for persisted in [false, true] {
        for indexed in [false, true] {
            run_full_node_source_admission(persisted, true, FullNodeSource::Lookup(indexed));
        }
    }
}

#[test]
fn full_node_index_key_copy_admitted_before_parameter_clone() {
    for persisted in [false, true] {
        run_full_node_source_admission(persisted, false, FullNodeSource::IndexKey);
    }
}
#[test]
fn full_node_lookup_key_copy_admitted_before_parameter_clone() {
    for persisted in [false, true] {
        run_full_node_source_admission(persisted, true, FullNodeSource::LookupKey);
    }
}

fn run_count_sum_source_admission(persisted: bool) {
    let size = 1024 * 1024 + 137;
    let directory = persisted.then(StoredPropertyDirectory::new);
    let replay = hawdb_storage::store::WalReplayConfig {
        residency_mode: hawdb_storage::store::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = if let Some(directory) = &directory {
        GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap()
    } else {
        GraphStore::default()
    };
    for label in ["Memory", "Other"] {
        store.create_node_table(&mut catalog, label).unwrap();
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                label,
                "body",
                PropertyType::String,
                false,
            )
            .unwrap();
    }
    let id = store
        .create_node(
            &mut catalog,
            "Other",
            BTreeMap::from([("body".into(), Value::String("X".repeat(size)))]),
        )
        .unwrap();
    if let Some(directory) = &directory {
        store.checkpoint(&catalog).unwrap();
        drop(store);
        catalog = Catalog::default();
        store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            Default::default(),
            replay,
        )
        .unwrap();
    }
    assert_eq!(store.is_out_of_core(), persisted);
    let plan = PhysicalPlan::OptionalRelationshipCountSumExec {
        variable: "n".into(),
        label: "Memory".into(),
        properties: BTreeMap::new(),
        legs: vec![hawdb_plan_cypher::RelationshipCountLeg {
            rel_type: "RELATES_TO".into(),
            direction: hawdb_core::RelationshipDirection::Outgoing,
            distinct: true,
            filter: None,
        }],
        output: "count".into(),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(16 * 1024 * 1024),
        blocking_operator_bytes: nz(4096),
        batch_payload_bytes: nz(4096),
        batch_rows: nz(8192),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::new(&plan);
    let parameters = BTreeMap::new();
    let external = TextSource {
        external_id: String::new(),
        calls: Cell::new(0),
    };
    let rows = Cell::new(0);
    let window = AllocationWindow::start(size);
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            rows.set(rows.get() + batch.len());
            Ok(BatchControl::Continue)
        },
    );
    assert_eq!(
        window.allocations(),
        0,
        "count sum copied an unused node payload before source admission"
    );
    drop(window);
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    assert_eq!(rows.get(), 0);
    assert_eq!(external.calls.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(store.node_owned(id).unwrap().unwrap().id, id);
}

#[test]
fn count_sum_admits_native_unused_payload_before_copy() {
    run_count_sum_source_admission(false);
}

#[test]
fn count_sum_admits_cold_unused_payload_before_decode() {
    run_count_sum_source_admission(true);
}
