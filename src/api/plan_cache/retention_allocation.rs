// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Independent allocation oracle for the actual facade key/IR/trace owner.

use super::*;
use crate::api::query_request::ScoringRequest;
use hawdb_core::graph_rag::{
    DecayTerm, MissingScoringFeature, ScoreFeature, ScoringCombination, ScoringProgram,
    ScoringSpec, ScoringTerm,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
}
static LIVE: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

struct Allocator;

fn allocated(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        LIVE.fetch_add(bytes, Ordering::Relaxed);
        LARGEST.fetch_max(bytes, Ordering::Relaxed);
    }
}

fn deallocated(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        LIVE.fetch_sub(bytes, Ordering::Relaxed);
    }
}

// SAFETY: unchanged layouts/pointers are forwarded to System. Const TLS and
// atomics do not allocate or panic. Only the oracle's thread enables tracking;
// all other unit tests retain ordinary allocation behavior.
unsafe impl GlobalAlloc for Allocator {
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
static ALLOCATOR: Allocator = Allocator;

struct Window;

impl Window {
    fn new() -> Self {
        LIVE.store(0, Ordering::Relaxed);
        LARGEST.store(0, Ordering::Relaxed);
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

fn fixture(candidates: usize) -> (String, BTreeMap<String, Value>) {
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
    query.push_str(" ORDER BY score_0 LIMIT 20");
    (query, parameters)
}

fn cached(
    query: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> (PlanCacheKey, Arc<CachedPlan>, usize) {
    let parameterized = parameterize_logical_plan(statement, parameters).unwrap();
    let (physical_template, mut trace) =
        CascadesOptimizer::default().optimize_with_trace(parameterized.logical());
    // Include spare capacity and nonempty framework-owned metadata.
    trace.warnings.reserve(32);
    trace.warnings.push("retention probe".repeat(32));
    trace.decisions.reserve(128);
    let key = PlanCacheKey {
        normalized_query: query.to_owned(),
        parameters: parameterized.cache_key().clone(),
        environment: OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(&Catalog::default()),
            proof_schema: Some(ProofSchemaKey {
                public_tables: vec![hawdb_core::TableId(7); 32],
                public_properties: vec![hawdb_core::PropertyId(9); 64],
                exists: vec![(
                    hawdb_core::ConstraintSubject::Node(LabelId(7)),
                    "required".repeat(32),
                )],
            }),
            statistics_generation: Some(42),
            ordered_range_indexes: vec![("Memory".into(), "updated_at".into())],
            ordered_composite_indexes: Some(Arc::new(vec![(
                "Memory".into(),
                vec![
                    ("space_id".into(), PropertyType::String),
                    ("updated_at".into(), PropertyType::Int),
                ],
            )])),
            node_index_size_classes: vec![(LabelId(7), true)],
        },
        max_optimizer_groups: Some(128),
        access_control: Some(AccessControlPlanCacheKey {
            policy_epoch: 9,
            visibility_property: "visibility".repeat(16),
            visibility_value_count: 4,
        }),
        scoring: Some(
            ScoringRequest::new(
                ScoringProgram::new(
                    ScoringCombination::WeightedSum,
                    MissingScoringFeature::Reject,
                    ScoringSpec {
                        terms: vec![ScoringTerm {
                            weight: 1.0,
                            feature: ScoreFeature::NodeProperty("importance".repeat(32)),
                        }],
                        decay: vec![DecayTerm {
                            feature: ScoreFeature::TimestampProperty("updated_at".repeat(32)),
                            half_life: 1000.0,
                            min_factor: 0.1,
                        }],
                    },
                )
                .unwrap(),
                "score".repeat(32),
                20,
            )
            .unwrap()
            .with_graph_seed_input("seed".repeat(16), "candidate".repeat(16))
            .unwrap()
            .with_reference_time_millis(0)
            .bind()
            .cache_key(),
        ),
        point_statistics: Some(PointStatisticsKey {
            label: "Memory".into(),
            property: "id".repeat(32),
            matched_rows: 3,
            index_seek_is_cheaper: true,
        }),
    };
    let value = Arc::new(CachedPlan {
        physical_template,
        trace,
        has_parameter_slots: parameterized.slot_count() > 0,
        has_source_regexes: false,
    });
    let charge = key
        .retained_heap_bytes()
        .unwrap()
        .checked_add(value.retained_heap_bytes().unwrap())
        .unwrap();
    (key, value, charge)
}

#[test]
fn actual_physical_cache_requested_heap_stays_within_charges_and_limits() {
    row_unwind_refuses_large_output_before_copying();
    for candidates in [20, 100, 512] {
        let (query, parameters) = fixture(candidates);
        let statement = cypher::parse(&query).unwrap();
        let (actual, charged) = {
            let window = Window::new();
            let mut cache = PlanCache::with_byte_limits(Some(128), Some(16 << 20), Some(8 << 20));
            let (key, value, charge) = cached(&query, &statement, &parameters);
            cache.insert_weighted(key, value, charge);
            assert_eq!(cache.stats().entries, 1, "{candidates}: {charge}");
            let actual = window.retained();
            let charged = cache.charged_bytes().unwrap();
            assert!(
                actual <= charged,
                "{candidates}: actual={actual}, charged={charged}"
            );
            assert!(charged <= 16 << 20);
            drop(cache);
            assert_eq!(window.retained(), 0);
            (actual, charged)
        };
        println!("physical-cache candidates={candidates} actual={actual} charged={charged}");
    }
    // Observe owned list expressions and UNWIND source payloads independently
    // of the accounting implementation, including nested retained values.
    for query in [
        "MATCH (n:Memory) RETURN [x IN reverse(range(0, 128))[1..100] WHERE x > 3 | x] AS filtered, range(0, 12)[1] AS indexed, [n.id, [n.body, 'literal-owned-payload']] AS literal_list".to_owned(),
        format!("MATCH (n:Memory) UNWIND [n.id, '{}', ['{}'], null] AS candidate RETURN candidate", "unwind".repeat(4096), "nested".repeat(2048)),
        format!("UNWIND ['{}', ['{}'], null] AS candidate RETURN candidate", "unwind".repeat(4096), "nested".repeat(2048)),
    ] {
        let statement = cypher::parse(&query).unwrap();
        let parameters = BTreeMap::new();
        let window = Window::new();
        let mut cache = PlanCache::with_byte_limits(Some(8), Some(16 << 20), Some(8 << 20));
        let (key, value, charge) = cached(&query, &statement, &parameters);
        cache.insert_weighted(key, value, charge);
        assert_eq!(cache.stats().entries, 1);
        assert!(window.retained() <= cache.charged_bytes().unwrap());
        drop(cache);
        assert_eq!(window.retained(), 0);
    }
    // Temporal calls, property access, map literals and a boxed duration
    // parameter stay in the same observed cache owner.
    {
        let query = "MATCH (p:P) RETURN p.birthday.month AS month, duration.between(p.a, p.b).minutes AS minutes, date.truncate('month', p.on) AS start, {retained_temporal_key: p.a, took: $took} AS map, $took AS took";
        let statement = cypher::parse(query).unwrap();
        let parameters = BTreeMap::from([(
            "took".into(),
            Value::Duration(Box::new(
                hawdb_core::TemporalDuration::from_parts(14, 3, 7_200_000_000_000).unwrap(),
            )),
        )]);
        let window = Window::new();
        let mut cache = PlanCache::with_byte_limits(Some(8), Some(16 << 20), Some(8 << 20));
        let (key, value, charge) = cached(query, &statement, &parameters);
        cache.insert_weighted(key, value, charge);
        assert_eq!(cache.stats().entries, 1);
        assert!(window.retained() <= cache.charged_bytes().unwrap());
        drop(cache);
        assert_eq!(window.retained(), 0);
    }
    // Measure the temporal IR through the real owner. Duration boxes dominate
    // key/container slack, so omitting their heap charge cannot hide in it.
    {
        use hawdb_core::temporal::{self, TemporalFunction, TemporalKind};
        use hawdb_plan_cypher::{Projection, ProjectionExpression};
        let date = temporal::parse_date("2026-11-01").unwrap();
        let time = temporal::parse_local_time("12:34:56").unwrap();
        let local = temporal::parse_local_datetime("2026-11-01T12:34:56").unwrap();
        let zoned = temporal::parse_datetime("2026-11-01T12:34:56+02:00").unwrap();
        let duration = temporal::parse_duration("P1M2DT3S").unwrap();
        let (query, parameters) = fixture(20);
        let statement = cypher::parse(&query).unwrap();
        let window = Window::new();
        let mut cache = PlanCache::with_byte_limits(Some(8), Some(16 << 20), Some(8 << 20));
        let (key, mut value, _) = cached(&query, &statement, &parameters);
        let mut literals = (0..4096)
            .map(|_| ProjectionExpression::Literal(Value::duration(duration)))
            .collect::<Vec<_>>();
        literals.extend([
            ProjectionExpression::Literal(Value::Date(date)),
            ProjectionExpression::Literal(Value::LocalTime(time)),
            ProjectionExpression::Literal(Value::LocalDateTime(local)),
            ProjectionExpression::Literal(Value::DateTime(zoned)),
        ]);
        let mut arguments = Vec::with_capacity(32);
        arguments.push(ProjectionExpression::Literal(Value::String(
            "2026-11-01".into(),
        )));
        literals.push(ProjectionExpression::TemporalFunction {
            function: TemporalFunction::Construct(TemporalKind::Date),
            arguments,
        });
        let mut field = String::with_capacity(1024);
        field.push_str("temporal");
        Arc::get_mut(&mut value).unwrap().physical_template = PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "temporal".into(),
                expression: ProjectionExpression::PropertyAccess {
                    expression: Box::new(ProjectionExpression::MapLiteral(vec![(
                        field.clone(),
                        ProjectionExpression::List(literals),
                    )])),
                    key: field,
                },
            }],
            input: Box::new(PhysicalPlan::EmptyExec),
        };
        let charge = key
            .retained_heap_bytes()
            .unwrap()
            .checked_add(value.retained_heap_bytes().unwrap())
            .unwrap();
        cache.insert_weighted(key, value, charge);
        assert_eq!(cache.stats().entries, 1);
        assert!(window.retained() <= cache.charged_bytes().unwrap());
        drop(cache);
        assert_eq!(window.retained(), 0);
    }
    // Keep newly added path IR in the actual cache owner under this same
    // independent allocator. Spare string capacities and the boxed traversal
    // descriptor must be charged even when their visible names are short.
    for selection in [
        hawdb_plan_cypher::ShortestPathSelection::Any,
        hawdb_plan_cypher::ShortestPathSelection::All,
    ] {
        use hawdb_plan_cypher::{
            GraphMatchNode, GraphMatchProgram, GraphMatchShortestPath, GraphMatchStep,
            PathFunction, Projection, ProjectionExpression,
        };
        let (query, parameters) = fixture(20);
        let statement = cypher::parse(&query).unwrap();
        let window = Window::new();
        let mut cache = PlanCache::with_byte_limits(Some(8), Some(16 << 20), Some(8 << 20));
        let (key, mut value, _) = cached(&query, &statement, &parameters);
        let spare = |value: &str| {
            let mut owned = String::with_capacity(1024);
            owned.push_str(value);
            owned
        };
        let program = GraphMatchProgram {
            imports: Vec::new(),
            introduced: vec!["a".into(), "b".into(), "p".into()],
            steps: vec![
                GraphMatchStep::Node(GraphMatchNode {
                    variable: "a".into(),
                    label: "Person".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(1))]),
                }),
                GraphMatchStep::Node(GraphMatchNode {
                    variable: "b".into(),
                    label: "Person".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(7))]),
                }),
                GraphMatchStep::ShortestPath(Box::new(GraphMatchShortestPath {
                    path: spare("p"),
                    source: spare("a"),
                    target: spare("b"),
                    rel_type: spare("KNOWS"),
                    direction: cypher::RelationshipDirection::Outgoing,
                    min_hops: 1,
                    max_hops: 4,
                    bounded: true,
                    selection,
                })),
            ],
            predicate: None,
            optional: true,
        };
        let items = [
            PathFunction::Length,
            PathFunction::Nodes,
            PathFunction::Relationships,
            PathFunction::NodeProperties(spare("name")),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, function)| Projection {
            name: format!("path_field_{index}"),
            expression: ProjectionExpression::Path {
                function,
                column: spare("p"),
            },
        })
        .collect();
        Arc::get_mut(&mut value).unwrap().physical_template = PhysicalPlan::ProjectExec {
            items,
            input: Box::new(PhysicalPlan::GraphMatchExec {
                program,
                input: None,
            }),
        };
        let charge = key
            .retained_heap_bytes()
            .unwrap()
            .checked_add(value.retained_heap_bytes().unwrap())
            .unwrap();
        cache.insert_weighted(key, value, charge);
        assert_eq!(cache.stats().entries, 1);
        assert!(window.retained() <= cache.charged_bytes().unwrap());
        drop(cache);
        assert_eq!(window.retained(), 0);
    }
    let (query, parameters) = fixture(20);
    let statement = cypher::parse(&query).unwrap();
    let entry_charge = cached(&query, &statement, &parameters).2;
    let budget = entry_charge.checked_mul(4).unwrap();
    let window = Window::new();
    let mut cache = PlanCache::with_byte_limits(Some(128), Some(budget), Some(entry_charge * 2));
    for index in 0..32 {
        let (mut key, value, _) = cached(&query, &statement, &parameters);
        if index % 2 == 0 {
            key.scoring = Some(ScoringPlanCacheKey::Host {
                plan: hawdb_plan_cypher::HostScoringPlan::new(
                    "host-retention".repeat(16),
                    "version".repeat(16),
                    std::num::NonZeroU64::new(1).unwrap(),
                    "score".repeat(16),
                    std::num::NonZeroUsize::new(512).unwrap(),
                    20,
                )
                .unwrap()
                .with_graph_seed_input("seed".repeat(16), "candidate".repeat(16))
                .unwrap(),
                candidate_window: true,
            });
        }
        key.normalized_query
            .push_str(&format!(" /* shape-{index} */"));
        let charge = key
            .retained_heap_bytes()
            .unwrap()
            .checked_add(value.retained_heap_bytes().unwrap())
            .unwrap();
        cache.insert_weighted(key, value, charge);
        assert!(window.retained() <= cache.charged_bytes().unwrap());
        assert!(cache.charged_bytes().unwrap() <= budget);
    }
    assert!(cache.stats().evictions > 0);
    drop(cache);
    assert_eq!(window.retained(), 0);
}

fn row_unwind_refuses_large_output_before_copying() {
    use hawdb_executor::binding::Binding;
    use hawdb_executor::observer::NoopExecutionObserver;
    use hawdb_executor::pipeline::{
        BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
    };
    use hawdb_executor::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
    use hawdb_plan_cypher::ProjectionExpression;
    use std::num::NonZeroUsize;
    struct OneRow;
    impl BindingBatchSource for OneRow {
        fn execute(
            &mut self,
            _: &PhysicalPlan,
            _: ExecutionLimit,
            emit: &mut dyn FnMut(BindingBatch) -> hawdb_core::Result<BatchControl>,
        ) -> hawdb_core::Result<BatchControl> {
            emit(vec![Binding::values(BTreeMap::new())])
        }
    }
    let expression =
        ProjectionExpression::Literal(Value::List(vec![Value::String("x".repeat(1 << 20))]));
    let catalog = Catalog::default();
    let memory = ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let window = Window::new();
    let error = hawdb_executor::transform::stream_unwind_rows_batches(
        &expression,
        "item",
        &PhysicalPlan::EmptyExec,
        &mut OneRow,
        BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &NoopExecutionObserver,
        },
        ExecutionLimit::unlimited(),
        &mut |_| panic!("over-budget output"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("batch_payload_bytes"), "{error}");
    assert!(
        LARGEST.load(Ordering::Relaxed) < 1 << 20,
        "UNWIND cloned the rejected payload"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(error);
    drop(window);
}
