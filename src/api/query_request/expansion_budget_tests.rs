// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::{Database, GraphExpansionBudget, HostScorer, HostScorerBatch, HostScorerDescriptor};
use std::cell::Cell;
use std::num::{NonZeroU64, NonZeroUsize};
use std::rc::Rc;

const PREFIX: &str = "CALL graph_seed_search($text, label := 'Memory', topK := 1) YIELD node AS seed, score MATCH (seed)-[:LINK]->(candidate:Entity) RETURN candidate.id AS id, score, candidate.authority AS authority";

fn fixture(count: usize) -> Database {
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'seed', title: 'needle'})")
        .unwrap();
    for index in 0..count {
        let parameters = BTreeMap::from([
            ("id".into(), Value::String(format!("candidate-{index:03}"))),
            (
                "authority".into(),
                Value::Float(if index + 1 == count { 1000.0 } else { 1.0 }),
            ),
        ]);
        database
            .query_with_params(
                "CREATE (:Entity {id: $id, authority: $authority})",
                &parameters,
            )
            .unwrap();
        database.query_with_params("MATCH (seed:Memory {id: 'seed'}), (candidate:Entity {id: $id}) CREATE (seed)-[:LINK]->(candidate)", &parameters).unwrap();
    }
    database
}

struct FullCohortScorer {
    calls: Rc<Cell<usize>>,
    rows: Rc<Cell<usize>>,
}

impl HostScorer for FullCohortScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new("expansion-cohort", "v1", NonZeroU64::MIN).unwrap()
    }

    fn score_batch(&mut self, batch: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls.set(self.calls.get() + 1);
        self.rows.set(batch.features.len());
        let max = batch
            .features
            .iter()
            .map(|row| row.numeric_property("authority").unwrap())
            .fold(0.0f64, f64::max);
        for (row, score) in batch.features.iter().zip(scores) {
            batch.checkpoint()?;
            *score = row.numeric_property("authority").unwrap() / max;
        }
        Ok(())
    }
}

fn assert_candidate_refusal(entry: usize) {
    let mut database = fixture(3);
    let parameters = BTreeMap::from([("text".into(), Value::String("needle".into()))]);
    database.config.execution_memory.graph_expansion_budget = Some(GraphExpansionBudget {
        candidate_limit: 2,
        payload_byte_limit: usize::MAX,
    });
    let query = match entry {
        3 => format!("{PREFIX} ORDER BY id"),
        4 => format!("{PREFIX} ORDER BY id LIMIT 3"),
        _ => PREFIX.to_string(),
    };
    let scalar = ScoringRequest::new(
        ScoringProgram::new(
            hawdb_core::graph_rag::ScoringCombination::WeightedProduct,
            hawdb_core::graph_rag::MissingScoringFeature::Reject,
            crate::ScoringSpec {
                terms: vec![crate::ScoringTerm {
                    weight: 1.0,
                    feature: crate::ScoreFeature::NodeProperty("authority".into()),
                }],
                decay: vec![],
            },
        )
        .unwrap(),
        "score",
        1,
    )
    .unwrap()
    .with_graph_seed_input("seed", "candidate")
    .unwrap();
    let calls = Rc::new(Cell::new(0));
    let rows = Rc::new(Cell::new(0));
    let mut scorer = FullCohortScorer {
        calls: Rc::clone(&calls),
        rows: Rc::clone(&rows),
    };
    let host = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(100).unwrap(), 1)
        .unwrap()
        .with_candidate_window();
    let request = match entry {
        1 => QueryRequest::new(&query)
            .with_params(&parameters)
            .with_scoring(&scalar),
        2..=4 => QueryRequest::new(&query)
            .with_params(&parameters)
            .with_host_scoring(&host),
        _ => QueryRequest::new(&query).with_params(&parameters),
    };
    let result = database.query_request(request);
    let error = result.expect_err("an exhausted expansion cannot return a successful prefix");
    assert_eq!(
        error,
        HawDBError::GraphExpansionCandidateLimitExceeded {
            requested: 3,
            limit: 2
        }
    );
    assert_eq!(
        calls.get(),
        0,
        "never invoke a host scorer on an incomplete cohort"
    );
    drop(host);

    database
        .config
        .execution_memory
        .graph_expansion_budget
        .as_mut()
        .unwrap()
        .candidate_limit = 3;
    let host = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(100).unwrap(), 1)
        .unwrap()
        .with_candidate_window();
    let request = match entry {
        1 => QueryRequest::new(&query)
            .with_params(&parameters)
            .with_scoring(&scalar),
        2..=4 => QueryRequest::new(&query)
            .with_params(&parameters)
            .with_host_scoring(&host),
        _ => QueryRequest::new(&query).with_params(&parameters),
    };
    let result = database.query_request(request).unwrap();
    assert_eq!(result.rows.len(), if entry == 0 { 3 } else { 1 });
    if entry >= 2 {
        assert_eq!(calls.get(), 1);
        assert_eq!(rows.get(), 3, "the host must receive the complete cohort");
        assert_eq!(result.rows[0]["id"], Value::String("candidate-002".into()));
    }
}

#[test]
fn expansion_budget_materialization_refuses_a_successful_prefix() {
    assert_candidate_refusal(0);
}

#[test]
fn expansion_budget_scalar_scoring_refuses_a_successful_prefix() {
    assert_candidate_refusal(1);
}

#[test]
fn expansion_budget_direct_host_preserves_the_typed_resource_cause() {
    assert_candidate_refusal(2);
}

#[test]
fn expansion_budget_sort_host_never_normalizes_an_incomplete_cohort() {
    assert_candidate_refusal(3);
}

#[test]
fn expansion_budget_topn_host_never_normalizes_an_incomplete_cohort() {
    assert_candidate_refusal(4);
}

#[test]
fn expansion_budget_payload_refuses_independently_of_candidate_and_memory_limits() {
    let mut database = fixture(3);
    let parameters = BTreeMap::from([("text".into(), Value::String("needle".into()))]);
    database.config.execution_memory.graph_expansion_budget = Some(GraphExpansionBudget {
        candidate_limit: 100,
        payload_byte_limit: 1,
    });
    let error = database
        .query_request(QueryRequest::new(PREFIX).with_params(&parameters))
        .expect_err("payload exhaustion cannot return successful empty rows");
    assert!(
        matches!(error, HawDBError::GraphExpansionPayloadLimitExceeded { requested, limit: 1 } if requested > 1),
        "{error:?}"
    );
    database
        .config
        .execution_memory
        .graph_expansion_budget
        .as_mut()
        .unwrap()
        .payload_byte_limit = 1024 * 1024;
    assert_eq!(
        database
            .query_request(QueryRequest::new(PREFIX).with_params(&parameters))
            .unwrap()
            .rows
            .len(),
        3
    );
}

#[test]
fn expansion_budget_default_allows_fanout_beyond_the_previous_seed_multiple() {
    let mut database = fixture(65);
    let parameters = BTreeMap::from([("text".into(), Value::String("needle".into()))]);
    let output = database
        .query_request(QueryRequest::new(PREFIX).with_params(&parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 65);
    for (index, row) in output.rows.iter().enumerate() {
        assert_eq!(row["id"], Value::String(format!("candidate-{index:03}")));
    }
}

fn wide_fixture(count: usize) -> Database {
    let mut database = Database::new();
    database.config.execution_memory.batch_payload_bytes = NonZeroUsize::new(16 * 1024).unwrap();
    database.config.execution_memory.blocking_operator_bytes =
        NonZeroUsize::new(128 * 1024 * 1024).unwrap();
    let mut access = database.runtime.get_mut().unwrap();
    let runtime = &mut *access;
    let seed = runtime
        .store
        .create_node(
            &mut runtime.catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::String("seed".into())),
                ("title".into(), Value::String("needle".into())),
            ]),
        )
        .unwrap();
    for index in 0..count {
        let node = runtime
            .store
            .create_node(
                &mut runtime.catalog,
                "Entity",
                BTreeMap::from([
                    ("id".into(), Value::String(format!("candidate-{index:05}"))),
                    ("payload".into(), Value::String("x".repeat(2048))),
                    (
                        "authority".into(),
                        Value::Float(if index + 1 == count { 1000.0 } else { 1.0 }),
                    ),
                ]),
            )
            .unwrap();
        runtime
            .store
            .create_relationship(&mut runtime.catalog, seed, node, "LINK", BTreeMap::new())
            .unwrap();
    }
    drop(access);
    database
}

fn ordered_rows(mut rows: Vec<crate::executor::Row>) -> Vec<crate::executor::Row> {
    rows.sort_by_key(|row| match &row["id"] {
        Value::String(id) => id.clone(),
        other => panic!("unexpected candidate identity: {other:?}"),
    });
    rows
}

#[test]
fn seeded_expansion_completes_large_cohorts_under_admitted_memory() {
    use crate::{RuntimeMemoryReservation, RuntimeTaskContext};
    use hawdb_core::{ResourceBudgetKind, ResourceBudgetScope};

    const COUNT: usize = 8193;
    const LOW_MEMORY: u64 = 64 * 1024;
    const HIGH_MEMORY: u64 = 256 * 1024 * 1024;
    let database = wide_fixture(COUNT);
    let mut snapshot = database.begin_read_transaction().unwrap();
    let columns = "RETURN candidate.id AS id, 1.0 AS score, candidate.authority AS authority, candidate.payload AS payload";
    let reference_query =
        format!("MATCH (seed:Memory {{id: 'seed'}})-[:LINK]->(candidate:Entity) {columns}");
    let mut reference = Vec::new();
    snapshot
        .query_request_streaming(QueryRequest::new(&reference_query), |row| {
            reference.push(row);
            Ok(())
        })
        .unwrap();
    let reference = ordered_rows(reference);
    assert_eq!(reference.len(), COUNT);
    for (index, row) in reference.iter().enumerate() {
        assert_eq!(row["id"], Value::String(format!("candidate-{index:05}")));
    }
    let parameters = BTreeMap::from([
        ("text".into(), Value::String("needle".into())),
        (
            "embedding".into(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        ),
    ]);
    let high = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(HIGH_MEMORY, HIGH_MEMORY));
    let low = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(LOW_MEMORY, LOW_MEMORY));
    for prefix in [
        "CALL graph_seed_search($text, label := 'Memory', topK := 1) YIELD node AS seed, score MATCH (seed)-[:LINK]->(candidate:Entity)",
        "CALL vector_search($embedding, topK := 1) YIELD id, score MATCH (seed:Memory) MATCH (seed)-[:LINK]->(candidate:Entity)",
    ] {
        let query = format!("{prefix} {columns}");
        let mut external = super::provenance_tests::Seeds::new(vec![("seed", 1.0)]);
        let error = snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(&query)
                    .with_params(&parameters)
                    .with_task_context(&low),
                &mut external,
                |_| Ok(()),
            )
            .expect_err(
                "a smaller admitted grant must fail instead of returning a successful prefix",
            );
        let HawDBError::ResourceBudgetExceeded(cause) = error else {
            panic!("expected a typed memory refusal, got {error:?}");
        };
        assert_eq!(cause.resource, ResourceBudgetKind::MemoryBytes);
        assert_eq!(cause.scope, ResourceBudgetScope::Query);
        assert_eq!(cause.budget, LOW_MEMORY);
        assert_eq!(
            cause.budget_option,
            "RuntimeTaskContext.memory_reservation.memory_bytes"
        );
        assert_eq!(low.checkpoint(), Ok(()));

        let mut actual = Vec::new();
        let report = snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(&query)
                    .with_params(&parameters)
                    .with_task_context(&high),
                &mut external,
                |row| {
                    actual.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(ordered_rows(actual), reference);
        assert_eq!(report.output_rows, COUNT);
        assert!(report.output_payload_bytes > 8 * 1024 * 1024);
        let expansion = &report.execution_profile.graph_expansion_reports[0];
        assert_eq!(expansion.seed_count, 1);
        assert_eq!(expansion.returned_count, COUNT);
        assert_eq!(expansion.expanded_node_count, COUNT);
        assert_eq!(expansion.expanded_edge_count, COUNT);
        assert!(expansion.payload_bytes_used > 8 * 1024 * 1024);
        assert_eq!(expansion.candidate_limit, usize::MAX);
        assert_eq!(expansion.payload_byte_limit, usize::MAX);
        assert!(!expansion.truncated());
        let memory = &report.execution_profile.pipeline_memory_report;
        assert_eq!(memory.query_memory_completion_bytes, 0);
        assert!(memory.query_memory_peak_bytes > LOW_MEMORY as usize);
        assert!(memory.query_memory_peak_bytes <= HIGH_MEMORY as usize);
        assert_eq!(high.checkpoint(), Ok(()));

        let limited = format!("{query} LIMIT 3");
        let report = snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(&limited)
                    .with_params(&parameters)
                    .with_task_context(&high),
                &mut external,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(report.output_rows, 3);
        assert!(!report.execution_profile.graph_expansion_reports[0].truncated());

        let calls = Rc::new(Cell::new(0));
        let rows = Rc::new(Cell::new(0));
        let mut scorer = FullCohortScorer {
            calls: Rc::clone(&calls),
            rows: Rc::clone(&rows),
        };
        let host =
            HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(COUNT).unwrap(), 1)
                .unwrap()
                .with_candidate_window();
        let refused = snapshot.query_request_streaming_with_external(
            QueryRequest::new(&query)
                .with_params(&parameters)
                .with_host_scoring(&host)
                .with_task_context(&low),
            &mut external,
            |_| panic!("an incomplete cohort must not produce a ranked row"),
        );
        assert!(matches!(
            refused,
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert_eq!(calls.get(), 0);
        let mut ranked = Vec::new();
        snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(&query)
                    .with_params(&parameters)
                    .with_host_scoring(&host)
                    .with_task_context(&high),
                &mut external,
                |row| {
                    ranked.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(rows.get(), COUNT);
        assert_eq!(ranked.len(), 1);
        assert_eq!(
            ranked[0]["id"],
            Value::String(format!("candidate-{:05}", COUNT - 1))
        );
        assert_eq!(low.checkpoint(), Ok(()));
        assert_eq!(high.checkpoint(), Ok(()));
    }
}
