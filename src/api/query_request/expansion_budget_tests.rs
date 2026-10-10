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
fn expansion_budget_default_still_refuses_excess_fanout_after_moderate_increase() {
    let mut database = fixture(65);
    let parameters = BTreeMap::from([("text".into(), Value::String("needle".into()))]);
    let error = database
        .query_request(QueryRequest::new(PREFIX).with_params(&parameters))
        .expect_err("default fanout exhaustion cannot return a successful prefix");
    assert!(
        matches!(error, HawDBError::GraphExpansionCandidateLimitExceeded { requested, limit } if requested == limit + 1 && limit < 65),
        "{error:?}"
    );
    database.config.execution_memory.graph_expansion_budget = Some(GraphExpansionBudget {
        candidate_limit: 65,
        payload_byte_limit: 8 * 1024 * 1024,
    });
    assert_eq!(
        database
            .query_request(QueryRequest::new(PREFIX).with_params(&parameters))
            .unwrap()
            .rows
            .len(),
        65
    );
}
