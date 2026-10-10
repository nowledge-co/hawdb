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

use super::super::tests::{fixture, parameters, QUERY};
use super::*;
use crate::{HostScorerBatch, HostScorerDescriptor, QueryRequest, QueryStreamOptions, Value};
use hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
use std::cell::Cell;
use std::num::NonZeroU64;
use std::rc::Rc;

struct CohortScorer {
    name: &'static str,
    version: &'static str,
    cpu: NonZeroU64,
    weight: f64,
    calls: Rc<Cell<usize>>,
    rows: Rc<Cell<usize>>,
    clock: Rc<Cell<u64>>,
    incomplete_once: bool,
    change_identity: bool,
}

impl CohortScorer {
    fn new(weight: f64) -> Self {
        Self {
            name: "cohort",
            version: "v1",
            cpu: NonZeroU64::MIN,
            weight,
            calls: Rc::new(Cell::new(0)),
            rows: Rc::new(Cell::new(0)),
            clock: Rc::new(Cell::new(0)),
            incomplete_once: false,
            change_identity: false,
        }
    }
}

impl HostScorer for CohortScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new(self.name, self.version, self.cpu).unwrap()
    }

    fn score_batch(&mut self, request: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls.set(self.calls.get() + 1);
        self.rows.set(request.features.len());
        self.clock.set(request.reference_time_millis);
        let max = request
            .features
            .iter()
            .map(|row| row.numeric_property("importance").unwrap())
            .fold(0.0f64, f64::max);
        for (index, (row, score)) in request.features.iter().zip(scores).enumerate() {
            request.checkpoint()?;
            assert!(
                matches!(row.returned_value("id"), Some(Value::String(id)) if ["early", "late", "other"].contains(&id.as_str()))
            );
            assert_eq!(row.returned_value("\0hawdb.scoring.vector_score"), None);
            if self.incomplete_once && index + 1 == request.features.len() {
                continue;
            }
            *score = (row.search_score().unwrap()
                + self.weight * row.numeric_property("importance").unwrap() / max)
                * (request.reference_time_millis as f64 / 1000.0);
        }
        self.incomplete_once = false;
        if self.change_identity {
            self.version = "v2";
        }
        Ok(())
    }
}

fn host(scorer: &mut CohortScorer, cap: usize, limit: usize, clock: u64) -> HostScoringRequest<'_> {
    HostScoringRequest::new(scorer, "score", NonZeroUsize::new(cap).unwrap(), limit)
        .unwrap()
        .with_reference_time_millis(clock)
}

#[test]
fn host_query_unsupported_procedure_input_is_a_typed_refusal_for_all_scoring_attachments() {
    let mut database = fixture();
    database
        .query("MATCH (a:Memory {id: 'early'}), (b:Memory {id: 'late'}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    let mut scorer = CohortScorer::new(1.0);
    let calls = Rc::clone(&scorer.calls);
    let host = host(&mut scorer, 2, 1, 1000);
    let program = crate::ScoringRequest::new(
        crate::ScoringProgram::new(
            crate::ScoringCombination::WeightedSum,
            crate::MissingScoringFeature::Reject,
            crate::ScoringSpec {
                terms: vec![crate::ScoringTerm {
                    weight: 1.0,
                    feature: crate::ScoreFeature::SearchScore,
                }],
                decay: vec![],
            },
        )
        .unwrap(),
        "score",
        1,
    )
    .unwrap();
    let procedure = "CALL project_graph('scoring-rejected', ['Memory'], ['LINK'])";
    for prefix in ["", "EXPLAIN ", "EXPLAIN ANALYZE "] {
        let query = format!("{prefix}{procedure}");
        for attachment in [
            QueryRequest::new(&query).with_host_scoring(&host),
            QueryRequest::new(&query).with_scoring(&program),
        ] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                database.query_request(attachment)
            }));
            assert!(
                result.is_ok(),
                "unsupported scoring input must return a typed refusal"
            );
            let error = result.unwrap().unwrap_err();
            assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
            assert!(error.to_string().contains("scoring"), "{error}");
            assert_eq!(calls.get(), 0);
        }
    }
    // Refusal cannot create the graph projection or poison the database.
    assert!(database
        .query("CALL page_rank('scoring-rejected')")
        .is_err());
    database.query(procedure).unwrap();
    assert!(!database
        .query("CALL page_rank('scoring-rejected')")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(calls.get(), 0);
}

#[test]
fn host_query_cache_rebinds_current_callback_clock_and_parameters_over_the_whole_cohort() {
    let mut database = fixture();
    let x = parameters("x");
    let y = parameters("y");
    let mut first = CohortScorer::new(0.0);
    let first_calls = Rc::clone(&first.calls);
    let first_rows = Rc::clone(&first.rows);
    let request = host(&mut first, 2, 1, 1000);
    let before = database.plan_cache_stats().unwrap();
    let output = database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&x)
                .with_host_scoring(&request),
        )
        .unwrap();
    assert_eq!(output.rows[0]["id"], Value::String("early".into()));
    assert_eq!(
        output.rows[0][SCORING_RERANK_SCORE_COLUMN],
        Value::Float(1.0)
    );
    assert_eq!(first_calls.get(), 1);
    assert_eq!(first_rows.get(), 2);
    let cached = database.plan_cache_stats().unwrap();
    assert_eq!(cached.misses, before.misses + 1);
    drop(request);

    let mut current = CohortScorer::new(1.0);
    let current_calls = Rc::clone(&current.calls);
    let current_rows = Rc::clone(&current.rows);
    let current_clock = Rc::clone(&current.clock);
    let request = host(&mut current, 2, 1, 2000);
    let output = database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&x)
                .with_host_scoring(&request),
        )
        .unwrap();
    assert_eq!(output.rows[0]["id"], Value::String("late".into()));
    let Some(Value::Float(score)) = output.rows[0].get(SCORING_RERANK_SCORE_COLUMN) else {
        panic!("missing score")
    };
    assert_eq!(score.to_bits(), 2.4f64.to_bits());
    assert_eq!(
        current_rows.get(),
        2,
        "finalK cannot truncate the normalization cohort"
    );
    assert_eq!(current_clock.get(), 2000);
    let output = database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&y)
                .with_host_scoring(&request),
        )
        .unwrap();
    assert_eq!(output.rows[0]["id"], Value::String("other".into()));
    assert_eq!(
        output.rows[0][SCORING_RERANK_SCORE_COLUMN],
        Value::Float(3.6)
    );
    assert_eq!(current_rows.get(), 1);
    assert_eq!(current_calls.get(), 2);
    assert_eq!(
        first_calls.get(),
        1,
        "a cached plan cannot own the prior callback"
    );
    assert_eq!(database.plan_cache_stats().unwrap().hits, cached.hits + 2);
}

#[test]
fn host_query_cache_separates_name_version_cost_candidate_cap_and_final_window() {
    let mut database = fixture();
    let params = parameters("x");
    let before = database.plan_cache_stats().unwrap();
    for (index, (name, version, cpu, cap, limit)) in [
        ("cohort", "v1", 1, 2, 1),
        ("other", "v1", 1, 2, 1),
        ("cohort", "v2", 1, 2, 1),
        ("cohort", "v1", 2, 2, 1),
        ("cohort", "v1", 1, 3, 1),
        ("cohort", "v1", 1, 2, 2),
    ]
    .into_iter()
    .enumerate()
    {
        let mut scorer = CohortScorer::new(1.0);
        scorer.name = name;
        scorer.version = version;
        scorer.cpu = NonZeroU64::new(cpu).unwrap();
        let request = host(&mut scorer, cap, limit, 1000);
        let output = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&params)
                    .with_host_scoring(&request),
            )
            .unwrap();
        assert_eq!(output.rows.len(), limit);
        assert_eq!(output.rows[0]["id"], Value::String("late".into()));
        assert_eq!(
            database.plan_cache_stats().unwrap().misses,
            before.misses + index as u64 + 1
        );
    }
}

#[test]
fn host_query_explain_exposes_identity_declared_cost_clock_and_cardinality_without_execution() {
    let mut database = fixture();
    let params = parameters("x");
    let explain = format!("EXPLAIN {QUERY}");
    let analyze = format!("EXPLAIN ANALYZE {QUERY}");
    let mut costs = Vec::new();
    for cpu in [7, 17] {
        let mut scorer = CohortScorer::new(1.0);
        scorer.cpu = NonZeroU64::new(cpu).unwrap();
        let calls = Rc::clone(&scorer.calls);
        let request = host(&mut scorer, 2, 1, 1234);
        let output = database
            .query_request(
                QueryRequest::new(&explain)
                    .with_params(&params)
                    .with_host_scoring(&request),
            )
            .unwrap();
        assert_eq!(calls.get(), 0, "EXPLAIN must not execute the host callback");
        let row = &output.rows[0];
        let rendered = format!("{row:?}");
        assert!(
            rendered.contains("HostScoringExec")
                && rendered.contains("cohort")
                && rendered.contains("v1")
                && rendered.contains("1234"),
            "{rendered}"
        );
        let Some(Value::Map(cost)) = row.get("selected_plan_cost") else {
            panic!("missing cost")
        };
        assert_eq!(cost["estimated_rows"], Value::Int(1));
        let Value::Int(cost) = cost["cost"] else {
            panic!("invalid cost")
        };
        costs.push(cost);
        let Some(Value::List(cardinalities)) = row.get("operator_cardinalities") else {
            panic!("missing cardinalities")
        };
        assert!(cardinalities.iter().any(|value| matches!(value, Value::Map(estimate) if estimate.get("operator") == Some(&Value::String("HostScoringExec".into())) && estimate.get("estimated_rows") == Some(&Value::Int(1)))));
        database
            .query_request(
                QueryRequest::new(&analyze)
                    .with_params(&params)
                    .with_host_scoring(&request),
            )
            .unwrap();
        assert_eq!(calls.get(), 1);
    }
    assert!(
        costs[1] > costs[0],
        "declared per-row CPU work must affect the plan cost"
    );
}

#[test]
fn host_query_snapshot_and_streaming_preserve_full_results_and_validate_limits_before_delivery() {
    let mut database = fixture();
    let params = parameters("x");
    let mut scorer = CohortScorer::new(1.0);
    let request = host(&mut scorer, 2, 1, 1000);
    let query = QueryRequest::new(QUERY)
        .with_params(&params)
        .with_host_scoring(&request);
    let expected = database.query_request(query).unwrap();
    let mut snapshot = database.begin_read_transaction().unwrap();
    assert_eq!(snapshot.query_request(query).unwrap(), expected);
    let mut delivered = Vec::new();
    let report = snapshot
        .query_request_streaming(query, |row| {
            delivered.push(row);
            Ok(())
        })
        .unwrap();
    assert!(report.fully_streamed);
    assert_eq!(
        delivered,
        expected
            .rows
            .iter()
            .map(|row| row.to_owned_row())
            .collect::<Vec<_>>()
    );
    let mut callbacks = 0;
    let result = snapshot.query_request_streaming(
        query.with_output_limits(QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(1),
        }),
        |_| {
            callbacks += 1;
            Ok(())
        },
    );
    assert!(result.is_err());
    assert_eq!(callbacks, 0);
    let token = hawdb_core::RuntimeCancellationToken::new();
    token.cancel();
    let task = hawdb_core::RuntimeTaskContext::without_deadline(token);
    assert!(snapshot
        .query_request(query.with_task_context(&task))
        .is_err());
    drop(request);
    let request = host(&mut scorer, 2, 2, 1000);
    let result = snapshot.query_request_streaming(
        QueryRequest::new(QUERY)
            .with_params(&params)
            .with_host_scoring(&request)
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: None,
            }),
        |_| {
            callbacks += 1;
            Ok(())
        },
    );
    assert!(
        result.is_err(),
        "output admission cannot silently truncate scoringK"
    );
    assert_eq!(callbacks, 0);
}

#[test]
fn host_query_window_write_identity_and_callback_borrow_boundaries_fail_closed() {
    let mut database = fixture();
    let params = parameters("x");
    let mut scorer = CohortScorer::new(1.0);
    let calls = Rc::clone(&scorer.calls);
    let request = host(&mut scorer, 2, 1, 1000);
    let window = format!("{QUERY} LIMIT 1");
    assert!(database
        .query_request(
            QueryRequest::new(&window)
                .with_params(&params)
                .with_host_scoring(&request)
        )
        .unwrap_err()
        .to_string()
        .contains("candidate window"));
    assert!(database
        .query_request(
            QueryRequest::new("CREATE (:Memory {id: 'host-rejected'})").with_host_scoring(&request)
        )
        .unwrap_err()
        .to_string()
        .contains("read query"));
    assert_eq!(calls.get(), 0);
    let borrowed = request.scorer.borrow_mut();
    let error = database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request),
        )
        .unwrap_err();
    assert!(error.to_string().contains("already executing"));
    assert_eq!(calls.get(), 0);
    drop(borrowed);
    database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request),
        )
        .unwrap();
    assert_eq!(calls.get(), 1);
    drop(request);
    let request = host(&mut scorer, 2, 1, 1000).with_candidate_window();
    let output = database
        .query_request(
            QueryRequest::new(&window)
                .with_params(&params)
                .with_host_scoring(&request),
        )
        .unwrap();
    assert_eq!(output.rows[0]["id"], Value::String("early".into()));
    drop(request);
    let before = calls.get();
    let request = host(&mut scorer, 2, 1, 1000);
    assert!(database
        .query_request(
            QueryRequest::new(&window)
                .with_params(&params)
                .with_host_scoring(&request)
        )
        .unwrap_err()
        .to_string()
        .contains("candidate window"));
    assert_eq!(calls.get(), before);
    drop(request);
    scorer.incomplete_once = true;
    let request = host(&mut scorer, 2, 1, 1000);
    let mut delivered = 0;
    assert!(database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request),
            |_| {
                delivered += 1;
                Ok(())
            }
        )
        .is_err());
    assert_eq!(delivered, 0);
    database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request),
        )
        .unwrap();
    drop(request);
    scorer.change_identity = true;
    let request = host(&mut scorer, 2, 1, 1000);
    assert!(database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request)
        )
        .unwrap_err()
        .to_string()
        .contains("identity changed"));
    let old_calls = calls.get();
    assert!(database
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_host_scoring(&request)
        )
        .unwrap_err()
        .to_string()
        .contains("planned identity"));
    assert_eq!(calls.get(), old_calls);
    assert!(database
        .query("MATCH (m:Memory) WHERE m.id = 'host-rejected' RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn host_query_explicit_limit_windows_complete_without_ordering() {
    let mut database = fixture();
    let params = parameters("x");
    let prefix = "MATCH (m:Memory) WHERE m.kind = $kind";
    let projection = "RETURN m.id AS id, m.seed AS score, m.importance AS importance";
    let queries = [
        format!("{prefix} {projection} LIMIT 1"),
        format!("{prefix} {projection} SKIP 1 LIMIT 1"),
        format!("{prefix} WITH m LIMIT 1 {projection}"),
        format!("{prefix} WITH m LIMIT 1 WITH m WHERE m.importance > 0 {projection}"),
    ];
    for query in queries {
        let expected = database.query_with_params(&query, &params).unwrap();
        assert_eq!(expected.rows.len(), 1, "{query}");
        let mut scorer = CohortScorer::new(1.0);
        let calls = Rc::clone(&scorer.calls);
        let seen = Rc::clone(&scorer.rows);
        let request = host(&mut scorer, 2, 1, 1000).with_candidate_window();
        let actual = database
            .query_request(
                QueryRequest::new(&query)
                    .with_params(&params)
                    .with_host_scoring(&request),
            )
            .unwrap();
        assert_eq!(calls.get(), 1, "{query}");
        assert_eq!(seen.get(), 1, "{query}");
        let mut row = actual.rows[0].to_owned_row();
        row.remove(SCORING_RERANK_SCORE_COLUMN);
        assert_eq!(vec![row], expected.rows, "{query}");
        let before = database.plan_cache_stats().unwrap();
        let cached = database
            .query_request(
                QueryRequest::new(&query)
                    .with_params(&params)
                    .with_host_scoring(&request),
            )
            .unwrap();
        assert_eq!(cached.rows, actual.rows, "{query}");
        assert_eq!(calls.get(), 2);
        assert_eq!(database.plan_cache_stats().unwrap().hits, before.hits + 1);
        let mut snapshot = database.begin_read_transaction().unwrap();
        let mut streamed = Vec::new();
        snapshot
            .query_request_streaming(
                QueryRequest::new(&query)
                    .with_params(&params)
                    .with_host_scoring(&request),
                |row| {
                    streamed.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(streamed, actual.rows, "{query}");
        assert_eq!(calls.get(), 3);
    }
}

#[test]
fn host_query_authorization_limits_the_normalization_cohort_and_rebinds_actor_scope() {
    let mut database = fixture();
    let params = parameters("x");
    let mut scorer = CohortScorer::new(1.0);
    let calls = Rc::clone(&scorer.calls);
    let rows = Rc::clone(&scorer.rows);
    let request = host(&mut scorer, 2, 1, 1000);
    let query = QueryRequest::new(QUERY)
        .with_params(&params)
        .with_host_scoring(&request);
    let first = crate::QueryAccessControlContext::visibility_scope(7, "space_id", "a");
    #[cfg(not(feature = "acl"))]
    {
        assert!(matches!(
            database.query_request(query.with_access_control(&first)),
            Err(HawDBError::CapabilityUnavailable { .. })
        ));
        assert_eq!(calls.get(), 0);
        assert_eq!(rows.get(), 0);
    }
    #[cfg(feature = "acl")]
    {
        let before = database.plan_cache_stats().unwrap();
        let output = database
            .query_request(query.with_access_control(&first))
            .unwrap();
        assert_eq!(output.rows[0]["id"], Value::String("early".into()));
        assert_eq!(
            output.rows[0][SCORING_RERANK_SCORE_COLUMN],
            Value::Float(2.0)
        );
        assert_eq!(rows.get(), 1);
        let cached = database.plan_cache_stats().unwrap();
        assert_eq!(cached.misses, before.misses + 1);
        let second = crate::QueryAccessControlContext::visibility_scope(7, "space_id", "b");
        let output = database
            .query_request(query.with_access_control(&second))
            .unwrap();
        assert_eq!(output.rows[0]["id"], Value::String("late".into()));
        assert_eq!(
            output.rows[0][SCORING_RERANK_SCORE_COLUMN],
            Value::Float(1.2)
        );
        assert_eq!(rows.get(), 1);
        assert_eq!(database.plan_cache_stats().unwrap().hits, cached.hits + 1);
        let mut snapshot = database.begin_read_transaction().unwrap();
        let output = snapshot
            .query_request(query.with_access_control(&first))
            .unwrap();
        assert_eq!(output.rows[0]["id"], Value::String("early".into()));
        assert_eq!(
            output.rows[0][SCORING_RERANK_SCORE_COLUMN],
            Value::Float(2.0)
        );
        assert_eq!(rows.get(), 1);
        assert_eq!(calls.get(), 3);
    }
}
