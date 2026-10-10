// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::{
    Database, DecayTerm, MissingScoringFeature, ScoreFeature, ScoringCombination, ScoringSpec,
    ScoringTerm,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::num::NonZeroUsize;

const QUERY: &str = "CALL graph_seed_search($text, label := $label, topK := $window) YIELD node AS seed, score AS original MATCH (seed)-[:LINK*0..2]->(candidate:Memory) WITH seed, candidate, id(seed) AS seed_id, id(candidate) AS node_id, 999.0 AS score, 999.0 AS pagerank, 999 AS hop RETURN seed.key AS seed_key, seed_id, node_id, candidate.key AS id, score, pagerank, hop ORDER BY id";

fn fixture() -> Database {
    let mut database = Database::new_with_config(crate::DatabaseConfig {
        runtime_capabilities: hawdb_core::RuntimeCapabilities::default().with(
            hawdb_core::RuntimeCapability::AccessControl,
            cfg!(feature = "acl"),
        ),
        ..Default::default()
    });
    for statement in [
        "CREATE (:Memory {id: 'shared', key: 'a', title: 'Graph GRAPH', pagerank: 1.0, space_id: 'in'})",
        "CREATE (:Memory {id: 'shared', key: 'b', title: 'Graph', summary: 'graph', pagerank: 1.0, space_id: 'out'})",
        "CREATE (:Memory {id: 'child-a', key: 'a-child', pagerank: 1000.0, space_id: 'in'})",
        "CREATE (:Memory {id: 'child-b', key: 'b-child', pagerank: 1.0, space_id: 'out'})",
        "CREATE (:Entity {id: 'shared', key: 'wrong-label', title: 'graph', summary: 'graph', content: 'graph', body: 'graph', text: 'graph', pagerank: 1000000.0})",
        "MATCH (a:Memory {key: 'a'}), (c:Memory {key: 'a-child'}) CREATE (a)-[:LINK]->(c)",
        "MATCH (b:Memory {key: 'b'}), (c:Memory {key: 'b-child'}) CREATE (b)-[:LINK]->(c)",
    ] { database.query(statement).unwrap(); }
    database
}

fn parameters(window: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("text".into(), Value::String("graph".into())),
        ("label".into(), Value::String("Memory".into())),
        ("window".into(), Value::Int(window)),
    ])
}

fn scoring(limit: usize, rank_weight: f64) -> ScoringRequest {
    ScoringRequest::new(
        ScoringProgram::new(
            ScoringCombination::WeightedProduct,
            MissingScoringFeature::Reject,
            ScoringSpec {
                terms: vec![
                    ScoringTerm {
                        weight: 1.0,
                        feature: ScoreFeature::GraphSeedScore,
                    },
                    ScoringTerm {
                        weight: rank_weight,
                        feature: ScoreFeature::NodeProperty("pagerank".into()),
                    },
                ],
                decay: vec![DecayTerm {
                    feature: ScoreFeature::HopDistance,
                    half_life: 1.0,
                    min_factor: 0.0,
                }],
            },
        )
        .unwrap(),
        "score",
        limit,
    )
    .unwrap()
    .with_graph_seed_input("seed", "candidate")
    .unwrap()
}

fn run(
    snapshot: &mut super::super::DatabaseReadTransaction,
    scoring: &ScoringRequest,
    params: &BTreeMap<String, Value>,
) -> Vec<crate::executor::Row> {
    let mut rows = Vec::new();
    let report = snapshot
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(params)
                .with_scoring(scoring),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    let memory = &report.execution_profile.pipeline_memory_report;
    assert_eq!(memory.query_memory_completion_bytes, 0);
    assert!(memory.query_memory_peak_bytes <= memory.query_memory_budget_bytes);
    rows
}

#[test]
fn cypher_graph_seed_scoring_uses_canonical_identity_original_relevance_and_the_complete_window() {
    let database = fixture();
    let mut snapshot = database.begin_read_transaction().unwrap();
    let params = parameters(8);
    let rows = run(&mut snapshot, &scoring(8, 1.0), &params);
    // Repeated GRAPH terms count once; title contributes 1 term + 2 substring
    // points. b also matches summary. Public aliases are deliberately forged.
    let expected = [
        ("a-child", "a", 1500.0),
        ("b", "b", 6.0),
        ("a", "a", 3.0),
        ("b-child", "b", 3.0),
    ];
    assert_eq!(rows.len(), expected.len());
    for (row, (id, seed, score)) in rows.iter().zip(expected) {
        assert_eq!(row["id"], Value::String(id.into()));
        assert_eq!(row["seed_key"], Value::String(seed.into()));
        assert_eq!(row[SCORING_RERANK_SCORE_COLUMN], Value::Float(score));
        assert_eq!(row["score"], Value::Float(999.0));
        assert_eq!(row["pagerank"], Value::Float(999.0));
        assert_eq!(row["hop"], Value::Int(999));
        assert!(row.keys().all(|key| !key.contains('\0')));
    }
    assert_ne!(
        rows[1]["seed_id"], rows[2]["seed_id"],
        "duplicate business IDs must retain different canonical seed nodes"
    );
    assert_eq!(
        run(&mut snapshot, &scoring(1, 1.0), &params),
        rows[..1],
        "final K must not shrink the seed window"
    );
    let one_seed = run(&mut snapshot, &scoring(1, 1.0), &parameters(1));
    assert_eq!(
        one_seed[0]["seed_key"],
        Value::String("b".into()),
        "candidate window selects original graph relevance before reranking"
    );
}

#[test]
fn cypher_graph_seed_scoring_never_substitutes_search_score_or_host_aliases() {
    let database = fixture();
    let params = parameters(8);
    let mut snapshot = database.begin_read_transaction().unwrap();
    let search = ScoringRequest::new(
        ScoringProgram::new(
            ScoringCombination::WeightedSum,
            MissingScoringFeature::Reject,
            ScoringSpec {
                terms: vec![ScoringTerm {
                    feature: ScoreFeature::SearchScore,
                    weight: 1.0,
                }],
                decay: vec![],
            },
        )
        .unwrap(),
        "score",
        8,
    )
    .unwrap()
    .with_graph_seed_input("seed", "candidate")
    .unwrap();
    assert!(snapshot
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&search),
            |_| panic!("graph relevance cannot impersonate SearchScore")
        )
        .is_err());
    let wrong = scoring(8, 1.0)
        .with_text_graph_input("seed", "candidate")
        .unwrap();
    assert!(snapshot
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&wrong),
            |_| panic!("wrong producer kind must reject before delivery")
        )
        .is_err());
    let forged = "MATCH (seed:Memory) WITH seed AS candidate, 999.0 AS score RETURN candidate.id AS id, score";
    let declared = scoring(8, 1.0)
        .with_graph_seed_input("candidate", "candidate")
        .unwrap();
    assert!(snapshot
        .query_request(QueryRequest::new(forged).with_scoring(&declared))
        .is_err());
    assert_eq!(
        run(&mut snapshot, &scoring(1, 1.0), &params)[0]["id"],
        Value::String("a-child".into())
    );
}

#[test]
fn cypher_graph_seed_scoring_preserves_snapshot_inputs_and_reports_the_current_shape() {
    let mut database = fixture();
    let params = parameters(8);
    let mut snapshot = database.begin_read_transaction().unwrap();
    let first = scoring(1, 1.0).with_reference_time_millis(1000);
    let changed = scoring(1, 0.0).with_reference_time_millis(2000);
    assert_eq!(first.bind().cache_key(), changed.bind().cache_key());
    let text = first
        .clone()
        .with_text_graph_input("seed", "candidate")
        .unwrap();
    assert_ne!(first.bind().cache_key(), text.bind().cache_key());
    let old = run(&mut snapshot, &first, &params);
    database
        .query("MATCH (m:Memory {key: 'a-child'}) SET m.pagerank = 0.001")
        .unwrap();
    assert_eq!(
        run(&mut snapshot, &first, &params),
        old,
        "canonical graph scores must stay on the pinned snapshot"
    );
    let fresh = run(
        &mut database.begin_read_transaction().unwrap(),
        &first,
        &params,
    );
    assert_eq!(fresh[0]["id"], Value::String("b".into()));
    let explain = format!("EXPLAIN {QUERY}");
    let output = snapshot
        .query_request(
            QueryRequest::new(&explain)
                .with_params(&params)
                .with_scoring(&changed),
        )
        .unwrap();
    let Value::String(plan) = &output.rows[0]["plan"] else {
        panic!("missing EXPLAIN plan")
    };
    for evidence in [
        "GraphSeedScan query=$text",
        "candidate_top_k=8",
        "kind: Graph",
        "GraphSeedScore",
        "reference_time_millis=2000",
    ] {
        assert!(plan.contains(evidence), "{evidence}: {plan}");
    }
    let plain = "CALL graph_seed_search($text, label := $label, topK := $window) YIELD node AS seed, score RETURN seed.key AS key, score";
    let mut entity = params.clone();
    entity.insert("label".into(), Value::String("Entity".into()));
    let output = snapshot
        .query_request(QueryRequest::new(plain).with_params(&entity))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["key"], Value::String("wrong-label".into()));
    let output = snapshot
        .query_request(QueryRequest::new(plain).with_params(&params))
        .unwrap();
    assert_eq!(
        output.rows.len(),
        2,
        "parameter rebinding must not reuse another canonical label"
    );
}

#[test]
fn cypher_graph_seed_scoring_refuses_cancelled_and_unadmitted_requests_before_delivery_and_recovers(
) {
    let mut database = fixture();
    let params = parameters(8);
    let score = scoring(8, 1.0);
    let task = RuntimeTaskContext::without_deadline(RuntimeCancellationToken::new());
    task.cancellation().cancel();
    assert!(database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&score)
                .with_task_context(&task),
            |_| panic!("cancelled graph query delivered a row")
        )
        .is_err());
    let old = database.config.execution_memory.query_memory_bytes;
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::MIN;
    assert!(database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&score),
            |_| panic!("unadmitted graph query delivered a row")
        )
        .is_err());
    database.config.execution_memory.query_memory_bytes = old;
    assert!(database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&score)
                .with_output_limits(QueryStreamOptions {
                    max_rows: None,
                    max_payload_bytes: Some(1)
                }),
            |_| panic!("oversized graph query delivered partial rows")
        )
        .is_err());
    assert_eq!(
        run(
            &mut database.begin_read_transaction().unwrap(),
            &scoring(1, 1.0),
            &params
        )[0]["id"],
        Value::String("a-child".into())
    );
}

#[cfg(feature = "acl")]
#[test]
fn cypher_graph_seed_visibility_filters_canonical_nodes_before_the_seed_window() {
    let database = fixture();
    let params = parameters(1);
    let score = scoring(8, 1.0);
    let access = QueryAccessControlContext::visibility_scope(42, "space_id", "in");
    let output = database
        .begin_read_transaction()
        .unwrap()
        .query_request(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&score)
                .with_access_control(&access),
        )
        .unwrap();
    assert_eq!(
        output.rows.len(),
        2,
        "higher-scored out-of-scope b must not consume the one-seed window"
    );
    assert!(output
        .rows
        .iter()
        .all(|row| row["seed_key"] == Value::String("a".into())));
    assert_eq!(output.rows[0]["id"], Value::String("a-child".into()));
}

#[test]
fn cypher_graph_seed_default_yields_materialize_canonical_nodes_and_keep_parameters_current() {
    let database = fixture();
    let mut snapshot = database.begin_read_transaction().unwrap();
    let mut params = parameters(8);
    for suffix in [
        "",
        " YIELD node, score",
        " YIELD node, score RETURN node, score",
    ] {
        let query =
            format!("CALL graph_seed_search($text, label := $label, topK := $window){suffix}");
        let output = snapshot
            .query_request(QueryRequest::new(&query).with_params(&params))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| matches!(row.get("node"), Some(Value::Map(_)))));
        assert_eq!(output.rows[0]["score"], Value::Float(6.0));
        assert_eq!(output.rows[1]["score"], Value::Float(3.0));
    }
    params.insert("text".into(), Value::String("shared".into()));
    let query = "CALL graph_seed_search($text, label := $label, topK := $window) YIELD node, score RETURN node.key AS key, score";
    let output = snapshot
        .query_request(QueryRequest::new(query).with_params(&params))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert!(
        output
            .rows
            .iter()
            .all(|row| row["score"] == Value::Float(11.0)),
        "exact canonical id contributes term + substring + identity bonus"
    );
    params.insert("window".into(), Value::Int(0));
    assert!(snapshot
        .query_request(QueryRequest::new(query).with_params(&params))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn cypher_graph_seed_scan_enforces_its_operator_budget_on_late_canonical_payloads() {
    let mut database = fixture();
    let values = BTreeMap::from([("body".into(), Value::String("x".repeat(64 * 1024)))]);
    database.query_with_params("CREATE (:Memory {id: 'large', key: 'late-large', title: 'graph', pagerank: 1.0, content: $body})", &values).unwrap();
    let params = parameters(1);
    let score = scoring(1, 1.0);
    let old = database.config.execution_memory.blocking_operator_bytes;
    database.config.execution_memory.blocking_operator_bytes =
        NonZeroUsize::new(32 * 1024).unwrap();
    let error = database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&score),
            |_| panic!("a late source-budget failure cannot deliver an earlier candidate"),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("GraphSeedScan"),
        "failure must reach the canonical producer account: {error}"
    );
    database.config.execution_memory.blocking_operator_bytes = old;
    assert_eq!(
        run(
            &mut database.begin_read_transaction().unwrap(),
            &scoring(1, 1.0),
            &parameters(8)
        )[0]["id"],
        Value::String("a-child".into())
    );
}

#[test]
fn cypher_graph_seed_relevance_matches_the_existing_graph_retriever_for_typed_properties() {
    let mut database = Database::new();
    let properties = [
        Value::String("Graph GRAPH archive_1".into()),
        Value::Null,
        Value::Bool(true),
        Value::Int(125),
        Value::Float(1.25),
        Value::Binary(vec![1, 255]),
        Value::List(vec![Value::String("Graph".into()), Value::Int(125)]),
        Value::Map(BTreeMap::from([
            ("topic".into(), Value::String("GRAPH".into())),
            ("amount".into(), Value::Float(1.25)),
        ])),
    ];
    for (index, value) in properties.into_iter().enumerate() {
        let params = BTreeMap::from([
            ("id".into(), Value::String(format!("identity_{index}"))),
            ("value".into(), value),
        ]);
        database
            .query_with_params("CREATE (:Item {id: $id, title: $value})", &params)
            .unwrap();
    }
    let all = database
        .query("MATCH (node:Item) RETURN id(node) AS node_id")
        .unwrap();
    for query in [
        "graph",
        "Graph GRAPH",
        "archive_1",
        "identity_0",
        "true",
        "125",
        "1.25",
        "topic",
        "xff",
        "!!!",
    ] {
        let params = BTreeMap::from([("query".into(), Value::String(query.into()))]);
        let terms = super::super::knowledge_query_terms(query);
        let mut expected = BTreeMap::new();
        for row in &all.rows {
            let Value::Int(id) = row["node_id"] else {
                panic!("canonical node id")
            };
            let node = hawdb_executor::store::GraphExecutionRead::node_owned(
                &database.runtime.get().unwrap().store,
                hawdb_storage::NodeId(u64::try_from(id).unwrap()),
            )
            .unwrap()
            .unwrap();
            let (score, _) =
                super::super::graph_seed_score(&node, &terms, &query.trim().to_ascii_lowercase());
            if score > 0.0 {
                expected.insert(id, score);
            }
        }
        let output = database.query_with_params("CALL graph_seed_search($query, label := 'Item', topK := 16) YIELD node, score RETURN id(node) AS node_id, score", &params).unwrap();
        let actual: BTreeMap<_, _> = output
            .rows
            .iter()
            .map(|row| {
                let Value::Int(id) = row["node_id"] else {
                    panic!("canonical node id")
                };
                let Value::Float(score) = row["score"] else {
                    panic!("original graph score")
                };
                (id, score)
            })
            .collect();
        assert_eq!(
            actual, expected,
            "query {query:?}; preserve the existing graph retriever's typed-value relevance"
        );
    }
}
