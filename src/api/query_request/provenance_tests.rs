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

use super::*;
use crate::executor::{
    ExternalReadOperator, Row, VectorExecutionReport, VectorScoreSource, VectorSeedExecutionOutput,
    VectorSeedExecutionRequest, VectorSeedExecutionRow,
};
use crate::executor::{VectorCompressionMode, VectorExecutionBackend};
use crate::{
    Database, DatabaseConfig, DecayTerm, MissingScoringFeature, ScoreFeature, ScoringCombination,
    ScoringSpec, ScoringTerm,
};
use hawdb_plan_cypher::VectorCandidateSource;

const QUERY: &str =
    "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id, score AS original \
 MATCH (seed:Memory) WHERE seed.id = seed_id \
 MATCH (seed)-[:LINK*0..2]->(candidate:Memory) \
 WITH candidate, 999.0 AS score, 999.0 AS pagerank, 999 AS hop \
 WITH candidate, score, pagerank, hop RETURN candidate.id AS id, score, pagerank, hop ORDER BY id";

struct Seeds {
    rows: Vec<(&'static str, f64)>,
    calls: usize,
    admitted_rows: Vec<usize>,
}
impl Seeds {
    fn new(rows: Vec<(&'static str, f64)>) -> Self {
        Self {
            rows,
            calls: 0,
            admitted_rows: Vec::new(),
        }
    }
}
impl ExternalReadOperator for Seeds {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        request.resources.checkpoint()?;
        self.calls += 1;
        self.admitted_rows.push(request.resources.result.max_rows);
        let rows: Vec<_> = self
            .rows
            .iter()
            .map(|(id, score)| VectorSeedExecutionRow {
                id: (*id).into(),
                external_id: Some((*id).into()),
                score: *score,
            })
            .collect();
        Ok(VectorSeedExecutionOutput {
            report: VectorExecutionReport {
                backend: VectorExecutionBackend::ScalarFlat,
                compression_mode: VectorCompressionMode::Disabled,
                candidate_source: VectorCandidateSource::Scalar,
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                candidate_score_source: VectorScoreSource::RawVector,
                final_score_source: VectorScoreSource::RawVector,
                generated_candidate_count: rows.len(),
                descriptor_pruned_count: 0,
                scalar_filtered_count: 0,
                residual_filtered_count: 0,
                candidate_scan_rounds: 1,
                reranked_candidate_count: rows.len(),
                returned_count: rows.len(),
                raw_vector_bytes_read: 0,
                candidate_scan_metrics: None,
                index_covered_document_count: None,
                index_candidate_document_count: None,
                index_coverage_complete: None,
                fallback_reason_codes: Vec::new(),
            },
            rows,
        })
    }
}

fn fixture() -> Database {
    let mut database = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: hawdb_core::RuntimeCapabilities::default().with(
            hawdb_core::RuntimeCapability::AccessControl,
            cfg!(feature = "acl"),
        ),
        ..DatabaseConfig::default()
    });
    for (id, rank) in [
        ("a", 1.0),
        ("a-child", 8.0),
        ("a-grand", 16.0),
        ("b", 1.0),
        ("b-child", 4.0),
        ("lonely", 1.0),
    ] {
        database
            .query(&format!(
                "CREATE (:Memory {{id: '{id}', pagerank: {rank}, created: 1000}})"
            ))
            .unwrap();
    }
    for (from, to) in [("a", "a-child"), ("a-child", "a-grand"), ("b", "b-child")] {
        database.query(&format!("MATCH (s:Memory {{id: '{from}'}}), (t:Memory {{id: '{to}'}}) CREATE (s)-[:LINK]->(t)")).unwrap();
    }
    database
}

fn params() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "embedding".into(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )])
}

fn program(limit: usize) -> ScoringRequest {
    ScoringRequest::new(
        ScoringProgram::new(
            ScoringCombination::WeightedProduct,
            MissingScoringFeature::Reject,
            ScoringSpec {
                terms: vec![
                    ScoringTerm {
                        weight: 1.0,
                        feature: ScoreFeature::SearchScore,
                    },
                    ScoringTerm {
                        weight: 1.0,
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
    .with_vector_graph_input("seed", "candidate")
    .unwrap()
}

fn run(
    database: &Database,
    cypher: &str,
    scoring: &ScoringRequest,
    seeds: &mut Seeds,
) -> Result<Vec<Row>> {
    let parameters = params();
    let mut snapshot = database.begin_read_transaction()?;
    let mut rows = Vec::new();
    snapshot.query_request_streaming_with_external(
        QueryRequest::new(cypher)
            .with_params(&parameters)
            .with_scoring(scoring),
        seeds,
        |row| {
            rows.push(row);
            Ok(())
        },
    )?;
    Ok(rows)
}

fn score(row: &Row) -> f64 {
    let Value::Float(score) = row[SCORING_RERANK_SCORE_COLUMN] else {
        panic!("expected combined score");
    };
    score
}

#[test]
fn vector_graph_uses_real_similarity_hops_and_canonical_properties_after_projection() {
    let database = fixture();
    let mut seeds = Seeds::new(vec![("a", 0.2), ("b", 0.9)]);
    let rows = run(&database, QUERY, &program(8), &mut seeds).unwrap();
    let mut expected = [
        ("a", 0.2, 1.0, 0i32),
        ("a-child", 0.2, 8.0, 1),
        ("a-grand", 0.2, 16.0, 2),
        ("b", 0.9, 1.0, 0),
        ("b-child", 0.9, 4.0, 1),
    ]
    .map(|(id, seed, rank, hops)| (id, seed * rank * 2f64.powi(-hops)));
    expected.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(right.0)));
    assert_eq!(rows.len(), expected.len());
    for (row, (id, expected_score)) in rows.iter().zip(expected) {
        assert_eq!(row["id"], Value::String(id.into()));
        assert_eq!(score(row).to_bits(), expected_score.to_bits());
        assert!(row
            .keys()
            .all(|name| !name.starts_with(hawdb_plan_cypher::SCORING_PROVENANCE_PREFIX)));
        assert_eq!(row["score"], Value::Float(999.0));
    }
    assert_eq!(seeds.calls, 1);
    assert_eq!(seeds.admitted_rows, [8]);
    let mut seeds = Seeds::new(vec![("a", 0.2), ("b", 0.9)]);
    let best = run(&database, QUERY, &program(1), &mut seeds).unwrap();
    assert_eq!(best, rows[..1]);
    assert_eq!(
        seeds.admitted_rows,
        [8],
        "scoring K became an upstream seed cap"
    );
}

#[test]
fn vector_graph_accumulates_observed_hops_across_two_expansions() {
    let database = fixture();
    let query = "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id \
 MATCH (seed:Memory) WHERE seed.id = seed_id \
 MATCH (seed)-[:LINK]->(middle:Memory) MATCH (middle)-[:LINK]->(candidate:Memory) \
 WITH candidate WITH candidate RETURN candidate.id AS id";
    let rows = run(
        &database,
        query,
        &program(8),
        &mut Seeds::new(vec![("a", 0.2)]),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], Value::String("a-grand".into()));
    assert_eq!(score(&rows[0]).to_bits(), (0.2f64 * 16.0 * 0.25).to_bits());
}

#[test]
fn vector_graph_rejects_unrelated_seed_lookup_before_external_execution() {
    let database = fixture();
    let query =
        "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id \
 MATCH (seed:Memory) WITH seed.id AS replacement_id WITH replacement_id \
 MATCH (replacement:Memory) WHERE replacement.id = replacement_id \
 MATCH (replacement)-[:LINK]->(candidate:Memory) WITH candidate WITH candidate RETURN candidate.id AS id";
    let mut seeds = Seeds::new(vec![("a", 0.2)]);
    assert!(matches!(
        run(&database, query, &program(8), &mut seeds),
        Err(HawDBError::Semantic(_))
    ));
    assert_eq!(seeds.calls, 0);
}

#[test]
fn vector_graph_unmatched_optional_hop_is_missing_instead_of_zero() {
    let database = fixture();
    let query =
        "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id \
 MATCH (seed:Memory) WHERE seed.id = seed_id \
 OPTIONAL MATCH (seed)-[:LINK]->(candidate:Memory) WITH candidate WITH candidate RETURN candidate.id AS id";
    let scoring = ScoringRequest::new(
        ScoringProgram::new(
            ScoringCombination::WeightedSum,
            MissingScoringFeature::Reject,
            ScoringSpec {
                terms: vec![ScoringTerm {
                    weight: 1.0,
                    feature: ScoreFeature::HopDistance,
                }],
                decay: vec![],
            },
        )
        .unwrap(),
        "score",
        8,
    )
    .unwrap()
    .with_vector_graph_input("seed", "candidate")
    .unwrap();
    let error = run(
        &database,
        query,
        &scoring,
        &mut Seeds::new(vec![("lonely", 0.7)]),
    )
    .unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error}");
    assert!(error.to_string().contains("missing"), "{error}");
}

#[test]
fn vector_graph_descriptor_separates_cache_and_explain_identity() {
    let first = program(8);
    let mut other = first.clone();
    other.vector_graph_input = Some(ScoringVectorGraphInput::new("seed", "middle").unwrap());
    assert_ne!(first.bind().cache_key(), other.bind().cache_key());
    let database = fixture();
    let parameters = params();
    let output = database
        .begin_read_transaction()
        .unwrap()
        .query_request(
            QueryRequest::new(&format!("EXPLAIN {QUERY}"))
                .with_params(&parameters)
                .with_scoring(&first),
        )
        .unwrap();
    let Value::String(plan) = &output.rows[0]["plan"] else {
        panic!("expected explain JSON");
    };
    assert!(plan.contains("ScoringVectorGraphInput"), "{plan}");
    assert!(plan.contains("candidate"), "{plan}");
}

#[test]
fn vector_graph_legacy_expansion_matches_pipeline_ranked_output() {
    let database = fixture();
    let scoring = program(8);
    let seed_rows = vec![("a", 0.2), ("b", 0.9)];
    let pipeline = run(
        &database,
        QUERY,
        &scoring,
        &mut Seeds::new(seed_rows.clone()),
    )
    .unwrap();
    let legacy = "CALL vector_search($embedding, topK := 8) YIELD id, score \
 MATCH (seed:Memory)-[:LINK*0..2]->(candidate:Memory) RETURN candidate.id AS id ORDER BY id";
    let ordinary = run(&database, legacy, &scoring, &mut Seeds::new(seed_rows)).unwrap();
    let signature = |rows: &[Row]| {
        rows.iter()
            .map(|row| (row["id"].clone(), score(row).to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(signature(&ordinary), signature(&pipeline));
    assert!(ordinary.iter().all(|row| row
        .keys()
        .all(|name| !name.starts_with(hawdb_plan_cypher::SCORING_PROVENANCE_PREFIX))));
}
