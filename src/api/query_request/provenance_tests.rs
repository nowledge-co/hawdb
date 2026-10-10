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

struct HostVectorScorer {
    version: std::rc::Rc<std::cell::Cell<&'static str>>,
    calls: std::rc::Rc<std::cell::Cell<usize>>,
}

impl crate::HostScorer for HostVectorScorer {
    fn descriptor(&self) -> crate::HostScorerDescriptor<'_> {
        crate::HostScorerDescriptor::new(
            "vector-cohort",
            self.version.get(),
            std::num::NonZeroU64::new(2).unwrap(),
        )
        .unwrap()
    }

    fn score_batch(&mut self, batch: crate::HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls.set(self.calls.get() + 1);
        assert_eq!(batch.features.len(), 5);
        for (row, output) in batch.features.iter().zip(scores) {
            batch.checkpoint()?;
            assert!(matches!(row.returned_value("id"), Some(Value::String(_))));
            assert_eq!(row.returned_value("score"), Some(&Value::Float(999.0)));
            // These keys actually exist on the vector-produced binding. They
            // are engine metadata, never a host-visible returned column.
            assert_eq!(row.returned_value("\0hawdb.scoring.seed_score"), None);
            assert_eq!(row.returned_value("\0hawdb.scoring.hops"), None);
            assert_eq!(row.graph_seed_score(), None);
            *output = row.search_score().unwrap()
                * row.numeric_property("pagerank").unwrap()
                * 2f64.powi(-(row.hop_distance().unwrap() as i32))
                * (batch.reference_time_millis as f64 / 1000.0);
        }
        Ok(())
    }
}

fn host_vector_request(scorer: &mut HostVectorScorer, clock: u64) -> crate::HostScoringRequest<'_> {
    crate::HostScoringRequest::new(scorer, "score", std::num::NonZeroUsize::new(8).unwrap(), 1)
        .unwrap()
        .with_reference_time_millis(clock)
        .with_vector_graph_input("seed", "candidate")
        .unwrap()
}

#[test]
fn host_query_vector_producer_preserves_original_similarity_hops_canonical_properties_and_metadata()
{
    let database = fixture();
    let parameters = params();
    let version = std::rc::Rc::new(std::cell::Cell::new("v1"));
    let mut snapshot = database.begin_read_transaction().unwrap();
    for clock in [1000, 2000] {
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut scorer = HostVectorScorer {
            version: std::rc::Rc::clone(&version),
            calls: std::rc::Rc::clone(&calls),
        };
        let request = host_vector_request(&mut scorer, clock);
        let mut seeds = Seeds::new(vec![("a", 0.2), ("b", 0.9)]);
        let mut rows = Vec::new();
        snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(QUERY)
                    .with_params(&parameters)
                    .with_host_scoring(&request),
                &mut seeds,
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], Value::String("b-child".into()));
        assert_eq!(
            score(&rows[0]).to_bits(),
            (1.8 * (clock as f64 / 1000.0)).to_bits()
        );
        assert_eq!(rows[0]["pagerank"], Value::Float(999.0));
        assert_eq!(rows[0]["hop"], Value::Int(999));
        assert!(rows[0].keys().all(|key| !key.contains('\0')));
        assert_eq!(calls.get(), 1);
        assert_eq!(seeds.calls, 1);
        assert_eq!(seeds.admitted_rows, [8]);
        let reserved = QUERY.replace(
            "candidate.id AS id",
            "candidate.id AS `\0hawdb.scoring.hops`",
        );
        assert!(snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(&reserved)
                    .with_params(&parameters)
                    .with_host_scoring(&request),
                &mut seeds,
                |_| panic!("reserved projection cannot deliver a row"),
            )
            .is_err());
        assert_eq!(calls.get(), 1);
    }
}

#[test]
fn host_query_external_candidate_staging_cannot_change_the_planned_identity() {
    struct ChangingSeeds {
        seeds: Seeds,
        version: std::rc::Rc<std::cell::Cell<&'static str>>,
    }
    impl ExternalReadOperator for ChangingSeeds {
        fn execute_vector_seed(
            &mut self,
            request: VectorSeedExecutionRequest<'_>,
        ) -> Result<VectorSeedExecutionOutput> {
            self.version.set("v2");
            self.seeds.execute_vector_seed(request)
        }
    }
    let database = fixture();
    let parameters = params();
    let version = std::rc::Rc::new(std::cell::Cell::new("v1"));
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut scorer = HostVectorScorer {
        version: std::rc::Rc::clone(&version),
        calls: std::rc::Rc::clone(&calls),
    };
    let request = host_vector_request(&mut scorer, 1000);
    let mut seeds = ChangingSeeds {
        seeds: Seeds::new(vec![("a", 0.2), ("b", 0.9)]),
        version: std::rc::Rc::clone(&version),
    };
    let mut snapshot = database.begin_read_transaction().unwrap();
    let mut delivered = 0;
    let error = snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_host_scoring(&request),
            &mut seeds,
            |_| {
                delivered += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("identity"), "{error}");
    assert_eq!(calls.get(), 0);
    assert_eq!(delivered, 0);
    assert_eq!(seeds.seeds.calls, 1);
    version.set("v1");
    let mut healthy = Seeds::new(vec![("a", 0.2), ("b", 0.9)]);
    snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_host_scoring(&request),
            &mut healthy,
            |_| {
                delivered += 1;
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(delivered, 1);
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
    other.seed_graph_input = Some(ScoringSeedGraphInput::new("seed", "middle").unwrap());
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
    assert!(plan.contains("ScoringSeedGraphInput"), "{plan}");
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

#[test]
fn vector_graph_rejects_reintroduced_seed_scope_before_external_execution() {
    let database = fixture();
    let query = "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id \
 MATCH (seed:Memory) WHERE seed.id = seed_id WITH 1 AS x WITH x \
 MATCH (seed:Memory)-[:LINK]->(candidate:Memory) RETURN candidate.id AS id";
    let mut seeds = Seeds::new(vec![("a", 0.2)]);
    assert!(matches!(
        run(&database, query, &program(8), &mut seeds),
        Err(HawDBError::Semantic(_))
    ));
    assert_eq!(seeds.calls, 0);
}

#[test]
fn vector_graph_rejects_reintroduced_middle_scope_before_external_execution() {
    let database = fixture();
    let query = "CALL vector_search($embedding, topK := 8) YIELD id AS seed_id \
 MATCH (seed:Memory) WHERE seed.id = seed_id MATCH (seed)-[:LINK]->(middle:Memory) \
 WITH 1 AS x WITH x MATCH (middle:Memory)-[:LINK]->(candidate:Memory) RETURN candidate.id AS id";
    let mut seeds = Seeds::new(vec![("a", 0.2)]);
    assert!(matches!(
        run(&database, query, &program(8), &mut seeds),
        Err(HawDBError::Semantic(_))
    ));
    assert_eq!(seeds.calls, 0);
}

#[test]
fn vector_graph_rejects_reintroduced_seed_node_and_optional_scope() {
    let database = fixture();
    for (continuation, candidate) in [
        ("MATCH (seed:Memory) RETURN seed.id AS id", "seed"),
        (
            "OPTIONAL MATCH (seed:Memory)-[:LINK]->(candidate:Memory) RETURN candidate.id AS id",
            "candidate",
        ),
    ] {
        let query = format!("CALL vector_search($embedding, topK := 8) YIELD id AS seed_id MATCH (seed:Memory) WHERE seed.id = seed_id WITH 1 AS x WITH x {continuation}");
        let scoring = program(8)
            .with_vector_graph_input("seed", candidate)
            .unwrap();
        let mut seeds = Seeds::new(vec![("a", 0.2)]);
        assert!(matches!(
            run(&database, &query, &scoring, &mut seeds),
            Err(HawDBError::Semantic(_))
        ));
        assert_eq!(seeds.calls, 0);
    }
}

fn similarity_program() -> ScoringRequest {
    ScoringRequest::new(
        ScoringProgram::new(
            ScoringCombination::WeightedSum,
            MissingScoringFeature::Reject,
            ScoringSpec {
                terms: vec![ScoringTerm {
                    weight: 1.0,
                    feature: ScoreFeature::SearchScore,
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
    .unwrap()
}

#[test]
fn vector_graph_optional_null_source_does_not_traverse_real_node_zero() {
    let database = fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!(
            "CALL vector_search($embedding, topK := 8) YIELD id, score \
 MATCH (seed:Memory) \
 OPTIONAL MATCH (seed)-[:LINK]->(middle) \
 MATCH (middle)-[:LINK]->(candidate:Memory) {with}RETURN candidate.id AS id"
        );
        let rows = run(
            &database,
            &query,
            &similarity_program(),
            &mut Seeds::new(vec![("lonely", 0.7)]),
        )
        .unwrap();
        assert!(
            rows.is_empty(),
            "NULL source must not traverse the real node 0; WITH variant {with:?}: {rows:?}"
        );
    }
}

#[test]
fn vector_graph_unknown_relationship_optional_preserves_null_row() {
    let database = fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!(
            "CALL vector_search($embedding, topK := 8) YIELD id, score \
 MATCH (seed:Memory) \
 OPTIONAL MATCH (seed)-[:ABSENT]->(candidate:Memory) {with}RETURN candidate.id AS id"
        );
        let rows = run(
            &database,
            &query,
            &similarity_program(),
            &mut Seeds::new(vec![("lonely", 0.7)]),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "unknown OPTIONAL type must NULL extend; WITH variant {with:?}"
        );
        assert_eq!(rows[0]["id"], Value::Null);
        assert_eq!(score(&rows[0]).to_bits(), 0.7f64.to_bits());
    }
}

#[test]
fn vector_graph_unknown_relationship_zero_hop_preserves_actual_score() {
    let database = fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!(
            "CALL vector_search($embedding, topK := 8) YIELD id, score \
 MATCH (seed:Memory) \
 MATCH (seed)-[:ABSENT*0..1]->(candidate:Memory) {with}RETURN candidate.id AS id"
        );
        let mut seeds = Seeds::new(vec![("lonely", 0.7)]);
        let rows = run(&database, &query, &program(8), &mut seeds).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "unknown type must retain the actual zero-hop candidate; WITH variant {with:?}"
        );
        assert_eq!(rows[0]["id"], Value::String("lonely".into()));
        assert_eq!(
            score(&rows[0]).to_bits(),
            0.7f64.to_bits(),
            "zero observed hops must retain the genuine seed score; WITH variant {with:?}"
        );
        assert!(rows[0]
            .keys()
            .all(|name| !name.starts_with(hawdb_plan_cypher::SCORING_PROVENANCE_PREFIX)));
        assert_eq!(seeds.calls, 1);
        assert_eq!(seeds.admitted_rows, [8]);
    }
}

#[cfg(feature = "acl")]
#[test]
fn vector_seed_lookup_rechecks_canonical_scope_for_plain_and_program_rows() {
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'a', space_id: 'allowed', pagerank: 2.0})")
        .unwrap();
    database
        .query("CREATE (:Memory {id: 'b', space_id: 'blocked', pagerank: 10000.0})")
        .unwrap();
    let query = "CALL vector_search($embedding, topK := 8) YIELD id, score MATCH (seed:Memory) RETURN seed.id AS id";
    let parameters = params();
    let scoring = program(8).with_vector_graph_input("seed", "seed").unwrap();
    let mut seeds = Seeds::new(vec![("a", 1.0), ("b", 10.0)]);
    let mut snapshot = database.begin_read_transaction().unwrap();
    for scored in [false, true] {
        for (space, expected) in [
            ("allowed", Some(("a", 2.0))),
            ("blocked", Some(("b", 100000.0))),
            ("missing", None),
        ] {
            let access = QueryAccessControlContext::visibility_scope(7, "space_id", space);
            let request = QueryRequest::new(query)
                .with_params(&parameters)
                .with_access_control(&access);
            let request = if scored {
                request.with_scoring(&scoring)
            } else {
                request
            };
            let mut rows = Vec::new();
            snapshot
                .query_request_streaming_with_external(request, &mut seeds, |row| {
                    rows.push(row);
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                rows.len(),
                usize::from(expected.is_some()),
                "vector sibling leaked canonical seed ({space}, scored={scored})"
            );
            if let Some((id, score)) = expected {
                assert_eq!(rows[0]["id"], Value::String(id.into()));
                if scored {
                    assert_eq!(rows[0][SCORING_RERANK_SCORE_COLUMN], Value::Float(score));
                }
            }
        }
    }
    assert_eq!(seeds.calls, 6);
}
