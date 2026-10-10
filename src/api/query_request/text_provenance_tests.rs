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
    ExternalReadOperator, Row, TextSeedExecutionOutput, TextSeedExecutionRequest,
    VectorSeedExecutionOutput, VectorSeedExecutionRequest,
};
use crate::{
    Database, DecayTerm, MissingScoringFeature, ScoreFeature, ScoringCombination, ScoringSpec,
    ScoringTerm,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_search::{SearchDocument, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader};
use std::num::NonZeroUsize;
use std::path::PathBuf;

const QUERY: &str =
    "CALL text_search($text, topK := $window) YIELD id AS document_id, score AS original \
 MATCH (seed:Memory) MATCH (seed)-[:LINK*0..2]->(candidate:Memory) \
 WITH candidate, 999.0 AS score, 999.0 AS pagerank, 999 AS hop \
 WITH candidate, score, pagerank, hop RETURN candidate.id AS id, score, pagerank, hop ORDER BY id";

struct OwnedRoot(PathBuf);

impl OwnedRoot {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!("hawdb-cypher-text-{}-{nonce}", std::process::id())))
    }
}

impl Drop for OwnedRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy)]
enum OutputMode {
    Healthy,
    ForeignAccount,
    FailedBuilder,
    CancelAfterRead,
}

struct TextSeeds {
    reader: SearchOutOfCoreReader,
    // Fixture-only pinned ID adapter. The real Mem scoped adapter is a separate
    // acceptance boundary; document IDs deliberately differ from graph IDs.
    canonical_ids: BTreeMap<String, String>,
    calls: usize,
    admitted_rows: Vec<usize>,
    mode: OutputMode,
    _root: OwnedRoot,
}

impl TextSeeds {
    fn new() -> Self {
        let root = OwnedRoot::new();
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
        let canonical_ids: BTreeMap<String, String> = BTreeMap::from([
            ("document:a".into(), "a".into()),
            ("document:b".into(), "b".into()),
        ]);
        for (id, external_id) in &canonical_ids {
            writer
                .push(SearchDocument {
                    id: id.clone(),
                    title: "Graph".into(),
                    content: "graph storage memory archive".into(),
                    embedding: None,
                    metadata: BTreeMap::from([("external_id".into(), external_id.clone())]),
                })
                .unwrap();
        }
        writer.finish().unwrap();
        Self {
            reader: SearchOutOfCoreReader::open(&root.0).unwrap(),
            canonical_ids,
            calls: 0,
            admitted_rows: Vec::new(),
            mode: OutputMode::Healthy,
            _root: root,
        }
    }

    fn reference_scores(&self) -> BTreeMap<String, f64> {
        let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "reference BM25", budget);
        let output = self
            .reader
            .text_seed_scores_with_context(
                "graph",
                8,
                &account,
                &RuntimeTaskContext::default(),
                |_| Ok(true),
            )
            .unwrap();
        let scores = output
            .scores()
            .iter()
            .map(|(id, score)| (self.canonical_ids[id].clone(), *score))
            .collect();
        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        scores
    }
}

impl ExternalReadOperator for TextSeeds {
    fn execute_vector_seed(
        &mut self,
        _request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        panic!("text query dispatched to the vector producer")
    }

    fn execute_text_seed(
        &mut self,
        request: TextSeedExecutionRequest<'_>,
    ) -> Result<TextSeedExecutionOutput> {
        request.resources.checkpoint()?;
        self.calls += 1;
        self.admitted_rows.push(request.resources.result.max_rows);
        let fallback_task = RuntimeTaskContext::default();
        let task = request.resources.task_context.unwrap_or(&fallback_task);
        let scores = self.reader.text_seed_scores_with_context(
            request.query_text,
            request.resources.result.max_rows,
            request.working_account,
            task,
            |_| Ok(true),
        )?;
        let foreign_ledger = QueryMemoryLedger::new(request.resources.result.max_memory_bytes);
        let foreign = foreign_ledger.account(
            QueryMemoryClass::ExternalRead,
            "foreign result account",
            request.resources.result.max_memory_bytes,
        );
        let account = if matches!(self.mode, OutputMode::ForeignAccount) {
            &foreign
        } else {
            request.result_account
        };
        let mut output = TextSeedExecutionOutput::new(account, request.resources.result)?;
        for (id, score) in scores.scores() {
            output.push(id, Some(&self.canonical_ids[id]), *score)?;
        }
        if matches!(self.mode, OutputMode::FailedBuilder) {
            assert!(output.push("invalid", Some("a"), f64::NAN).is_err());
            // A provider catching a builder error cannot return partial success.
        }
        if matches!(self.mode, OutputMode::CancelAfterRead) {
            task.cancellation().cancel();
        }
        Ok(output)
    }
}

fn fixture() -> Database {
    let mut database = Database::new();
    for (id, rank) in [
        ("a", 1.0),
        ("a-child", 8.0),
        ("a-grand", 16.0),
        ("b", 1.0),
        ("b-child", 10000.0),
    ] {
        database
            .query(&format!(
                "CREATE (:Memory {{id: '{id}', pagerank: {rank}}})"
            ))
            .unwrap();
    }
    for (from, to) in [("a", "a-child"), ("a-child", "a-grand"), ("b", "b-child")] {
        database
            .query(&format!("MATCH (s:Memory {{id: '{from}'}}), (t:Memory {{id: '{to}'}}) CREATE (s)-[:LINK]->(t)"))
            .unwrap();
    }
    database
}

fn params(text: &str, window: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("text".into(), Value::String(text.into())),
        ("window".into(), Value::Int(window)),
    ])
}

fn program(limit: usize, rank_weight: f64) -> ScoringRequest {
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
    .with_text_graph_input("seed", "candidate")
    .unwrap()
}

fn run(database: &Database, scoring: &ScoringRequest, seeds: &mut TextSeeds) -> Vec<Row> {
    let parameters = params("graph", 8);
    let mut snapshot = database.begin_read_transaction().unwrap();
    let mut rows = Vec::new();
    let report = snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(scoring),
            seeds,
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
fn expansion_budget_text_producer_sort_scoring_refuses_and_recovers_the_complete_cohort() {
    let query = "CALL text_search($text, topK := $window) YIELD id, score MATCH (seed:Memory)-[:LINK]->(candidate:Memory) RETURN candidate.id AS id, score, candidate.pagerank AS pagerank ORDER BY id";
    let mut database = fixture();
    database.config.execution_memory.graph_expansion_budget = Some(crate::GraphExpansionBudget {
        candidate_limit: 1,
        payload_byte_limit: 8 * 1024 * 1024,
    });
    let mut seeds = TextSeeds::new();
    let parameters = params("graph", 8);
    let scoring = program(1, 1.0);
    let mut delivered = 0;
    let error = database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming_with_external(
            QueryRequest::new(query)
                .with_params(&parameters)
                .with_scoring(&scoring),
            &mut seeds,
            |_| {
                delivered += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(
        error,
        HawDBError::GraphExpansionCandidateLimitExceeded {
            requested: 2,
            limit: 1
        }
    );
    assert_eq!(delivered, 0);
    database
        .config
        .execution_memory
        .graph_expansion_budget
        .as_mut()
        .unwrap()
        .candidate_limit = 32;
    let mut cohorts = Vec::new();
    for limit in [1, 8] {
        let scoring = program(limit, 1.0);
        let mut rows = Vec::new();
        let report = database
            .begin_read_transaction()
            .unwrap()
            .query_request_streaming_with_external(
                QueryRequest::new(query)
                    .with_params(&parameters)
                    .with_scoring(&scoring),
                &mut seeds,
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
        cohorts.push(rows);
    }
    assert_eq!(cohorts[1].len(), 2);
    assert_eq!(cohorts[0], cohorts[1][..1]);
}

#[test]
fn cypher_text_bm25_graph_scoring_preserves_provenance_and_the_complete_seed_window() {
    let database = fixture();
    let mut seeds = TextSeeds::new();
    let raw = seeds.reference_scores();
    assert_eq!(raw.len(), 2);
    let rows = run(&database, &program(8, 1.0), &mut seeds);
    let mut expected = [
        ("a", "a", 1.0, 0i32),
        ("a-child", "a", 8.0, 1),
        ("a-grand", "a", 16.0, 2),
        ("b", "b", 1.0, 0),
        ("b-child", "b", 10000.0, 1),
    ]
    .map(|(id, seed, rank, hops)| (id, raw[seed] * rank * 2f64.powi(-hops)));
    expected.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(right.0)));
    assert_eq!(rows.len(), expected.len());
    for (row, (id, score)) in rows.iter().zip(expected) {
        assert_eq!(row["id"], Value::String(id.into()));
        let Value::Float(actual) = row[SCORING_RERANK_SCORE_COLUMN] else {
            panic!("missing combined score")
        };
        assert_eq!(actual.to_bits(), score.to_bits(), "{id}");
        assert_eq!(row["score"], Value::Float(999.0));
        assert_eq!(row["pagerank"], Value::Float(999.0));
        assert_eq!(row["hop"], Value::Int(999));
        assert!(row.keys().all(|name| !name.contains('\0')));
    }
    let best = run(&database, &program(1, 1.0), &mut seeds);
    assert_eq!(best, rows[..1]);
    assert_eq!(best[0]["id"], Value::String("b-child".into()));
    assert_eq!(seeds.admitted_rows, [8, 8]);
}

#[test]
fn cypher_text_rebinds_parameters_weights_and_rejects_a_vector_descriptor() {
    let database = fixture();
    let mut seeds = TextSeeds::new();
    let mut snapshot = database.begin_read_transaction().unwrap();
    let before_cache = snapshot.plan_cache_stats();
    let parameters = params("graph", 8);
    let mut ranked = Vec::new();
    for weight in [1.0, 0.0] {
        let scoring = program(1, weight);
        let mut rows = Vec::new();
        let report = snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(QUERY)
                    .with_params(&parameters)
                    .with_scoring(&scoring),
                &mut seeds,
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
        ranked.push(rows);
    }
    let weighted = &ranked[0];
    let unweighted = &ranked[1];
    assert_eq!(weighted[0]["id"], Value::String("b-child".into()));
    assert_eq!(unweighted[0]["id"], Value::String("a".into()));
    let parameters = params("missingterm", 1);
    let mut rows = Vec::new();
    snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(&program(1, 1.0)),
            &mut seeds,
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(
        seeds.admitted_rows,
        [8, 8, 1],
        "producer window did not rebind"
    );
    let mut wrong = program(1, 1.0);
    wrong.seed_graph_input = Some(ScoringSeedGraphInput::new("seed", "candidate").unwrap());
    let parameters = params("graph", 8);
    let calls = seeds.calls;
    let error = snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(&wrong),
            &mut seeds,
            |_| panic!("producer-kind mismatch emitted a row"),
        )
        .unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
    assert_eq!(seeds.calls, calls);
    let after_cache = snapshot.plan_cache_stats();
    // Text procedure plans deliberately bind fresh instead of populating a
    // cache with exact raw-query/window variants. This is the cache policy
    // in SCORING_RERANK_EXECUTION, separate from the row/score oracle above.
    assert_eq!(after_cache.hits, before_cache.hits);
    assert_eq!(after_cache.misses, before_cache.misses);
    assert_eq!(after_cache.admissions, before_cache.admissions);
    assert!(after_cache.bypasses > before_cache.bypasses);
}

#[test]
fn cypher_text_refuses_foreign_or_failed_result_owners_and_post_read_cancellation() {
    let database = fixture();
    let parameters = params("graph", 8);
    let scoring = program(1, 1.0);
    let mut snapshot = database.begin_read_transaction().unwrap();
    for mode in [
        OutputMode::ForeignAccount,
        OutputMode::FailedBuilder,
        OutputMode::CancelAfterRead,
    ] {
        let mut seeds = TextSeeds::new();
        seeds.mode = mode;
        let task = RuntimeTaskContext::default();
        let mut delivered = 0;
        let error = snapshot
            .query_request_streaming_with_external(
                QueryRequest::new(QUERY)
                    .with_params(&parameters)
                    .with_scoring(&scoring)
                    .with_task_context(&task),
                &mut seeds,
                |_| {
                    delivered += 1;
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error}");
        assert_eq!(delivered, 0);
        assert_eq!(seeds.calls, 1);
        seeds.mode = OutputMode::Healthy;
        assert_eq!(run(&database, &scoring, &mut seeds).len(), 1);
    }
}

#[test]
fn cypher_text_zero_windows_and_pre_cancel_do_not_read_the_source() {
    let database = fixture();
    let mut seeds = TextSeeds::new();
    assert!(run(&database, &program(0, 1.0), &mut seeds).is_empty());
    let zero_window = params("graph", 0);
    let scoring = program(1, 1.0);
    let mut snapshot = database.begin_read_transaction().unwrap();
    snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&zero_window)
                .with_scoring(&scoring),
            &mut seeds,
            |_| panic!("zero producer window emitted a row"),
        )
        .unwrap();
    assert_eq!(seeds.calls, 0);
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task = RuntimeTaskContext::without_deadline(token);
    let parameters = params("graph", 8);
    assert!(snapshot
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(&scoring)
                .with_task_context(&task),
            &mut seeds,
            |_| panic!("cancelled query emitted a row"),
        )
        .is_err());
    assert_eq!(seeds.calls, 0);
}

#[test]
fn cypher_text_explain_identifies_the_text_producer_and_current_scoring_values() {
    let database = fixture();
    let parameters = params("graph", 8);
    let first = program(1, 1.0).with_reference_time_millis(1000);
    let current = program(1, 0.0).with_reference_time_millis(2000);
    assert_eq!(first.bind().cache_key(), current.bind().cache_key());
    let vector = first
        .clone()
        .with_vector_graph_input("seed", "candidate")
        .unwrap();
    assert_ne!(first.bind().cache_key(), vector.bind().cache_key());
    let explain = format!("EXPLAIN {QUERY}");
    for scoring in [&first, &current] {
        let output = database
            .begin_read_transaction()
            .unwrap()
            .query_request(
                QueryRequest::new(&explain)
                    .with_params(&parameters)
                    .with_scoring(scoring),
            )
            .unwrap();
        let Value::String(plan) = &output.rows[0]["plan"] else {
            panic!("missing EXPLAIN plan")
        };
        assert!(plan.contains("TextSeedScan query=$text top_k=8"), "{plan}");
        assert!(plan.contains("kind: Text"), "{plan}");
        assert!(plan.contains("top_k=8"), "{plan}");
        assert!(
            plan.contains(&format!(
                "reference_time_millis={}",
                scoring.reference_time_millis.unwrap()
            )),
            "{plan}"
        );
    }
    // Procedure plans deliberately bypass the cache, like vector plans. A
    // descriptor-key distinction does not claim a cached text execution.
}

#[test]
fn cypher_text_plain_yields_use_raw_scores_and_do_not_require_a_graph() {
    let database = fixture();
    let mut seeds = TextSeeds::new();
    let raw = seeds.reference_scores();
    let parameters = params("graph", 8);
    for query in [
        "CALL text_search($text, topK := $window) RETURN id, score ORDER BY id",
        "CALL text_search($text, topK := $window) YIELD id AS doc, score AS relevance RETURN doc AS id, relevance AS score ORDER BY id",
    ] {
        let mut rows = Vec::new();
        database.begin_read_transaction().unwrap().query_request_streaming_with_external(
            QueryRequest::new(query).with_params(&parameters),
            &mut seeds,
            |row| { rows.push(row); Ok(()) },
        ).unwrap();
        assert_eq!(rows.len(), 2);
        for (row, (document_id, canonical_id)) in rows.iter().zip(&seeds.canonical_ids) {
            assert_eq!(row["id"], Value::String(document_id.clone()));
            assert_eq!(row["score"], Value::Float(raw[canonical_id]));
            assert_eq!(row.len(), 2);
        }
    }
}

#[test]
fn cypher_text_requires_capability_provider_and_root_budget_before_source_admission() {
    let mut database = fixture();
    let parameters = params("graph", 8);
    let scoring = program(1, 1.0);
    let mut seeds = TextSeeds::new();
    database.config.runtime_capabilities = database
        .config
        .runtime_capabilities
        .with(hawdb_core::RuntimeCapability::FullTextSearch, false);
    let error = database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(&scoring),
            &mut seeds,
            |_| panic!("disabled text capability emitted a row"),
        )
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::CapabilityUnavailable { .. }),
        "{error}"
    );
    assert_eq!(seeds.calls, 0);
    database.config.runtime_capabilities = database
        .config
        .runtime_capabilities
        .with(hawdb_core::RuntimeCapability::FullTextSearch, true);
    let old_budget = database.config.execution_memory.query_memory_bytes;
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::MIN;
    assert!(database
        .begin_read_transaction()
        .unwrap()
        .query_request_streaming_with_external(
            QueryRequest::new(QUERY)
                .with_params(&parameters)
                .with_scoring(&scoring),
            &mut seeds,
            |_| panic!("unadmitted text allowance emitted a row"),
        )
        .is_err());
    assert_eq!(seeds.calls, 0);
    database.config.execution_memory.query_memory_bytes = old_budget;
    let error = database
        .begin_read_transaction()
        .unwrap()
        .query_request(
            QueryRequest::new("CALL text_search($text, topK := $window)").with_params(&parameters),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("without a text projection"),
        "{error}"
    );
    assert_eq!(run(&database, &scoring, &mut seeds).len(), 1);
}

#[cfg(feature = "acl")]
fn scoped_seed_lookup(kind: u8) {
    struct ScopedScorer {
        seen: Vec<String>,
    }
    impl crate::HostScorer for ScopedScorer {
        fn descriptor(&self) -> crate::HostScorerDescriptor<'_> {
            crate::HostScorerDescriptor::new("scope", "v1", std::num::NonZeroU64::MIN).unwrap()
        }
        fn score_batch(
            &mut self,
            batch: crate::HostScorerBatch<'_>,
            scores: &mut [f64],
        ) -> Result<()> {
            for (feature, score) in batch.features.iter().zip(scores) {
                let Some(Value::String(id)) = feature.returned_value("id") else {
                    panic!("canonical id missing")
                };
                self.seen.push(id.clone());
                *score =
                    feature.search_score().unwrap() * feature.numeric_property("pagerank").unwrap();
            }
            Ok(())
        }
    }
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'a', space_id: 'allowed', pagerank: 2.0})")
        .unwrap();
    database
        .query("CREATE (:Memory {id: 'b', space_id: 'blocked', pagerank: 10000.0})")
        .unwrap();
    let mut seeds = TextSeeds::new();
    let raw = seeds.reference_scores();
    let parameters = params("graph", 8);
    for query in [
        "CALL text_search($text, topK := $window) YIELD id, score MATCH (seed:Memory) RETURN seed.id AS id, seed.pagerank AS rank",
        "CALL text_search($text, topK := $window) YIELD id, score MATCH (seed) RETURN seed.id AS id, seed.pagerank AS rank",
    ] {
    let scoring = ScoringRequest::new(
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
                decay: vec![],
            },
        )
        .unwrap(),
        "score",
        8,
    )
    .unwrap()
    .with_text_graph_input("seed", "seed")
    .unwrap();
    let mut snapshot = database.begin_read_transaction().unwrap();
    for (space, expected) in [
        ("allowed", Some(("a", 2.0))),
        ("blocked", Some(("b", 10000.0))),
        ("missing", None),
    ] {
        let access = QueryAccessControlContext::visibility_scope(7, "space_id", space);
        let mut scorer = ScopedScorer { seen: vec![] };
        let host = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(8).unwrap(), 8)
            .unwrap()
            .with_text_graph_input("seed", "seed")
            .unwrap();
        let request = QueryRequest::new(query)
            .with_params(&parameters)
            .with_access_control(&access);
        let request = match kind {
            0 => request,
            1 => request.with_scoring(&scoring),
            2 => request.with_host_scoring(&host),
            _ => unreachable!(),
        };
        let mut rows = Vec::new();
        let report = snapshot
            .query_request_streaming_with_external(request, &mut seeds, |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            rows.len(),
            usize::from(expected.is_some()),
            "canonical seed lookup leaked an unauthorized node ({space}, attachment {kind})"
        );
        if let Some((id, rank)) = expected {
            assert_eq!(rows[0]["id"], Value::String(id.into()));
            assert_eq!(rows[0]["rank"], Value::Float(rank));
            if kind != 0 {
                assert_eq!(
                    rows[0][SCORING_RERANK_SCORE_COLUMN],
                    Value::Float(raw[id] * rank)
                );
            }
        }
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
        drop(host);
        if kind == 2 {
            assert_eq!(
                scorer.seen,
                expected
                    .map(|(id, _)| id.to_string())
                    .into_iter()
                    .collect::<Vec<_>>(),
                "unauthorized seed entered host normalization cohort"
            );
        }
    }
    }
    assert_eq!(
        seeds.calls, 6,
        "projection returns both IDs; canonical graph enforces actor scope"
    );
}

#[cfg(feature = "acl")]
#[test]
fn cypher_text_seed_lookup_rechecks_canonical_scope_for_plain_rows() {
    scoped_seed_lookup(0);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_text_seed_lookup_rechecks_canonical_scope_for_program_scoring() {
    scoped_seed_lookup(1);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_text_seed_lookup_rechecks_canonical_scope_before_host_cohort() {
    scoped_seed_lookup(2);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_lookup_visibility_preserves_optional_nulls_and_rebinds_cached_actor_scope() {
    let mut database = Database::new();
    for query in [
        "CREATE (:Seed {id: 'allowed-hidden', space_id: 'allowed', target: 'b'})",
        "CREATE (:Seed {id: 'blocked-hidden', space_id: 'blocked', target: 'a'})",
        "CREATE (:Seed {id: 'allowed-visible', space_id: 'allowed', target: 'a'})",
        "CREATE (:Seed {id: 'blocked-visible', space_id: 'blocked', target: 'b'})",
        "CREATE (:Memory {id: 'a', space_id: 'allowed'})",
        "CREATE (:Memory {id: 'b', space_id: 'blocked'})",
    ] {
        database.query(query).unwrap();
    }
    let query = "MATCH (s:Seed) WITH s.id AS source, s.target AS key OPTIONAL MATCH (t:Memory) WHERE t.id = key RETURN source, t.id AS id ORDER BY source";
    let before = database.plan_cache_stats().unwrap();
    for (space, visible) in [("allowed", "a"), ("blocked", "b"), ("allowed", "a")] {
        let access = QueryAccessControlContext::visibility_scope(7, "space_id", space);
        let rows = database
            .query_request(QueryRequest::new(query).with_access_control(&access))
            .unwrap()
            .rows;
        assert_eq!(rows, vec![
            BTreeMap::from([("source".into(), Value::String(format!("{space}-hidden"))), ("id".into(), Value::Null)]),
            BTreeMap::from([("source".into(), Value::String(format!("{space}-visible"))), ("id".into(), Value::String(visible.into()))]),
        ], "optional lookup leaked an invisible match, rejected a visible match or lost its null row ({space})");
    }
    assert_eq!(
        database.plan_cache_stats().unwrap().hits,
        before.hits + 2,
        "ordinary optional lookup must bind canonical actor policy on cache hits"
    );
    let rows = database.query(query).unwrap().rows;
    assert_eq!(rows.len(), 4);
    assert!(rows
        .iter()
        .all(|row| matches!(&row["id"], Value::String(id) if id == "a" || id == "b")));
}
