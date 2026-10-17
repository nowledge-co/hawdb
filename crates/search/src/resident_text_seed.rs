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
use crate::analyzer_memory::Memory;
use crate::analyzer_stream::{visit_admitted_token_list, Control};
use crate::lexical_projection::{QueryContext, ScoreCollector, ScoringInputs};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::mem::size_of;

/// Bounded raw BM25 scores retained by the caller's query working account.
#[derive(Debug)]
pub struct SearchResidentTextSeedOutput {
    scores: BTreeMap<String, f64>,
    pub corpus_document_count: usize,
    pub matching_document_count: usize,
    _memory: Option<QueryMemoryLease>,
}

impl SearchResidentTextSeedOutput {
    pub fn scores(&self) -> &BTreeMap<String, f64> {
        &self.scores
    }
}

impl SearchIndex {
    /// Scores the filtered resident corpus without materializing its candidate IDs.
    /// The in-memory path scans and tokenizes twice to retain corpus statistics
    /// without caching per-document frequencies outside the query memory account.
    pub fn text_seed_scores_with_context(
        &self,
        query_text: &str,
        metadata_filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<SearchResidentTextSeedOutput> {
        let context = QueryContext {
            memory,
            task,
            execution: None,
        };
        context.checkpoint()?;
        self.require_search_capabilities(SearchMode::Text)?;
        if max_candidates > self.lexical_config.max_query_score_entries.get() {
            return Err(HawDBError::Storage(format!(
                "text seed window exceeds the admitted {} score entries",
                self.lexical_config.max_query_score_entries
            )));
        }
        if max_candidates == 0 {
            return Ok(SearchResidentTextSeedOutput {
                scores: BTreeMap::new(),
                corpus_document_count: 0,
                matching_document_count: 0,
                _memory: None,
            });
        }

        let filter_bytes = metadata_filters
            .iter()
            .try_fold(0usize, |bytes, (key, value)| {
                bytes
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(value.len()))
                    .ok_or_else(|| HawDBError::Execution("text filter size overflow".into()))
            })?;
        let filter_reservation = filter_bytes
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(metadata_filters.len().saturating_mul(4096)))
            .ok_or_else(|| HawDBError::Execution("text filter admission overflow".into()))?;
        let _filter_memory = memory.reserve(filter_reservation)?;
        let predicates = search_metadata_predicate_pushdown(metadata_filters).predicates;

        let control = Control {
            memory: Some(Memory::Query(memory)),
            task: Some(task),
            ..Control::default()
        };
        let mut query_terms = BTreeMap::<String, usize>::new();
        let mut term_nodes = memory.reserve(0)?;
        visit_admitted_token_list(query_text, &self.analyzer_lexicon, control, |term, _| {
            if query_terms.contains_key(term.as_str()) {
                return Ok(());
            }
            if query_terms.len() >= self.lexical_config.max_query_terms.get() {
                return Err(HawDBError::Storage(format!(
                    "lexical query produced more than {} terms",
                    self.lexical_config.max_query_terms
                )));
            }
            term_nodes.grow(size_of::<(String, usize)>() + 128 + term.len())?;
            let index = query_terms.len();
            query_terms.insert(term.as_str().to_owned(), index);
            Ok(())
        })?;
        if query_terms.is_empty() {
            return Ok(SearchResidentTextSeedOutput {
                scores: BTreeMap::new(),
                corpus_document_count: 0,
                matching_document_count: 0,
                _memory: None,
            });
        }

        if let Some((projection, delta)) = self.lexical_snapshot() {
            let term_ref_bytes = query_terms
                .len()
                .checked_mul(size_of::<&str>() + 128)
                .ok_or_else(|| HawDBError::Execution("text term reference size overflow".into()))?;
            let _term_refs_memory = memory.reserve(term_ref_bytes)?;
            let terms = query_terms
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            let scored = projection.score_terms_accounted(
                &terms,
                ScoringInputs {
                    delta: &delta,
                    max_term_bytes: self.lexical_config.max_term_bytes,
                    retained_score_limit: Some(max_candidates),
                    global_statistics: None,
                    prune_blocks: false,
                    context: Some(context),
                },
                |id| {
                    let document = self.documents.get(id).ok_or_else(|| {
                        HawDBError::Storage(format!("text seed document {id} is missing"))
                    })?;
                    admitted_filter_match(document, &predicates, metadata_filters, memory)
                },
            )?;
            context.checkpoint()?;
            return Ok(SearchResidentTextSeedOutput {
                scores: scored.report.scores,
                corpus_document_count: self.documents.len(),
                matching_document_count: scored.report.matching_document_count,
                _memory: scored._memory,
            });
        }

        let vector_bytes = query_terms
            .len()
            .checked_mul(2 * size_of::<usize>())
            .ok_or_else(|| HawDBError::Execution("text statistic size overflow".into()))?;
        let _vectors_memory = memory.reserve(vector_bytes)?;
        let mut document_frequency = vec![0usize; query_terms.len()];
        let mut frequencies = vec![0usize; query_terms.len()];
        let mut corpus_document_count = 0usize;
        let mut corpus_token_count = 0usize;
        for document in self.documents.values() {
            context.checkpoint()?;
            if !admitted_filter_match(document, &predicates, metadata_filters, memory)? {
                continue;
            }
            frequencies.fill(0);
            let document_len = count_document_tokens(
                document,
                &self.analyzer_lexicon,
                &query_terms,
                &mut frequencies,
                control,
            )?;
            corpus_document_count += 1;
            corpus_token_count = corpus_token_count.saturating_add(document_len);
            for (frequency, document_frequency) in frequencies.iter().zip(&mut document_frequency) {
                if *frequency > 0 {
                    *document_frequency += 1;
                }
            }
        }
        let average_document_len = if corpus_document_count == 0 {
            0.0
        } else {
            corpus_token_count as f64 / corpus_document_count as f64
        };

        let mut collector = ScoreCollector::new(
            Some(max_candidates),
            self.lexical_config.max_query_score_entries.get(),
            Some(context),
        )?;
        let mut matching_document_count = 0usize;
        for document in self.documents.values() {
            context.checkpoint()?;
            if !admitted_filter_match(document, &predicates, metadata_filters, memory)? {
                continue;
            }
            frequencies.fill(0);
            let document_len = count_document_tokens(
                document,
                &self.analyzer_lexicon,
                &query_terms,
                &mut frequencies,
                control,
            )?;
            let mut score = 0.0;
            for index in query_terms.values() {
                let frequency = frequencies[*index];
                let df = document_frequency[*index];
                if frequency == 0 || df == 0 {
                    continue;
                }
                let idf = (1.0
                    + (corpus_document_count as f64 - df as f64 + 0.5) / (df as f64 + 0.5))
                    .ln();
                let frequency = frequency as f64;
                let denominator = frequency
                    + BM25_K1
                        * (1.0 - BM25_B
                            + BM25_B * document_len as f64 / average_document_len.max(1.0));
                score += idf * (frequency * (BM25_K1 + 1.0)) / denominator;
            }
            if score > 0.0 {
                matching_document_count += 1;
                collector.push(&document.id, score)?;
            }
        }
        let (scores, retained_memory) = collector.finish()?;
        context.checkpoint()?;
        Ok(SearchResidentTextSeedOutput {
            scores,
            corpus_document_count,
            matching_document_count,
            _memory: retained_memory,
        })
    }
}

fn count_document_tokens(
    document: &SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
    query_terms: &BTreeMap<String, usize>,
    frequencies: &mut [usize],
    control: Control<'_>,
) -> Result<usize> {
    let mut document_len = 0usize;
    for (field, weight) in document_token_fields(document) {
        visit_admitted_token_list(field, analyzer, control, |term, _| {
            document_len = document_len.saturating_add(weight);
            if let Some(index) = query_terms.get(term.as_str()) {
                frequencies[*index] = frequencies[*index].saturating_add(weight);
            }
            Ok(())
        })?;
    }
    Ok(document_len)
}

fn admitted_filter_match(
    document: &SearchDocument,
    predicates: &SearchPredicateSet,
    metadata_filters: &BTreeMap<String, String>,
    memory: &QueryMemoryAccount,
) -> Result<bool> {
    if metadata_filters.is_empty() {
        return Ok(true);
    }
    let value_bytes = predicates
        .predicates()
        .iter()
        .try_fold(0usize, |bytes, predicate| {
            let key = predicate.field().name();
            bytes
                .checked_add(if key == SEARCH_DOCUMENT_ID_FIELD {
                    document.id.len()
                } else {
                    document.metadata.get(key).map_or(0, String::len)
                })
                .ok_or_else(|| HawDBError::Execution("text filter value size overflow".into()))
        })?;
    let scratch = value_bytes
        .checked_mul(8)
        .and_then(|bytes| bytes.checked_add(metadata_filters.len().saturating_mul(1024)))
        .ok_or_else(|| HawDBError::Execution("text filter scratch overflow".into()))?;
    let _scratch = memory.reserve(scratch)?;
    Ok(search_document_matches_predicates(document, predicates))
}

#[cfg(all(test, feature = "full-text-search"))]
mod tests {
    use super::*;
    use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
    use std::num::NonZeroUsize;

    #[test]
    fn resident_text_seed_preserves_filtered_bm25_scores_and_releases_budget() {
        let mut index = SearchIndex::in_memory();
        for (id, title, body, space, labels) in [
            (
                "a",
                "Graph graph",
                "storage relation",
                "team",
                r#"["database"]"#,
            ),
            (
                "b",
                "中国 graph",
                "storage relation",
                "other",
                r#"["planning"]"#,
            ),
            (
                "c",
                "Graph",
                "storage graph graph",
                "team",
                r#"["planning"]"#,
            ),
        ] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: id.into(),
                    title: title.into(),
                    body: body.into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::from([
                        ("space_id".into(), space.into()),
                        ("labels".into(), labels.into()),
                    ]),
                })
                .unwrap();
        }
        let budget = NonZeroUsize::new(1024 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "resident text", budget);
        let task = RuntimeTaskContext::default();
        for query in ["graph storage", "Graph GRAPH", "中国 graph", "absent"] {
            for filters in [
                BTreeMap::new(),
                BTreeMap::from([("space_id".into(), "team".into())]),
                BTreeMap::from([("labels__in".into(), r#"["planning"]"#.into())]),
            ] {
                for limit in [1, 2, 3] {
                    let expected = index
                        .try_search_with_options(
                            query,
                            None,
                            SearchMode::Text,
                            SearchQueryOptions {
                                limit,
                                offset: 0,
                                rank_window: None,
                                fusion_weights: SearchFusionWeights::default(),
                                metadata_filters: filters.clone(),
                                policy_epoch: None,
                            },
                        )
                        .unwrap()
                        .hits
                        .into_iter()
                        .map(|hit| (hit.id, hit.text_score.to_bits()))
                        .collect::<BTreeMap<_, _>>();
                    let actual = index
                        .text_seed_scores_with_context(query, &filters, limit, &account, &task)
                        .unwrap();
                    let actual_bits = actual
                        .scores()
                        .iter()
                        .map(|(id, score)| (id.clone(), score.to_bits()))
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(
                        actual_bits, expected,
                        "query={query} filters={filters:?} limit={limit}"
                    );
                    drop(actual);
                    assert_eq!(ledger.snapshot().used_bytes, 0);
                }
            }
        }

        let tiny = NonZeroUsize::new(256).unwrap();
        let tiny_ledger = QueryMemoryLedger::new(tiny);
        let tiny_account = tiny_ledger.account(QueryMemoryClass::ExternalRead, "tiny text", tiny);
        assert!(index
            .text_seed_scores_with_context(
                "graph",
                &BTreeMap::from([("space_id".into(), "team".into())]),
                1,
                &tiny_account,
                &task,
            )
            .is_err());
        assert_eq!(tiny_ledger.snapshot().used_bytes, 0);

        let cancellation = crate::RuntimeCancellationToken::new();
        let cancelled_task = RuntimeTaskContext::without_deadline(cancellation.clone());
        cancellation.cancel();
        assert!(index
            .text_seed_scores_with_context("graph", &BTreeMap::new(), 1, &account, &cancelled_task,)
            .is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn resident_text_seed_peak_allocation_fits_its_working_account() {
        let _serial = crate::test_allocation::serial();
        let mut index = SearchIndex::in_memory();
        for number in 0..48 {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: format!("memory-{number}"),
                    title: "Graph storage title".into(),
                    body: "Graph storage body with repeated searchable terms".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::from([
                        ("space_id".into(), "team".into()),
                        ("labels".into(), r#"["database","planning","rust"]"#.into()),
                    ]),
                })
                .unwrap();
        }
        let budget = NonZeroUsize::new(1024 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "resident peak", budget);
        let filters = BTreeMap::from([
            ("space_id".into(), "team".into()),
            ("labels__in".into(), r#"["planning"]"#.into()),
        ]);
        let task = RuntimeTaskContext::default();
        let baseline = crate::test_allocation::live();
        let (output, peak) = crate::test_allocation::measure(|| {
            index
                .text_seed_scores_with_context("graph storage", &filters, 3, &account, &task)
                .unwrap()
        });
        let allocated = peak.checked_sub(baseline).unwrap();
        let charged = ledger.snapshot().peak_bytes;
        assert!(
            allocated <= charged && allocated <= budget.get(),
            "resident text peak exceeded its account: allocated {allocated}, charged {charged}"
        );
        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn persisted_resident_text_seed_matches_segmented_search_scores() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = crate::test_temp_dir().join(format!(
            "resident-text-seed-persisted-{}-{nonce}",
            std::process::id()
        ));
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, space) in [("a", "team"), ("b", "other"), ("c", "team")] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: id.into(),
                    title: format!("Graph {id}"),
                    body: "storage graph query".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::from([("space_id".into(), space.into())]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
        drop(index);
        let index = SearchIndex::open(&path).unwrap();
        assert!(index.lexical_snapshot().is_some());
        let budget = NonZeroUsize::new(1024 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "persisted text", budget);
        for filters in [
            BTreeMap::new(),
            BTreeMap::from([("space_id".into(), "team".into())]),
        ] {
            let expected = index
                .try_search_with_options(
                    "graph storage",
                    None,
                    SearchMode::Text,
                    SearchQueryOptions {
                        limit: 3,
                        offset: 0,
                        rank_window: None,
                        fusion_weights: SearchFusionWeights::default(),
                        metadata_filters: filters.clone(),
                        policy_epoch: None,
                    },
                )
                .unwrap()
                .hits
                .into_iter()
                .map(|hit| (hit.id, hit.text_score.to_bits()))
                .collect::<BTreeMap<_, _>>();
            let actual = index
                .text_seed_scores_with_context(
                    "graph storage",
                    &filters,
                    3,
                    &account,
                    &RuntimeTaskContext::default(),
                )
                .unwrap();
            let actual_bits = actual
                .scores()
                .iter()
                .map(|(id, score)| (id.clone(), score.to_bits()))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual_bits, expected, "filters={filters:?}");
            drop(actual);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
        let _serial = crate::test_allocation::serial();
        let measured_ledger = QueryMemoryLedger::new(budget);
        let measured_account = measured_ledger.account(
            QueryMemoryClass::ExternalRead,
            "persisted text peak",
            budget,
        );
        let filters = BTreeMap::from([("space_id".into(), "team".into())]);
        let task = RuntimeTaskContext::default();
        let baseline = crate::test_allocation::live();
        let (output, peak) = crate::test_allocation::measure(|| {
            index
                .text_seed_scores_with_context(
                    "graph storage",
                    &filters,
                    2,
                    &measured_account,
                    &task,
                )
                .unwrap()
        });
        let allocated = peak.checked_sub(baseline).unwrap();
        let charged = measured_ledger.snapshot().peak_bytes;
        assert!(
            allocated <= charged && allocated <= budget.get(),
            "persisted text peak exceeded its account: allocated {allocated}, charged {charged}"
        );
        drop(output);
        assert_eq!(measured_ledger.snapshot().used_bytes, 0);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
