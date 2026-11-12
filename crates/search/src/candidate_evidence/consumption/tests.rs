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

#[cfg(feature = "full-text-search")]
mod retrieval {
    use super::*;
    use crate::candidate_evidence::{
        effective_search_candidate_mode, search_candidate_report, SearchCandidateRequest,
    };
    use crate::{AdaptiveVectorSearchOptions, SearchDocument, SearchIndex, SearchQueryOptions};
    use std::collections::BTreeMap;

    fn index() -> SearchIndex {
        let mut index = SearchIndex::in_memory();
        for (id, embedding) in [("first", vec![1.0, 0.0]), ("second", vec![0.9, 0.1])] {
            index
                .upsert(SearchDocument {
                    id: id.to_owned(),
                    title: "Graph evidence".to_owned(),
                    content: "graph evidence".to_owned(),
                    embedding: Some(embedding),
                    metadata: BTreeMap::from([
                        ("kind".to_owned(), "memory".to_owned()),
                        ("space_id".to_owned(), "selected".to_owned()),
                    ]),
                })
                .unwrap();
        }
        index
    }

    fn output(index: &SearchIndex, request: &SearchCandidateRequest) -> SearchCandidateOutput {
        let mode = effective_search_candidate_mode(request);
        let result = index
            .search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                SearchQueryOptions {
                    limit: request.limit,
                    offset: request.offset,
                    rank_window: request.rank_window,
                    fusion_weights: request.fusion_weights,
                    metadata_filters: request.metadata_filters.clone(),
                    policy_epoch: None,
                },
                AdaptiveVectorSearchOptions::new(mode),
            )
            .unwrap();
        let report = search_candidate_report(request, mode, &result);
        SearchCandidateOutput {
            result,
            report,
            out_of_core_metrics: None,
        }
    }

    #[test]
    fn complete_text_and_completed_empty_query_have_checked_views() {
        let index = index();
        let all = output(&index, &SearchCandidateRequest::text("graph", 16));
        let checked = all.require_exhaustive().unwrap();
        assert_eq!(checked.hits().len(), 2);
        assert!(std::ptr::eq(checked.report(), &all.report));
        assert!(std::ptr::eq(checked.hits(), all.result.hits.as_slice()));

        let absent = output(&index, &SearchCandidateRequest::text("unmatchedzz", 16));
        assert_eq!(absent.result.candidate_set.cardinality, 2);
        assert!(absent.require_exhaustive().unwrap().hits().is_empty());
    }

    #[test]
    fn bounded_page_cannot_certify_a_post_filtered_empty_result() {
        let index = index();
        let bounded = output(&index, &SearchCandidateRequest::text("graph", 1));
        let omitted = if bounded.result.hits[0].id == "first" {
            "second"
        } else {
            "first"
        };
        assert_eq!(
            bounded
                .result
                .hits
                .iter()
                .filter(|hit| hit.id == omitted)
                .count(),
            0
        );
        assert!(matches!(
            bounded.require_exhaustive(),
            Err(SearchCandidateExhaustionError::ResultWindowLimited)
        ));
        assert_eq!(
            bounded.result.hits.len(),
            1,
            "bounded top-k stays available"
        );

        let complete = output(&index, &SearchCandidateRequest::text("graph", 16));
        assert!(complete
            .require_exhaustive()
            .unwrap()
            .hits()
            .iter()
            .any(|hit| hit.id == omitted));
    }

    #[test]
    fn offset_and_unproven_rank_windows_do_not_certify_exhaustion() {
        let index = index();
        let offset = output(
            &index,
            &SearchCandidateRequest::text("graph", 16).with_offset(1),
        );
        assert!(matches!(
            offset.require_exhaustive(),
            Err(SearchCandidateExhaustionError::ResultWindowLimited)
        ));
        let ranked = output(
            &index,
            &SearchCandidateRequest::text("graph", 16).with_rank_window(Some(1)),
        );
        assert!(matches!(
            ranked.require_exhaustive(),
            Err(SearchCandidateExhaustionError::RankingWindowLimited)
        ));
        let all = output(
            &index,
            &SearchCandidateRequest::text("graph", 16).with_rank_window(Some(16)),
        );
        assert_eq!(all.require_exhaustive().unwrap().hits().len(), 2);
    }

    #[test]
    fn exactly_empty_eligibility_is_a_proof_without_search_hits() {
        let index = index();
        let request = SearchCandidateRequest::text("graph", 16).with_metadata_filters(
            BTreeMap::from([("space_id".to_owned(), "absent".to_owned())]),
        );
        let empty = output(&index, &request);
        assert_eq!(empty.result.candidate_set.cardinality, 0);
        assert_eq!(empty.result.filtered_document_count, 0);
        assert!(empty.require_exhaustive().unwrap().hits().is_empty());
    }

    #[test]
    fn out_of_core_checks_execution_evidence_and_preserves_metrics() {
        let path = crate::test_temp_dir().join(format!(
            "hawdb-checked-exhaustion-{}-{}",
            std::process::id(),
            crate::QUARANTINE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let mut persisted = SearchIndex::open(&path).unwrap();
        for document in index().documents.values() {
            persisted.upsert(document.clone()).unwrap();
        }
        persisted.checkpoint().unwrap();
        drop(persisted);

        let reader = crate::SearchOutOfCoreReader::open(&path).unwrap();
        let mut requests = vec![
            SearchCandidateRequest::text("graph", 16),
            SearchCandidateRequest::text("unmatchedzz", 16),
            SearchCandidateRequest::text("graph", 0),
        ];
        if cfg!(feature = "vector-search") {
            requests.push(SearchCandidateRequest::vector(vec![1.0, 0.0], 16));
            requests.push(SearchCandidateRequest::hybrid("graph", vec![1.0, 0.0], 16));
        }
        for request in requests {
            let options = SearchQueryOptions {
                limit: request.limit,
                offset: request.offset,
                rank_window: request.rank_window,
                fusion_weights: request.fusion_weights,
                metadata_filters: request.metadata_filters.clone(),
                policy_epoch: None,
            };
            let exact = reader
                .search_with_options_exact(
                    &request.query_text,
                    request.query_embedding.as_deref(),
                    request.mode,
                    options.clone(),
                )
                .unwrap();
            let report = search_candidate_report(
                &request,
                request.compressed_vector_search_mode,
                &exact.result,
            );
            let output = SearchCandidateOutput {
                result: exact.result,
                report,
                out_of_core_metrics: Some(exact.metrics),
            };
            if request.limit == 0 {
                assert!(matches!(
                    output.require_exhaustive(),
                    Err(SearchCandidateExhaustionError::ResultWindowLimited)
                ));
            } else {
                assert!(output.require_exhaustive().is_ok());
            }
            assert!(output.out_of_core_metrics.is_some());

            if request.mode != SearchMode::Text {
                let approximate = reader
                    .search_with_options(
                        &request.query_text,
                        request.query_embedding.as_deref(),
                        request.mode,
                        options,
                    )
                    .unwrap();
                let vector = approximate
                    .result
                    .retrievers
                    .iter()
                    .find(|leg| leg.name == "vector")
                    .unwrap();
                assert_eq!(vector.candidate_score_source, "graph_distance");
                assert_eq!(vector.final_score_source, "raw_vector");
                let report = search_candidate_report(
                    &request,
                    request.compressed_vector_search_mode,
                    &approximate.result,
                );
                let output = SearchCandidateOutput {
                    result: approximate.result,
                    report,
                    out_of_core_metrics: Some(approximate.metrics),
                };
                assert!(matches!(
                    output.require_exhaustive(),
                    Err(SearchCandidateExhaustionError::RetrieverCoverageUnproven {
                        retriever: "vector"
                    })
                ));
            }
        }
        drop(reader);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn unknown_and_missing_retriever_evidence_refuse_without_private_details() {
        let index = index();
        let request = SearchCandidateRequest::text("graph", 16);
        let mut inexact = output(&index, &request);
        inexact.result.candidate_set.exact = false;
        assert!(matches!(
            inexact.require_exhaustive(),
            Err(SearchCandidateExhaustionError::CandidateSetNotExact)
        ));

        let mut missing = output(&index, &request);
        missing
            .result
            .retrievers
            .retain(|retriever| retriever.name != "text");
        assert!(matches!(
            missing.require_exhaustive(),
            Err(SearchCandidateExhaustionError::RetrieverUnavailable { retriever: "text" })
        ));

        let mut unknown = output(&index, &request);
        unknown
            .result
            .retrievers
            .iter_mut()
            .find(|leg| leg.name == "text")
            .unwrap()
            .candidate_score_source = "PRIVATE_PROVIDER_SENTINEL".to_owned();
        let error = unknown.require_exhaustive().unwrap_err();
        assert_eq!(
            error,
            SearchCandidateExhaustionError::RetrieverCoverageUnproven { retriever: "text" }
        );
        assert!(!format!("{error:?}: {error}").contains("PRIVATE_PROVIDER_SENTINEL"));
        assert!(!format!(
            "{:?}",
            output(&index, &request).require_exhaustive().unwrap()
        )
        .contains("Graph evidence"));
    }

    #[cfg(feature = "vector-search")]
    #[test]
    fn exact_metadata_and_raw_rescoring_do_not_certify_ann_candidate_coverage() {
        let index = index();
        let mut native = output(&index, &SearchCandidateRequest::vector(vec![1.0, 0.0], 16));
        assert_eq!(native.require_exhaustive().unwrap().hits().len(), 2);
        assert!(native.result.candidate_set.exact);
        let vector = native
            .result
            .retrievers
            .iter_mut()
            .find(|leg| leg.name == "vector")
            .unwrap();
        assert_eq!(vector.candidate_score_source, "raw_vector");
        assert_eq!(vector.final_score_source, "raw_vector");
        vector.candidate_score_source = "graph_distance".to_owned();
        assert!(matches!(
            native.require_exhaustive(),
            Err(SearchCandidateExhaustionError::RetrieverCoverageUnproven {
                retriever: "vector"
            })
        ));
    }
}

#[test]
fn typed_refusals_have_safe_diagnostics() {
    for refusal in [
        SearchCandidateExhaustionError::CandidateSetNotExact,
        SearchCandidateExhaustionError::ResultWindowLimited,
        SearchCandidateExhaustionError::RankingWindowLimited,
        SearchCandidateExhaustionError::RetrieverUnavailable {
            retriever: "vector",
        },
        SearchCandidateExhaustionError::RetrieverCoverageUnproven { retriever: "text" },
    ] {
        let error: &dyn std::error::Error = &refusal;
        assert!(error.source().is_none());
        assert!(!error.to_string().is_empty());
    }
}
