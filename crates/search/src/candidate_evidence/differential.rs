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

use super::test_report::ready_report;
use super::*;
use crate::SearchMode;
use std::collections::HashSet;

// The fixture starts with every readiness fact satisfied. Each numbered case
// invalidates one fact; this table is the expected policy, not a call back into
// the production readiness reducer.
fn readiness_campaign(policy_count: u16) {
    for policy in 0..policy_count {
        let bit = |index: u32| policy & (1_u16 << index) != 0_u16;
        let options = NowledgeMemSearchCandidateReadinessOptions {
            require_hits: bit(0),
            require_metadata_pushdown: bit(1),
            require_segment_descriptor: bit(2),
            require_text_retriever: bit(3),
            require_vector_retriever: bit(4),
            require_source_chunk_identity: bit(5),
            require_fail_soft_observation: bit(6),
            require_projection_marker_status: bit(7),
            require_projection_watermark: bit(8),
            require_embedding_identity: bit(9),
            active_embedding_model: Some("model".to_string()),
            active_embedding_dimension: Some(2),
        };
        for fault in 0..16 {
            let mut report = ready_report();
            let mut expected = Vec::new();
            let mut require = |enabled, code| {
                if enabled {
                    expected.push(code);
                }
            };
            match fault {
                0 => {}
                1 => {
                    report.protocol = "wrong".to_string();
                    require(true, "search_candidate_report_protocol_mismatch");
                    require(bit(7), "search_candidate_projection_marker_status_missing");
                }
                2 => {
                    report.returned_hit_count = 0;
                    require(bit(0), "search_candidate_no_hits");
                    require(bit(6), "search_candidate_fail_soft_not_observed");
                }
                3 => {
                    report.metadata_filter_count = 0;
                    require(bit(1), "search_candidate_metadata_filter_missing");
                    require(bit(1), "search_candidate_metadata_filter_not_fully_pushed");
                }
                4 => {
                    report.pushed_predicate_count = 0;
                    require(bit(1), "search_candidate_metadata_filter_not_fully_pushed");
                }
                5 => {
                    report.residual_predicate_count = 1;
                    require(true, "search_candidate_metadata_filter_residual");
                    require(bit(1), "search_candidate_metadata_filter_not_fully_pushed");
                }
                6 => {
                    report.persisted_segment_descriptor_used = false;
                    require(bit(2), "search_candidate_segment_descriptor_not_used");
                }
                7 | 8 => {
                    report
                        .retriever_available
                        .remove(if fault == 7 { "text" } else { "vector" });
                    require(
                        bit(if fault == 7 { 3 } else { 4 }),
                        if fault == 7 {
                            "search_candidate_text_retriever_unavailable"
                        } else {
                            "search_candidate_vector_retriever_unavailable"
                        },
                    );
                }
                9 => {
                    report.returned_missing_source_id_count = 1;
                    require(bit(5), "search_candidate_source_chunk_identity_missing");
                }
                10 => {
                    report.fallback_reason_codes.clear();
                    require(bit(6), "search_candidate_fail_soft_not_observed");
                }
                11 => {
                    report.projection_durable_source_graph_commit_epoch = None;
                    require(bit(8), "search_candidate_projection_watermark_missing");
                }
                12..=14 => {
                    match fault {
                        12 => report.projection_embedding_model = None,
                        13 => report.projection_embedding_dimension = Some(3),
                        _ => report.projection_embedding_model = Some("other".to_string()),
                    }
                    require(bit(9), "search_candidate_embedding_identity_not_ready");
                }
                15 => {
                    report.returned_kind_counts.clear();
                    require(bit(5), "search_candidate_source_chunk_identity_missing");
                }
                _ => unreachable!(),
            }
            expected.sort_unstable();
            let result = NowledgeMemSearchCandidateReadinessReport::from_candidate_report(
                report.clone(),
                &options,
            );
            assert_eq!(
                result.blocker_codes, expected,
                "policy={policy} fault={fault}"
            );
            assert_eq!(result.ready, expected.is_empty());
            assert!(result.present);
            assert_eq!(result.candidate_report, report);
            assert_eq!(
                result.protocol,
                NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL
            );
        }
    }
}

fn accumulator_campaign(seeds: u32) {
    for seed in 0..seeds {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        let mut counts = [0_u64; 4];
        let mut overlap_seen = false;
        for step in 0..32_u32 {
            let primary = (0..8)
                .filter(|bit| seed.rotate_left(step) & (1 << bit) != 0)
                .map(|bit| format!("id-{bit}"))
                .collect::<Vec<_>>();
            let mut shadow = primary.clone();
            if step % 3 == 0 {
                shadow.push("extra".to_string());
            }
            if step % 5 == 0 {
                shadow.pop();
            }
            // Duplicate IDs must not increase per-request candidate counts.
            let mut duplicated = primary.clone();
            duplicated.extend(primary.iter().cloned());
            let primary_set = primary.iter().collect::<HashSet<_>>();
            let shadow_set = shadow.iter().collect::<HashSet<_>>();
            let matched = primary_set.intersection(&shadow_set).count() as u64;
            counts[0] += primary_set.len() as u64;
            counts[1] += shadow_set.len() as u64;
            counts[2] += matched;
            counts[3] += primary_set.difference(&shadow_set).count() as u64;
            overlap_seen |= !primary.is_empty() && primary == shadow;
            accumulator.record_compare_candidate_ids(&duplicated, &shadow);
            accumulator.record_top_k_overlap_candidate_ids(SearchMode::Text, &primary, &shadow);
            let evidence = accumulator.evidence();
            assert_eq!(evidence.request_count, u64::from(step + 1));
            assert_eq!(
                [
                    evidence.primary_candidate_count,
                    evidence.shadow_candidate_count,
                    evidence.matched_candidate_count,
                    evidence.primary_only_candidate_count,
                ],
                counts,
                "seed={seed} step={step}"
            );
            assert!(evidence.fts_top_k_overlap_observed);
            assert_eq!(evidence.fts_top_k_overlap_ready, overlap_seen);
            assert!(evidence.primary_candidate_identity_checksum.is_some());
        }
    }
}

#[test]
fn candidate_evidence_differential_smoke() {
    readiness_campaign(4);
    accumulator_campaign(4);
}

#[test]
#[ignore = "complete local candidate evidence policy and accumulator campaign"]
fn candidate_evidence_differential_campaign() {
    readiness_campaign(1 << 10);
    accumulator_campaign(64);
}
