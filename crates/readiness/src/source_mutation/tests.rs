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

#[test]
fn source_mutation_dual_write_readiness_accepts_complete_family_coverage() {
    let report = nowledge_mem_source_mutation_dual_write_readiness(
        &nowledge_mem_source_mutation_dual_write_evidence_all_ready(),
    );

    assert!(report.ready);
    assert_eq!(
        report.protocol,
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL
    );
    assert_eq!(
        report.required_family_count,
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len()
    );
    assert_eq!(
        report.ready_family_count,
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len()
    );
    assert!(report.blocker_codes.is_empty());
    assert!(report.requirements.iter().any(|requirement| {
        requirement.family == NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE
            && requirement.requires_search_projection_payload
    }));
    assert_eq!(report.json()["ready"], true);
}

#[test]
fn source_mutation_dual_write_readiness_blocks_composite_source_ingest_gaps() {
    let mut evidence = nowledge_mem_source_mutation_dual_write_evidence_all_ready();
    let ingest = evidence
        .iter_mut()
        .find(|item| item.family == NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE)
        .unwrap();
    ingest.payload_frozen = false;
    ingest.hawdb_ack_recorded = false;
    ingest.independent_watermarks_recorded = false;
    ingest.replay_idempotent = false;
    ingest.search_projection_payload_frozen = false;
    evidence.push(NowledgeMemSourceMutationDualWriteEvidence::ready(
        "unknown_source_mutation",
    ));

    let report = nowledge_mem_source_mutation_dual_write_readiness(&evidence);

    assert!(!report.ready);
    assert_eq!(
        report.payload_not_frozen_families,
        vec![NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE.to_string()]
    );
    assert_eq!(
        report.search_projection_payload_not_frozen_families,
        vec![NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE.to_string()]
    );
    assert_eq!(report.unknown_families, vec!["unknown_source_mutation"]);
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_payload_not_frozen".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_hawdb_ack_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_independent_watermarks_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_replay_not_idempotent".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_search_projection_payload_not_frozen".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"source_mutation_dual_write_unknown_families".to_string()));
}
