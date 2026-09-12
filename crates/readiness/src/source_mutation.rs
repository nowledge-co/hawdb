//! Source mutation dual-write evidence contracts and readiness evaluation.
//!
//! This evaluates caller-supplied evidence; actual writes, ACK recording, replay,
//! bootstrap verification and activation remain in the embedded host path.

use std::collections::{BTreeMap, BTreeSet};

pub const NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-source-mutation-dual-write-readiness-v1";

pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE: &str = "source_patch_delete";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE: &str = "source_lifecycle";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE: &str = "source_graph_delete";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE: &str = "source_ingest_create";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE: &str =
    "source_content_refresh_reparse";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION: &str =
    "source_indexed_transition";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES: &str = "source_revision_edges";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS: &str =
    "source_search_projection_effects";

pub const REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES: &[&str] = &[
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationFamilyRequirement {
    pub family: String,
    pub requires_search_projection_payload: bool,
}

impl NowledgeMemSourceMutationFamilyRequirement {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "family": self.family,
            "requires_search_projection_payload": self.requires_search_projection_payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationDualWriteEvidence {
    pub family: String,
    pub payload_frozen: bool,
    pub legacy_ack_recorded: bool,
    pub skein_ack_recorded: bool,
    pub independent_watermarks_recorded: bool,
    pub replay_idempotent: bool,
    pub search_projection_payload_frozen: bool,
}

impl NowledgeMemSourceMutationDualWriteEvidence {
    pub fn ready(family: impl Into<String>) -> Self {
        let family = family.into();
        Self {
            search_projection_payload_frozen: source_mutation_family_requires_projection(&family),
            family,
            payload_frozen: true,
            legacy_ack_recorded: true,
            skein_ack_recorded: true,
            independent_watermarks_recorded: true,
            replay_idempotent: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationDualWriteReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub required_family_count: usize,
    pub evidence_family_count: usize,
    pub ready_family_count: usize,
    pub requirements: Vec<NowledgeMemSourceMutationFamilyRequirement>,
    pub evidence: Vec<NowledgeMemSourceMutationDualWriteEvidence>,
    pub ready_families: Vec<String>,
    pub missing_required_families: Vec<String>,
    pub unknown_families: Vec<String>,
    pub duplicate_families: Vec<String>,
    pub payload_not_frozen_families: Vec<String>,
    pub legacy_ack_missing_families: Vec<String>,
    pub skein_ack_missing_families: Vec<String>,
    pub independent_watermarks_missing_families: Vec<String>,
    pub replay_not_idempotent_families: Vec<String>,
    pub search_projection_payload_not_frozen_families: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemSourceMutationDualWriteReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "required_family_count": self.required_family_count,
            "evidence_family_count": self.evidence_family_count,
            "ready_family_count": self.ready_family_count,
            "requirements": self.requirements.iter().map(NowledgeMemSourceMutationFamilyRequirement::json).collect::<Vec<_>>(),
            "evidence": self.evidence.iter().map(source_mutation_dual_write_evidence_json).collect::<Vec<_>>(),
            "ready_families": self.ready_families,
            "missing_required_families": self.missing_required_families,
            "unknown_families": self.unknown_families,
            "duplicate_families": self.duplicate_families,
            "payload_not_frozen_families": self.payload_not_frozen_families,
            "legacy_ack_missing_families": self.legacy_ack_missing_families,
            "skein_ack_missing_families": self.skein_ack_missing_families,
            "independent_watermarks_missing_families": self.independent_watermarks_missing_families,
            "replay_not_idempotent_families": self.replay_not_idempotent_families,
            "search_projection_payload_not_frozen_families": self.search_projection_payload_not_frozen_families,
            "blocker_codes": self.blocker_codes,
        })
    }
}

pub fn nowledge_mem_source_mutation_family_requirements(
) -> Vec<NowledgeMemSourceMutationFamilyRequirement> {
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .map(|family| NowledgeMemSourceMutationFamilyRequirement {
            family: (*family).to_string(),
            requires_search_projection_payload: source_mutation_family_requires_projection(family),
        })
        .collect()
}

pub fn nowledge_mem_source_mutation_dual_write_evidence_all_ready(
) -> Vec<NowledgeMemSourceMutationDualWriteEvidence> {
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .map(|family| NowledgeMemSourceMutationDualWriteEvidence::ready(*family))
        .collect()
}

pub fn nowledge_mem_source_mutation_dual_write_readiness(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
) -> NowledgeMemSourceMutationDualWriteReadinessReport {
    let required_families = REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut family_counts = BTreeMap::<&str, usize>::new();
    for item in evidence {
        *family_counts.entry(item.family.as_str()).or_default() += 1;
    }
    let observed_required_families = family_counts
        .keys()
        .copied()
        .filter(|family| required_families.contains(family))
        .collect::<BTreeSet<_>>();
    let missing_required_families = REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .copied()
        .filter(|family| !observed_required_families.contains(family))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_families = family_counts
        .keys()
        .copied()
        .filter(|family| !required_families.contains(family))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_families = family_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(family, _)| (*family).to_string())
        .collect::<Vec<_>>();
    let payload_not_frozen_families =
        source_mutation_families_where(evidence, |item| !item.payload_frozen);
    let legacy_ack_missing_families =
        source_mutation_families_where(evidence, |item| !item.legacy_ack_recorded);
    let skein_ack_missing_families =
        source_mutation_families_where(evidence, |item| !item.skein_ack_recorded);
    let independent_watermarks_missing_families =
        source_mutation_families_where(evidence, |item| !item.independent_watermarks_recorded);
    let replay_not_idempotent_families =
        source_mutation_families_where(evidence, |item| !item.replay_idempotent);
    let search_projection_payload_not_frozen_families =
        source_mutation_families_where(evidence, |item| {
            source_mutation_family_requires_projection(&item.family)
                && !item.search_projection_payload_frozen
        });
    let ready_families = source_mutation_families_where(evidence, |item| {
        required_families.contains(item.family.as_str())
            && item.payload_frozen
            && item.legacy_ack_recorded
            && item.skein_ack_recorded
            && item.independent_watermarks_recorded
            && item.replay_idempotent
            && (!source_mutation_family_requires_projection(&item.family)
                || item.search_projection_payload_frozen)
    });

    let mut blocker_codes = Vec::new();
    if !missing_required_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_missing_required_families".to_string());
    }
    if !unknown_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_unknown_families".to_string());
    }
    if !duplicate_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_duplicate_families".to_string());
    }
    if !payload_not_frozen_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_payload_not_frozen".to_string());
    }
    if !legacy_ack_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_legacy_ack_missing".to_string());
    }
    if !skein_ack_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_skein_ack_missing".to_string());
    }
    if !independent_watermarks_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_independent_watermarks_missing".to_string());
    }
    if !replay_not_idempotent_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_replay_not_idempotent".to_string());
    }
    if !search_projection_payload_not_frozen_families.is_empty() {
        blocker_codes
            .push("source_mutation_dual_write_search_projection_payload_not_frozen".to_string());
    }

    let ready = blocker_codes.is_empty();
    NowledgeMemSourceMutationDualWriteReadinessReport {
        protocol: NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL.to_string(),
        ready,
        required_family_count: REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
        evidence_family_count: observed_required_families.len(),
        ready_family_count: ready_families.len(),
        requirements: nowledge_mem_source_mutation_family_requirements(),
        evidence: normalized_source_mutation_evidence(evidence),
        ready_families,
        missing_required_families,
        unknown_families,
        duplicate_families,
        payload_not_frozen_families,
        legacy_ack_missing_families,
        skein_ack_missing_families,
        independent_watermarks_missing_families,
        replay_not_idempotent_families,
        search_projection_payload_not_frozen_families,
        blocker_codes,
    }
}

fn source_mutation_family_requires_projection(family: &str) -> bool {
    matches!(
        family,
        NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS
    )
}

fn source_mutation_families_where(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
    predicate: impl Fn(&NowledgeMemSourceMutationDualWriteEvidence) -> bool,
) -> Vec<String> {
    let mut families = evidence
        .iter()
        .filter(|item| predicate(item))
        .map(|item| item.family.clone())
        .collect::<Vec<_>>();
    families.sort();
    families.dedup();
    families
}

fn normalized_source_mutation_evidence(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
) -> Vec<NowledgeMemSourceMutationDualWriteEvidence> {
    let mut normalized = evidence.to_vec();
    normalized.sort_by(|left, right| left.family.cmp(&right.family));
    normalized
}

fn source_mutation_dual_write_evidence_json(
    evidence: &NowledgeMemSourceMutationDualWriteEvidence,
) -> serde_json::Value {
    serde_json::json!({
        "family": evidence.family,
        "payload_frozen": evidence.payload_frozen,
        "legacy_ack_recorded": evidence.legacy_ack_recorded,
        "skein_ack_recorded": evidence.skein_ack_recorded,
        "independent_watermarks_recorded": evidence.independent_watermarks_recorded,
        "replay_idempotent": evidence.replay_idempotent,
        "search_projection_payload_frozen": evidence.search_projection_payload_frozen,
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod differential;
