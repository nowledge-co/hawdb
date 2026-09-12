use super::{
    nowledge_mem_source_mutation_dual_write_evidence_all_ready,
    nowledge_mem_source_mutation_dual_write_readiness,
    nowledge_mem_source_mutation_family_requirements, NowledgeMemSourceMutationDualWriteEvidence,
};
use serde_json::{json, Value};

// Freeze the pre-extraction family order and projection contract independently
// of the production inventory and constructors.
const FAMILIES: &[(&str, bool)] = &[
    ("source_patch_delete", false),
    ("source_lifecycle", false),
    ("source_graph_delete", false),
    ("source_ingest_create", true),
    ("source_content_refresh_reparse", true),
    ("source_indexed_transition", true),
    ("source_revision_edges", false),
    ("source_search_projection_effects", true),
];
const UNKNOWN: &[&str] = &[
    "unknown",
    "",
    " ",
    "SOURCE_LIFECYCLE",
    "\u{2603}/source",
    "\0unknown",
];
const PAYLOAD: u8 = 1;
const LEGACY_ACK: u8 = 2;
const SKEIN_ACK: u8 = 4;
const WATERMARKS: u8 = 8;
const REPLAY: u8 = 16;
const PROJECTION: u8 = 32;

#[derive(Clone, Debug)]
struct Row {
    family: String,
    bits: u8,
}

impl Row {
    fn evidence(&self) -> NowledgeMemSourceMutationDualWriteEvidence {
        NowledgeMemSourceMutationDualWriteEvidence {
            family: self.family.clone(),
            payload_frozen: self.bits & PAYLOAD != 0,
            legacy_ack_recorded: self.bits & LEGACY_ACK != 0,
            skein_ack_recorded: self.bits & SKEIN_ACK != 0,
            independent_watermarks_recorded: self.bits & WATERMARKS != 0,
            replay_idempotent: self.bits & REPLAY != 0,
            search_projection_payload_frozen: self.bits & PROJECTION != 0,
        }
    }

    fn json(&self) -> Value {
        json!({
            "family": self.family,
            "payload_frozen": self.bits & PAYLOAD != 0,
            "legacy_ack_recorded": self.bits & LEGACY_ACK != 0,
            "skein_ack_recorded": self.bits & SKEIN_ACK != 0,
            "independent_watermarks_recorded": self.bits & WATERMARKS != 0,
            "replay_idempotent": self.bits & REPLAY != 0,
            "search_projection_payload_frozen": self.bits & PROJECTION != 0,
        })
    }
}

#[test]
fn constructors_preserve_family_and_projection_contract() {
    let expected = complete_rows();
    assert_eq!(
        nowledge_mem_source_mutation_dual_write_evidence_all_ready(),
        expected.iter().map(Row::evidence).collect::<Vec<_>>()
    );
    for row in expected {
        assert_eq!(
            NowledgeMemSourceMutationDualWriteEvidence::ready(&row.family),
            row.evidence()
        );
    }
    let requirements = nowledge_mem_source_mutation_family_requirements();
    assert_eq!(requirements.len(), FAMILIES.len());
    for (requirement, (family, projection)) in requirements.iter().zip(FAMILIES) {
        assert_eq!(requirement.family, *family);
        assert_eq!(requirement.requires_search_projection_payload, *projection);
    }
    for family in UNKNOWN {
        let row = Row {
            family: (*family).to_string(),
            bits: 31,
        };
        assert_eq!(
            NowledgeMemSourceMutationDualWriteEvidence::ready(*family),
            row.evidence()
        );
    }
}

#[test]
fn source_mutation_readiness_differential_smoke() {
    campaign(&[0, 31, 32, 63], 4, 16);
}

#[test]
#[ignore = "explicit local exhaustive and generated readiness campaign"]
fn source_mutation_readiness_differential_campaign() {
    campaign(&(0..64).collect::<Vec<_>>(), 128, 64);
}

fn campaign(pair_masks: &[u8], seeds: u64, generated_per_seed: usize) {
    let mut single_cases = 0;
    let mut duplicate_cases = 0;
    for family in 0..FAMILIES.len() {
        for bits in 0..64 {
            let mut rows = complete_rows();
            rows[family].bits = bits;
            check(&rows, &format!("single family={family} bits={bits}"));
            single_cases += 1;
        }
        for &left in pair_masks {
            for &right in pair_masks {
                let mut rows = complete_rows();
                rows[family].bits = left;
                rows.insert(
                    family + 1,
                    Row {
                        family: FAMILIES[family].0.to_string(),
                        bits: right,
                    },
                );
                check(
                    &rows,
                    &format!("duplicate family={family} left={left} right={right}"),
                );
                duplicate_cases += 1;
            }
        }
    }
    let mut mixed_cases = 0;
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        for case in 0..generated_per_seed {
            let mut rows = Vec::new();
            for _ in 0..rng.below(24) {
                let choice = rng.below(FAMILIES.len() + UNKNOWN.len());
                let family = if choice < FAMILIES.len() {
                    FAMILIES[choice].0
                } else {
                    UNKNOWN[choice - FAMILIES.len()]
                };
                rows.push(Row {
                    family: family.to_string(),
                    bits: rng.below(64) as u8,
                });
            }
            check(&rows, &format!("mixed seed={seed} case={case}"));
            mixed_cases += 1;
        }
    }
    eprintln!(
        "source-mutation-readiness-differential-v1 seeds={seeds} single_cases={single_cases} duplicate_cases={duplicate_cases} mixed_cases={mixed_cases}"
    );
}

fn complete_rows() -> Vec<Row> {
    FAMILIES
        .iter()
        .map(|(family, projection)| Row {
            family: (*family).to_string(),
            bits: if *projection { 63 } else { 31 },
        })
        .collect()
}

fn check(rows: &[Row], case: &str) {
    let evidence = rows.iter().map(Row::evidence).collect::<Vec<_>>();
    let before = evidence.clone();
    let report = nowledge_mem_source_mutation_dual_write_readiness(&evidence);
    assert_eq!(report.json(), reference(rows), "{case}: {rows:?}");
    assert_eq!(evidence, before, "input mutated: {case}");
    assert_eq!(report.ready, report.blocker_codes.is_empty(), "{case}");
    assert_eq!(
        report.ready_family_count,
        report.ready_families.len(),
        "{case}"
    );
    assert_eq!(
        report.evidence.len(),
        rows.len(),
        "duplicate evidence dropped: {case}"
    );
}

fn reference(rows: &[Row]) -> Value {
    let observed = FAMILIES
        .iter()
        .filter(|(family, _)| rows.iter().any(|row| row.family == *family))
        .count();
    let missing = FAMILIES
        .iter()
        .filter(|(family, _)| !rows.iter().any(|row| row.family == *family))
        .map(|(family, _)| *family)
        .collect::<Vec<_>>();
    let unknown = select(rows, |row| {
        !FAMILIES.iter().any(|(family, _)| row.family == *family)
    });
    let duplicates = select(rows, |row| {
        rows.iter()
            .filter(|other| other.family == row.family)
            .count()
            > 1
    });
    let payload = select(rows, |row| row.bits & PAYLOAD == 0);
    let legacy_ack = select(rows, |row| row.bits & LEGACY_ACK == 0);
    let skein_ack = select(rows, |row| row.bits & SKEIN_ACK == 0);
    let watermarks = select(rows, |row| row.bits & WATERMARKS == 0);
    let replay = select(rows, |row| row.bits & REPLAY == 0);
    let projection = select(rows, |row| {
        FAMILIES
            .iter()
            .any(|(family, needed)| *needed && row.family == *family)
            && row.bits & PROJECTION == 0
    });
    let ready = select(rows, |row| {
        FAMILIES.iter().any(|(family, projection)| {
            let required = if *projection { 63 } else { 31 };
            row.family == *family && row.bits & required == required
        })
    });
    let mut blockers = Vec::new();
    for (blocked, code) in [
        (
            !missing.is_empty(),
            "source_mutation_dual_write_missing_required_families",
        ),
        (
            !unknown.is_empty(),
            "source_mutation_dual_write_unknown_families",
        ),
        (
            !duplicates.is_empty(),
            "source_mutation_dual_write_duplicate_families",
        ),
        (
            !payload.is_empty(),
            "source_mutation_dual_write_payload_not_frozen",
        ),
        (
            !legacy_ack.is_empty(),
            "source_mutation_dual_write_legacy_ack_missing",
        ),
        (
            !skein_ack.is_empty(),
            "source_mutation_dual_write_skein_ack_missing",
        ),
        (
            !watermarks.is_empty(),
            "source_mutation_dual_write_independent_watermarks_missing",
        ),
        (
            !replay.is_empty(),
            "source_mutation_dual_write_replay_not_idempotent",
        ),
        (
            !projection.is_empty(),
            "source_mutation_dual_write_search_projection_payload_not_frozen",
        ),
    ] {
        if blocked {
            blockers.push(code);
        }
    }
    // Sort by original position on ties. The oracle does not share the reducer's
    // stable sort, but still checks that conflicting duplicate rows remain intact
    // and retain input order rather than being collapsed or arbitrarily reordered.
    let mut positions = (0..rows.len()).collect::<Vec<_>>();
    positions.sort_unstable_by_key(|&index| (&rows[index].family, index));
    json!({
        "protocol": "skein-nowledge-mem-source-mutation-dual-write-readiness-v1",
        "ready": blockers.is_empty(),
        "required_family_count": FAMILIES.len(),
        "evidence_family_count": observed,
        "ready_family_count": ready.len(),
        "requirements": FAMILIES.iter().map(|(family, projection)| json!({
            "family": family, "requires_search_projection_payload": projection,
        })).collect::<Vec<_>>(),
        "evidence": positions.iter().map(|&index| rows[index].json()).collect::<Vec<_>>(),
        "ready_families": ready,
        "missing_required_families": missing,
        "unknown_families": unknown,
        "duplicate_families": duplicates,
        "payload_not_frozen_families": payload,
        "legacy_ack_missing_families": legacy_ack,
        "skein_ack_missing_families": skein_ack,
        "independent_watermarks_missing_families": watermarks,
        "replay_not_idempotent_families": replay,
        "search_projection_payload_not_frozen_families": projection,
        "blocker_codes": blockers,
    })
}

fn select(rows: &[Row], predicate: impl Fn(&Row) -> bool) -> Vec<&str> {
    let mut result = Vec::new();
    for row in rows {
        if predicate(row) && !result.contains(&row.family.as_str()) {
            result.push(row.family.as_str());
        }
    }
    result.sort();
    result
}

struct Generator(u64);

impl Generator {
    fn below(&mut self, limit: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % limit as u64) as usize
    }
}
