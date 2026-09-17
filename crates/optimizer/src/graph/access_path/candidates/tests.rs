use super::*;
use crate::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};

fn take_fingerprint_evaluations() -> usize {
    FINGERPRINT_EVALUATIONS.with(|count| count.replace(0))
}

#[test]
fn tied_equality_candidates_compute_each_fingerprint_at_most_once() {
    let properties = (0..32)
        .map(|index| format!("field_{index:03}"))
        .collect::<Vec<_>>();
    let predicates = properties
        .iter()
        .map(|property| Predicate::PropertyEq {
            variable: "n".into(),
            property: property.clone(),
            value: Value::String("shared-payload".repeat(8)),
        })
        .collect::<Vec<_>>();
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            properties
                .iter()
                .map(|property| ("Node".into(), property.clone())),
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Node".into(), 4096)],
            [],
            [],
            [],
            [],
            properties
                .iter()
                .map(|property| (("Node".into(), property.clone()), 16)),
            [],
        ),
    );
    let full_predicate = Predicate::And(predicates.clone());
    take_fingerprint_evaluations();
    let selected =
        equality_index_seek_candidate(&predicates, &full_predicate, "n", "Node", &catalog);
    assert!(selected.is_some());
    let evaluations = take_fingerprint_evaluations();
    assert_eq!(
        evaluations,
        properties.len(),
        "{evaluations} fingerprints for {} candidates",
        properties.len()
    );
}

#[derive(Clone, Copy, Debug)]
enum CandidateFamily {
    Equality,
    MultiSeek,
    CompositeEquality,
    Range,
    CompositeRange,
}

impl CandidateFamily {
    fn predicates(self, properties: &[String]) -> Vec<Predicate> {
        let mut predicates = Vec::new();
        if matches!(self, Self::CompositeEquality | Self::CompositeRange) {
            predicates.push(Predicate::PropertyEq {
                variable: "n".into(),
                property: "tenant".into(),
                value: Value::Int(7),
            });
        }
        for property in properties {
            predicates.push(match self {
                Self::Equality | Self::CompositeEquality => Predicate::PropertyEq {
                    variable: "n".into(),
                    property: property.clone(),
                    value: Value::Int(42),
                },
                Self::MultiSeek => Predicate::PropertyIn {
                    variable: "n".into(),
                    property: property.clone(),
                    values: vec![Value::Int(42)],
                },
                Self::Range | Self::CompositeRange => Predicate::PropertyCompare {
                    variable: "n".into(),
                    property: property.clone(),
                    op: skein_plan::ComparisonOp::Gt,
                    value: Value::Int(42),
                },
            });
        }
        predicates
    }

    fn select(
        self,
        predicates: &[Predicate],
        full: &Predicate,
        catalog: &OptimizerCatalog,
    ) -> AccessCandidate {
        let factory = match self {
            Self::Equality | Self::MultiSeek => equality_index_seek_candidate,
            Self::CompositeEquality => composite_index_seek_candidate,
            Self::Range => range_index_seek_candidate,
            Self::CompositeRange => composite_range_index_seek_candidate,
        };
        factory(predicates, full, "n", "Node", catalog).expect("indexed candidate")
    }
}

#[test]
fn every_rule_local_family_preserves_the_lexical_winner_without_recomputing_keys() {
    let properties = (0..16)
        .map(|index| format!("field_{index:03}"))
        .collect::<Vec<_>>();
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            properties
                .iter()
                .map(|property| ("Node".into(), property.clone())),
            properties
                .iter()
                .map(|property| ("Node".into(), vec!["tenant".into(), property.clone()])),
            properties
                .iter()
                .map(|property| ("Node".into(), property.clone())),
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Node".into(), 4096)],
            [],
            [],
            [],
            [],
            properties
                .iter()
                .cloned()
                .chain(std::iter::once("tenant".into()))
                .map(|property| (("Node".into(), property), 16)),
            properties.iter().cloned().map(|property| {
                (
                    ("Node".into(), property),
                    (0..48).map(Value::Int).collect::<Vec<_>>(),
                )
            }),
        ),
    );
    for family in [
        CandidateFamily::Equality,
        CandidateFamily::MultiSeek,
        CandidateFamily::CompositeEquality,
        CandidateFamily::Range,
        CandidateFamily::CompositeRange,
    ] {
        let predicates = family.predicates(&properties);
        let full = Predicate::And(predicates.clone());
        take_fingerprint_evaluations();
        let selected = family.select(&predicates, &full, &catalog);
        assert_eq!(
            take_fingerprint_evaluations(),
            properties.len(),
            "{family:?}"
        );
        let expected = properties
            .iter()
            .map(|property| {
                family.select(
                    &family.predicates(std::slice::from_ref(property)),
                    &full,
                    &catalog,
                )
            })
            .min_by_key(|candidate| candidate.plan.instance_fingerprint())
            .unwrap();
        assert_eq!(selected.plan, expected.plan, "{family:?}");
        assert_eq!(selected.decision, expected.decision, "{family:?}");
    }
}

fn synthetic_candidate(
    position: usize,
    key: u64,
    cost: u64,
    rule_id: &'static str,
) -> PhysicalCandidate {
    let mut plan = PhysicalPlan::IndexNodeSeek {
        variable: "n".into(),
        label: "Node".into(),
        property: format!("field_{}", key % 16),
        value: Value::String(format!("payload-{key}\u{1f4da}")),
    };
    for _ in 0..key % 4 {
        plan = PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyIn {
                variable: "n".into(),
                property: "tenant".into(),
                values: vec![Value::String("long-residual".repeat(16)); 4],
            },
            input: Box::new(plan),
        };
    }
    PhysicalCandidate {
        access: AccessCandidate {
            plan,
            decision: format!("candidate-{position}"),
            fingerprint: OnceCell::new(),
        },
        cost,
        rule_id,
    }
}

fn assert_matches_legacy_ranking(mut candidates: Vec<PhysicalCandidate>) {
    let mut expected = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.cost,
                candidate.access.plan.instance_fingerprint(),
                candidate.rule_id,
                candidate.access.plan.clone(),
                candidate.access.decision.clone(),
            )
        })
        .collect::<Vec<_>>();
    let insertion_order = expected.clone();
    // Retain the exact legacy ordering; the unique decision also checks stable ties.
    expected
        .sort_by(|left, right| (&left.0, &left.1, &left.2).cmp(&(&right.0, &right.1, &right.2)));
    take_fingerprint_evaluations();
    sort_candidates(&mut candidates);
    let evaluations = take_fingerprint_evaluations();
    assert!(evaluations <= candidates.len());
    assert_eq!(
        evaluations,
        candidates
            .iter()
            .filter(|candidate| candidate.access.fingerprint.get().is_some())
            .count()
    );
    for (actual, expected) in candidates.iter().zip(&expected) {
        assert_eq!((actual.cost, actual.rule_id), (expected.0, expected.2));
        assert_eq!(actual.access.plan, expected.3);
        assert_eq!(actual.access.decision, expected.4);
    }
    sort_candidates(&mut candidates);
    assert_eq!(take_fingerprint_evaluations(), 0);

    // Rule-local selection omits rule ID and retains the first exact tie.
    let mut expected_best = None;
    let mut actual_best = None;
    for (cost, fingerprint, _, plan, decision) in insertion_order {
        if expected_best
            .as_ref()
            .is_none_or(|(best_cost, best_key, _)| (cost, &fingerprint) < (*best_cost, best_key))
        {
            expected_best = Some((cost, fingerprint, decision.clone()));
        }
        keep_best_candidate(&mut actual_best, cost, plan, decision);
    }
    assert!(take_fingerprint_evaluations() <= candidates.len());
    assert_eq!(
        actual_best.map(|(cost, candidate)| (cost, candidate.decision)),
        expected_best.map(|(cost, _, decision)| (cost, decision))
    );
}

#[test]
fn final_ranking_preserves_cost_fingerprint_rule_and_stable_duplicate_order() {
    assert_matches_legacy_ranking(vec![
        synthetic_candidate(0, 7, 4, "z_rule"),
        synthetic_candidate(1, 7, 4, "a_rule"),
        synthetic_candidate(2, 7, 4, "a_rule"),
        synthetic_candidate(3, 2, 4, "z_rule"),
        synthetic_candidate(4, 1, u64::MAX, "a_rule"),
        synthetic_candidate(5, 99, 0, "z_rule"),
    ]);
}

#[test]
fn unequal_costs_never_construct_fingerprints() {
    let mut candidates = (0..32)
        .rev()
        .map(|position| {
            synthetic_candidate(position, 31 - position as u64, position as u64, "rule")
        })
        .collect::<Vec<_>>();
    take_fingerprint_evaluations();
    sort_candidates(&mut candidates);
    assert_eq!(take_fingerprint_evaluations(), 0);
    let mut best = None;
    for candidate in candidates {
        keep_best_candidate(
            &mut best,
            candidate.cost,
            candidate.access.plan,
            candidate.access.decision,
        );
    }
    assert_eq!(take_fingerprint_evaluations(), 0);
    assert_eq!(best.unwrap().0, 0);
}

#[test]
fn cached_keys_survive_rule_local_choice_and_final_costing() {
    let catalog = OptimizerCatalog::default();
    let mut candidates = Vec::new();
    for position in 0..2 {
        let candidate = synthetic_candidate(position, position as u64 * 4, 0, "rule");
        let _ = candidate.access.fingerprint();
        push_candidate(&mut candidates, Some(candidate.access), "rule", &catalog);
    }
    assert_eq!(candidates[0].cost, candidates[1].cost);
    take_fingerprint_evaluations();
    sort_candidates(&mut candidates);
    assert_eq!(take_fingerprint_evaluations(), 0);
}

#[test]
#[ignore = "local-only deterministic candidate ranking campaign"]
fn candidate_fingerprint_differential_campaign() {
    let mut candidate_count = 0;
    for seed in [216_u64, 7, 0x5eed] {
        let mut state = seed;
        for case in 0..256 {
            let mut candidates = Vec::new();
            for position in 0..case % 64 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let key = state % 16;
                let cost = match case % 4 {
                    0 => 0,
                    1 => u64::MAX,
                    _ => state % 7,
                };
                let rule = ["a_rule", "m_rule", "z_rule"][(state / 16 % 3) as usize];
                candidates.push(synthetic_candidate(position, key, cost, rule));
            }
            candidate_count += candidates.len();
            assert_matches_legacy_ranking(candidates);
        }
    }
    println!("Candidate ranking differential: 768 cases, {candidate_count} candidates");
}
