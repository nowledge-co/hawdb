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
use crate::relational_join_cost::RelationalJoinCardinality;
use crate::{
    enumerate_relational_inner_joins, RelationalAccessPathDescriptor, RelationalAccessPathKind,
};
use std::collections::BTreeSet;

#[test]
fn memo_identity_includes_applied_operator_set() {
    let bindings = BindingSet::from([BindingId::new(1), BindingId::new(2)]);
    let first = SemanticKey {
        bindings: bindings.clone(),
        applied_operators: BTreeSet::from([RelationalJoinOperatorId::new(1)]),
    };
    let second = SemanticKey {
        bindings,
        applied_operators: BTreeSet::from([RelationalJoinOperatorId::new(2)]),
    };
    assert_ne!(first, second);
}

#[test]
fn thirteen_relation_chain_uses_actual_connected_memo_budget() {
    let result = enumerate_relational_inner_joins(
        &chain(13),
        &RequiredProperties::default(),
        RelationalJoinEnumerationConfig::default(),
    )
    .expect("a chain has only quadratic connected memo state");
    assert_eq!(result.memo_groups, 13 * 14 / 2);
    assert_eq!(result.memo_expressions, 13 * 13);
    assert_eq!(result.plan.binding_order().len(), 13);
}

#[test]
fn sixty_four_relation_chain_is_not_limited_by_a_subset_bitmask() {
    let result = enumerate_relational_inner_joins(
        &chain(64),
        &RequiredProperties::default(),
        RelationalJoinEnumerationConfig::default(),
    )
    .unwrap();
    assert_eq!(result.memo_groups, 64 * 65 / 2);
    assert_eq!(result.memo_expressions, 64 * 64);
    assert_eq!(
        result
            .plan
            .binding_order()
            .into_iter()
            .collect::<BindingSet>()
            .len(),
        64
    );
}

#[test]
fn exact_connected_budgets_succeed_and_one_less_fails_at_the_next_insertion() {
    let graph = chain(13);
    let config = RelationalJoinEnumerationConfig {
        max_groups: 91,
        max_expressions: 169,
    };
    enumerate_relational_inner_joins(&graph, &RequiredProperties::default(), config).unwrap();
    assert_eq!(
        enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig {
                max_groups: 90,
                ..config
            },
        ),
        Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
            required_groups: 91,
            max_groups: 90
        })
    );
    assert_eq!(
        enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig {
                max_expressions: 168,
                ..config
            },
        ),
        Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: 169,
            max_expressions: 168
        })
    );
    assert_eq!(
        enumerate_relational_inner_joins(
            &chain(1),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig {
                max_groups: 0,
                max_expressions: 1
            },
        ),
        Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
            required_groups: 1,
            max_groups: 0
        })
    );
    assert_eq!(
        enumerate_relational_inner_joins(
            &chain(1),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig {
                max_groups: 1,
                max_expressions: 0
            },
        ),
        Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: 1,
            max_expressions: 0
        })
    );
}

#[test]
fn failed_memo_admission_does_not_mutate_groups_or_expressions() {
    let binding = BindingId::new(0);
    let config = RelationalJoinEnumerationConfig {
        max_groups: 1,
        max_expressions: 2,
    };
    let mut memo = JoinMemo::new();
    memo.add(
        SemanticKey::relation(binding),
        JoinExpression::Relation(binding),
        config,
    )
    .unwrap();
    let other = BindingId::new(1);
    let before = normalize_memo(&memo);
    assert_eq!(
        memo.add(
            SemanticKey::relation(other),
            JoinExpression::Relation(other),
            config
        ),
        Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
            required_groups: 2,
            max_groups: 1
        })
    );
    assert_eq!(normalize_memo(&memo), before);
    assert_eq!(memo.expression_count, 1);
    assert_eq!(memo.keys.len(), 1);
    let exhausted = RelationalJoinEnumerationConfig {
        max_expressions: 1,
        ..config
    };
    assert_eq!(
        memo.add(
            SemanticKey::relation(binding),
            JoinExpression::Relation(binding),
            exhausted
        ),
        Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: 2,
            max_expressions: 1
        })
    );
    assert_eq!(normalize_memo(&memo), before);
    assert_eq!(memo.expression_count, 1);
}

#[test]
fn genuinely_exponential_star_still_stops_at_the_actual_group_budget() {
    let mut graph = chain(13);
    for predicate in &mut graph.predicates {
        predicate.bindings = [BindingId::new(0), BindingId::new(predicate.id.get())].into();
    }
    assert_eq!(
        enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        ),
        Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
            required_groups: 4096,
            max_groups: 4095
        })
    );
}

#[test]
fn all_five_relation_graphs_match_exhaustive_subset_pairs() {
    let edges = (0..5)
        .flat_map(|left| (left + 1..5).map(move |right| (left, right)))
        .collect::<Vec<_>>();
    for mask in 0..1u32 << edges.len() {
        let mut graph = chain(5);
        graph.predicates = edges
            .iter()
            .enumerate()
            .filter(|(index, _)| mask & (1 << index) != 0)
            .map(|(index, (left, right))| RelationalJoinPredicate {
                id: RelationalJoinPredicateId::new(index as u32),
                bindings: [BindingId::new(*left), BindingId::new(*right)].into(),
            })
            .collect();
        let memo = build_join_memo(
            &graph.relations,
            Connectivity::Predicates(&graph.predicates),
            EnumerationDomain::Inner,
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();
        let expected = exhaustive_subset_pairs(&graph);
        assert_eq!(normalize_memo(&memo), expected, "graph mask={mask}");
        assert_eq!(
            memo.expression_count,
            expected
                .iter()
                .map(|(bindings, pairs)| if bindings.len() == 1 { 1 } else { pairs.len() })
                .sum::<usize>()
        );
    }
}

#[test]
fn hyperedges_and_shuffled_bindings_preserve_all_predicate_activations() {
    for seed in 1..=32u64 {
        let mut state = seed;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut graph = chain(7);
        graph.predicates.clear();
        for id in 0..12 {
            let bindings = (0..7)
                .filter(|_| next() % 3 == 0)
                .map(BindingId::new)
                .collect::<BindingSet>();
            if bindings.len() >= 2 {
                graph.predicates.push(RelationalJoinPredicate {
                    id: RelationalJoinPredicateId::new(id),
                    bindings,
                });
            }
        }
        // Duplicate edge shapes have distinct IDs and must activate together.
        if let Some(mut duplicate) = graph.predicates.first().cloned() {
            duplicate.id = RelationalJoinPredicateId::new(100);
            graph.predicates.push(duplicate);
        }
        graph.predicates.reverse();
        let remap = |binding: BindingId| BindingId::new(100 - binding.get() * 7);
        for relation in &mut graph.relations {
            relation.binding = remap(relation.binding);
        }
        for predicate in &mut graph.predicates {
            predicate.bindings = predicate.bindings.iter().map(remap).collect();
        }
        graph.relations.rotate_left(seed as usize % 7);
        let memo = build_join_memo(
            &graph.relations,
            Connectivity::Predicates(&graph.predicates),
            EnumerationDomain::Inner,
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();
        assert_eq!(
            normalize_memo(&memo),
            exhaustive_subset_pairs(&graph),
            "seed={seed}"
        );
    }
    let mut hyperedge_only = chain(3);
    hyperedge_only.predicates = vec![RelationalJoinPredicate {
        id: RelationalJoinPredicateId::new(0),
        bindings: [BindingId::new(0), BindingId::new(1), BindingId::new(2)].into(),
    }];
    assert_eq!(
        enumerate_relational_inner_joins(
            &hyperedge_only,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        ),
        Err(RelationalJoinEnumerationError::Disconnected)
    );
}

#[test]
fn selected_plans_match_exhaustive_connected_permutations() {
    let mut compared = 0;
    for seed in 0..24 {
        let mut graph = chain(5);
        for (index, relation) in graph.relations.iter_mut().enumerate() {
            relation.access_paths[0].descriptor.estimated_rows = 1 + (index * 3 + seed) % 7;
            if (index + seed) % 2 == 0 {
                relation.access_paths[0].properties.ordering = vec!["ordered".to_owned()];
            }
        }
        if seed % 2 == 0 {
            graph.predicates.push(RelationalJoinPredicate {
                id: RelationalJoinPredicateId::new(10),
                bindings: [BindingId::new(0), BindingId::new(4)].into(),
            });
        }
        for ordering in [
            Vec::new(),
            vec!["ordered".to_owned()],
            vec!["unavailable".to_owned()],
        ] {
            let required = RequiredProperties {
                ordering,
                ..RequiredProperties::default()
            };
            let expected = exhaustive_plan(&graph, &required);
            let actual = enumerate_relational_inner_joins(
                &graph,
                &required,
                RelationalJoinEnumerationConfig::default(),
            );
            match expected {
                Some(expected) => assert_eq!(actual.unwrap().plan, expected),
                None => assert_eq!(
                    actual,
                    Err(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)
                ),
            }
            compared += 1;
        }
    }
    assert_eq!(compared, 72);
}

type Pairs = BTreeMap<BindingSet, Vec<(BindingSet, BindingId, Vec<RelationalJoinPredicateId>)>>;

fn normalize_memo(memo: &JoinMemo) -> Pairs {
    memo.groups
        .iter()
        .map(|(key, group)| {
            let mut pairs = Vec::new();
            assert!(key.applied_operators.is_empty());
            for expression in memo.memo.group(*group).unwrap().expressions() {
                match expression {
                    JoinExpression::Relation(binding) => {
                        assert_eq!(key.bindings, BindingSet::from(*binding))
                    }
                    JoinExpression::Join {
                        left,
                        right,
                        operator_id,
                        operator_kind,
                        predicate_ids,
                    } => {
                        assert!(operator_id.is_none());
                        assert_eq!(*operator_kind, RelationalJoinOperatorKind::Inner);
                        assert_eq!(memo.keys[right].bindings.len(), 1);
                        pairs.push((
                            memo.keys[left].bindings.clone(),
                            memo.keys[right].bindings.iter().next().unwrap(),
                            predicate_ids.clone(),
                        ));
                    }
                }
            }
            pairs.sort();
            (key.bindings.clone(), pairs)
        })
        .collect()
}

// Keep the exhaustive oracle's tie-break independent from the shared selector.
fn compare_plans(left: &RelationalJoinPlan, right: &RelationalJoinPlan) -> std::cmp::Ordering {
    let key = |plan: &RelationalJoinPlan| {
        std::iter::once((plan.base_binding, &plan.base_access_path))
            .chain(
                plan.steps
                    .iter()
                    .map(|step| (step.binding, &step.access_path)),
            )
            .map(|(binding, access)| format!("{}:{}", binding.get(), access.descriptor.name))
            .collect::<Vec<_>>()
            .join("/")
    };
    left.cost_breakdown
        .cost
        .cmp(&right.cost_breakdown.cost)
        .then_with(|| {
            left.cost_breakdown
                .estimated_rows
                .cmp(&right.cost_breakdown.estimated_rows)
        })
        .then_with(|| key(left).cmp(&key(right)))
}

// Deliberately enumerate every small subset and partition in the oracle; it
// shares neither frontier traversal nor complement detection with production.
fn exhaustive_subset_pairs(graph: &RelationalJoinGraph) -> Pairs {
    let bindings = graph
        .relations
        .iter()
        .map(|relation| relation.binding)
        .collect::<Vec<_>>();
    assert!(bindings.len() <= 7);
    let mut groups: Pairs = BTreeMap::new();
    for size in 1..=bindings.len() {
        for mask in 1..1u32 << bindings.len() {
            if mask.count_ones() as usize != size {
                continue;
            }
            let subset = bindings
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, binding)| *binding)
                .collect::<BindingSet>();
            let mut pairs = Vec::new();
            for right in subset.iter() {
                let left = subset.without(right);
                if !groups.contains_key(&left) {
                    continue;
                }
                let predicates = graph
                    .predicates
                    .iter()
                    .filter(|predicate| {
                        predicate.bindings.is_subset(&subset)
                            && !predicate.bindings.is_subset(&left)
                    })
                    .map(|predicate| predicate.id)
                    .collect::<Vec<_>>();
                if !predicates.is_empty() {
                    pairs.push((left, right, predicates));
                }
            }
            if size == 1 || !pairs.is_empty() {
                pairs.sort();
                groups.insert(subset, pairs);
            }
        }
    }
    groups
}

fn exhaustive_plan(
    graph: &RelationalJoinGraph,
    required: &RequiredProperties,
) -> Option<RelationalJoinPlan> {
    fn extend(
        graph: &RelationalJoinGraph,
        plan: RelationalJoinPlan,
        best: &mut Option<RelationalJoinPlan>,
    ) {
        let bindings = plan.binding_order().into_iter().collect::<BindingSet>();
        if bindings.len() == graph.relations.len() {
            if best
                .as_ref()
                .is_none_or(|current| compare_plans(&plan, current).is_lt())
            {
                *best = Some(plan);
            }
            return;
        }
        for relation in &graph.relations {
            if bindings.contains(relation.binding) {
                continue;
            }
            let mut available = bindings.clone();
            available.insert(relation.binding);
            let predicates = graph
                .predicates
                .iter()
                .filter(|predicate| {
                    predicate.bindings.is_subset(&available)
                        && !predicate.bindings.is_subset(&bindings)
                })
                .map(|predicate| predicate.id)
                .collect::<Vec<_>>();
            if predicates.is_empty() {
                continue;
            }
            for access in &relation.access_paths {
                if !access.supports_probe() || !access.required_bindings.is_subset(&bindings) {
                    continue;
                }
                let mut next = plan.clone();
                next.cost_breakdown = estimate_relational_join_cost(
                    next.cost_breakdown,
                    estimate_relational_access_path_cost(&access.descriptor),
                    RelationalJoinCardinality::Inner,
                    RelationalJoinRightInput::Probe,
                    RelationalJoinSelectivity::Unknown,
                );
                next.steps.push(RelationalJoinStep {
                    binding: relation.binding,
                    access_path: access.clone(),
                    activated_predicates: predicates.clone(),
                });
                extend(graph, next, best);
            }
        }
    }
    let mut best = None;
    for relation in &graph.relations {
        for access in &relation.access_paths {
            if access.supports_base() && access.properties.satisfies(required) {
                extend(
                    graph,
                    RelationalJoinPlan {
                        base_binding: relation.binding,
                        base_access_path: access.clone(),
                        steps: Vec::new(),
                        cost_breakdown: estimate_relational_access_path_cost(&access.descriptor),
                        properties: access.properties.clone(),
                    },
                    &mut best,
                );
            }
        }
    }
    best
}

fn chain(count: usize) -> RelationalJoinGraph {
    let relations = (0..count)
        .map(|index| RelationalJoinRelation {
            binding: BindingId::new(index as u32),
            access_paths: vec![RelationalJoinAccessPath::base_and_probe(
                RelationalAccessPathDescriptor {
                    kind: RelationalAccessPathKind::FullScan,
                    name: "__full_scan".to_owned(),
                    index_columns: Vec::new(),
                    access_columns: BTreeSet::new(),
                    equality_prefix_len: 0,
                    order_prefix_len: 0,
                    exclusive_range: false,
                    reverse_order: false,
                    unique_point: false,
                    covering: false,
                    requires_row_fetch: false,
                    estimated_rows: 1,
                },
            )],
        })
        .collect();
    let predicates = (1..count)
        .map(|index| RelationalJoinPredicate {
            id: RelationalJoinPredicateId::new(index as u32),
            bindings: [
                BindingId::new(index as u32 - 1),
                BindingId::new(index as u32),
            ]
            .into(),
        })
        .collect();
    RelationalJoinGraph {
        relations,
        predicates,
    }
}
