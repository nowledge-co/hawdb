// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::PropertyType;
use hawdb_plan_cypher::{ComparisonOp, CompositeOrderedSeek, SortDirection};

/// Marginal NDV cannot distinguish uniform matches from a rejected prefix.
/// Only discount an ordered alternative when its residual cannot reject access
/// rows. This is a local exact implication, not a statistical uniqueness proof.
pub(super) fn prefix_consumption_is_proven(plan: &PhysicalPlan) -> bool {
    let PhysicalPlan::NodeProjectionScanExec {
        variable,
        access: NodeProjectionAccess::CompositeOrdered { seek },
        predicate,
        ..
    } = plan
    else {
        return false;
    };
    fn implied(predicate: &Predicate, variable: &str, seek: &CompositeOrderedSeek) -> bool {
        match predicate {
            Predicate::ConstantBool(true) => true,
            Predicate::And(parts) => parts.iter().all(|part| implied(part, variable, seek)),
            Predicate::PropertyEq {
                variable: source,
                property,
                value,
            } => {
                source == variable
                    && seek
                        .equality_prefix
                        .iter()
                        .any(|(key, expected)| key == property && expected == value)
            }
            _ => {
                let Some(after) = &seek.after else {
                    return false;
                };
                let Some([first, second]) = seek.index_properties.get(seek.equality_prefix.len()..)
                else {
                    return false;
                };
                let op = if seek.descending {
                    ComparisonOp::Lt
                } else {
                    ComparisonOp::Gt
                };
                // The native seek already enforces this exact strict cursor.
                // Other residuals must still be proved independently.
                continuation(predicate, variable, first, second, op)
                    .is_some_and(|values| &values == after)
            }
        }
    }
    predicate
        .as_ref()
        .is_none_or(|predicate| implied(predicate, variable, seek))
}

fn conjuncts<'a>(predicate: &'a Predicate, output: &mut Vec<&'a Predicate>) {
    if let Predicate::And(parts) = predicate {
        for part in parts {
            conjuncts(part, output);
        }
    } else {
        output.push(predicate);
    }
}

fn property<'a>(key: &'a SortKey, items: &'a [Projection], variable: &str) -> Option<&'a str> {
    let expression = match key {
        SortKey::Property {
            variable: source,
            property,
        } if source == variable => return Some(property),
        SortKey::Expression(expression) => expression,
        SortKey::Column(column) => {
            let mut matches = items.iter().filter(|item| &item.name == column);
            let item = matches.next()?;
            if matches.next().is_some() {
                return None;
            }
            &item.expression
        }
        _ => return None,
    };
    match expression {
        ProjectionExpression::Property {
            variable: source,
            property,
        } if source == variable => Some(property),
        _ => None,
    }
}

fn equality<'a>(predicate: &'a Predicate, variable: &str, property: &str) -> Option<&'a Value> {
    match predicate {
        Predicate::PropertyEq {
            variable: source,
            property: key,
            value,
        } if source == variable && key == property => Some(value),
        _ => None,
    }
}

fn comparison<'a>(
    predicate: &'a Predicate,
    variable: &str,
    property: &str,
    op: ComparisonOp,
) -> Option<&'a Value> {
    match predicate {
        Predicate::PropertyCompare {
            variable: source,
            property: key,
            op: actual,
            value,
        } if source == variable && key == property && *actual == op => Some(value),
        _ => None,
    }
}

fn continuation(
    predicate: &Predicate,
    variable: &str,
    first: &str,
    second: &str,
    op: ComparisonOp,
) -> Option<Vec<Value>> {
    let Predicate::Or(branches) = predicate else {
        return None;
    };
    if branches.len() != 2 {
        return None;
    }
    for (one, two) in [(0, 1), (1, 0)] {
        let Some(first_value) = comparison(&branches[one], variable, first, op) else {
            continue;
        };
        let mut parts = Vec::new();
        conjuncts(&branches[two], &mut parts);
        if parts.len() != 2 {
            continue;
        }
        for (eq, cmp) in [(0, 1), (1, 0)] {
            if equality(parts[eq], variable, first) == Some(first_value)
                && let Some(second_value) = comparison(parts[cmp], variable, second, op)
            {
                return Some(vec![first_value.clone(), second_value.clone()]);
            }
        }
    }
    None
}

// Retain source constraints even when a previous rule consumed its filter.
fn access_predicates(access: &NodeProjectionAccess, variable: &str) -> Option<Vec<Predicate>> {
    let eq = |property: &String, value: &Value| Predicate::PropertyEq {
        variable: variable.into(),
        property: property.clone(),
        value: value.clone(),
    };
    let range =
        |property: &String, lower: &Option<(Value, bool)>, upper: &Option<(Value, bool)>| {
            lower
                .iter()
                .map(|(value, inclusive)| Predicate::PropertyCompare {
                    variable: variable.into(),
                    property: property.clone(),
                    op: if *inclusive {
                        ComparisonOp::Gte
                    } else {
                        ComparisonOp::Gt
                    },
                    value: value.clone(),
                })
                .chain(
                    upper
                        .iter()
                        .map(|(value, inclusive)| Predicate::PropertyCompare {
                            variable: variable.into(),
                            property: property.clone(),
                            op: if *inclusive {
                                ComparisonOp::Lte
                            } else {
                                ComparisonOp::Lt
                            },
                            value: value.clone(),
                        }),
                )
                .collect::<Vec<_>>()
        };
    Some(match access {
        NodeProjectionAccess::LabelScan => vec![],
        NodeProjectionAccess::PropertyValues { property, values } => {
            if let [value] = values.as_slice() {
                vec![eq(property, value)]
            } else {
                vec![Predicate::Or(
                    values.iter().map(|value| eq(property, value)).collect(),
                )]
            }
        }
        NodeProjectionAccess::CompositeEquality { predicates } => predicates
            .iter()
            .map(|(property, value)| eq(property, value))
            .collect(),
        NodeProjectionAccess::PropertyRange {
            property,
            lower,
            upper,
        } => range(property, lower, upper),
        NodeProjectionAccess::CompositeRange { seek } => {
            let mut parts = seek
                .equality_prefix
                .iter()
                .map(|(property, value)| eq(property, value))
                .collect::<Vec<_>>();
            parts.extend(range(&seek.range_property, &seek.lower, &seek.upper));
            parts
        }
        _ => return None,
    })
}

pub(super) fn candidate(
    input: &PhysicalPlan,
    order: &[SortItem],
    catalog: &OptimizerCatalog,
) -> Option<PhysicalPlan> {
    if catalog.ordered_composite_indexes.is_empty()
        || order.len() != 2
        || order[0].direction != order[1].direction
    {
        return None;
    }
    let fused;
    let input = if let PhysicalPlan::ProjectExec { items, input } = input {
        fused = select_node_projection_scan(items, input, &mut Vec::new(), &mut Vec::new())?;
        &fused
    } else {
        input
    };
    let PhysicalPlan::NodeProjectionScanExec {
        variable,
        label,
        access,
        required_properties,
        predicate,
        items,
    } = input
    else {
        return None;
    };
    let first = property(&order[0].key, items, variable)?;
    let second = property(&order[1].key, items, variable)?;
    let mut predicates = access_predicates(access, variable)?;
    if let Some(predicate) = predicate {
        // An access constraint can also remain in the residual filter. Keep
        // one exact syntactic copy: changing access must not make costing
        // multiply the same selectivity twice. These synthesized constraints
        // are pure property comparisons; no errorful expression is reordered.
        let mut residual_parts = Vec::new();
        conjuncts(predicate, &mut residual_parts);
        predicates.retain(|part| !residual_parts.contains(&part));
        predicates.push(predicate.clone());
    }
    let predicate = Predicate::And(predicates);
    let mut parts = Vec::new();
    conjuncts(&predicate, &mut parts);
    for ((index_label, properties), types) in &catalog.ordered_composite_indexes {
        if index_label != label
            || properties.len() < 3
            || properties[properties.len() - 2..] != [first, second]
        {
            continue;
        }
        let prefix_len = properties.len() - 2;
        let prefix = properties[..prefix_len]
            .iter()
            .zip(types)
            .map(|(property, declared)| {
                let value = parts
                    .iter()
                    .find_map(|part| equality(part, variable, property))?;
                let kind = catalog.scalar_type(value)?;
                // The initial numeric-prefix opt-in is limited to an enforced Int domain.
                if !matches!(kind, PropertyType::Bool | PropertyType::String)
                    && !(kind == PropertyType::Int && *declared == PropertyType::Int)
                {
                    return None;
                }
                Some((property.clone(), value.clone()))
            })
            .collect::<Option<Vec<_>>>();
        let Some(prefix) = prefix else {
            continue;
        };
        let descending = order[0].direction == SortDirection::Desc;
        let op = if descending {
            ComparisonOp::Lt
        } else {
            ComparisonOp::Gt
        };
        let after = parts
            .iter()
            .find_map(|part| continuation(part, variable, first, second, op));
        if let Some(values) = &after
            && !values
                .iter()
                .zip(&types[prefix_len..])
                .all(|(value, expected)| catalog.scalar_type(value).as_ref() == Some(expected))
        {
            continue;
        }
        let mut required = required_properties.iter().cloned().collect::<BTreeSet<_>>();
        required.extend(properties.iter().cloned());
        return Some(PhysicalPlan::NodeProjectionScanExec {
            variable: variable.clone(),
            label: label.clone(),
            access: NodeProjectionAccess::CompositeOrdered {
                seek: CompositeOrderedSeek {
                    index_properties: properties.clone(),
                    equality_prefix: prefix,
                    after,
                    descending,
                },
            },
            required_properties: required.into_iter().collect(),
            predicate: Some(predicate),
            items: items.clone(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};

    fn catalog(proven: bool) -> OptimizerCatalog {
        let properties = vec!["kind".into(), "rank".into(), "id".into()];
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [("Item".into(), properties)], [], []),
            OptimizerCatalogStatistics::new(
                [("Item".into(), 1000)],
                [],
                [],
                [],
                [],
                [(("Item".into(), "kind".into()), 10)],
                [],
            ),
        );
        if proven {
            catalog.with_ordered_composite_indexes([(
                "Item".into(),
                vec![
                    ("kind".into(), PropertyType::Any),
                    ("rank".into(), PropertyType::Int),
                    ("id".into(), PropertyType::String),
                ],
            )])
        } else {
            catalog
        }
    }

    fn logical(descending: bool, rank: Value) -> LogicalPlan {
        let op = if descending {
            ComparisonOp::Lt
        } else {
            ComparisonOp::Gt
        };
        let compare = |property: &str, value| Predicate::PropertyCompare {
            variable: "n".into(),
            property: property.into(),
            op,
            value,
        };
        LogicalPlan::Limit {
            offset: 0,
            limit: Some(7),
            input: Box::new(LogicalPlan::Sort {
                items: ["rank", "id"]
                    .into_iter()
                    .map(|name| SortItem {
                        key: SortKey::Column(name.into()),
                        direction: if descending {
                            SortDirection::Desc
                        } else {
                            SortDirection::Asc
                        },
                    })
                    .collect(),
                input: Box::new(LogicalPlan::Project {
                    items: ["rank", "id"]
                        .into_iter()
                        .map(|name| Projection {
                            name: name.into(),
                            expression: ProjectionExpression::Property {
                                variable: "n".into(),
                                property: name.into(),
                            },
                        })
                        .collect(),
                    input: Box::new(LogicalPlan::Filter {
                        predicate: Predicate::And(vec![
                            Predicate::PropertyEq {
                                variable: "n".into(),
                                property: "kind".into(),
                                value: Value::String("note".into()),
                            },
                            Predicate::Or(vec![
                                compare("rank", rank.clone()),
                                Predicate::And(vec![
                                    Predicate::PropertyEq {
                                        variable: "n".into(),
                                        property: "rank".into(),
                                        value: rank,
                                    },
                                    compare("id", Value::String("after-id".into())),
                                ]),
                            ]),
                        ]),
                        input: Box::new(LogicalPlan::NodeScan {
                            variable: "n".into(),
                            label: "Item".into(),
                        }),
                    }),
                }),
            }),
        }
    }

    #[test]
    fn bounded_ordered_keyset_proof_matches_the_exact_native_cursor() {
        for descending in [false, true] {
            for reversed in [false, true] {
                let eq = |property: &str, value| Predicate::PropertyEq {
                    variable: "n".into(),
                    property: property.into(),
                    value,
                };
                let compare = |property: &str, value| Predicate::PropertyCompare {
                    variable: "n".into(),
                    property: property.into(),
                    op: if descending {
                        ComparisonOp::Lt
                    } else {
                        ComparisonOp::Gt
                    },
                    value,
                };
                let mut tie = vec![
                    eq("rank", Value::Int(12)),
                    compare("id", Value::String("after-id".into())),
                ];
                if reversed {
                    tie.reverse();
                }
                let mut branches = vec![compare("rank", Value::Int(12)), Predicate::And(tie)];
                if reversed {
                    branches.reverse();
                }
                let plan = PhysicalPlan::NodeProjectionScanExec {
                    variable: "n".into(),
                    label: "Item".into(),
                    access: NodeProjectionAccess::CompositeOrdered {
                        seek: CompositeOrderedSeek {
                            index_properties: vec!["kind".into(), "rank".into(), "id".into()],
                            equality_prefix: vec![("kind".into(), Value::String("note".into()))],
                            after: Some(vec![Value::Int(12), Value::String("after-id".into())]),
                            descending,
                        },
                    },
                    required_properties: vec!["kind".into(), "rank".into(), "id".into()],
                    predicate: Some(Predicate::And(vec![
                        eq("kind", Value::String("note".into())),
                        Predicate::Or(branches),
                    ])),
                    items: vec![],
                };
                assert!(prefix_consumption_is_proven(&plan));
                for mismatch in 0..4 {
                    let mut rejected = plan.clone();
                    let PhysicalPlan::NodeProjectionScanExec {
                        access: NodeProjectionAccess::CompositeOrdered { seek },
                        ..
                    } = &mut rejected
                    else {
                        unreachable!()
                    };
                    match mismatch {
                        0 => seek.after = None,
                        1 => {
                            seek.after =
                                Some(vec![Value::Int(13), Value::String("after-id".into())])
                        }
                        2 => {
                            seek.after =
                                Some(vec![Value::Int(12), Value::String("other-id".into())])
                        }
                        3 => seek.descending = !descending,
                        _ => unreachable!(),
                    }
                    assert!(!prefix_consumption_is_proven(&rejected));
                }
                let mut rejecting = plan.clone();
                let PhysicalPlan::NodeProjectionScanExec { predicate, .. } = &mut rejecting else {
                    unreachable!()
                };
                *predicate = Some(Predicate::And(vec![
                    predicate.take().unwrap(),
                    eq("visible", Value::Bool(true)),
                ]));
                assert!(!prefix_consumption_is_proven(&rejecting));
            }
        }
    }

    #[test]
    fn ordered_composite_requires_complete_capability_and_comparator_in_both_lowerings() {
        for max_groups in [1, 1000] {
            for descending in [false, true] {
                for proven in [false, true] {
                    for rank in [Value::Int(12), Value::Float(12.5)] {
                        let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups });
                        let (plan, _) = optimizer.optimize_with_catalog(
                            &logical(descending, rank.clone()),
                            &catalog(proven),
                        );
                        let explain = plan.explain(0);
                        let ordered = proven && matches!(rank, Value::Int(_));
                        assert_eq!(
                            explain.contains("IndexNodeCompositeOrderedSeek"),
                            ordered,
                            "{explain}"
                        );
                        assert_eq!(
                            !explain.contains("SortExec") && !explain.contains("TopNExec"),
                            ordered,
                            "{explain}"
                        );
                        if ordered {
                            assert!(explain.contains("after: Some"), "{explain}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn ordered_composite_rejects_mixed_directions_and_ambiguous_aliases() {
        let mut plan = logical(false, Value::Int(12));
        let LogicalPlan::Limit { input, .. } = &mut plan else {
            unreachable!()
        };
        let LogicalPlan::Sort { items, .. } = input.as_mut() else {
            unreachable!()
        };
        items[1].direction = SortDirection::Desc;
        let optimizer = CascadesOptimizer::default();
        let (physical, _) = optimizer.optimize_with_catalog(&plan, &catalog(true));
        assert!(!physical
            .explain(0)
            .contains("IndexNodeCompositeOrderedSeek"));
        let mut plan = logical(false, Value::Int(12));
        let LogicalPlan::Limit { input, .. } = &mut plan else {
            unreachable!()
        };
        let LogicalPlan::Sort { input, .. } = input.as_mut() else {
            unreachable!()
        };
        let LogicalPlan::Project { items, .. } = input.as_mut() else {
            unreachable!()
        };
        items[1].name = "rank".into();
        let (physical, _) = optimizer.optimize_with_catalog(&plan, &catalog(true));
        assert!(!physical
            .explain(0)
            .contains("IndexNodeCompositeOrderedSeek"));
    }
}
