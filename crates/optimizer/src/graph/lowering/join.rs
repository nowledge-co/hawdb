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
use hawdb_plan::HashJoinKey;

pub(super) fn lower_property_join(
    input: PhysicalPlan,
    predicate: &Predicate,
    decisions: &mut Vec<String>,
) -> PhysicalPlan {
    match input {
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left = Box::new(lower_property_join(*left, predicate, decisions));
            let right = Box::new(lower_property_join(*right, predicate, decisions));
            if let Some((left_key, right_key)) = equality_keys(predicate, &left, &right) {
                decisions.push(format!(
                    "choose HashJoinExec for {}.{} = {}.{}; retain residual filter",
                    left_key.variable, left_key.property, right_key.variable, right_key.property,
                ));
                PhysicalPlan::HashJoinExec {
                    left_key,
                    right_key,
                    left,
                    right,
                }
            } else {
                PhysicalPlan::NodeCartesianProductExec { left, right }
            }
        }
        PhysicalPlan::FilterExec {
            predicate: residual,
            input,
        } => PhysicalPlan::FilterExec {
            predicate: residual,
            input: Box::new(lower_property_join(*input, predicate, decisions)),
        },
        input => input,
    }
}

fn equality_keys(
    predicate: &Predicate,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) -> Option<(HashJoinKey, HashJoinKey)> {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .find_map(|p| equality_keys(p, left, right)),
        Predicate::ExpressionEq {
            expression:
                ProjectionExpression::Property {
                    variable: a,
                    property: a_key,
                },
            value:
                ProjectionExpression::Property {
                    variable: b,
                    property: b_key,
                },
        } => {
            let a_left = binds_node(left, a) && !binds_node(right, a);
            let b_right = binds_node(right, b) && !binds_node(left, b);
            let b_left = binds_node(left, b) && !binds_node(right, b);
            let a_right = binds_node(right, a) && !binds_node(left, a);
            if a_left && b_right {
                Some((
                    HashJoinKey {
                        variable: a.clone(),
                        property: a_key.clone(),
                    },
                    HashJoinKey {
                        variable: b.clone(),
                        property: b_key.clone(),
                    },
                ))
            } else if b_left && a_right {
                Some((
                    HashJoinKey {
                        variable: b.clone(),
                        property: b_key.clone(),
                    },
                    HashJoinKey {
                        variable: a.clone(),
                        property: a_key.clone(),
                    },
                ))
            } else {
                None
            }
        }
        // A conjunct must be mandatory for every result; OR and NOT do not
        // prove a join key. Computed expressions retain their existing path.
        _ => None,
    }
}

fn binds_node(plan: &PhysicalPlan, name: &str) -> bool {
    match plan {
        PhysicalPlan::SeqNodeScan { variable, .. }
        | PhysicalPlan::SourceSegmentScan { variable, .. }
        | PhysicalPlan::IndexNodeSeek { variable, .. }
        | PhysicalPlan::IndexNodeMultiSeek { variable, .. }
        | PhysicalPlan::IndexNodeUnionSeek { variable, .. }
        | PhysicalPlan::IndexNodeCompositeSeek { variable, .. }
        | PhysicalPlan::IndexNodeCompositeRangeSeek { variable, .. }
        | PhysicalPlan::IndexNodeRangeSeek { variable, .. }
        | PhysicalPlan::IndexNodeTextSeek { variable, .. } => variable == name,
        PhysicalPlan::NodeProjectionScanExec {
            variable, items, ..
        } => items.is_empty() && variable == name,
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            binds_node(left, name) || binds_node(right, name)
        }
        PhysicalPlan::FilterExec { input, .. } => binds_node(input, name),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_plan::{plan, visit_plan, PhysicalPlanKind};

    fn optimize(query: &str) -> PhysicalPlan {
        let logical = plan(&hawdb_cypher::parse(query).unwrap()).unwrap();
        let (plan, _) = CascadesOptimizer::default()
            .optimize_with_catalog(&logical, &OptimizerCatalog::default());
        plan
    }

    #[test]
    fn graph_hash_join_lowers_mandatory_oriented_properties_and_nested_products() {
        for (query, count) in [
            ("MATCH (a:Left), (b:Right) WHERE a.key = b.key RETURN a.key, b.key", 1),
            ("MATCH (a:Left), (b:Right) WHERE b.key = a.key AND a.active = true RETURN a.key, b.key LIMIT 3", 1),
        ] {
            let plan = optimize(query);
            let mut joins = 0;
            visit_plan(&plan, &mut |node| {
                assert_ne!(node.kind(), PhysicalPlanKind::NodeCartesianProductExec, "{query}");
                if node.kind() == PhysicalPlanKind::HashJoinExec {
                    joins += 1;
                    assert_eq!(node.children().len(), 2);
                }
            });
            assert_eq!(joins, count, "{}", plan.explain(0));
            assert!(plan.explain(0).contains("FilterExec"));
            assert!(plan.instance_fingerprint().contains("HashJoinExec"));
        }
    }

    #[test]
    fn graph_hash_join_keeps_nested_join_and_explicit_order_limit_plan_boundaries() {
        // The current multi-node parser accepts two inputs. Exercise the
        // existing logical IR directly for nested products and ordered limits.
        let scan = |variable: &str| LogicalPlan::NodeScan {
            variable: variable.into(),
            label: variable.into(),
        };
        let eq = |a: &str, b: &str| Predicate::ExpressionEq {
            expression: ProjectionExpression::Property {
                variable: a.into(),
                property: "key".into(),
            },
            value: ProjectionExpression::Property {
                variable: b.into(),
                property: "key".into(),
            },
        };
        let logical = LogicalPlan::Limit {
            offset: 2,
            limit: Some(3),
            input: Box::new(LogicalPlan::Sort {
                items: vec![SortItem {
                    key: SortKey::Property {
                        variable: "a".into(),
                        property: "key".into(),
                    },
                    direction: hawdb_plan::SortDirection::Asc,
                }],
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::And(vec![eq("a", "b"), eq("b", "c")]),
                    input: Box::new(LogicalPlan::NodeCartesianProduct {
                        left: Box::new(LogicalPlan::NodeCartesianProduct {
                            left: Box::new(scan("a")),
                            right: Box::new(scan("b")),
                        }),
                        right: Box::new(scan("c")),
                    }),
                }),
            }),
        };
        let (plan, _) = CascadesOptimizer::default()
            .optimize_with_catalog(&logical, &OptimizerCatalog::default());
        assert!(matches!(
            plan,
            PhysicalPlan::TopNExec {
                offset: 2,
                limit: 3,
                ..
            }
        ));
        let mut joins = 0;
        visit_plan(&plan, &mut |node| {
            assert_ne!(node.kind(), PhysicalPlanKind::NodeCartesianProductExec);
            if let PhysicalPlan::HashJoinExec { left, right, .. } = node {
                joins += 1;
                for child in [left, right] {
                    assert!(!matches!(
                        **child,
                        PhysicalPlan::LimitExec { .. } | PhysicalPlan::TopNExec { .. }
                    ));
                }
            }
        });
        assert_eq!(joins, 2);
    }

    #[test]
    fn graph_hash_join_leaves_products_without_a_mandatory_cross_input_key() {
        for query in [
            "MATCH (a:Left), (b:Right) RETURN a.key, b.key",
            "MATCH (a:Left), (b:Right) WHERE a.key > b.key RETURN a.key, b.key",
            "MATCH (a:Left), (b:Right) WHERE a.key = b.key OR a.active = true RETURN a.key, b.key",
            "MATCH (a:Left), (b:Right) WHERE a.key = a.other RETURN a.key, b.key",
        ] {
            let plan = optimize(query);
            assert!(!plan.explain(0).contains("HashJoinExec"), "{query}");
            assert!(
                plan.explain(0).contains("NodeCartesianProductExec"),
                "{query}"
            );
        }
    }

    #[test]
    fn graph_hash_join_fingerprint_covers_both_key_bindings() {
        let first = optimize("MATCH (a:Left), (b:Right) WHERE a.key = b.key RETURN a.key");
        let second = optimize("MATCH (a:Left), (b:Right) WHERE a.other = b.key RETURN a.key");
        assert_ne!(first.instance_fingerprint(), second.instance_fingerprint());
    }
}
