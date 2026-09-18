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

use super::cardinality::estimate_full_text_rows;
use super::costing::{
    estimate_node_full_scan_cost, estimate_node_index_seek_cost, node_index_seek_is_cheaper,
    NODE_INDEX_EQ_STARTUP_COST, NODE_INDEX_RANGE_STARTUP_COST, NODE_INDEX_TEXT_STARTUP_COST,
};
use super::stages::ACCESS_PATH_SELECTION_STAGE;
use super::value_range::{
    merge_lower_bound, merge_upper_bound, range_bounds_for_comparison, ValueRangeBounds,
};
use super::{OptimizerCatalog, PhysicalPlan};
use crate::{OptimizerRule, RuleApplication, RuleId, RuleKind, RulePromise, StageTrace};
use hawdb_core::Value;
use hawdb_plan::{LogicalPlan, Predicate};
use std::collections::BTreeMap;

mod candidates;

use candidates::{exact_union_index_seek_candidate, index_seek_from_conjunction};

#[derive(Debug, Clone, PartialEq)]
enum GraphRuleExpr {
    Filter {
        predicate: Box<Predicate>,
        input: Box<LogicalPlan>,
    },
    Physical(Box<PhysicalPlan>),
}

struct NodeEqualitySeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeInSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeRangeSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeTextSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeUnionSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

pub(super) fn index_seek_from_filter(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let union_rule = NodeUnionSeekRule { catalog };
    let equality_rule = NodeEqualitySeekRule { catalog };
    let in_rule = NodeInSeekRule { catalog };
    let range_rule = NodeRangeSeekRule { catalog };
    let text_rule = NodeTextSeekRule { catalog };
    let rule: &dyn OptimizerRule<GraphRuleExpr> = match (predicate, input) {
        (Predicate::Or(_), LogicalPlan::NodeScan { .. }) => &union_rule,
        (
            Predicate::And(predicates),
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) => {
            return index_seek_from_conjunction(
                predicates,
                predicate,
                scan_variable,
                label,
                catalog,
                decisions,
                stage_events,
            );
        }
        (
            Predicate::PropertyEq { variable, .. },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                ..
            },
        ) if variable == scan_variable => &equality_rule,
        (
            Predicate::PropertyIn { variable, .. },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                ..
            },
        ) if variable == scan_variable => &in_rule,
        (
            Predicate::PropertyCompare { variable, .. },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                ..
            },
        ) if variable == scan_variable => &range_rule,
        (
            Predicate::PropertyContains { variable, .. },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                ..
            },
        ) if variable == scan_variable => &text_rule,
        _ => return None,
    };
    let plan = index_seek_from_rule(predicate, input, rule, decisions, stage_events);
    if plan.is_none() {
        decisions.extend(scan_decision(predicate, input, catalog));
    }
    plan
}

fn scan_decision(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
) -> Option<String> {
    let LogicalPlan::NodeScan { label, .. } = input else {
        return None;
    };
    let (property, index_kind, descriptor) = match predicate {
        Predicate::PropertyEq { property, .. } | Predicate::PropertyIn { property, .. } => (
            property,
            "equality",
            catalog.has_property_index(label, property),
        ),
        Predicate::PropertyCompare { property, .. } => (
            property,
            "range",
            catalog.has_range_property_index(label, property),
        ),
        Predicate::PropertyContains { property, .. } => (
            property,
            "fulltext",
            catalog.has_full_text_property_index(label, property),
        ),
        _ => return None,
    };
    if !descriptor {
        return Some(format!(
            "choose SeqNodeScan for {label}.{property}: no {index_kind} index descriptor"
        ));
    }
    let label_count = catalog.label_count(label);
    let scan_cost = estimate_node_full_scan_cost(label_count);
    let (estimated_rows, startup_cost, suffix) = match predicate {
        Predicate::PropertyEq { .. } => (
            catalog.estimate_property_index_eq_rows(label, property),
            NODE_INDEX_EQ_STARTUP_COST,
            format!(
                "distinct_count={}",
                catalog.distinct_count(label, property).max(1)
            ),
        ),
        Predicate::PropertyIn { values, .. } => (
            catalog.estimate_property_index_in_rows(label, property, values.len() as u64),
            values.len() as u64,
            format!(
                "distinct_count={} value_count={}",
                catalog.distinct_count(label, property).max(1),
                values.len()
            ),
        ),
        Predicate::PropertyCompare { op, value, .. } => (
            catalog.estimate_range_rows(label, property, *op, value),
            NODE_INDEX_RANGE_STARTUP_COST,
            String::new(),
        ),
        Predicate::PropertyContains { .. } => (
            estimate_full_text_rows(label_count),
            NODE_INDEX_TEXT_STARTUP_COST,
            String::new(),
        ),
        _ => return None,
    };
    let seek_cost = estimate_node_index_seek_cost(estimated_rows, startup_cost);
    let suffix = if suffix.is_empty() {
        format!("estimated_rows={estimated_rows}")
    } else {
        suffix
    };
    Some(format!(
        "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} {suffix}"
    ))
}

impl OptimizerRule<GraphRuleExpr> for NodeUnionSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_exact_index_union_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::Or(predicates) = predicate.as_ref() else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan { variable, label } = input.as_ref() else {
            return RulePromise::NEVER;
        };
        if exact_union_index_seek_candidate(predicates, predicate, variable, label, self.catalog)
            .is_some()
        {
            RulePromise::new(110)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::Or(predicates) = predicate.as_ref() else {
            return None;
        };
        let LogicalPlan::NodeScan { variable, label } = input.as_ref() else {
            return None;
        };
        exact_union_index_seek_candidate(predicates, predicate, variable, label, self.catalog).map(
            |(plan, decision)| {
                RuleApplication::new(GraphRuleExpr::Physical(Box::new(plan)), decision)
            },
        )
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeTextSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_text_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyContains {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = estimate_full_text_rows(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(85)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyContains {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = estimate_full_text_rows(label_count);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::FilterExec {
                predicate: predicate.as_ref().clone(),
                input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    query: value.clone(),
                }),
            })),
            format!(
                "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeRangeSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_range_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(90)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeRangeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                lower,
                upper,
            })),
            format!(
                "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeInSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_in_index_multi_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows =
            self.catalog
                .estimate_property_index_in_rows(label, property, values.len() as u64);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(95)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows =
            self.catalog
                .estimate_property_index_in_rows(label, property, values.len() as u64);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeMultiSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                values: values.clone(),
            })),
            format!(
                "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeEqualitySeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_equality_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyEq {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_property_index_eq_rows(label, property);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(100)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows = self
            .catalog
            .estimate_property_index_eq_rows(label, property);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                value: value.clone(),
            })),
            format!(
                "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            ),
        ))
    }
}

fn index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    rule: &dyn OptimizerRule<GraphRuleExpr>,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    physical_plan_from_rule_batch(&expression, &[rule], decisions, stage_events)
}

fn physical_plan_from_rule_batch(
    expression: &GraphRuleExpr,
    rules: &[&dyn OptimizerRule<GraphRuleExpr>],
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let batch = ACCESS_PATH_SELECTION_STAGE.execute_rule_batch(expression, rules);
    let (expressions, events, trace) = batch.into_parts();
    decisions.extend(events.into_iter().map(|event| event.into_decision()));
    stage_events.push(trace);
    expressions.into_iter().find_map(|applied| {
        let application = applied.into_application();
        match application.into_expression() {
            GraphRuleExpr::Physical(plan) => Some(*plan),
            GraphRuleExpr::Filter { .. } => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};

    fn indexed_catalog() -> OptimizerCatalog {
        OptimizerCatalog::new(
            OptimizerCatalogIndexes::new(
                [
                    ("Memory".to_string(), "id".to_string()),
                    ("Memory".to_string(), "kind".to_string()),
                ],
                [],
                [("Memory".to_string(), "score".to_string())],
                [("Memory".to_string(), "title".to_string())],
            ),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 100)],
                [],
                [],
                [],
                [],
                [
                    (("Memory".to_string(), "id".to_string()), 100),
                    (("Memory".to_string(), "kind".to_string()), 10),
                ],
                [(
                    ("Memory".to_string(), "score".to_string()),
                    (0..100).map(Value::Int).collect(),
                )],
            ),
        )
    }

    fn assert_single_seek_decision(predicate: Predicate, operator: &str) {
        let input = LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        };
        let mut decisions = Vec::new();
        let mut stage_events = Vec::new();

        let plan = index_seek_from_filter(
            &predicate,
            &input,
            &indexed_catalog(),
            &mut decisions,
            &mut stage_events,
        )
        .unwrap_or_else(|| panic!("{operator} should be selected: {decisions:?}"));

        assert!(plan.explain(0).contains(operator));
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| decision.contains(&format!("choose {operator}")))
                .count(),
            1,
            "unexpected decisions: {decisions:?}"
        );
        assert_eq!(stage_events.len(), 1);
    }

    #[test]
    fn single_predicate_seek_rules_emit_one_choice_decision() {
        assert_single_seek_decision(
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            },
            "IndexNodeSeek",
        );
        assert_single_seek_decision(
            Predicate::PropertyIn {
                variable: "m".to_string(),
                property: "kind".to_string(),
                values: vec![Value::String("note".to_string())],
            },
            "IndexNodeMultiSeek",
        );
        assert_single_seek_decision(
            Predicate::PropertyCompare {
                variable: "m".to_string(),
                property: "score".to_string(),
                op: hawdb_plan::ComparisonOp::Gt,
                value: Value::Int(98),
            },
            "IndexNodeRangeSeek",
        );
        assert_single_seek_decision(
            Predicate::PropertyContains {
                variable: "m".to_string(),
                property: "title".to_string(),
                value: "graph".to_string(),
            },
            "IndexNodeTextSeek",
        );
        assert_single_seek_decision(
            Predicate::Or(vec![
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(1),
                },
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "kind".to_string(),
                    value: Value::String("note".to_string()),
                },
            ]),
            "IndexNodeUnionSeek",
        );
    }
}
