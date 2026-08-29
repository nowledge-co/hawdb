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
use skein_core::Value;
use skein_plan::{LogicalPlan, Predicate};
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
    match (predicate, input) {
        (Predicate::Or(_), LogicalPlan::NodeScan { .. }) => {
            union_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
        }
        (
            Predicate::And(predicates),
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) => index_seek_from_conjunction(
            predicates,
            predicate,
            scan_variable,
            label,
            catalog,
            decisions,
            stage_events,
        ),
        (
            Predicate::PropertyEq {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                equality_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let estimated_rows = catalog.estimate_property_index_eq_rows(label, property);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                Some(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    value: value.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                None
            }
        }
        (
            Predicate::PropertyIn {
                variable,
                property,
                values,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                in_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let estimated_rows =
                catalog.estimate_property_index_in_rows(label, property, values.len() as u64);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                Some(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    values: values.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                None
            }
        }
        (
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                range_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_range_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no range index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = catalog.estimate_range_rows(label, property, *op, value);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
                decisions.push(format!(
                    "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::IndexNodeRangeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    lower,
                    upper,
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        (
            Predicate::PropertyContains {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                text_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_full_text_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no fulltext index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = label_count.div_ceil(4).max(1);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                        variable: variable.clone(),
                        label: label.clone(),
                        property: property.clone(),
                        query: value.clone(),
                    }),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        _ => None,
    }
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
        let estimated_rows = label_count.div_ceil(4).max(1);
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
        let estimated_rows = label_count.div_ceil(4).max(1);
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

fn text_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeTextSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn union_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeUnionSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn range_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeRangeSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn in_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeInSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn equality_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeEqualitySeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
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
        decisions.push(application.detail().to_string());
        match application.into_expression() {
            GraphRuleExpr::Physical(plan) => Some(*plan),
            GraphRuleExpr::Filter { .. } => None,
        }
    })
}
