// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Optional semantic rewrites. Unsupported producers/evaluators are fences;
//! absence of a proof preserves the original operator and evaluation boundary.

mod facts;
use super::OptimizerCatalog;
use crate::{OptimizerQualificationOptions, OptionalOptimizerRule, RuleEvent};
use facts::{Atom, Facts, MAX_FIELDS};
use hawdb_core::Value;
use hawdb_plan_cypher::{
    AggregateFunction, AggregateTarget, LogicalPlan, Projection, ProjectionExpression, SortItem,
    SortKey,
};
use std::collections::BTreeSet;

const MAX_PROOF_NODES: usize = 2048;
const MAX_PROOF_DEPTH: usize = 128;

pub(super) fn rewrite(
    plan: LogicalPlan,
    catalog: &OptimizerCatalog,
    options: OptimizerQualificationOptions,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    let mut budget = Budget {
        remaining: MAX_PROOF_NODES,
        reported: false,
    };
    visit(plan, catalog, options, events, &mut budget, 0).0
}
struct Budget {
    remaining: usize,
    reported: bool,
}

fn applied(
    rule: OptionalOptimizerRule,
    detail: impl Into<String>,
    options: OptimizerQualificationOptions,
    events: &mut Vec<RuleEvent>,
) -> bool {
    let detail = detail.into();
    if options.rule_enabled(rule) {
        events.push(RuleEvent::applied(rule.as_str(), detail));
        true
    } else {
        events.push(RuleEvent::skipped(
            rule.as_str(),
            format!("disabled by statement qualification: {detail}"),
        ));
        false
    }
}

fn order_atom(item: &SortItem) -> Option<Atom> {
    Atom::sort_key(&item.key)
}
fn sort_scalar(
    item: &SortItem,
    facts: &Facts,
    catalog: &OptimizerCatalog,
) -> Option<facts::ScalarFact> {
    match &item.key {
        SortKey::Expression(expression) => facts.expression(expression, catalog),
        _ => order_atom(item).and_then(|atom| facts.scalar(&atom, catalog)),
    }
}

fn visit(
    plan: LogicalPlan,
    catalog: &OptimizerCatalog,
    options: OptimizerQualificationOptions,
    events: &mut Vec<RuleEvent>,
    budget: &mut Budget,
    depth: usize,
) -> (LogicalPlan, Facts) {
    if budget.remaining == 0 || depth == MAX_PROOF_DEPTH {
        if !budget.reported {
            events.push(RuleEvent::skipped(
                "transformation:proof_budget",
                "bounded proof node/depth limit; retained unsupported subtree",
            ));
            budget.reported = true;
        }
        return (plan, Facts::default());
    }
    budget.remaining -= 1;
    match plan {
        LogicalPlan::NodeScan { variable, label } => {
            let facts = Facts::scan(&variable, &label);
            (LogicalPlan::NodeScan { variable, label }, facts)
        }
        LogicalPlan::Filter { predicate, input } => {
            let (input, mut facts) = visit(*input, catalog, options, events, budget, depth + 1);
            facts.filter(&predicate, catalog);
            (
                LogicalPlan::Filter {
                    predicate,
                    input: Box::new(input),
                },
                facts,
            )
        }
        LogicalPlan::Project { items, input } => {
            let (input, facts) = visit(*input, catalog, options, events, budget, depth + 1);
            let output = facts.project(&items, catalog);
            (
                LogicalPlan::Project {
                    items,
                    input: Box::new(input),
                },
                output,
            )
        }
        LogicalPlan::Distinct { input } => {
            let (input, mut facts) = visit(*input, catalog, options, events, budget, depth + 1);
            if facts.safe
                && facts.values_are_unique()
                && applied(
                    OptionalOptimizerRule::RedundantDistinct,
                    "projected tuple contains a proved key or at most one row",
                    options,
                    events,
                )
            {
                (input, facts)
            } else {
                facts.distinct();
                (
                    LogicalPlan::Distinct {
                        input: Box::new(input),
                    },
                    facts,
                )
            }
        }
        LogicalPlan::Sort { items, input } => {
            let (input, mut facts) = visit(*input, catalog, options, events, budget, depth + 1);
            if items.len() > MAX_FIELDS {
                facts.safe = false;
                facts.ordering.clear();
                return (
                    LogicalPlan::Sort {
                        items,
                        input: Box::new(input),
                    },
                    facts,
                );
            }
            let mut keep = Vec::new();
            let mut known = Vec::new();
            let mut total = true;
            for (ordinal, item) in items.iter().enumerate() {
                let scalar = sort_scalar(item, &facts, catalog);
                total &= scalar.is_some();
                if scalar.as_ref().is_some_and(|fact| fact.constant.is_some()) {
                    continue;
                }
                if let Some(atom) = order_atom(item) {
                    if known
                        .iter()
                        .any(|(prior, _)| facts.equivalent(prior, &atom))
                    {
                        continue;
                    }
                    known.push((atom, item.direction));
                } else {
                    total = false;
                }
                keep.push(ordinal);
            }
            let supplied = known.len() <= facts.ordering.len()
                && known.iter().zip(&facts.ordering).all(
                    |((needed, direction), (actual, actual_direction))| {
                        direction == actual_direction && facts.equivalent(needed, actual)
                    },
                );
            let redundant =
                facts.max_rows.is_some_and(|max| max <= 1) || keep.is_empty() || supplied;
            let changed = redundant || keep.len() != items.len();
            let rewrite=facts.safe && total && changed && applied(OptionalOptimizerRule::RedundantOrdering,format!("constant/equivalent keys removed={} input_order_satisfies={} at_most_one={}",items.len()-keep.len(),supplied,facts.max_rows.is_some_and(|max|max<=1)),options,events);
            facts.safe &= total;
            if rewrite && redundant {
                return (input, facts);
            }
            let items = if rewrite {
                items
                    .into_iter()
                    .enumerate()
                    .filter_map(|(ordinal, item)| keep.contains(&ordinal).then_some(item))
                    .collect()
            } else {
                items
            };
            facts.ordering = items
                .iter()
                .map(|item| order_atom(item).map(|atom| (atom, item.direction)))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();
            (
                LogicalPlan::Sort {
                    items,
                    input: Box::new(input),
                },
                facts,
            )
        }
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => {
            let (input, facts) = visit(*input, catalog, options, events, budget, depth + 1);
            let bounded_shape = group_keys.len().saturating_add(items.len()) <= MAX_FIELDS;
            let count_all = bounded_shape
                && items.iter().all(|item| {
                    item.function == AggregateFunction::Count
                        && item.target == AggregateTarget::All
                        && !item.distinct
                });
            let unique_names = bounded_shape
                && group_keys
                    .iter()
                    .map(|key| key.name.as_str())
                    .chain(items.iter().map(|item| item.name.as_str()))
                    .all(|name| name.len() <= 128)
                && group_keys
                    .iter()
                    .map(|key| key.name.as_str())
                    .chain(items.iter().map(|item| item.name.as_str()))
                    .collect::<BTreeSet<_>>()
                    .len()
                    == group_keys.len() + items.len();
            // Generic projection preserves graph bindings; aggregation clears
            // them. Restrict this rewrite to an already values-only producer.
            if facts.safe
                && facts.values_only
                && !group_keys.is_empty()
                && group_keys.len() + items.len() <= MAX_FIELDS
                && unique_names
                && count_all
                && facts.grouping_is_unique(&group_keys)
                && group_keys
                    .iter()
                    .all(|key| facts.expression(&key.expression, catalog).is_some())
                && applied(
                    OptionalOptimizerRule::SingletonGroup,
                    "values-only unique grouping tuple with pure COUNT(*) results",
                    options,
                    events,
                )
            {
                let mut projections = group_keys;
                projections.extend(items.into_iter().map(|item| Projection {
                    name: item.name,
                    expression: ProjectionExpression::Literal(Value::Int(1)),
                }));
                let output = facts.project(&projections, catalog);
                return (
                    LogicalPlan::Project {
                        items: projections,
                        input: Box::new(input),
                    },
                    output,
                );
            }
            let mut output = facts.project(&group_keys, catalog);
            output.values_only = true;
            output.safe &=
                count_all && unique_names && group_keys.len() + items.len() <= MAX_FIELDS;
            output.ordering.clear();
            output.set_group_key(&group_keys);
            for item in items.iter().take(MAX_FIELDS) {
                output.result_column(&item.name, item.function == AggregateFunction::Count);
            }
            (
                LogicalPlan::Aggregate {
                    group_keys,
                    items,
                    input: Box::new(input),
                },
                output,
            )
        }
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => {
            let (input, mut facts) = visit(*input, catalog, options, events, budget, depth + 1);
            facts.max_rows = match (facts.max_rows, limit) {
                (Some(rows), Some(limit)) => Some(rows.saturating_sub(offset).min(limit)),
                (Some(rows), None) => Some(rows.saturating_sub(offset)),
                (None, limit) => limit,
            };
            (
                LogicalPlan::Limit {
                    offset,
                    limit,
                    input: Box::new(input),
                },
                facts,
            )
        }
        LogicalPlan::NodeCartesianProduct { left, right } => {
            let (left, left_facts) = visit(*left, catalog, options, events, budget, depth + 1);
            let (right, right_facts) = visit(*right, catalog, options, events, budget, depth + 1);
            (
                LogicalPlan::NodeCartesianProduct {
                    left: Box::new(left),
                    right: Box::new(right),
                },
                Facts::product(left_facts, right_facts),
            )
        }
        // OPTIONAL, graph traversal/path, mutation, opaque/external producers
        // and unsupported evaluators remain barriers. Do not rewrite through
        // them on assumptions about multiplicity, NULLs, errors or volatility.
        plan => (plan, Facts::default()),
    }
}

#[cfg(test)]
mod tests;
