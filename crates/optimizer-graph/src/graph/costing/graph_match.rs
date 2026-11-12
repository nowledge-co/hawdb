// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cost the executed MATCH steps with the same statistics as path selection.

use super::*;
use crate::graph::pattern_start::{self, PathEdge, PathNode, WalkEstimate};
use hawdb_plan_cypher::{GraphMatchNode, GraphMatchProgram, GraphMatchStep};
use std::collections::BTreeSet;

pub(super) fn estimate(
    program: &GraphMatchProgram,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
    base: PlanCostBreakdown,
) -> PlanCostBreakdown {
    if !program.seeded_union.is_empty() {
        // Branches share the input, but each executes its own seek and walk.
        // Cost each actual branch, including a structural fallback when its
        // disjunctive or unbounded path has no statistical estimate.
        let mut rows = 0_u64;
        let mut work = 0_u64;
        for branch in &program.seeded_union {
            let local = single(
                branch,
                catalog,
                bindings,
                PlanCostBreakdown::new(base.estimated_rows, 0, 0, 0, 0),
            );
            rows = rows.saturating_add(local.estimated_rows);
            work = work.saturating_add(local.random_io);
        }
        return base.with_random_io(rows.max(1), work, 0);
    }
    single(program, catalog, bindings, base)
}

fn single(
    program: &GraphMatchProgram,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
    base: PlanCostBreakdown,
) -> PlanCostBreakdown {
    let Some(local) = statistical_walk(program, catalog, bindings) else {
        return structural(program, catalog, base);
    };
    let input = base.estimated_rows as f64;
    let rows = (local.rows * input).ceil() as u64;
    let rows = if program.optional {
        rows.max(base.estimated_rows)
    } else {
        rows.max(1)
    };
    base.with_random_io(rows, (local.work * input).ceil() as u64, 0)
}

fn statistical_walk(
    program: &GraphMatchProgram,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> Option<WalkEstimate> {
    // Hash builds have a different binding scope; retain their current model.
    if program.hash_join.is_some() {
        return None;
    }
    let mut nodes = BTreeMap::new();
    for step in &program.steps {
        let node = match step {
            GraphMatchStep::Node(node) | GraphMatchStep::Expand { target: node, .. } => node,
            GraphMatchStep::ShortestPath(_) => return None,
        };
        let label = if node.label.is_empty() {
            bindings.node_label(&node.variable).unwrap_or("")
        } else {
            &node.label
        };
        if nodes
            .get(node.variable.as_str())
            .is_some_and(|previous: &&str| {
                !previous.is_empty() && !label.is_empty() && *previous != label
            })
        {
            // Multiple labels on a repeated variable need an intersection
            // population; one label's path count cannot describe it.
            return None;
        }
        nodes
            .entry(node.variable.as_str())
            .and_modify(|current: &mut &str| {
                if current.is_empty() {
                    *current = label;
                }
            })
            .or_insert(label);
    }
    let mut bound = program
        .imports
        .iter()
        .map(|import| import.variable.as_str())
        .collect::<BTreeSet<_>>();
    let mut rows = 1.0;
    let mut output = 1.0;
    let mut work = 1.0;
    let filter_points = graph_match_filter_points(program);
    let mut walk_context = pattern_start::WalkContext::new(catalog);
    for (index, step) in program.steps.iter().enumerate() {
        if filter_points.contains(&index) {
            rows /= crate::cardinality_defaults::FILTER_SELECTIVITY_DIVISOR as f64;
        }
        let (node, candidates, surviving) = match step {
            GraphMatchStep::Node(node) => {
                let view = path_node(
                    node,
                    nodes[node.variable.as_str()],
                    bound.contains(node.variable.as_str()),
                );
                let estimate = pattern_start::node_work(catalog, &view).unwrap_or_else(|| {
                    let rows = catalog.label_count(view.label) as f64;
                    WalkEstimate { rows, work: rows }
                });
                (&node.variable, estimate.work, estimate.rows)
            }
            GraphMatchStep::Expand {
                source,
                target,
                rel_type,
                rel_types,
                properties,
                direction,
                min_hops,
                max_hops,
                ..
            } => {
                let from = PathNode {
                    variable: source,
                    label: nodes.get(source.as_str())?,
                    equalities: Vec::new(),
                    values: BTreeMap::new(),
                    bound: true,
                };
                let target_node = path_node(
                    target,
                    nodes[target.variable.as_str()],
                    bound.contains(target.variable.as_str()),
                );
                let edge = PathEdge {
                    rel_type: (!rel_type.is_empty() && rel_types.is_empty()).then_some(rel_type),
                    equalities: properties.keys().map(String::as_str).collect(),
                    values: properties
                        .iter()
                        .map(|(key, value)| (key.as_str(), value))
                        .collect(),
                    direction: *direction,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    reversible: true,
                };
                let expansion = pattern_start::expand_work(
                    &mut walk_context,
                    &from,
                    &target_node,
                    &edge,
                    *direction,
                )
                .unwrap_or_else(|| {
                    let rows = catalog.label_count(target_node.label) as f64;
                    WalkEstimate { rows, work: rows }
                });
                let surviving = if target_node.bound {
                    expansion.rows / catalog.label_count(target_node.label) as f64
                } else {
                    expansion.rows * pattern_start::node_selectivity(catalog, &target_node)
                };
                (&target.variable, expansion.work, surviving)
            }
            GraphMatchStep::ShortestPath(_) => return None,
        };
        // Keep fractional selectivity until the operator boundary, but cap
        // each product before another multiplication can create infinity/NaN.
        let candidates = candidates.min(u64::MAX as f64);
        let surviving = surviving.min(u64::MAX as f64);
        work = (work + rows * candidates).min(u64::MAX as f64);
        rows = (rows * surviving).min(u64::MAX as f64);
        output = (output * surviving).min(u64::MAX as f64);
        bound.insert(node.as_str());
    }
    if program.predicate.is_some() {
        output /= crate::cardinality_defaults::FILTER_SELECTIVITY_DIVISOR as f64;
    }
    Some(WalkEstimate { rows: output, work })
}

fn path_node<'a>(node: &'a GraphMatchNode, label: &'a str, bound: bool) -> PathNode<'a> {
    PathNode {
        variable: &node.variable,
        label,
        equalities: node.properties.keys().map(String::as_str).collect(),
        values: node
            .properties
            .iter()
            .map(|(key, value)| (key.as_str(), value))
            .collect(),
        bound,
    }
}

/// The step indexes before which `GraphMatchExec` evaluates WHERE conjuncts;
/// `steps.len()` stands for the end of the match.
fn graph_match_filter_points(
    program: &hawdb_plan_cypher::GraphMatchProgram,
) -> std::collections::BTreeSet<usize> {
    if program.predicate.is_none() {
        return std::collections::BTreeSet::new();
    }
    if program.predicate_steps.is_empty() {
        return std::collections::BTreeSet::from([program.steps.len()]);
    }
    program.predicate_steps.iter().copied().collect()
}

/// Existing fallback for shapes without a compatible statistical walk.
fn structural(
    program: &GraphMatchProgram,
    catalog: &OptimizerCatalog,
    base: PlanCostBreakdown,
) -> PlanCostBreakdown {
    // A hash join walks the steps once and probes once per input row.
    let mut rows = if program.hash_join.is_some() {
        1
    } else {
        base.estimated_rows
    };
    let mut output_rows = base.estimated_rows;
    let mut work = base.estimated_rows;
    // Steps run as a per-row nested loop in plan order. A step that
    // binds a fresh node multiplies the rows by its label; a step that
    // reaches an imported or already-bound node only filters them. The
    // output estimate is therefore order-independent, while the work
    // (the sum of intermediate rows) reflects the anchored order.
    let mut bound = std::collections::BTreeSet::new();
    // Each group of WHERE conjuncts keeps a default fraction of the
    // rows at the point it runs, so later steps expand fewer rows.
    let filter_points = graph_match_filter_points(program);
    let filter = |index: usize, rows: u64| {
        if filter_points.contains(&index) {
            rows.div_ceil(crate::cardinality_defaults::FILTER_SELECTIVITY_DIVISOR)
                .max(1)
        } else {
            rows
        }
    };
    for (index, step) in program.steps.iter().enumerate() {
        rows = filter(index, rows);
        let node = match step {
            hawdb_plan_cypher::GraphMatchStep::Node(node) => node,
            hawdb_plan_cypher::GraphMatchStep::Expand { target, .. } => target,
            // A search may visit every relationship of its type for
            // each endpoint pair; it keeps the row count of its input.
            hawdb_plan_cypher::GraphMatchStep::ShortestPath(step) => {
                work = work.saturating_add(
                    rows.saturating_mul(catalog.traversable_relationship_count(&step.rel_type)),
                );
                continue;
            }
        };
        let newly_bound =
            bound.insert(&node.variable) && program.introduced.contains(&node.variable);
        if newly_bound {
            let multiplier = catalog.label_count(&node.label).max(1);
            rows = rows.saturating_mul(multiplier);
            output_rows = output_rows.saturating_mul(multiplier);
        }
        work = work.saturating_add(rows);
    }
    // Placement changes intermediate work, not the selectivity of
    // the complete WHERE. Apply that output estimate only once.
    if program.predicate.is_some() {
        output_rows = output_rows
            .div_ceil(crate::cardinality_defaults::FILTER_SELECTIVITY_DIVISOR)
            .max(1);
    }
    base.with_random_io(output_rows, work, 0)
}

#[cfg(test)]
mod tests;
