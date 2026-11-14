// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Execution strategy of a correlated `GraphMatchExec` (#1485).
//!
//! A MATCH whose only import is a node `key`, such as
//! `WITH DISTINCT friend MATCH (friend)-[:WORK_AT]->(c)-[:IS_LOCATED_IN]->
//! (:Place {name: $name})`, repeats the same walk for every input row. When
//! one run of the pattern with `key` unbound, started from its cheapest node
//! (here the `Place.name` seek), plus a hash insert per match and a probe per
//! input row, is estimated at most half the per-row nested loop, lowering
//! marks the program as a hash join on `key` (see
//! [`hawdb_plan_cypher::GraphMatchHashJoin`]). Unknown estimates keep the
//! per-row program.
//!
//! The build evaluates a text-order prefix of WHERE conjuncts that cannot
//! fail and read only the pattern's variables, placed as usual. Every other
//! conjunct runs on each joined row, in order: a build match that joins no
//! input row is never filtered by a conjunct that could raise an error.
//!
//! Independently, a conjunct `node IN column` over a node this MATCH binds
//! is marked for the executor's id-set check, a hash semi-join that builds
//! the list's id set once per input row and rejects a candidate before its
//! record is read.

use super::match_order::{self, StartCosting};
use super::match_predicates;
use super::*;
use crate::{OptionalOptimizerRule, RuleEvent};
use hawdb_plan_cypher::{
    membership_conjunct, predicate_cannot_fail, GraphMatchHashJoin, GraphMatchProgram,
};

/// The program `GraphMatchExec` runs over `input`: a hash join on its
/// imported node when clearly cheaper, else the anchored per-row program.
pub(super) fn lower_graph_match_program(
    program: &GraphMatchProgram,
    input: Option<&PhysicalPlan>,
    catalog: &OptimizerCatalog,
    optimizer_context: &OptimizerContext,
    decisions: &mut Vec<String>,
) -> GraphMatchProgram {
    let labels = input.map(bound_node_labels).unwrap_or_default();
    let options = optimizer_context.qualification_options();
    let costing = StartCosting {
        catalog,
        options,
        labels: Some(&labels),
    };
    let joined = input.and_then(|input| hash_join_program(program, input, costing, decisions));
    let binder = program;
    let program = joined.unwrap_or_else(|| {
        let mut program = placed_graph_match_program(
            anchored_graph_match_program(program, costing, decisions),
            decisions,
        );
        program.hash_join = None;
        program
    });
    let mut program = plan_id_sets(program, options, decisions);
    if input.is_none() {
        program.seeded_union =
            super::seeded_union::seeded_union_branches(binder, &program, costing, decisions);
    }
    program
}

fn hash_join_program(
    program: &GraphMatchProgram,
    input: &PhysicalPlan,
    costing: StartCosting<'_>,
    decisions: &mut Vec<String>,
) -> Option<GraphMatchProgram> {
    let estimate = match_order::meeting_join_estimate(program, costing)?;
    let key = program.imports[0].variable.clone();
    // An Expand chain split at its meeting node already chose the hash join
    // with the chain's own row estimate (see `chain_start`).
    if program
        .hash_join
        .as_ref()
        .is_some_and(|join| join.key == key)
    {
        decisions.push(format!(
            "GraphMatchExec runs as the hash join chosen when its Expand chain was split at {key}"
        ));
        return Some(build_hash_join(program, estimate, key, costing, decisions));
    }
    let input_rows = estimate_physical_plan_cost(input, costing.catalog)
        .estimated_rows
        .max(1) as f64;
    let rule = OptionalOptimizerRule::GraphMatchHashJoin;
    let detail = format!(
        "run GraphMatchExec once from {} and hash its matches on {key}: estimated work {:.1} + {:.1} build matches + {input_rows:.0} probes instead of {input_rows:.0} input rows x {:.1} per row",
        estimate.build_start,
        estimate.estimate.build,
        estimate.estimate.build_rows,
        estimate.estimate.per_row
    );
    if !estimate.estimate.hash_join_is_cheaper(input_rows) {
        decisions.push(
            RuleEvent::estimated(
                rule.as_str(),
                format!("keep the per-row GraphMatchExec; would {detail}"),
            )
            .into_decision(),
        );
        return None;
    }
    if !costing.options.rule_enabled(rule) {
        decisions.push(
            RuleEvent::skipped(
                rule.as_str(),
                format!("disabled by statement qualification: {detail}"),
            )
            .into_decision(),
        );
        return None;
    }
    decisions.push(RuleEvent::applied(rule.as_str(), detail).into_decision());
    Some(build_hash_join(program, estimate, key, costing, decisions))
}

/// `program` started where `estimate` builds it, with the build's
/// conjunct placement and the hash join on `key`.
fn build_hash_join(
    program: &GraphMatchProgram,
    estimate: match_order::MeetingJoinEstimate,
    key: String,
    costing: StartCosting<'_>,
    decisions: &mut Vec<String>,
) -> GraphMatchProgram {
    // One flat conjunct list, so the build and the per-row placements index
    // the same conjuncts.
    let mut joined = program.clone();
    if let Some(predicate) = joined.predicate.take() {
        let mut conjuncts = Vec::new();
        flatten(predicate, &mut conjuncts);
        joined.predicate = Some(if conjuncts.len() == 1 {
            conjuncts.pop().expect("one conjunct")
        } else {
            Predicate::And(conjuncts)
        });
    }
    // The order in which the per-row program evaluates the conjuncts, which
    // the conjuncts left for joined rows keep (see `GraphMatchHashJoin`).
    let mut per_row = anchored_graph_match_program(&joined, costing, &mut Vec::new());
    match_predicates::place_graph_match_predicates(&mut per_row);
    let per_row_end = per_row.steps.len();
    let per_row_step = |index: usize| {
        per_row
            .predicate_steps
            .get(index)
            .copied()
            .unwrap_or(per_row_end)
    };
    joined.steps = estimate.steps;
    // Place conjuncts for the build run, where `key` is bound by a step.
    let mut build = joined.clone();
    build.imports.clear();
    build.introduced.push(key.clone());
    if let Some(decision) = match_predicates::place_graph_match_predicates(&mut build) {
        decisions.push(format!("hash join build: {decision}"));
    }
    joined.predicate_steps = build.predicate_steps;
    joined.edge_predicates = build.edge_predicates;
    let variables = joined
        .introduced
        .iter()
        .map(String::as_str)
        .chain([key.as_str()])
        .collect::<BTreeSet<_>>();
    let conjuncts = joined.predicate_conjuncts();
    let prefix = conjuncts
        .iter()
        .take_while(|conjunct| {
            predicate_cannot_fail(conjunct)
                && match_predicates::predicate_variables(conjunct)
                    .iter()
                    .all(|name| variables.contains(name.as_str()))
        })
        .count();
    let mut residual_order = (prefix..conjuncts.len()).collect::<Vec<_>>();
    // Within a step the per-row program evaluates a relationship's own
    // conjuncts before reading its target, then the step's group in order.
    residual_order.sort_by_key(|index| {
        (
            per_row_step(*index),
            !per_row.edge_predicates.contains(index),
            *index,
        )
    });
    let residual_placed = residual_order
        .iter()
        .filter(|index| per_row_step(**index) < per_row_end)
        .count();
    let probe_conjuncts = residual_order
        .iter()
        .take_while(|index| {
            per_row_step(**index) == 0
                && !match_predicates::predicate_variables(&conjuncts[**index])
                    .iter()
                    .any(|name| variables.contains(name.as_str()))
        })
        .count();
    let end = joined.steps.len();
    if !joined.predicate_steps.is_empty() {
        for step in &mut joined.predicate_steps[prefix..] {
            *step = end;
        }
        joined.edge_predicates.retain(|index| *index < prefix);
    }
    joined.hash_join = Some(GraphMatchHashJoin {
        key,
        build_conjuncts: (0..prefix).collect(),
        residual_order,
        residual_placed,
        probe_conjuncts,
    });
    joined
}

fn flatten(predicate: Predicate, output: &mut Vec<Predicate>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                flatten(child, output);
            }
        }
        predicate => output.push(predicate),
    }
}

/// Marks `node IN column` conjuncts over nodes this MATCH binds.
fn plan_id_sets(
    mut program: GraphMatchProgram,
    options: crate::OptimizerQualificationOptions,
    decisions: &mut Vec<String>,
) -> GraphMatchProgram {
    let candidates = program
        .predicate_conjuncts()
        .iter()
        .enumerate()
        .filter_map(|(index, conjunct)| {
            let (variable, column) = membership_conjunct(conjunct)?;
            let binds = |name: &str| {
                program.steps.iter().any(|step| match step {
                    hawdb_plan_cypher::GraphMatchStep::Node(node) => node.variable == name,
                    hawdb_plan_cypher::GraphMatchStep::Expand { target, .. } => {
                        target.variable == name
                    }
                    hawdb_plan_cypher::GraphMatchStep::ShortestPath(_) => false,
                })
            };
            let own = |name: &str| {
                program.introduced.iter().any(|own| own == name)
                    || program.imports.iter().any(|import| import.variable == name)
            };
            (binds(variable)
                && program.introduced.iter().any(|own| own == variable)
                && !own(column))
            .then(|| (index, format!("{variable} IN {column}")))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return program;
    }
    let rule = OptionalOptimizerRule::GraphMatchIdSetSemiJoin;
    let detail = format!(
        "check [{}] against each input row's list id set before reading the node",
        candidates
            .iter()
            .map(|(_, text)| text.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if options.rule_enabled(rule) {
        decisions.push(RuleEvent::applied(rule.as_str(), detail).into_decision());
        program.id_set_conjuncts = candidates.into_iter().map(|(index, _)| index).collect();
    } else {
        decisions.push(
            RuleEvent::skipped(
                rule.as_str(),
                format!("disabled by statement qualification: {detail}"),
            )
            .into_decision(),
        );
    }
    program
}

/// Labels of the nodes `plan` binds, for variables that keep their node
/// through every operator above the one that bound it.
pub(super) fn bound_node_labels(plan: &PhysicalPlan) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    collect_labels(plan, &mut labels);
    labels.retain(|_, label| !label.is_empty() && !label.contains(':'));
    labels
}

fn collect_labels(plan: &PhysicalPlan, labels: &mut BTreeMap<String, String>) {
    match plan {
        PhysicalPlan::SeqNodeScan { variable, label }
        | PhysicalPlan::IndexNodeSeek {
            variable, label, ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            variable, label, ..
        }
        | PhysicalPlan::NodeProjectionScanExec {
            variable, label, ..
        } => {
            labels.insert(variable.clone(), label.clone());
        }
        // An optional expansion binds NULL or a node with this label.
        PhysicalPlan::AdjacencyExpandExec {
            target_variable,
            target_label,
            input,
            ..
        } => {
            collect_labels(input, labels);
            labels.insert(target_variable.clone(), target_label.clone());
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::LimitExec { input, .. }
        | PhysicalPlan::SortExec { input, .. } => collect_labels(input, labels),
        PhysicalPlan::ProjectExec { items, input } => {
            let mut inner = BTreeMap::new();
            collect_labels(input, &mut inner);
            for item in items {
                if let hawdb_plan_cypher::ProjectionExpression::Variable { variable } =
                    &item.expression
                    && let Some(label) = inner.get(variable)
                {
                    labels.insert(item.name.clone(), label.clone());
                }
            }
        }
        _ => {}
    }
}
