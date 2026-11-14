// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A first MATCH with a disjunctive WHERE as a union of seeded branches
//! (#1625).
//!
//! LDBC IC12 starts with
//! `MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
//! WHERE tag.name = $name OR baseTagClass.name = $name`. Neither equality
//! holds for every row, so the program scans every `Tag` and expands each
//! one before the `OR` filters. When every disjunct of a top-level `OR`
//! conjunct requires an equality `v.p = value` on a different node `v` of
//! the pattern, and an equality index on `v`'s label covers `p`, each such
//! equality is a seed: every row that satisfies the WHERE satisfies at least
//! one seed. Branch `i` starts the path at seed `i`'s node with the equality
//! as its node pattern property, which the executor seeks by index, and keeps
//! the whole predicate. It also requires every earlier seed `j < i` not to
//! hold (`v_j.p_j IS NULL OR value_j IS NULL OR v_j.p_j <> value_j`, the
//! exact complement of the two-valued node pattern match), so a row that
//! satisfies several seeds comes only from the first such branch. Each match
//! of the original program is therefore produced exactly once, and a branch
//! walks the same path from another node, which keeps its multiplicity.
//!
//! The union applies only when every conjunct of the WHERE cannot fail, so
//! skipping the matches that no seed reaches skips no error, and when the
//! estimated seeds are at most half the rows the current start scans.

use super::match_order::{self, StartCosting};
use super::match_predicates;
use super::*;
use crate::{OptionalOptimizerRule, RuleEvent};
use hawdb_plan_cypher::{predicate_cannot_fail, GraphMatchProgram, GraphMatchStep};

/// The most disjuncts a WHERE may split into.
const MAX_SEED_DISJUNCTS: usize = 8;

/// One disjunct's seed: `variable.property = value`.
struct Seed<'a> {
    variable: &'a str,
    label: &'a str,
    property: &'a str,
    value: &'a Value,
    rows: u64,
}

/// The branches of `program` (the binder's program of a MATCH without an
/// input), or none when the union does not apply. `lowered` is the program
/// lowering would otherwise run, whose start the union must beat.
pub(super) fn seeded_union_branches(
    program: &GraphMatchProgram,
    lowered: &GraphMatchProgram,
    costing: StartCosting<'_>,
    decisions: &mut Vec<String>,
) -> Vec<GraphMatchProgram> {
    let catalog = costing.catalog;
    if program.optional || !program.imports.is_empty() || program.hash_join.is_some() {
        return Vec::new();
    }
    let Some(predicate) = &program.predicate else {
        return Vec::new();
    };
    let Some(seeds) = seeds(program, catalog) else {
        return Vec::new();
    };
    let rule = OptionalOptimizerRule::DisjunctiveSeedUnion;
    let seeds_text = seeds
        .iter()
        .map(|seed| format!("{}:{}.{}", seed.variable, seed.label, seed.property))
        .collect::<Vec<_>>()
        .join(", ");
    // A WHERE that may fail must fail on the matches no seed reaches.
    if !predicate_cannot_fail(predicate) {
        decisions.push(
            RuleEvent::estimated(
                rule.as_str(),
                format!("keep GraphMatchExec: seeds [{seeds_text}] but the WHERE may fail"),
            )
            .into_decision(),
        );
        return Vec::new();
    }
    let Some(GraphMatchStep::Node(start)) = lowered.steps.first() else {
        return Vec::new();
    };
    let start_rows = start_rows(start, catalog);
    let seed_rows = seeds
        .iter()
        .fold(0u64, |total, seed| total.saturating_add(seed.rows));
    let detail = format!(
        "run GraphMatchExec as a union of {} branches seeded by index on [{seeds_text}]: estimated {seed_rows} seed rows instead of {start_rows} start rows at {}",
        seeds.len(),
        start.variable
    );
    if (seed_rows as f64) * super::super::pattern_start::START_COST_MARGIN > start_rows as f64 {
        decisions.push(
            RuleEvent::estimated(
                rule.as_str(),
                format!("keep GraphMatchExec; would {detail}"),
            )
            .into_decision(),
        );
        return Vec::new();
    }
    let mut branches = Vec::with_capacity(seeds.len());
    for (index, seed) in seeds.iter().enumerate() {
        let Some(steps) = match_order::seeded_path_steps(
            &program.steps,
            seed.variable,
            seed.property,
            seed.value,
        ) else {
            return Vec::new();
        };
        let mut conjuncts = program.predicate_conjuncts().to_vec();
        conjuncts.extend(seeds[..index].iter().map(not_seeded));
        let predicate = if conjuncts.len() == 1 {
            conjuncts.pop()
        } else {
            Some(Predicate::And(conjuncts))
        };
        let mut branch = GraphMatchProgram {
            steps,
            predicate,
            predicate_steps: Vec::new(),
            edge_predicates: Vec::new(),
            ..program.clone()
        };
        match_predicates::place_graph_match_predicates(&mut branch);
        branches.push(branch);
    }
    if !costing.options.rule_enabled(rule) {
        decisions.push(
            RuleEvent::skipped(
                rule.as_str(),
                format!("disabled by statement qualification: {detail}"),
            )
            .into_decision(),
        );
        return Vec::new();
    }
    decisions.push(RuleEvent::applied(rule.as_str(), detail).into_decision());
    branches
}

/// One seed per disjunct of the first top-level `OR` conjunct that has one,
/// each on a different node. `None` when no conjunct qualifies.
fn seeds<'a>(program: &'a GraphMatchProgram, catalog: &OptimizerCatalog) -> Option<Vec<Seed<'a>>> {
    let labels = node_labels(&program.steps);
    program.predicate_conjuncts().iter().find_map(|conjunct| {
        let Predicate::Or(disjuncts) = conjunct else {
            return None;
        };
        if !(2..=MAX_SEED_DISJUNCTS).contains(&disjuncts.len()) {
            return None;
        }
        let seeds = disjuncts
            .iter()
            .map(|disjunct| disjunct_seed(disjunct, program, &labels, catalog))
            .collect::<Option<Vec<_>>>()?;
        let variables = seeds
            .iter()
            .map(|seed| seed.variable)
            .collect::<BTreeSet<_>>();
        (variables.len() == seeds.len()).then_some(seeds)
    })
}

/// The most selective indexed equality among `disjunct`'s top-level
/// conjuncts, on a node the program introduces.
fn disjunct_seed<'a>(
    disjunct: &'a Predicate,
    program: &GraphMatchProgram,
    labels: &BTreeMap<&'a str, &'a str>,
    catalog: &OptimizerCatalog,
) -> Option<Seed<'a>> {
    let conjuncts = match disjunct {
        Predicate::And(conjuncts) => conjuncts.as_slice(),
        disjunct => std::slice::from_ref(disjunct),
    };
    conjuncts
        .iter()
        .filter_map(|conjunct| {
            let Predicate::PropertyEq {
                variable,
                property,
                value,
            } = conjunct
            else {
                return None;
            };
            let label = *labels.get(variable.as_str())?;
            (*value != Value::Null
                && program.introduced.contains(variable)
                && !label.is_empty()
                && !label.contains(':')
                && catalog.has_property_index(label, property))
            .then(|| Seed {
                variable,
                label,
                property,
                value,
                rows: catalog.estimate_property_index_eq_rows(label, property),
            })
        })
        .min_by_key(|seed| seed.rows)
}

/// The label of each node variable, from the step that labels it.
fn node_labels(steps: &[GraphMatchStep]) -> BTreeMap<&str, &str> {
    let mut labels = BTreeMap::new();
    for step in steps {
        let node = match step {
            GraphMatchStep::Node(node) => node,
            GraphMatchStep::Expand { target, .. } => target,
            GraphMatchStep::ShortestPath(_) => continue,
        };
        let label = labels.entry(node.variable.as_str()).or_insert("");
        if label.is_empty() {
            *label = node.label.as_str();
        }
    }
    labels
}

/// Estimated rows the program's current start reads.
fn start_rows(start: &hawdb_plan_cypher::GraphMatchNode, catalog: &OptimizerCatalog) -> u64 {
    if start.label.is_empty() || start.label.contains(':') {
        return catalog.scan_population(&start.label);
    }
    start
        .properties
        .iter()
        .filter(|(property, value)| {
            **value != Value::Null && catalog.has_property_index(&start.label, property)
        })
        .map(|(property, _)| catalog.estimate_property_index_eq_rows(&start.label, property))
        .min()
        .unwrap_or_else(|| catalog.scan_population(&start.label))
}

/// True exactly when the node pattern match `seed.variable {property: value}`
/// fails: the property or the value is NULL, or they differ.
fn not_seeded(seed: &Seed<'_>) -> Predicate {
    Predicate::Or(vec![
        Predicate::PropertyIsNull {
            variable: seed.variable.to_string(),
            property: seed.property.to_string(),
        },
        // The value is a parameter in a cached plan and may be NULL.
        Predicate::ExpressionEq {
            expression: ProjectionExpression::IsNull {
                expression: Box::new(ProjectionExpression::Literal(seed.value.clone())),
                negated: false,
            },
            value: ProjectionExpression::Literal(Value::Bool(true)),
        },
        Predicate::PropertyNotEq {
            variable: seed.variable.to_string(),
            property: seed.property.to_string(),
            value: seed.value.clone(),
        },
    ])
}

#[cfg(test)]
mod tests;
