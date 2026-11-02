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

//! Places each top-level WHERE conjunct of a `GraphMatchExec` right after the
//! step that binds the last variable it reads (#1424).
//!
//! The executor runs the steps as a per-row nested loop. A conjunct that only
//! reads early variables can then prune a partial candidate before the later
//! steps expand it. The WHERE of a MATCH is a conjunction that keeps a
//! candidate only when every conjunct is true, so dropping a candidate as soon
//! as one conjunct is false or NULL removes exactly the complete matches the
//! final evaluation would have removed. OPTIONAL MATCH still pads a row only
//! when no complete match survives.
//!
//! The placement reads the final step order, so it must run after any step
//! reordering at lowering.

use hawdb_plan_cypher::{GraphMatchProgram, GraphMatchStep, Predicate, ProjectionExpression};
use std::collections::{BTreeMap, BTreeSet};

/// Splits `program.predicate` into conjuncts and records where each one runs.
/// Returns the decision text when at least one conjunct runs before the end.
pub(super) fn place_graph_match_predicates(program: &mut GraphMatchProgram) -> Option<String> {
    program.predicate_steps.clear();
    let end = program.steps.len();
    let steps = {
        let mut conjuncts = Vec::new();
        conjunction_parts(program.predicate.as_ref()?, &mut conjuncts);
        let bound_after = bound_after_steps(program);
        conjuncts
            .into_iter()
            .map(|conjunct| conjunct_step(conjunct, &bound_after, end))
            .collect::<Vec<_>>()
    };
    let placed = steps.iter().filter(|step| **step < end).count();
    if placed == 0 {
        // Keep the bound predicate unchanged when everything runs at the end.
        return None;
    }
    let mut conjuncts = Vec::with_capacity(steps.len());
    flatten_conjunction(program.predicate.take()?, &mut conjuncts);
    program.predicate = Some(if conjuncts.len() == 1 {
        conjuncts.pop()?
    } else {
        Predicate::And(conjuncts)
    });
    let decision = format!(
        "evaluate {placed} of {} GraphMatchExec WHERE conjunct(s) before the last step: after steps {steps:?} of {end}",
        steps.len()
    );
    program.predicate_steps = steps;
    Some(decision)
}

fn conjunction_parts<'a>(predicate: &'a Predicate, output: &mut Vec<&'a Predicate>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                conjunction_parts(child, output);
            }
        }
        predicate => output.push(predicate),
    }
}

/// Nested conjunctions evaluate in the same order and short-circuit at the
/// same conjunct when flattened.
fn flatten_conjunction(predicate: Predicate, output: &mut Vec<Predicate>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                flatten_conjunction(child, output);
            }
        }
        predicate => output.push(predicate),
    }
}

/// For every variable this program introduces, the number of steps after
/// which it is bound. Imports and variables bound before the clause are
/// available before the first step and are absent from the map.
fn bound_after_steps(program: &GraphMatchProgram) -> BTreeMap<&str, usize> {
    let imported = program
        .imports
        .iter()
        .map(|import| import.variable.as_str())
        .collect::<BTreeSet<_>>();
    let introduced = program
        .introduced
        .iter()
        .map(String::as_str)
        .filter(|name| !imported.contains(name))
        .collect::<BTreeSet<_>>();
    let mut bound = BTreeMap::new();
    for (index, step) in program.steps.iter().enumerate() {
        let names: Vec<&str> = match step {
            GraphMatchStep::Node(node) => vec![&node.variable],
            // A variable-length step binds its target only when a whole trail
            // is found; it cannot bind a relationship variable.
            GraphMatchStep::Expand {
                relationship,
                target,
                ..
            } => relationship
                .iter()
                .map(String::as_str)
                .chain([target.variable.as_str()])
                .collect(),
            GraphMatchStep::ShortestPath(path) => vec![&path.path],
        };
        for name in names {
            if introduced.contains(name) {
                bound.entry(name).or_insert(index + 1);
            }
        }
    }
    // An introduced name that no step binds stays unknown: conjuncts that
    // read it keep their end-of-match position.
    for name in introduced {
        bound.entry(name).or_insert(usize::MAX);
    }
    bound
}

fn conjunct_step(conjunct: &Predicate, bound_after: &BTreeMap<&str, usize>, end: usize) -> usize {
    let mut names = BTreeSet::new();
    predicate_names(conjunct, &mut names);
    names
        .iter()
        .map(|name| bound_after.get(name.as_str()).copied().unwrap_or(0))
        .max()
        .unwrap_or(0)
        .min(end)
}

/// Every row variable or column a predicate reads. The predicate IR has no
/// aggregates, writes or nondeterministic functions, so its variables decide
/// alone whether it can run early. The matches are exhaustive so a new
/// construct must state what it reads.
fn predicate_names(predicate: &Predicate, names: &mut BTreeSet<String>) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                predicate_names(child, names);
            }
        }
        Predicate::Not(child) => predicate_names(child, names),
        Predicate::ConstantBool(_) => {}
        Predicate::RelationshipExists { variable, .. }
        | Predicate::IdEq { variable, .. }
        | Predicate::IdNotEq { variable, .. }
        | Predicate::IdCompare { variable, .. }
        | Predicate::IdIn { variable, .. }
        | Predicate::PropertyEq { variable, .. }
        | Predicate::PropertyNotEq { variable, .. }
        | Predicate::PropertyCompare { variable, .. }
        | Predicate::PropertyListContains { variable, .. }
        | Predicate::PropertyListContainsLower { variable, .. }
        | Predicate::PropertyContains { variable, .. }
        | Predicate::PropertyStartsWith { variable, .. }
        | Predicate::PropertyEndsWith { variable, .. }
        | Predicate::PropertyRegexMatch { variable, .. }
        | Predicate::PropertyIsNull { variable, .. }
        | Predicate::PropertyIsNotNull { variable, .. }
        | Predicate::PropertyIn { variable, .. } => {
            names.insert(variable.clone());
        }
        Predicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            names.insert(source_variable.clone());
            names.insert(target_variable.clone());
        }
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            expression_names(expression, names);
            expression_names(value, names);
        }
        Predicate::ExpressionIn { expression, .. } => expression_names(expression, names),
    }
}

fn expression_names(expression: &ProjectionExpression, names: &mut BTreeSet<String>) {
    match expression {
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::Id { variable }
        | ProjectionExpression::RelationshipType { variable }
        | ProjectionExpression::DatePart { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { variable, .. }
        | ProjectionExpression::CasePropertyEqualsRank { variable, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { variable, .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            names.insert(variable.clone());
        }
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            names.insert(rank.variable.clone());
        }
        // Columns and path values live in the same row namespace as graph
        // variables; a path column is bound by its shortest-path step.
        ProjectionExpression::Column(column)
        | ProjectionExpression::ColumnProperty { column, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
        | ProjectionExpression::Path { column, .. } => {
            names.insert(column.clone());
        }
        ProjectionExpression::CaseColumnSearchRank(rank) => {
            names.insert(rank.column.clone());
        }
        // A comprehension's own element reads no row column.
        ProjectionExpression::LocalVariable { .. } | ProjectionExpression::Literal(_) => {}
        ProjectionExpression::Coalesce(_)
        | ProjectionExpression::VectorDistance { .. }
        | ProjectionExpression::JsonValue { .. }
        | ProjectionExpression::Left { .. }
        | ProjectionExpression::Lower(_)
        | ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. }
        | ProjectionExpression::List(_)
        | ProjectionExpression::ListFunction { .. }
        | ProjectionExpression::ListComprehension(_)
        | ProjectionExpression::ListIndex { .. }
        | ProjectionExpression::ListSlice { .. }
        | ProjectionExpression::Conversion { .. }
        | ProjectionExpression::TemporalFunction { .. }
        | ProjectionExpression::PropertyAccess { .. }
        | ProjectionExpression::MapLiteral(_) => {
            expression.all_children(|child| {
                expression_names(child, names);
                true
            });
        }
    }
}

#[cfg(test)]
mod tests;
