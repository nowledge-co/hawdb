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

//! Node properties that `GraphMatchExec` must read.
//!
//! `GraphMatchExec` used to read every property of every node it bound, even
//! when the rest of the query only compares an id or reads one property. This
//! pass walks the finished plan from its root, collects what every operator
//! above a `GraphMatchExec` reads from each graph variable, and records in
//! [`GraphMatchProgram::projected_nodes`] the properties a node variable needs
//! when nothing above it uses the whole node.
//!
//! The analysis is conservative:
//! - A variable used as a whole value (`RETURN n`, `collect(n)`, `n = m`,
//!   `n IN list`, `WITH n`, a path) or through a column of the same name is
//!   read in full.
//! - The walk descends only through operators whose expressions it knows
//!   (projection, aggregation, filter, sort, limit, distinct and
//!   `GraphMatchExec`) from a root that returns values. Any other operator
//!   keeps its whole subtree, and every `GraphMatchExec` in it, unchanged.
//! - The properties a program's own pattern and predicate test are always
//!   included, so every occurrence of a variable sees the same record.

use hawdb_plan_cypher::{
    AggregateFunction, AggregateTarget, GraphEntityKind, GraphMatchNode, GraphMatchProgram,
    GraphMatchStep, PhysicalPlan, Predicate, ProjectionExpression, SortItem, SortKey,
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

/// What later operators read from one graph variable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Need {
    /// The whole entity value, with every property.
    Whole,
    /// Its identity and labels, plus these properties.
    Properties(BTreeSet<String>),
}

/// Reads of graph variables by the operators above a plan node. A variable
/// that is absent is not read, beyond its identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Demand(BTreeMap<String, Need>);

impl Demand {
    fn whole(&mut self, variable: &str) {
        self.0.insert(variable.to_string(), Need::Whole);
    }

    fn identity(&mut self, variable: &str) {
        self.0
            .entry(variable.to_string())
            .or_insert_with(|| Need::Properties(BTreeSet::new()));
    }

    fn property(&mut self, variable: &str, property: &str) {
        let need = self
            .0
            .entry(variable.to_string())
            .or_insert_with(|| Need::Properties(BTreeSet::new()));
        if let Need::Properties(properties) = need {
            properties.insert(property.to_string());
        }
    }

    fn merge(&mut self, other: &Demand) {
        for (variable, need) in &other.0 {
            match need {
                Need::Whole => self.whole(variable),
                Need::Properties(properties) => {
                    self.identity(variable);
                    for property in properties {
                        self.property(variable, property);
                    }
                }
            }
        }
    }

    fn projection(&self, variable: &str) -> Option<BTreeSet<String>> {
        match self.0.get(variable) {
            None => Some(BTreeSet::new()),
            Some(Need::Properties(properties)) => Some(properties.clone()),
            Some(Need::Whole) => None,
        }
    }
}

/// Fills `projected_nodes` for every `GraphMatchExec` the analysis reaches,
/// and records one decision naming them.
pub(super) fn project_graph_match_reads(
    plan: PhysicalPlan,
    decisions: &mut Vec<String>,
) -> PhysicalPlan {
    if !returns_values(&plan) {
        return plan;
    }
    let mut projected = Vec::new();
    let plan = project(plan, Demand::default(), &mut projected);
    if !projected.is_empty() {
        decisions.push(format!(
            "GraphMatchExec reads only the node properties later operators use: {}",
            projected.join(", ")
        ));
    }
    plan
}

/// The query delivers only the value columns of a projection or aggregation;
/// its graph bindings never leave the plan.
fn returns_values(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::ProjectExec { .. } | PhysicalPlan::AggregateExec { .. } => true,
        PhysicalPlan::LimitExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::FilterExec { input, .. } => returns_values(input),
        _ => false,
    }
}

fn project(plan: PhysicalPlan, mut demand: Demand, projected: &mut Vec<String>) -> PhysicalPlan {
    match plan {
        PhysicalPlan::ProjectExec { items, input } => {
            // Projection keeps its input's graph bindings, so later demand
            // passes through.
            for item in &items {
                expression(&item.expression, &mut demand);
            }
            PhysicalPlan::ProjectExec {
                items,
                input: Box::new(project(*input, demand, projected)),
            }
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            for key in &group_keys {
                expression(&key.expression, &mut demand);
            }
            for item in &items {
                aggregate(item.function, &item.target, &mut demand);
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                input: Box::new(project(*input, demand, projected)),
            }
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            self::predicate(&predicate, &mut demand);
            PhysicalPlan::FilterExec {
                predicate,
                input: Box::new(project(*input, demand, projected)),
            }
        }
        PhysicalPlan::SortExec { items, input } => {
            sort_items(&items, &mut demand);
            PhysicalPlan::SortExec {
                items,
                input: Box::new(project(*input, demand, projected)),
            }
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => {
            sort_items(&items, &mut demand);
            PhysicalPlan::TopNExec {
                items,
                offset,
                limit,
                input: Box::new(project(*input, demand, projected)),
            }
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => PhysicalPlan::LimitExec {
            offset,
            limit,
            input: Box::new(project(*input, demand, projected)),
        },
        // Equal rows bind equal ids, and so equal projected records.
        PhysicalPlan::DistinctExec { input } => PhysicalPlan::DistinctExec {
            input: Box::new(project(*input, demand, projected)),
        },
        PhysicalPlan::GraphMatchExec { program, input } => {
            let (program, input_demand) = project_program(program, &demand, projected);
            PhysicalPlan::GraphMatchExec {
                program,
                input: input.map(|input| Box::new(project(*input, input_demand, projected))),
            }
        }
        plan => plan,
    }
}

/// Fills one program's projection from the demand above it and returns the
/// demand on its input rows.
fn project_program(
    mut program: GraphMatchProgram,
    demand: &Demand,
    projected: &mut Vec<String>,
) -> (GraphMatchProgram, Demand) {
    let own = program_demand(&program);
    // The program clears introduced names and re-reads imports, so neither
    // reaches its input.
    let bound = program
        .introduced
        .iter()
        .chain(program.imports.iter().map(|import| &import.variable))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut nodes = program
        .imports
        .iter()
        .filter(|import| import.kind == GraphEntityKind::Node)
        .map(|import| import.variable.clone())
        .collect::<BTreeSet<_>>();
    for step in &program.steps {
        match step {
            GraphMatchStep::Node(node) => nodes.insert(node.variable.clone()),
            GraphMatchStep::Expand { target, .. } => nodes.insert(target.variable.clone()),
            GraphMatchStep::ShortestPath(_) => false,
        };
    }
    let mut total = demand.clone();
    total.merge(&own);
    let mut projected_nodes = BTreeMap::new();
    for variable in nodes.iter().filter(|variable| bound.contains(*variable)) {
        if let Some(properties) = total.projection(variable) {
            projected.push(format!(
                "{variable}[{}]",
                properties.iter().cloned().collect::<Vec<_>>().join(",")
            ));
            projected_nodes.insert(variable.clone(), properties);
        }
    }
    program.projected_nodes = projected_nodes;
    // A variable bound by the input keeps the demand of both the program and
    // the operators above it.
    let input = Demand(
        total
            .0
            .into_iter()
            .filter(|(variable, _)| !bound.contains(variable))
            .collect(),
    );
    (program, input)
}

/// What the program's own steps and predicate read.
fn program_demand(program: &GraphMatchProgram) -> Demand {
    fn node(node: &GraphMatchNode, demand: &mut Demand) {
        demand.identity(&node.variable);
        for property in node.properties.keys() {
            demand.property(&node.variable, property);
        }
    }
    let mut demand = Demand::default();
    for step in &program.steps {
        match step {
            GraphMatchStep::Node(pattern) => node(pattern, &mut demand),
            GraphMatchStep::Expand {
                source,
                relationship,
                target,
                ..
            } => {
                demand.identity(source);
                if let Some(relationship) = relationship {
                    demand.whole(relationship);
                }
                node(target, &mut demand);
            }
            // Endpoints are compared by id; path values are read in full.
            GraphMatchStep::ShortestPath(step) => {
                demand.identity(&step.source);
                demand.identity(&step.target);
                demand.whole(&step.path);
            }
        }
    }
    if let Some(predicate) = &program.predicate {
        self::predicate(predicate, &mut demand);
    }
    demand
}

fn sort_items(items: &[SortItem], demand: &mut Demand) {
    for item in items {
        match &item.key {
            SortKey::Property { variable, property } => demand.property(variable, property),
            SortKey::Id { variable } => demand.identity(variable),
            SortKey::Expression(expression) => self::expression(expression, demand),
            SortKey::Column(column) => demand.whole(column),
        }
    }
}

fn aggregate(function: AggregateFunction, target: &AggregateTarget, demand: &mut Demand) {
    match target {
        AggregateTarget::All => {}
        // Counting a variable tests only whether it is bound.
        AggregateTarget::Variable(variable) if function == AggregateFunction::Count => {
            demand.identity(variable)
        }
        AggregateTarget::Variable(variable) | AggregateTarget::Column(variable) => {
            demand.whole(variable)
        }
        AggregateTarget::ColumnProperty { column, .. } => demand.whole(column),
        AggregateTarget::Property { variable, property } => demand.property(variable, property),
        AggregateTarget::Ranked {
            document_id, score, ..
        } => {
            demand.whole(document_id);
            demand.whole(score);
        }
        AggregateTarget::Expression(expression) => self::expression(expression, demand),
    }
}

fn predicate(predicate: &Predicate, demand: &mut Demand) {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            for predicate in predicates {
                self::predicate(predicate, demand);
            }
        }
        Predicate::Not(predicate) => self::predicate(predicate, demand),
        Predicate::ConstantBool(_) => {}
        // Pattern predicates expand from the bound node ids.
        Predicate::RelationshipExists { variable, .. } => demand.identity(variable),
        Predicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            demand.identity(source_variable);
            demand.identity(target_variable);
        }
        Predicate::IdEq { variable, .. }
        | Predicate::IdNotEq { variable, .. }
        | Predicate::IdCompare { variable, .. }
        | Predicate::IdIn { variable, .. } => demand.identity(variable),
        Predicate::PropertyEq {
            variable, property, ..
        }
        | Predicate::PropertyNotEq {
            variable, property, ..
        }
        | Predicate::PropertyCompare {
            variable, property, ..
        }
        | Predicate::PropertyListContains {
            variable, property, ..
        }
        | Predicate::PropertyListContainsLower {
            variable, property, ..
        }
        | Predicate::PropertyContains {
            variable, property, ..
        }
        | Predicate::PropertyStartsWith {
            variable, property, ..
        }
        | Predicate::PropertyEndsWith {
            variable, property, ..
        }
        | Predicate::PropertyRegexMatch {
            variable, property, ..
        }
        | Predicate::PropertyIsNull { variable, property }
        | Predicate::PropertyIsNotNull { variable, property }
        | Predicate::PropertyIn {
            variable, property, ..
        } => demand.property(variable, property),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            self::expression(expression, demand);
            self::expression(value, demand);
        }
        Predicate::ExpressionIn { expression, .. } => self::expression(expression, demand),
    }
}

fn expression(expression: &ProjectionExpression, demand: &mut Demand) {
    match expression {
        // Whole values: a node value carries every property.
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::RelationshipType { variable } => demand.whole(variable),
        // A column may shadow a graph variable of the same name.
        ProjectionExpression::Column(column)
        | ProjectionExpression::ColumnProperty { column, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
        | ProjectionExpression::Path { column, .. } => demand.whole(column),
        ProjectionExpression::CaseColumnSearchRank(rank) => demand.whole(&rank.column),
        ProjectionExpression::Id { variable } => demand.identity(variable),
        ProjectionExpression::Property { variable, property }
        | ProjectionExpression::DatePart {
            variable, property, ..
        }
        | ProjectionExpression::DefaultIfNullOrEq {
            variable, property, ..
        }
        | ProjectionExpression::DefaultIfNull {
            variable, property, ..
        }
        | ProjectionExpression::CasePropertyNotNullOrEq {
            variable, property, ..
        }
        | ProjectionExpression::CasePropertyEqualsRank {
            variable, property, ..
        }
        | ProjectionExpression::CaseLowerPropertyDefault {
            variable, property, ..
        } => demand.property(variable, property),
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            demand.identity(variable);
            for term in terms {
                demand.property(variable, &term.property);
            }
        }
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            demand.property(&rank.variable, &rank.name_property);
            demand.property(&rank.variable, &rank.aliases_property);
        }
        ProjectionExpression::Literal(_) | ProjectionExpression::LocalVariable { .. } => {}
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
                self::expression(child, demand);
                true
            });
        }
    }
}
