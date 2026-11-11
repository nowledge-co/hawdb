// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pattern predicates and pattern comprehensions as correlated MATCH
//! programs.
//!
//! A pattern expression is planned as a [`GraphMatchProgram`] the executor
//! runs once per evaluation, over a row that holds only what the pattern and
//! its body read. Each pattern variable is classified against the enclosing
//! scope: a column (including a list comprehension or `reduce` local) is
//! imported by value, a native graph variable is copied from the enclosing
//! row, and any other variable is introduced by the pattern and visible only
//! inside it. The body reads enclosing columns as local variables, so the
//! subquery row never copies a column such as a collected list.

use super::*;
use hawdb_cypher::{MatchPattern, RelationshipDirection};
use std::cell::Cell;

/// The parts of a pattern expression in source form.
pub(super) struct PatternParts<'a> {
    pub(super) pattern: &'a MatchPattern,
    pub(super) predicate: Option<&'a ScalarExpression>,
    /// `None` for a pattern predicate.
    pub(super) projection: Option<&'a ScalarExpression>,
}

/// Where the value of a pattern variable comes from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A node or relationship already bound in the enclosing row.
    Graph,
    /// An enclosing column or local value, imported as a graph entity.
    Value,
    /// Bound by the pattern itself.
    Introduced,
}

pub(super) fn plan_pattern_expression(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    parts: PatternParts<'_>,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    let pattern = &parts.pattern.kind;
    let kind = if parts.projection.is_some() {
        "pattern comprehensions"
    } else {
        "pattern predicates"
    };
    if pattern.variable.is_some() {
        return Err(HawDBError::Semantic(format!(
            "path variables are not supported in {kind}"
        )));
    }
    if pattern.steps.is_empty() {
        return Err(HawDBError::Semantic(format!(
            "{kind} require at least one relationship"
        )));
    }
    if pattern.steps.len() + 1 > MAX_PATTERN_EXPRESSION_STEPS {
        return Err(HawDBError::Semantic(format!(
            "{kind} exceed the maximum pattern depth"
        )));
    }
    let mut taken: BTreeSet<String> = scope.union(column_scope).cloned().collect();
    // Anonymous nodes are scoped to the pattern. Give each a name that no
    // enclosing variable, column or user-written identifier can have.
    let mut nodes = std::iter::once(&pattern.first)
        .chain(pattern.steps.iter().map(|step| &step.target))
        .map(|node| {
            let mut node = node.kind.clone();
            if node.anonymous {
                node.variable = fresh_name(&mut taken);
            }
            node
        })
        .collect::<Vec<_>>();
    let mut origins = BTreeMap::<String, (Origin, GraphEntityKind)>::new();
    let mut introduced = Vec::new();
    let mut imports = Vec::new();
    let mut arguments = Vec::new();
    let mut classify = |name: &str, kind: GraphEntityKind| -> Result<Origin> {
        if let Some((origin, previous)) = origins.get(name) {
            if *previous != kind {
                return Err(HawDBError::Semantic(format!(
                    "variable '{name}' is used as both a node and a relationship in a pattern"
                )));
            }
            return Ok(*origin);
        }
        let origin = if column_scope.contains(name) {
            imports.push(GraphBindingImport {
                variable: name.to_string(),
                column: name.to_string(),
                kind,
            });
            arguments.push((
                name.to_string(),
                ProjectionExpression::Column(name.to_string()),
            ));
            Origin::Value
        } else if scope.contains(name) {
            Origin::Graph
        } else {
            introduced.push(name.to_string());
            Origin::Introduced
        };
        origins.insert(name.to_string(), (origin, kind));
        Ok(origin)
    };
    let mut bound = Vec::with_capacity(nodes.len());
    bound.push(classify(&nodes[0].variable, GraphEntityKind::Node)? != Origin::Introduced);
    for (index, step) in pattern.steps.iter().enumerate() {
        let relationship = &step.relationship;
        if relationship.search.is_shortest() {
            return Err(HawDBError::Semantic(format!(
                "shortest paths are not supported in {kind}"
            )));
        }
        super::pipeline::validate_bounded_relationship(relationship)?;
        if relationship.max_hops == hawdb_cypher::UNBOUNDED_MAX_HOPS {
            return Err(HawDBError::Semantic(format!(
                "unbounded variable-length relationships are not supported in {kind}"
            )));
        }
        if let Some(variable) = &relationship.variable {
            classify(variable, GraphEntityKind::Relationship)?;
        }
        bound.push(
            classify(&nodes[index + 1].variable, GraphEntityKind::Node)? != Origin::Introduced,
        );
    }
    // Row-dependent node properties, such as `{id: a.id}`, become conjuncts
    // of the body predicate; constant ones stay in the pattern.
    let mut conjuncts = Vec::new();
    for node in &mut nodes {
        let dynamic = node
            .properties
            .iter()
            .filter(|(_, value)| value.reads_row_binding())
            .map(|(property, value)| (property.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (property, value) in dynamic {
            node.properties.remove(&property);
            conjuncts.push(AstNode::synthetic(ScalarExpressionKind::Binary {
                left: Box::new(AstNode::synthetic(ScalarExpressionKind::Property {
                    variable: node.variable.clone(),
                    property,
                })),
                op: hawdb_cypher::ScalarBinaryOp::Eq,
                right: Box::new(row_value_expression(value)?),
            }));
        }
    }
    // Seeds: a node the pattern introduces that every disjunct of the WHERE
    // equates with an enclosing-row expression starts the seeded program
    // (#1607). The unseeded program is kept for evaluations without seeds.
    let seeds = match parts.predicate {
        Some(predicate) if bound.iter().all(|bound| !bound) => seed_plan(
            predicate,
            &nodes,
            &introduced,
            scope,
            column_scope,
            parameters,
        )
        .map(|(index, expressions)| -> Result<PatternSeeds> {
            let variable = nodes[index].variable.clone();
            let column = fresh_name(&mut taken);
            let mut seeded_bound = bound.clone();
            seeded_bound[index] = true;
            let mut seeded_imports = imports.clone();
            seeded_imports.push(GraphBindingImport {
                variable: variable.clone(),
                column: column.clone(),
                kind: GraphEntityKind::Node,
            });
            Ok(PatternSeeds {
                program: GraphMatchProgram {
                    imports: seeded_imports,
                    introduced: introduced
                        .iter()
                        .filter(|name| **name != variable)
                        .cloned()
                        .collect(),
                    projected_nodes: BTreeMap::new(),
                    projected_relationships: BTreeMap::new(),
                    steps: steps(pattern, &nodes, &seeded_bound, parameters)?,
                    predicate: None,
                    predicate_steps: Vec::new(),
                    edge_predicates: Vec::new(),
                    optional: false,
                    hash_join: None,
                    id_set_conjuncts: Vec::new(),
                    seeded_union: Vec::new(),
                },
                variable,
                column,
                expressions,
            })
        })
        .transpose()?,
        _ => None,
    };
    let program = GraphMatchProgram {
        imports,
        introduced: introduced.clone(),
        projected_nodes: BTreeMap::new(),
        projected_relationships: BTreeMap::new(),
        steps: steps(pattern, &nodes, &bound, parameters)?,
        predicate: None,
        predicate_steps: Vec::new(),
        edge_predicates: Vec::new(),
        optional: false,
        hash_join: None,
        id_set_conjuncts: Vec::new(),
        seeded_union: Vec::new(),
    };

    // The body sees the introduced variables as graph variables. Enclosing
    // columns are rebound to locals the executor lends from the outer row.
    let mut body_scope = scope.clone();
    body_scope.extend(introduced.iter().cloned());
    let plan_body = |expression: &ScalarExpression| {
        plan_scalar_expression_with_columns(&body_scope, column_scope, expression, parameters)
    };
    let predicate = conjuncts
        .into_iter()
        .chain(parts.predicate.cloned())
        .reduce(|left, right| {
            AstNode::synthetic(ScalarExpressionKind::Binary {
                left: Box::new(left),
                op: hawdb_cypher::ScalarBinaryOp::And,
                right: Box::new(right),
            })
        });
    let mut body = Vec::new();
    let mut predicate = predicate.as_ref().map(&plan_body).transpose()?;
    let mut projection = parts.projection.map(&plan_body).transpose()?;
    body.extend(predicate.iter_mut());
    body.extend(projection.iter_mut());
    // Column and map-key reads become locals the executor lends; forms that
    // read the row column itself (path functions, the column CASE forms)
    // read a copy the subquery row receives as an argument.
    let mut columns = Vec::new();
    let mut copied_columns = BTreeSet::new();
    for column in column_scope {
        let mut lent = false;
        let mut copied = false;
        for expression in &mut body {
            let (local, direct) = column_reads(expression, column);
            if local {
                lend_column(expression, column);
            }
            lent |= local;
            copied |= direct;
        }
        if lent {
            columns.push(column.clone());
        }
        if copied {
            copied_columns.insert(column.clone());
            if !arguments.iter().any(|(name, _)| name == column) {
                arguments.push((column.clone(), ProjectionExpression::Column(column.clone())));
            }
        }
    }
    // Import columns the body never reads from the subquery row reach the
    // match by identity alone (#1615).
    let identity_columns = if LEGACY_VALUE_IMPORTS.with(Cell::get) {
        Vec::new()
    } else {
        program
            .imports
            .iter()
            .map(|import| &import.column)
            .filter(|column| !copied_columns.contains(*column))
            .chain(seeds.iter().map(|seeds| &seeds.column))
            .cloned()
            .collect()
    };
    let mut read = BTreeSet::new();
    for expression in &body {
        graph_variables_read(expression, &mut read);
    }
    read.extend(
        origins
            .iter()
            .filter(|(_, (origin, _))| *origin == Origin::Graph)
            .map(|(name, _)| name.clone()),
    );
    let graph_variables = read
        .into_iter()
        .filter(|name| scope.contains(name) && !column_scope.contains(name))
        .collect();
    Ok(ProjectionExpression::Pattern(Box::new(PatternProjection {
        program,
        graph_variables,
        arguments,
        columns,
        predicate,
        projection,
        seeds,
        identity_columns,
    })))
}

thread_local! {
    static LEGACY_VALUE_IMPORTS: Cell<bool> = const { Cell::new(false) };
}

/// Plans `f` with pattern expressions that copy every argument and seed value
/// into the subquery row, as before #1615, on this thread only. Differential
/// tests compare it with the default planning; it is not a production
/// control.
#[doc(hidden)]
pub fn with_legacy_pattern_value_imports<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            LEGACY_VALUE_IMPORTS.with(|legacy| legacy.set(self.0));
        }
    }
    let _restore = Restore(LEGACY_VALUE_IMPORTS.with(|legacy| legacy.replace(true)));
    f()
}

/// The most disjuncts of a pattern WHERE that seed planning considers.
const MAX_SEED_DISJUNCTS: usize = 8;

/// The first node the pattern introduces that every disjunct of `predicate`
/// equates with an expression over the enclosing row, with those
/// expressions planned in the enclosing scope. `None` when no node
/// qualifies or an expression reads a variable the pattern introduces.
fn seed_plan(
    predicate: &ScalarExpression,
    nodes: &[hawdb_cypher::NodePatternKind],
    introduced: &[String],
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Option<(usize, Vec<ProjectionExpression>)> {
    let mut disjuncts = Vec::new();
    flatten(predicate, hawdb_cypher::ScalarBinaryOp::Or, &mut disjuncts);
    if disjuncts.len() > MAX_SEED_DISJUNCTS {
        return None;
    }
    nodes.iter().enumerate().find_map(|(index, node)| {
        let variable = &node.variable;
        if !introduced.contains(variable) {
            return None;
        }
        let mut expressions = Vec::with_capacity(disjuncts.len());
        for disjunct in &disjuncts {
            let mut conjuncts = Vec::new();
            flatten(disjunct, hawdb_cypher::ScalarBinaryOp::And, &mut conjuncts);
            let value = conjuncts
                .iter()
                .find_map(|conjunct| equated(conjunct, variable))?;
            // Planned without the pattern's variables in scope, so an
            // expression that reads one of them fails here and seeds nothing.
            let planned =
                plan_scalar_expression_with_columns(scope, column_scope, value, parameters).ok()?;
            if !expressions.contains(&planned) {
                expressions.push(planned);
            }
        }
        Some((index, expressions))
    })
}

/// The operands of a left-nested chain of `op`.
fn flatten<'a>(
    expression: &'a ScalarExpression,
    op: hawdb_cypher::ScalarBinaryOp,
    out: &mut Vec<&'a ScalarExpression>,
) {
    match &expression.kind {
        ScalarExpressionKind::Binary {
            left,
            op: binary,
            right,
        } if *binary == op => {
            flatten(left, op, out);
            flatten(right, op, out);
        }
        _ => out.push(expression),
    }
}

/// The other side of `variable = value` or `value = variable`.
fn equated<'a>(expression: &'a ScalarExpression, variable: &str) -> Option<&'a ScalarExpression> {
    let ScalarExpressionKind::Binary {
        left,
        op: hawdb_cypher::ScalarBinaryOp::Eq,
        right,
    } = &expression.kind
    else {
        return None;
    };
    match (&left.kind, &right.kind) {
        (ScalarExpressionKind::Variable(name), _) if name == variable => Some(right),
        (_, ScalarExpressionKind::Variable(name)) if name == variable => Some(left),
        _ => None,
    }
}

/// The most nodes one pattern expression may match, as for a MATCH clause.
const MAX_PATTERN_EXPRESSION_STEPS: usize = 32;

fn fresh_name(taken: &mut BTreeSet<String>) -> String {
    // `#` cannot start a Cypher identifier, so no written name collides.
    let name = (0..)
        .map(|index| format!("#anon{index}"))
        .find(|name| !taken.contains(name))
        .expect("unbounded names");
    taken.insert(name.clone());
    name
}

/// Match steps starting at the first bound node, or at the first node when
/// none is bound: the steps to its right in pattern order, then those to its
/// left with reversed directions. A pattern matches the same paths whichever
/// node it starts from, and a bound start avoids scanning every node.
fn steps(
    pattern: &hawdb_cypher::MatchPatternKind,
    nodes: &[hawdb_cypher::NodePatternKind],
    bound: &[bool],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<GraphMatchStep>> {
    let anchor = bound.iter().position(|bound| *bound).unwrap_or(0);
    let mut steps = vec![GraphMatchStep::Node(node(&nodes[anchor], parameters)?)];
    let expand = |source: usize, target: usize, reverse: bool| -> Result<GraphMatchStep> {
        let relationship = &pattern.steps[source.min(target)].relationship;
        let direction = match (relationship.direction, reverse) {
            (RelationshipDirection::Outgoing, true) => RelationshipDirection::Incoming,
            (RelationshipDirection::Incoming, true) => RelationshipDirection::Outgoing,
            (direction, _) => direction,
        };
        Ok(GraphMatchStep::Expand {
            source: nodes[source].variable.clone(),
            relationship: relationship.variable.clone(),
            rel_type: relationship.rel_type.clone(),
            rel_types: relationship.rel_type_disjunction.clone(),
            properties: bind_properties(&relationship.properties, parameters)?,
            direction,
            min_hops: relationship.min_hops,
            max_hops: relationship.max_hops,
            target: node(&nodes[target], parameters)?,
        })
    };
    for index in anchor..pattern.steps.len() {
        steps.push(expand(index, index + 1, false)?);
    }
    for index in (0..anchor).rev() {
        steps.push(expand(index + 1, index, true)?);
    }
    Ok(steps)
}

fn node(
    node: &hawdb_cypher::NodePatternKind,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphMatchNode> {
    Ok(GraphMatchNode {
        variable: node.variable.clone(),
        label: node.label.clone(),
        properties: bind_properties(&node.properties, parameters)?,
    })
}

/// Graph variables `expression` reads natively, including inside nested
/// pattern expressions.
pub(super) fn graph_variables_read(expression: &ProjectionExpression, read: &mut BTreeSet<String>) {
    match expression {
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::Id { variable }
        | ProjectionExpression::RelationshipType { variable }
        | ProjectionExpression::RelationshipEndpoint { variable, .. }
        | ProjectionExpression::DatePart { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { variable, .. }
        | ProjectionExpression::CasePropertyEqualsRank { variable, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { variable, .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            read.insert(variable.clone());
        }
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            read.insert(rank.variable.clone());
        }
        ProjectionExpression::Pattern(pattern) => {
            read.extend(pattern.graph_variables.iter().cloned());
        }
        _ => {}
    }
    expression.all_children(|child| {
        graph_variables_read(child, read);
        true
    });
}

/// How `expression` reads the enclosing column `column`: as a value or map
/// key that can be lent as a local (including through a nested pattern that
/// lends it), and directly from the row.
fn column_reads(expression: &ProjectionExpression, column: &str) -> (bool, bool) {
    let (mut local, mut direct) = match expression {
        ProjectionExpression::Column(name)
        | ProjectionExpression::ColumnProperty { column: name, .. } => (name == column, false),
        ProjectionExpression::Path { column: name, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { column: name, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column: name, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column: name, .. } => {
            (false, name == column)
        }
        ProjectionExpression::CaseColumnSearchRank(rank) => (false, rank.column == column),
        ProjectionExpression::Pattern(pattern) => {
            (pattern.columns.iter().any(|name| name == column), false)
        }
        _ => (false, false),
    };
    expression.all_children(|child| {
        let (child_local, child_direct) = column_reads(child, column);
        local |= child_local;
        direct |= child_direct;
        true
    });
    (local, direct)
}

/// Rebinds value and map-key reads of `column` to locals, leaving the forms
/// that read the row column to the copy the subquery row receives.
fn lend_column(expression: &mut ProjectionExpression, column: &str) {
    match expression {
        ProjectionExpression::Column(name) if name == column => {
            *expression = ProjectionExpression::LocalVariable {
                name: column.to_string(),
                property: None,
            };
        }
        ProjectionExpression::ColumnProperty {
            column: name,
            property,
        } if name == column => {
            *expression = ProjectionExpression::LocalVariable {
                name: column.to_string(),
                property: Some(std::mem::take(property)),
            };
        }
        _ => {
            let _ = expression.try_for_each_child_mut(|child| {
                lend_column(child, column);
                Ok::<(), std::convert::Infallible>(())
            });
        }
    }
}
