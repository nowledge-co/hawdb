// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! SET items that are general expressions over the matched rows (#1791).
//!
//! The single-pattern SET commands evaluate only literals, parameters and a
//! few fixed read-modify-writes of the written property. Every other SET is
//! bound here: the clauses before SET become an ordinary read plan that
//! returns one column per variable in scope, and [`MatchSet`] evaluates the
//! SET items over those rows.

use super::*;
use crate::logical::{
    MatchSet, MatchSetAssignment, MatchSetBinding, MatchSetRelationship, MatchSetReturn,
    MatchSetValue,
};
use hawdb_cypher::{ProjectionClause, QueryPipelineKind};

/// The input column of `variable`. A NUL byte cannot occur in a Cypher name,
/// so the column never collides with a user column.
fn binding_column(variable: &str) -> String {
    format!("\0set.{variable}")
}

/// Whether a SET item needs row evaluation: its value is a general
/// expression, or a fixed form the single-pattern commands cannot evaluate,
/// such as a property of another variable or a non-integer increment.
/// Binding errors such as a missing parameter stay with the fixed forms, so
/// they report what they always reported.
pub(super) fn needs_row_evaluation(
    set: &SetProperty,
    parameters: &BTreeMap<String, Value>,
) -> bool {
    let other = |variable: &String, property: &String| {
        *variable != set.variable || *property != set.property
    };
    // `x.p + v` is an integer increment only for an integer `v`; a value
    // read from the row, such as another variable's property, is general.
    let non_integer = |value: &ValueExpression| {
        value.kind.reads_row_binding()
            || bind_value(value, parameters).is_ok_and(|value| !matches!(value, Value::Int(_)))
    };
    match &set.value {
        SetValueExpression::Value(value) => value.kind.reads_row_binding(),
        SetValueExpression::Expression(_) | SetValueExpression::Property { .. } => true,
        SetValueExpression::CoalesceProperty {
            variable, property, ..
        }
        | SetValueExpression::DecrementFloorZero { variable, property }
        | SetValueExpression::PreserveNewerExisting {
            variable, property, ..
        }
        | SetValueExpression::PreserveOlderExisting {
            variable, property, ..
        } => other(variable, property),
        SetValueExpression::PropertyAdd {
            variable,
            property,
            value,
        } => other(variable, property) || non_integer(value),
        SetValueExpression::CoalescePropertyAdd {
            variable,
            property,
            default,
            value,
        } => {
            // The fixed form only supports a zero default.
            other(variable, property)
                || bind_value(default, parameters).is_ok_and(|default| default != Value::Int(0))
                || non_integer(value)
        }
    }
}

/// Binds `<read clauses> SET ... [SET ...] [RETURN ...]` whose read clauses
/// the single-pattern commands cannot take: `WITH`, `UNWIND`, `OPTIONAL
/// MATCH` or a named path (#1795). Returns `None` for every other shape.
pub(super) fn bind_pipeline(
    clauses: &[Clause],
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<LogicalPlan>> {
    let start = clauses
        .iter()
        .position(|clause| matches!(clause.kind, ClauseKind::Set(_)))?;
    let (prefix, tail) = clauses.split_at(start);
    let read_only = prefix.iter().all(|clause| {
        matches!(
            clause.kind,
            ClauseKind::Match { .. }
                | ClauseKind::With(_)
                | ClauseKind::Unwind { .. }
                | ClauseKind::UnwindExpression { .. }
        )
    });
    let needs_rows = prefix.iter().any(|clause| match &clause.kind {
        ClauseKind::Match {
            optional, patterns, ..
        } => *optional || patterns.iter().any(|pattern| pattern.variable.is_some()),
        _ => true,
    });
    if !read_only || !needs_rows {
        return None;
    }
    let (sets, returns) = match tail.split_last() {
        Some((
            Clause {
                kind: ClauseKind::Return(projection),
                ..
            },
            sets,
        )) => (sets, Some(projection)),
        _ => (tail, None),
    };
    let mut assignments = Vec::new();
    for clause in sets {
        let ClauseKind::Set(items) = &clause.kind else {
            return Some(Err(unsupported(
                "mutation has unsupported following clauses",
            )));
        };
        assignments.extend(items.iter().cloned());
    }
    Some(bind(prefix, &assignments, returns, parameters))
}

pub(super) fn bind(
    prefix: &[Clause],
    sets: &[SetProperty],
    returns: Option<&ProjectionClause>,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if sets.is_empty() {
        return Err(unsupported("SET requires at least one assignment"));
    }
    let RowInput {
        input,
        bindings,
        graph,
        values,
        ..
    } = bind_rows(prefix, parameters)?;
    let mut assignments = Vec::with_capacity(sets.len());
    for set in sets {
        if !graph.contains(&set.variable) {
            return Err(HawDBError::Semantic(format!(
                "unknown variable '{}' in set item: SET requires a bound node or relationship",
                set.variable
            )));
        }
        let value = match &set.value {
            SetValueExpression::Expression(expression) => MatchSetValue::Expression(
                plan_scalar_expression_with_columns(&graph, &values, expression, parameters)?,
            ),
            SetValueExpression::Value(value) if value.kind.reads_row_binding() => {
                MatchSetValue::Expression(plan_scalar_expression_with_columns(
                    &graph,
                    &values,
                    &row_value_expression(value.clone())?,
                    parameters,
                )?)
            }
            value => match plan_set_value(set, parameters) {
                Ok(value) => MatchSetValue::Local(value),
                Err(error) => {
                    let expression = fixed_form_expression(value).ok_or(error)?;
                    MatchSetValue::Expression(plan_scalar_expression_with_columns(
                        &graph,
                        &values,
                        &expression,
                        parameters,
                    )?)
                }
            },
        };
        assignments.push(MatchSetAssignment {
            variable: set.variable.clone(),
            property: set.property.clone(),
            value,
        });
    }
    let returns = returns
        .map(|projection| bind_return(&graph, &values, projection, parameters))
        .transpose()?;
    Ok(LogicalPlan::MatchSet(Box::new(MatchSet {
        input,
        bindings,
        assignments,
        relationship: None,
        returns,
    })))
}

/// Binds `<read clauses> CREATE|MERGE (a)-[:R {...}]->(b) [RETURN ...]`
/// between two bound nodes, for the read clauses the two-endpoint commands
/// cannot take: `IN` lists, pattern predicates, `UNWIND` rows, `WITH`, or the
/// same node at both ends (#1790). Returns `None` for every other shape.
pub(super) fn bind_relationship_pipeline(
    clauses: &[Clause],
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<LogicalPlan>> {
    let start = clauses.iter().position(|clause| {
        matches!(
            clause.kind,
            ClauseKind::Create(_) | ClauseKind::Merge { .. }
        )
    })?;
    let (prefix, tail) = clauses.split_at(start);
    if prefix.is_empty()
        || !prefix.iter().all(|clause| {
            matches!(
                clause.kind,
                ClauseKind::Match { .. }
                    | ClauseKind::With(_)
                    | ClauseKind::Unwind { .. }
                    | ClauseKind::UnwindExpression { .. }
            )
        })
    {
        return None;
    }
    let (write, returns) = match tail {
        [write] => (write, None),
        [write, Clause {
            kind: ClauseKind::Return(projection),
            ..
        }] => (write, Some(projection)),
        _ => return None,
    };
    let (pattern, merge, on_create) = match &write.kind {
        ClauseKind::Create(patterns) => match patterns.as_slice() {
            [pattern] => (pattern, false, &[][..]),
            _ => return None,
        },
        ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            if !on_match.is_empty() {
                return None;
            }
            (pattern, true, on_create.as_slice())
        }
        _ => return None,
    };
    // Only a relationship between two bound, otherwise unconstrained nodes.
    let [step] = pattern.steps.as_slice() else {
        return None;
    };
    let relationship = &step.relationship;
    let bound =
        |node: &NodePattern| !node.anonymous && node.label.is_empty() && node.properties.is_empty();
    if pattern.variable.is_some()
        || !bound(&pattern.first)
        || !bound(&step.target)
        || relationship.rel_type.is_empty()
        || !relationship.rel_type_disjunction.is_empty()
        || relationship.min_hops != 1
        || relationship.max_hops != 1
        || relationship.search != PathSearch::All
    {
        return None;
    }
    let (source, target) = match relationship.direction {
        RelationshipDirection::Outgoing => (&pattern.first.variable, &step.target.variable),
        RelationshipDirection::Incoming => (&step.target.variable, &pattern.first.variable),
        RelationshipDirection::Undirected => return None,
    };
    Some(bind_relationship(
        prefix,
        RelationshipWrite {
            merge,
            source,
            target,
            pattern: relationship,
            on_create,
        },
        returns,
        parameters,
    ))
}

struct RelationshipWrite<'a> {
    merge: bool,
    source: &'a str,
    target: &'a str,
    pattern: &'a hawdb_cypher::RelationshipPattern,
    on_create: &'a [SetProperty],
}

fn bind_relationship(
    prefix: &[Clause],
    write: RelationshipWrite<'_>,
    returns: Option<&ProjectionClause>,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let RowInput {
        input,
        bindings,
        graph,
        values,
        nodes,
    } = bind_rows(prefix, parameters)?;
    for endpoint in [write.source, write.target] {
        if !nodes.contains(endpoint) {
            return Err(HawDBError::Semantic(format!(
                "relationship endpoint '{endpoint}' must be a node bound by an earlier clause"
            )));
        }
    }
    if let Some(variable) = &write.pattern.variable
        && (graph.contains(variable) || values.contains(variable))
    {
        return Err(HawDBError::Semantic(format!(
            "relationship variable '{variable}' is already bound"
        )));
    }
    let plan_value = |value: &ValueExpression| {
        plan_scalar_expression_with_columns(
            &graph,
            &values,
            &row_value_expression(value.clone())?,
            parameters,
        )
    };
    let properties = write
        .pattern
        .properties
        .iter()
        .map(|(property, value)| Ok((property.clone(), plan_value(value)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut on_create = Vec::with_capacity(write.on_create.len());
    for set in write.on_create {
        if write.pattern.variable.as_deref() != Some(set.variable.as_str()) {
            return Err(HawDBError::Semantic(format!(
                "ON CREATE SET variable '{}' is not the merged relationship",
                set.variable
            )));
        }
        // The new relationship is not readable yet: values come from the row.
        let expression = match &set.value {
            SetValueExpression::Value(value) => row_value_expression(value.clone())?,
            value => fixed_form_expression(value).ok_or_else(|| {
                unsupported("ON CREATE SET of a row-evaluated MERGE cannot read the relationship")
            })?,
        };
        on_create.push((
            set.property.clone(),
            plan_scalar_expression_with_columns(&graph, &values, &expression, parameters)?,
        ));
    }
    // The created relationship is not in RETURN's scope; matched variables
    // are.
    let returns = returns
        .map(|projection| bind_return(&graph, &values, projection, parameters))
        .transpose()?;
    Ok(LogicalPlan::MatchSet(Box::new(MatchSet {
        input,
        bindings,
        assignments: Vec::new(),
        relationship: Some(MatchSetRelationship {
            merge: write.merge,
            source: write.source.to_string(),
            target: write.target.to_string(),
            rel_type: write.pattern.rel_type.clone(),
            properties,
            on_create,
        }),
        returns,
    })))
}

/// The read input of a row-evaluated mutation and the variables it binds.
struct RowInput {
    input: LogicalPlan,
    bindings: Vec<MatchSetBinding>,
    /// Named nodes and relationships, read by id for each row.
    graph: BTreeSet<String>,
    /// Scalar variables, read as values.
    values: BTreeSet<String>,
    nodes: BTreeSet<String>,
}

/// Plans the clauses before the mutation as a read returning one column per
/// variable in scope: the id of each named node or relationship and the value
/// of each scalar.
fn bind_rows(prefix: &[Clause], parameters: &BTreeMap<String, Value>) -> Result<RowInput> {
    // Bind the read clauses as the read input will be bound, with pattern
    // properties that read row values lowered to predicates.
    let pipeline = AstNode::synthetic(QueryPipelineKind {
        clauses: prefix.to_vec(),
    });
    let lowered;
    let pipeline = if pattern_properties::needs_lowering(&pipeline) {
        lowered = pattern_properties::lower(&pipeline)?;
        &lowered
    } else {
        &pipeline
    };
    let (_, scope) =
        bind_read_clauses_scoped(&pipeline.clauses, None, Scope::default(), parameters)?;
    // Anonymous pattern nodes cannot be named by a SET item or RETURN.
    let anonymous = prefix
        .iter()
        .filter_map(|clause| match &clause.kind {
            ClauseKind::Match { patterns, .. } => Some(patterns),
            _ => None,
        })
        .flatten()
        .flat_map(|pattern| {
            std::iter::once(&pattern.first).chain(pattern.steps.iter().map(|step| &step.target))
        })
        .filter(|node| node.anonymous)
        .map(|node| node.variable.as_str())
        .collect::<BTreeSet<_>>();
    let mut graph = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    let mut values = BTreeSet::new();
    let mut bindings = Vec::new();
    let mut items = Vec::new();
    for (variable, binding) in &scope.0 {
        let (expression, entity) = match binding {
            BindingType::Graph { kind, .. } => {
                if anonymous.contains(variable.as_str()) {
                    continue;
                }
                graph.insert(variable.clone());
                if *kind == GraphEntityKind::Node {
                    nodes.insert(variable.clone());
                }
                (ScalarExpressionKind::Id(variable.clone()), Some(*kind))
            }
            BindingType::Scalar | BindingType::Unwound | BindingType::Path => {
                values.insert(variable.clone());
                (ScalarExpressionKind::Variable(variable.clone()), None)
            }
            BindingType::UnmaterializedPath => continue,
        };
        let column = binding_column(variable);
        items.push(AstNode::synthetic(ReturnItemKind {
            expression: AstNode::synthetic(ReturnExpressionKind::Value(AstNode::synthetic(
                expression,
            ))),
            alias: Some(column.clone()),
        }));
        bindings.push(MatchSetBinding {
            variable: variable.clone(),
            column,
            entity,
        });
    }
    let mut clauses = prefix.to_vec();
    clauses.push(AstNode::synthetic(ClauseKind::Return(ProjectionClause {
        distinct: false,
        items,
        predicate: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
    })));
    let input = plan_parsed_pipeline_query(
        &AstNode::synthetic(QueryPipelineKind { clauses }),
        parameters,
    )?;
    Ok(RowInput {
        input,
        bindings,
        graph,
        values,
        nodes,
    })
}

/// Plans the RETURN of the updated rows. `ORDER BY` reads a returned column
/// by its name, which shadows a variable of the same name, or any variable
/// of the row, as after any non-aggregating RETURN.
fn bind_return(
    graph: &BTreeSet<String>,
    values: &BTreeSet<String>,
    projection: &ProjectionClause,
    parameters: &BTreeMap<String, Value>,
) -> Result<MatchSetReturn> {
    if projection.predicate.is_some() {
        return Err(unsupported("RETURN does not support WHERE"));
    }
    let PlannedReturns::Projections(mut items) =
        plan_return_items_with_columns(graph, values, &projection.items, parameters)?
    else {
        return Err(unsupported(
            "RETURN after a row-evaluated SET does not support aggregation",
        ));
    };
    let names = unique_projection_names(items.iter().map(|item| item.name.clone()).collect());
    for (item, name) in items.iter_mut().zip(names) {
        item.name = name;
    }
    let returned = items
        .iter()
        .map(|item| item.name.clone())
        .collect::<BTreeSet<_>>();
    // A returned entity (`RETURN n`) stays a graph variable for its keys.
    let shadowing = items
        .iter()
        .filter(|item| {
            !matches!(&item.expression, ProjectionExpression::Variable { variable }
                if *variable == item.name && graph.contains(variable))
        })
        .map(|item| item.name.clone())
        .collect::<BTreeSet<_>>();
    let key_graph = graph
        .difference(&shadowing)
        .cloned()
        .collect::<BTreeSet<_>>();
    let key_columns = values.union(&shadowing).cloned().collect::<BTreeSet<_>>();
    let order_by = plan_sort_items(&key_graph, &key_columns, &projection.order_by, parameters)?;
    if projection.distinct
        && order_by
            .iter()
            .any(|item| !matches!(&item.key, SortKey::Column(name) if returned.contains(name)))
    {
        return Err(unsupported(
            "ORDER BY after RETURN DISTINCT must name returned columns",
        ));
    }
    Ok(MatchSetReturn {
        items,
        distinct: projection.distinct,
        order_by,
        offset: projection
            .offset
            .as_ref()
            .map(|value| bind_pagination_value(value, parameters, "offset"))
            .transpose()?
            .unwrap_or(0),
        limit: projection
            .limit
            .as_ref()
            .map(|value| bind_pagination_value(value, parameters, "limit"))
            .transpose()?,
    })
}

/// The expression a fixed SET form denotes, for the forms row evaluation
/// computes generally, with openCypher semantics: `x.p + v` is `NULL` when
/// `x.p` is `NULL`, and concatenates strings.
fn fixed_form_expression(value: &SetValueExpression) -> Option<ScalarExpression> {
    let property = |variable: &String, property: &String| {
        AstNode::synthetic(ScalarExpressionKind::Property {
            variable: variable.clone(),
            property: property.clone(),
        })
    };
    // A row-bound operand, such as `m.x` in `n.p + m.x`, reads the row.
    let value_expression = |value: &ValueExpression| {
        row_value_expression(value.clone())
            .unwrap_or_else(|_| AstNode::synthetic(ScalarExpressionKind::Value(value.clone())))
    };
    let binary = |left, op, right| {
        AstNode::synthetic(ScalarExpressionKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    };
    let coalesce = |variable: &String, name: &String, default: &ValueExpression| {
        AstNode::synthetic(ScalarExpressionKind::Coalesce(vec![
            property(variable, name),
            value_expression(default),
        ]))
    };
    Some(match value {
        SetValueExpression::Expression(expression) => expression.clone(),
        SetValueExpression::Value(value) => value_expression(value),
        SetValueExpression::Property {
            variable,
            property: name,
        } => property(variable, name),
        SetValueExpression::CoalesceProperty {
            variable,
            property: name,
            default,
        } => coalesce(variable, name, default),
        SetValueExpression::PropertyAdd {
            variable,
            property: name,
            value,
        } => binary(
            property(variable, name),
            hawdb_cypher::ScalarBinaryOp::Add,
            value_expression(value),
        ),
        SetValueExpression::CoalescePropertyAdd {
            variable,
            property: name,
            default,
            value,
        } => binary(
            coalesce(variable, name, default),
            hawdb_cypher::ScalarBinaryOp::Add,
            value_expression(value),
        ),
        SetValueExpression::DecrementFloorZero {
            variable,
            property: name,
        } => case(
            vec![(
                binary(
                    property(variable, name),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    integer(0),
                ),
                binary(
                    property(variable, name),
                    hawdb_cypher::ScalarBinaryOp::Subtract,
                    integer(1),
                ),
            )],
            integer(0),
        ),
        SetValueExpression::PreserveNewerExisting {
            variable,
            property: name,
            incoming,
            preserve,
        }
        | SetValueExpression::PreserveOlderExisting {
            variable,
            property: name,
            incoming,
            preserve,
        } => {
            // Keep the current value when the incoming one is NULL, or when
            // `preserve` holds and the current value is newer (older).
            let current = property(variable, name);
            let incoming = value_expression(incoming);
            let keeps = if matches!(value, SetValueExpression::PreserveNewerExisting { .. }) {
                binary(
                    current.clone(),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    incoming.clone(),
                )
            } else {
                binary(
                    incoming.clone(),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    current.clone(),
                )
            };
            let present = AstNode::synthetic(ScalarExpressionKind::IsNull {
                expression: Box::new(current.clone()),
                negated: true,
            });
            let preserved = binary(
                binary(
                    value_expression(preserve),
                    hawdb_cypher::ScalarBinaryOp::And,
                    present,
                ),
                hawdb_cypher::ScalarBinaryOp::And,
                keeps,
            );
            let missing = AstNode::synthetic(ScalarExpressionKind::IsNull {
                expression: Box::new(incoming.clone()),
                negated: false,
            });
            case(
                vec![(missing, current.clone()), (preserved, current)],
                incoming,
            )
        }
    })
}

fn integer(value: i64) -> ScalarExpression {
    AstNode::synthetic(ScalarExpressionKind::Value(AstNode::synthetic(
        ValueExpressionKind::Literal(Value::Int(value)),
    )))
}

fn case(
    branches: Vec<(ScalarExpression, ScalarExpression)>,
    otherwise: ScalarExpression,
) -> ScalarExpression {
    AstNode::synthetic(ScalarExpressionKind::Case {
        operand: None,
        branches,
        otherwise: Some(Box::new(otherwise)),
    })
}
