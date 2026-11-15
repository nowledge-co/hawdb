// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bind mutation clauses directly to the existing atomic storage commands.

use super::*;
use hawdb_cypher::{
    Clause, MatchPattern, NodePattern, RelationshipPattern, SetValueExpression, ValueExpression,
    ValueExpressionKind,
};

mod match_set;
mod node;
mod relationship;
mod relationship_batch;
mod returns;

#[derive(Default)]
struct MutationInput<'a> {
    patterns: Vec<&'a MatchPattern>,
    predicates: Vec<PropertyPredicate>,
}

impl MutationInput<'_> {
    fn predicate(&self) -> Option<PropertyPredicate> {
        match self.predicates.as_slice() {
            [] => None,
            [predicate] => Some(predicate.clone()),
            predicates => Some(PropertyPredicate::And(predicates.to_vec())),
        }
    }

    fn single_pattern(&self) -> Result<&MatchPattern> {
        match self.patterns.as_slice() {
            [pattern] => Ok(pattern),
            _ => Err(unsupported("mutation requires one bound MATCH pattern")),
        }
    }
}

pub(super) fn bind_unwind_mutation_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    // Beyond `UNWIND <mutation>`, the row binder below owns only
    // `UNWIND ... MATCH ... CREATE`; relationship MERGE over matched
    // endpoints and every other shape keep the general mutation binder.
    if query.clauses.len() != 2
        && !matches!(
            query.clauses.last().map(|clause| &clause.kind),
            Some(ClauseKind::Create(_))
        )
    {
        return bind_mutation_pipeline(query, parameters);
    }
    let Some((unwind, tail)) = query.clauses.split_first() else {
        unreachable!("UNWIND binding requires an UNWIND first clause")
    };
    let ClauseKind::Unwind { source, variable } = &unwind.kind else {
        unreachable!("UNWIND binding requires an UNWIND first clause")
    };
    let rows = bind_unwind_rows(source, parameters)?;
    let operation = match bind_row_mutation("UNWIND", variable, tail, parameters) {
        Ok(operation) => operation,
        Err(error) => return relationship_from_rows(query, parameters, error),
    };
    Ok(LogicalPlan::UnwindMutation {
        rows,
        variable: variable.clone(),
        operation,
    })
}

/// Binds `LOAD CSV ... <row mutation>`; records are read only at execution.
pub(super) fn bind_load_csv_mutation_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let Some((load, tail)) = query.clauses.split_first() else {
        unreachable!("LOAD CSV binding requires a LOAD CSV first clause")
    };
    let ClauseKind::LoadCsv(clause) = &load.kind else {
        unreachable!("LOAD CSV binding requires a LOAD CSV first clause")
    };
    let source = bind_load_csv_source(clause)?;
    let operation = bind_row_mutation("LOAD CSV", &clause.variable, tail, parameters)?;
    Ok(LogicalPlan::LoadCsvMutation { source, operation })
}

pub(super) fn bind_load_csv_source(clause: &hawdb_cypher::LoadCsvClause) -> Result<LoadCsvSource> {
    let location = match &clause.source.kind {
        ValueExpressionKind::Literal(Value::String(location)) => {
            CsvLocation::Literal(location.clone())
        }
        ValueExpressionKind::Parameter(name) => CsvLocation::Parameter(name.clone()),
        _ => {
            return Err(unsupported(
                "LOAD CSV FROM accepts only a string literal or parameter source",
            ))
        }
    };
    let field_terminator = u8::try_from(clause.field_terminator)
        .ok()
        .filter(|byte| byte.is_ascii() && !matches!(byte, b'"' | b'\r' | b'\n'))
        .ok_or_else(|| {
            unsupported(
                "FIELDTERMINATOR must be one ASCII character other than a double quote or line break",
            )
        })?;
    Ok(LoadCsvSource {
        location,
        with_headers: clause.with_headers,
        field_terminator,
        variable: clause.variable.clone(),
    })
}

/// Binds the clauses that follow a bounded row source to one mutation that is
/// resolved per row: a node CREATE or MERGE, or a CREATE of a relationship
/// between two independently matched nodes.
fn bind_row_mutation(
    source: &str,
    row_variable: &str,
    tail: &[Clause],
    parameters: &BTreeMap<String, Value>,
) -> Result<BatchMutationOperation> {
    let mut input = MutationInput::default();
    let mut clauses = tail;
    while let Some(clause) = clauses.first() {
        let ClauseKind::Match {
            optional,
            patterns,
            predicate,
        } = &clause.kind
        else {
            break;
        };
        if *optional {
            return Err(HawDBError::Semantic(format!(
                "{source} mutations do not support OPTIONAL MATCH"
            )));
        }
        if predicate.is_some() {
            return Err(HawDBError::Semantic(format!(
                "{source} MATCH does not support WHERE yet; select endpoints with property maps"
            )));
        }
        if patterns.iter().any(|pattern| pattern.variable.is_some()) {
            return Err(unsupported("atomic mutations do not bind path variables"));
        }
        input.patterns.extend(patterns);
        clauses = &clauses[1..];
    }
    let [mutation] = clauses else {
        return Err(HawDBError::Semantic(format!(
            "{source} mutations require exactly one following mutation clause"
        )));
    };
    let context = RowContext {
        source,
        row_variable,
        parameters,
    };
    match &mutation.kind {
        ClauseKind::Create(patterns) => {
            let [pattern] = patterns.as_slice() else {
                return Err(HawDBError::Semantic(format!(
                    "{source} CREATE requires one node pattern"
                )));
            };
            require_unbound_path(pattern)?;
            if !input.patterns.is_empty() {
                return bind_row_matched_relationship(&context, &input, pattern);
            }
            if !pattern.steps.is_empty() {
                return Err(HawDBError::Semantic(format!(
                    "{source} CREATE currently supports one node pattern or a relationship between matched nodes"
                )));
            }
            require_node_label(&pattern.first)?;
            Ok(BatchMutationOperation::CreateNode {
                label: pattern.first.label.clone(),
                properties: context.properties(&pattern.first.properties)?,
            })
        }
        ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            require_unbound_path(pattern)?;
            if !input.patterns.is_empty() || !pattern.steps.is_empty() {
                return Err(HawDBError::Semantic(format!(
                    "{source} MERGE currently supports one node pattern"
                )));
            }
            if !on_match.is_empty() {
                return Err(HawDBError::Semantic(format!(
                    "{source} MERGE does not support ON MATCH assignments yet"
                )));
            }
            let node = &pattern.first;
            require_node_label(node)?;
            Ok(BatchMutationOperation::MergeNode {
                label: node.label.clone(),
                match_properties: context.properties(&node.properties)?,
                on_create_properties: context.on_create_properties(
                    (!node.anonymous).then_some(node.variable.as_str()),
                    on_create,
                )?,
            })
        }
        ClauseKind::Set(_) | ClauseKind::Delete { .. } => Err(HawDBError::Semantic(format!(
            "{source} SET and DELETE require a bound mutation target and are not supported yet"
        ))),
        _ => Err(HawDBError::Semantic(format!(
            "{source} must be followed by CREATE, MERGE, SET, or DELETE"
        ))),
    }
}

fn bind_row_matched_relationship(
    context: &RowContext<'_>,
    input: &MutationInput<'_>,
    pattern: &MatchPattern,
) -> Result<BatchMutationOperation> {
    let (relationship, target) = one_hop(pattern, "CREATE")?;
    let (source, target) = relationship::bound_endpoints(input, &pattern.first, target)?;
    Ok(BatchMutationOperation::CreateMatchedRelationship(Box::new(
        BatchMatchedRelationship {
            source_label: source.label.clone(),
            source_properties: context.properties(&source.properties)?,
            target_label: target.label.clone(),
            target_properties: context.properties(&target.properties)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: context.properties(&relationship.properties)?,
        },
    )))
}

fn bind_unwind_rows(
    source: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let Value::List(rows) = bind_value(source, parameters)? else {
        return Err(HawDBError::Semantic(
            "UNWIND source must resolve to a list value".to_string(),
        ));
    };
    Ok(rows)
}

/// The row source a per-row mutation value may reference.
struct RowContext<'a> {
    source: &'a str,
    row_variable: &'a str,
    parameters: &'a BTreeMap<String, Value>,
}

impl RowContext<'_> {
    fn properties(
        &self,
        properties: &BTreeMap<String, ValueExpression>,
    ) -> Result<BTreeMap<String, BatchMutationValue>> {
        properties
            .iter()
            .map(|(property, value)| Ok((property.clone(), self.value(value)?)))
            .collect()
    }

    fn on_create_properties(
        &self,
        node_variable: Option<&str>,
        sets: &[SetProperty],
    ) -> Result<BTreeMap<String, BatchMutationValue>> {
        let source = self.source;
        if sets.is_empty() {
            return Ok(BTreeMap::new());
        }
        let Some(node_variable) = node_variable else {
            return Err(HawDBError::Semantic(format!(
                "{source} MERGE ON CREATE SET requires a bound node variable"
            )));
        };
        sets.iter()
            .map(|set| {
                if set.variable != node_variable {
                    return Err(HawDBError::Semantic(format!(
                        "{source} MERGE ON CREATE SET variable '{}' does not match bound variable '{node_variable}'",
                        set.variable
                    )));
                }
                let value = match &set.value {
                    SetValueExpression::Value(value) => self.value(value)?,
                    SetValueExpression::Property { variable, property }
                        if variable == self.row_variable =>
                    {
                        BatchMutationValue::RowProperty(property.clone())
                    }
                    _ => {
                        return Err(HawDBError::Semantic(format!(
                            "{source} MERGE ON CREATE SET supports row values only"
                        )))
                    }
                };
                Ok((set.property.clone(), value))
            })
            .collect()
    }

    fn value(&self, value: &ValueExpression) -> Result<BatchMutationValue> {
        let (source, row_variable) = (self.source, self.row_variable);
        match &value.kind {
            ValueExpressionKind::BindingProperty { variable, property } => {
                if variable != row_variable {
                    return Err(HawDBError::Semantic(format!(
                        "{source} mutation value '{variable}.{property}' does not reference row variable '{row_variable}'"
                    )));
                }
                Ok(BatchMutationValue::RowProperty(property.clone()))
            }
            ValueExpressionKind::BindingIndex { variable, index } => {
                if variable != row_variable {
                    return Err(HawDBError::Semantic(format!(
                        "{source} mutation value '{variable}[{index}]' does not reference row variable '{row_variable}'"
                    )));
                }
                Ok(BatchMutationValue::RowIndex(*index))
            }
            ValueExpressionKind::Conversion { function, argument } => {
                Ok(match self.value(argument)? {
                    BatchMutationValue::Static(value) => {
                        BatchMutationValue::Static(function.apply_unaccounted(&value)?)
                    }
                    input => BatchMutationValue::Converted {
                        function: *function,
                        input: Box::new(input),
                    },
                })
            }
            _ => bind_value(value, self.parameters).map(BatchMutationValue::Static),
        }
    }
}

/// One `UNWIND` row value: `row.key`, `row[index]`, a conversion of them, or
/// a value bound when the statement is planned.
fn bind_batch_value(
    value: &ValueExpression,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BatchMutationValue> {
    RowContext {
        source: "UNWIND",
        row_variable,
        parameters,
    }
    .value(value)
}

pub(super) fn bind_mutation_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    bind_atomic_mutation_pipeline(query, parameters)
        .or_else(|error| relationship_from_rows(query, parameters, error))
}

/// A relationship CREATE or MERGE the two-endpoint commands refuse is bound
/// over the rows of its read clauses when its shape allows; otherwise the
/// commands' own refusal stands.
pub(super) fn relationship_from_rows(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
    error: HawDBError,
) -> Result<LogicalPlan> {
    if !matches!(error, HawDBError::Semantic(_)) {
        return Err(error);
    }
    match match_set::bind_relationship_pipeline(&query.clauses, parameters) {
        Some(Ok(plan)) => Ok(plan),
        _ => Err(error),
    }
}

fn bind_atomic_mutation_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if let Some(plan) = match_set::bind_pipeline(&query.clauses, parameters) {
        return plan;
    }
    let has_unwind = query
        .clauses
        .iter()
        .any(|clause| matches!(clause.kind, ClauseKind::Unwind { .. }));
    let has_relationship_merge = query.clauses.iter().any(|clause| matches!(&clause.kind, ClauseKind::Merge { pattern, .. } if !pattern.steps.is_empty()));
    // Without UNWIND, relationship MERGE ... RETURN uses the atomic MERGE
    // command below and projects the merged relationship and endpoints.
    if has_relationship_merge && has_unwind {
        return relationship_batch::bind(query, parameters);
    }
    if has_unwind {
        return Err(unsupported(
            "UNWIND mutation pipeline is unsupported for this mutation shape",
        ));
    }
    let mut input = MutationInput::default();
    let mut clauses = query.clauses.as_slice();
    while let Some(clause) = clauses.first() {
        let ClauseKind::Match {
            optional,
            patterns,
            predicate,
        } = &clause.kind
        else {
            break;
        };
        if *optional || patterns.iter().any(|pattern| pattern.variable.is_some()) {
            return Err(unsupported(
                "atomic mutations require non-optional node or relationship matches",
            ));
        }
        input.patterns.extend(patterns);
        input
            .predicates
            .extend(predicate.iter().map(|predicate| predicate.kind.clone()));
        clauses = &clauses[1..];
    }
    let Some((clause, tail)) = clauses.split_first() else {
        return Err(unsupported("query requires a mutation clause"));
    };
    match &clause.kind {
        ClauseKind::Create(patterns) => {
            let projection = mutation_return(tail, "CREATE")?;
            let [pattern] = patterns.as_slice() else {
                return Err(unsupported("atomic CREATE requires one connected pattern"));
            };
            require_unbound_path(pattern)?;
            let plan = relationship::bind_create(&input, pattern, parameters)?;
            match projection {
                Some(projection) => returns::bind(
                    plan,
                    MutationReturnState::Written,
                    created_bindings(pattern),
                    projection,
                    parameters,
                ),
                None => Ok(plan),
            }
        }
        ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            require_unbound_path(pattern)?;
            let (tail, projection) = match tail.split_last() {
                Some((
                    Clause {
                        kind: ClauseKind::Return(projection),
                        ..
                    },
                    preceding,
                )) => (preceding, Some(projection)),
                _ => (tail, None),
            };
            let post_sets = set_tail(tail)?;
            let plan = if input.patterns.is_empty() && pattern.steps.is_empty() {
                let node = &pattern.first;
                require_node_label(node)?;
                let variable = (!node.anonymous).then_some(node.variable.as_str());
                let on_create_properties =
                    bind_on_create_set_properties(variable, on_create, parameters)?;
                let on_match_assignments =
                    bind_on_match_set_assignments(variable, on_match, parameters)?;
                let post_merge_assignments =
                    bind_post_merge_set_assignments(variable, &post_sets, parameters)?;
                LogicalPlan::MergeNode {
                    label: node.label.clone(),
                    match_properties: bind_properties(&node.properties, parameters)?,
                    on_create_properties,
                    on_match_assignments,
                    post_merge_assignments,
                }
            } else {
                if !post_sets.is_empty() {
                    return Err(unsupported(
                        "relationship MERGE does not support a following SET; use ON CREATE SET or ON MATCH SET",
                    ));
                }
                relationship::bind_merge(&input, pattern, on_create, on_match, parameters)?
            };
            match projection {
                // MERGE summary rows use the CREATE columns: `node_id`, or
                // `source_node_id`, `rel_id` and `target_node_id`.
                Some(projection) => returns::bind(
                    plan,
                    MutationReturnState::Written,
                    created_bindings(pattern),
                    projection,
                    parameters,
                ),
                None => Ok(plan),
            }
        }
        ClauseKind::Set(sets) => {
            let (tail, projection) = match tail.split_last() {
                Some((clause, preceding)) if matches!(clause.kind, ClauseKind::Return(_)) => {
                    let ClauseKind::Return(projection) = &clause.kind else {
                        unreachable!()
                    };
                    (preceding, Some(projection))
                }
                _ => (tail, None),
            };
            let mut assignments = sets.clone();
            assignments.extend(set_tail(tail)?);
            // The single-pattern commands return unmodified rows only.
            let modified = projection.is_some_and(|projection| {
                projection.distinct
                    || projection.predicate.is_some()
                    || !projection.order_by.is_empty()
                    || projection.offset.is_some()
                    || projection.limit.is_some()
            });
            if modified
                || assignments
                    .iter()
                    .any(|set| match_set::needs_row_evaluation(set, parameters))
            {
                let prefix = &query.clauses[..query.clauses.len() - clauses.len()];
                return match_set::bind(prefix, &assignments, projection, parameters);
            }
            node::bind_set(
                &input,
                &assignments,
                projection.map(|projection| projection.items.as_slice()),
                parameters,
            )
        }
        ClauseKind::Delete { detach, variables } => {
            let projection = mutation_return(tail, "DELETE")?;
            let [variable] = variables.as_slice() else {
                return Err(unsupported("atomic DELETE requires one target variable"));
            };
            let plan = node::bind_delete(&input, variable, *detach, parameters)?;
            match projection {
                Some(projection) => {
                    let bindings = deleted_bindings(&plan)?;
                    returns::bind(
                        plan,
                        MutationReturnState::Deleted,
                        bindings,
                        projection,
                        parameters,
                    )
                }
                None => Ok(plan),
            }
        }
        _ => Err(unsupported(
            "clause cannot feed the atomic mutation command",
        )),
    }
}

/// The `RETURN` that may conclude a mutation; any other following clause is
/// refused for `operation`.
fn mutation_return<'a>(
    tail: &'a [Clause],
    operation: &str,
) -> Result<Option<&'a hawdb_cypher::ProjectionClause>> {
    match tail {
        [] => Ok(None),
        [clause] => match &clause.kind {
            ClauseKind::Return(projection) => Ok(Some(projection)),
            _ => Err(HawDBError::Semantic(format!(
                "{operation} has unsupported following clauses"
            ))),
        },
        _ => Err(HawDBError::Semantic(format!(
            "{operation} has unsupported following clauses"
        ))),
    }
}

/// The named variables of a created pattern, keyed by the summary-row columns
/// storage reports for each created node or relationship.
fn created_bindings(pattern: &MatchPattern) -> Vec<MutationReturnBinding> {
    let node = |node: &NodePattern, column: &str| {
        (!node.anonymous).then(|| MutationReturnBinding {
            variable: node.variable.clone(),
            entity: MutationReturnEntity::Node,
            column: column.to_string(),
        })
    };
    let Some(step) = pattern.steps.first() else {
        return node(&pattern.first, "node_id").into_iter().collect();
    };
    let relationship = step
        .relationship
        .variable
        .as_ref()
        .map(|variable| MutationReturnBinding {
            variable: variable.clone(),
            entity: MutationReturnEntity::Relationship,
            column: "rel_id".to_string(),
        });
    node(&pattern.first, "source_node_id")
        .into_iter()
        .chain(relationship)
        .chain(node(&step.target, "target_node_id"))
        .collect()
}

/// The variables a DELETE can return, read as they were before the delete:
/// the deleted node, or the deleted relationship and its endpoints.
fn deleted_bindings(plan: &LogicalPlan) -> Result<Vec<MutationReturnBinding>> {
    let binding = |variable: &str, entity, column: &str| MutationReturnBinding {
        variable: variable.to_string(),
        entity,
        column: column.to_string(),
    };
    match plan {
        LogicalPlan::DeleteNode { variable, .. } => Ok(vec![binding(
            variable,
            MutationReturnEntity::Node,
            "node_id",
        )]),
        LogicalPlan::DeleteRelationship {
            source_variable,
            rel_variable,
            target_variable,
            ..
        } => Ok(vec![
            binding(
                source_variable,
                MutationReturnEntity::RelationshipSource,
                "rel_id",
            ),
            binding(rel_variable, MutationReturnEntity::Relationship, "rel_id"),
            binding(
                target_variable,
                MutationReturnEntity::RelationshipTarget,
                "rel_id",
            ),
        ]),
        // Storage reports each deleted target node once, while a MATCH row
        // exists per source and relationship; refuse rather than miscount.
        _ => Err(unsupported(
            "DELETE RETURN supports node DELETE and relationship-variable DELETE",
        )),
    }
}

fn set_tail(clauses: &[Clause]) -> Result<Vec<SetProperty>> {
    let mut assignments = Vec::new();
    for clause in clauses {
        let ClauseKind::Set(sets) = &clause.kind else {
            return Err(unsupported("mutation has unsupported following clauses"));
        };
        assignments.extend(sets.iter().cloned());
    }
    Ok(assignments)
}

fn one_hop<'a>(
    pattern: &'a MatchPattern,
    operation: &str,
) -> Result<(&'a RelationshipPattern, &'a NodePattern)> {
    let [step] = pattern.steps.as_slice() else {
        return Err(unsupported("mutation requires one relationship step"));
    };
    let relationship = &step.relationship;
    if relationship.min_hops != 1
        || relationship.max_hops != 1
        || relationship.search != PathSearch::All
    {
        return Err(HawDBError::Semantic(format!(
            "relationship {operation} supports only one-hop relationship patterns"
        )));
    }
    if relationship.direction != RelationshipDirection::Outgoing {
        return Err(HawDBError::Semantic(format!(
            "relationship {operation} supports only outgoing relationship patterns"
        )));
    }
    if relationship.rel_type.is_empty() {
        return Err(HawDBError::Semantic(format!(
            "relationship {operation} requires a relationship type"
        )));
    }
    Ok((relationship, &step.target))
}

fn require_unbound_path(pattern: &MatchPattern) -> Result<()> {
    if pattern.variable.is_some() {
        return Err(unsupported("atomic mutations do not bind path variables"));
    }
    Ok(())
}

fn require_node_label(node: &NodePattern) -> Result<()> {
    if node.label.is_empty() {
        return Err(unsupported("node creation requires a label"));
    }
    Ok(())
}

fn unsupported(message: &str) -> HawDBError {
    HawDBError::Semantic(message.to_string())
}
