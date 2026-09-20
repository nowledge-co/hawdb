//! Bind mutation clauses directly to the existing atomic storage commands.

use super::*;
use hawdb_cypher::{
    Clause, MatchPattern, NodePattern, RelationshipPattern, SetValueExpression, ValueExpression,
    ValueExpressionKind,
};

mod node;
mod relationship;

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
    let [unwind, mutation] = query.clauses.as_slice() else {
        return Err(unsupported(
            "UNWIND mutations require exactly one following mutation clause",
        ));
    };
    let ClauseKind::Unwind { source, variable } = &unwind.kind else {
        unreachable!("UNWIND binding requires an UNWIND first clause")
    };
    let rows = bind_unwind_rows(source, parameters)?;
    let operation = match &mutation.kind {
        ClauseKind::Create(patterns) => {
            let [pattern] = patterns.as_slice() else {
                return Err(unsupported("UNWIND CREATE requires one node pattern"));
            };
            require_unbound_path(pattern)?;
            if !pattern.steps.is_empty() {
                return Err(unsupported(
                    "UNWIND CREATE currently supports one node pattern",
                ));
            }
            require_node_label(&pattern.first)?;
            BatchMutationOperation::CreateNode {
                label: pattern.first.label.clone(),
                properties: bind_batch_properties(&pattern.first.properties, variable, parameters)?,
            }
        }
        ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            require_unbound_path(pattern)?;
            if !pattern.steps.is_empty() {
                return Err(unsupported(
                    "UNWIND MERGE currently supports one node pattern",
                ));
            }
            if !on_match.is_empty() {
                return Err(unsupported(
                    "UNWIND MERGE does not support ON MATCH assignments yet",
                ));
            }
            let node = &pattern.first;
            require_node_label(node)?;
            BatchMutationOperation::MergeNode {
                label: node.label.clone(),
                match_properties: bind_batch_properties(&node.properties, variable, parameters)?,
                on_create_properties: bind_batch_on_create_properties(
                    (!node.anonymous).then_some(node.variable.as_str()),
                    on_create,
                    variable,
                    parameters,
                )?,
            }
        }
        ClauseKind::Set(_) | ClauseKind::Delete { .. } => {
            return Err(unsupported(
                "UNWIND SET and DELETE require a bound mutation target and are not supported yet",
            ));
        }
        _ => {
            return Err(unsupported(
                "UNWIND must be followed by CREATE, MERGE, SET, or DELETE",
            ))
        }
    };
    Ok(LogicalPlan::UnwindMutation {
        rows,
        variable: variable.clone(),
        operation,
    })
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

fn bind_batch_properties(
    properties: &BTreeMap<String, ValueExpression>,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, BatchMutationValue>> {
    properties
        .iter()
        .map(|(property, value)| {
            Ok((
                property.clone(),
                bind_batch_value(value, row_variable, parameters)?,
            ))
        })
        .collect()
}

fn bind_batch_on_create_properties(
    node_variable: Option<&str>,
    sets: &[SetProperty],
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, BatchMutationValue>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(node_variable) = node_variable else {
        return Err(HawDBError::Semantic(
            "UNWIND MERGE ON CREATE SET requires a bound node variable".to_string(),
        ));
    };
    sets.iter()
        .map(|set| {
            if set.variable != node_variable {
                return Err(HawDBError::Semantic(format!(
                    "UNWIND MERGE ON CREATE SET variable '{}' does not match bound variable '{node_variable}'",
                    set.variable
                )));
            }
            let value = match &set.value {
                SetValueExpression::Value(value) => {
                    bind_batch_value(value, row_variable, parameters)?
                }
                SetValueExpression::Property { variable, property } if variable == row_variable => {
                    BatchMutationValue::RowProperty(property.clone())
                }
                _ => {
                    return Err(HawDBError::Semantic(
                        "UNWIND MERGE ON CREATE SET supports row values only".to_string(),
                    ))
                }
            };
            Ok((set.property.clone(), value))
        })
        .collect()
}

fn bind_batch_value(
    value: &ValueExpression,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BatchMutationValue> {
    match &value.kind {
        ValueExpressionKind::BindingProperty { variable, property } => {
            if variable != row_variable {
                return Err(HawDBError::Semantic(format!(
                    "UNWIND mutation value '{variable}.{property}' does not reference row variable '{row_variable}'"
                )));
            }
            Ok(BatchMutationValue::RowProperty(property.clone()))
        }
        _ => bind_value(value, parameters).map(BatchMutationValue::Static),
    }
}

pub(super) fn bind_mutation_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
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
            if !tail.is_empty() {
                return Err(unsupported("CREATE has unsupported following clauses"));
            }
            let [pattern] = patterns.as_slice() else {
                return Err(unsupported("atomic CREATE requires one connected pattern"));
            };
            require_unbound_path(pattern)?;
            relationship::bind_create(&input, pattern, parameters)
        }
        ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            require_unbound_path(pattern)?;
            let post_sets = set_tail(tail)?;
            if input.patterns.is_empty() && pattern.steps.is_empty() {
                let node = &pattern.first;
                require_node_label(node)?;
                let variable = (!node.anonymous).then_some(node.variable.as_str());
                let on_create_properties =
                    bind_on_create_set_properties(variable, on_create, parameters)?;
                let on_match_assignments =
                    bind_on_match_set_assignments(variable, on_match, parameters)?;
                let post_merge_assignments =
                    bind_post_merge_set_assignments(variable, &post_sets, parameters)?;
                return Ok(LogicalPlan::MergeNode {
                    label: node.label.clone(),
                    match_properties: bind_properties(&node.properties, parameters)?,
                    on_create_properties,
                    on_match_assignments,
                    post_merge_assignments,
                });
            }
            if !on_match.is_empty() || !post_sets.is_empty() {
                return Err(unsupported(
                    "relationship MERGE supports only ON CREATE assignments",
                ));
            }
            relationship::bind_merge(&input, pattern, on_create, parameters)
        }
        ClauseKind::Set(sets) => {
            let (tail, projection) = match tail.split_last() {
                Some((clause, preceding)) if matches!(clause.kind, ClauseKind::Return(_)) => {
                    let ClauseKind::Return(projection) = &clause.kind else {
                        unreachable!()
                    };
                    if projection.distinct
                        || projection.predicate.is_some()
                        || !projection.order_by.is_empty()
                        || projection.offset.is_some()
                        || projection.limit.is_some()
                    {
                        return Err(unsupported("SET RETURN does not support result modifiers"));
                    }
                    (preceding, Some(projection.items.as_slice()))
                }
                _ => (tail, None),
            };
            let mut assignments = sets.clone();
            assignments.extend(set_tail(tail)?);
            node::bind_set(&input, &assignments, projection, parameters)
        }
        ClauseKind::Delete { detach, variables } => {
            if !tail.is_empty() {
                return Err(unsupported("DELETE has unsupported following clauses"));
            }
            let [variable] = variables.as_slice() else {
                return Err(unsupported("atomic DELETE requires one target variable"));
            };
            node::bind_delete(&input, variable, *detach, parameters)
        }
        _ => Err(unsupported(
            "clause cannot feed the atomic mutation command",
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
