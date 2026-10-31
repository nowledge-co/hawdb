// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn bind(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let mutation_index = query
        .clauses
        .iter()
        .position(|clause| matches!(clause.kind, ClauseKind::Merge { .. }))
        .ok_or_else(|| unsupported("row-bound relationship mutation requires MERGE"))?;
    let ClauseKind::Merge {
        pattern,
        on_create,
        on_match,
    } = &query.clauses[mutation_index].kind
    else {
        unreachable!()
    };
    require_unbound_path(pattern)?;
    let (relationship, target) = one_hop(pattern, "MERGE")?;
    if !on_match.is_empty() {
        return Err(unsupported(
            "relationship MERGE supports only ON CREATE assignments",
        ));
    }
    let mut input = MutationInput::default();
    let mut row_variable = String::new();
    let mut rows = vec![Value::Null];
    let mut predicates = Vec::new();
    for clause in &query.clauses[..mutation_index] {
        match &clause.kind {
            ClauseKind::Match {
                optional: false,
                patterns,
                predicate,
            } => {
                if patterns
                    .iter()
                    .any(|pattern| pattern.variable.is_some() || !pattern.steps.is_empty())
                {
                    return Err(unsupported(
                        "row-bound relationship MERGE requires independent node matches",
                    ));
                }
                // A MATCH before UNWIND cannot reference the later row binding.
                for pattern in patterns {
                    bind_relationship_properties(
                        &pattern.first.properties,
                        &row_variable,
                        parameters,
                    )?;
                }
                input.patterns.extend(patterns);
                let scope = input
                    .patterns
                    .iter()
                    .map(|pattern| pattern.first.variable.as_str())
                    .collect::<BTreeSet<_>>();
                if let Some(predicate) = predicate {
                    validate_filter_scope(&predicate.kind, &scope)?;
                    predicates.push((&predicate.kind, row_variable.clone()));
                }
            }
            ClauseKind::Unwind { source, variable } if row_variable.is_empty() => {
                rows = bind_unwind_rows(source, parameters)?;
                row_variable = variable.clone();
            }
            _ => {
                return Err(unsupported(
                    "relationship MERGE input supports node MATCH and one UNWIND",
                ))
            }
        }
    }
    let (source, target) = relationship::bound_endpoints(&input, &pattern.first, target)?;
    if relationship.variable.as_deref().is_some_and(|variable| {
        variable == source.variable || variable == target.variable || variable == row_variable
    }) {
        return Err(unsupported("MERGE relationship variable is already bound"));
    }
    if !row_variable.is_empty()
        && (row_variable == source.variable || row_variable == target.variable)
    {
        return Err(unsupported(
            "UNWIND variable collides with a matched endpoint",
        ));
    }
    let mut source_properties =
        bind_relationship_properties(&source.properties, &row_variable, parameters)?;
    let mut target_properties =
        bind_relationship_properties(&target.properties, &row_variable, parameters)?;
    for (predicate, predicate_row_variable) in predicates {
        bind_filter(
            predicate,
            &source.variable,
            &mut source_properties,
            &target.variable,
            &mut target_properties,
            &predicate_row_variable,
            parameters,
        )?;
    }
    let returns = match &query.clauses[mutation_index + 1..] {
        [] => None,
        [clause] => {
            let ClauseKind::Return(projection) = &clause.kind else {
                return Err(unsupported(
                    "relationship MERGE supports only a following RETURN",
                ));
            };
            if projection.distinct
                || projection.predicate.is_some()
                || !projection.order_by.is_empty()
                || projection.offset.is_some()
                || projection.limit.is_some()
            {
                return Err(unsupported(
                    "relationship MERGE RETURN does not support result modifiers",
                ));
            }
            let scope = BTreeSet::from([source.variable.clone(), target.variable.clone()]);
            let mut items = projection
                .items
                .iter()
                .map(|item| plan_projection(&scope, item, parameters))
                .collect::<Result<Vec<_>>>()?;
            let names =
                unique_projection_names(items.iter().map(|item| item.name.clone()).collect());
            for (item, name) in items.iter_mut().zip(names) {
                item.name = name;
            }
            Some(items)
        }
        _ => {
            return Err(unsupported(
                "relationship MERGE has unsupported following clauses",
            ))
        }
    };
    let mut on_create_properties = BTreeMap::new();
    for set in on_create {
        if relationship.variable.as_deref() != Some(set.variable.as_str()) {
            return Err(unsupported(
                "ON CREATE SET must reference the merged relationship",
            ));
        }
        let value = match &set.value {
            SetValueExpression::Value(value) => {
                bind_relationship_value(value, &row_variable, parameters)?
            }
            SetValueExpression::Property { variable, property } if variable == &row_variable => {
                BatchMutationValue::RowProperty(property.clone())
            }
            _ => {
                return Err(unsupported(
                    "ON CREATE SET requires parameter or UNWIND row values",
                ))
            }
        };
        on_create_properties.insert(set.property.clone(), value);
    }
    Ok(LogicalPlan::UnwindMutation {
        rows,
        variable: row_variable.clone(),
        operation: BatchMutationOperation::MergeRelationship(Box::new(BatchRelationshipMerge {
            source_variable: source.variable.clone(),
            source_label: source.label.clone(),
            source_properties,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            target_properties,
            rel_type: relationship.rel_type.clone(),
            rel_match_properties: bind_relationship_properties(
                &relationship.properties,
                &row_variable,
                parameters,
            )?,
            on_create_properties,
            returns,
        })),
    })
}

fn validate_filter_scope(predicate: &PropertyPredicate, scope: &BTreeSet<&str>) -> Result<()> {
    let variable = match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                validate_filter_scope(predicate, scope)?;
            }
            return Ok(());
        }
        PropertyPredicate::Eq { variable, .. } => variable,
        PropertyPredicate::ExpressionEq { expression, .. } => {
            let hawdb_cypher::ScalarExpressionKind::Property { variable, .. } = &expression.kind
            else {
                return Err(unsupported(
                    "relationship MERGE requires endpoint property equality",
                ));
            };
            variable
        }
        _ => {
            return Err(unsupported(
                "relationship MERGE supports AND-connected endpoint equalities",
            ))
        }
    };
    if !scope.contains(variable.as_str()) {
        return Err(unsupported(
            "MATCH predicate references an endpoint before it is bound",
        ));
    }
    Ok(())
}

fn bind_filter(
    predicate: &PropertyPredicate,
    source: &str,
    source_properties: &mut BTreeMap<String, BatchMutationValue>,
    target: &str,
    target_properties: &mut BTreeMap<String, BatchMutationValue>,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    use hawdb_cypher::ScalarExpressionKind as S;
    let (variable, property, value) = match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                bind_filter(
                    predicate,
                    source,
                    source_properties,
                    target,
                    target_properties,
                    row_variable,
                    parameters,
                )?;
            }
            return Ok(());
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } => (variable, property, value.clone()),
        PropertyPredicate::ExpressionEq { expression, value } => {
            let S::Property { variable, property } = &expression.kind else {
                return Err(unsupported(
                    "relationship MERGE requires endpoint property equality",
                ));
            };
            let value = match &value.kind {
                S::Variable(variable) => ValueExpressionKind::Binding(variable.clone()),
                S::Property { variable, property } => ValueExpressionKind::BindingProperty {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                S::Value(value) => value.kind.clone(),
                _ => {
                    return Err(unsupported(
                        "relationship MERGE requires endpoint property equality",
                    ))
                }
            };
            (variable, property, AstNode::synthetic(value))
        }
        _ => {
            return Err(unsupported(
                "relationship MERGE supports AND-connected endpoint equalities",
            ))
        }
    };
    let properties = if variable == source {
        source_properties
    } else if variable == target {
        target_properties
    } else {
        return Err(unsupported(
            "relationship MERGE predicate references an unknown endpoint",
        ));
    };
    let value = bind_relationship_value(&value, row_variable, parameters)?;
    if properties
        .get(property)
        .is_some_and(|existing| existing != &value)
    {
        return Err(unsupported(
            "conflicting relationship MERGE endpoint equalities",
        ));
    }
    properties.insert(property.clone(), value);
    Ok(())
}

fn bind_relationship_properties(
    properties: &BTreeMap<String, ValueExpression>,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, BatchMutationValue>> {
    properties
        .iter()
        .map(|(property, value)| {
            Ok((
                property.clone(),
                bind_relationship_value(value, row_variable, parameters)?,
            ))
        })
        .collect()
}

fn bind_relationship_value(
    value: &ValueExpression,
    row_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<BatchMutationValue> {
    match &value.kind {
        ValueExpressionKind::Binding(variable) if variable == row_variable => {
            Ok(BatchMutationValue::Row)
        }
        _ => bind_batch_value(value, row_variable, parameters),
    }
}
