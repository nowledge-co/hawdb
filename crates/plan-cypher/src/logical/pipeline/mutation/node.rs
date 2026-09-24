use super::*;

pub(super) fn bind_set(
    input: &MutationInput<'_>,
    sets: &[SetProperty],
    returns: Option<&[ReturnItem]>,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let pattern = input.single_pattern()?;
    let source = &pattern.first;
    let predicate = input.predicate();
    if returns.is_some() && !pattern.steps.is_empty() {
        return Err(unsupported(
            "SET RETURN supports only single-node MATCH updates",
        ));
    }
    if sets.is_empty() {
        return Err(unsupported("SET requires at least one assignment"));
    }
    if pattern.steps.is_empty() {
        let scope = BTreeSet::from([source.variable.clone()]);
        if let Some(predicate) = &predicate {
            validate_predicate(&scope, predicate)?;
        }
        let assignments = sets
            .iter()
            .map(|set| {
                if set.variable != source.variable {
                    return Err(HawDBError::Semantic(format!(
                        "unknown variable '{}' in set item",
                        set.variable
                    )));
                }
                Ok(SetAssignment {
                    property: set.property.clone(),
                    value: plan_set_value(set, parameters)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let returns = returns
            .map(|returns| {
                plan_set_node_properties_return_mode(&source.variable, returns, parameters)
            })
            .transpose()?;
        let predicate = combine_pattern_and_optional_cypher_predicate(
            &source.variable,
            &source.properties,
            predicate.as_ref(),
            &scope,
            parameters,
        )?;
        return Ok(match returns {
            Some(returns) => LogicalPlan::SetNodePropertiesReturn {
                variable: source.variable.clone(),
                label: source.label.clone(),
                predicate,
                assignments,
                returns,
            },
            None => LogicalPlan::SetNodeProperties {
                variable: source.variable.clone(),
                label: source.label.clone(),
                predicate,
                assignments,
            },
        });
    }
    let (relationship, target) = one_hop(pattern, "SET")?;
    let variable = relationship
        .variable
        .as_ref()
        .ok_or_else(|| unsupported("relationship SET requires a relationship variable"))?;
    for set in sets {
        if set.variable != *variable {
            return Err(HawDBError::Semantic(format!(
                "relationship SET can only update relationship variable '{}', got '{}'",
                variable, set.variable
            )));
        }
    }
    let mutation_predicate = plan_relationship_mutation_predicate(
        predicate.as_ref(),
        &source.variable,
        variable,
        &target.variable,
        &target.properties,
        parameters,
    )?;
    let predicate = combine_pattern_and_optional_predicate(
        &source.variable,
        &source.properties,
        mutation_predicate.source_predicate,
        parameters,
    )?;
    let mut assignments = sets
        .iter()
        .map(|set| {
            Ok(RelationshipSetAssignment {
                property: set.property.clone(),
                value: bind_relationship_set_value(&set.value, parameters)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let rel_properties = bind_properties(&relationship.properties, parameters)?;
    if assignments.len() == 1 {
        let assignment = assignments.pop().expect("one relationship assignment");
        Ok(LogicalPlan::SetRelationshipProperty {
            source_variable: source.variable.clone(),
            source_label: source.label.clone(),
            predicate,
            rel_variable: variable.clone(),
            rel_type: relationship.rel_type.clone(),
            rel_properties,
            rel_predicate: mutation_predicate.rel_predicate,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            target_properties: mutation_predicate.target_properties,
            property: assignment.property,
            value: assignment.value,
        })
    } else {
        Ok(LogicalPlan::SetRelationshipProperties {
            source_variable: source.variable.clone(),
            source_label: source.label.clone(),
            predicate,
            rel_variable: variable.clone(),
            rel_type: relationship.rel_type.clone(),
            rel_properties,
            rel_predicate: mutation_predicate.rel_predicate,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            target_properties: mutation_predicate.target_properties,
            assignments,
        })
    }
}

pub(super) fn bind_delete(
    input: &MutationInput<'_>,
    variable: &str,
    detach: bool,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let pattern = input.single_pattern()?;
    let source = &pattern.first;
    let predicate = input.predicate();
    if pattern.steps.is_empty() {
        let scope = BTreeSet::from([source.variable.clone()]);
        if let Some(predicate) = &predicate {
            validate_predicate(&scope, predicate)?;
        }
        if variable != source.variable {
            return Err(HawDBError::Semantic(format!(
                "unknown variable '{variable}' in delete item"
            )));
        }
        return Ok(LogicalPlan::DeleteNode {
            variable: variable.to_string(),
            label: source.label.clone(),
            predicate: combine_pattern_and_optional_cypher_predicate(
                &source.variable,
                &source.properties,
                predicate.as_ref(),
                &scope,
                parameters,
            )?,
            detach,
        });
    }
    let (relationship, target) = one_hop(pattern, "DELETE")?;
    if detach {
        if variable != target.variable {
            return Err(HawDBError::Semantic(format!("DETACH DELETE after relationship MATCH can only delete target variable '{}', got '{}'", target.variable, variable)));
        }
        if relationship.variable.is_some() || !relationship.properties.is_empty() {
            return Err(unsupported("DETACH DELETE after relationship MATCH does not support relationship variables or properties yet"));
        }
        let scope = BTreeSet::from([source.variable.clone()]);
        return Ok(LogicalPlan::DeleteRelationshipTargetNodes {
            source_variable: source.variable.clone(),
            source_label: source.label.clone(),
            source_predicate: combine_pattern_and_optional_cypher_predicate(
                &source.variable,
                &source.properties,
                predicate.as_ref(),
                &scope,
                parameters,
            )?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: BTreeMap::new(),
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            target_properties: bind_properties(&target.properties, parameters)?,
            detach: true,
        });
    }
    let rel_variable = relationship
        .variable
        .as_ref()
        .ok_or_else(|| unsupported("relationship DELETE requires a relationship variable"))?;
    if variable != rel_variable {
        return Err(HawDBError::Semantic(format!(
            "relationship DELETE can only delete relationship variable '{}', got '{}'",
            rel_variable, variable
        )));
    }
    let mutation_predicate = plan_relationship_mutation_predicate(
        predicate.as_ref(),
        &source.variable,
        rel_variable,
        &target.variable,
        &target.properties,
        parameters,
    )?;
    Ok(LogicalPlan::DeleteRelationship {
        source_variable: source.variable.clone(),
        source_label: source.label.clone(),
        predicate: combine_pattern_and_optional_predicate(
            &source.variable,
            &source.properties,
            mutation_predicate.source_predicate,
            parameters,
        )?,
        rel_variable: rel_variable.clone(),
        rel_type: relationship.rel_type.clone(),
        rel_properties: bind_properties(&relationship.properties, parameters)?,
        rel_predicate: mutation_predicate.rel_predicate,
        target_variable: target.variable.clone(),
        target_label: target.label.clone(),
        target_properties: mutation_predicate.target_properties,
    })
}
