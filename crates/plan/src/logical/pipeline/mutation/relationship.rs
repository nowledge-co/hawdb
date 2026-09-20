use super::*;

pub(super) fn bind_create(
    input: &MutationInput<'_>,
    pattern: &MatchPattern,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if input.patterns.is_empty() && pattern.steps.is_empty() {
        require_node_label(&pattern.first)?;
        return Ok(LogicalPlan::CreateNode {
            label: pattern.first.label.clone(),
            properties: bind_properties(&pattern.first.properties, parameters)?,
        });
    }
    let (relationship, target) = one_hop(pattern, "CREATE")?;
    if input.patterns.is_empty() {
        require_node_label(&pattern.first)?;
        require_node_label(target)?;
        return Ok(LogicalPlan::CreateRelationship {
            source_label: pattern.first.label.clone(),
            source_properties: bind_properties(&pattern.first.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: target.label.clone(),
            target_properties: bind_properties(&target.properties, parameters)?,
        });
    }
    let (source, target) = bound_endpoints(input, &pattern.first, target)?;
    let (source_properties, target_properties) = bound_filters(input, source, target, parameters)?;
    Ok(LogicalPlan::CreateMatchedRelationship {
        source_label: source.label.clone(),
        source_properties,
        target_label: target.label.clone(),
        target_properties,
        rel_type: relationship.rel_type.clone(),
        rel_properties: bind_properties(&relationship.properties, parameters)?,
    })
}

pub(super) fn bind_merge(
    input: &MutationInput<'_>,
    pattern: &MatchPattern,
    on_create: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let (relationship, target) = one_hop(pattern, "MERGE")?;
    if input.patterns.is_empty() {
        require_node_label(&pattern.first)?;
        require_node_label(target)?;
        if !on_create.is_empty() {
            return Err(unsupported(
                "connected-node MERGE does not support ON CREATE assignments",
            ));
        }
        return Ok(LogicalPlan::MergeRelationship {
            source_label: pattern.first.label.clone(),
            source_properties: bind_properties(&pattern.first.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: target.label.clone(),
            target_properties: bind_properties(&target.properties, parameters)?,
        });
    }
    if input
        .patterns
        .iter()
        .all(|pattern| pattern.steps.is_empty())
    {
        let (source, target) = bound_endpoints(input, &pattern.first, target)?;
        let (source_properties, target_properties) =
            bound_filters(input, source, target, parameters)?;
        let on_create_properties = bind_relationship_on_create_set_properties(
            relationship.variable.as_deref(),
            on_create,
            parameters,
        )?;
        return Ok(LogicalPlan::MergeMatchedRelationship {
            source_label: source.label.clone(),
            source_properties,
            target_label: target.label.clone(),
            target_properties,
            rel_type: relationship.rel_type.clone(),
            rel_match_properties: bind_properties(&relationship.properties, parameters)?,
            on_create_properties,
        });
    }
    require_reference(&pattern.first)?;
    require_reference(target)?;
    let mut matched_edges = input
        .patterns
        .iter()
        .filter(|pattern| !pattern.steps.is_empty());
    let matched = matched_edges
        .next()
        .ok_or_else(|| unsupported("MERGE requires a bound relationship"))?;
    if matched_edges.next().is_some() {
        return Err(unsupported(
            "atomic relationship MERGE supports one matched relationship",
        ));
    }
    let source = &matched.first;
    let (old_relationship, old_target) = one_hop(matched, "MERGE")?;
    if input.patterns.len() == 1 {
        if pattern.first.variable != source.variable || target.variable != old_target.variable {
            return Err(unsupported(
                "relationship-copy MERGE must preserve the bound endpoints",
            ));
        }
        if !input.predicates.is_empty() {
            return Err(unsupported(
                "relationship-copy MERGE does not support WHERE predicates",
            ));
        }
        let on_create_properties = bind_relationship_copy_on_create_set_properties(
            relationship.variable.as_deref(),
            old_relationship.variable.as_deref(),
            on_create,
            parameters,
        )?;
        return Ok(LogicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label: source.label.clone(),
            source_properties: bind_properties(&source.properties, parameters)?,
            old_rel_variable: old_relationship.variable.clone(),
            old_rel_type: old_relationship.rel_type.clone(),
            old_rel_properties: bind_properties(&old_relationship.properties, parameters)?,
            target_label: old_target.label.clone(),
            target_properties: bind_properties(&old_target.properties, parameters)?,
            new_rel_type: relationship.rel_type.clone(),
            new_rel_match_properties: bind_properties(&relationship.properties, parameters)?,
            on_create_properties,
        });
    }
    let independent = input
        .patterns
        .iter()
        .filter(|pattern| pattern.steps.is_empty())
        .collect::<Vec<_>>();
    let [independent] = independent.as_slice() else {
        return Err(unsupported(
            "relationship retarget MERGE requires one independent endpoint match",
        ));
    };
    let new_node = &independent.first;
    let (source_properties, new_properties) = bound_filters(input, source, new_node, parameters)?;
    let on_create_properties = bind_relationship_on_create_set_properties(
        relationship.variable.as_deref(),
        on_create,
        parameters,
    )?;
    let old_rel_properties = bind_properties(&old_relationship.properties, parameters)?;
    let old_target_properties = bind_properties(&old_target.properties, parameters)?;
    let new_rel_match_properties = bind_properties(&relationship.properties, parameters)?;
    if pattern.first.variable == source.variable && target.variable == new_node.variable {
        Ok(LogicalPlan::MergeRelationshipToMatchedTarget {
            source_label: source.label.clone(),
            source_properties,
            old_rel_type: old_relationship.rel_type.clone(),
            old_rel_properties,
            old_target_label: old_target.label.clone(),
            old_target_properties,
            new_target_label: new_node.label.clone(),
            new_target_properties: new_properties,
            new_rel_type: relationship.rel_type.clone(),
            new_rel_match_properties,
            on_create_properties,
        })
    } else if pattern.first.variable == new_node.variable && target.variable == old_target.variable
    {
        Ok(LogicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label: source.label.clone(),
            old_source_properties: source_properties,
            old_rel_type: old_relationship.rel_type.clone(),
            old_rel_properties,
            old_target_label: old_target.label.clone(),
            old_target_properties,
            new_source_label: new_node.label.clone(),
            new_source_properties: new_properties,
            new_rel_type: relationship.rel_type.clone(),
            new_rel_match_properties,
            on_create_properties,
        })
    } else {
        Err(unsupported(
            "relationship retarget MERGE endpoints do not reference the matched nodes",
        ))
    }
}

fn require_reference(node: &NodePattern) -> Result<()> {
    if node.anonymous || !node.label.is_empty() || !node.properties.is_empty() {
        return Err(unsupported(
            "a bound mutation endpoint must be an undecorated variable reference",
        ));
    }
    Ok(())
}

fn bound_endpoints<'a>(
    input: &'a MutationInput<'_>,
    source: &NodePattern,
    target: &NodePattern,
) -> Result<(&'a NodePattern, &'a NodePattern)> {
    require_reference(source)?;
    require_reference(target)?;
    if input.patterns.len() != 2
        || input
            .patterns
            .iter()
            .any(|pattern| !pattern.steps.is_empty())
        || source.variable == target.variable
    {
        return Err(unsupported(
            "atomic relationship creation requires two independent endpoint matches",
        ));
    }
    let lookup = |name: &str| {
        input
            .patterns
            .iter()
            .find(|pattern| pattern.first.variable == name)
            .map(|pattern| &pattern.first)
            .ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "unknown variable '{name}' in relationship mutation"
                ))
            })
    };
    Ok((lookup(&source.variable)?, lookup(&target.variable)?))
}

fn bound_filters(
    input: &MutationInput<'_>,
    source: &NodePattern,
    target: &NodePattern,
    parameters: &BTreeMap<String, Value>,
) -> Result<(BTreeMap<String, Value>, BTreeMap<String, Value>)> {
    bind_two_node_relationship_create_filters(
        &source.variable,
        &source.properties,
        &target.variable,
        &target.properties,
        input.predicate().as_ref(),
        parameters,
    )
}
