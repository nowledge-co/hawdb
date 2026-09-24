use super::*;

pub(super) fn bind_shortest_path_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let [matched, returned] = query.clauses.as_slice() else {
        return Err(unsupported(
            "ALL SHORTEST requires a MATCH followed by RETURN",
        ));
    };
    let ClauseKind::Match {
        optional: false,
        patterns,
        predicate,
    } = &matched.kind
    else {
        return Err(unsupported("ALL SHORTEST requires a non-optional MATCH"));
    };
    let ClauseKind::Return(projection) = &returned.kind else {
        return Err(unsupported("ALL SHORTEST requires a RETURN projection"));
    };
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return Err(unsupported(
            "ALL SHORTEST does not support projection modifiers",
        ));
    }
    let [pattern] = patterns.as_slice() else {
        return Err(unsupported("ALL SHORTEST requires one path pattern"));
    };
    let Some(path_variable) = &pattern.variable else {
        return Err(unsupported("ALL SHORTEST requires a path variable"));
    };
    let [step] = pattern.steps.as_slice() else {
        return Err(unsupported(
            "ALL SHORTEST requires one bounded relationship pattern",
        ));
    };
    let source = &pattern.first;
    let target = &step.target;
    let relationship = &step.relationship;
    if relationship.search != PathSearch::AllShortest {
        return Err(unsupported("path projection requires ALL SHORTEST"));
    }
    if relationship.direction == RelationshipDirection::Incoming {
        return Err(unsupported(
            "ALL SHORTEST path reads do not support incoming-only patterns",
        ));
    }
    if !source.properties.is_empty() || !target.properties.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST path reads require endpoint ids in WHERE predicates",
        ));
    }
    if relationship.min_hops == 0
        || relationship.max_hops == 0
        || relationship.min_hops > relationship.max_hops
    {
        return Err(unsupported(
            "ALL SHORTEST path reads require a finite positive hop range",
        ));
    }
    if relationship.variable.is_some() && !relationship.rel_type.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST path reads do not bind relationship variables",
        ));
    }
    if !relationship.properties.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST does not support relationship property filters",
        ));
    }
    let scope = BTreeSet::from([source.variable.clone(), target.variable.clone()]);
    let predicate = predicate.as_ref().map(|predicate| &predicate.kind);
    if let Some(predicate) = predicate {
        validate_predicate(&scope, predicate)?;
    }
    let source_id = endpoint_id_value(predicate, &source.variable, parameters, "source")?;
    let target_id = endpoint_id_value(predicate, &target.variable, parameters, "target")?;
    // Endpoint visibility predicates also constrain intermediate nodes in the executor.
    // Do not repurpose them for ordinary WHERE conditions or silently discard conditions.
    if let Some(predicate) = predicate {
        validate_endpoint_predicates(
            predicate,
            &source.variable,
            &target.variable,
            &mut BTreeSet::new(),
        )?;
    }
    let returns = projection
        .items
        .iter()
        .map(|item| {
            let (referenced_path, expression) = match &item.expression.kind {
                ReturnExpressionKind::Path(ShortestPathReturnExpression::NodePropertyList {
                    path_variable,
                    property,
                }) => (
                    path_variable,
                    ShortestPathProjectionExpression::NodePropertyList {
                        property: property.clone(),
                    },
                ),
                ReturnExpressionKind::Path(ShortestPathReturnExpression::Length {
                    path_variable,
                }) => (path_variable, ShortestPathProjectionExpression::Length),
                _ => return Err(unsupported("ALL SHORTEST requires a path projection")),
            };
            if referenced_path != path_variable {
                return Err(unsupported(
                    "shortest path projection references an unknown path",
                ));
            }
            let name = item
                .alias
                .clone()
                .ok_or_else(|| unsupported("shortest path projection requires an alias"))?;
            Ok(ShortestPathProjection { expression, name })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::ShortestPath {
        source_variable: source.variable.clone(),
        source_label: source.label.clone(),
        source_id,
        source_visibility_predicate: None,
        rel_type: relationship.rel_type.clone(),
        direction: relationship.direction,
        target_variable: target.variable.clone(),
        target_label: target.label.clone(),
        target_id,
        target_visibility_predicate: None,
        min_hops: relationship.min_hops,
        max_hops: relationship.max_hops,
        returns,
    })
}

fn validate_endpoint_predicates<'a>(
    predicate: &'a PropertyPredicate,
    source: &str,
    target: &str,
    seen: &mut BTreeSet<&'a str>,
) -> Result<()> {
    match predicate {
        PropertyPredicate::And(children) => {
            for child in children {
                validate_endpoint_predicates(child, source, target, seen)?;
            }
            Ok(())
        }
        PropertyPredicate::Eq {
            variable, property, ..
        } if (variable == source || variable == target) && property == "id" => {
            if !seen.insert(variable) {
                return Err(unsupported(
                    "ALL SHORTEST requires one id equality per endpoint",
                ));
            }
            Ok(())
        }
        _ => Err(unsupported(
            "ALL SHORTEST supports only endpoint id equalities",
        )),
    }
}

fn unsupported(message: &str) -> HawDBError {
    HawDBError::Semantic(message.to_string())
}
