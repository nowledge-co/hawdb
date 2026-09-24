//! Preserve conventional read operators when a generic MATCH has the same semantics.

use super::*;

mod aggregate_projection;
mod order;

pub(super) fn normalize(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphMatch {
            program,
            input: None,
        } => {
            if independent_nodes(&program, None) && program.steps.len() > 1 {
                node_product(program, None)
            } else if let Some(plan) = initial_match(&program) {
                plan
            } else {
                LogicalPlan::GraphMatch {
                    program,
                    input: None,
                }
            }
        }
        LogicalPlan::GraphMatch {
            program,
            input: Some(input),
        } => {
            let input = normalize(*input);
            if let Some(plan) = chained_match(&program, &input) {
                plan
            } else if let Some(plan) = chained_optional_match(&program, input.clone()) {
                plan
            } else if let Some(plan) = column_node_lookup(&program, input.clone()) {
                plan
            } else if independent_nodes(&program, Some(&input)) {
                node_product(program, Some(input))
            } else {
                LogicalPlan::GraphMatch {
                    program,
                    input: Some(Box::new(input)),
                }
            }
        }
        LogicalPlan::Project { items, input } => project(items, normalize(*input)),
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(normalize(*input)),
        },
        LogicalPlan::Sort { items, input } => aggregate_projection::sort(items, normalize(*input)),
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => {
            let input = normalize(*input);
            let input = if group_keys.is_empty() {
                lower_optional_count_match(&items, &input)
            } else {
                None
            }
            .unwrap_or(input);
            LogicalPlan::Aggregate {
                group_keys,
                items,
                input: Box::new(input),
            }
        }
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(normalize(*input)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(normalize(*input)),
        },
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input: Box::new(normalize(*input)),
        },
        plan => plan,
    }
}

/// A predicate-free optional one-hop MATCH over an existing source binding has
/// the same null-extending behavior as one optional Expand.
fn chained_optional_match(program: &GraphMatchProgram, input: LogicalPlan) -> Option<LogicalPlan> {
    if !program.optional || !program.imports.is_empty() || program.predicate.is_some() {
        return None;
    }
    let [GraphMatchStep::Node(source), GraphMatchStep::Expand {
        source: expand_source,
        relationship,
        rel_type,
        properties,
        direction,
        min_hops,
        max_hops,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    if expand_source != &source.variable || !source.properties.is_empty() {
        return None;
    }
    let mut introduced = BTreeSet::from([target.variable.as_str()]);
    if let Some(relationship) = relationship {
        introduced.insert(relationship);
    }
    (introduced == program.introduced.iter().map(String::as_str).collect()).then(|| {
        LogicalPlan::Expand {
            source_variable: source.variable.clone(),
            source_label: source.label.clone(),
            rel_variable: relationship.clone(),
            rel_type: rel_type.clone(),
            rel_properties: properties.clone(),
            direction: *direction,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            optional: true,
            input: Box::new(input),
        }
    })
}

/// A single-node MATCH constrained by an existing scalar can use the bounded
/// column lookup operator instead of scanning every node and filtering it.
fn column_node_lookup(program: &GraphMatchProgram, input: LogicalPlan) -> Option<LogicalPlan> {
    if !program.imports.is_empty() {
        return None;
    }
    let [GraphMatchStep::Node(node)] = program.steps.as_slice() else {
        return None;
    };
    if program.introduced.as_slice() != [node.variable.as_str()] || !node.properties.is_empty() {
        return None;
    }
    let Some(Predicate::ExpressionEq { expression, value }) = &program.predicate else {
        return None;
    };
    let ProjectionExpression::Property { variable, property } = expression else {
        return None;
    };
    let ProjectionExpression::Column(column) = value else {
        return None;
    };
    (variable == &node.variable).then(|| LogicalPlan::NodeColumnLookup {
        variable: node.variable.clone(),
        label: node.label.clone(),
        property: property.clone(),
        column: column.clone(),
        optional: program.optional,
        input: Box::new(input),
    })
}

fn independent_nodes(program: &GraphMatchProgram, input: Option<&LogicalPlan>) -> bool {
    if program.optional || !program.imports.is_empty() || program.steps.is_empty() {
        return false;
    }
    let mut variables = BTreeSet::new();
    for step in &program.steps {
        let GraphMatchStep::Node(node) = step else {
            return false;
        };
        if !variables.insert(node.variable.as_str()) {
            return false;
        }
    }
    if variables != program.introduced.iter().map(String::as_str).collect() {
        return false;
    }
    // Projection may retain stale native bindings after dropping their logical scope.
    // Only combine with inputs whose actual native bindings are fully known here.
    input.is_none_or(|input| disjoint_node_input(input, &variables))
}

fn disjoint_node_input(input: &LogicalPlan, variables: &BTreeSet<&str>) -> bool {
    match input {
        LogicalPlan::NodeScan { variable, .. } => !variables.contains(variable.as_str()),
        LogicalPlan::Filter { input, .. } => disjoint_node_input(input, variables),
        LogicalPlan::NodeCartesianProduct { left, right } => {
            disjoint_node_input(left, variables) && disjoint_node_input(right, variables)
        }
        _ => false,
    }
}

fn node_product(program: GraphMatchProgram, mut input: Option<LogicalPlan>) -> LogicalPlan {
    for step in program.steps {
        let GraphMatchStep::Node(node) = step else {
            unreachable!()
        };
        let predicate = combine_predicates(node_predicates(&node));
        let mut right = LogicalPlan::NodeScan {
            variable: node.variable,
            label: node.label,
        };
        if let Some(predicate) = predicate {
            right = LogicalPlan::Filter {
                predicate,
                input: Box::new(right),
            };
        }
        input = Some(match input {
            Some(left) => LogicalPlan::NodeCartesianProduct {
                left: Box::new(left),
                right: Box::new(right),
            },
            None => right,
        });
    }
    let mut input = input.expect("at least one independent node pattern");
    if let Some(predicate) = program.predicate {
        input = LogicalPlan::Filter {
            predicate,
            input: Box::new(input),
        };
    }
    input
}

/// Lowers a later one-hop MATCH whose source is already bound by an earlier
/// clause. The generic representation includes the source node again so the
/// binder can validate scope; the conventional plan represents that clause as
/// one more `Expand` over the prior input.
fn chained_match(program: &GraphMatchProgram, input: &LogicalPlan) -> Option<LogicalPlan> {
    if program.optional || !program.imports.is_empty() {
        return None;
    }
    let [GraphMatchStep::Node(source), GraphMatchStep::Expand {
        source: expand_source,
        relationship,
        rel_type,
        properties,
        direction,
        min_hops,
        max_hops,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    if expand_source != &source.variable {
        return None;
    }

    let mut introduced = BTreeSet::from([target.variable.as_str()]);
    if let Some(relationship) = relationship {
        introduced.insert(relationship);
    }
    if introduced != program.introduced.iter().map(String::as_str).collect() {
        return None;
    }

    let (input, prior_predicate) = match input.clone() {
        LogicalPlan::Filter { predicate, input } if reorderable_filter(&predicate) => {
            (*input, Some(predicate))
        }
        input => (input, None),
    };
    let mut output = LogicalPlan::Expand {
        source_variable: source.variable.clone(),
        source_label: source.label.clone(),
        rel_variable: relationship.clone(),
        rel_type: rel_type.clone(),
        rel_properties: properties.clone(),
        direction: *direction,
        target_variable: target.variable.clone(),
        target_label: target.label.clone(),
        min_hops: *min_hops,
        max_hops: *max_hops,
        optional: false,
        input: Box::new(input),
    };
    let mut predicates = node_predicates(source);
    predicates.extend(node_predicates(target));
    let predicate =
        combine_optional_predicates(combine_predicates(predicates), program.predicate.clone());
    let predicate = match (prior_predicate, predicate) {
        (Some(prior), Some(predicate)) => combine_predicates(vec![prior, predicate]),
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (None, None) => None,
    };
    if let Some(predicate) = predicate
        && let Some(predicate) =
            pushdown_relationship_property_eq_predicates(&mut output, predicate)
    {
        output = LogicalPlan::Filter {
            predicate,
            input: Box::new(output),
        };
    }
    Some(output)
}

fn reorderable_filter(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().all(reorderable_filter)
        }
        Predicate::Not(child) => reorderable_filter(child),
        Predicate::ConstantBool(_)
        | Predicate::IdEq { .. }
        | Predicate::IdNotEq { .. }
        | Predicate::IdCompare { .. }
        | Predicate::IdIn { .. }
        | Predicate::PropertyEq { .. }
        | Predicate::PropertyNotEq { .. }
        | Predicate::PropertyCompare { .. }
        | Predicate::PropertyListContains { .. }
        | Predicate::PropertyListContainsLower { .. }
        | Predicate::PropertyContains { .. }
        | Predicate::PropertyStartsWith { .. }
        | Predicate::PropertyEndsWith { .. }
        | Predicate::PropertyRegexMatch { .. }
        | Predicate::PropertyIsNull { .. }
        | Predicate::PropertyIsNotNull { .. }
        | Predicate::PropertyIn { .. } => true,
        Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::ExpressionEq { .. }
        | Predicate::ExpressionNotEq { .. }
        | Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. } => false,
    }
}

/// A global `COUNT` ignores unmatched rows from an OPTIONAL MATCH. It can
/// therefore use the existing Expand path when the generic program is exactly
/// one optional hop and returns no other optional bindings.
fn lower_optional_count_match(items: &[Aggregation], input: &LogicalPlan) -> Option<LogicalPlan> {
    let [Aggregation {
        function: AggregateFunction::Count,
        target: AggregateTarget::Variable(counted),
        ..
    }] = items
    else {
        return None;
    };
    if let LogicalPlan::Expand {
        source_variable,
        source_label,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops: 1,
        max_hops: 1,
        optional: true,
        input,
    } = input
    {
        let counts_relationship = rel_variable.as_deref() == Some(counted);
        let counts_target = target_variable == counted;
        if counts_relationship || counts_target {
            return Some(LogicalPlan::Expand {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                direction: *direction,
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: input.clone(),
            });
        }
    }
    let LogicalPlan::GraphMatch {
        program,
        input: Some(input),
    } = input
    else {
        return None;
    };
    if !program.optional || !program.imports.is_empty() || program.predicate.is_some() {
        return None;
    }
    let [GraphMatchStep::Node(first), GraphMatchStep::Expand {
        source,
        relationship,
        rel_type,
        properties,
        direction,
        min_hops: 1,
        max_hops: 1,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    let counts_relationship = relationship.as_deref() == Some(counted);
    let counts_target = target.variable == *counted;
    let first_is_introduced = program
        .introduced
        .iter()
        .any(|name| name == &first.variable);
    let target_is_introduced = program
        .introduced
        .iter()
        .any(|name| name == &target.variable);
    if !first.properties.is_empty() || !target.properties.is_empty() {
        return None;
    }

    if source == &first.variable
        && !first_is_introduced
        && target_is_introduced
        && (counts_relationship || counts_target)
    {
        return Some(LogicalPlan::Expand {
            source_variable: first.variable.clone(),
            source_label: first.label.clone(),
            rel_variable: relationship.clone(),
            rel_type: rel_type.clone(),
            rel_properties: properties.clone(),
            direction: *direction,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: input.clone(),
        });
    }
    if source == &first.variable
        && first_is_introduced
        && !target_is_introduced
        && counts_relationship
    {
        return Some(LogicalPlan::Expand {
            source_variable: target.variable.clone(),
            source_label: target.label.clone(),
            rel_variable: relationship.clone(),
            rel_type: rel_type.clone(),
            rel_properties: properties.clone(),
            direction: reverse_direction(*direction),
            target_variable: first.variable.clone(),
            target_label: first.label.clone(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: input.clone(),
        });
    }
    None
}

fn reverse_direction(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    }
}

fn initial_match(program: &GraphMatchProgram) -> Option<LogicalPlan> {
    if program.optional || !program.imports.is_empty() {
        return None;
    }
    let [GraphMatchStep::Node(first), tail @ ..] = program.steps.as_slice() else {
        return None;
    };
    // Multiple expansions in one clause normally have relationship-isomorphism
    // constraints that conventional Expand operators cannot express. Fixed,
    // distinct relationship types cannot alias the same relationship, so that
    // subset can retain the conventional chain shape safely.
    if tail.len() > 1 && !has_distinct_fixed_relationship_types(tail) {
        return None;
    }
    let mut introduced = BTreeSet::from([first.variable.as_str()]);
    let mut predicates = node_predicates(first);
    let mut input = LogicalPlan::NodeScan {
        variable: first.variable.clone(),
        label: first.label.clone(),
    };
    let mut source_variable = first.variable.clone();
    let mut source_label = first.label.clone();
    for step in tail {
        let GraphMatchStep::Expand {
            source,
            relationship,
            rel_type,
            properties,
            direction,
            min_hops,
            max_hops,
            target,
        } = step
        else {
            return None;
        };
        if source != &source_variable || !introduced.insert(target.variable.as_str()) {
            return None;
        }
        if let Some(relationship) = relationship
            && !introduced.insert(relationship.as_str())
        {
            return None;
        }
        predicates.extend(node_predicates(target));
        input = LogicalPlan::Expand {
            source_variable: source_variable.clone(),
            source_label,
            rel_variable: relationship.clone(),
            rel_type: rel_type.clone(),
            rel_properties: properties.clone(),
            direction: *direction,
            target_variable: target.variable.clone(),
            target_label: target.label.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            optional: false,
            input: Box::new(input),
        };
        source_variable = target.variable.clone();
        source_label = target.label.clone();
    }
    if introduced != program.introduced.iter().map(String::as_str).collect() {
        return None;
    }
    let predicate =
        combine_optional_predicates(combine_predicates(predicates), program.predicate.clone());
    if let Some(predicate) = predicate
        && let Some(predicate) = pushdown_relationship_property_eq_predicates(&mut input, predicate)
    {
        input = LogicalPlan::Filter {
            predicate,
            input: Box::new(input),
        };
    }
    Some(input)
}

fn has_distinct_fixed_relationship_types(steps: &[GraphMatchStep]) -> bool {
    let mut relationship_types = BTreeSet::new();
    steps.iter().all(|step| {
        let GraphMatchStep::Expand { rel_type, .. } = step else {
            return false;
        };
        !rel_type.is_empty() && relationship_types.insert(rel_type)
    })
}

fn node_predicates(node: &GraphMatchNode) -> Vec<Predicate> {
    node.properties
        .iter()
        .map(|(property, value)| Predicate::PropertyEq {
            variable: node.variable.clone(),
            property: property.clone(),
            value: value.clone(),
        })
        .collect()
}

fn project(projections: Vec<Projection>, input: LogicalPlan) -> LogicalPlan {
    let (projections, input) = match aggregate_projection::project(projections, input) {
        aggregate_projection::ProjectRewrite::Applied(plan) => return plan,
        aggregate_projection::ProjectRewrite::Unchanged { items, input } => (items, input),
    };
    if let Some(keys) = order::inline_keys(&projections, &input) {
        return order::remove_hidden_keys(projections.len(), input, keys);
    }
    if let LogicalPlan::Aggregate {
        group_keys, items, ..
    } = &input
    {
        let input_names = group_keys
            .iter()
            .map(|item| &item.name)
            .chain(items.iter().map(|item| &item.name));
        let mut output_names = BTreeSet::new();
        let identity = group_keys.iter().all(|item| item.name.starts_with('\0'))
            && items.iter().all(|item| item.name.starts_with('\0'))
            && projections.len() == group_keys.len() + items.len()
            && projections.iter().zip(input_names).all(|(projection, name)| {
                matches!(&projection.expression, ProjectionExpression::Column(column) if column == name)
                    && output_names.insert(projection.name.as_str())
            });
        if identity {
            let LogicalPlan::Aggregate {
                mut group_keys,
                mut items,
                input,
            } = input
            else {
                unreachable!()
            };
            for (name, projection) in group_keys
                .iter_mut()
                .map(|item| &mut item.name)
                .chain(items.iter_mut().map(|item| &mut item.name))
                .zip(projections)
            {
                *name = projection.name;
            }
            return LogicalPlan::Aggregate {
                group_keys,
                items,
                input,
            };
        }
    }
    LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    }
}
