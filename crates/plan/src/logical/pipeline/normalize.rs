//! Preserve conventional read operators when a generic MATCH has the same semantics.

use super::*;

mod order;

pub(super) fn normalize(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphMatch {
            program,
            input: None,
        } => {
            if let Some(plan) = initial_match(&program) {
                plan
            } else {
                LogicalPlan::GraphMatch {
                    program,
                    input: None,
                }
            }
        }
        LogicalPlan::GraphMatch { program, input } => LogicalPlan::GraphMatch {
            program,
            input: input.map(|input| Box::new(normalize(*input))),
        },
        LogicalPlan::Project { items, input } => project(items, normalize(*input)),
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(normalize(*input)),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(normalize(*input)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(normalize(*input)),
        },
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

fn initial_match(program: &GraphMatchProgram) -> Option<LogicalPlan> {
    if program.optional || !program.imports.is_empty() {
        return None;
    }
    let [GraphMatchStep::Node(first), tail @ ..] = program.steps.as_slice() else {
        return None;
    };
    // Multiple expansions in one clause have relationship-isomorphism constraints.
    // Conventional Expand operators do not enforce those across separate operators.
    if tail.len() > 1 {
        return None;
    }
    let mut introduced = BTreeSet::from([first.variable.as_str()]);
    let mut predicates = node_predicates(first);
    let mut input = LogicalPlan::NodeScan {
        variable: first.variable.clone(),
        label: first.label.clone(),
    };
    if let [GraphMatchStep::Expand {
        source,
        relationship,
        rel_type,
        properties,
        direction,
        min_hops,
        max_hops,
        target,
    }] = tail
    {
        if source != &first.variable || !introduced.insert(target.variable.as_str()) {
            return None;
        }
        if let Some(relationship) = relationship
            && !introduced.insert(relationship.as_str())
        {
            return None;
        }
        predicates.extend(node_predicates(target));
        input = LogicalPlan::Expand {
            source_variable: source.clone(),
            source_label: first.label.clone(),
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
    } else if !tail.is_empty() {
        return None;
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
