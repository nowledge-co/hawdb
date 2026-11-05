// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Preserve conventional read operators when a generic MATCH has the same semantics.

use super::*;

mod aggregate_projection;
mod order;

pub(super) fn normalize(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::VectorIndexSearch(scan) => {
            LogicalPlan::VectorIndexSearch(Box::new((*scan).map_input(normalize)))
        }
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
        LogicalPlan::UnwindRows {
            expression,
            variable,
            input,
        } => LogicalPlan::UnwindRows {
            expression,
            variable,
            input: Box::new(normalize(*input)),
        },
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
            node_visibility_predicate,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            node_visibility_predicate,
            input: Box::new(normalize(*input)),
        },
        plan => plan,
    }
}

/// A predicate-free optional one-hop MATCH over an existing source binding has
/// the same null-extending behavior as one optional Expand.
fn chained_optional_match(program: &GraphMatchProgram, input: LogicalPlan) -> Option<LogicalPlan> {
    if !program.optional
        || !program.imports.is_empty()
        || program.predicate.is_some()
        || !introduced_bindings_are_disjoint(program, &input)
    {
        return None;
    }
    let [GraphMatchStep::Node(source), GraphMatchStep::Expand {
        source: expand_source,
        relationship,
        rel_type,
        rel_types,
        properties,
        direction,
        min_hops,
        max_hops,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    // Expand reads one relationship type; a disjunction stays a GraphMatch.
    if !rel_types.is_empty() {
        return None;
    }
    // Expand cannot encode target-node properties. A filter above an optional
    // Expand would remove the required NULL row when all endpoints are rejected.
    if expand_source != &source.variable
        || !source.properties.is_empty()
        || !target.properties.is_empty()
    {
        return None;
    }
    introduces_expand_bindings(program, &target.variable, relationship.as_deref()).then(|| {
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

fn introduces_expand_bindings(
    program: &GraphMatchProgram,
    target: &str,
    relationship: Option<&str>,
) -> bool {
    let mut introduced = BTreeSet::from([target]);
    introduced.extend(relationship);
    introduced == program.introduced.iter().map(String::as_str).collect()
}

/// A single-node MATCH constrained by an existing scalar can use the bounded
/// column lookup operator instead of scanning every node and filtering it.
///
/// A mandatory MATCH may carry further conjuncts: one `node.property = column`
/// conjunct becomes the lookup key and the others its node predicate, so
/// each upstream row no longer scans the label (#1544). The other conjuncts
/// must be infallible and must not include another equality on the node, which
/// an index could serve. An optional MATCH still requires a lone key.
fn column_node_lookup(program: &GraphMatchProgram, input: LogicalPlan) -> Option<LogicalPlan> {
    if !program.imports.is_empty() || !introduced_bindings_are_disjoint(program, &input) {
        return None;
    }
    let [GraphMatchStep::Node(node)] = program.steps.as_slice() else {
        return None;
    };
    if program.introduced.as_slice() != [node.variable.as_str()] || !node.properties.is_empty() {
        return None;
    }
    let predicate = program.predicate.as_ref()?;
    let mut conjuncts = Vec::new();
    flatten_conjuncts(predicate, &mut conjuncts);
    if program.optional && conjuncts.len() != 1 {
        return None;
    }
    let key = conjuncts
        .iter()
        .position(|conjunct| lookup_key(conjunct, &node.variable).is_some())?;
    // Planning has no index catalog. Another equality on the node may be the
    // indexed one the per-row path would seek, so keep that path (#1545).
    if conjuncts
        .iter()
        .enumerate()
        .any(|(index, conjunct)| index != key && node_equality(conjunct, &node.variable))
    {
        return None;
    }
    let (property, column) = lookup_key(conjuncts[key], &node.variable)?;
    let mut residual = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index != key)
        .map(|(_, conjunct)| conjunct.clone())
        .collect::<Vec<_>>();
    // The scan evaluates every conjunct on every candidate node, so a
    // conjunct that can fail surfaces its error even when the key matches no
    // node. Defer only conjuncts that cannot fail, keeping that contract.
    if !residual
        .iter()
        .all(|conjunct| infallible_predicate(conjunct, &node.variable))
    {
        return None;
    }
    // The remaining conjuncts become the lookup's node predicate, which it
    // evaluates on each matched node before counting the row, so a row limit
    // still stops the lookup. Visibility policies extend the same predicate.
    let node_predicate = match residual.len() {
        0 => None,
        1 => residual.pop(),
        _ => Some(Predicate::And(residual)),
    };
    Some(LogicalPlan::NodeColumnLookup {
        variable: node.variable.clone(),
        label: node.label.clone(),
        property: property.to_owned(),
        column: column.to_owned(),
        optional: program.optional,
        node_visibility_predicate: node_predicate,
        input: Box::new(input),
    })
}

fn flatten_conjuncts<'a>(predicate: &'a Predicate, conjuncts: &mut Vec<&'a Predicate>) {
    match predicate {
        Predicate::And(predicates) => {
            for predicate in predicates {
                flatten_conjuncts(predicate, conjuncts);
            }
        }
        predicate => conjuncts.push(predicate),
    }
}

/// Predicates whose evaluation cannot raise an error on any binding; anything
/// else keeps the per-row scan. Deliberately conservative: comparisons,
/// string operators and function calls are excluded.
/// Only properties of the matched node itself are admitted, so no conjunct
/// depends on another binding being declared.
fn infallible_predicate(predicate: &Predicate, node: &str) -> bool {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|predicate| infallible_predicate(predicate, node)),
        Predicate::Not(predicate) => infallible_predicate(predicate, node),
        Predicate::ConstantBool(_) => true,
        Predicate::PropertyEq { variable, .. }
        | Predicate::PropertyNotEq { variable, .. }
        | Predicate::PropertyIn { variable, .. }
        | Predicate::PropertyIsNull { variable, .. }
        | Predicate::PropertyIsNotNull { variable, .. }
        // The pattern is validated at planning; non-strings do not match.
        | Predicate::PropertyRegexMatch { variable, .. } => variable == node,
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value } => {
            infallible_expression(expression, node) && infallible_expression(value, node)
        }
        _ => false,
    }
}

fn infallible_expression(expression: &ProjectionExpression, node: &str) -> bool {
    match expression {
        ProjectionExpression::Column(_) | ProjectionExpression::Literal(_) => true,
        ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. } => variable == node,
        ProjectionExpression::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| infallible_expression(expression, node)),
        _ => false,
    }
}

/// An equality or membership test on a plain property of `node`, in either
/// orientation: the per-row path can seek such a conjunct through an index.
fn node_equality(predicate: &Predicate, node: &str) -> bool {
    let property = |expression: &ProjectionExpression| matches!(expression, ProjectionExpression::Property { variable, .. } if variable == node);
    match predicate {
        Predicate::PropertyEq { variable, .. } | Predicate::PropertyIn { variable, .. } => {
            variable == node
        }
        Predicate::ExpressionEq { expression, value } => property(expression) || property(value),
        Predicate::ExpressionIn { expression, .. } => property(expression),
        _ => false,
    }
}

/// `variable.property = column`, the only shape the lookup operator binds.
fn lookup_key<'a>(predicate: &'a Predicate, variable: &str) -> Option<(&'a str, &'a str)> {
    let Predicate::ExpressionEq { expression, value } = predicate else {
        return None;
    };
    let ProjectionExpression::Property {
        variable: bound,
        property,
    } = expression
    else {
        return None;
    };
    let ProjectionExpression::Column(column) = value else {
        return None;
    };
    (bound == variable).then_some((property.as_str(), column.as_str()))
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

// Project retains native nodes and relationships after their logical variables
// leave scope. Native operators cannot clear logically introduced bindings;
// keep GraphMatch's explicit scope transition unless absence is proven here.
fn introduced_bindings_are_disjoint(program: &GraphMatchProgram, input: &LogicalPlan) -> bool {
    let variables = program.introduced.iter().map(String::as_str).collect();
    disjoint_native_bindings(input, &variables, 0)
}

fn disjoint_native_bindings(input: &LogicalPlan, variables: &BTreeSet<&str>, depth: usize) -> bool {
    if depth >= 256 {
        return false;
    }
    let next = depth + 1;
    match input {
        LogicalPlan::NodeScan { variable, .. } => !variables.contains(variable.as_str()),
        LogicalPlan::VectorSeed { .. }
        | LogicalPlan::TextSeed { .. }
        | LogicalPlan::Unwind { .. } => true,
        LogicalPlan::GraphSeed { variable, .. } => !variables.contains(variable.as_str()),
        LogicalPlan::VectorIndexSearch(scan) => !variables.contains(scan.variable.as_str()),
        // Aggregate constructs fresh value-only bindings, including its spill
        // paths. No native entity binding survives from the input.
        LogicalPlan::Aggregate { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::UnwindRows { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::OptionalDegree { input, .. } => {
            disjoint_native_bindings(input, variables, next)
        }
        LogicalPlan::NodeColumnLookup {
            variable, input, ..
        } => {
            !variables.contains(variable.as_str())
                && disjoint_native_bindings(input, variables, next)
        }
        LogicalPlan::Expand {
            source_variable,
            target_variable,
            rel_variable,
            input,
            ..
        } => {
            !variables.contains(source_variable.as_str())
                && !variables.contains(target_variable.as_str())
                && rel_variable
                    .as_ref()
                    .is_none_or(|name| !variables.contains(name.as_str()))
                && disjoint_native_bindings(input, variables, next)
        }
        LogicalPlan::NodeCartesianProduct { left, right } => {
            disjoint_native_bindings(left, variables, next)
                && disjoint_native_bindings(right, variables, next)
        }
        LogicalPlan::GraphMatch { program, input } => {
            program
                .imports
                .iter()
                .all(|import| !variables.contains(import.variable.as_str()))
                && program.steps.iter().all(|step| match step {
                    GraphMatchStep::Node(node) => !variables.contains(node.variable.as_str()),
                    GraphMatchStep::Expand {
                        source,
                        relationship,
                        target,
                        ..
                    } => {
                        !variables.contains(source.as_str())
                            && !variables.contains(target.variable.as_str())
                            && relationship
                                .as_ref()
                                .is_none_or(|name| !variables.contains(name.as_str()))
                    }
                    // A path is a value binding, never a native one.
                    GraphMatchStep::ShortestPath(_) => true,
                })
                && input
                    .as_ref()
                    .is_none_or(|input| disjoint_native_bindings(input, variables, next))
        }
        _ => false,
    }
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
    if program.optional
        || !program.imports.is_empty()
        || !introduced_bindings_are_disjoint(program, input)
    {
        return None;
    }
    let [GraphMatchStep::Node(source), GraphMatchStep::Expand {
        source: expand_source,
        relationship,
        rel_type,
        rel_types,
        properties,
        direction,
        min_hops,
        max_hops,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    // Expand reads one relationship type; a disjunction stays a GraphMatch.
    if expand_source != &source.variable || !rel_types.is_empty() {
        return None;
    }

    if !introduces_expand_bindings(program, &target.variable, relationship.as_deref()) {
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
        | Predicate::ExpressionContains { .. }
        | Predicate::ExpressionRegexMatch { .. } => false,
        Predicate::ExpressionIn { .. } => false,
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
        if (counts_relationship || counts_target)
            && disjoint_native_bindings(input, &BTreeSet::from([counted.as_str()]), 0)
        {
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
    if !program.optional
        || !program.imports.is_empty()
        || program.predicate.is_some()
        || !introduced_bindings_are_disjoint(program, input)
    {
        return None;
    }
    let [GraphMatchStep::Node(first), GraphMatchStep::Expand {
        source,
        relationship,
        rel_type,
        rel_types,
        properties,
        direction,
        min_hops: 1,
        max_hops: 1,
        target,
    }] = program.steps.as_slice()
    else {
        return None;
    };
    // Expand reads one relationship type; a disjunction stays a GraphMatch.
    if !rel_types.is_empty() {
        return None;
    }
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
        && introduces_expand_bindings(program, &target.variable, relationship.as_deref())
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
        && introduces_expand_bindings(program, &first.variable, relationship.as_deref())
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
    let expansions = tail
        .iter()
        .filter(|step| matches!(step, GraphMatchStep::Expand { .. }))
        .count();
    if expansions > 1 && !has_distinct_fixed_relationship_types(tail) {
        return None;
    }
    let mut introduced = BTreeSet::from([first.variable.as_str()]);
    let mut node_labels = BTreeMap::from([(first.variable.as_str(), first.label.as_str())]);
    let mut predicates = node_predicates(first);
    let mut input = LogicalPlan::NodeScan {
        variable: first.variable.clone(),
        label: first.label.clone(),
    };
    let mut source_variable = first.variable.clone();
    let mut source_label = first.label.clone();
    for (index, step) in tail.iter().enumerate() {
        // A later comma pattern that starts from a node this clause already
        // bound, as in `(a)-[:R]->(b), (b)-[:S]->(c)`, continues from that
        // binding. The repeated node must not narrow the label; its property
        // map becomes an ordinary conjunct like any other pattern property.
        // Only a repeated node that starts the next expansion is accepted.
        if let GraphMatchStep::Node(node) = step {
            if !matches!(
                tail.get(index + 1),
                Some(GraphMatchStep::Expand { source, .. }) if source == &node.variable
            ) {
                return None;
            }
            let label = node_labels.get(node.variable.as_str())?;
            if !node.label.is_empty() && node.label != *label {
                return None;
            }
            predicates.extend(node_predicates(node));
            source_variable = node.variable.clone();
            source_label = label.to_string();
            continue;
        }
        let GraphMatchStep::Expand {
            source,
            relationship,
            rel_type,
            rel_types,
            properties,
            direction,
            min_hops,
            max_hops,
            target,
        } = step
        else {
            return None;
        };
        // Expand reads one relationship type; a disjunction stays a GraphMatch.
        if !rel_types.is_empty() {
            return None;
        }
        if source != &source_variable || !introduced.insert(target.variable.as_str()) {
            return None;
        }
        if let Some(relationship) = relationship
            && !introduced.insert(relationship.as_str())
        {
            return None;
        }
        node_labels.insert(target.variable.as_str(), target.label.as_str());
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
    steps.iter().all(|step| match step {
        GraphMatchStep::Expand {
            rel_type,
            rel_types,
            ..
        } => rel_types.is_empty() && !rel_type.is_empty() && relationship_types.insert(rel_type),
        GraphMatchStep::Node(_) => true,
        GraphMatchStep::ShortestPath(_) => false,
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
