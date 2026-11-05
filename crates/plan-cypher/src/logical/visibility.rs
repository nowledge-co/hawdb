// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

/// Applies a node visibility policy while preserving each clause's optional boundary.
/// The facade supplies its policy values; plan traversal belongs to the plan owner.
pub fn apply_node_visibility_predicates(
    logical: LogicalPlan,
    predicate_for: &impl Fn(&str) -> Predicate,
) -> LogicalPlan {
    match logical {
        LogicalPlan::NodeScan { variable, label } => LogicalPlan::Filter {
            predicate: predicate_for(&variable),
            input: Box::new(LogicalPlan::NodeScan { variable, label }),
        },
        LogicalPlan::NodeCartesianProduct { left, right } => LogicalPlan::NodeCartesianProduct {
            left: Box::new(apply_node_visibility_predicates(*left, predicate_for)),
            right: Box::new(apply_node_visibility_predicates(*right, predicate_for)),
        },
        LogicalPlan::GraphMatch { mut program, input } => {
            restrict_program(&mut program, predicate_for);
            LogicalPlan::GraphMatch {
                program,
                input: input
                    .map(|input| Box::new(apply_node_visibility_predicates(*input, predicate_for))),
            }
        }
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            node_visibility_predicate,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable: variable.clone(),
            label,
            property,
            column,
            optional,
            node_visibility_predicate: Some(match node_visibility_predicate {
                Some(existing) => Predicate::And(vec![existing, predicate_for(&variable)]),
                None => predicate_for(&variable),
            }),
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => {
            let target_predicate = predicate_for(&target_variable);
            LogicalPlan::Filter {
                predicate: target_predicate,
                input: Box::new(LogicalPlan::Expand {
                    source_variable,
                    source_label,
                    rel_variable,
                    rel_type,
                    rel_properties,
                    direction,
                    target_variable,
                    target_label,
                    min_hops,
                    max_hops,
                    optional,
                    input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
                }),
            }
        }
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::VectorIndexSearch(mut scan) => {
            scan.input = scan
                .input
                .map(|input| Box::new(apply_node_visibility_predicates(*input, predicate_for)));
            let visibility = predicate_for(&scan.variable);
            scan.node_visibility_predicate = Some(match scan.node_visibility_predicate {
                Some(existing) => Predicate::And(vec![existing, visibility]),
                None => visibility,
            });
            LogicalPlan::VectorIndexSearch(scan)
        }
        LogicalPlan::GraphSeed {
            query_parameter,
            label,
            variable,
            score_column,
            top_k,
            node_visibility_predicate,
        } => LogicalPlan::GraphSeed {
            query_parameter,
            label,
            score_column,
            top_k,
            node_visibility_predicate: Some(match node_visibility_predicate {
                Some(existing) => Predicate::And(vec![existing, predicate_for(&variable)]),
                None => predicate_for(&variable),
            }),
            variable,
        },
        LogicalPlan::GraphAlgorithmOutput(mut output) => {
            output.node_visibility_predicate = Some(predicate_for("node"));
            LogicalPlan::GraphAlgorithmOutput(output)
        }
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            return_node_identity,
            node_visibility_predicate: _,
        } => LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            return_node_identity,
            node_visibility_predicate: Some(predicate_for("node")),
        },
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            source_visibility_predicate: _,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            target_visibility_predicate: _,
            min_hops,
            max_hops,
            returns,
        } => LogicalPlan::ShortestPath {
            source_visibility_predicate: Some(predicate_for(&source_variable)),
            target_visibility_predicate: Some(predicate_for(&target_variable)),
            source_variable,
            source_label,
            source_id,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            min_hops,
            max_hops,
            returns,
        },
        LogicalPlan::Filter {
            mut predicate,
            input,
        } => {
            restrict_predicate(&mut predicate, predicate_for);
            LogicalPlan::Filter {
                predicate,
                input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
            }
        }
        LogicalPlan::Project { mut items, input } => {
            restrict_projections(&mut items, predicate_for);
            LogicalPlan::Project {
                items,
                input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
            }
        }
        LogicalPlan::Aggregate {
            mut group_keys,
            mut items,
            input,
        } => {
            restrict_projections(&mut group_keys, predicate_for);
            for item in &mut items {
                if let AggregateTarget::Expression(expression) = &mut item.target {
                    restrict_expression(expression, predicate_for);
                }
            }
            LogicalPlan::Aggregate {
                group_keys,
                items,
                input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
            }
        }
        LogicalPlan::UnwindRows {
            mut expression,
            variable,
            input,
        } => {
            restrict_expression(&mut expression, predicate_for);
            LogicalPlan::UnwindRows {
                expression,
                variable,
                input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
            }
        }
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Sort { mut items, input } => {
            for item in &mut items {
                if let SortKey::Expression(expression) = &mut item.key {
                    restrict_expression(expression, predicate_for);
                }
            }
            LogicalPlan::Sort {
                items,
                input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
            }
        }
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        mut other => {
            // Mutation WHERE predicates may hold pattern expressions.
            for predicate in mutation_predicates(&mut other) {
                restrict_predicate(predicate, predicate_for);
            }
            other
        }
    }
}

/// Adds the visibility of every node a match program binds to its predicate.
/// Visibility participates in matching, before an OPTIONAL clause emits its
/// null row; pattern expressions inside the predicate are restricted too.
fn restrict_program(program: &mut GraphMatchProgram, predicate_for: &impl Fn(&str) -> Predicate) {
    let variables = program
        .steps
        .iter()
        .filter_map(|step| match step {
            GraphMatchStep::Node(node) => Some(&node.variable),
            GraphMatchStep::Expand { target, .. } => Some(&target.variable),
            // Path endpoints are Node steps; intermediate nodes are
            // not bound and the path value has no visibility row.
            GraphMatchStep::ShortestPath(_) => None,
        })
        .collect::<BTreeSet<_>>();
    let mut predicates = variables
        .into_iter()
        .map(|variable| predicate_for(variable))
        .collect::<Vec<_>>();
    if let Some(mut predicate) = program.predicate.take() {
        restrict_predicate(&mut predicate, predicate_for);
        predicates.push(predicate);
    }
    program.predicate = match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Predicate::And(predicates)),
    };
    // Conjunct placement is computed at lowering from the final predicate.
    program.predicate_steps.clear();
    program.edge_predicates.clear();
}

fn restrict_projections(items: &mut [Projection], predicate_for: &impl Fn(&str) -> Predicate) {
    for item in items {
        restrict_expression(&mut item.expression, predicate_for);
    }
}

/// Restricts the match program of every pattern expression in `expression`,
/// including patterns nested in another pattern's arguments or body.
fn restrict_expression(
    expression: &mut ProjectionExpression,
    predicate_for: &impl Fn(&str) -> Predicate,
) {
    match expression {
        ProjectionExpression::Pattern(pattern) => {
            restrict_program(&mut pattern.program, predicate_for);
        }
        // An endpoint without its pattern step is read from the graph by id.
        ProjectionExpression::RelationshipEndpoint {
            endpoint,
            variable,
            endpoints: None,
        } => {
            *expression = ProjectionExpression::RelationshipValueEndpoint {
                endpoint: *endpoint,
                relationship: Box::new(ProjectionExpression::Variable {
                    variable: std::mem::take(variable),
                }),
                visibility: None,
            };
        }
        _ => {}
    }
    if let ProjectionExpression::RelationshipValueEndpoint { visibility, .. } = expression {
        let policy = predicate_for(hawdb_expression::ENDPOINT_VISIBILITY_VARIABLE);
        *visibility = Some(Box::new(match visibility.take() {
            Some(existing) => Predicate::And(vec![*existing, policy]),
            None => policy,
        }));
    }
    let _ = expression.try_for_each_child_mut(|child| {
        restrict_expression(child, predicate_for);
        Ok::<(), std::convert::Infallible>(())
    });
}

fn restrict_predicate(predicate: &mut Predicate, predicate_for: &impl Fn(&str) -> Predicate) {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            for predicate in predicates {
                restrict_predicate(predicate, predicate_for);
            }
        }
        Predicate::Not(predicate) => restrict_predicate(predicate, predicate_for),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            restrict_expression(expression, predicate_for);
            restrict_expression(value, predicate_for);
        }
        Predicate::ExpressionRegexMatch { expression, .. }
        | Predicate::ExpressionIn { expression, .. } => {
            restrict_expression(expression, predicate_for)
        }
        _ => {}
    }
}

fn mutation_predicates(plan: &mut LogicalPlan) -> Vec<&mut Predicate> {
    match plan {
        LogicalPlan::SetNodeProperty { predicate, .. }
        | LogicalPlan::SetNodeProperties { predicate, .. }
        | LogicalPlan::SetNodePropertiesReturn { predicate, .. }
        | LogicalPlan::DeleteNode { predicate, .. }
        | LogicalPlan::DeleteRelationshipTargetNodes {
            source_predicate: predicate,
            ..
        } => predicate.iter_mut().collect(),
        LogicalPlan::SetRelationshipProperty {
            predicate,
            rel_predicate,
            ..
        }
        | LogicalPlan::SetRelationshipProperties {
            predicate,
            rel_predicate,
            ..
        }
        | LogicalPlan::DeleteRelationship {
            predicate,
            rel_predicate,
            ..
        } => predicate
            .iter_mut()
            .chain(rel_predicate.iter_mut())
            .collect(),
        _ => Vec::new(),
    }
}

/// Fails closed when a plan still holds a pattern program or an endpoint read
/// by id that [`apply_node_visibility_predicates`] did not restrict with
/// `predicate_for`. Every plan node is matched exhaustively, so a new kind of
/// plan node is a compile error here until its expressions are covered.
pub fn validate_node_visibility_predicates(
    plan: &LogicalPlan,
    predicate_for: &impl Fn(&str) -> Predicate,
) -> Result<()> {
    let mut children: Vec<&LogicalPlan> = Vec::new();
    let mut predicates: Vec<&Predicate> = Vec::new();
    let mut expressions: Vec<&ProjectionExpression> = Vec::new();
    match plan {
        LogicalPlan::CreateNodeLabel { .. }
        | LogicalPlan::CreateRelationshipType { .. }
        | LogicalPlan::CreateNodeTable { .. }
        | LogicalPlan::CreateRelationshipTable { .. }
        | LogicalPlan::CreateProperty { .. }
        | LogicalPlan::AlterTableState { .. }
        | LogicalPlan::AlterPropertyState { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreateCompositeIndex { .. }
        | LogicalPlan::CreateRangeIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateUniqueConstraint { .. }
        | LogicalPlan::CreateNodePropertyExistsConstraint { .. }
        | LogicalPlan::CreateRelationshipUniqueConstraint { .. }
        | LogicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | LogicalPlan::TextSeed { .. }
        | LogicalPlan::VectorSeed { .. }
        | LogicalPlan::CreateNode { .. }
        | LogicalPlan::Unwind { .. }
        | LogicalPlan::UnwindMutation { .. }
        | LogicalPlan::LoadCsvMutation { .. }
        | LogicalPlan::LoadCsv { .. }
        | LogicalPlan::MergeNode { .. }
        | LogicalPlan::MergeRelationship { .. }
        | LogicalPlan::MergeMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipToMatchedTarget { .. }
        | LogicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | LogicalPlan::CreateRelationship { .. }
        | LogicalPlan::CreateMatchedRelationship { .. }
        | LogicalPlan::NodeScan { .. }
        | LogicalPlan::OptionalRelationshipCountSum { .. }
        | LogicalPlan::ThreadRepairStats { .. } => {}
        LogicalPlan::ProjectGraph {
            relationship_predicates,
            ..
        } => predicates.extend(relationship_predicates.values()),
        LogicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        }
        | LogicalPlan::GraphSeed {
            node_visibility_predicate,
            ..
        } => predicates.extend(node_visibility_predicate),
        LogicalPlan::GraphAlgorithmOutput(output) => {
            predicates.extend(&output.node_visibility_predicate)
        }
        LogicalPlan::VectorIndexSearch(scan) => {
            predicates.extend(&scan.node_visibility_predicate);
            children.extend(scan.input.as_deref());
        }
        LogicalPlan::UnwindRows {
            expression, input, ..
        } => {
            expressions.push(expression);
            children.push(input);
        }
        LogicalPlan::SetNodeProperty { predicate, .. }
        | LogicalPlan::SetNodeProperties { predicate, .. }
        | LogicalPlan::SetNodePropertiesReturn { predicate, .. }
        | LogicalPlan::DeleteNode { predicate, .. }
        | LogicalPlan::DeleteRelationshipTargetNodes {
            source_predicate: predicate,
            ..
        } => predicates.extend(predicate),
        LogicalPlan::SetRelationshipProperty {
            predicate,
            rel_predicate,
            ..
        }
        | LogicalPlan::SetRelationshipProperties {
            predicate,
            rel_predicate,
            ..
        }
        | LogicalPlan::DeleteRelationship {
            predicate,
            rel_predicate,
            ..
        } => predicates.extend(predicate.iter().chain(rel_predicate)),
        LogicalPlan::GraphMatch { program, input } => {
            predicates.extend(&program.predicate);
            children.extend(input.as_deref());
        }
        LogicalPlan::NodeCartesianProduct { left, right } => {
            children.push(left);
            children.push(right);
        }
        LogicalPlan::NodeColumnLookup {
            node_visibility_predicate,
            input,
            ..
        } => {
            predicates.extend(node_visibility_predicate);
            children.push(input);
        }
        LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Limit { input, .. } => children.push(input),
        LogicalPlan::ShortestPath {
            source_visibility_predicate,
            target_visibility_predicate,
            ..
        } => predicates.extend(
            source_visibility_predicate
                .iter()
                .chain(target_visibility_predicate),
        ),
        LogicalPlan::Filter { predicate, input } => {
            predicates.push(predicate);
            children.push(input);
        }
        LogicalPlan::Project { items, input } => {
            expressions.extend(items.iter().map(|item| &item.expression));
            children.push(input);
        }
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => {
            expressions.extend(group_keys.iter().map(|item| &item.expression));
            expressions.extend(items.iter().filter_map(|item| match &item.target {
                AggregateTarget::Expression(expression) => Some(expression),
                _ => None,
            }));
            children.push(input);
        }
        LogicalPlan::Sort { items, input } => {
            expressions.extend(items.iter().filter_map(|item| match &item.key {
                SortKey::Expression(expression) => Some(expression),
                _ => None,
            }));
            children.push(input);
        }
    }
    for predicate in predicates {
        validate_predicate_expressions(predicate, predicate_for)?;
    }
    for expression in expressions {
        validate_expression(expression, predicate_for)?;
    }
    for child in children {
        validate_node_visibility_predicates(child, predicate_for)?;
    }
    Ok(())
}

fn unrestricted(what: &str) -> HawDBError {
    HawDBError::Execution(format!(
        "access control could not restrict a {what}; the query is refused"
    ))
}

/// Whether `policy` is `predicate` or one of its top-level conjuncts.
fn admits(predicate: Option<&Predicate>, policy: &Predicate) -> bool {
    match predicate {
        Some(predicate) if predicate == policy => true,
        Some(Predicate::And(conjuncts)) => conjuncts
            .iter()
            .any(|conjunct| admits(Some(conjunct), policy)),
        _ => false,
    }
}

fn validate_expression(
    expression: &ProjectionExpression,
    predicate_for: &impl Fn(&str) -> Predicate,
) -> Result<()> {
    match expression {
        ProjectionExpression::Pattern(pattern) => {
            let program = &pattern.program;
            for step in &program.steps {
                let variable = match step {
                    GraphMatchStep::Node(node) => &node.variable,
                    GraphMatchStep::Expand { target, .. } => &target.variable,
                    GraphMatchStep::ShortestPath(_) => continue,
                };
                if !admits(program.predicate.as_ref(), &predicate_for(variable)) {
                    return Err(unrestricted("pattern expression"));
                }
            }
            if let Some(predicate) = &program.predicate {
                validate_predicate_expressions(predicate, predicate_for)?;
            }
        }
        ProjectionExpression::RelationshipValueEndpoint { visibility, .. } => {
            let policy = predicate_for(hawdb_expression::ENDPOINT_VISIBILITY_VARIABLE);
            if !admits(visibility.as_deref(), &policy) {
                return Err(unrestricted("relationship endpoint read"));
            }
        }
        ProjectionExpression::RelationshipEndpoint {
            endpoints: None, ..
        } => return Err(unrestricted("relationship endpoint read")),
        _ => {}
    }
    let mut result = Ok(());
    expression.all_children(|child| {
        result = validate_expression(child, predicate_for);
        result.is_ok()
    });
    result
}

fn validate_predicate_expressions(
    predicate: &Predicate,
    predicate_for: &impl Fn(&str) -> Predicate,
) -> Result<()> {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .try_for_each(|predicate| validate_predicate_expressions(predicate, predicate_for)),
        Predicate::Not(predicate) => validate_predicate_expressions(predicate, predicate_for),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            validate_expression(expression, predicate_for)?;
            validate_expression(value, predicate_for)
        }
        Predicate::ExpressionRegexMatch { expression, .. }
        | Predicate::ExpressionIn { expression, .. } => {
            validate_expression(expression, predicate_for)
        }
        Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
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
        | Predicate::PropertyIn { .. } => Ok(()),
    }
}
