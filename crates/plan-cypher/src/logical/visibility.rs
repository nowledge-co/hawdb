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
            // Visibility participates in matching, before an OPTIONAL clause emits its null row.
            let variables = program
                .steps
                .iter()
                .map(|step| match step {
                    GraphMatchStep::Node(node) => &node.variable,
                    GraphMatchStep::Expand { target, .. } => &target.variable,
                })
                .collect::<BTreeSet<_>>();
            let mut predicates = variables
                .into_iter()
                .map(|variable| predicate_for(variable))
                .collect::<Vec<_>>();
            if let Some(predicate) = program.predicate.take() {
                predicates.push(predicate);
            }
            program.predicate = match predicates.len() {
                0 => None,
                1 => predicates.pop(),
                _ => Some(Predicate::And(predicates)),
            };
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
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
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
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate: _,
        } => LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
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
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(apply_node_visibility_predicates(*input, predicate_for)),
        },
        other => other,
    }
}
