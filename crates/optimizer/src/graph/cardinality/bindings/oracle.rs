//! Original recursive metadata lookups retained only as a differential oracle.
//! Copied from main f6a8d0b153a6d557ab1bb63f6f43aa77085e3d92.
use super::super::super::{OptimizerCatalog, PhysicalPlan};
use skein_plan::NodeProjectionAccess;

// The fixture vocabulary includes every variable and property queried by its
// operators. Resolve counts independently of the production borrowed fold.
pub(super) fn bindings<'a>(
    plan: &'a PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> super::PlanBindings<'a> {
    let mut result = super::PlanBindings::default();
    for variable in super::fixtures::VARIABLES {
        if let Some(label) = physical_plan_node_label(plan, variable) {
            result.node_labels.insert(variable, label);
        }
        if let Some(rel_type) = physical_plan_relationship_type(plan, variable) {
            result.relationship_types.insert(variable, rel_type);
        }
        if let Some(count) = physical_plan_node_variable_distinct_count(plan, variable, catalog) {
            result
                .node_populations
                .insert(variable, super::NodePopulation::OracleCount(count));
        }
        for property in super::fixtures::PROPERTIES {
            if physical_plan_access_path_covers_property(plan, variable, property) {
                result.covered_properties.insert((variable, property));
            }
        }
    }
    result
}

pub(super) fn physical_plan_node_variable_distinct_count(
    plan: &PhysicalPlan,
    variable: &str,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match plan {
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            source_label,
            rel_type,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            if source_variable == variable {
                catalog
                    .bounded_path_source_distinct_count(
                        source_label,
                        rel_type,
                        target_label,
                        *min_hops,
                        *max_hops,
                    )
                    .or_else(|| {
                        catalog.path_source_distinct_count(source_label, rel_type, target_label)
                    })
                    .or_else(|| Some(catalog.label_count(source_label)))
            } else if target_variable == variable {
                catalog
                    .bounded_path_target_distinct_count(
                        source_label,
                        rel_type,
                        target_label,
                        *min_hops,
                        *max_hops,
                    )
                    .or_else(|| {
                        catalog.path_target_distinct_count(source_label, rel_type, target_label)
                    })
                    .or_else(|| Some(catalog.label_count(target_label)))
            } else {
                physical_plan_node_variable_distinct_count(input, variable, catalog)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            physical_plan_node_variable_distinct_count(left, variable, catalog)
                .or_else(|| physical_plan_node_variable_distinct_count(right, variable, catalog))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            physical_plan_node_variable_distinct_count(input, variable, catalog)
        }
        _ => physical_plan_node_label(plan, variable).map(|label| catalog.label_count(label)),
    }
}

pub(super) fn physical_plan_access_path_covers_property(
    plan: &PhysicalPlan,
    variable: &str,
    property: &str,
) -> bool {
    match plan {
        PhysicalPlan::IndexNodeSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::NodeProjectionScanExec {
            variable: plan_variable,
            access:
                NodeProjectionAccess::FullText {
                    property: plan_property,
                    ..
                },
            ..
        } => plan_variable == variable && plan_property == property,
        PhysicalPlan::IndexNodeCompositeSeek {
            variable: plan_variable,
            predicates,
            ..
        } => {
            plan_variable == variable
                && predicates
                    .iter()
                    .any(|(plan_property, _)| plan_property == property)
        }
        PhysicalPlan::IndexNodeCompositeRangeSeek {
            variable: plan_variable,
            seek,
            ..
        } => {
            plan_variable == variable
                && (seek.range_property == property
                    || seek
                        .equality_prefix
                        .iter()
                        .any(|(plan_property, _)| plan_property == property))
        }
        PhysicalPlan::IndexNodeUnionSeek {
            variable: plan_variable,
            branches,
            ..
        } => plan_variable == variable && branches.iter().any(|branch| branch.property == property),
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            physical_plan_access_path_covers_property(left, variable, property)
                || physical_plan_access_path_covers_property(right, variable, property)
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            physical_plan_access_path_covers_property(input, variable, property)
        }
        _ => false,
    }
}

pub(super) fn physical_plan_node_label<'a>(
    plan: &'a PhysicalPlan,
    variable: &str,
) -> Option<&'a str> {
    match plan {
        PhysicalPlan::SeqNodeScan {
            variable: plan_variable,
            label,
        }
        | PhysicalPlan::IndexNodeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeUnionSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeCompositeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeCompositeRangeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::NodeProjectionScanExec {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::NodeColumnLookupExec {
            variable: plan_variable,
            label,
            ..
        } if plan_variable == variable => Some(label.as_str()),
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            source_label,
            target_variable,
            target_label,
            input,
            ..
        } => {
            if source_variable == variable {
                Some(source_label.as_str())
            } else if target_variable == variable {
                Some(target_label.as_str())
            } else {
                physical_plan_node_label(input, variable)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            physical_plan_node_label(left, variable)
                .or_else(|| physical_plan_node_label(right, variable))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => physical_plan_node_label(input, variable),
        _ => None,
    }
}

pub(super) fn physical_plan_relationship_type<'a>(
    plan: &'a PhysicalPlan,
    variable: &str,
) -> Option<&'a str> {
    match plan {
        PhysicalPlan::AdjacencyExpandExec {
            rel_variable: Some(rel_variable),
            rel_type,
            input,
            ..
        } => {
            if rel_variable == variable {
                Some(rel_type.as_str())
            } else {
                physical_plan_relationship_type(input, variable)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            physical_plan_relationship_type(left, variable)
                .or_else(|| physical_plan_relationship_type(right, variable))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => physical_plan_relationship_type(input, variable),
        _ => None,
    }
}
