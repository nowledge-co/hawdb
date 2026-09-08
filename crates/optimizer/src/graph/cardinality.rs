use super::{OptimizerCatalog, PhysicalPlan};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{
    AggregateTarget, Aggregation, NodeProjectionAccess, Predicate, Projection, ProjectionExpression,
};
use std::collections::{BTreeMap, BTreeSet};

// Until query-specific full-text statistics exist, use the same quarter-label
// fallback before and after projection fusion. Materialization is not selectivity.
const FULL_TEXT_SELECTIVITY_DIVISOR: u64 = 4;

pub(super) fn estimate_full_text_rows(label_rows: u64) -> u64 {
    label_rows.div_ceil(FULL_TEXT_SELECTIVITY_DIVISOR).max(1)
}

pub(super) fn estimate_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    estimate_relationship_filter_rows(predicate, input, input_rows, catalog)
        .or_else(|| estimate_node_property_filter_rows(predicate, input, input_rows, catalog))
        .unwrap_or_else(|| input_rows.div_ceil(2).max(1))
        .max(1)
}

pub(super) fn estimate_aggregate_rows(
    group_keys: &[Projection],
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    if group_keys.is_empty() {
        return 1;
    }
    let mut distinct_product = 1_u64;
    for group_key in group_keys {
        let ProjectionExpression::Property { variable, property } = &group_key.expression else {
            return input_rows.div_ceil(4).max(1);
        };
        let distinct_count = if let Some(label) = physical_plan_node_label(input, variable) {
            catalog.known_distinct_count(label, property)
        } else if let Some(rel_type) = physical_plan_relationship_type(input, variable) {
            catalog.known_rel_property_distinct_count(rel_type, property)
        } else {
            None
        };
        let Some(distinct_count) = distinct_count else {
            return input_rows.div_ceil(4).max(1);
        };
        distinct_product = distinct_product.saturating_mul(distinct_count.max(1));
    }
    input_rows.min(distinct_product).max(1)
}

pub(super) fn estimate_aggregate_work_rows(
    items: &[Aggregation],
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    let distinct_property_work = items
        .iter()
        .filter_map(|item| {
            if !item.distinct {
                return None;
            }
            match &item.target {
                AggregateTarget::Variable(variable) => {
                    if let Some(distinct_count) =
                        physical_plan_node_variable_distinct_count(input, variable, catalog)
                    {
                        Some(distinct_count)
                    } else {
                        physical_plan_relationship_type(input, variable)
                            .map(|rel_type| catalog.relationship_count(rel_type))
                    }
                }
                AggregateTarget::Property { variable, property } => {
                    if let Some(label) = physical_plan_node_label(input, variable) {
                        catalog.known_distinct_count(label, property)
                    } else if let Some(rel_type) = physical_plan_relationship_type(input, variable)
                    {
                        catalog.known_rel_property_distinct_count(rel_type, property)
                    } else {
                        None
                    }
                }
                AggregateTarget::All => None,
            }
        })
        .fold(0_u64, |sum, distinct_count| {
            sum.saturating_add(input_rows.min(distinct_count.max(1)))
        });

    input_rows.saturating_add(distinct_property_work)
}

pub(super) fn estimate_optional_degree_work(
    rel_type: &str,
    rel_properties: &BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_label: &str,
    target_properties: &BTreeMap<String, Value>,
    catalog: &OptimizerCatalog,
) -> u64 {
    let rel_count = catalog
        .rel_type_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let source_count = catalog
        .rel_type_source_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let target_count = catalog
        .rel_type_target_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let fanout = match direction {
        RelationshipDirection::Outgoing => rel_count.div_ceil(source_count).max(1),
        RelationshipDirection::Incoming => rel_count.div_ceil(target_count).max(1),
        RelationshipDirection::Undirected => rel_count
            .div_ceil(source_count)
            .saturating_add(rel_count.div_ceil(target_count))
            .max(1),
    };
    let rel_property_distinct_product = rel_properties
        .keys()
        .map(|property| {
            catalog
                .rel_property_distinct_count(rel_type, property)
                .max(1)
        })
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    let target_property_distinct_product = target_properties
        .keys()
        .map(|property| catalog.distinct_count(target_label, property).max(1))
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    fanout
        .div_ceil(rel_property_distinct_product.saturating_mul(target_property_distinct_product))
        .max(1)
        .saturating_add(1)
}

fn physical_plan_node_variable_distinct_count(
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
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
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

fn estimate_id_in_rows(values: &[Value], input_rows: u64) -> u64 {
    values
        .iter()
        .collect::<BTreeSet<_>>()
        .len()
        .min(input_rows as usize) as u64
}

fn estimate_node_property_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match predicate {
        Predicate::And(predicates) => {
            if predicates
                .iter()
                .all(|predicate| exact_property_predicate_is_covered(predicate, input))
            {
                return Some(input_rows);
            }
            let mut rows = input_rows;
            let mut matched = false;
            for predicate in predicates {
                if let Some(estimated) =
                    estimate_node_property_filter_rows(predicate, input, rows, catalog)
                {
                    rows = estimated;
                    matched = true;
                    if rows == 0 {
                        break;
                    }
                }
            }
            matched.then_some(rows)
        }
        Predicate::Or(predicates) => {
            if predicates
                .iter()
                .all(|predicate| exact_property_predicate_is_covered(predicate, input))
            {
                return Some(input_rows);
            }
            let mut rows = 0_u64;
            for predicate in predicates {
                let estimated =
                    estimate_node_property_filter_rows(predicate, input, input_rows, catalog)?;
                rows = rows.saturating_add(estimated);
                if rows >= input_rows {
                    return Some(input_rows);
                }
            }
            Some(rows)
        }
        Predicate::ConstantBool(value) => Some(if *value { input_rows } else { 0 }),
        Predicate::IdEq { variable, .. } => {
            physical_plan_node_label(input, variable).map(|_| input_rows.min(1))
        }
        Predicate::IdNotEq { variable, .. } => {
            physical_plan_node_label(input, variable).map(|_| input_rows.saturating_sub(1))
        }
        Predicate::IdIn {
            variable, values, ..
        } => physical_plan_node_label(input, variable)
            .map(|_| estimate_id_in_rows(values, input_rows)),
        Predicate::PropertyEq {
            variable, property, ..
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                None
            } else {
                physical_plan_node_label(input, variable)
                    .map(|label| catalog.estimate_property_eq_rows(label, property, input_rows))
            }
        }
        Predicate::PropertyNotEq {
            variable, property, ..
        } => physical_plan_node_label(input, variable)
            .map(|label| catalog.estimate_property_not_eq_rows(label, property, input_rows)),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                None
            } else {
                physical_plan_node_label(input, variable).map(|label| {
                    catalog.estimate_property_range_rows(label, property, *op, value, input_rows)
                })
            }
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                None
            } else {
                physical_plan_node_label(input, variable).map(|label| {
                    catalog.estimate_property_in_rows(label, property, values, input_rows)
                })
            }
        }
        Predicate::PropertyContains {
            variable, property, ..
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                physical_plan_node_label(input, variable).map(|_| input_rows)
            } else {
                physical_plan_node_label(input, variable).map(|label| {
                    catalog.estimate_property_string_match_rows(label, property, input_rows, 4)
                })
            }
        }
        Predicate::PropertyStartsWith {
            variable, property, ..
        } => physical_plan_node_label(input, variable).map(|label| {
            catalog.estimate_property_string_match_rows(label, property, input_rows, 8)
        }),
        Predicate::PropertyEndsWith {
            variable, property, ..
        } => physical_plan_node_label(input, variable).map(|label| {
            catalog.estimate_property_string_match_rows(label, property, input_rows, 6)
        }),
        Predicate::PropertyIsNull { variable, property } => {
            physical_plan_node_label(input, variable)
                .map(|label| catalog.estimate_property_null_rows(label, property, input_rows))
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            physical_plan_node_label(input, variable)
                .map(|label| catalog.estimate_property_not_null_rows(label, property, input_rows))
        }
        _ => None,
    }
}

fn exact_property_predicate_is_covered(predicate: &Predicate, input: &PhysicalPlan) -> bool {
    match predicate {
        Predicate::PropertyEq {
            variable, property, ..
        }
        | Predicate::PropertyIn {
            variable, property, ..
        }
        | Predicate::PropertyCompare {
            variable, property, ..
        } => physical_plan_access_path_covers_property(input, variable, property),
        _ => false,
    }
}

fn physical_plan_access_path_covers_property(
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
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
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

fn physical_plan_node_label<'a>(plan: &'a PhysicalPlan, variable: &str) -> Option<&'a str> {
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
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
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

fn physical_plan_relationship_type<'a>(plan: &'a PhysicalPlan, variable: &str) -> Option<&'a str> {
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
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
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

fn estimate_relationship_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match predicate {
        Predicate::And(predicates) => {
            let mut rows = input_rows;
            let mut matched = false;
            for predicate in predicates {
                if let Some(estimated) =
                    estimate_relationship_filter_rows(predicate, input, rows, catalog)
                {
                    rows = estimated;
                    matched = true;
                    if rows == 0 {
                        break;
                    }
                }
            }
            matched.then_some(rows)
        }
        Predicate::Or(predicates) => {
            let mut rows = 0_u64;
            for predicate in predicates {
                let estimated =
                    estimate_relationship_filter_rows(predicate, input, input_rows, catalog)?;
                rows = rows.saturating_add(estimated);
                if rows >= input_rows {
                    return Some(input_rows);
                }
            }
            Some(rows)
        }
        Predicate::ConstantBool(value) => Some(if *value { input_rows } else { 0 }),
        Predicate::IdEq { variable, .. } => {
            physical_plan_relationship_type(input, variable).map(|_| input_rows.min(1))
        }
        Predicate::IdNotEq { variable, .. } => {
            physical_plan_relationship_type(input, variable).map(|_| input_rows.saturating_sub(1))
        }
        Predicate::IdIn {
            variable, values, ..
        } => physical_plan_relationship_type(input, variable)
            .map(|_| estimate_id_in_rows(values, input_rows)),
        Predicate::PropertyEq {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_eq_rows(rel_type, property, rows)
            },
        ),
        Predicate::PropertyNotEq {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_not_eq_rows(rel_type, property, rows)
            },
        ),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_range_rows(rel_type, property, *op, value, rows)
            },
        ),
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_in_rows(rel_type, property, values, rows)
            },
        ),
        Predicate::PropertyContains {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 4)
            },
        ),
        Predicate::PropertyStartsWith {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 8)
            },
        ),
        Predicate::PropertyEndsWith {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 6)
            },
        ),
        Predicate::PropertyIsNull { variable, property } => {
            estimate_relationship_property_filter_rows(
                input,
                variable,
                property,
                input_rows,
                catalog,
                |catalog, rel_type, property, rows| {
                    catalog.estimate_rel_property_null_rows(rel_type, property, rows)
                },
            )
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            estimate_relationship_property_filter_rows(
                input,
                variable,
                property,
                input_rows,
                catalog,
                |catalog, rel_type, property, rows| {
                    catalog.estimate_rel_property_not_null_rows(rel_type, property, rows)
                },
            )
        }
        _ => None,
    }
}

fn estimate_relationship_property_filter_rows(
    input: &PhysicalPlan,
    variable: &str,
    property: &str,
    input_rows: u64,
    catalog: &OptimizerCatalog,
    estimate: impl Fn(&OptimizerCatalog, &str, &str, u64) -> u64,
) -> Option<u64> {
    match input {
        PhysicalPlan::AdjacencyExpandExec {
            rel_variable: Some(rel_variable),
            rel_type,
            ..
        } if rel_variable == variable => Some(estimate(catalog, rel_type, property, input_rows)),
        _ => None,
    }
}
