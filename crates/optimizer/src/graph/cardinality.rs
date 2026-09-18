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

use super::{OptimizerCatalog, PhysicalPlan};
use crate::cardinality_defaults::{
    AGGREGATE_GROUPS_DIVISOR, CONTAINS_SELECTIVITY_DIVISOR, ENDS_WITH_SELECTIVITY_DIVISOR,
    FILTER_SELECTIVITY_DIVISOR, FULL_TEXT_SELECTIVITY_DIVISOR, STARTS_WITH_SELECTIVITY_DIVISOR,
};
use hawdb_core::Value;
use hawdb_cypher::RelationshipDirection;
use hawdb_plan::{AggregateTarget, Aggregation, Predicate, Projection, ProjectionExpression};
use std::collections::{BTreeMap, BTreeSet};

mod bindings;
pub(super) use bindings::PlanBindings;

#[cfg(test)]
thread_local! {
    static METADATA_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn take_metadata_visits() -> usize {
    METADATA_VISITS.with(|count| count.replace(0))
}

pub(super) fn estimate_full_text_rows(label_rows: u64) -> u64 {
    label_rows.div_ceil(FULL_TEXT_SELECTIVITY_DIVISOR).max(1)
}

pub(super) fn estimate_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> u64 {
    estimate_relationship_filter_rows(predicate, input, input_rows, catalog, bindings)
        .or_else(|| estimate_node_property_filter_rows(predicate, bindings, input_rows, catalog))
        .unwrap_or_else(|| input_rows.div_ceil(FILTER_SELECTIVITY_DIVISOR).max(1))
        .max(1)
}

pub(super) fn estimate_aggregate_rows(
    group_keys: &[Projection],
    input: &PlanBindings<'_>,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    if group_keys.is_empty() {
        return 1;
    }
    let mut distinct_product = 1_u64;
    for group_key in group_keys {
        let ProjectionExpression::Property { variable, property } = &group_key.expression else {
            return input_rows.div_ceil(AGGREGATE_GROUPS_DIVISOR).max(1);
        };
        let distinct_count = if let Some(label) = input.node_label(variable) {
            catalog.known_distinct_count(label, property)
        } else if let Some(rel_type) = input.relationship_type(variable) {
            catalog.known_rel_property_distinct_count(rel_type, property)
        } else {
            None
        };
        let Some(distinct_count) = distinct_count else {
            return input_rows.div_ceil(AGGREGATE_GROUPS_DIVISOR).max(1);
        };
        distinct_product = distinct_product.saturating_mul(distinct_count.max(1));
    }
    input_rows.min(distinct_product).max(1)
}

pub(super) fn estimate_aggregate_work_rows(
    items: &[Aggregation],
    input: &PlanBindings<'_>,
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
                    if let Some(distinct_count) = input.node_distinct_count(variable, catalog) {
                        Some(distinct_count)
                    } else {
                        input
                            .relationship_type(variable)
                            .map(|rel_type| catalog.relationship_count(rel_type))
                    }
                }
                AggregateTarget::Property { variable, property } => {
                    if let Some(label) = input.node_label(variable) {
                        catalog.known_distinct_count(label, property)
                    } else if let Some(rel_type) = input.relationship_type(variable) {
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

fn estimate_id_in_rows(values: &[Value], input_rows: u64) -> u64 {
    values
        .iter()
        .collect::<BTreeSet<_>>()
        .len()
        .min(input_rows as usize) as u64
}

fn estimate_node_property_filter_rows(
    predicate: &Predicate,
    input: &PlanBindings<'_>,
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
        Predicate::IdEq { variable, .. } => input.node_label(variable).map(|_| input_rows.min(1)),
        Predicate::IdNotEq { variable, .. } => input
            .node_label(variable)
            .map(|_| input_rows.saturating_sub(1)),
        Predicate::IdIn {
            variable, values, ..
        } => input
            .node_label(variable)
            .map(|_| estimate_id_in_rows(values, input_rows)),
        Predicate::PropertyEq {
            variable, property, ..
        } => {
            if input.covers_property(variable, property) {
                None
            } else {
                input
                    .node_label(variable)
                    .map(|label| catalog.estimate_property_eq_rows(label, property, input_rows))
            }
        }
        Predicate::PropertyNotEq {
            variable, property, ..
        } => input
            .node_label(variable)
            .map(|label| catalog.estimate_property_not_eq_rows(label, property, input_rows)),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            if input.covers_property(variable, property) {
                None
            } else {
                input.node_label(variable).map(|label| {
                    catalog.estimate_property_range_rows(label, property, *op, value, input_rows)
                })
            }
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            if input.covers_property(variable, property) {
                None
            } else {
                input.node_label(variable).map(|label| {
                    catalog.estimate_property_in_rows(label, property, values, input_rows)
                })
            }
        }
        Predicate::PropertyContains {
            variable, property, ..
        } => {
            if input.covers_property(variable, property) {
                input.node_label(variable).map(|_| input_rows)
            } else {
                input.node_label(variable).map(|label| {
                    catalog.estimate_property_string_match_rows(
                        label,
                        property,
                        input_rows,
                        CONTAINS_SELECTIVITY_DIVISOR,
                    )
                })
            }
        }
        Predicate::PropertyStartsWith {
            variable, property, ..
        } => input.node_label(variable).map(|label| {
            catalog.estimate_property_string_match_rows(
                label,
                property,
                input_rows,
                STARTS_WITH_SELECTIVITY_DIVISOR,
            )
        }),
        Predicate::PropertyEndsWith {
            variable, property, ..
        } => input.node_label(variable).map(|label| {
            catalog.estimate_property_string_match_rows(
                label,
                property,
                input_rows,
                ENDS_WITH_SELECTIVITY_DIVISOR,
            )
        }),
        Predicate::PropertyIsNull { variable, property } => input
            .node_label(variable)
            .map(|label| catalog.estimate_property_null_rows(label, property, input_rows)),
        Predicate::PropertyIsNotNull { variable, property } => input
            .node_label(variable)
            .map(|label| catalog.estimate_property_not_null_rows(label, property, input_rows)),
        _ => None,
    }
}

fn exact_property_predicate_is_covered(predicate: &Predicate, input: &PlanBindings<'_>) -> bool {
    match predicate {
        Predicate::PropertyEq {
            variable, property, ..
        }
        | Predicate::PropertyIn {
            variable, property, ..
        }
        | Predicate::PropertyCompare {
            variable, property, ..
        } => input.covers_property(variable, property),
        _ => false,
    }
}

fn estimate_relationship_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> Option<u64> {
    match predicate {
        Predicate::And(predicates) => {
            let mut rows = input_rows;
            let mut matched = false;
            for predicate in predicates {
                if let Some(estimated) =
                    estimate_relationship_filter_rows(predicate, input, rows, catalog, bindings)
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
                let estimated = estimate_relationship_filter_rows(
                    predicate, input, input_rows, catalog, bindings,
                )?;
                rows = rows.saturating_add(estimated);
                if rows >= input_rows {
                    return Some(input_rows);
                }
            }
            Some(rows)
        }
        Predicate::ConstantBool(value) => Some(if *value { input_rows } else { 0 }),
        Predicate::IdEq { variable, .. } => bindings
            .relationship_type(variable)
            .map(|_| input_rows.min(1)),
        Predicate::IdNotEq { variable, .. } => bindings
            .relationship_type(variable)
            .map(|_| input_rows.saturating_sub(1)),
        Predicate::IdIn {
            variable, values, ..
        } => bindings
            .relationship_type(variable)
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
                catalog.estimate_rel_property_string_match_rows(
                    rel_type,
                    property,
                    rows,
                    CONTAINS_SELECTIVITY_DIVISOR,
                )
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
                catalog.estimate_rel_property_string_match_rows(
                    rel_type,
                    property,
                    rows,
                    STARTS_WITH_SELECTIVITY_DIVISOR,
                )
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
                catalog.estimate_rel_property_string_match_rows(
                    rel_type,
                    property,
                    rows,
                    ENDS_WITH_SELECTIVITY_DIVISOR,
                )
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
