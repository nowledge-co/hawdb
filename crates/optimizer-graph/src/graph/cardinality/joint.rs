// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Statistical cardinalities for small same-binding key sets. Missing data or
//! an unsupported shape retains the ordinary marginal/default model.

use super::{
    OptimizerCatalog, PlanBindings, Predicate, Projection, ProjectionExpression, StatisticsUsage,
    Value,
};
use hawdb_plan_cypher::{HashJoinKey, PhysicalPlan};
use std::collections::BTreeMap;

const MAX_KEYS: usize = 4;
const MAX_CONJUNCTS: usize = 16;

#[derive(Clone, Copy)]
pub(super) struct JointEstimate {
    pub(super) rows: u64,
    pub(super) statistics: Option<StatisticsUsage>,
}

impl JointEstimate {
    fn plain(rows: u64) -> Self {
        Self {
            rows,
            statistics: None,
        }
    }
}

pub(super) fn key_ndv(
    key: &HashJoinKey,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> Option<u64> {
    if !bindings.has_label_population(&key.variable) {
        return None;
    }
    let label = bindings.node_label(&key.variable)?;
    let ndv = catalog.known_distinct_count(label, &key.property)?;
    (ndv > 0 && ndv <= catalog.label_count(label)).then_some(ndv)
}

fn join_keys<'a>(
    predicate: &'a Predicate,
    left: &str,
    right: &str,
    count: &mut usize,
    keys: &mut BTreeMap<&'a str, &'a str>,
) -> Option<()> {
    *count += 1;
    if *count > MAX_CONJUNCTS {
        return None;
    }
    match predicate {
        Predicate::And(parts) => {
            for part in parts {
                join_keys(part, left, right, count, keys)?;
            }
        }
        Predicate::ConstantBool(true) => {}
        Predicate::ExpressionEq {
            expression:
                ProjectionExpression::Property {
                    variable: a,
                    property: a_key,
                },
            value:
                ProjectionExpression::Property {
                    variable: b,
                    property: b_key,
                },
        } => {
            let (a_key, b_key) = if a == left && b == right {
                (a_key.as_str(), b_key.as_str())
            } else if b == left && a == right {
                (b_key.as_str(), a_key.as_str())
            } else {
                return None;
            };
            if keys.get(a_key).is_some_and(|existing| *existing != b_key)
                || keys.iter().any(|(a, b)| *a != a_key && *b == b_key)
            {
                return None;
            }
            keys.insert(a_key, b_key);
            if keys.len() > MAX_KEYS {
                return None;
            }
        }
        _ => return None,
    }
    Some(())
}

pub(super) fn join_filter_estimate(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> Option<JointEstimate> {
    let PhysicalPlan::HashJoinExec {
        left_key,
        right_key,
        ..
    } = input
    else {
        return None;
    };
    let mut keys = BTreeMap::new();
    join_keys(
        predicate,
        &left_key.variable,
        &right_key.variable,
        &mut 0,
        &mut keys,
    )?;
    if keys.get(left_key.property.as_str()).copied() != Some(right_key.property.as_str()) {
        return None;
    }
    // Only suppress the ordinary residual fallback when hash costing applied
    // the mandatory key. Unknown/incompatible NDV leaves the Cartesian product.
    let left_ndv = key_ndv(left_key, catalog, bindings)?;
    let right_ndv = key_ndv(right_key, catalog, bindings)?;
    if keys.len() == 1 {
        return Some(JointEstimate::plain(input_rows));
    }
    let complete = (|| {
        let left_label = bindings.node_label(&left_key.variable)?;
        let right_label = bindings.node_label(&right_key.variable)?;
        Some((
            left_label,
            right_label,
            left_ndv,
            right_ndv,
            catalog.joint_statistics(left_label, &keys.keys().copied().collect::<Vec<_>>())?,
            catalog.joint_statistics(right_label, &keys.values().copied().collect::<Vec<_>>())?,
        ))
    })();
    let Some((left_label, right_label, left_ndv, right_ndv, left_stats, right_stats)) = complete
    else {
        // The mandatory hash key has already been applied. With no compatible
        // joint domain, do not add an unrelated default /4 selectivity for the
        // residual equality set or assume independent remaining keys.
        return Some(JointEstimate::plain(input_rows));
    };
    if left_stats.index_size != catalog.label_count(left_label)
        || right_stats.index_size != catalog.label_count(right_label)
    {
        return Some(JointEstimate::plain(input_rows));
    }
    let enforced_ndv = left_ndv.max(right_ndv);
    let joint_ndv = left_stats.distinct_count.max(right_stats.distinct_count);
    if joint_ndv < enforced_ndv {
        return Some(JointEstimate::plain(input_rows));
    }
    Some(JointEstimate {
        rows: (u128::from(input_rows) * u128::from(enforced_ndv))
            .div_ceil(u128::from(joint_ndv))
            .min(u128::from(input_rows))
            .max(1) as u64,
        statistics: Some(StatisticsUsage::JoinFilter {
            keys: keys.len(),
            left_ndv: left_stats.distinct_count,
            right_ndv: right_stats.distinct_count,
            enforced_ndv,
        }),
    })
}

fn equalities<'a>(
    predicates: &'a [Predicate],
    depth: usize,
    count: &mut usize,
    variable: &mut Option<&'a str>,
    keys: &mut BTreeMap<&'a str, &'a Value>,
) -> Option<()> {
    if depth > MAX_CONJUNCTS {
        return None;
    }
    for predicate in predicates {
        *count += 1;
        if *count > MAX_CONJUNCTS {
            return None;
        }
        match predicate {
            Predicate::And(parts) => equalities(parts, depth + 1, count, variable, keys)?,
            Predicate::PropertyEq {
                variable: source,
                property,
                value,
            } if !matches!(value, Value::Null) => {
                if variable.is_some_and(|variable| variable != source) {
                    return None;
                }
                *variable = Some(source);
                if keys
                    .get(property.as_str())
                    .is_some_and(|previous| *previous != value)
                {
                    return None;
                }
                keys.insert(property, value);
                if keys.len() > MAX_KEYS {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(())
}

pub(super) fn filter_estimate(
    predicates: &[Predicate],
    bindings: &PlanBindings<'_>,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<JointEstimate> {
    if !catalog.has_joint_statistics() {
        return None;
    }
    let mut keys = BTreeMap::new();
    let mut variable = None;
    equalities(predicates, 0, &mut 0, &mut variable, &mut keys)?;
    let variable = variable?;
    let label = bindings.node_label(variable)?;
    let properties = keys.keys().copied().collect::<Vec<_>>();
    let statistics = catalog.joint_statistics(label, &properties)?;
    let population = catalog.label_count(label);
    let covered = properties
        .iter()
        .copied()
        .filter(|property| bindings.covers_property(variable, property))
        .collect::<Vec<_>>();
    for property in &covered {
        // Range or multi-value coverage does not condition the input on one
        // equality. Even fixed access must constrain the same literal value.
        let value = bindings.fixed_access_value(variable, property)?;
        if keys[property] != value {
            return Some(JointEstimate::plain(1));
        }
    }
    if covered.len() == properties.len() {
        return Some(JointEstimate::plain(input_rows));
    }
    if statistics.index_size == population
        && properties.iter().any(|property| {
            catalog
                .known_distinct_count(label, property)
                .is_some_and(|ndv| ndv > statistics.distinct_count)
        })
    {
        return None;
    }
    let rows = if covered.is_empty() {
        (u128::from(input_rows) * u128::from(statistics.index_size))
            .div_ceil(u128::from(population) * u128::from(statistics.distinct_count))
    } else {
        // Conditional NDV requires the same complete non-NULL population.
        // A sparse full-key sample cannot establish this conditional domain.
        if statistics.index_size != population {
            return None;
        }
        let covered_ndv = if let [property] = covered.as_slice() {
            catalog.known_distinct_count(label, property)?
        } else {
            let subset = catalog.joint_statistics(label, &covered)?;
            if subset.index_size != population {
                return None;
            }
            subset.distinct_count
        };
        if covered_ndv == 0 || covered_ndv > statistics.distinct_count {
            return None;
        }
        (u128::from(input_rows) * u128::from(covered_ndv))
            .div_ceil(u128::from(statistics.distinct_count))
    };
    // Input rows can repeat a node after a join; source-population frequency
    // estimates are not row-count upper bounds. Sparse-key populations also
    // differ from a whole-column marginal domain. Bound by the actual input
    // cardinality, not a second independent marginal estimate.
    Some(JointEstimate {
        rows: rows.min(u128::from(input_rows)).max(1) as u64,
        statistics: Some(StatisticsUsage::Filter {
            keys: properties.len(),
            conditioned: covered.len(),
            population,
            indexed: statistics.index_size,
            ndv: statistics.distinct_count,
        }),
    })
}

pub(super) fn group_estimate(
    group_keys: &[Projection],
    bindings: &PlanBindings<'_>,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<JointEstimate> {
    if !catalog.has_joint_statistics() || !(2..=MAX_KEYS).contains(&group_keys.len()) {
        return None;
    }
    let mut variable = None;
    let mut keys = Vec::new();
    for key in group_keys {
        let (source, property) = bindings.property_source(&key.expression)?;
        if variable.is_some_and(|variable| variable != source) {
            return None;
        }
        variable = Some(source);
        keys.push(property);
    }
    let variable = variable?;
    if !bindings.has_label_population(variable) {
        return None;
    }
    let label = bindings.node_label(variable)?;
    let statistics = catalog.joint_statistics(label, &keys)?;
    // Missing or NULL components are absent from the composite index. Every
    // such source row can contribute a distinct SQL/Cypher grouping tuple;
    // do not silently erase their NULL-bearing groups.
    let missing = catalog
        .label_count(label)
        .saturating_sub(statistics.index_size);
    Some(JointEstimate {
        rows: input_rows
            .min(statistics.distinct_count.saturating_add(missing))
            .max(1),
        statistics: Some(StatisticsUsage::Group {
            keys: keys.len(),
            population: catalog.label_count(label),
            indexed: statistics.index_size,
            ndv: statistics.distinct_count,
        }),
    })
}

#[cfg(test)]
fn filter_rows(
    predicates: &[Predicate],
    bindings: &PlanBindings<'_>,
    rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    filter_estimate(predicates, bindings, rows, catalog).map(|estimate| estimate.rows)
}
#[cfg(test)]
fn group_rows(
    keys: &[Projection],
    bindings: &PlanBindings<'_>,
    rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    group_estimate(keys, bindings, rows, catalog).map(|estimate| estimate.rows)
}
#[cfg(test)]
fn join_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    rows: u64,
    catalog: &OptimizerCatalog,
    bindings: &PlanBindings<'_>,
) -> Option<u64> {
    join_filter_estimate(predicate, input, rows, catalog, bindings).map(|estimate| estimate.rows)
}

#[cfg(test)]
mod tests;
