use super::*;
use crate::graph::costing::estimate_physical_plan_cost;
use crate::{RuleEvent, StageStats};
use skein_plan::{CompositeRangeSeek, ExactPropertySeekBranch};
use std::cell::OnceCell;

const MAX_EXACT_UNION_LOOKUP_VALUES: usize = 64;

#[cfg(test)]
thread_local! {
    static FINGERPRINT_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn candidate_fingerprint(plan: &PhysicalPlan) -> String {
    #[cfg(test)]
    FINGERPRINT_EVALUATIONS.with(|count| count.set(count.get() + 1));
    plan.instance_fingerprint()
}

struct AccessCandidate {
    plan: PhysicalPlan,
    decision: String,
    // The plan stays immutable across ranking stages. Only cost ties need this key.
    fingerprint: OnceCell<String>,
}

impl AccessCandidate {
    fn fingerprint(&self) -> &str {
        self.fingerprint
            .get_or_init(|| candidate_fingerprint(&self.plan))
    }
}

struct PhysicalCandidate {
    access: AccessCandidate,
    cost: u64,
    rule_id: &'static str,
}

fn keep_best_candidate(
    best: &mut Option<(u64, AccessCandidate)>,
    cost: u64,
    plan: PhysicalPlan,
    decision: String,
) {
    let candidate = AccessCandidate {
        plan,
        decision,
        fingerprint: OnceCell::new(),
    };
    let replace = best.as_ref().is_none_or(|(best_cost, best)| {
        cost < *best_cost || (cost == *best_cost && candidate.fingerprint() < best.fingerprint())
    });
    if replace {
        *best = Some((cost, candidate));
    }
}

fn sort_candidates(candidates: &mut [PhysicalCandidate]) {
    candidates.sort_by(|left, right| {
        left.cost
            .cmp(&right.cost)
            .then_with(|| left.access.fingerprint().cmp(right.access.fingerprint()))
            .then_with(|| left.rule_id.cmp(right.rule_id))
    });
}

pub(super) fn exact_union_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<(PhysicalPlan, String)> {
    let mut branches = Vec::<ExactPropertySeekBranch>::new();
    let mut lookup_value_count = 0usize;
    for predicate in predicates {
        let (variable, property, values) = match predicate {
            Predicate::PropertyEq {
                variable,
                property,
                value,
            } => (variable, property, std::slice::from_ref(value)),
            Predicate::PropertyIn {
                variable,
                property,
                values,
            } => (variable, property, values.as_slice()),
            _ => return None,
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            return None;
        }
        let branch = if let Some(branch) = branches
            .iter_mut()
            .find(|branch| branch.property == *property)
        {
            branch
        } else {
            branches.push(ExactPropertySeekBranch {
                property: property.clone(),
                values: Vec::new(),
            });
            branches.last_mut()?
        };
        for value in values {
            if !branch.values.contains(value) {
                branch.values.push(value.clone());
                lookup_value_count = lookup_value_count.saturating_add(1);
                if lookup_value_count > MAX_EXACT_UNION_LOOKUP_VALUES {
                    return None;
                }
            }
        }
    }
    branches.retain(|branch| !branch.values.is_empty());
    if branches.len() < 2 {
        return None;
    }

    let label_count = catalog.label_count(label);
    let scan_cost = estimate_node_full_scan_cost(label_count);
    let mut seek_cost = 0u64;
    let mut estimated_rows = 0u64;
    for branch in &branches {
        let branch_rows = catalog.estimate_property_index_in_rows(
            label,
            &branch.property,
            branch.values.len() as u64,
        );
        estimated_rows = estimated_rows.saturating_add(branch_rows);
        seek_cost = seek_cost.saturating_add(estimate_node_index_seek_cost(
            branch_rows,
            branch.values.len() as u64,
        ));
    }
    estimated_rows = estimated_rows.min(label_count).max(1);
    if !node_index_seek_is_cheaper(label_count, seek_cost) {
        return None;
    }

    let branch_count = branches.len();
    Some((
        PhysicalPlan::FilterExec {
            predicate: full_predicate.clone(),
            input: Box::new(PhysicalPlan::IndexNodeUnionSeek {
                variable: scan_variable.to_string(),
                label: label.to_string(),
                branches,
            }),
        },
        format!(
            "choose IndexNodeUnionSeek for {label}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows} branch_count={branch_count} lookup_value_count={lookup_value_count}"
        ),
    ))
}

fn composite_range_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<AccessCandidate> {
    let mut equality_values = BTreeMap::<String, Value>::new();
    let mut ranges = BTreeMap::<String, ValueRangeBounds>::new();
    for predicate in predicates {
        match predicate {
            Predicate::PropertyEq {
                variable,
                property,
                value,
            } if variable == scan_variable => {
                equality_values.insert(property.clone(), value.clone());
            }
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            } if variable == scan_variable => {
                let (candidate_lower, candidate_upper) =
                    range_bounds_for_comparison(*op, value.clone());
                let (lower, upper) = ranges.entry(property.clone()).or_default();
                merge_lower_bound(lower, candidate_lower);
                merge_upper_bound(upper, candidate_upper);
            }
            _ => {}
        }
    }

    let label_count = catalog.label_count(label);
    let scan_cost = estimate_node_full_scan_cost(label_count);
    let mut best = None;
    for index_properties in catalog.composite_property_indexes_for_label(label) {
        if index_properties.len() < 2
            || !catalog.has_composite_property_index(label, &index_properties)
        {
            continue;
        }
        let equality_prefix = index_properties
            .iter()
            .take_while(|property| equality_values.contains_key(*property))
            .map(|property| {
                (
                    property.clone(),
                    equality_values
                        .get(property)
                        .expect("checked equality prefix")
                        .clone(),
                )
            })
            .collect::<Vec<_>>();
        if equality_prefix.is_empty() || equality_prefix.len() == index_properties.len() {
            continue;
        }
        let range_property = &index_properties[equality_prefix.len()];
        let Some((lower, upper)) = ranges.get(range_property) else {
            continue;
        };
        let equality_properties = equality_prefix
            .iter()
            .map(|(property, _)| property.clone())
            .collect::<Vec<_>>();
        let estimated_rows = catalog.estimate_composite_prefix_range_rows(
            label,
            &index_properties,
            &equality_properties,
            range_property,
            lower.as_ref(),
            upper.as_ref(),
        );
        let seek_cost = estimate_node_index_seek_cost(
            estimated_rows,
            equality_prefix.len().saturating_add(1) as u64,
        );
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            continue;
        }
        let seek = CompositeRangeSeek {
            index_properties: index_properties.clone(),
            equality_prefix,
            range_property: range_property.clone(),
            lower: lower.clone(),
            upper: upper.clone(),
        };
        let decision = format!(
            "choose IndexNodeCompositeRangeSeek for {label}.{:?}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows} equality_prefix_len={}",
            index_properties,
            seek.equality_prefix.len(),
        );
        let plan = PhysicalPlan::FilterExec {
            predicate: full_predicate.clone(),
            input: Box::new(PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable: scan_variable.to_string(),
                label: label.to_string(),
                seek,
            }),
        };
        keep_best_candidate(&mut best, seek_cost, plan, decision);
    }
    best.map(|(_, candidate)| candidate)
}

pub(super) fn index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let mut candidates = Vec::new();
    let mut evaluated_rule_count = 0usize;
    let mut evaluate_candidate = |candidate, rule_id| {
        evaluated_rule_count = evaluated_rule_count.saturating_add(1);
        push_candidate(&mut candidates, candidate, rule_id, catalog);
    };
    evaluate_candidate(
        composite_range_index_seek_candidate(
            predicates,
            full_predicate,
            scan_variable,
            label,
            catalog,
        ),
        "node_composite_range_seek",
    );
    evaluate_candidate(
        composite_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog),
        "node_composite_index_seek",
    );
    evaluate_candidate(
        equality_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog),
        "node_conjunction_index_seek",
    );
    evaluate_candidate(
        range_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog),
        "node_range_index_seek",
    );

    let alternative_count = candidates.len();
    stage_events.push(ACCESS_PATH_SELECTION_STAGE.trace(
        StageStats::new(1, alternative_count).with_rule_counts(
            alternative_count,
            evaluated_rule_count.saturating_sub(alternative_count),
        ),
    ));
    sort_candidates(&mut candidates);
    let selected = candidates.into_iter().next()?;
    decisions.push(
        RuleEvent::applied(
            format!("implementation:{}", selected.rule_id),
            format!(
                "total_cost={} alternatives_considered={alternative_count}",
                selected.cost
            ),
        )
        .into_decision(),
    );
    decisions.push(format!(
        "select access path candidate: rule={} total_cost={} alternatives_considered={}",
        selected.rule_id, selected.cost, alternative_count,
    ));
    decisions.push(selected.access.decision);
    Some(selected.access.plan)
}

fn equality_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<AccessCandidate> {
    let label_count = catalog.label_count(label);
    let scan_cost = estimate_node_full_scan_cost(label_count);
    let mut best_candidate = None;
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let estimated_rows = catalog.estimate_property_index_eq_rows(label, property);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    value: value.clone(),
                }),
            };
            keep_best_candidate(&mut best_candidate, seek_cost, plan, decision);
        }
    }
    for predicate in predicates {
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let estimated_rows =
            catalog.estimate_property_index_in_rows(label, property, values.len() as u64);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeMultiSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    values: values.clone(),
                }),
            };
            keep_best_candidate(&mut best_candidate, seek_cost, plan, decision);
        }
    }
    best_candidate.map(|(_, candidate)| candidate)
}

fn composite_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<AccessCandidate> {
    let mut equality_values = BTreeMap::<String, Value>::new();
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable == scan_variable {
            equality_values.insert(property.clone(), value.clone());
        }
    }
    let mut best_candidate = None;
    for properties in catalog.composite_property_indexes_for_label(label) {
        if properties.len() < 2 || !catalog.has_composite_property_index(label, &properties) {
            continue;
        }
        let mut seek_predicates = Vec::with_capacity(properties.len());
        for property in &properties {
            let Some(value) = equality_values.get(property) else {
                seek_predicates.clear();
                break;
            };
            seek_predicates.push((property.clone(), value.clone()));
        }
        if seek_predicates.is_empty() {
            continue;
        }
        let label_count = catalog.label_count(label);
        let distinct_product = catalog.composite_distinct_count(label, &properties);
        let estimated_rows = catalog.estimate_composite_property_index_rows(label, &properties);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, properties.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeCompositeSeek for {label}.{:?}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_product={distinct_product}",
                properties
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeCompositeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    predicates: seek_predicates,
                }),
            };
            keep_best_candidate(&mut best_candidate, seek_cost, plan, decision);
        }
    }
    best_candidate.map(|(_, candidate)| candidate)
}

fn range_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<AccessCandidate> {
    let mut ranges = BTreeMap::<String, ValueRangeBounds>::new();
    for predicate in predicates {
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_range_property_index(label, property) {
            continue;
        }
        let (lower, upper) = ranges.entry(property.clone()).or_default();
        let (candidate_lower, candidate_upper) = range_bounds_for_comparison(*op, value.clone());
        merge_lower_bound(lower, candidate_lower);
        merge_upper_bound(upper, candidate_upper);
    }

    let mut best_plan: Option<(u64, AccessCandidate)> = None;
    for (property, (lower, upper)) in ranges {
        let label_count = catalog.label_count(label);
        let estimated_rows =
            catalog.estimate_range_bounds_rows(label, &property, lower.as_ref(), upper.as_ref());
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        let best_seek_cost = best_plan.as_ref().map_or(u64::MAX, |(cost, _)| *cost);
        if node_index_seek_is_cheaper(label_count, seek_cost) && seek_cost <= best_seek_cost {
            let decision = format!(
                "choose IndexNodeRangeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeRangeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    property,
                    lower,
                    upper,
                }),
            };
            keep_best_candidate(&mut best_plan, seek_cost, plan, decision);
        }
    }
    best_plan.map(|(_, candidate)| candidate)
}

fn push_candidate(
    candidates: &mut Vec<PhysicalCandidate>,
    candidate: Option<AccessCandidate>,
    rule_id: &'static str,
    catalog: &OptimizerCatalog,
) {
    if let Some(access) = candidate {
        candidates.push(PhysicalCandidate {
            cost: estimate_physical_plan_cost(&access.plan, catalog).cost,
            access,
            rule_id,
        });
    }
}

#[cfg(test)]
mod tests;
