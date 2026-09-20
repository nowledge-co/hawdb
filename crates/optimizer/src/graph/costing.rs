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

use super::cardinality::{
    estimate_aggregate_rows, estimate_aggregate_work_rows, estimate_filter_rows,
    estimate_full_text_rows, estimate_optional_degree_work, PlanBindings,
};
use super::{OptimizerCatalog, PhysicalPlan, PlanCost, PlanCostBreakdown};
use hawdb_core::Value;
use hawdb_cypher::RelationshipDirection;
use hawdb_plan::{
    CompositeRangeSeek, ExactPropertySeekBranch, NodeProjectionAccess, PlanChildren,
    RelationshipCountLeg,
};
use std::collections::BTreeMap;

pub(super) const NODE_INDEX_EQ_STARTUP_COST: u64 = 1;
pub(super) const NODE_INDEX_RANGE_STARTUP_COST: u64 = 2;
pub(super) const NODE_INDEX_TEXT_STARTUP_COST: u64 = 3;

const NODE_FULL_SCAN_STARTUP_COST: u64 = 4;
const NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD: u64 = 8;
const VECTOR_SEED_TOTAL_COST_PER_ROW: u64 = 10;

#[cfg(test)]
thread_local! {
    static COST_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn take_cost_evaluations() -> usize {
    COST_EVALUATIONS.with(|count| count.replace(0))
}

pub(super) fn estimate_node_cartesian_product_cost(
    left_cost: PlanCost,
    right_cost: PlanCost,
) -> PlanCost {
    combine_node_cartesian_product_cost(
        PlanCostBreakdown::from_scalar(left_cost),
        PlanCostBreakdown::from_scalar(right_cost),
    )
    .as_plan_cost()
}

fn combine_node_cartesian_product_cost(
    left_cost: PlanCostBreakdown,
    right_cost: PlanCostBreakdown,
) -> PlanCostBreakdown {
    let rows = left_cost
        .estimated_rows
        .saturating_mul(right_cost.estimated_rows)
        .max(1);
    PlanCostBreakdown::combine_with_cpu(left_cost, right_cost, rows, rows, 0)
}

pub(super) fn estimate_node_full_scan_cost(label_count: u64) -> u64 {
    node_full_scan_work(label_count).cost
}

pub(super) fn estimate_node_index_seek_cost(estimated_rows: u64, startup_cost: u64) -> u64 {
    node_index_seek_work(estimated_rows, startup_cost).cost
}

fn node_full_scan_work(rows: u64) -> PlanCostBreakdown {
    PlanCostBreakdown::new(
        rows,
        0,
        0,
        rows.saturating_add(NODE_FULL_SCAN_STARTUP_COST),
        0,
    )
}

fn node_index_seek_work(rows: u64, startup: u64) -> PlanCostBreakdown {
    // Locating a candidate and materializing it are distinct work components.
    // Both access ranking and final costing consume this unweighted breakdown.
    PlanCostBreakdown::new(rows, rows, rows.saturating_add(startup), 0, 0)
}

fn projected_access_cost(
    access: &NodeProjectionAccess,
    label: &str,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    match access {
        NodeProjectionAccess::LabelScan => {
            let rows = catalog.label_count(label).max(1);
            node_full_scan_work(rows)
        }
        NodeProjectionAccess::PropertyValues { property, values } => {
            let rows = if values.len() == 1 {
                catalog.estimate_property_index_eq_rows(label, property)
            } else {
                catalog.estimate_property_index_in_rows(label, property, values.len() as u64)
            }
            .max(1);
            node_index_seek_work(rows, values.len().max(1) as u64)
        }
        NodeProjectionAccess::PropertyUnion { branches } => {
            exact_property_union_cost(branches, label, catalog)
        }
        NodeProjectionAccess::CompositeEquality { predicates } => {
            let properties = predicates
                .iter()
                .map(|(property, _)| property.clone())
                .collect::<Vec<_>>();
            let rows = catalog
                .estimate_composite_property_index_rows(label, &properties)
                .max(1);
            node_index_seek_work(rows, predicates.len().max(1) as u64)
        }
        NodeProjectionAccess::CompositeRange { seek } => composite_range_cost(seek, label, catalog),
        NodeProjectionAccess::PropertyRange {
            property,
            lower,
            upper,
        } => {
            let rows = catalog
                .estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref())
                .max(1);
            node_index_seek_work(rows, NODE_INDEX_RANGE_STARTUP_COST)
        }
        NodeProjectionAccess::FullText { .. } => {
            let rows = estimate_full_text_rows(catalog.label_count(label));
            node_index_seek_work(rows, NODE_INDEX_TEXT_STARTUP_COST)
        }
    }
}

fn composite_range_cost(
    seek: &CompositeRangeSeek,
    label: &str,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    let equality_properties = seek
        .equality_prefix
        .iter()
        .map(|(property, _)| property.clone())
        .collect::<Vec<_>>();
    let rows = catalog.estimate_composite_prefix_range_rows(
        label,
        &seek.index_properties,
        &equality_properties,
        &seek.range_property,
        seek.lower.as_ref(),
        seek.upper.as_ref(),
    );
    node_index_seek_work(rows, seek.equality_prefix.len().saturating_add(1) as u64)
}

fn exact_property_union_cost(
    branches: &[ExactPropertySeekBranch],
    label: &str,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    let mut rows = 0u64;
    let mut cpu = 0u64;
    let mut random_io = 0u64;
    for branch in branches {
        let branch_rows = catalog.estimate_property_index_in_rows(
            label,
            &branch.property,
            branch.values.len() as u64,
        );
        rows = rows.saturating_add(branch_rows);
        let work = node_index_seek_work(branch_rows, branch.values.len().max(1) as u64);
        cpu = cpu.saturating_add(work.cpu);
        random_io = random_io.saturating_add(work.random_io);
    }
    PlanCostBreakdown::new(rows.min(catalog.label_count(label)), cpu, random_io, 0, 0)
}

pub(super) fn node_index_seek_is_cheaper(label_count: u64, seek_cost: u64) -> bool {
    label_count > NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD
        && seek_cost <= estimate_node_full_scan_cost(label_count)
}

pub(super) fn estimate_physical_plan_cost(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> PlanCost {
    estimate_physical_plan_cost_breakdown(plan, catalog).as_plan_cost()
}

pub(super) fn estimate_physical_plan_cost_breakdown(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    estimate_costed_plan(plan, catalog).cost
}

pub(super) struct CostedPlan<'a> {
    pub(super) cost: PlanCostBreakdown,
    bindings: PlanBindings<'a>,
}

fn estimate_costed_plan<'a>(plan: &'a PhysicalPlan, catalog: &OptimizerCatalog) -> CostedPlan<'a> {
    let inputs = estimate_input_costs(plan, |input| estimate_costed_plan(input, catalog));
    estimate_operator_cost(plan, catalog, inputs)
}

// Keep the cost formulas fixed while tests substitute the original recursive
// metadata queries for the production fold.
#[cfg(test)]
pub(super) fn estimate_cost_with_test_bindings<'a>(
    plan: &'a PhysicalPlan,
    catalog: &OptimizerCatalog,
    bindings: &impl Fn(&'a PhysicalPlan) -> PlanBindings<'a>,
) -> PlanCostBreakdown {
    let inputs = estimate_input_costs(plan, |input| {
        estimate_cost_with_test_bindings(input, catalog, bindings)
    });
    estimate_local_operator_cost(plan, catalog, inputs, &bindings(plan))
}

// Finish the child traversal before entering the large operator-cost match, so
// its stack frame does not accumulate with plan depth in unoptimized builds.
pub(super) fn estimate_input_costs<'a, T>(
    plan: &'a PhysicalPlan,
    mut visit: impl FnMut(&'a PhysicalPlan) -> T,
) -> [Option<T>; 2] {
    match plan.children() {
        PlanChildren::None => [None, None],
        PlanChildren::Unary(input) => [Some(visit(input)), None],
        PlanChildren::Binary(left, right) => [Some(visit(left)), Some(visit(right))],
    }
}

pub(super) fn estimate_operator_cost<'a>(
    plan: &'a PhysicalPlan,
    catalog: &OptimizerCatalog,
    inputs: [Option<CostedPlan<'a>>; 2],
) -> CostedPlan<'a> {
    let costs = inputs
        .each_ref()
        .map(|input| input.as_ref().map(|input| input.cost));
    let bindings =
        PlanBindings::for_operator(plan, inputs.map(|input| input.map(|input| input.bindings)));
    let cost = estimate_local_operator_cost(plan, catalog, costs, &bindings);
    CostedPlan { cost, bindings }
}

fn estimate_local_operator_cost(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    inputs: [Option<PlanCostBreakdown>; 2],
    bindings: &PlanBindings<'_>,
) -> PlanCostBreakdown {
    #[cfg(test)]
    COST_EVALUATIONS.with(|count| count.set(count.get() + 1));
    match plan {
        PhysicalPlan::EmptyExec => PlanCostBreakdown::new(1, 0, 0, 0, 0),
        PhysicalPlan::NodeCountExec { .. } | PhysicalPlan::RelationshipCountExec { .. } => {
            PlanCostBreakdown::new(1, 1, 0, 0, 0)
        }
        PhysicalPlan::UnwindMutation { rows, .. } => {
            let row_count = rows.len().max(1) as u64;
            PlanCostBreakdown::new(row_count, row_count, 0, 0, 0)
        }
        PhysicalPlan::SeqNodeScan { label, .. } => {
            let rows = catalog.label_count(label);
            node_full_scan_work(rows)
        }
        PhysicalPlan::NodeProjectionScanExec {
            label,
            access,
            predicate,
            items,
            ..
        } => {
            let access_cost = projected_access_cost(access, label, catalog);
            let input_rows = access_cost.estimated_rows;
            let rows = predicate.as_ref().map_or(input_rows, |predicate| {
                estimate_filter_rows(predicate, plan, input_rows, catalog, bindings).max(1)
            });
            let cpu_rows = if !items.is_empty() || predicate.is_some() {
                rows
            } else {
                0
            };
            access_cost.with_cpu(rows, cpu_rows, 0)
        }
        PhysicalPlan::SourceSegmentScan { .. } => {
            let rows = catalog.label_count("Source");
            // Persisted segment summaries add bounded range I/O before the
            // sequential candidate scan.
            PlanCostBreakdown::new(rows, 0, NODE_FULL_SCAN_STARTUP_COST, rows, 0)
        }
        PhysicalPlan::VectorSeedScan { vector_plan, .. } => {
            let rows = vector_top_k(vector_plan) as u64;
            let total_cost = rows.saturating_mul(VECTOR_SEED_TOTAL_COST_PER_ROW).max(1);
            PlanCostBreakdown::new(rows, total_cost.saturating_sub(rows), 0, 0, rows)
        }
        PhysicalPlan::HashJoinExec { .. } => {
            let left = inputs[0].expect("left input cost");
            let right = inputs[1].expect("right input cost");
            // Without cross-input key statistics, preserve the conservative
            // duplicate-heavy output bound. Hashing adds one visit per input;
            // the enclosing residual filter retains existing selectivity rules.
            let rows = left
                .estimated_rows
                .saturating_mul(right.estimated_rows)
                .max(1);
            let work = left
                .estimated_rows
                .saturating_add(right.estimated_rows)
                .saturating_add(rows);
            PlanCostBreakdown::combine_with_cpu(left, right, rows, work, 0)
        }
        PhysicalPlan::NodeCartesianProductExec { .. } => {
            let left_cost = inputs[0].expect("left input cost");
            let right_cost = inputs[1].expect("right input cost");
            combine_node_cartesian_product_cost(left_cost, right_cost)
        }
        PhysicalPlan::GraphMatchExec { program, .. } => {
            let base = inputs[0].unwrap_or_else(|| PlanCostBreakdown::new(1, 0, 0, 0, 0));
            let mut rows = base.estimated_rows;
            let mut work = rows;
            let mut bound = std::collections::BTreeSet::new();
            for step in &program.steps {
                let (node, expanding) = match step {
                    hawdb_plan::GraphMatchStep::Node(node) => (node, false),
                    hawdb_plan::GraphMatchStep::Expand { target, .. } => (target, true),
                };
                let newly_bound =
                    bound.insert(&node.variable) && program.introduced.contains(&node.variable);
                if expanding || newly_bound {
                    rows = rows.saturating_mul(catalog.label_count(&node.label).max(1));
                }
                work = work.saturating_add(rows);
            }
            base.with_random_io(rows, work, 0)
        }
        PhysicalPlan::NodeColumnLookupExec { label, .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            let label_rows = catalog.label_count(label).max(1);
            input_cost.with_random_io(
                input_cost.estimated_rows.max(1),
                input_cost.estimated_rows.saturating_mul(label_rows),
                0,
            )
        }
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        } => {
            let rows = catalog.estimate_property_index_eq_rows(label, property);
            node_index_seek_work(rows, NODE_INDEX_EQ_STARTUP_COST)
        }
        PhysicalPlan::IndexNodeMultiSeek {
            label,
            property,
            values,
            ..
        } => {
            let rows =
                catalog.estimate_property_index_in_rows(label, property, values.len() as u64);
            node_index_seek_work(rows, values.len() as u64)
        }
        PhysicalPlan::IndexNodeUnionSeek {
            label, branches, ..
        } => exact_property_union_cost(branches, label, catalog),
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => {
            let properties = predicates
                .iter()
                .map(|(property, _)| property.clone())
                .collect::<Vec<_>>();
            let rows = catalog.estimate_composite_property_index_rows(label, &properties);
            node_index_seek_work(rows, predicates.len() as u64)
        }
        PhysicalPlan::IndexNodeCompositeRangeSeek { label, seek, .. } => {
            composite_range_cost(seek, label, catalog)
        }
        PhysicalPlan::IndexNodeRangeSeek {
            label,
            property,
            lower,
            upper,
            ..
        } => {
            let rows =
                catalog.estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref());
            node_index_seek_work(rows, NODE_INDEX_RANGE_STARTUP_COST)
        }
        PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            let rows = estimate_full_text_rows(catalog.label_count(label));
            node_index_seek_work(rows, NODE_INDEX_TEXT_STARTUP_COST)
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            rel_properties,
            target_label,
            min_hops,
            max_hops,
            ..
        } => {
            let input_cost = inputs[0].expect("unary input cost");
            let expand_estimate = catalog.estimate_expand_rows(
                source_label,
                rel_type,
                rel_properties,
                target_label,
                *min_hops,
                *max_hops,
            );
            let source_rows = catalog.label_count(source_label).max(1);
            let scaled_rows = expand_estimate
                .estimated_rows
                .saturating_mul(input_cost.estimated_rows.max(1))
                .div_ceil(source_rows)
                .max(1);
            input_cost.with_random_io(
                scaled_rows,
                input_cost.estimated_rows.saturating_add(scaled_rows),
                0,
            )
        }
        PhysicalPlan::AdjacencyExistsExec { .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            input_cost.with_random_io(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input_cost = inputs[0].expect("unary input cost");
            let rows = estimate_filter_rows(
                predicate,
                input,
                input_cost.estimated_rows,
                catalog,
                bindings,
            );
            input_cost.with_cpu(rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::ProjectExec { .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            ..
        } => {
            let input_cost = inputs[0].expect("unary input cost");
            let degree_work = estimate_optional_degree_work(
                rel_type,
                rel_properties,
                *direction,
                target_label,
                target_properties,
                catalog,
            );
            input_cost.with_random_io(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(degree_work),
                0,
            )
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            ..
        } => {
            let scalar =
                estimate_optional_relationship_count_sum(label, properties, legs, catalog).cost;
            PlanCostBreakdown::from_scalar(scalar)
        }
        PhysicalPlan::ThreadRepairStatsExec { .. } => PlanCostBreakdown::new(1, 32, 0, 0, 0),
        PhysicalPlan::ShortestPathExec { max_hops, .. } => PlanCostBreakdown::new(
            1,
            0,
            (*max_hops as u64).saturating_mul(8).saturating_add(4),
            0,
            0,
        ),
        PhysicalPlan::AggregateExec {
            group_keys, items, ..
        } => {
            let input_cost = inputs[0].expect("unary input cost");
            let rows =
                estimate_aggregate_rows(group_keys, bindings, input_cost.estimated_rows, catalog);
            let work_rows =
                estimate_aggregate_work_rows(items, bindings, input_cost.estimated_rows, catalog);
            input_cost.with_cpu(rows, work_rows, 0)
        }
        PhysicalPlan::DistinctExec { .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::SortExec { .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            input_cost.with_cpu(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(2),
                0,
            )
        }
        PhysicalPlan::TopNExec { offset, limit, .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            let retained_rows =
                (offset.saturating_add(*limit) as u64).min(input_cost.estimated_rows);
            let rows = input_cost
                .estimated_rows
                .saturating_sub(*offset as u64)
                .min(*limit as u64);
            let heap_depth = retained_rows.max(2).ilog2().max(1) as u64;
            let selection_cost = input_cost
                .estimated_rows
                .saturating_add(retained_rows.saturating_mul(heap_depth));
            input_cost.with_cpu(rows, selection_cost, 0)
        }
        PhysicalPlan::LimitExec { offset, limit, .. } => {
            let input_cost = inputs[0].expect("unary input cost");
            let remaining_rows = input_cost.estimated_rows.saturating_sub(*offset as u64);
            let rows = limit
                .map(|limit| remaining_rows.min(limit as u64))
                .unwrap_or(remaining_rows)
                .max(1);
            input_cost.with_cpu(rows, rows, 0)
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => PlanCostBreakdown::new(1, 1, 0, 0, 0),
    }
}

fn vector_top_k(plan: &hawdb_plan::VectorPhysicalPlan) -> usize {
    match plan {
        hawdb_plan::VectorPhysicalPlan::TopK { limit, .. } => *limit,
        _ => 1,
    }
}

pub(super) fn push_optional_relationship_count_sum_cost_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
) {
    let estimate = estimate_optional_relationship_count_sum(label, properties, legs, catalog);
    let leg_rows = estimate
        .leg_rows
        .iter()
        .map(|leg| {
            format!(
                "{}:{}:{}",
                leg.rel_type,
                format_relationship_direction(leg.direction),
                leg.rows
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    decisions.push(format!(
        "estimate OptionalRelationshipCountSum for {label}: seed_rows={} leg_rows=[{}] estimated_rows={} cost={}",
        estimate.seed_rows, leg_rows, estimate.cost.estimated_rows, estimate.cost.cost
    ));
}

struct OptionalRelationshipCountSumEstimate {
    seed_rows: u64,
    leg_rows: Vec<OptionalRelationshipCountLegEstimate>,
    cost: PlanCost,
}

struct OptionalRelationshipCountLegEstimate {
    rel_type: String,
    direction: RelationshipDirection,
    rows: u64,
}

fn estimate_optional_relationship_count_sum(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    catalog: &OptimizerCatalog,
) -> OptionalRelationshipCountSumEstimate {
    let seed_rows = estimate_seed_rows_from_properties(label, properties, catalog);
    let leg_rows = legs
        .iter()
        .map(|leg| OptionalRelationshipCountLegEstimate {
            rel_type: leg.rel_type.clone(),
            direction: leg.direction,
            rows: estimate_relationship_count_leg_rows(seed_rows, leg, catalog),
        })
        .collect::<Vec<_>>();
    let relationship_rows = leg_rows
        .iter()
        .map(|leg| leg.rows)
        .fold(0_u64, |acc, rows| acc.saturating_add(rows));
    let cost = PlanCost {
        estimated_rows: 1,
        cost: seed_rows
            .saturating_add(relationship_rows)
            .saturating_add(legs.len() as u64)
            .saturating_add(4),
    };
    OptionalRelationshipCountSumEstimate {
        seed_rows,
        leg_rows,
        cost,
    }
}

fn format_relationship_direction(direction: RelationshipDirection) -> &'static str {
    match direction {
        RelationshipDirection::Outgoing => "out",
        RelationshipDirection::Incoming => "in",
        RelationshipDirection::Undirected => "both",
    }
}

fn estimate_seed_rows_from_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    catalog: &OptimizerCatalog,
) -> u64 {
    let label_rows = catalog.label_count(label).max(1);
    let distinct_product = properties
        .keys()
        .map(|property| catalog.distinct_count(label, property).max(1))
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    label_rows.div_ceil(distinct_product).max(1)
}

fn estimate_relationship_count_leg_rows(
    seed_rows: u64,
    leg: &RelationshipCountLeg,
    catalog: &OptimizerCatalog,
) -> u64 {
    let rel_count = catalog
        .rel_type_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let source_count = catalog
        .rel_type_source_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let target_count = catalog
        .rel_type_target_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let per_seed = match leg.direction {
        RelationshipDirection::Outgoing => rel_count.div_ceil(source_count).max(1),
        RelationshipDirection::Incoming => rel_count.div_ceil(target_count).max(1),
        RelationshipDirection::Undirected => rel_count
            .div_ceil(source_count)
            .saturating_add(rel_count.div_ceil(target_count))
            .max(1),
    };
    seed_rows.saturating_mul(per_seed).max(1)
}
