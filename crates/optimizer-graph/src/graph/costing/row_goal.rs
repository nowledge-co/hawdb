// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A bounded-output request changes consumed work, never result semantics or
//! the full cardinalities used to derive selectivity. Retain a startup/variable
//! split in the existing postorder fold: no second metadata walk or per-node
//! allocation. Uniform output distribution is an estimate, not a work budget.

use super::{
    estimate_local_operator_cost, OptimizerCatalog, PhysicalPlan, PlanBindings, PlanCostBreakdown,
};
use hawdb_plan_cypher::NodeProjectionAccess;

#[derive(Clone, Copy)]
pub(super) struct ConsumptionCost {
    pub(super) total: PlanCostBreakdown,
    pub(super) startup: PlanCostBreakdown,
}

impl ConsumptionCost {
    pub(super) fn prefix(self, rows: u64) -> PlanCostBreakdown {
        if rows >= self.total.estimated_rows {
            return self.total;
        }
        if rows == 0 {
            return zero();
        }
        let scale = |total: u64, startup: u64| {
            let startup = startup.min(total);
            let variable = (u128::from(total - startup) * u128::from(rows))
                .div_ceil(u128::from(self.total.estimated_rows));
            startup.saturating_add(variable as u64)
        };
        PlanCostBreakdown::new(
            rows,
            scale(self.total.cpu, self.startup.cpu),
            scale(self.total.random_io, self.startup.random_io),
            scale(self.total.sequential_io, self.startup.sequential_io),
            scale(self.total.output_rows, self.startup.output_rows),
        )
    }
}

fn zero() -> PlanCostBreakdown {
    // Keep the estimator's one-row floor while charging no consumed work.
    // Executable LIMIT 0 plans are lowered to EmptyExec before this fold.
    PlanCostBreakdown::new(1, 0, 0, 0, 0)
}

fn seek_startup(work: u64) -> PlanCostBreakdown {
    PlanCostBreakdown::new(1, 0, work, 0, 0)
}

fn access_startup(access: &NodeProjectionAccess, total: PlanCostBreakdown) -> PlanCostBreakdown {
    match access {
        NodeProjectionAccess::LabelScan => {
            PlanCostBreakdown::new(1, 0, 0, super::NODE_FULL_SCAN_STARTUP_COST, 0)
        }
        NodeProjectionAccess::PropertyValues { values, .. } => {
            seek_startup(values.len().max(1) as u64)
        }
        NodeProjectionAccess::CompositeEquality { predicates } => {
            seek_startup(predicates.len().max(1) as u64)
        }
        NodeProjectionAccess::CompositeRange { seek } => {
            seek_startup(seek.equality_prefix.len().saturating_add(1) as u64)
        }
        NodeProjectionAccess::CompositeOrdered { seek } => {
            seek_startup(seek.equality_prefix.len() as u64)
        }
        NodeProjectionAccess::PropertyRange { .. } => {
            seek_startup(super::NODE_INDEX_RANGE_STARTUP_COST)
        }
        // Candidate union/dedup and posting-list work are not proven prefix
        // producers by this model. Keep their complete access work.
        NodeProjectionAccess::PropertyUnion { .. } | NodeProjectionAccess::FullText { .. } => total,
    }
}

pub(super) fn cost_operator(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    inputs: [Option<ConsumptionCost>; 2],
    bindings: &PlanBindings<'_>,
    statistics: &mut Option<super::StatisticsUsage>,
) -> ConsumptionCost {
    let mut costs = inputs.map(|input| input.map(|input| input.total));
    if let PhysicalPlan::LimitExec {
        offset,
        limit: Some(limit),
        ..
    } = plan
    {
        // LIMIT 0 does not consume OFFSET either. Otherwise OFFSET is part of
        // the work goal, not part of the returned-row bound.
        let wanted = if *limit == 0 {
            0
        } else {
            offset.saturating_add(*limit) as u64
        };
        costs[0] = Some(inputs[0].expect("unary input cost").prefix(wanted));
    }
    let total = estimate_local_operator_cost(plan, catalog, costs, bindings, statistics);
    let startup = match plan {
        PhysicalPlan::SeqNodeScan { .. } => {
            PlanCostBreakdown::new(1, 0, 0, super::NODE_FULL_SCAN_STARTUP_COST, 0)
        }
        PhysicalPlan::SourceSegmentScan { .. } => seek_startup(super::NODE_FULL_SCAN_STARTUP_COST),
        PhysicalPlan::NodeProjectionScanExec { access, .. } => access_startup(access, total),
        PhysicalPlan::IndexNodeSeek { .. } => seek_startup(super::NODE_INDEX_EQ_STARTUP_COST),
        PhysicalPlan::IndexNodeMultiSeek { values, .. } => seek_startup(values.len() as u64),
        PhysicalPlan::IndexNodeCompositeSeek { predicates, .. } => {
            seek_startup(predicates.len() as u64)
        }
        PhysicalPlan::IndexNodeCompositeRangeSeek { seek, .. } => {
            seek_startup(seek.equality_prefix.len().saturating_add(1) as u64)
        }
        PhysicalPlan::IndexNodeRangeSeek { .. } => {
            seek_startup(super::NODE_INDEX_RANGE_STARTUP_COST)
        }
        PhysicalPlan::FilterExec { .. }
        | PhysicalPlan::ProjectExec { .. }
        | PhysicalPlan::NodeColumnLookupExec { .. }
        | PhysicalPlan::AdjacencyExpandExec { .. }
        | PhysicalPlan::AdjacencyExistsExec { .. }
        | PhysicalPlan::OptionalDegreeExec { .. } => {
            // These paths produce batches/probes incrementally. Scaling the
            // full output cardinality also scales input work by the estimated
            // filter selectivity or expansion fanout, rounding work upward.
            inputs[0].expect("unary input cost").startup
        }
        PhysicalPlan::LimitExec { offset, limit, .. } => {
            let input = inputs[0].expect("unary input cost");
            if *limit == Some(0) {
                zero()
            } else if *offset == 0 {
                input.startup
            } else {
                input.prefix(*offset as u64)
            }
        }
        // Sorting, TopN, grouping, dedup, hash build, retrieval and unknown
        // producers must retain their full input work before any output. Do
        // not scale them merely because a parent asks for fewer result rows.
        _ => total,
    };
    ConsumptionCost { total, startup }
}
