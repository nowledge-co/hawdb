// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Logical work estimates, not measured instructions, allocator bytes or IO.

use super::{shape::RowShape, width, OptimizerCatalog, PhysicalPlan, PlanCostBreakdown};
use crate::graph::cardinality::PlanBindings;
use hawdb_core::TableKind;

#[derive(Clone, Copy)]
pub(super) struct WidthReport {
    pub(super) row_bytes: u64,
    pub(super) retained_bytes: u64,
    pub(super) comparison_work: u64,
    pub(super) spill_work: u64,
    pub(super) memory_hint: Option<u64>,
    pub(super) proxy_fields: usize,
    pub(super) sampled_values: usize,
}

fn clamp(value: u128) -> u64 {
    value.min(u128::from(u64::MAX)) as u64
}

fn ceil_log2(rows: u64) -> u64 {
    if rows <= 1 {
        0
    } else {
        u64::from(u64::BITS - (rows - 1).leading_zeros())
    }
}

fn sort_comparisons(rows: u64) -> u64 {
    if rows <= 1 {
        return 0;
    }
    let depth = ceil_log2(rows);
    clamp(u128::from(rows) * u128::from(depth) - ((1u128 << depth) - 1))
}

fn heap_push_comparisons(rows: u64) -> u64 {
    if rows <= 1 {
        return 0;
    }
    let depth = u64::from(rows.ilog2());
    let mut result = 0u128;
    for level in 0..depth {
        result += (1u128 << level) * u128::from(level);
    }
    result += (u128::from(rows) - (1u128 << depth) + 1) * u128::from(depth);
    clamp(result)
}

fn spill_work(bytes: u64, memory: Option<u64>) -> u64 {
    // Browser execution has no disk spill. Runtime admission stays authoritative
    // on every platform, including lower live soft targets and directory quotas.
    if cfg!(all(target_arch = "wasm32", target_os = "unknown")) {
        return 0;
    }
    let Some(memory) = memory.filter(|memory| *memory != 0) else {
        return 0;
    };
    let runs = bytes.div_ceil(memory);
    if runs <= 1 {
        return 0;
    }
    // Two-way merge depth is a conservative work proxy. These are logical
    // payload units; encoded spill bytes and adaptive fan-in may differ.
    clamp(u128::from(bytes).div_ceil(64) * 2 * u128::from(ceil_log2(runs).saturating_add(1)))
}

pub(super) struct Inputs {
    pub(super) costs: [Option<PlanCostBreakdown>; 2],
    pub(super) row_bytes: [u64; 2],
}

pub(super) fn apply(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    inputs: Inputs,
    shape: &RowShape<'_>,
    bindings: &PlanBindings<'_>,
    base: PlanCostBreakdown,
    memory_hint: Option<u64>,
) -> (PlanCostBreakdown, Option<WidthReport>) {
    let input_row_bytes = inputs.row_bytes;
    let inputs = inputs.costs;
    let rows = inputs[0].map_or(base.estimated_rows, |input| input.estimated_rows);
    let mut cpu = base.cpu;
    let mut retained = 0;
    let mut spill_stream_bytes = 0;
    let mut comparisons = 0;
    let mut active = true;
    match plan {
        PhysicalPlan::NodeProjectionScanExec {
            label,
            access,
            required_properties,
            ..
        } => {
            let visited = super::projected_access_cost(access, label, catalog).estimated_rows;
            let mut work = 0u64;
            for property in required_properties.iter().take(64) {
                work = work.saturating_add(
                    width::field_width(catalog, TableKind::Node, label, property)
                        .copy_units()
                        .unwrap_or(0),
                );
            }
            // Unsupported widths retain the existing opaque access charge;
            // they still have positive retained-state proxies in RowShape.
            cpu = cpu.saturating_add(visited.saturating_mul(work));
        }
        PhysicalPlan::SeqNodeScan { label, .. }
        | PhysicalPlan::IndexNodeSeek { label, .. }
        | PhysicalPlan::IndexNodeMultiSeek { label, .. }
        | PhysicalPlan::IndexNodeUnionSeek { label, .. }
        | PhysicalPlan::IndexNodeCompositeSeek { label, .. }
        | PhysicalPlan::IndexNodeCompositeRangeSeek { label, .. }
        | PhysicalPlan::IndexNodeRangeSeek { label, .. }
        | PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            cpu = cpu.saturating_add(base.estimated_rows.saturating_mul(
                width::record_decode_units(catalog, TableKind::Node, label).unwrap_or(0),
            ));
        }
        PhysicalPlan::NodeColumnLookupExec { label, .. } => {
            cpu = cpu.saturating_add(rows.saturating_mul(
                width::record_decode_units(catalog, TableKind::Node, label).unwrap_or(0),
            ));
        }
        PhysicalPlan::ProjectExec { .. } => {
            // The executor preserves graph bindings by moving them. Only the
            // newly materialized value map is copied here, not the whole graph.
            cpu = cpu.saturating_add(rows.saturating_mul(shape.clone_work_units()));
        }
        PhysicalPlan::SortExec { items, .. } | PhysicalPlan::TopNExec { items, .. } => {
            let (key_bytes, comparison_units) = shape.sort_key_bytes(items, catalog, bindings);
            let count = match plan {
                PhysicalPlan::TopNExec { offset, limit, .. } => {
                    (offset.saturating_add(*limit) as u64).min(rows)
                }
                _ => rows,
            };
            let sort_row_bytes = shape
                .retained_bytes()
                .saturating_add(key_bytes)
                .saturating_add(8);
            retained = count.saturating_mul(sort_row_bytes);
            spill_stream_bytes = rows.saturating_mul(sort_row_bytes);
            let cmp_count = if matches!(plan, PhysicalPlan::TopNExec { .. }) && rows > count {
                let remainder = rows.saturating_sub(count);
                let replacements =
                    remainder.min(count.saturating_mul(ceil_log2(rows.div_ceil(count.max(1)))));
                let depth = if count > 1 {
                    u64::from(count.ilog2())
                } else {
                    0
                };
                remainder
                    .saturating_add(heap_push_comparisons(count))
                    .saturating_add(replacements.saturating_mul(depth).saturating_mul(2))
                    .saturating_add(sort_comparisons(count))
            } else {
                // If the estimate fits in the window, use the same ordering
                // proxy as Sort. Favor bounded retention on equal rankings:
                // underestimated fanout must not force unbounded state merely
                // to save the estimated initial heap construction.
                sort_comparisons(rows)
            };
            comparisons = cmp_count.saturating_mul(comparison_units);
            // Sorting moves headers/pointers. Payload width affects key
            // materialization and retained state, not every binding move.
            let keys = rows.saturating_mul(
                (items.len() as u64)
                    .max(1)
                    .saturating_add(key_bytes.div_ceil(64)),
            );
            cpu = inputs[0]
                .expect("sort input")
                .cpu
                .saturating_add(keys)
                .saturating_add(comparisons)
                .saturating_add(count);
        }
        PhysicalPlan::AggregateExec { .. } => {
            retained = base
                .estimated_rows
                .saturating_mul(shape.values_bytes().saturating_add(64));
        }
        PhysicalPlan::DistinctExec { .. } => {
            // Each distinct entry owns both a cloned value key and its
            // representative Binding, including any retained graph payload.
            retained = base.estimated_rows.saturating_mul(
                shape
                    .retained_bytes()
                    .saturating_add(shape.values_bytes())
                    .saturating_add(32),
            );
        }
        PhysicalPlan::HashJoinExec { .. } => {
            // The executor builds the right input before probing the left.
            retained = inputs[1]
                .expect("hash build")
                .estimated_rows
                .saturating_mul(input_row_bytes[1].saturating_add(64));
        }
        _ => active = false,
    }
    if !active {
        return (base, None);
    }
    // If K rows cannot fit, TopN repeatedly spills before retaining K and
    // loses its cutoff. Price processing the full input, not one K-row run.
    let spill_bytes = if matches!(plan, PhysicalPlan::TopNExec { .. })
        && memory_hint.is_some_and(|memory| memory != 0 && retained > memory)
    {
        retained.max(spill_stream_bytes)
    } else {
        retained
    };
    let spill = spill_work(spill_bytes, memory_hint);
    let cost = PlanCostBreakdown::new(
        base.estimated_rows,
        cpu,
        base.random_io,
        base.sequential_io.saturating_add(spill),
        base.output_rows,
    );
    (
        cost,
        Some(WidthReport {
            row_bytes: shape.retained_bytes(),
            retained_bytes: retained,
            comparison_work: comparisons,
            spill_work: spill,
            memory_hint,
            proxy_fields: shape.uncertain_fields(),
            sampled_values: shape.sample_values(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn comparison_and_spill_arithmetic_is_bounded_at_zero_and_u64_limits() {
        for rows in [0, 1, 2, 10, 10000, u64::MAX] {
            assert!(sort_comparisons(rows) <= rows.saturating_mul(64));
            assert!(heap_push_comparisons(rows) <= rows.saturating_mul(64));
            assert_eq!(spill_work(rows, None), 0);
            assert_eq!(spill_work(rows, Some(0)), 0);
        }
        assert_eq!(sort_comparisons(2), 1);
        assert_eq!(heap_push_comparisons(6), 8);
        if cfg!(all(target_arch = "wasm32", target_os = "unknown")) {
            assert_eq!(spill_work(1 << 20, Some(1 << 10)), 0);
        } else {
            assert!(spill_work(1 << 20, Some(1 << 10)) > 0);
        }
    }
}
