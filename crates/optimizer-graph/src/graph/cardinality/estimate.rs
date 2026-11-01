// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Copy-only provenance collected with the cost fold. Formatting is deferred
//! until the selected plan is reported; alternative costing allocates no log.

#[derive(Clone, Copy)]
pub(in crate::graph) enum StatisticsUsage {
    Filter {
        keys: usize,
        conditioned: usize,
        population: u64,
        indexed: u64,
        ndv: u64,
    },
    Group {
        keys: usize,
        population: u64,
        indexed: u64,
        ndv: u64,
    },
    HashJoin {
        left_ndv: u64,
        right_ndv: u64,
    },
    JoinFilter {
        keys: usize,
        left_ndv: u64,
        right_ndv: u64,
        enforced_ndv: u64,
    },
}

impl StatisticsUsage {
    pub(in crate::graph) fn explain(self) -> String {
        match self {
            Self::Filter { keys, conditioned, population, indexed, ndv } => format!("source=catalog_composite_ndv operation=filter keys={keys} conditioned_keys={conditioned} population={population} complete_key_rows={indexed} joint_ndv={ndv}"),
            Self::Group { keys, population, indexed, ndv } => format!("source=catalog_composite_ndv operation=group keys={keys} population={population} complete_key_rows={indexed} joint_ndv={ndv} missing_key_rows={}",population.saturating_sub(indexed)),
            Self::HashJoin { left_ndv, right_ndv } => format!("source=catalog_property_ndv operation=hash_join left_ndv={left_ndv} right_ndv={right_ndv}"),
            Self::JoinFilter { keys, left_ndv, right_ndv, enforced_ndv } => format!("source=catalog_composite_ndv operation=join_filter keys={keys} left_joint_ndv={left_ndv} right_joint_ndv={right_ndv} enforced_key_ndv={enforced_ndv} complete_domains=true"),
        }
    }
}
