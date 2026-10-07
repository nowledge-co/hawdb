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

//! Canonical cost composition for relational join enumeration.

use crate::cardinality_defaults::JOIN_SELECTIVITY_DIVISOR;
use crate::cost::PlanCostBreakdown;
use crate::{RelationalAccessPathDescriptor, RelationalAccessPathKind};
use hawdb_expression::BindingId;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

const FULL_SCAN_SETUP_CPU: u64 = 4;
// Descriptor and key artifacts are initialized per row-root invocation.
const SNAPSHOT_ROOT_ARTIFACT_SETUP_CPU: u64 = 2;
// A checked root descriptor reads its fixed record and both nonempty key bounds.
const SNAPSHOT_ROOT_DESCRIPTOR_READS: u64 = 3;

/// Immutable structural work for the row source of one relation.
///
/// The default retains descriptor-only logical costing. A snapshot context
/// must come from the exact pinned reader used by execution, rather than a
/// global storage mode or cache counter. Counts describe logical lookup work;
/// they do not certify cache warmth or predict physical I/O or elapsed time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RelationalAccessCostContext {
    snapshot_rows: Option<RelationalSnapshotRowCost>,
}

/// Per-binding row-source costs for one immutable join enumeration.
///
/// Bindings, rather than a query-wide storage mode, distinguish mixed sources
/// and self-joins. Missing bindings retain the descriptor-only default. The
/// caller derives each entry from the reader pinned for that relation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RelationalJoinCostContexts {
    relations: BTreeMap<BindingId, RelationalAccessCostContext>,
}

impl RelationalJoinCostContexts {
    pub fn with_relation(
        mut self,
        binding: BindingId,
        context: RelationalAccessCostContext,
    ) -> Self {
        self.relations.insert(binding, context);
        self
    }

    pub fn for_relation(&self, binding: BindingId) -> RelationalAccessCostContext {
        self.relations.get(&binding).copied().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelationalSnapshotRowCost {
    table_rows: NonZeroU64,
    table_pages: NonZeroU64,
}

impl RelationalSnapshotRowCost {
    fn point_work(self, rows: u64) -> SnapshotRootWork {
        let search_steps = u64::from(u64::BITS - self.table_pages.get().leading_zeros());
        SnapshotRootWork {
            cpu: rows.saturating_mul(SNAPSHOT_ROOT_ARTIFACT_SETUP_CPU.saturating_add(search_steps)),
            random: rows
                .saturating_mul(search_steps)
                .saturating_mul(SNAPSHOT_ROOT_DESCRIPTOR_READS),
            sequential: 0,
        }
    }
}

#[derive(Default)]
struct SnapshotRootWork {
    cpu: u64,
    random: u64,
    sequential: u64,
}

impl RelationalAccessCostContext {
    /// Records bounded table metadata from a caller-pinned row snapshot.
    /// Empty or unavailable row roots retain the default context. Mixed row
    /// sources require one context per relation, including inside join memos.
    pub const fn for_snapshot_rows(table_rows: NonZeroU64, table_pages: NonZeroU64) -> Self {
        Self {
            snapshot_rows: Some(RelationalSnapshotRowCost {
                table_rows,
                table_pages,
            }),
        }
    }

    fn additional_work(
        self,
        access: &RelationalAccessPathDescriptor,
        rows: u64,
    ) -> SnapshotRootWork {
        let Some(snapshot) = self.snapshot_rows else {
            return SnapshotRootWork::default();
        };
        // Each point invocation initializes descriptor/key artifact access,
        // then searches the checked page bounds. The current demand scan also
        // initializes both artifacts for every descriptor it visits. Estimate work
        // using the binary-search depth bound and average page occupancy.
        // Descriptor records and both bounds also incur logical access work:
        // point probes jump among descriptors, while scans visit them in order.
        // Cache residency does not remove those checked metadata reads.
        match access.kind {
            RelationalAccessPathKind::FullScan => {
                let pages = (u128::from(rows) * u128::from(snapshot.table_pages.get()))
                    .div_ceil(u128::from(snapshot.table_rows.get()))
                    .min(u128::from(snapshot.table_pages.get())) as u64;
                SnapshotRootWork {
                    cpu: pages.saturating_mul(SNAPSHOT_ROOT_ARTIFACT_SETUP_CPU.saturating_add(1)),
                    random: 0,
                    sequential: pages.saturating_mul(SNAPSHOT_ROOT_DESCRIPTOR_READS),
                }
            }
            RelationalAccessPathKind::PrimaryKey => snapshot.point_work(rows),
            RelationalAccessPathKind::Index if access.requires_row_fetch => {
                snapshot.point_work(rows)
            }
            RelationalAccessPathKind::Index => SnapshotRootWork::default(),
        }
    }

    fn locator_fetch_work(self, rows: u64) -> SnapshotRootWork {
        let Some(snapshot) = self.snapshot_rows else {
            // Keep the existing descriptor-only join contract for callers
            // without a pinned canonical row snapshot.
            return SnapshotRootWork::default();
        };
        let root = snapshot.point_work(rows);
        SnapshotRootWork {
            cpu: rows.saturating_add(root.cpu),
            random: rows.saturating_add(root.random),
            sequential: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinCardinality {
    Inner,
    PreserveLeft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinRightInput {
    /// The right access runs once per outer row, so its component costs scale
    /// with the outer cardinality instead of being charged as a subtree.
    Probe,
    /// The right subtree is produced once, then the join charges row-pair work.
    Materialized,
    /// Build a hash table once, then probe it with the left input.
    Hash,
    /// Merge two compatibly ordered inputs without sorting them.
    Merge,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinSelectivity {
    /// No trustworthy predicate statistics are available. Materialized joins
    /// use the documented 10% fallback; probe inputs ignore this value because
    /// their row estimate is already a per-outer-row fanout.
    #[default]
    Unknown,
    /// Distinct-value counts for the complete equality key on either side.
    /// One known side is still a useful conservative denominator; when both
    /// sides are known, the standard `1 / max(left_ndv, right_ndv)` estimate is
    /// used.
    EquiJoin {
        left_distinct_values: Option<u64>,
        right_distinct_values: Option<u64>,
    },
}

impl RelationalJoinSelectivity {
    pub const fn equi_join(
        left_distinct_values: Option<u64>,
        right_distinct_values: Option<u64>,
    ) -> Self {
        Self::EquiJoin {
            left_distinct_values,
            right_distinct_values,
        }
    }

    fn materialized_divisor(self, left_rows: u64, right_rows: u64) -> u64 {
        let cap = |distinct_values: u64, rows: u64| distinct_values.max(1).min(rows.max(1));
        match self {
            Self::Unknown => JOIN_SELECTIVITY_DIVISOR,
            Self::EquiJoin {
                left_distinct_values,
                right_distinct_values,
            } => left_distinct_values
                .map(|distinct_values| cap(distinct_values, left_rows))
                .into_iter()
                .chain(
                    right_distinct_values.map(|distinct_values| cap(distinct_values, right_rows)),
                )
                .max()
                .unwrap_or(JOIN_SELECTIVITY_DIVISOR),
        }
    }
}

pub fn estimate_relational_access_cost(estimated_rows: usize) -> PlanCostBreakdown {
    let rows = u64::try_from(estimated_rows).unwrap_or(u64::MAX).max(1);
    PlanCostBreakdown::new(rows, rows, 0, 0, 0)
}

/// Estimates logical work for a validated access descriptor.
///
/// Cardinality counts visited candidate rows, before residual filtering. A full
/// scan visits sequential rows; primary-key access directly locates rows. An
/// index navigates once, visits entries, and fetches each non-covering row.
/// Ordering changes delivered properties, not the number of visited entries.
/// These units do not predict file I/O: residency, row width, page clustering
/// and caches require separate runtime evidence. The rows-only helper retains
/// its original CPU-only contract for callers without an access descriptor.
pub fn estimate_relational_access_path_cost(
    access: &RelationalAccessPathDescriptor,
) -> PlanCostBreakdown {
    estimate_relational_access_path_cost_with_context(
        access,
        RelationalAccessCostContext::default(),
    )
}

/// Uses the same raw-component contract with relation-specific structural
/// context. Row-root setup CPU and metadata access patterns retain separate raw
/// components; cardinality and output stay independent. The final constructor
/// weights every component once.
pub fn estimate_relational_access_path_cost_with_context(
    access: &RelationalAccessPathDescriptor,
    context: RelationalAccessCostContext,
) -> PlanCostBreakdown {
    let rows = u64::try_from(access.estimated_rows)
        .unwrap_or(u64::MAX)
        .max(1);
    let (cpu, random, sequential) = match access.kind {
        RelationalAccessPathKind::FullScan => (rows.saturating_add(FULL_SCAN_SETUP_CPU), 0, rows),
        RelationalAccessPathKind::PrimaryKey => (rows, rows, 0),
        RelationalAccessPathKind::Index => {
            let fetches = if access.requires_row_fetch { rows } else { 0 };
            (
                rows.saturating_add(fetches),
                1_u64.saturating_add(fetches),
                if access.unique_point { 0 } else { rows },
            )
        }
    };
    let root_work = context.additional_work(access, rows);
    PlanCostBreakdown::new(
        rows,
        cpu.saturating_add(root_work.cpu),
        random.saturating_add(root_work.random),
        sequential.saturating_add(root_work.sequential),
        rows,
    )
}

/// Extends a left-deep plan with one probe join using the canonical
/// relational cost model.
pub fn estimate_relational_probe_join_cost(
    left: PlanCostBreakdown,
    inner_estimated_rows: usize,
    cardinality: RelationalJoinCardinality,
) -> PlanCostBreakdown {
    estimate_relational_join_cost(
        left,
        estimate_relational_access_cost(inner_estimated_rows),
        cardinality,
        RelationalJoinRightInput::Probe,
        RelationalJoinSelectivity::Unknown,
    )
}

pub fn estimate_relational_join_cost(
    left: PlanCostBreakdown,
    right: PlanCostBreakdown,
    cardinality: RelationalJoinCardinality,
    right_input: RelationalJoinRightInput,
    selectivity: RelationalJoinSelectivity,
) -> PlanCostBreakdown {
    estimate_relational_join_cost_with_contexts(
        left,
        right,
        cardinality,
        right_input,
        selectivity,
        RelationalAccessCostContext::default(),
        RelationalAccessCostContext::default(),
    )
}

/// Extends join composition with the original row sources of its two inputs.
///
/// The current relational Hash adapter retains locators, even without spill,
/// and point-fetches both projected rows for every matching candidate. Charge
/// that resident work in addition to producing the inputs. LEFT also allows
/// one unmatched locator fetch for every left row, a conservative bound since
/// equality pair counts do not identify distinct matched outer rows. Merge
/// retains projected rows; probe/materialized costs already charge their input
/// invocations. Default contexts preserve the descriptor-only contract.
/// Spill validation and repartitioning are additional unmodeled work: this is
/// a resident logical estimate, not a guarantee of execution within a budget.
pub fn estimate_relational_join_cost_with_contexts(
    left: PlanCostBreakdown,
    right: PlanCostBreakdown,
    cardinality: RelationalJoinCardinality,
    right_input: RelationalJoinRightInput,
    selectivity: RelationalJoinSelectivity,
    left_context: RelationalAccessCostContext,
    right_context: RelationalAccessCostContext,
) -> PlanCostBreakdown {
    let candidate_pairs = left.estimated_rows.saturating_mul(right.estimated_rows);
    let joined_rows = match right_input {
        RelationalJoinRightInput::Probe => candidate_pairs,
        RelationalJoinRightInput::Materialized
        | RelationalJoinRightInput::Hash
        | RelationalJoinRightInput::Merge => candidate_pairs
            .div_ceil(selectivity.materialized_divisor(left.estimated_rows, right.estimated_rows)),
    };
    let estimated_rows = match cardinality {
        RelationalJoinCardinality::Inner => joined_rows,
        RelationalJoinCardinality::PreserveLeft => joined_rows.max(left.estimated_rows),
    };
    let (right_multiplier, join_cpu) = match right_input {
        RelationalJoinRightInput::Probe => (left.estimated_rows, 0),
        RelationalJoinRightInput::Materialized => (1, candidate_pairs),
        RelationalJoinRightInput::Hash => (
            1,
            left.estimated_rows
                .saturating_add(right.estimated_rows.saturating_mul(2))
                .saturating_add(joined_rows),
        ),
        RelationalJoinRightInput::Merge => (
            1,
            left.estimated_rows
                .saturating_add(right.estimated_rows)
                .saturating_add(joined_rows),
        ),
    };
    let (left_replay, right_replay) = if right_input == RelationalJoinRightInput::Hash {
        let left_fetches = joined_rows.saturating_add(match cardinality {
            RelationalJoinCardinality::Inner => 0,
            RelationalJoinCardinality::PreserveLeft => left.estimated_rows,
        });
        (
            left_context.locator_fetch_work(left_fetches),
            right_context.locator_fetch_work(joined_rows),
        )
    } else {
        (SnapshotRootWork::default(), SnapshotRootWork::default())
    };
    PlanCostBreakdown::new(
        estimated_rows,
        left.cpu
            .saturating_add(right.cpu.saturating_mul(right_multiplier))
            .saturating_add(join_cpu)
            .saturating_add(left_replay.cpu)
            .saturating_add(right_replay.cpu),
        left.random_io
            .saturating_add(right.random_io.saturating_mul(right_multiplier))
            .saturating_add(left_replay.random)
            .saturating_add(right_replay.random),
        left.sequential_io
            .saturating_add(right.sequential_io.saturating_mul(right_multiplier)),
        left.output_rows
            .saturating_add(right.output_rows.saturating_mul(right_multiplier)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlanCost;

    #[test]
    fn snapshot_hash_locator_replay_preserves_mixed_sources_outer_floor_and_saturation() {
        let snapshot = |rows, pages| {
            RelationalAccessCostContext::for_snapshot_rows(
                NonZeroU64::new(rows).unwrap(),
                NonZeroU64::new(pages).unwrap(),
            )
        };
        let empty = RelationalAccessCostContext::default();
        let left_context = snapshot(10, 4);
        let right_context = snapshot(4, 1);
        let left = PlanCostBreakdown::new(10, 11, 13, 17, 19);
        let right = PlanCostBreakdown::new(4, 23, 29, 31, 37);
        let selectivity = RelationalJoinSelectivity::equi_join(Some(10), Some(4));
        // Four estimated pairs. Resident join work is CPU22; each A fetch
        // costs CPU6/random10, each B fetch CPU4/random4. Input components
        // are supplied independently, not reconstructed by an access helper.
        for (left_source, right_source, cpu, random) in [
            (empty, empty, 56, 42),
            (left_context, empty, 80, 82),
            (empty, right_context, 72, 58),
            (left_context, right_context, 96, 98),
        ] {
            let result = estimate_relational_join_cost_with_contexts(
                left,
                right,
                RelationalJoinCardinality::Inner,
                RelationalJoinRightInput::Hash,
                selectivity,
                left_source,
                right_source,
            );
            assert_eq!(result, PlanCostBreakdown::new(4, cpu, random, 48, 56));
        }
        // Pair counts do not identify matched distinct probes. LEFT's bound
        // allows another ten left fetches while preserving output cardinality
        // and charging the supplied input/output components only once.
        assert_eq!(
            estimate_relational_join_cost_with_contexts(
                left,
                right,
                RelationalJoinCardinality::PreserveLeft,
                RelationalJoinRightInput::Hash,
                selectivity,
                left_context,
                right_context,
            ),
            PlanCostBreakdown::new(10, 156, 198, 48, 56),
        );
        for cardinality in [
            RelationalJoinCardinality::Inner,
            RelationalJoinCardinality::PreserveLeft,
        ] {
            for input in [
                RelationalJoinRightInput::Probe,
                RelationalJoinRightInput::Materialized,
                RelationalJoinRightInput::Merge,
            ] {
                assert_eq!(
                    estimate_relational_join_cost_with_contexts(
                        left,
                        right,
                        cardinality,
                        input,
                        selectivity,
                        left_context,
                        right_context
                    ),
                    estimate_relational_join_cost(left, right, cardinality, input, selectivity),
                );
            }
            assert_eq!(
                estimate_relational_join_cost_with_contexts(
                    left,
                    right,
                    cardinality,
                    RelationalJoinRightInput::Hash,
                    selectivity,
                    empty,
                    empty
                ),
                estimate_relational_join_cost(
                    left,
                    right,
                    cardinality,
                    RelationalJoinRightInput::Hash,
                    selectivity
                ),
            );
        }
        let max = PlanCostBreakdown::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        let max_context = snapshot(u64::MAX, u64::MAX);
        assert_eq!(
            estimate_relational_join_cost_with_contexts(
                max,
                max,
                RelationalJoinCardinality::PreserveLeft,
                RelationalJoinRightInput::Hash,
                RelationalJoinSelectivity::Unknown,
                max_context,
                max_context
            ),
            max,
        );
    }

    #[test]
    fn equi_join_algorithms_charge_linear_work_and_preserve_component_costs() {
        for input in [
            RelationalJoinRightInput::Hash,
            RelationalJoinRightInput::Merge,
        ] {
            let left = PlanCostBreakdown::new(100, 7, 11, 13, 17);
            let right = PlanCostBreakdown::new(200, 19, 23, 29, 31);
            let result = estimate_relational_join_cost(
                left,
                right,
                RelationalJoinCardinality::Inner,
                input,
                RelationalJoinSelectivity::equi_join(Some(100), Some(200)),
            );
            assert_eq!(result.estimated_rows, 100);
            assert_eq!(
                result.cpu,
                26 + if input == RelationalJoinRightInput::Hash {
                    600
                } else {
                    400
                }
            );
            assert_eq!(
                (result.random_io, result.sequential_io, result.output_rows),
                (34, 42, 48)
            );
            let empty = PlanCostBreakdown::new(0, 0, 0, 0, 0);
            let outer = estimate_relational_join_cost(
                left,
                empty,
                RelationalJoinCardinality::PreserveLeft,
                input,
                RelationalJoinSelectivity::Unknown,
            );
            assert_eq!(outer.estimated_rows, 100);
        }
    }

    #[test]
    fn probe_join_preserves_the_existing_scalar_cost() {
        let left = estimate_relational_access_cost(5);
        let cost = estimate_relational_probe_join_cost(left, 2, RelationalJoinCardinality::Inner);

        assert_eq!(
            cost.as_plan_cost(),
            PlanCost {
                estimated_rows: 10,
                cost: 15,
            }
        );
    }

    #[test]
    fn materialized_join_charges_the_right_plan_once() {
        let left = estimate_relational_access_cost(2);
        let right = estimate_relational_access_cost(3);

        let cost = estimate_relational_join_cost(
            left,
            right,
            RelationalJoinCardinality::Inner,
            RelationalJoinRightInput::Materialized,
            RelationalJoinSelectivity::Unknown,
        );

        assert_eq!(
            cost.as_plan_cost(),
            PlanCost {
                estimated_rows: 1,
                cost: 11,
            }
        );
    }

    #[test]
    fn materialized_equi_join_uses_the_larger_distinct_count() {
        let left = estimate_relational_access_cost(10_000);
        let right = estimate_relational_access_cost(20_000);

        let cost = estimate_relational_join_cost(
            left,
            right,
            RelationalJoinCardinality::Inner,
            RelationalJoinRightInput::Materialized,
            RelationalJoinSelectivity::equi_join(Some(10_000), Some(5_000)),
        );

        assert_eq!(cost.estimated_rows, 20_000);
        assert_eq!(cost.cpu, 200_030_000);
    }

    #[test]
    fn materialized_equi_join_uses_one_known_distinct_count() {
        let left = estimate_relational_access_cost(10_000);
        let right = estimate_relational_access_cost(20_000);

        let cost = estimate_relational_join_cost(
            left,
            right,
            RelationalJoinCardinality::Inner,
            RelationalJoinRightInput::Materialized,
            RelationalJoinSelectivity::equi_join(Some(10_000), None),
        );

        assert_eq!(cost.estimated_rows, 20_000);
    }

    #[test]
    fn probe_join_scales_the_right_cost_components_by_outer_rows() {
        let left = PlanCostBreakdown::new(2, 3, 5, 7, 11);
        let right = PlanCostBreakdown::new(3, 13, 17, 19, 23);

        let cost = estimate_relational_join_cost(
            left,
            right,
            RelationalJoinCardinality::Inner,
            RelationalJoinRightInput::Probe,
            RelationalJoinSelectivity::equi_join(Some(2), Some(3)),
        );

        assert_eq!(cost.estimated_rows, 6);
        assert_eq!(cost.cpu, 29);
        assert_eq!(cost.random_io, 39);
        assert_eq!(cost.sequential_io, 45);
        assert_eq!(cost.output_rows, 57);
        assert_eq!(cost.cost, 209);
    }

    #[test]
    fn left_preserving_join_keeps_the_outer_cardinality_floor() {
        let left = PlanCostBreakdown::new(4, 4, 0, 0, 0);
        let empty_right = PlanCostBreakdown {
            estimated_rows: 0,
            cost: 0,
            cpu: 0,
            random_io: 0,
            sequential_io: 0,
            output_rows: 0,
        };

        let cost = estimate_relational_join_cost(
            left,
            empty_right,
            RelationalJoinCardinality::PreserveLeft,
            RelationalJoinRightInput::Probe,
            RelationalJoinSelectivity::Unknown,
        );

        assert_eq!(cost.estimated_rows, 4);
        assert_eq!(cost.as_plan_cost().cost, 4);
    }
}
