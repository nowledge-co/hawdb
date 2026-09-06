//! Canonical cost composition for relational join enumeration.

use crate::cost::PlanCostBreakdown;

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
        const DEFAULT_SELECTIVITY_DIVISOR: u64 = 10;

        let cap = |distinct_values: u64, rows: u64| distinct_values.max(1).min(rows.max(1));
        match self {
            Self::Unknown => DEFAULT_SELECTIVITY_DIVISOR,
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
                .unwrap_or(DEFAULT_SELECTIVITY_DIVISOR),
        }
    }
}

pub fn estimate_relational_access_cost(estimated_rows: usize) -> PlanCostBreakdown {
    let rows = u64::try_from(estimated_rows).unwrap_or(u64::MAX).max(1);
    PlanCostBreakdown::new(rows, rows, 0, 0, 0)
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
    PlanCostBreakdown::new(
        estimated_rows,
        left.cpu
            .saturating_add(right.cpu.saturating_mul(right_multiplier))
            .saturating_add(join_cpu),
        left.random_io
            .saturating_add(right.random_io.saturating_mul(right_multiplier)),
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
        assert_eq!(cost.cost, 170);
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
