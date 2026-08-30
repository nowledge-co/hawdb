//! Canonical cost composition for relational join enumeration.

use crate::cost::PlanCostBreakdown;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinCardinality {
    Inner,
    PreserveLeft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationalJoinRightInput {
    /// The right access runs once per outer row, so its component costs scale
    /// with the outer cardinality instead of being charged as a subtree.
    Probe,
    /// The right subtree is produced once, then the join charges row-pair work.
    Materialized,
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
    )
}

pub(crate) fn estimate_relational_join_cost(
    left: PlanCostBreakdown,
    right: PlanCostBreakdown,
    cardinality: RelationalJoinCardinality,
    right_input: RelationalJoinRightInput,
) -> PlanCostBreakdown {
    let joined_rows = left.estimated_rows.saturating_mul(right.estimated_rows);
    let estimated_rows = match cardinality {
        RelationalJoinCardinality::Inner => joined_rows,
        RelationalJoinCardinality::PreserveLeft => joined_rows.max(left.estimated_rows),
    };
    let (right_multiplier, join_cpu) = match right_input {
        RelationalJoinRightInput::Probe => (left.estimated_rows, 0),
        RelationalJoinRightInput::Materialized => (1, joined_rows),
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
        );

        assert_eq!(
            cost.as_plan_cost(),
            PlanCost {
                estimated_rows: 6,
                cost: 11,
            }
        );
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
        );

        assert_eq!(cost.estimated_rows, 4);
        assert_eq!(cost.as_plan_cost().cost, 4);
    }
}
