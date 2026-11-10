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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCost {
    pub estimated_rows: u64,
    pub cost: u64,
}

impl PlanCost {
    pub fn with_cardinality_floor(self) -> Self {
        Self {
            estimated_rows: self.estimated_rows.max(1),
            ..self
        }
    }
}

/// Cardinality and unweighted logical work, plus the weighted scalar total.
///
/// CPU work is the scalar normalization unit. Random/sequential work describes
/// access patterns, not physical file pages; output work counts produced rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCostBreakdown {
    pub estimated_rows: u64,
    pub cost: u64,
    pub cpu: u64,
    pub random_io: u64,
    pub sequential_io: u64,
    pub output_rows: u64,
}

/// Relative weights applied once to raw logical work components.
///
/// These are planning units rather than hardware latency coefficients. Exposing
/// the immutable policy lets offline qualification re-score frozen candidates;
/// it does not add a runtime tuning surface or change the engine default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCostWeights {
    pub cpu: u64,
    pub random_access: u64,
    pub sequential_access: u64,
    pub output: u64,
}

impl PlanCostWeights {
    pub const fn new(cpu: u64, random_access: u64, sequential_access: u64, output: u64) -> Self {
        Self {
            cpu,
            random_access,
            sequential_access,
            output,
        }
    }

    fn score(self, cpu: u64, random_access: u64, sequential_access: u64, output: u64) -> u64 {
        cpu.saturating_mul(self.cpu)
            .saturating_add(random_access.saturating_mul(self.random_access))
            .saturating_add(sequential_access.saturating_mul(self.sequential_access))
            .saturating_add(output.saturating_mul(self.output))
    }
}

/// Stable identity of the default logical-work policy recorded in qualification
/// evidence. A different default requires a separately reviewed policy change.
pub const DEFAULT_PLAN_COST_POLICY_ID: &str = "hawdb-logical-cost-v1";

// CPU is the scalar normalization unit: from_scalar must not weight a cost twice.
// These are conservative logical-work weights, not physical page counts or time.
// Random access costs more than sequential access even when the OS caches bytes.
pub const DEFAULT_PLAN_COST_WEIGHTS: PlanCostWeights = PlanCostWeights::new(1, 2, 1, 1);

impl PlanCostBreakdown {
    pub fn with_cardinality_floor(self) -> Self {
        Self {
            estimated_rows: self.estimated_rows.max(1),
            ..self
        }
    }

    pub fn new(
        estimated_rows: u64,
        cpu: u64,
        random_io: u64,
        sequential_io: u64,
        output_rows: u64,
    ) -> Self {
        Self {
            estimated_rows,
            cost: DEFAULT_PLAN_COST_WEIGHTS.score(cpu, random_io, sequential_io, output_rows),
            cpu,
            random_io,
            sequential_io,
            output_rows,
        }
        .with_cardinality_floor()
    }

    /// Re-scores retained raw work without changing cardinality or the stored
    /// engine-default total. Intended for offline qualification and fitting.
    pub fn score_with_weights(self, weights: PlanCostWeights) -> PlanCost {
        PlanCost {
            estimated_rows: self.estimated_rows,
            cost: weights.score(
                self.cpu,
                self.random_io,
                self.sequential_io,
                self.output_rows,
            ),
        }
        .with_cardinality_floor()
    }

    pub fn from_scalar(cost: PlanCost) -> Self {
        Self::new(cost.estimated_rows, cost.cost, 0, 0, 0)
    }

    pub fn with_cpu(self, estimated_rows: u64, cpu: u64, output_rows: u64) -> Self {
        Self::new(
            estimated_rows,
            self.cpu.saturating_add(cpu),
            self.random_io,
            self.sequential_io,
            self.output_rows.saturating_add(output_rows),
        )
    }

    pub fn with_random_io(self, estimated_rows: u64, random_io: u64, output_rows: u64) -> Self {
        Self::new(
            estimated_rows,
            self.cpu,
            self.random_io.saturating_add(random_io),
            self.sequential_io,
            self.output_rows.saturating_add(output_rows),
        )
    }

    pub fn combine_with_cpu(
        left: Self,
        right: Self,
        estimated_rows: u64,
        cpu: u64,
        output_rows: u64,
    ) -> Self {
        Self::new(
            estimated_rows,
            left.cpu.saturating_add(right.cpu).saturating_add(cpu),
            left.random_io.saturating_add(right.random_io),
            left.sequential_io.saturating_add(right.sequential_io),
            left.output_rows
                .saturating_add(right.output_rows)
                .saturating_add(output_rows),
        )
    }

    pub fn as_plan_cost(self) -> PlanCost {
        PlanCost {
            estimated_rows: self.estimated_rows,
            cost: self.cost,
        }
        .with_cardinality_floor()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PlanCost, PlanCostBreakdown, PlanCostWeights, DEFAULT_PLAN_COST_POLICY_ID,
        DEFAULT_PLAN_COST_WEIGHTS,
    };

    #[test]
    fn cost_breakdown_sums_stable_components() {
        let cost = PlanCostBreakdown::new(7, 10, 20, 30, 40);

        assert_eq!(cost.estimated_rows, 7);
        assert_eq!(cost.cost, 120);
        assert_eq!(
            cost.as_plan_cost(),
            PlanCost {
                estimated_rows: 7,
                cost: 120,
            }
        );
    }

    #[test]
    fn cost_breakdown_combines_child_costs_with_parent_cpu() {
        let left = PlanCostBreakdown::new(2, 3, 5, 7, 11);
        let right = PlanCostBreakdown::new(13, 17, 19, 23, 29);

        let combined = PlanCostBreakdown::combine_with_cpu(left, right, 26, 31, 37);

        assert_eq!(combined.estimated_rows, 26);
        assert_eq!(combined.cpu, 51);
        assert_eq!(combined.random_io, 24);
        assert_eq!(combined.sequential_io, 30);
        assert_eq!(combined.output_rows, 77);
        assert_eq!(combined.cost, 206);
    }

    #[test]
    fn cost_breakdown_preserves_a_non_zero_cardinality_floor() {
        let cost = PlanCostBreakdown::new(0, 0, 0, 0, 0);

        assert_eq!(cost.estimated_rows, 1);
        assert_eq!(cost.as_plan_cost().estimated_rows, 1);
    }

    #[test]
    fn scalar_cost_preserves_a_non_zero_cardinality_floor() {
        let cost = PlanCost {
            estimated_rows: 0,
            cost: 7,
        }
        .with_cardinality_floor();

        assert_eq!(cost.estimated_rows, 1);
        assert_eq!(cost.cost, 7);
    }

    #[test]
    fn qualification_can_rescore_raw_work_without_mutating_the_default_total() {
        let cost = PlanCostBreakdown::new(7, 10, 20, 30, 40);

        assert_eq!(DEFAULT_PLAN_COST_POLICY_ID, "hawdb-logical-cost-v1");
        assert_eq!(DEFAULT_PLAN_COST_WEIGHTS, PlanCostWeights::new(1, 2, 1, 1));
        assert_eq!(cost.cost, 120);
        assert_eq!(
            cost.score_with_weights(PlanCostWeights::new(1, 5, 2, 0)),
            PlanCost {
                estimated_rows: 7,
                cost: 170,
            }
        );
        assert_eq!(cost.cost, 120);
    }
}
