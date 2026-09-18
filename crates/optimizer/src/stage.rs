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

use std::str::FromStr;

use crate::rule::{apply_rule_batch, AppliedRule, OptimizerRule};
use crate::search::{RuleEvent, RuleOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ApplyOrder {
    Once,
    TopDown,
    BottomUp,
    FixedPoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizationStage {
    name: &'static str,
    apply_order: ApplyOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StageStats {
    pub input_count: usize,
    pub output_count: usize,
    pub applied_rules: usize,
    pub skipped_rules: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageTrace {
    name: String,
    apply_order: ApplyOrder,
    stats: StageStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRuleBatch<E> {
    expressions: Vec<AppliedRule<E>>,
    events: Vec<RuleEvent>,
    trace: StageTrace,
}

pub struct RuleStage<'a, E> {
    stage: OptimizationStage,
    rules: Vec<&'a dyn OptimizerRule<E>>,
}

pub struct OptimizationPipeline<'a, E> {
    stages: Vec<RuleStage<'a, E>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineExecution<E> {
    expression: E,
    events: Vec<RuleEvent>,
    traces: Vec<StageTrace>,
}

impl ApplyOrder {
    pub fn all() -> &'static [Self] {
        const ALL: &[ApplyOrder] = &[
            ApplyOrder::Once,
            ApplyOrder::TopDown,
            ApplyOrder::BottomUp,
            ApplyOrder::FixedPoint,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ApplyOrder::Once => "once",
            ApplyOrder::TopDown => "top_down",
            ApplyOrder::BottomUp => "bottom_up",
            ApplyOrder::FixedPoint => "fixed_point",
        }
    }
}

impl FromStr for ApplyOrder {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|order| order.as_str() == value)
            .ok_or("unknown optimizer stage apply order")
    }
}

impl OptimizationStage {
    pub const fn new(name: &'static str, apply_order: ApplyOrder) -> Self {
        Self { name, apply_order }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn apply_order(&self) -> ApplyOrder {
        self.apply_order
    }

    pub fn trace(&self, stats: StageStats) -> StageTrace {
        StageTrace {
            name: self.name.to_string(),
            apply_order: self.apply_order,
            stats,
        }
    }

    pub fn execute_rule_batch<E>(
        &self,
        expression: &E,
        rules: &[&dyn OptimizerRule<E>],
    ) -> StageRuleBatch<E> {
        let batch = apply_rule_batch(expression, rules);
        let (expressions, events) = batch.into_parts();
        let applied_rules = events
            .iter()
            .filter(|event| event.outcome() == RuleOutcome::Applied)
            .count();
        let skipped_rules = events
            .iter()
            .filter(|event| event.outcome() == RuleOutcome::Skipped)
            .count();
        let stats =
            StageStats::new(1, expressions.len()).with_rule_counts(applied_rules, skipped_rules);

        StageRuleBatch {
            expressions,
            events,
            trace: self.trace(stats),
        }
    }
}

impl StageStats {
    pub const fn new(input_count: usize, output_count: usize) -> Self {
        Self {
            input_count,
            output_count,
            applied_rules: 0,
            skipped_rules: 0,
        }
    }

    pub const fn with_rule_counts(mut self, applied_rules: usize, skipped_rules: usize) -> Self {
        self.applied_rules = applied_rules;
        self.skipped_rules = skipped_rules;
        self
    }
}

impl StageTrace {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn apply_order(&self) -> ApplyOrder {
        self.apply_order
    }

    pub fn stats(&self) -> StageStats {
        self.stats
    }
}

impl<E> StageRuleBatch<E> {
    pub fn expressions(&self) -> &[AppliedRule<E>] {
        &self.expressions
    }

    pub fn events(&self) -> &[RuleEvent] {
        &self.events
    }

    pub fn trace(&self) -> &StageTrace {
        &self.trace
    }

    pub fn into_parts(self) -> (Vec<AppliedRule<E>>, Vec<RuleEvent>, StageTrace) {
        (self.expressions, self.events, self.trace)
    }
}

impl<'a, E> RuleStage<'a, E> {
    pub fn new(stage: OptimizationStage, rules: Vec<&'a dyn OptimizerRule<E>>) -> Self {
        Self { stage, rules }
    }

    pub fn stage(&self) -> &OptimizationStage {
        &self.stage
    }

    pub fn rules(&self) -> &[&'a dyn OptimizerRule<E>] {
        &self.rules
    }
}

impl<'a, E> OptimizationPipeline<'a, E> {
    pub fn new(stages: Vec<RuleStage<'a, E>>) -> Self {
        Self { stages }
    }

    pub fn stages(&self) -> &[RuleStage<'a, E>] {
        &self.stages
    }
}

impl<E: Clone> OptimizationPipeline<'_, E> {
    pub fn execute(&self, expression: E) -> PipelineExecution<E> {
        let mut expression = expression;
        let mut events = Vec::new();
        let mut traces = Vec::new();

        for stage in &self.stages {
            let batch = stage.stage.execute_rule_batch(&expression, &stage.rules);
            let (expressions, stage_events, trace) = batch.into_parts();
            events.extend(stage_events);
            traces.push(trace);
            if let Some(applied) = expressions.into_iter().next() {
                expression = applied.into_application().into_expression();
            }
        }

        PipelineExecution {
            expression,
            events,
            traces,
        }
    }
}

impl<E> PipelineExecution<E> {
    pub fn expression(&self) -> &E {
        &self.expression
    }

    pub fn into_expression(self) -> E {
        self.expression
    }

    pub fn events(&self) -> &[RuleEvent] {
        &self.events
    }

    pub fn traces(&self) -> &[StageTrace] {
        &self.traces
    }

    pub fn into_parts(self) -> (E, Vec<RuleEvent>, Vec<StageTrace>) {
        (self.expression, self.events, self.traces)
    }
}

#[cfg(test)]
mod tests {
    use super::{ApplyOrder, OptimizationPipeline, OptimizationStage, RuleStage, StageStats};
    use crate::{OptimizerRule, RuleApplication, RuleId, RuleKind, RuleOutcome, RulePromise};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestExpr(&'static str);

    struct RenameRule {
        id: RuleId,
        priority: u16,
        input: &'static str,
        output: Option<&'static str>,
    }

    impl OptimizerRule<TestExpr> for RenameRule {
        fn id(&self) -> RuleId {
            self.id
        }

        fn promise(&self, expression: &TestExpr) -> RulePromise {
            if expression.0 == self.input {
                RulePromise::new(self.priority)
            } else {
                RulePromise::NEVER
            }
        }

        fn apply(&self, _expression: &TestExpr) -> Option<RuleApplication<TestExpr>> {
            self.output
                .map(|output| RuleApplication::new(TestExpr(output), format!("to={output}")))
        }
    }

    #[test]
    fn apply_order_strings_round_trip() {
        for order in ApplyOrder::all() {
            assert_eq!(order.as_str().parse::<ApplyOrder>(), Ok(*order));
        }
        assert!("sideways".parse::<ApplyOrder>().is_err());
    }

    #[test]
    fn stage_trace_records_stable_stage_metadata() {
        let stage = OptimizationStage::new("predicate_pushdown", ApplyOrder::BottomUp);
        let trace = stage.trace(StageStats::new(3, 2).with_rule_counts(1, 4));

        assert_eq!(trace.name(), "predicate_pushdown");
        assert_eq!(trace.apply_order(), ApplyOrder::BottomUp);
        assert_eq!(trace.stats().input_count, 3);
        assert_eq!(trace.stats().output_count, 2);
        assert_eq!(trace.stats().applied_rules, 1);
        assert_eq!(trace.stats().skipped_rules, 4);
    }

    #[test]
    fn stage_executes_rules_and_records_structured_trace() {
        let stage = OptimizationStage::new("access_path_selection", ApplyOrder::BottomUp);
        let high = RenameRule {
            id: RuleId::new("node_equality_index_seek", RuleKind::Implementation),
            priority: 100,
            input: "filter",
            output: Some("seek"),
        };
        let skipped = RenameRule {
            id: RuleId::new("node_text_index_seek", RuleKind::Implementation),
            priority: 10,
            input: "filter",
            output: None,
        };

        let batch = stage.execute_rule_batch(&TestExpr("filter"), &[&skipped, &high]);

        assert_eq!(batch.expressions().len(), 1);
        assert_eq!(
            batch.expressions()[0].id().stable_name(),
            "implementation:node_equality_index_seek"
        );
        assert_eq!(batch.events().len(), 2);
        assert_eq!(batch.events()[0].outcome(), RuleOutcome::Applied);
        assert_eq!(batch.events()[1].outcome(), RuleOutcome::Skipped);
        assert_eq!(batch.trace().name(), "access_path_selection");
        assert_eq!(batch.trace().stats().input_count, 1);
        assert_eq!(batch.trace().stats().output_count, 1);
        assert_eq!(batch.trace().stats().applied_rules, 1);
        assert_eq!(batch.trace().stats().skipped_rules, 1);
    }

    #[test]
    fn pipeline_executes_named_stages_in_order() {
        let scan_to_filter = RenameRule {
            id: RuleId::new("scan_to_filter", RuleKind::Transformation),
            priority: 50,
            input: "scan",
            output: Some("filter"),
        };
        let filter_to_seek = RenameRule {
            id: RuleId::new("filter_to_seek", RuleKind::Implementation),
            priority: 100,
            input: "filter",
            output: Some("seek"),
        };
        let pipeline = OptimizationPipeline::new(vec![
            RuleStage::new(
                OptimizationStage::new("logical_rewrite", ApplyOrder::TopDown),
                vec![&scan_to_filter],
            ),
            RuleStage::new(
                OptimizationStage::new("access_path_selection", ApplyOrder::BottomUp),
                vec![&filter_to_seek],
            ),
        ]);

        let execution = pipeline.execute(TestExpr("scan"));

        assert_eq!(execution.expression(), &TestExpr("seek"));
        assert_eq!(execution.events().len(), 2);
        assert_eq!(execution.traces().len(), 2);
        assert_eq!(execution.traces()[0].name(), "logical_rewrite");
        assert_eq!(execution.traces()[1].name(), "access_path_selection");
        assert_eq!(execution.traces()[0].stats().applied_rules, 1);
        assert_eq!(execution.traces()[1].stats().applied_rules, 1);
    }
}
