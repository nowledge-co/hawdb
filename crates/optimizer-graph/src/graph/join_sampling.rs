// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Query-local statistics for both endpoint orientations of a small inner path.
//! Estimates affect ranking only; shape checks establish rewrite legality.

use super::OptimizerCatalog;
use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan_cypher::{LogicalPlan, PhysicalPlan, Predicate};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamplingNode {
    pub variable: String,
    pub label: String,
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamplingHop {
    pub variable: Option<String>,
    pub rel_type: String,
    pub direction: RelationshipDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSamplingShape {
    pub nodes: Vec<SamplingNode>,
    pub hops: Vec<SamplingHop>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampledPath {
    pub seed_rows: u64,
    pub work_rows: u64,
    /// Candidate rows before, and output rows after, the target equality.
    pub hops: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampledMatchStatistics {
    pub shape: JoinSamplingShape,
    pub forward: SampledPath,
    pub reverse: SampledPath,
}

impl SampledMatchStatistics {
    pub fn is_valid(&self) -> bool {
        let hops = self.shape.hops.len();
        (1..=2).contains(&hops)
            && self.shape.nodes.len() == hops + 1
            && [&self.forward, &self.reverse].iter().all(|path| {
                path.hops.len() == hops
                    && path.seed_rows > 0
                    && path.work_rows >= path.seed_rows
                    && path.hops.iter().all(|(candidates, output)| {
                        *candidates > 0 && *output > 0 && output <= candidates
                    })
            })
    }

    /// Require a substantial predicted work reduction and compatible output
    /// estimates. This is an activation heuristic, not a confidence interval.
    pub fn prefers_reverse(&self) -> bool {
        let outputs = self.forward.hops.last().zip(self.reverse.hops.last());
        self.reverse.work_rows.saturating_mul(2) < self.forward.work_rows
            && outputs.is_some_and(|((_, forward), (_, reverse))| {
                *forward > 0
                    && *reverse > 0
                    && *forward <= reverse.saturating_mul(4)
                    && *reverse <= forward.saturating_mul(4)
            })
    }

    fn chosen(&self) -> (&SampledPath, bool) {
        if self.prefers_reverse() {
            (&self.reverse, true)
        } else {
            (&self.forward, false)
        }
    }

    fn seed(&self) -> (&SamplingNode, u64) {
        let (path, reverse) = self.chosen();
        (
            &self.shape.nodes[if reverse {
                self.shape.nodes.len() - 1
            } else {
                0
            }],
            path.seed_rows,
        )
    }

    pub(super) fn expansion(&self, source: &str, target: &str) -> Option<u64> {
        let (path, reverse) = self.chosen();
        self.shape.hops.iter().enumerate().find_map(|(index, _)| {
            let (from, to, step) = if reverse {
                (index + 1, index, self.shape.hops.len() - 1 - index)
            } else {
                (index, index + 1, index)
            };
            (self.shape.nodes[from].variable == source && self.shape.nodes[to].variable == target)
                .then(|| path.hops[step].0)
        })
    }

    pub(super) fn seed_rows(&self, variable: &str, label: &str, property: &str) -> Option<u64> {
        let (seed, rows) = self.seed();
        (seed.variable == variable && seed.label == label && seed.property == property)
            .then_some(rows)
    }

    pub(super) fn filtered_rows(&self, predicate: &Predicate, input: &PhysicalPlan) -> Option<u64> {
        // The closed shape has exactly one equality per variable. Matching its
        // value here would compare an opaque template slot with a bound value;
        // the host fences both parameters and snapshot in the physical cache.
        let Predicate::PropertyEq {
            variable, property, ..
        } = predicate
        else {
            return None;
        };
        let node = self
            .shape
            .nodes
            .iter()
            .find(|node| node.variable == *variable && node.property == *property)?;
        let (path, reverse) = self.chosen();
        match input {
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                target_variable,
                ..
            } if target_variable == &node.variable => {
                self.shape.hops.iter().enumerate().find_map(|(index, _)| {
                    let (from, to, step) = if reverse {
                        (index + 1, index, self.shape.hops.len() - 1 - index)
                    } else {
                        (index, index + 1, index)
                    };
                    (self.shape.nodes[from].variable == *source_variable
                        && self.shape.nodes[to].variable == *target_variable)
                        .then(|| path.hops[step].1)
                })
            }
            PhysicalPlan::SeqNodeScan {
                variable: source, ..
            }
            | PhysicalPlan::IndexNodeSeek {
                variable: source, ..
            } if source == variable => {
                let (seed, rows) = self.seed();
                (seed.variable == *variable).then_some(rows)
            }
            _ => None,
        }
    }
}

impl JoinSamplingShape {
    /// One closed path, with one mandatory bounded scalar equality per node.
    /// Keep LIMIT, OPTIONAL, shared bindings and repeated relationship types
    /// outside this initial capability rather than changing their semantics.
    pub fn from_logical(plan: &LogicalPlan) -> Option<Self> {
        Self::from_logical_with_value_check(plan, &scalar_ok)
    }

    /// The host may admit bounded opaque parameter slots after validating their
    /// bound scalar values, without teaching the optimizer the slot encoding.
    pub fn from_logical_with_value_check(
        plan: &LogicalPlan,
        value_ok: &impl Fn(&Value) -> bool,
    ) -> Option<Self> {
        let root = path_root(plan, 0)?;
        if !has_expand(root, 0) {
            return None;
        }
        let mut nodes = Vec::new();
        let mut hops = Vec::new();
        let mut predicates = BTreeMap::new();
        collect_path(root, &mut nodes, &mut hops, &mut predicates, 0, value_ok)?;
        if !(1..=2).contains(&hops.len())
            || nodes.len() != hops.len() + 1
            || predicates.len() != nodes.len()
        {
            return None;
        }
        let mut variables = BTreeSet::new();
        let mut labels = Vec::new();
        for (variable, label) in nodes {
            if !name_ok(&variable) || !name_ok(&label) || !variables.insert(variable.clone()) {
                return None;
            }
            let (property, value) = predicates.remove(&variable)?;
            labels.push(SamplingNode {
                variable,
                label,
                property,
                value,
            });
        }
        let mut types = BTreeSet::new();
        for hop in &hops {
            if !name_ok(&hop.rel_type) || !types.insert(hop.rel_type.as_str()) {
                return None;
            }
            if let Some(variable) = &hop.variable
                && (!name_ok(variable) || !variables.insert(variable.clone()))
            {
                return None;
            }
        }
        Some(Self {
            nodes: labels,
            hops,
        })
    }

    fn plan(&self, reverse: bool) -> LogicalPlan {
        let seed = if reverse { self.nodes.len() - 1 } else { 0 };
        let mut plan = self.filtered(
            seed,
            LogicalPlan::NodeScan {
                variable: self.nodes[seed].variable.clone(),
                label: self.nodes[seed].label.clone(),
            },
        );
        for step in 0..self.hops.len() {
            let index = if reverse {
                self.hops.len() - 1 - step
            } else {
                step
            };
            let (from, to) = if reverse {
                (index + 1, index)
            } else {
                (index, index + 1)
            };
            let hop = &self.hops[index];
            let direction = if reverse {
                flip(hop.direction)
            } else {
                hop.direction
            };
            plan = self.filtered(
                to,
                LogicalPlan::Expand {
                    source_variable: self.nodes[from].variable.clone(),
                    source_label: self.nodes[from].label.clone(),
                    rel_variable: hop.variable.clone(),
                    rel_type: hop.rel_type.clone(),
                    rel_properties: BTreeMap::new(),
                    direction,
                    target_variable: self.nodes[to].variable.clone(),
                    target_label: self.nodes[to].label.clone(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(plan),
                },
            );
        }
        plan
    }

    fn filtered(&self, index: usize, input: LogicalPlan) -> LogicalPlan {
        let node = &self.nodes[index];
        LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: node.variable.clone(),
                property: node.property.clone(),
                value: node.value.clone(),
            },
            input: Box::new(input),
        }
    }
}

fn name_ok(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128
}

fn has_expand(plan: &LogicalPlan, depth: usize) -> bool {
    match plan {
        LogicalPlan::Expand { .. } => true,
        LogicalPlan::Filter { input, .. } if depth < 8 => has_expand(input, depth + 1),
        _ => false,
    }
}

fn scalar_ok(value: &Value) -> bool {
    matches!(value, Value::Int(_) | Value::Bool(_))
        || matches!(value, Value::String(text) if text.len() <= 256)
}

fn path_root(plan: &LogicalPlan, depth: usize) -> Option<&LogicalPlan> {
    if depth > 8 {
        return None;
    }
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Distinct { input } => path_root(input, depth + 1),
        LogicalPlan::Filter { .. } | LogicalPlan::Expand { .. } => Some(plan),
        _ => None,
    }
}

type Equalities = BTreeMap<String, (String, Value)>;

fn collect_predicate(
    predicate: &Predicate,
    predicates: &mut Equalities,
    visited: &mut usize,
    value_ok: &impl Fn(&Value) -> bool,
) -> Option<()> {
    *visited += 1;
    if *visited > 16 {
        return None;
    }
    match predicate {
        Predicate::And(parts) => {
            for part in parts {
                collect_predicate(part, predicates, visited, value_ok)?;
            }
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } if name_ok(variable) && name_ok(property) && value_ok(value) => {
            let pair = (property.clone(), value.clone());
            if predicates.get(variable).is_some_and(|old| old != &pair) {
                return None;
            }
            predicates.insert(variable.clone(), pair);
        }
        _ => return None,
    }
    Some(())
}

fn collect_path(
    plan: &LogicalPlan,
    nodes: &mut Vec<(String, String)>,
    hops: &mut Vec<SamplingHop>,
    predicates: &mut Equalities,
    depth: usize,
    value_ok: &impl Fn(&Value) -> bool,
) -> Option<()> {
    if depth > 8 {
        return None;
    }
    match plan {
        LogicalPlan::NodeScan { variable, label } => nodes.push((variable.clone(), label.clone())),
        LogicalPlan::Filter { predicate, input } => {
            collect_predicate(predicate, predicates, &mut 0, value_ok)?;
            collect_path(input, nodes, hops, predicates, depth + 1, value_ok)?;
        }
        LogicalPlan::Expand {
            source_variable,
            source_label,
            target_variable,
            target_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input,
        } if rel_properties.is_empty()
            && matches!(
                direction,
                RelationshipDirection::Outgoing | RelationshipDirection::Incoming
            ) =>
        {
            collect_path(input, nodes, hops, predicates, depth + 1, value_ok)?;
            if nodes.last()? != &(source_variable.clone(), source_label.clone()) || hops.len() >= 2
            {
                return None;
            }
            nodes.push((target_variable.clone(), target_label.clone()));
            hops.push(SamplingHop {
                variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                direction: *direction,
            });
        }
        _ => return None,
    }
    Some(())
}

fn flip(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    }
}

pub(super) fn rewrite(plan: &LogicalPlan, catalog: &OptimizerCatalog) -> Option<LogicalPlan> {
    let statistics = catalog.sampled_match.as_ref()?;
    let value_ok = |value: &Value| {
        statistics
            .shape
            .nodes
            .iter()
            .any(|node| &node.value == value)
    };
    if !statistics.prefers_reverse()
        || JoinSamplingShape::from_logical_with_value_check(plan, &value_ok)? != statistics.shape
    {
        return None;
    }
    let mut output = plan.clone();
    let mut cursor = &mut output;
    loop {
        cursor = match cursor {
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input } => input,
            _ => {
                *cursor = statistics.shape.plan(true);
                break;
            }
        };
    }
    Some(output)
}

impl OptimizerCatalog {
    /// These statistics belong to one bound path and one immutable query read.
    /// They are never inserted into the shared schema/statistics catalog.
    pub fn with_sampled_match_statistics(mut self, statistics: SampledMatchStatistics) -> Self {
        if statistics.is_valid() {
            self.sampled_match = Some(statistics);
        }
        self
    }
}

hawdb_core::impl_retained_heap_for_struct!(SamplingNode {
    variable,
    label,
    property,
    value
});
hawdb_core::impl_retained_heap_for_struct!(SamplingHop {
    variable,
    rel_type,
    direction
});
hawdb_core::impl_retained_heap_for_struct!(JoinSamplingShape { nodes, hops });
hawdb_core::impl_retained_heap_for_struct!(SampledPath {
    seed_rows,
    work_rows,
    hops
});
hawdb_core::impl_retained_heap_for_struct!(SampledMatchStatistics {
    shape,
    forward,
    reverse
});

#[cfg(test)]
mod tests {
    use super::*;

    fn statistics() -> SampledMatchStatistics {
        SampledMatchStatistics {
            shape: JoinSamplingShape {
                nodes: ["a", "b", "c"]
                    .into_iter()
                    .map(|name| SamplingNode {
                        variable: name.into(),
                        label: name.to_uppercase(),
                        property: "kind".into(),
                        value: Value::Int(1),
                    })
                    .collect(),
                hops: ["R", "S"]
                    .into_iter()
                    .map(|name| SamplingHop {
                        variable: Some(name.to_lowercase()),
                        rel_type: name.into(),
                        direction: RelationshipDirection::Outgoing,
                    })
                    .collect(),
            },
            forward: SampledPath {
                seed_rows: 3,
                work_rows: 30,
                hops: vec![(20, 4), (7, 7)],
            },
            reverse: SampledPath {
                seed_rows: 1,
                work_rows: 11,
                hops: vec![(3, 3), (7, 7)],
            },
        }
    }

    #[test]
    fn sampled_path_reversal_keeps_bindings_and_rejects_unsafe_shapes() {
        let statistics = statistics();
        assert!(statistics.is_valid() && statistics.prefers_reverse());
        let plan = statistics.shape.plan(false);
        assert_eq!(
            JoinSamplingShape::from_logical(&plan),
            Some(statistics.shape.clone())
        );
        let catalog = OptimizerCatalog::default().with_sampled_match_statistics(statistics.clone());
        assert_eq!(rewrite(&plan, &catalog), Some(statistics.shape.plan(true)));
        assert_eq!(statistics.expansion("c", "b"), Some(3));
        assert_eq!(statistics.expansion("b", "a"), Some(7));
        assert_eq!(statistics.expansion("a", "b"), None);
        let mut shared_type = statistics.shape.clone();
        shared_type.hops[1].rel_type = "R".into();
        assert!(JoinSamplingShape::from_logical(&shared_type.plan(false)).is_none());
        let mut shared_binding = statistics.shape.clone();
        shared_binding.nodes[2].variable = "a".into();
        assert!(JoinSamplingShape::from_logical(&shared_binding.plan(false)).is_none());
    }

    #[test]
    fn sampled_statistics_require_complete_positive_compatible_paths() {
        let mut sample = statistics();
        sample.reverse.hops.pop();
        assert!(!sample.is_valid());
        assert!(OptimizerCatalog::default()
            .with_sampled_match_statistics(sample)
            .sampled_match
            .is_none());
        let mut sample = statistics();
        sample.reverse.hops[1] = (7, 8);
        assert!(!sample.is_valid());
        let mut sample = statistics();
        sample.reverse.hops[1] = (7, 1);
        assert!(
            !sample.prefers_reverse(),
            "incompatible output estimates must not activate reversal"
        );
        let mut sample = statistics();
        sample.reverse.work_rows = 15;
        assert!(
            !sample.prefers_reverse(),
            "require more than a twofold reduction"
        );
    }
}
