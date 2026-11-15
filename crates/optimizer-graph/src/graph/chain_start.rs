// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cost-based start of a linear `Expand` chain (#1485).
//!
//! A first MATCH whose relationship types are distinct normalizes to a
//! `NodeScan` of its first node in text order followed by one `Expand` per
//! relationship. When a later node of that chain is index-seekable and
//! clearly cheaper to start from (see [`super::pattern_start`]), this pass
//! rebuilds the chain from that node: it scans the new start, expands towards
//! the chain's end, then back to its first node with each crossed direction
//! flipped. Every filter of the chain is lifted above the new chain, and the
//! ordinary logical rewrite pushes each conjunct back down to the expansion
//! that binds its variables, so the new start's equality becomes its seek.
//!
//! An `Expand` chain is a conjunction of independent relationship
//! constraints; it enforces no uniqueness between its expansions (the binder
//! only builds one when the types differ, or when the clauses differ), and a
//! variable-length trail is the same trail walked either way. Walking it
//! from another node therefore changes only the work and the row order, not
//! the row multiset. A named variable-length relationship binds a list in
//! text order and is never walked backwards.

use super::pattern_start::{self, PathEdge, PathNode, StartDecision};
use super::OptimizerCatalog;
use crate::{OptimizerQualificationOptions, OptionalOptimizerRule, RuleEvent};
use hawdb_plan_cypher::{
    GraphBindingImport, GraphEntityKind, GraphMatchHashJoin, GraphMatchNode, GraphMatchProgram,
    GraphMatchStep, LogicalPlan, Predicate,
};
use std::collections::{BTreeMap, BTreeSet};

/// A split of a chain at `meeting` that the hash join makes clearly cheaper.
struct ChainSplit {
    meeting: usize,
    estimate: pattern_start::MeetingEstimate,
    input_rows: f64,
    total: f64,
}

/// Rewrites every eligible chain in `plan`.
pub(super) fn rewrite(
    plan: LogicalPlan,
    catalog: &OptimizerCatalog,
    options: OptimizerQualificationOptions,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    // Optimistic catalogs carry no path statistics, so no chain is estimated.
    if catalog.path_counts.is_empty() {
        return plan;
    }
    visit(plan, catalog, options, events)
}

fn visit(
    plan: LogicalPlan,
    catalog: &OptimizerCatalog,
    options: OptimizerQualificationOptions,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    if let Some(chain) = Chain::collect(&plan) {
        // A chain's only leaf is its scan; there is nothing else to visit.
        return chain.restart(catalog, options, events).unwrap_or(plan);
    }
    let mut child = |input: Box<LogicalPlan>| Box::new(visit(*input, catalog, options, events));
    match plan {
        LogicalPlan::VectorIndexSearch(scan) => LogicalPlan::VectorIndexSearch(Box::new(
            (*scan).map_input(|input| visit(input, catalog, options, events)),
        )),
        LogicalPlan::GraphMatch { program, input } => LogicalPlan::GraphMatch {
            program,
            input: input.map(child),
        },
        LogicalPlan::NodeCartesianProduct { left, right } => {
            let left = child(left);
            LogicalPlan::NodeCartesianProduct {
                left,
                right: child(right),
            }
        }
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            node_visibility_predicate,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            node_visibility_predicate,
            input: child(input),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input: child(input),
        },
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: child(input),
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: child(input),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: child(input),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            graph_keys,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            graph_keys,
            input: child(input),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: child(input),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: child(input),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: child(input),
        },
        leaf => leaf,
    }
}

/// One required expansion of a chain, oriented from `nodes[i]` to
/// `nodes[i + 1]` in text order.
struct ChainExpand<'a> {
    rel_variable: &'a Option<String>,
    rel_type: &'a str,
    rel_properties: &'a std::collections::BTreeMap<String, hawdb_core::Value>,
    direction: hawdb_core::RelationshipDirection,
    min_hops: usize,
    max_hops: usize,
}

struct ChainNode<'a> {
    variable: &'a str,
    label: &'a str,
}

/// `Filter* (Expand | Filter)* NodeScan`, where each required `Expand`
/// continues from the previous one's target and binds fresh variables.
struct Chain<'a> {
    nodes: Vec<ChainNode<'a>>,
    expands: Vec<ChainExpand<'a>>,
    predicates: Vec<&'a Predicate>,
}

impl<'a> Chain<'a> {
    fn collect(plan: &'a LogicalPlan) -> Option<Self> {
        let mut predicates = Vec::new();
        let mut expands = Vec::new();
        let mut current = plan;
        let (variable, label) = loop {
            match current {
                LogicalPlan::Filter { predicate, input } => {
                    predicates.push(predicate);
                    current = input;
                }
                LogicalPlan::Expand {
                    optional: false,
                    input,
                    ..
                } => {
                    expands.push(current);
                    current = input;
                }
                LogicalPlan::NodeScan { variable, label } => break (variable, label),
                _ => return None,
            }
        };
        if expands.is_empty() {
            return None;
        }
        let mut nodes = vec![ChainNode { variable, label }];
        let mut chain_expands = Vec::with_capacity(expands.len());
        let mut variables = BTreeSet::from([variable.as_str()]);
        for expand in expands.into_iter().rev() {
            let LogicalPlan::Expand {
                source_variable,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                ..
            } = expand
            else {
                unreachable!("only expansions were collected");
            };
            let previous = nodes.last().expect("the scan is the first node");
            if source_variable != previous.variable
                || !variables.insert(target_variable)
                || rel_variable
                    .as_ref()
                    .is_some_and(|name| !variables.insert(name))
            {
                return None;
            }
            nodes.push(ChainNode {
                variable: target_variable,
                label: target_label,
            });
            chain_expands.push(ChainExpand {
                rel_variable,
                rel_type,
                rel_properties,
                direction: *direction,
                min_hops: *min_hops,
                max_hops: *max_hops,
            });
        }
        Some(Self {
            nodes,
            expands: chain_expands,
            predicates,
        })
    }

    /// The chain rebuilt from a clearly cheaper start, or `None` to keep it.
    fn restart(
        &self,
        catalog: &OptimizerCatalog,
        options: OptimizerQualificationOptions,
        events: &mut Vec<RuleEvent>,
    ) -> Option<LogicalPlan> {
        let mut conjuncts = Vec::new();
        for predicate in &self.predicates {
            flatten(predicate, &mut conjuncts);
        }
        let nodes = self
            .nodes
            .iter()
            .map(|node| PathNode {
                variable: node.variable,
                label: node.label,
                equalities: conjuncts
                    .iter()
                    .filter_map(|conjunct| match conjunct {
                        Predicate::PropertyEq {
                            variable,
                            property,
                            value,
                        } if variable == node.variable && *value != hawdb_core::Value::Null => {
                            Some(property.as_str())
                        }
                        _ => None,
                    })
                    .collect(),
                values: conjuncts
                    .iter()
                    .filter_map(|conjunct| match conjunct {
                        Predicate::PropertyEq {
                            variable,
                            property,
                            value,
                        } if variable == node.variable => Some((property.as_str(), value)),
                        _ => None,
                    })
                    .collect(),
                bound: false,
            })
            .collect::<Vec<_>>();
        let edges = self
            .expands
            .iter()
            .map(|expand| PathEdge {
                rel_type: (!expand.rel_type.is_empty()).then_some(expand.rel_type),
                equalities: expand.rel_properties.keys().map(String::as_str).collect(),
                values: expand
                    .rel_properties
                    .iter()
                    .map(|(key, value)| (key.as_str(), value))
                    .collect(),
                direction: expand.direction,
                min_hops: expand.min_hops,
                max_hops: expand.max_hops,
                reversible: expand.rel_variable.is_none()
                    || (expand.min_hops == 1 && expand.max_hops == 1),
            })
            .collect::<Vec<_>>();
        let restart = pattern_start::choose_start(catalog, &nodes, &edges, 0);
        if let Some(split) = self.best_split(catalog, &nodes, &edges)
            && !matches!(&restart, Some(StartDecision::Chosen(choice)) if choice.work <= split.total)
        {
            let rule = OptionalOptimizerRule::GraphMatchHashJoin;
            let detail = format!(
                "split Expand chain at {}: expand {} to it, then join a GraphMatchExec built once from {}: estimated work {:.1} + {:.1} build matches + {:.0} probes instead of {:.0} rows x {:.1} per row",
                self.nodes[split.meeting].variable,
                self.nodes[0].variable,
                self.nodes[split.meeting + split.estimate.build_start].variable,
                split.estimate.build,
                split.estimate.build_rows,
                split.input_rows,
                split.input_rows,
                split.estimate.per_row,
            );
            if options.rule_enabled(rule) {
                events.push(RuleEvent::applied(rule.as_str(), detail));
                return Some(self.split(split.meeting, &conjuncts));
            }
            events.push(RuleEvent::skipped(
                rule.as_str(),
                format!("disabled by statement qualification: {detail}"),
            ));
        }
        let rule = OptionalOptimizerRule::CostBasedPatternStart;
        let choice = match restart? {
            StartDecision::Kept { detail } => {
                events.push(RuleEvent::estimated(
                    rule.as_str(),
                    format!("keep Expand chain start {}: {detail}", nodes[0].variable),
                ));
                return None;
            }
            StartDecision::Chosen(choice) => choice,
        };
        let start = &self.nodes[choice.start];
        let detail = format!(
            "start Expand chain at {} with an index seek on {}.{}: estimated work {:.1} instead of {:.1} from {}",
            start.variable,
            start.label,
            choice.property,
            choice.work,
            choice.current_work,
            self.nodes[0].variable
        );
        if !options.rule_enabled(rule) {
            events.push(RuleEvent::skipped(
                rule.as_str(),
                format!("disabled by statement qualification: {detail}"),
            ));
            return None;
        }
        events.push(RuleEvent::applied(rule.as_str(), detail));
        Some(self.rebuild(choice.start))
    }

    /// The cheapest node to split the chain at: the chain runs from its start
    /// to that node, and the rest of the chain is one GraphMatch that the
    /// lowering can run as a hash join on it. Only a rest whose relationship
    /// types are distinct qualifies: a GraphMatch keeps its relationships
    /// pairwise distinct, which an `Expand` chain does not require.
    fn best_split(
        &self,
        catalog: &OptimizerCatalog,
        nodes: &[PathNode<'_>],
        edges: &[PathEdge<'_>],
    ) -> Option<ChainSplit> {
        let mut best: Option<ChainSplit> = None;
        for meeting in 1..self.expands.len() {
            let rest = &self.expands[meeting..];
            let mut types = BTreeSet::new();
            if rest
                .iter()
                .any(|expand| expand.rel_type.is_empty() || !types.insert(expand.rel_type))
            {
                continue;
            }
            let Some(prefix) =
                pattern_start::walk(catalog, &nodes[..=meeting], &edges[..meeting], 0)
            else {
                continue;
            };
            let Some(estimate) =
                pattern_start::meeting(catalog, &nodes[meeting..], &edges[meeting..], 0, 0)
            else {
                continue;
            };
            let input_rows = prefix.rows.max(1.0);
            if !estimate.hash_join_is_cheaper(input_rows) {
                continue;
            }
            let total = prefix.work + estimate.build + estimate.build_rows + input_rows;
            if best.as_ref().is_none_or(|best| total < best.total) {
                best = Some(ChainSplit {
                    meeting,
                    estimate,
                    input_rows,
                    total,
                });
            }
        }
        best
    }

    /// The chain up to `meeting`, then a GraphMatch over the rest that imports
    /// the meeting node. Conjuncts that read only the first part stay on it;
    /// a constant equality on a node of the rest becomes its pattern property
    /// (so the executor can seek it); the others keep their order in the
    /// GraphMatch predicate.
    fn split(&self, meeting: usize, conjuncts: &[&Predicate]) -> LogicalPlan {
        let mut plan = LogicalPlan::NodeScan {
            variable: self.nodes[0].variable.to_string(),
            label: self.nodes[0].label.to_string(),
        };
        let mut first = BTreeSet::from([self.nodes[0].variable.to_string()]);
        for (index, expand) in self.expands[..meeting].iter().enumerate() {
            first.insert(self.nodes[index + 1].variable.to_string());
            first.extend(expand.rel_variable.iter().cloned());
            plan = LogicalPlan::Expand {
                source_variable: self.nodes[index].variable.to_string(),
                source_label: self.nodes[index].label.to_string(),
                rel_variable: expand.rel_variable.clone(),
                rel_type: expand.rel_type.to_string(),
                rel_properties: expand.rel_properties.clone(),
                direction: expand.direction,
                target_variable: self.nodes[index + 1].variable.to_string(),
                target_label: self.nodes[index + 1].label.to_string(),
                min_hops: expand.min_hops,
                max_hops: expand.max_hops,
                optional: false,
                input: Box::new(plan),
            };
        }
        let key = self.nodes[meeting].variable;
        let rest_nodes = self.nodes[meeting + 1..]
            .iter()
            .map(|node| node.variable)
            .collect::<BTreeSet<_>>();
        let mut properties: BTreeMap<&str, BTreeMap<String, hawdb_core::Value>> = BTreeMap::new();
        let mut on_first = Vec::new();
        let mut on_rest = Vec::new();
        for conjunct in conjuncts {
            if super::lowering::predicate_variables(conjunct)
                .iter()
                .all(|name| first.contains(name))
            {
                on_first.push((*conjunct).clone());
                continue;
            }
            if let Predicate::PropertyEq {
                variable,
                property,
                value,
            } = conjunct
                && rest_nodes.contains(variable.as_str())
                && !properties
                    .get(variable.as_str())
                    .is_some_and(|existing| existing.contains_key(property))
            {
                properties
                    .entry(variable.as_str())
                    .or_default()
                    .insert(property.clone(), value.clone());
                continue;
            }
            on_rest.push((*conjunct).clone());
        }
        if !on_first.is_empty() {
            plan = LogicalPlan::Filter {
                predicate: if on_first.len() == 1 {
                    on_first.pop().expect("one conjunct")
                } else {
                    Predicate::And(on_first)
                },
                input: Box::new(plan),
            };
        }
        let node = |index: usize| GraphMatchNode {
            variable: self.nodes[index].variable.to_string(),
            label: self.nodes[index].label.to_string(),
            properties: properties
                .get(self.nodes[index].variable)
                .cloned()
                .unwrap_or_default(),
        };
        let mut steps = vec![GraphMatchStep::Node(GraphMatchNode {
            properties: BTreeMap::new(),
            ..node(meeting)
        })];
        let mut introduced = Vec::new();
        for (offset, expand) in self.expands[meeting..].iter().enumerate() {
            let index = meeting + offset;
            introduced.extend(expand.rel_variable.iter().cloned());
            introduced.push(self.nodes[index + 1].variable.to_string());
            steps.push(GraphMatchStep::Expand {
                source: self.nodes[index].variable.to_string(),
                relationship: expand.rel_variable.clone(),
                rel_type: expand.rel_type.to_string(),
                rel_types: Vec::new(),
                properties: expand.rel_properties.clone(),
                direction: expand.direction,
                min_hops: expand.min_hops,
                max_hops: expand.max_hops,
                target: node(index + 1),
            });
        }
        LogicalPlan::GraphMatch {
            program: GraphMatchProgram {
                imports: vec![GraphBindingImport {
                    variable: key.to_string(),
                    column: key.to_string(),
                    kind: GraphEntityKind::Node,
                }],
                introduced,
                projected_nodes: BTreeMap::new(),
                projected_relationships: BTreeMap::new(),
                steps,
                predicate: match on_rest.len() {
                    0 => None,
                    1 => on_rest.pop(),
                    _ => Some(Predicate::And(on_rest)),
                },
                predicate_steps: Vec::new(),
                edge_predicates: Vec::new(),
                optional: false,
                // Decided here, with this chain's own row estimate; lowering
                // places the build's conjuncts.
                hash_join: Some(GraphMatchHashJoin {
                    key: key.to_string(),
                    build_conjuncts: Vec::new(),
                    residual_order: Vec::new(),
                    residual_placed: 0,
                    probe_conjuncts: 0,
                }),
                id_set_conjuncts: Vec::new(),
                seeded_union: Vec::new(),
            },
            input: Some(Box::new(plan)),
        }
    }

    fn rebuild(&self, start: usize) -> LogicalPlan {
        let mut plan = LogicalPlan::NodeScan {
            variable: self.nodes[start].variable.to_string(),
            label: self.nodes[start].label.to_string(),
        };
        let forward = (start..self.expands.len()).map(|index| (index, index + 1, false));
        let backward = (0..start).rev().map(|index| (index + 1, index, true));
        for (from, to, reversed) in forward.chain(backward) {
            let expand = &self.expands[from.min(to)];
            plan = LogicalPlan::Expand {
                source_variable: self.nodes[from].variable.to_string(),
                source_label: self.nodes[from].label.to_string(),
                rel_variable: expand.rel_variable.clone(),
                rel_type: expand.rel_type.to_string(),
                rel_properties: expand.rel_properties.clone(),
                direction: if reversed {
                    pattern_start::reverse(expand.direction)
                } else {
                    expand.direction
                },
                target_variable: self.nodes[to].variable.to_string(),
                target_label: self.nodes[to].label.to_string(),
                min_hops: expand.min_hops,
                max_hops: expand.max_hops,
                optional: false,
                input: Box::new(plan),
            };
        }
        // Filters keep their original order, innermost first.
        let mut predicates = self
            .predicates
            .iter()
            .rev()
            .map(|predicate| (*predicate).clone())
            .collect::<Vec<_>>();
        match predicates.len() {
            0 => plan,
            1 => LogicalPlan::Filter {
                predicate: predicates.pop().expect("one predicate"),
                input: Box::new(plan),
            },
            _ => LogicalPlan::Filter {
                predicate: Predicate::And(predicates),
                input: Box::new(plan),
            },
        }
    }
}

fn flatten<'p>(predicate: &'p Predicate, output: &mut Vec<&'p Predicate>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                flatten(child, output);
            }
        }
        predicate => output.push(predicate),
    }
}

#[cfg(test)]
mod tests;
