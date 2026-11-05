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
use hawdb_plan_cypher::{LogicalPlan, Predicate};
use std::collections::BTreeSet;

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
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
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
                bound: false,
            })
            .collect::<Vec<_>>();
        let edges = self
            .expands
            .iter()
            .map(|expand| PathEdge {
                rel_type: (!expand.rel_type.is_empty()).then_some(expand.rel_type),
                equalities: expand.rel_properties.keys().map(String::as_str).collect(),
                direction: expand.direction,
                min_hops: expand.min_hops,
                max_hops: expand.max_hops,
                reversible: expand.rel_variable.is_none()
                    || (expand.min_hops == 1 && expand.max_hops == 1),
            })
            .collect::<Vec<_>>();
        let rule = OptionalOptimizerRule::CostBasedPatternStart;
        let choice = match pattern_start::choose_start(catalog, &nodes, &edges, 0)? {
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
