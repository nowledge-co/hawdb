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

//! Step order for `GraphMatchExec`.
//!
//! The binder emits one `Node` step per pattern path followed by its
//! expansions, in text order. The executor runs those steps as a nested loop
//! for every input row, so a path whose first node is fresh scans its label
//! (or every node) once per row, even when a later node of the same path is
//! already bound. This module restarts each path at its best anchor and walks
//! outwards, flipping the direction of the relationships it crosses backwards.
//! Every step after a path's anchor is then an expansion from a bound node.
//!
//! A MATCH is a conjunction of node and relationship constraints with
//! pairwise-distinct relationships, so step order does not change the
//! multiset of rows; it changes only how many candidates are visited.

use super::super::pattern_start::{self, PathEdge, PathNode, StartDecision};
use super::super::OptimizerCatalog;
use crate::{OptimizerQualificationOptions, OptionalOptimizerRule, RuleEvent};
use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan_cypher::{GraphMatchNode, GraphMatchProgram, GraphMatchStep};
use std::collections::{BTreeMap, BTreeSet};

/// Why a node may start a path. Lower ranks are preferred.
///
/// Within a path, the cost model may then move the start to an
/// index-seekable node that is clearly cheaper (#1485).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum AnchorRank {
    /// Bound in every input row: an imported column or an earlier MATCH.
    Bound,
    /// The path's first node in text order, which scans its label.
    TextStart,
}

/// Steps reordered so that each path starts at its chosen anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AnchoredSteps {
    /// The reordered steps, or `None` when text order is already chosen.
    pub(super) steps: Option<Vec<GraphMatchStep>>,
    /// Bound variables that start a path, in execution order.
    pub(super) anchors: Vec<String>,
    /// Cost-based start decisions, one per path with a seekable candidate.
    pub(super) events: Vec<RuleEvent>,
}

/// Statistics and rule controls for cost-based path starts.
#[derive(Clone, Copy)]
pub(super) struct StartCosting<'a> {
    pub(super) catalog: &'a OptimizerCatalog,
    pub(super) options: OptimizerQualificationOptions,
}

/// One relationship of a path, oriented from `nodes[i]` to `nodes[i + 1]`.
#[derive(Debug, Clone)]
struct PathRelationship {
    relationship: Option<String>,
    rel_type: String,
    rel_types: Vec<String>,
    properties: BTreeMap<String, Value>,
    direction: RelationshipDirection,
    min_hops: usize,
    max_hops: usize,
}

#[derive(Debug, Clone)]
struct PatternPath {
    nodes: Vec<GraphMatchNode>,
    relationships: Vec<PathRelationship>,
}

/// Returns the anchored step order, or `None` when the steps are not plain
/// paths. `steps` is `None` when text order already starts every path at its
/// chosen node.
pub(super) fn anchor_graph_match_steps(
    program: &GraphMatchProgram,
    costing: StartCosting<'_>,
) -> Option<AnchoredSteps> {
    let paths = split_paths(&program.steps)?;
    // Variables not introduced by this MATCH are bound before its first step.
    let mut bound: BTreeSet<&str> = paths
        .iter()
        .flat_map(|path| path.nodes.iter())
        .map(|node| node.variable.as_str())
        .filter(|variable| !program.introduced.iter().any(|name| name == variable))
        .collect();
    let mut remaining: Vec<&PatternPath> = paths.iter().collect();
    let mut steps = Vec::with_capacity(program.steps.len());
    let mut anchors = Vec::new();
    let mut events = Vec::new();
    while !remaining.is_empty() {
        // Best anchor across the remaining paths; ties keep text order.
        let (slot, anchor) = remaining
            .iter()
            .enumerate()
            .filter_map(|(slot, path)| {
                best_anchor(path, &bound).map(|(anchor, rank)| (slot, anchor, rank))
            })
            .min_by_key(|(slot, _, rank)| (*rank, *slot))
            .map(|(slot, anchor, _)| (slot, anchor))
            .expect("every path has a text-order start");
        let path = remaining.remove(slot);
        let start = costed_start(path, anchor, &bound, costing, &mut events);
        if bound.contains(path.nodes[start].variable.as_str()) {
            anchors.push(path.nodes[start].variable.clone());
        }
        emit_path(path, start, &mut steps);
        bound.extend(path.nodes.iter().map(|node| node.variable.as_str()));
    }
    Some(AnchoredSteps {
        steps: (steps != program.steps).then_some(steps),
        anchors,
        events,
    })
}

/// Moves the path's start from `anchor` to an index-seekable node when the
/// cost model finds it clearly cheaper per input row.
fn costed_start(
    path: &PatternPath,
    anchor: usize,
    bound: &BTreeSet<&str>,
    costing: StartCosting<'_>,
    events: &mut Vec<RuleEvent>,
) -> usize {
    let nodes = path
        .nodes
        .iter()
        .map(|node| PathNode {
            variable: &node.variable,
            label: &node.label,
            equalities: node
                .properties
                .iter()
                .filter(|(_, value)| **value != Value::Null)
                .map(|(property, _)| property.as_str())
                .collect(),
            bound: bound.contains(node.variable.as_str()),
        })
        .collect::<Vec<_>>();
    let edges = path
        .relationships
        .iter()
        .map(|relationship| PathEdge {
            rel_type: (relationship.rel_types.is_empty() && !relationship.rel_type.is_empty())
                .then_some(relationship.rel_type.as_str()),
            equalities: relationship.properties.keys().map(String::as_str).collect(),
            direction: relationship.direction,
            min_hops: relationship.min_hops,
            max_hops: relationship.max_hops,
            reversible: relationship.relationship.is_none()
                || (relationship.min_hops == 1 && relationship.max_hops == 1),
        })
        .collect::<Vec<_>>();
    let rule = OptionalOptimizerRule::CostBasedPatternStart;
    match pattern_start::choose_start(costing.catalog, &nodes, &edges, anchor) {
        None => anchor,
        Some(StartDecision::Kept { detail }) => {
            events.push(RuleEvent::estimated(
                rule.as_str(),
                format!(
                    "keep GraphMatchExec path start {}: {detail}",
                    nodes[anchor].variable
                ),
            ));
            anchor
        }
        Some(StartDecision::Chosen(choice)) => {
            let node = &nodes[choice.start];
            let detail = format!(
                "start GraphMatchExec path at {} with an index seek on {}.{}: estimated work {:.1} per input row instead of {:.1} from {}",
                node.variable,
                node.label,
                choice.property,
                choice.work,
                choice.current_work,
                nodes[anchor].variable
            );
            if costing.options.rule_enabled(rule) {
                events.push(RuleEvent::applied(rule.as_str(), detail));
                choice.start
            } else {
                events.push(RuleEvent::skipped(
                    rule.as_str(),
                    format!("disabled by statement qualification: {detail}"),
                ));
                anchor
            }
        }
    }
}

fn anchor_rank(
    node: &GraphMatchNode,
    position: usize,
    bound: &BTreeSet<&str>,
) -> Option<AnchorRank> {
    if bound.contains(node.variable.as_str()) {
        Some(AnchorRank::Bound)
    } else if position == 0 {
        Some(AnchorRank::TextStart)
    } else {
        None
    }
}

fn best_anchor(path: &PatternPath, bound: &BTreeSet<&str>) -> Option<(usize, AnchorRank)> {
    path.nodes
        .iter()
        .enumerate()
        .filter_map(|(position, node)| {
            anchor_rank(node, position, bound).map(|rank| (position, rank))
        })
        .min_by_key(|(position, rank)| (*rank, *position))
}

/// Splits steps into paths; `None` for shapes this pass does not reorder.
fn split_paths(steps: &[GraphMatchStep]) -> Option<Vec<PatternPath>> {
    let mut paths: Vec<PatternPath> = Vec::new();
    for step in steps {
        match step {
            GraphMatchStep::Node(node) => paths.push(PatternPath {
                nodes: vec![node.clone()],
                relationships: Vec::new(),
            }),
            GraphMatchStep::Expand {
                source,
                relationship,
                rel_type,
                rel_types,
                properties,
                direction,
                min_hops,
                max_hops,
                target,
            } => {
                let path = paths.last_mut()?;
                if path.nodes.last()?.variable != *source {
                    return None;
                }
                path.relationships.push(PathRelationship {
                    relationship: relationship.clone(),
                    rel_type: rel_type.clone(),
                    rel_types: rel_types.clone(),
                    properties: properties.clone(),
                    direction: *direction,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                });
                path.nodes.push(target.clone());
            }
            // Shortest paths need both endpoints bound by earlier Node steps.
            GraphMatchStep::ShortestPath(_) => return None,
        }
    }
    Some(paths)
}

/// Emits `Node(anchor)`, then expansions towards the path's end, then
/// reversed expansions towards its start.
fn emit_path(path: &PatternPath, anchor: usize, steps: &mut Vec<GraphMatchStep>) {
    steps.push(GraphMatchStep::Node(path.nodes[anchor].clone()));
    for index in anchor..path.relationships.len() {
        steps.push(expand(
            &path.nodes[index],
            &path.relationships[index],
            path.relationships[index].direction,
            &path.nodes[index + 1],
        ));
    }
    for index in (0..anchor).rev() {
        steps.push(expand(
            &path.nodes[index + 1],
            &path.relationships[index],
            pattern_start::reverse(path.relationships[index].direction),
            &path.nodes[index],
        ));
    }
}

fn expand(
    source: &GraphMatchNode,
    relationship: &PathRelationship,
    direction: RelationshipDirection,
    target: &GraphMatchNode,
) -> GraphMatchStep {
    GraphMatchStep::Expand {
        source: source.variable.clone(),
        relationship: relationship.relationship.clone(),
        rel_type: relationship.rel_type.clone(),
        rel_types: relationship.rel_types.clone(),
        properties: relationship.properties.clone(),
        direction,
        min_hops: relationship.min_hops,
        max_hops: relationship.max_hops,
        target: target.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_plan_cypher::{GraphBindingImport, GraphEntityKind};
    use RelationshipDirection::{Incoming, Outgoing, Undirected};

    fn node(variable: &str, label: &str) -> GraphMatchNode {
        GraphMatchNode {
            variable: variable.to_string(),
            label: label.to_string(),
            properties: BTreeMap::new(),
        }
    }

    fn props(name: &str, value: i64) -> BTreeMap<String, Value> {
        BTreeMap::from([(name.to_string(), Value::Int(value))])
    }

    fn hop(
        source: &str,
        rel_type: &str,
        direction: RelationshipDirection,
        target: GraphMatchNode,
    ) -> GraphMatchStep {
        GraphMatchStep::Expand {
            source: source.to_string(),
            relationship: None,
            rel_type: rel_type.to_string(),
            rel_types: Vec::new(),
            properties: BTreeMap::new(),
            direction,
            min_hops: 1,
            max_hops: 1,
            target,
        }
    }

    fn catalog() -> OptimizerCatalog {
        OptimizerCatalog::optimistic()
    }

    fn costing(catalog: &OptimizerCatalog) -> StartCosting<'_> {
        StartCosting {
            catalog,
            options: OptimizerQualificationOptions::default(),
        }
    }

    fn reordered(program: &GraphMatchProgram) -> Option<Vec<GraphMatchStep>> {
        anchor_graph_match_steps(program, costing(&catalog())).and_then(|anchored| anchored.steps)
    }

    fn anchored(program: &GraphMatchProgram, reason: &str) -> AnchoredSteps {
        let anchored = anchor_graph_match_steps(program, costing(&catalog())).expect(reason);
        assert!(anchored.steps.is_some(), "{reason}");
        assert!(anchored.events.is_empty(), "{:?}", anchored.events);
        anchored
    }

    fn program(steps: Vec<GraphMatchStep>, imported: &[&str]) -> GraphMatchProgram {
        let mut introduced = Vec::new();
        for step in &steps {
            let mut names = Vec::new();
            match step {
                GraphMatchStep::Node(node) => names.push(&node.variable),
                GraphMatchStep::Expand {
                    relationship,
                    target,
                    ..
                } => {
                    names.extend(relationship.iter());
                    names.push(&target.variable);
                }
                GraphMatchStep::ShortestPath(step) => names.push(&step.path),
            }
            for name in names {
                if !imported.contains(&name.as_str()) && !introduced.contains(name) {
                    introduced.push(name.clone());
                }
            }
        }
        GraphMatchProgram {
            imports: imported
                .iter()
                .map(|name| GraphBindingImport {
                    variable: name.to_string(),
                    column: name.to_string(),
                    kind: GraphEntityKind::Node,
                })
                .collect(),
            introduced,
            steps,
            predicate_steps: Vec::new(),
            edge_predicates: Vec::new(),
            projected_nodes: BTreeMap::new(),
            projected_relationships: BTreeMap::new(),
            predicate: None,
            optional: true,
        }
    }

    #[test]
    fn imported_end_of_path_reverses_steps_and_directions() {
        // (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum), forum imported.
        let program = program(
            vec![
                GraphMatchStep::Node(node("friend", "")),
                hop("friend", "HAS_CREATOR", Incoming, node("post", "")),
                hop("post", "CONTAINER_OF", Incoming, node("forum", "Forum")),
            ],
            &["forum"],
        );
        let anchored = anchored(&program, "forum anchors the path");
        assert_eq!(anchored.anchors, vec!["forum".to_string()]);
        assert_eq!(
            anchored.steps.unwrap(),
            vec![
                GraphMatchStep::Node(node("forum", "Forum")),
                hop("forum", "CONTAINER_OF", Outgoing, node("post", "")),
                hop("post", "HAS_CREATOR", Outgoing, node("friend", "")),
            ]
        );
    }

    #[test]
    fn bound_middle_node_expands_both_sides_and_keeps_filters() {
        let mut a = node("a", "A");
        a.properties = props("x", 1);
        let mut m = node("m", "Post:Comment");
        m.properties = props("y", 2);
        let program = program(
            vec![
                GraphMatchStep::Node(a.clone()),
                GraphMatchStep::Expand {
                    source: "a".to_string(),
                    relationship: Some("r".to_string()),
                    rel_type: "R".to_string(),
                    rel_types: Vec::new(),
                    properties: props("w", 3),
                    direction: Outgoing,
                    min_hops: 1,
                    max_hops: 1,
                    target: m.clone(),
                },
                hop("m", "S", Undirected, node("c", "")),
            ],
            &["m"],
        );
        let anchored = anchored(&program, "m anchors the path");
        assert_eq!(
            anchored.steps.unwrap(),
            vec![
                GraphMatchStep::Node(m),
                hop("m", "S", Undirected, node("c", "")),
                GraphMatchStep::Expand {
                    source: "m".to_string(),
                    relationship: Some("r".to_string()),
                    rel_type: "R".to_string(),
                    rel_types: Vec::new(),
                    properties: props("w", 3),
                    direction: Incoming,
                    min_hops: 1,
                    max_hops: 1,
                    target: a,
                },
            ]
        );
    }

    #[test]
    fn variable_length_relationship_keeps_bounds_when_reversed() {
        let program = program(
            vec![
                GraphMatchStep::Node(node("a", "Person")),
                GraphMatchStep::Expand {
                    source: "a".to_string(),
                    relationship: None,
                    rel_type: "KNOWS".to_string(),
                    rel_types: Vec::new(),
                    properties: BTreeMap::new(),
                    direction: Outgoing,
                    min_hops: 0,
                    max_hops: 3,
                    target: node("b", "Person"),
                },
            ],
            &["b"],
        );
        let anchored = anchored(&program, "b anchors the path");
        assert_eq!(
            anchored.steps.unwrap(),
            vec![
                GraphMatchStep::Node(node("b", "Person")),
                GraphMatchStep::Expand {
                    source: "b".to_string(),
                    relationship: None,
                    rel_type: "KNOWS".to_string(),
                    rel_types: Vec::new(),
                    properties: BTreeMap::new(),
                    direction: Incoming,
                    min_hops: 0,
                    max_hops: 3,
                    target: node("a", "Person"),
                },
            ]
        );
    }

    #[test]
    fn text_order_is_kept_without_a_better_anchor() {
        let unbound = program(
            vec![
                GraphMatchStep::Node(node("a", "")),
                hop("a", "R", Outgoing, node("b", "B")),
            ],
            &[],
        );
        assert_eq!(reordered(&unbound), None);
        let bound_first = program(
            vec![
                GraphMatchStep::Node(node("a", "")),
                hop("a", "R", Outgoing, node("b", "B")),
            ],
            &["a", "b"],
        );
        assert_eq!(reordered(&bound_first), None);
    }

    #[test]
    fn comma_patterns_start_from_bound_paths_and_chain_new_bindings() {
        // MATCH (x)-[:R]->(y), (y)<-[:S]-(z)-[:T]->(b) with b bound.
        let program = program(
            vec![
                GraphMatchStep::Node(node("x", "")),
                hop("x", "R", Outgoing, node("y", "")),
                GraphMatchStep::Node(node("y", "")),
                hop("y", "S", Incoming, node("z", "")),
                hop("z", "T", Outgoing, node("b", "")),
            ],
            &["b"],
        );
        let anchored = anchored(&program, "b anchors the second path");
        assert_eq!(
            anchored.steps.unwrap(),
            vec![
                GraphMatchStep::Node(node("b", "")),
                hop("b", "T", Incoming, node("z", "")),
                hop("z", "S", Outgoing, node("y", "")),
                // y is bound now, so the first path starts from its end.
                GraphMatchStep::Node(node("y", "")),
                hop("y", "R", Incoming, node("x", "")),
            ]
        );
        assert_eq!(anchored.anchors, vec!["b".to_string(), "y".to_string()]);
    }

    #[test]
    fn repeated_variables_close_cycles_from_the_anchor() {
        // (a)-[:R]->(b)-[:R]->(a) with b bound.
        let program = program(
            vec![
                GraphMatchStep::Node(node("a", "")),
                hop("a", "R", Outgoing, node("b", "")),
                hop("b", "R", Outgoing, node("a", "")),
            ],
            &["b"],
        );
        let anchored = anchored(&program, "b anchors the cycle");
        assert_eq!(
            anchored.steps.unwrap(),
            vec![
                GraphMatchStep::Node(node("b", "")),
                hop("b", "R", Outgoing, node("a", "")),
                hop("b", "R", Incoming, node("a", "")),
            ]
        );
    }

    #[test]
    fn a_seekable_node_starts_the_path_when_clearly_cheaper() {
        let catalog = super::super::super::pattern_start::tests::catalog();
        let mut place = node("p", "Place");
        place.properties =
            BTreeMap::from([("name".to_string(), Value::String("Country3".to_string()))]);
        let program = program(
            vec![
                GraphMatchStep::Node(node("f", "Person")),
                hop("f", "WORK_AT", Outgoing, node("c", "Organisation")),
                hop("c", "IS_LOCATED_IN", Outgoing, place.clone()),
            ],
            &[],
        );
        let anchored = anchor_graph_match_steps(&program, costing(&catalog)).unwrap();
        assert_eq!(
            anchored.steps,
            Some(vec![
                GraphMatchStep::Node(place),
                hop("p", "IS_LOCATED_IN", Incoming, node("c", "Organisation")),
                hop("c", "WORK_AT", Incoming, node("f", "Person")),
            ])
        );
        assert!(anchored.anchors.is_empty());
        assert!(
            matches!(anchored.events.as_slice(), [event] if event.outcome() == crate::RuleOutcome::Applied
                && event.detail().starts_with("start GraphMatchExec path at p with an index seek on Place.name")),
            "{:?}",
            anchored.events
        );

        let disabled = StartCosting {
            catalog: &catalog,
            options: OptimizerQualificationOptions::default()
                .without_rule(OptionalOptimizerRule::CostBasedPatternStart),
        };
        let kept = anchor_graph_match_steps(&program, disabled).unwrap();
        assert_eq!(kept.steps, None);
        assert!(
            matches!(kept.events.as_slice(), [event] if event.outcome() == crate::RuleOutcome::Skipped),
            "{:?}",
            kept.events
        );

        // A bound person is a cheaper start than the place seek.
        let bound = program_with_steps(&program, &["f"]);
        let anchored = anchor_graph_match_steps(&bound, costing(&catalog)).unwrap();
        assert_eq!(anchored.steps, None);
        assert!(
            matches!(anchored.events.as_slice(), [event] if event.outcome() == crate::RuleOutcome::Estimated),
            "{:?}",
            anchored.events
        );
    }

    fn program_with_steps(source: &GraphMatchProgram, imported: &[&str]) -> GraphMatchProgram {
        program(source.steps.clone(), imported)
    }

    #[test]
    fn shortest_path_steps_are_left_unchanged() {
        let mut program = program(
            vec![
                GraphMatchStep::Node(node("a", "")),
                GraphMatchStep::Node(node("b", "")),
            ],
            &["b"],
        );
        program.steps.push(GraphMatchStep::ShortestPath(Box::new(
            hawdb_plan_cypher::GraphMatchShortestPath {
                path: "p".to_string(),
                source: "a".to_string(),
                target: "b".to_string(),
                rel_type: "R".to_string(),
                direction: Outgoing,
                min_hops: 1,
                max_hops: 3,
                bounded: true,
                selection: hawdb_plan_cypher::ShortestPathSelection::Any,
            },
        )));
        assert_eq!(
            anchor_graph_match_steps(&program, costing(&catalog())),
            None
        );
    }
}
