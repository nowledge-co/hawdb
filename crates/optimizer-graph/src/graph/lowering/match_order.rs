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

use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan_cypher::{GraphMatchNode, GraphMatchProgram, GraphMatchStep};
use std::collections::{BTreeMap, BTreeSet};

/// Why a node may start a path. Lower ranks are preferred.
///
/// A cost model can add finer ranks between these two (an index seek, then a
/// label scan ordered by estimated label size) once unlabelled and
/// multi-label counts are meaningful (#1400).
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
    pub(super) steps: Vec<GraphMatchStep>,
    /// Bound variables that start a path, in execution order.
    pub(super) anchors: Vec<String>,
}

/// One relationship of a path, oriented from `nodes[i]` to `nodes[i + 1]`.
#[derive(Debug, Clone)]
struct PathRelationship {
    relationship: Option<String>,
    rel_type: String,
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

/// Returns the anchored step order, or `None` when the text order already
/// starts every path at its best anchor or the steps are not plain paths.
pub(super) fn anchor_graph_match_steps(program: &GraphMatchProgram) -> Option<AnchoredSteps> {
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
    while !remaining.is_empty() {
        // Best anchor across the remaining paths; ties keep text order.
        let (slot, anchor, rank) = remaining
            .iter()
            .enumerate()
            .filter_map(|(slot, path)| {
                best_anchor(path, &bound).map(|(anchor, rank)| (slot, anchor, rank))
            })
            .min_by_key(|(slot, _, rank)| (*rank, *slot))
            .expect("every path has a text-order start");
        let path = remaining.remove(slot);
        if rank == AnchorRank::Bound {
            anchors.push(path.nodes[anchor].variable.clone());
        }
        emit_path(path, anchor, &mut steps);
        bound.extend(path.nodes.iter().map(|node| node.variable.as_str()));
    }
    (steps != program.steps).then_some(AnchoredSteps { steps, anchors })
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
            reverse(path.relationships[index].direction),
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
        properties: relationship.properties.clone(),
        direction,
        min_hops: relationship.min_hops,
        max_hops: relationship.max_hops,
        target: target.clone(),
    }
}

fn reverse(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
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
            properties: BTreeMap::new(),
            direction,
            min_hops: 1,
            max_hops: 1,
            target,
        }
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
            projected_nodes: BTreeMap::new(),
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
        let anchored = anchor_graph_match_steps(&program).expect("forum anchors the path");
        assert_eq!(anchored.anchors, vec!["forum".to_string()]);
        assert_eq!(
            anchored.steps,
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
        let anchored = anchor_graph_match_steps(&program).expect("m anchors the path");
        assert_eq!(
            anchored.steps,
            vec![
                GraphMatchStep::Node(m),
                hop("m", "S", Undirected, node("c", "")),
                GraphMatchStep::Expand {
                    source: "m".to_string(),
                    relationship: Some("r".to_string()),
                    rel_type: "R".to_string(),
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
                    properties: BTreeMap::new(),
                    direction: Outgoing,
                    min_hops: 0,
                    max_hops: 3,
                    target: node("b", "Person"),
                },
            ],
            &["b"],
        );
        let anchored = anchor_graph_match_steps(&program).expect("b anchors the path");
        assert_eq!(
            anchored.steps,
            vec![
                GraphMatchStep::Node(node("b", "Person")),
                GraphMatchStep::Expand {
                    source: "b".to_string(),
                    relationship: None,
                    rel_type: "KNOWS".to_string(),
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
        assert_eq!(anchor_graph_match_steps(&unbound), None);
        let bound_first = program(
            vec![
                GraphMatchStep::Node(node("a", "")),
                hop("a", "R", Outgoing, node("b", "B")),
            ],
            &["a", "b"],
        );
        assert_eq!(anchor_graph_match_steps(&bound_first), None);
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
        let anchored = anchor_graph_match_steps(&program).expect("b anchors the second path");
        assert_eq!(
            anchored.steps,
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
        let anchored = anchor_graph_match_steps(&program).expect("b anchors the cycle");
        assert_eq!(
            anchored.steps,
            vec![
                GraphMatchStep::Node(node("b", "")),
                hop("b", "R", Outgoing, node("a", "")),
                hop("b", "R", Incoming, node("a", "")),
            ]
        );
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
        assert_eq!(anchor_graph_match_steps(&program), None);
    }
}
