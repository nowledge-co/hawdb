// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cost-based start of one linear pattern path (#1485).
//!
//! A path runs as a nested loop from its start node: each step expands every
//! row bound so far. Starting at a selective node that an equality index can
//! seek visits far fewer candidates than starting at a label scan, even when
//! that node comes last in the text. This module estimates the work of
//! walking a path from each candidate start, using only maintained catalog
//! statistics, and names a start only when it is clearly cheaper.
//!
//! The estimate is the sum of the candidate rows every step reads. A step
//! from `u` to `v` reads `rows * fanout`, where the fanout is the number of
//! relationships of that type between the two labels, in the direction the
//! step walks, divided by the size of `u`'s label. Equality filters on `v`
//! then keep `1 / distinct_count` of those rows, and reaching a bound or
//! already visited node keeps `1 / label_count`. Directed variable-length
//! steps prefer published bounded-path counts and otherwise repeat the
//! one-hop fanout per hop. Every input is an average, so the result
//! is unknown (and the current start is kept) when a label or relationship
//! type it needs is missing, unlabelled, a union (`:A:B`, #1400) or a type
//! disjunction, or when advanced path statistics are not published.

use super::OptimizerCatalog;
use hawdb_core::RelationshipDirection;

/// A candidate replaces the current start only when its estimated work is at
/// most half of the current start's.
///
/// Fanouts are label-pair averages and the walk treats steps as independent,
/// so a skewed graph can easily be off by a small factor in either direction
/// (a high-degree start node, or a seek value with more matches than the
/// average). Two estimates within that error band are a tie, and a tie keeps
/// the current plan; a seek that wins by an order of magnitude, the case this
/// targets, is unaffected.
pub(super) const START_COST_MARGIN: f64 = 2.0;

/// Variable-length steps longer than this are not estimated.
const MAX_ESTIMATED_HOPS: usize = 8;

/// One node of a path, in text order.
#[derive(Debug, Clone)]
pub(super) struct PathNode<'a> {
    pub(super) variable: &'a str,
    pub(super) label: &'a str,
    /// Properties compared for equality with a constant or a parameter.
    pub(super) equalities: Vec<&'a str>,
    pub(super) values: std::collections::BTreeMap<&'a str, &'a hawdb_core::Value>,
    /// Bound before the path starts: an imported column or an earlier path.
    pub(super) bound: bool,
}

/// The relationship between `nodes[i]` and `nodes[i + 1]`, in text direction.
#[derive(Debug, Clone)]
pub(super) struct PathEdge<'a> {
    /// The one relationship type, or `None` for an untyped step or a type
    /// disjunction, which this model does not estimate.
    pub(super) rel_type: Option<&'a str>,
    pub(super) equalities: Vec<&'a str>,
    pub(super) values: std::collections::BTreeMap<&'a str, &'a hawdb_core::Value>,
    pub(super) direction: RelationshipDirection,
    pub(super) min_hops: usize,
    pub(super) max_hops: usize,
    /// Whether walking this step against its text direction binds the same
    /// values. A named variable-length relationship is a list in text order,
    /// so a reversed walk would bind it reversed.
    pub(super) reversible: bool,
}

/// A start that is clearly cheaper than the current one.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct StartChoice {
    pub(super) start: usize,
    /// The equality-indexed property the start seeks.
    pub(super) property: String,
    pub(super) work: f64,
    pub(super) current_work: f64,
}

/// The outcome for a path that has an index-seekable candidate.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum StartDecision {
    /// Start at a different node.
    Chosen(StartChoice),
    /// Keep the current start; `detail` says why, for the optimizer trace.
    Kept { detail: String },
}

/// The equality-indexed property that `node` can seek, preferring the one
/// with the fewest estimated rows.
pub(super) fn seekable_property<'a>(
    catalog: &OptimizerCatalog,
    node: &PathNode<'a>,
) -> Option<&'a str> {
    if node.bound || !single_label(node.label) {
        return None;
    }
    node.equalities
        .iter()
        .copied()
        .filter(|property| catalog.has_property_index(node.label, property))
        .min_by_key(|property| {
            node.values.get(property).map_or_else(
                || catalog.estimate_property_index_eq_rows(node.label, property),
                |value| catalog.estimate_property_index_value_rows(node.label, property, value),
            )
        })
}

/// Chooses among `current` and the index-seekable nodes of the path. `None`
/// when no other node can seek or the current start has no estimate, so
/// there is nothing to compare.
pub(super) fn choose_start(
    catalog: &OptimizerCatalog,
    nodes: &[PathNode<'_>],
    edges: &[PathEdge<'_>],
    current: usize,
) -> Option<StartDecision> {
    debug_assert_eq!(nodes.len(), edges.len() + 1);
    let candidates = nodes
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != current)
        .filter_map(|(position, node)| {
            seekable_property(catalog, node).map(|property| (position, property))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }
    // Without an estimate for the current start there is nothing to beat.
    let current_work = walk_work(catalog, nodes, edges, current)?;
    let mut best: Option<StartChoice> = None;
    let mut considered = Vec::new();
    for (start, property) in candidates {
        let Some(work) = walk_work(catalog, nodes, edges, start) else {
            considered.push(format!("{}=unknown", nodes[start].variable));
            continue;
        };
        considered.push(format!("{}={work:.1}", nodes[start].variable));
        if best.as_ref().is_none_or(|best| work < best.work) {
            best = Some(StartChoice {
                start,
                property: property.to_string(),
                work,
                current_work,
            });
        }
    }
    match best {
        Some(choice) if choice.work * START_COST_MARGIN <= current_work => {
            Some(StartDecision::Chosen(choice))
        }
        _ => Some(StartDecision::Kept {
            detail: format!(
                "current start {} estimated work {current_work:.1}; candidates [{}] are not {START_COST_MARGIN}x cheaper",
                nodes[current].variable,
                considered.join(", ")
            ),
        }),
    }
}

/// Estimated candidate rows read when the path starts at `start`, walks to
/// its end and then back to its first node. `None` when an input is unknown
/// or the walk would reverse a step that cannot be reversed.
pub(super) fn walk_work(
    catalog: &OptimizerCatalog,
    nodes: &[PathNode<'_>],
    edges: &[PathEdge<'_>],
    start: usize,
) -> Option<f64> {
    walk(catalog, nodes, edges, start).map(|estimate| estimate.work)
}

/// The estimated work of a walk and the complete matches it produces.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct WalkEstimate {
    pub(super) work: f64,
    pub(super) rows: f64,
}

/// [`walk_work`] with the estimated number of complete matches.
pub(super) fn walk(
    catalog: &OptimizerCatalog,
    nodes: &[PathNode<'_>],
    edges: &[PathEdge<'_>],
    start: usize,
) -> Option<WalkEstimate> {
    // Without published path statistics every fanout would be a guess.
    if catalog.path_counts.is_empty() {
        return None;
    }
    // A repeated variable carries its label on one occurrence only.
    let labelled = nodes
        .iter()
        .map(|node| {
            let label = nodes
                .iter()
                .find(|other| other.variable == node.variable && !other.label.is_empty())
                .map_or(node.label, |other| other.label);
            PathNode {
                label,
                ..node.clone()
            }
        })
        .collect::<Vec<_>>();
    let nodes = labelled.as_slice();
    if edges[..start].iter().any(|edge| !edge.reversible) {
        return None;
    }
    let initial = node_work(catalog, &nodes[start])?;
    let (mut rows, mut work) = (initial.rows, initial.work);
    let mut visited = vec![false; nodes.len()];
    visited[start] = true;
    let steps = (start..edges.len())
        .map(|index| (index, index + 1, edges[index].direction))
        .chain(
            (0..start)
                .rev()
                .map(|index| (index + 1, index, reverse(edges[index].direction))),
        );
    for (from, to, direction) in steps {
        let edge = &edges[from.min(to)];
        let expansion = expand_work(catalog, &nodes[from], &nodes[to], edge, direction)?;
        work += rows * expansion.work;
        let candidates = rows * expansion.rows;
        let target = &nodes[to];
        // A repeated variable closes a cycle: the step only checks identity.
        let repeated = nodes
            .iter()
            .enumerate()
            .any(|(position, node)| visited[position] && node.variable == target.variable);
        rows = if target.bound || repeated {
            candidates / catalog.label_count(target.label) as f64
        } else {
            candidates * node_selectivity(catalog, target)
        };
        visited[to] = true;
    }
    Some(WalkEstimate { work, rows })
}

/// Candidate reads and surviving rows of a path's initial node.
pub(super) fn node_work(catalog: &OptimizerCatalog, node: &PathNode<'_>) -> Option<WalkEstimate> {
    let (rows, work) = if node.bound {
        (1.0, 1.0)
    } else if !single_label(node.label) {
        return None;
    } else if let Some(property) = seekable_property(catalog, node) {
        let seek = node.values.get(property).map_or_else(
            || catalog.estimate_property_index_eq_rows(node.label, property),
            |value| catalog.estimate_property_index_value_rows(node.label, property, value),
        ) as f64;
        let rows = node
            .equalities
            .iter()
            .filter(|other| **other != property)
            .fold(seek, |rows, other| {
                rows * equality_selectivity(
                    catalog,
                    node.label,
                    other,
                    node.values.get(other).copied(),
                )
            });
        (rows, seek + 1.0)
    } else {
        let scanned = catalog.label_count(node.label) as f64;
        (scanned * node_selectivity(catalog, node), scanned)
    };
    Some(WalkEstimate { work, rows })
}

/// A path that meets its input at the node `key`, estimated as a per-row
/// nested loop and as one build run hashed on `key`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MeetingEstimate {
    /// Work of one per-row walk with `key` bound, from its chosen start.
    pub(super) per_row: f64,
    /// Work and matches of one walk with `key` unbound, from `build_start`.
    pub(super) build: f64,
    pub(super) build_rows: f64,
    pub(super) build_start: usize,
}

impl MeetingEstimate {
    /// Whether one build run, a hash insert per build match and a probe per
    /// input row is at most half the per-row nested loop. The work both
    /// plans share (producing the input and the joined rows) is left out,
    /// so the margin applies to the work that differs.
    pub(super) fn hash_join_is_cheaper(&self, input_rows: f64) -> bool {
        let hash_join = self.build + self.build_rows + input_rows;
        hash_join * START_COST_MARGIN <= input_rows * self.per_row
    }
}

/// Estimates the path both ways. `per_row_start` is where the per-row walk
/// starts; the build starts at the text start or a clearly cheaper seek,
/// as an unbound first MATCH would. `None` when an estimate is unknown.
pub(super) fn meeting(
    catalog: &OptimizerCatalog,
    nodes: &[PathNode<'_>],
    edges: &[PathEdge<'_>],
    key: usize,
    per_row_start: usize,
) -> Option<MeetingEstimate> {
    let key_variable = nodes[key].variable;
    let with = |bound: bool| {
        nodes
            .iter()
            .map(|node| PathNode {
                bound: if node.variable == key_variable {
                    bound
                } else {
                    node.bound
                },
                ..node.clone()
            })
            .collect::<Vec<_>>()
    };
    let per_row_nodes = with(true);
    let per_row = match choose_start(catalog, &per_row_nodes, edges, per_row_start) {
        Some(StartDecision::Chosen(choice)) => choice.work,
        _ => walk_work(catalog, &per_row_nodes, edges, per_row_start)?,
    };
    let build_nodes = with(false);
    if build_nodes.iter().any(|node| node.bound) {
        return None;
    }
    let build_start = match choose_start(catalog, &build_nodes, edges, 0) {
        Some(StartDecision::Chosen(choice)) => choice.start,
        _ => 0,
    };
    let build = walk(catalog, &build_nodes, edges, build_start)?;
    Some(MeetingEstimate {
        per_row,
        build: build.work,
        build_rows: build.rows,
        build_start,
    })
}

/// Candidate visits and matching paths per source row. Relationship property
/// tests reduce matches, but do not remove the reads needed to test them.
pub(super) fn expand_work(
    catalog: &OptimizerCatalog,
    from: &PathNode<'_>,
    to: &PathNode<'_>,
    edge: &PathEdge<'_>,
    direction: RelationshipDirection,
) -> Option<WalkEstimate> {
    let rows = fanout(catalog, from, to, edge, direction)?;
    let work = if edge.equalities.is_empty() {
        rows
    } else {
        let unfiltered = PathEdge {
            equalities: Vec::new(),
            values: Default::default(),
            ..edge.clone()
        };
        fanout(catalog, from, to, &unfiltered, direction)?
    };
    Some(WalkEstimate { work, rows })
}

/// Matching paths per source row by one step walking `direction`.
fn fanout(
    catalog: &OptimizerCatalog,
    from: &PathNode<'_>,
    to: &PathNode<'_>,
    edge: &PathEdge<'_>,
    direction: RelationshipDirection,
) -> Option<f64> {
    let rel_type = edge.rel_type?;
    if catalog.path_counts.is_empty() || !single_label(from.label) || !single_label(to.label) {
        return None;
    }
    let paths = |source: &str, target: &str| {
        catalog
            .path_counts
            .get(&(source.to_string(), rel_type.to_string(), target.to_string()))
            .copied()
            .unwrap_or(0) as f64
    };
    let relationships = match direction {
        RelationshipDirection::Outgoing => paths(from.label, to.label),
        RelationshipDirection::Incoming => paths(to.label, from.label),
        RelationshipDirection::Undirected => {
            paths(from.label, to.label) + paths(to.label, from.label)
        }
    };
    let selectivity = edge.equalities.iter().fold(1.0, |selectivity, property| {
        let rows = catalog.relationship_count(rel_type).max(1);
        let selected = edge.values.get(property).and_then(|value| {
            catalog.rel_property_equality_estimate(rel_type, property, value, rows)
        });
        selectivity
            * selected.map_or_else(
                || {
                    1.0 / catalog
                        .rel_property_distinct_count(rel_type, property)
                        .max(1) as f64
                },
                |estimate| estimate.rows.max(1) as f64 / rows as f64,
            )
    });
    let one_hop = relationships / catalog.label_count(from.label) as f64 * selectivity;
    if edge.min_hops == 1 && edge.max_hops == 1 {
        return Some(one_hop);
    }
    if edge.max_hops > MAX_ESTIMATED_HOPS {
        return None;
    }
    // Checkpoint counts describe homogeneous directed walks. An undirected
    // walk can mix directions, so summing the two directed counts would miss
    // those paths. Keep the fanout fallback for it and for missing hop counts.
    Some(
        (edge.min_hops..=edge.max_hops)
            .map(|hop| {
                let (source, target) = match direction {
                    RelationshipDirection::Outgoing => (from.label, to.label),
                    RelationshipDirection::Incoming => (to.label, from.label),
                    RelationshipDirection::Undirected => {
                        return one_hop.powi(hop as i32);
                    }
                };
                if hop == 0 {
                    return 1.0;
                }
                catalog
                    .bounded_path_counts
                    .get(&(
                        source.to_string(),
                        rel_type.to_string(),
                        target.to_string(),
                        hop,
                    ))
                    .map_or_else(
                        || one_hop.powi(hop as i32),
                        |count| {
                            *count as f64 / catalog.label_count(from.label) as f64
                                * selectivity.powi(hop as i32)
                        },
                    )
            })
            .sum(),
    )
}

pub(super) fn node_selectivity(catalog: &OptimizerCatalog, node: &PathNode<'_>) -> f64 {
    if !single_label(node.label) {
        return 1.0;
    }
    node.equalities.iter().fold(1.0, |selectivity, property| {
        selectivity
            * equality_selectivity(
                catalog,
                node.label,
                property,
                node.values.get(property).copied(),
            )
    })
}

fn equality_selectivity(
    catalog: &OptimizerCatalog,
    label: &str,
    property: &str,
    value: Option<&hawdb_core::Value>,
) -> f64 {
    let rows = catalog.label_count(label);
    value.map_or_else(
        || catalog.estimate_property_eq_rows(label, property, rows),
        |value| catalog.estimate_property_value_rows(label, property, value, rows),
    ) as f64
        / rows as f64
}

fn single_label(label: &str) -> bool {
    !label.is_empty() && !label.contains(':')
}

pub(super) fn reverse(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    }
}

#[cfg(test)]
pub(super) mod tests;
