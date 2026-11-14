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

//! Bidirectional breadth-first search for `shortestPath` and
//! `allShortestPaths` between two bound nodes.
//!
//! Each side keeps a visited index and parent links instead of complete path
//! prefixes. The side with the smaller frontier expands one complete level at
//! a time. The first level that discovers a node already visited by the other
//! side fixes the shortest length; every such meeting node lies at the same
//! position of every shortest path, so each shortest path is enumerated
//! exactly once through its meeting node.
//!
//! `shortestPath` keeps the first parent link of each node and stops at the
//! first meeting. The result is deterministic for one snapshot: the first
//! meeting in ordered-adjacency order, joined with the first-discovered parent
//! chains on both sides. `allShortestPaths` keeps every parent link from the
//! previous level and enumerates paths lazily in meeting-discovery order, then
//! source-side and target-side parent order, so a downstream LIMIT bounds the
//! work without materializing the complete result.
//!
//! All index, parent-link and enumeration state is charged to the blocking
//! operator account before allocation; exceeding it fails the query.

use crate::kernel::OperatorMemoryTracker;
use hawdb_core::{HawDBError, RelationshipDirection, Result};
use hawdb_storage::{adjacency::AdjacencyDirection, NodeId, RelId};
use std::collections::{BTreeSet, HashMap};
use std::mem::size_of;
use std::ops::Range;

use crate::store::ScanControl;

const NONE: u32 = u32::MAX;

/// One shortest-path search between already bound endpoints.
pub(crate) struct ShortestPathBfsSpec<'a> {
    pub(crate) source: NodeId,
    pub(crate) target: NodeId,
    pub(crate) direction: RelationshipDirection,
    /// 0 or 1. Equal endpoints yield a zero-length path only for 0.
    pub(crate) min_hops: usize,
    pub(crate) max_hops: usize,
    /// False when `max_hops` is the execution cap of an unbounded pattern;
    /// reaching that cap with an unexplored frontier is then an error.
    pub(crate) bounded: bool,
    pub(crate) all: bool,
    /// Relationships already bound by the enclosing MATCH clause.
    pub(crate) excluded: &'a BTreeSet<RelId>,
}

/// Visits `(relationship, neighbor)` pairs of one node in one adjacency
/// direction, in deterministic adjacency order. Undirected self-loops are
/// reported once.
pub(crate) type AdjacencyVisitor<'a> = dyn FnMut(
        NodeId,
        AdjacencyDirection,
        &mut dyn FnMut(RelId, NodeId) -> Result<ScanControl>,
    ) -> Result<ScanControl>
    + 'a;

/// Receives one shortest path as ordered node and relationship ids.
pub(crate) type PathVisitor<'a> = dyn FnMut(&[NodeId], &[RelId]) -> Result<ScanControl> + 'a;

/// Receives one parent chain: entries from its start to the side's root, and
/// the relationships between them.
type ChainVisitor<'a> =
    dyn FnMut(&[u32], &[RelId], &mut OperatorMemoryTracker) -> Result<ScanControl> + 'a;

#[derive(Clone, Copy)]
struct Entry {
    node: NodeId,
    depth: u32,
    first_link: u32,
    last_link: u32,
}

#[derive(Clone, Copy)]
struct Link {
    /// Entry one step closer to this side's root.
    parent: u32,
    relationship: RelId,
    next: u32,
}

#[derive(Default)]
struct Side {
    index: HashMap<NodeId, u32>,
    index_bytes: usize,
    entries: Vec<Entry>,
    links: Vec<Link>,
    level: Range<usize>,
    depth: usize,
}

impl Side {
    fn root(node: NodeId, tracker: &mut OperatorMemoryTracker) -> Result<Self> {
        let mut side = Self::default();
        side.insert(node, 0, tracker)?;
        side.level = 0..1;
        Ok(side)
    }

    fn insert(
        &mut self,
        node: NodeId,
        depth: u32,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<u32> {
        if self.index.len() == self.index.capacity() {
            let capacity = self.index.capacity().saturating_mul(2).max(8);
            // Conservative allowance for bucket slack, control bytes and
            // alignment; the old table stays charged while rehashing.
            let bytes = capacity
                .saturating_mul(4)
                .saturating_mul(size_of::<(NodeId, u32)>() + size_of::<usize>())
                .saturating_add(128);
            charge(tracker, bytes)?;
            let mut index = HashMap::with_capacity(capacity);
            index.extend(self.index.drain());
            self.index = index;
            tracker.release(self.index_bytes);
            self.index_bytes = bytes;
        }
        grow_vec(&mut self.entries, tracker)?;
        let entry = u32::try_from(self.entries.len()).map_err(|_| {
            HawDBError::Execution("shortestPath search exceeds its node index limit".into())
        })?;
        self.entries.push(Entry {
            node,
            depth,
            first_link: NONE,
            last_link: NONE,
        });
        self.index.insert(node, entry);
        Ok(entry)
    }

    fn link(
        &mut self,
        entry: u32,
        parent: u32,
        relationship: RelId,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<()> {
        grow_vec(&mut self.links, tracker)?;
        let link = u32::try_from(self.links.len()).map_err(|_| {
            HawDBError::Execution("shortestPath search exceeds its parent link limit".into())
        })?;
        self.links.push(Link {
            parent,
            relationship,
            next: NONE,
        });
        let entry = &mut self.entries[entry as usize];
        if entry.last_link == NONE {
            entry.first_link = link;
        } else {
            self.links[entry.last_link as usize].next = link;
        }
        entry.last_link = link;
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        self.index_bytes
            .saturating_add(vector_bytes(&self.entries))
            .saturating_add(vector_bytes(&self.links))
    }

    /// Visits every parent chain from `start` to this side's root, in
    /// parent-link order. Chains list entries from `start` to the root and the
    /// relationship between each consecutive pair.
    fn visit_chains(
        &self,
        start: u32,
        tracker: &mut OperatorMemoryTracker,
        checkpoint: &mut dyn FnMut() -> Result<()>,
        visit: &mut ChainVisitor<'_>,
    ) -> Result<ScanControl> {
        let depth = self.entries[start as usize].depth as usize;
        let scratch = depth
            .saturating_add(1)
            .saturating_mul(2 * size_of::<u32>() + size_of::<RelId>());
        charge(tracker, scratch)?;
        let mut entries = Vec::with_capacity(depth + 1);
        let mut cursors = Vec::with_capacity(depth);
        let mut relationships = Vec::with_capacity(depth);
        entries.push(start);
        let control = loop {
            checkpoint()?;
            let top = *entries.last().expect("chain keeps its start entry");
            let first = self.entries[top as usize].first_link;
            if first != NONE {
                let link = self.links[first as usize];
                cursors.push(first);
                relationships.push(link.relationship);
                entries.push(link.parent);
                continue;
            }
            if visit(&entries, &relationships, tracker)? == ScanControl::Stop {
                break ScanControl::Stop;
            }
            // Advance the deepest link that has a sibling.
            let advanced = loop {
                let Some(link) = cursors.pop() else {
                    break false;
                };
                relationships.pop();
                entries.pop();
                let next = self.links[link as usize].next;
                if next != NONE {
                    let link = self.links[next as usize];
                    cursors.push(next);
                    relationships.push(link.relationship);
                    entries.push(link.parent);
                    break true;
                }
            };
            if !advanced {
                break ScanControl::Continue;
            }
        };
        drop((entries, cursors, relationships));
        tracker.release(scratch);
        Ok(control)
    }
}

/// Discovered shortest paths, represented by their meeting nodes.
pub(crate) struct ShortestPathBfs {
    forward: Side,
    backward: Side,
    /// `(forward entry, backward entry)` pairs in discovery order.
    meetings: Vec<(u32, u32)>,
    all: bool,
}

/// Outcome of [`ShortestPathBfs::search`].
pub(crate) enum ShortestPathBfsOutcome {
    /// The endpoints are equal and zero hops are allowed.
    ZeroLength,
    Found(Box<ShortestPathBfs>),
    NotFound,
}

impl ShortestPathBfs {
    pub(crate) fn search(
        spec: &ShortestPathBfsSpec<'_>,
        adjacency: &mut AdjacencyVisitor<'_>,
        tracker: &mut OperatorMemoryTracker,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<ShortestPathBfsOutcome> {
        checkpoint()?;
        if spec.source == spec.target {
            return Ok(if spec.min_hops == 0 {
                ShortestPathBfsOutcome::ZeroLength
            } else {
                ShortestPathBfsOutcome::NotFound
            });
        }
        let mut search = Self {
            forward: Side::root(spec.source, tracker)?,
            backward: Side::root(spec.target, tracker)?,
            meetings: Vec::new(),
            all: spec.all,
        };
        while search.forward.depth + search.backward.depth < spec.max_hops {
            checkpoint()?;
            if search.forward.level.is_empty() || search.backward.level.is_empty() {
                return search.not_found(tracker);
            }
            // Expand the smaller frontier; ties expand from the source.
            let expand_forward = search.forward.level.len() <= search.backward.level.len();
            search.expand_level(
                expand_forward,
                spec,
                &mut SearchIo {
                    adjacency: &mut *adjacency,
                    tracker: &mut *tracker,
                    checkpoint: &mut *checkpoint,
                },
            )?;
            if !search.meetings.is_empty() {
                return Ok(ShortestPathBfsOutcome::Found(Box::new(search)));
            }
        }
        if !spec.bounded && !search.forward.level.is_empty() && !search.backward.level.is_empty() {
            return Err(HawDBError::Execution(format!(
                "shortestPath search reached the {}-hop limit for unbounded patterns without finding a path; specify an explicit upper bound such as *1..{}",
                spec.max_hops, spec.max_hops
            )));
        }
        search.not_found(tracker)
    }

    fn not_found(self, tracker: &mut OperatorMemoryTracker) -> Result<ShortestPathBfsOutcome> {
        self.release(tracker);
        Ok(ShortestPathBfsOutcome::NotFound)
    }

    /// Returns all search state to the operator account.
    pub(crate) fn release(self, tracker: &mut OperatorMemoryTracker) {
        let bytes = self
            .forward
            .memory_bytes()
            .saturating_add(self.backward.memory_bytes())
            .saturating_add(vector_bytes(&self.meetings));
        drop(self);
        tracker.release(bytes);
    }

    /// Visits shortest paths as ordered node and relationship ids, stopping
    /// when the consumer stops. `shortestPath` visits exactly one path.
    pub(crate) fn visit_paths(
        &self,
        tracker: &mut OperatorMemoryTracker,
        checkpoint: &mut dyn FnMut() -> Result<()>,
        consumer: &mut PathVisitor<'_>,
    ) -> Result<ScanControl> {
        let meetings = if self.all {
            &self.meetings[..]
        } else {
            &self.meetings[..1]
        };
        let length = meetings.first().map_or(0, |&(forward, backward)| {
            (self.forward.entries[forward as usize].depth
                + self.backward.entries[backward as usize].depth) as usize
        });
        let path_bytes = length
            .saturating_add(1)
            .saturating_mul(size_of::<NodeId>())
            .saturating_add(length.saturating_mul(size_of::<RelId>()));
        charge(tracker, path_bytes)?;
        let mut nodes = Vec::with_capacity(length + 1);
        let mut relationships = Vec::with_capacity(length);
        let mut stopped = false;
        for &(forward, backward) in meetings {
            let control = self.forward.visit_chains(
                forward,
                tracker,
                checkpoint,
                &mut |prefix, prefix_relationships, tracker| {
                    self.backward.visit_chains(
                        backward,
                        tracker,
                        &mut || Ok(()),
                        &mut |suffix, suffix_relationships, _| {
                            nodes.clear();
                            relationships.clear();
                            nodes.extend(
                                prefix
                                    .iter()
                                    .rev()
                                    .map(|&entry| self.forward.entries[entry as usize].node),
                            );
                            nodes.extend(
                                suffix[1..]
                                    .iter()
                                    .map(|&entry| self.backward.entries[entry as usize].node),
                            );
                            relationships.extend(prefix_relationships.iter().rev().copied());
                            relationships.extend(suffix_relationships.iter().copied());
                            stopped = consumer(&nodes, &relationships)? == ScanControl::Stop;
                            // `shortestPath` selects only the first path.
                            Ok(if stopped || !self.all {
                                ScanControl::Stop
                            } else {
                                ScanControl::Continue
                            })
                        },
                    )
                },
            )?;
            if control == ScanControl::Stop {
                break;
            }
        }
        drop((nodes, relationships));
        tracker.release(path_bytes);
        Ok(if stopped {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    }
}

/// Storage, accounting and cancellation hooks of one search.
struct SearchIo<'a, 'b> {
    adjacency: &'a mut AdjacencyVisitor<'b>,
    tracker: &'a mut OperatorMemoryTracker,
    checkpoint: &'a mut dyn FnMut() -> Result<()>,
}

impl ShortestPathBfs {
    /// Expands one complete level of one side and records meeting nodes.
    fn expand_level(
        &mut self,
        forward: bool,
        spec: &ShortestPathBfsSpec<'_>,
        io: &mut SearchIo<'_, '_>,
    ) -> Result<()> {
        let Self {
            forward: forward_side,
            backward: backward_side,
            meetings,
            ..
        } = self;
        let (side, other, directions) = if forward {
            (
                forward_side,
                &*backward_side,
                adjacency_directions(spec.direction),
            )
        } else {
            (
                backward_side,
                &*forward_side,
                adjacency_directions(reverse(spec.direction)),
            )
        };
        let SearchIo {
            adjacency,
            tracker,
            checkpoint,
        } = io;
        let next_depth = u32::try_from(side.depth + 1)
            .map_err(|_| HawDBError::Execution("shortestPath depth overflow".into()))?;
        let level = side.level.clone();
        'level: for current in level.clone() {
            checkpoint()?;
            let current = current as u32;
            let node = side.entries[current as usize].node;
            for &direction in directions {
                let control = adjacency(node, direction, &mut |relationship, neighbor| {
                    checkpoint()?;
                    if spec.excluded.contains(&relationship) {
                        return Ok(ScanControl::Continue);
                    }
                    match side.index.get(&neighbor) {
                        Some(&entry) if side.entries[entry as usize].depth == next_depth => {
                            if spec.all {
                                side.link(entry, current, relationship, tracker)?;
                            }
                            Ok(ScanControl::Continue)
                        }
                        Some(_) => Ok(ScanControl::Continue),
                        None => {
                            let entry = side.insert(neighbor, next_depth, tracker)?;
                            side.link(entry, current, relationship, tracker)?;
                            if let Some(&met) = other.index.get(&neighbor) {
                                grow_vec(meetings, tracker)?;
                                meetings.push(if forward { (entry, met) } else { (met, entry) });
                                if !spec.all {
                                    return Ok(ScanControl::Stop);
                                }
                            }
                            Ok(ScanControl::Continue)
                        }
                    }
                })?;
                if control == ScanControl::Stop {
                    break 'level;
                }
            }
        }
        side.level = level.end..side.entries.len();
        side.depth += 1;
        Ok(())
    }
}

fn adjacency_directions(direction: RelationshipDirection) -> &'static [AdjacencyDirection] {
    match direction {
        RelationshipDirection::Outgoing => &[AdjacencyDirection::Outgoing],
        RelationshipDirection::Incoming => &[AdjacencyDirection::Incoming],
        RelationshipDirection::Undirected => {
            &[AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming]
        }
    }
}

fn reverse(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    }
}

fn charge(tracker: &mut OperatorMemoryTracker, bytes: usize) -> Result<()> {
    if tracker.would_exceed(bytes) {
        return Err(HawDBError::Execution(format!(
            "shortestPath search state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)
}

fn vector_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(size_of::<T>())
}

/// Doubles a vector's capacity after charging the replacement; the old buffer
/// stays charged until it is released.
fn grow_vec<T>(values: &mut Vec<T>, tracker: &mut OperatorMemoryTracker) -> Result<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let capacity = values.capacity().saturating_mul(2).max(4);
    charge(tracker, capacity.saturating_mul(size_of::<T>()))?;
    let old_bytes = vector_bytes(values);
    let mut replacement = Vec::with_capacity(capacity);
    replacement.append(values);
    *values = replacement;
    tracker.release(old_bytes);
    Ok(())
}

#[cfg(test)]
mod tests;
