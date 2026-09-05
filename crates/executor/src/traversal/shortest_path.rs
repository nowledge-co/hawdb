//! Level-synchronous discovery and ordered, output-bounded path enumeration.

use super::*;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Range;

pub(super) struct ShortestPathSearchResult {
    pub(super) paths: Vec<Vec<NodeId>>,
    pub(super) tracker: OperatorMemoryTracker,
    pub(super) visited_paths: usize,
}

struct SearchNode {
    id: NodeId,
    depth: usize,
    successors: Range<usize>,
    reaches_target: bool,
}

#[derive(Default)]
struct SearchDag {
    nodes: Vec<SearchNode>,
    edges: Vec<usize>,
    index: HashMap<(NodeId, usize), usize>,
    index_bytes: usize,
}

impl SearchDag {
    fn node(
        &mut self,
        id: NodeId,
        depth: usize,
        lower_bounded: bool,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<usize> {
        let key = (id, if lower_bounded { depth } else { 0 });
        if let Some(&index) = self.index.get(&key) {
            return Ok(index);
        }
        if self.index.len() == self.index.capacity() {
            let capacity = self.index.capacity().saturating_mul(2).max(8);
            // Conservative allowance for bucket slack, control bytes and
            // alignment. Keep the old table charged throughout rehashing.
            let bytes = capacity
                .saturating_mul(4)
                .saturating_mul(size_of::<((NodeId, usize), usize)>() + size_of::<usize>())
                .saturating_add(128);
            charge(tracker, bytes)?;
            let mut index = HashMap::with_capacity(capacity);
            index.extend(self.index.drain());
            self.index = index;
            tracker.release(self.index_bytes);
            self.index_bytes = bytes;
        }
        grow_vec(&mut self.nodes, tracker)?;
        let index = self.nodes.len();
        self.nodes.push(SearchNode {
            id,
            depth,
            successors: 0..0,
            reaches_target: false,
        });
        self.index.insert(key, index);
        Ok(index)
    }

    fn memory_bytes(&self) -> usize {
        vector_bytes(&self.nodes)
            .saturating_add(vector_bytes(&self.edges))
            .saturating_add(self.index_bytes)
    }

    fn mark_target(
        &mut self,
        target: NodeId,
        depth: usize,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        // Nodes are appended in BFS level order, so every edge points forward.
        for index in (0..self.nodes.len()).rev() {
            checkpoint()?;
            let node = &self.nodes[index];
            let mut reaches = node.id == target && node.depth == depth;
            for &next in &self.edges[node.successors.clone()] {
                checkpoint()?;
                reaches |= self.nodes[next].reaches_target;
            }
            self.nodes[index].reaches_target = reaches;
        }
        Ok(())
    }

    fn materialize(
        &mut self,
        search: &ShortestPathSearch<'_>,
        depth: usize,
        limit: usize,
        tracker: &mut OperatorMemoryTracker,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Vec<Vec<NodeId>>> {
        self.mark_target(search.target, depth, &mut checkpoint)?;
        let mut paths = Vec::new();
        if !self.nodes[0].reaches_target {
            return Ok(paths);
        }
        let length = depth.saturating_add(1);
        let scratch_bytes = length.saturating_mul(size_of::<NodeId>() + size_of::<Range<usize>>());
        charge(tracker, scratch_bytes)?;
        let mut path = Vec::with_capacity(length);
        let mut frames = Vec::with_capacity(length);
        path.push(search.source);
        frames.push(self.nodes[0].successors.clone());
        while let Some(frame) = frames.last_mut() {
            checkpoint()?;
            let Some(edge) = frame.next() else {
                frames.pop();
                path.pop();
                continue;
            };
            let next = &self.nodes[self.edges[edge]];
            if !next.reaches_target || (search.min_hops > 1 && path.contains(&next.id)) {
                continue;
            }
            path.push(next.id);
            if next.id == search.target {
                grow_vec(&mut paths, tracker)?;
                charge(tracker, path.len().saturating_mul(size_of::<NodeId>()))?;
                paths.push(path.clone());
                path.pop();
                if paths.len() == limit {
                    break;
                }
            } else {
                frames.push(next.successors.clone());
            }
        }
        drop(frames);
        drop(path);
        tracker.release(scratch_bytes);
        Ok(paths)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn search_shortest_paths(
    store: &dyn GraphExecutionRead,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    memory_account: QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
) -> Result<ShortestPathSearchResult> {
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, memory_account.clone());
    let mut paths = Vec::new();
    let mut visited_paths = 0usize;
    // A positive node-simple path cannot return to its source or use more than
    // N - 1 edges. The cap also bounds layered search in cyclic components.
    let max_hops = search
        .max_hops
        .min(store.node_count_for_label(None).saturating_sub(1));
    if result_limit == 0
        || search.source == search.target
        || max_hops < search.min_hops
        || max_hops == 0
    {
        return Ok(ShortestPathSearchResult {
            paths,
            tracker,
            visited_paths,
        });
    }
    let lower_bounded = search.min_hops > 1;
    let mut dag = SearchDag::default();
    dag.node(search.source, 0, lower_bounded, &mut tracker)?;
    let mut level = 0..1;
    for depth in 0..max_hops {
        runtime_checkpoint(task_context)?;
        let next_depth = depth + 1;
        let mut found_target = false;
        for current in level.clone() {
            runtime_checkpoint(task_context)?;
            if dag.nodes[current].id == search.target {
                continue;
            }
            visited_paths = visited_paths.saturating_add(1);
            let edge_start = dag.edges.len();
            visit_one_hop_relationships_with_budget(
                store,
                OneHopRelationshipSpec {
                    source: dag.nodes[current].id,
                    rel_type_id: search.rel_type_id,
                    target_label_ids: None,
                    rel_properties: &BTreeMap::new(),
                    relationship_scan_filter: None,
                    direction: search.direction,
                },
                AdjacencyReadMemory {
                    budget_bytes: memory_budget.get(),
                    account: Some(&memory_account),
                },
                observer,
                &mut |_, next| {
                    runtime_checkpoint(task_context)?;
                    if next.id == search.source
                        || search
                            .path_node_visibility_filter
                            .is_some_and(|filter| !node_matches_property_filter(&next, filter))
                    {
                        return Ok(ScanControl::Continue);
                    }
                    let is_target = next.id == search.target;
                    if (is_target && next_depth < search.min_hops)
                        || (!is_target
                            && (next_depth == max_hops || (found_target && !lower_bounded)))
                    {
                        return Ok(ScanControl::Continue);
                    }
                    let next = dag.node(next.id, next_depth, lower_bounded, &mut tracker)?;
                    if dag.nodes[next].depth == next_depth {
                        grow_vec(&mut dag.edges, &mut tracker)?;
                        dag.edges.push(next);
                        found_target |= is_target;
                    }
                    Ok(ScanControl::Continue)
                },
            )?;
            dag.nodes[current].successors = edge_start..dag.edges.len();
        }
        if found_target {
            paths = dag.materialize(&search, next_depth, result_limit, &mut tracker, || {
                runtime_checkpoint(task_context)
            })?;
            if !paths.is_empty() {
                break;
            }
            // A layered walk to the target may repeat a node. Only a valid
            // simple path can end the minimum-hop-constrained search.
        }
        level = level.end..dag.nodes.len();
        if level.is_empty() {
            break;
        }
    }
    runtime_checkpoint(task_context)?;
    let dag_bytes = dag.memory_bytes();
    drop(dag);
    tracker.release(dag_bytes);
    debug_assert_eq!(
        tracker.used_bytes,
        vector_bytes(&paths) + paths.iter().map(vector_bytes).sum::<usize>()
    );
    Ok(ShortestPathSearchResult {
        paths,
        tracker,
        visited_paths,
    })
}

fn charge(tracker: &mut OperatorMemoryTracker, bytes: usize) -> Result<()> {
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "ShortestPathExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)
}

fn vector_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(size_of::<T>())
}

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
