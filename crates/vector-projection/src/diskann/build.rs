// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::num::NonZeroUsize;

/// Sampled local radii and the LS exponent affect construction, never queries.
/// Radii are the mean Euclidean distances to the closest sampled neighbors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LsgBuildConfig {
    pub sample_size: usize,
    pub neighbors: usize,
    pub exponent: f64,
}

impl Default for LsgBuildConfig {
    fn default() -> Self {
        Self {
            sample_size: 256,
            neighbors: 10,
            exponent: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskAnnBuildConfig {
    pub max_degree: usize,
    pub build_beam: usize,
    /// Vamana's occlusion relaxation, applied to squared construction distances.
    pub alpha: f64,
    pub lsg: Option<LsgBuildConfig>,
    pub max_working_bytes: usize,
    /// Upper bound on construction worker threads. The build uses at most the
    /// CPU parallelism its task was admitted with (one without a task), within
    /// the task's executor-thread limit and `max_working_bytes`. One worker
    /// inserts sequentially; several workers insert in batches and build the
    /// same graph for every worker count.
    pub max_build_threads: NonZeroUsize,
}

impl Default for DiskAnnBuildConfig {
    fn default() -> Self {
        Self {
            max_degree: 32,
            build_beam: 128,
            alpha: 1.2,
            lsg: None,
            max_working_bytes: 64 << 20,
            max_build_threads: NonZeroUsize::MAX,
        }
    }
}

#[derive(Debug)]
struct Node {
    id: u64,
    vector: Vec<f32>,
    neighbors: Vec<u32>,
}

/// A private, immutable graph builder result. Database CRUD publishes successor
/// graphs; mutating a graph already visible to an old MVCC pin is forbidden.
#[derive(Debug)]
pub struct DiskAnnIndex {
    dimension: usize,
    metric: VectorDistance,
    config: DiskAnnBuildConfig,
    nodes: Vec<Node>,
    radii: Vec<f64>,
    /// A deterministic permutation supplies both insertion order and a directed
    /// connectivity backbone. Retaining one backbone edge prevents disconnection
    /// when pruning duplicate vectors or extremely separated clusters.
    order: Vec<u32>,
    successor: Vec<u32>,
}

impl DiskAnnIndex {
    /// Preflight for an exact-capacity owned source, before copying any vectors.
    pub fn build_memory_bound(
        count: usize,
        dimension: usize,
        config: DiskAnnBuildConfig,
    ) -> Result<usize> {
        if count > u32::MAX as usize {
            return Err(configuration("DiskANN source count exceeds ordinal space"));
        }
        validate_config(dimension, config)?;
        let overflow = || configuration("DiskANN build memory bound overflow");
        let per_node = dimension
            .checked_mul(size_of::<f32>())
            .and_then(|bytes| bytes.checked_add(config.max_degree.checked_mul(size_of::<u32>())?))
            .and_then(|bytes| {
                bytes.checked_add(
                    size_of::<(u64, Vec<f32>)>()
                        + size_of::<Node>()
                        + 3 * size_of::<u32>()
                        + size_of::<f64>()
                        + size_of::<Neighbor>(),
                )
            })
            .ok_or_else(overflow)?;
        let required = count
            .checked_mul(per_node)
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| {
                bytes.checked_add(scratch_bytes(count, config.build_beam.min(count), count).ok()?)
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    config
                        .max_degree
                        .checked_add(1)?
                        .checked_mul(size_of::<u32>() + size_of::<Neighbor>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    config
                        .lsg
                        .map_or(0, |lsg| lsg.neighbors.min(count))
                        .checked_mul(size_of::<f64>())?,
                )
            })
            .ok_or_else(overflow)?;
        require_memory(required, config.max_working_bytes)?;
        Ok(required)
    }
    /// Consume an admitted private source. IDs are stable within the caller's
    /// source binding; input order cannot affect ties or the resulting graph.
    pub fn build(
        dimension: usize,
        metric: VectorDistance,
        mut entries: Vec<(u64, Vec<f32>)>,
        config: DiskAnnBuildConfig,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        checkpoint(task)?;
        validate_config(dimension, config)?;
        if entries.len() > u32::MAX as usize {
            return Err(configuration("DiskANN node count exceeds u32"));
        }
        let required = build_bytes(&entries, config)?;
        require_memory(required, config.max_working_bytes)?;
        for (_, vector) in &entries {
            validate_query(dimension, metric, vector, task)?;
        }
        // Heapsort is allocation-free; checkpoints also bound comparison work.
        sort_entries(&mut entries, task)?;
        for pair in entries.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(ProjectionError::DuplicateId(pair[0].0));
            }
        }
        let count = entries.len();
        let mut index = Self {
            dimension,
            metric,
            config,
            nodes: Vec::with_capacity(count),
            radii: if config.lsg.is_some() {
                vec![1.0; count]
            } else {
                Vec::new()
            },
            order: (0..count as u32).collect(),
            successor: vec![0; count],
        };
        for (id, vector) in entries {
            checkpoint(task)?;
            index.nodes.push(Node {
                id,
                vector,
                neighbors: Vec::with_capacity(config.max_degree),
            });
        }
        // A fixed shuffle avoids a sorted-input graph while remaining replayable.
        let mut random = 0x9e37_79b9_7f4a_7c15u64;
        for position in (1..count).rev() {
            checkpoint(task)?;
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            index
                .order
                .swap(position, (random % (position + 1) as u64) as usize);
        }
        if count < 2 {
            return Ok(index);
        }
        for position in 0..count {
            checkpoint(task)?;
            let ordinal = index.order[position];
            let next = index.order[(position + 1) % count];
            let previous = index.order[(position + count - 1) % count];
            index.successor[ordinal as usize] = next;
            index.nodes[ordinal as usize].neighbors.push(next);
            if next != previous {
                index.nodes[ordinal as usize].neighbors.push(previous);
            }
        }
        if let Some(lsg) = config.lsg {
            index.compute_radii(lsg, task)?;
        }
        let threads = build_threads(count, config, task, required)?;
        if threads == 1 {
            index.insert_sequentially(config, task)?;
            return Ok(index);
        }
        let mut scratches = (0..threads)
            .map(|_| {
                let mut scratch = Scratch::new(count, config.build_beam.min(count), count)?;
                scratch.collect_visited = true;
                scratch.visited = Vec::with_capacity(count);
                Ok(scratch)
            })
            .collect::<Result<Vec<_>>>()?;
        // Vamana first removes redundant short edges with alpha=1, then adds
        // longer alternatives using the requested relaxation. With more than one
        // worker, insertions run in batches whose sizes depend only on the node
        // count: each batch searches and prunes against the graph as of the
        // batch start, then applies its forward lists and the pruned reverse
        // links (#1472). The result is the same for every thread count above
        // one; a single worker keeps the sequential insertion order above.
        for alpha in [1.0, config.alpha] {
            let mut start = 0;
            while start < count {
                let end = start + batch_size(start, count);
                let forward = index.map_parallel(
                    &mut scratches,
                    start..end,
                    task,
                    |index, scratch, position| {
                        let ordinal = index.order[position];
                        traverse(
                            index,
                            scratch,
                            DiskAnnSearchOptions {
                                beam_width: config.build_beam,
                                max_visited: count,
                                max_working_bytes: config.max_working_bytes,
                                task,
                            },
                            |_| Ok(true),
                            |other| index.construction_distance(ordinal, other, task),
                        )?;
                        index.prune(ordinal, &scratch.visited, alpha, task)
                    },
                )?;
                for (position, neighbors) in (start..end).zip(forward) {
                    let ordinal = index.order[position];
                    index.nodes[ordinal as usize].neighbors = neighbors;
                }
                // Reverse links are pruned under the same metric and degree cap,
                // once per target with every new source of this batch.
                let mut reverse = Vec::new();
                for position in start..end {
                    checkpoint(task)?;
                    let ordinal = index.order[position];
                    for &other in &index.nodes[ordinal as usize].neighbors {
                        if !index.nodes[other as usize].neighbors.contains(&ordinal) {
                            reverse.push((other, ordinal));
                        }
                    }
                }
                reverse.sort_unstable();
                let mut targets: Vec<(u32, Vec<u32>)> = Vec::new();
                for (other, ordinal) in reverse {
                    match targets.last_mut() {
                        Some((target, sources)) if *target == other => sources.push(ordinal),
                        _ => targets.push((other, vec![ordinal])),
                    }
                }
                let pruned = index.map_parallel(
                    &mut scratches,
                    0..targets.len(),
                    task,
                    |index, _, slot| {
                        let (other, sources) = &targets[slot];
                        let mut candidates = Vec::with_capacity(config.max_degree + sources.len());
                        candidates.extend_from_slice(&index.nodes[*other as usize].neighbors);
                        candidates.extend_from_slice(sources);
                        index.prune(*other, &candidates, alpha, task)
                    },
                )?;
                for ((other, _), neighbors) in targets.into_iter().zip(pruned) {
                    index.nodes[other as usize].neighbors = neighbors;
                }
                start = end;
            }
        }
        Ok(index)
    }

    pub fn config(&self) -> DiskAnnBuildConfig {
        self.config
    }

    pub(super) fn vector(&self, ordinal: u32) -> Result<&[f32]> {
        self.nodes
            .get(ordinal as usize)
            .map(|node| node.vector.as_slice())
            .ok_or_else(|| corrupt("DiskANN ordinal is outside its pinned graph"))
    }

    pub fn local_radius(&self, ordinal: u32) -> Option<f64> {
        self.radii.get(ordinal as usize).copied()
    }

    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.nodes.capacity() * size_of::<Node>()
            + (self.order.capacity() + self.successor.capacity()) * size_of::<u32>()
            + self.radii.capacity() * size_of::<f64>()
            + self
                .nodes
                .iter()
                .map(|node| {
                    node.vector.capacity() * size_of::<f32>()
                        + node.neighbors.capacity() * size_of::<u32>()
                })
                .sum::<usize>()
    }

    /// The single-worker construction: each insertion sees every earlier one.
    fn insert_sequentially(
        &mut self,
        config: DiskAnnBuildConfig,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<()> {
        let count = self.nodes.len();
        let mut scratch = Scratch::new(count, config.build_beam.min(count), count)?;
        scratch.collect_visited = true;
        scratch.visited = Vec::with_capacity(count);
        for alpha in [1.0, config.alpha] {
            for position in 0..count {
                checkpoint(task)?;
                let ordinal = self.order[position];
                traverse(
                    &*self,
                    &mut scratch,
                    DiskAnnSearchOptions {
                        beam_width: config.build_beam,
                        max_visited: count,
                        max_working_bytes: config.max_working_bytes,
                        task,
                    },
                    |_| Ok(true),
                    |other| self.construction_distance(ordinal, other, task),
                )?;
                let neighbors = self.prune(ordinal, &scratch.visited, alpha, task)?;
                self.nodes[ordinal as usize].neighbors = neighbors;
                // Reverse links are pruned under the same metric and degree cap.
                for position in 0..self.nodes[ordinal as usize].neighbors.len() {
                    let other = self.nodes[ordinal as usize].neighbors[position];
                    let mut candidates = Vec::with_capacity(config.max_degree + 1);
                    candidates.extend_from_slice(&self.nodes[other as usize].neighbors);
                    if candidates.contains(&ordinal) {
                        continue;
                    }
                    candidates.push(ordinal);
                    self.nodes[other as usize].neighbors =
                        self.prune(other, &candidates, alpha, task)?;
                }
            }
        }
        Ok(())
    }

    /// Runs `work` for every item of `items` against this frozen graph, on up
    /// to `scratches.len()` threads, and returns the results in item order.
    /// Small batches run inline. The first error in item order is returned.
    fn map_parallel<T: Send>(
        &self,
        scratches: &mut [Scratch],
        items: std::ops::Range<usize>,
        task: Option<&RuntimeTaskContext>,
        work: impl Fn(&Self, &mut Scratch, usize) -> Result<T> + Sync,
    ) -> Result<Vec<T>> {
        let threads = scratches
            .len()
            .min(items.len().div_ceil(MIN_ITEMS_PER_THREAD))
            .max(1);
        if threads == 1 {
            let scratch = &mut scratches[0];
            return items
                .map(|item| {
                    checkpoint(task)?;
                    work(self, scratch, item)
                })
                .collect();
        }
        let chunk = items.len().div_ceil(threads);
        let work = &work;
        let parts = std::thread::scope(|scope| {
            let handles = scratches[..threads]
                .iter_mut()
                .enumerate()
                .map(|(thread, scratch)| {
                    let first = items.start + thread * chunk;
                    let last = (first + chunk).min(items.end);
                    scope.spawn(move || {
                        (first..last)
                            .map(|item| {
                                checkpoint(task)?;
                                work(self, scratch, item)
                            })
                            .collect::<Result<Vec<T>>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(result) => result,
                    Err(payload) => std::panic::resume_unwind(payload),
                })
                .collect::<Vec<_>>()
        });
        let mut results = Vec::with_capacity(items.len());
        for part in parts {
            results.extend(part?);
        }
        Ok(results)
    }

    fn construction_distance(
        &self,
        left: u32,
        right: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        let raw = self.distance(&self.nodes[left as usize].vector, right, task)?;
        let squared = match self.metric {
            VectorDistance::Euclidean => raw * raw,
            // Squared Euclidean distance after unit normalization.
            VectorDistance::Cosine => 2.0 * raw,
        };
        Ok(if let Some(lsg) = self.config.lsg {
            squared / (self.radii[left as usize] * self.radii[right as usize]).powf(lsg.exponent)
        } else {
            squared
        })
    }

    fn prune(
        &self,
        ordinal: u32,
        candidates: &[u32],
        alpha: f64,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<u32>> {
        let mut scored = Vec::with_capacity(candidates.len());
        for &other in candidates {
            checkpoint(task)?;
            if other != ordinal {
                scored.push(Neighbor {
                    ordinal: other,
                    id: self.id(other)?,
                    distance: self.construction_distance(ordinal, other, task)?,
                    expanded: false,
                });
            }
        }
        // Provider ordinals are unique and traversal visits each at most once;
        // reciprocal lists also preserve uniqueness.
        scored.sort_unstable_by(Neighbor::compare);
        let mut selected = Vec::with_capacity(self.config.max_degree);
        for candidate in scored {
            checkpoint(task)?;
            if selected.len() == self.config.max_degree - 1 {
                break;
            }
            let mut occluded = false;
            for &neighbor in &selected {
                if alpha * self.construction_distance(neighbor, candidate.ordinal, task)?
                    <= candidate.distance
                {
                    occluded = true;
                    break;
                }
            }
            if !occluded {
                selected.push(candidate.ordinal);
            }
        }
        let backbone = self.successor[ordinal as usize];
        if !selected.contains(&backbone) {
            selected.push(backbone);
        }
        selected.sort_unstable();
        Ok(selected)
    }

    fn compute_radii(
        &mut self,
        lsg: LsgBuildConfig,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<()> {
        let sample_size = lsg.sample_size.min(self.nodes.len());
        let mut closest = Vec::with_capacity(lsg.neighbors.min(sample_size));
        let mut largest = 0.0f64;
        for ordinal in 0..self.nodes.len() {
            closest.clear();
            for &sample in self.order.iter().take(sample_size) {
                checkpoint(task)?;
                if sample as usize == ordinal {
                    continue;
                }
                let raw = self.distance(&self.nodes[ordinal].vector, sample, task)?;
                let radius = match self.metric {
                    VectorDistance::Euclidean => raw,
                    VectorDistance::Cosine => (2.0 * raw).sqrt(),
                };
                let position = closest.partition_point(|value| *value < radius);
                if position < lsg.neighbors {
                    if closest.len() == lsg.neighbors {
                        closest.pop();
                    }
                    closest.insert(position, radius);
                }
            }
            let radius = closest.iter().sum::<f64>() / closest.len().max(1) as f64;
            self.radii[ordinal] = radius;
            largest = largest.max(radius);
        }
        // Identical points have a zero neighborhood radius. A relative floor
        // avoids NaN/Inf while preserving the scale of nondegenerate datasets.
        let floor = if largest == 0.0 { 1.0 } else { largest * 1e-12 };
        for chunk in self.radii.chunks_mut(4096) {
            checkpoint(task)?;
            for radius in chunk {
                *radius = radius.max(floor);
            }
        }
        Ok(())
    }
}

impl DiskAnnRead for DiskAnnIndex {
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn dimension(&self) -> usize {
        self.dimension
    }
    fn max_degree(&self) -> usize {
        self.config.max_degree
    }
    fn metric(&self) -> VectorDistance {
        self.metric
    }
    fn entry_point(&self) -> Option<u32> {
        self.order.first().copied()
    }
    fn ids_are_sorted(&self) -> bool {
        true
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        self.nodes
            .get(ordinal as usize)
            .map(|node| node.id)
            .ok_or_else(|| corrupt("DiskANN ordinal is outside its pinned graph"))
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        Ok(self
            .nodes
            .get(ordinal as usize)
            .ok_or_else(|| corrupt("DiskANN ordinal is outside its pinned graph"))?
            .neighbors
            .iter()
            .copied())
    }
    fn distance(
        &self,
        query: &[f32],
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        let node = self
            .nodes
            .get(ordinal as usize)
            .ok_or_else(|| corrupt("DiskANN ordinal is outside its pinned graph"))?;
        distance(self.metric, query, &node.vector, task)
    }
    fn distance_prepared(
        &self,
        query: &hawdb_core::vector::PreparedVectorQuery<'_>,
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        let node = self
            .nodes
            .get(ordinal as usize)
            .ok_or_else(|| corrupt("DiskANN ordinal is outside its pinned graph"))?;
        with_distance_work(task, |checkpoint| {
            query.distance_slices_with_work(self.metric, &node.vector, checkpoint)
        })
    }
}

pub(super) fn validate_config(dimension: usize, config: DiskAnnBuildConfig) -> Result<()> {
    if dimension == 0 || dimension > u32::MAX as usize {
        return Err(configuration("DiskANN dimension must be a positive u32"));
    }
    if config.max_degree < 2
        || config.max_degree > u32::MAX as usize
        || config.build_beam < config.max_degree
        || config.build_beam > u32::MAX as usize
        || !config.alpha.is_finite()
        || config.alpha < 1.0
    {
        return Err(configuration(
            "DiskANN requires degree >= 2, build beam >= degree and finite alpha >= 1",
        ));
    }
    if let Some(lsg) = config.lsg
        && (lsg.sample_size < 2
            || lsg.sample_size > u32::MAX as usize
            || lsg.neighbors == 0
            || lsg.neighbors > lsg.sample_size
            || !lsg.exponent.is_finite()
            || lsg.exponent <= 0.0
            || lsg.exponent > 2.0)
    {
        return Err(configuration(
            "LSG requires 0 < neighbors <= sample size, sample size >= 2 and exponent in (0, 2]",
        ));
    }
    Ok(())
}

/// Batches start at one insertion and double up to 1% of the nodes (at least
/// one), as in parallel Vamana implementations. The schedule depends only on
/// the node count, so the graph does not depend on the thread count.
const MAX_BATCH_DIVISOR: usize = 100;

fn batch_size(start: usize, count: usize) -> usize {
    let largest = (count / MAX_BATCH_DIVISOR).max(1);
    start.clamp(1, largest).min(count - start)
}

#[cfg(test)]
pub(super) fn batch_size_for_test(start: usize, count: usize) -> usize {
    batch_size(start, count)
}

#[cfg(test)]
pub(super) fn batch_bytes_for_test(count: usize, config: DiskAnnBuildConfig) -> usize {
    batch_bytes(count, config).unwrap()
}

#[cfg(test)]
pub(super) fn build_threads_for_test(
    count: usize,
    config: DiskAnnBuildConfig,
    task: Option<&RuntimeTaskContext>,
    base_bytes: usize,
) -> usize {
    build_threads(count, config, task, base_bytes).unwrap()
}

/// Each thread below this many items costs more to start than it saves.
const MIN_ITEMS_PER_THREAD: usize = 8;

/// Worker threads: the configured cap, the task's admitted CPU parallelism
/// (one without a task) and executor-thread limit, and the threads whose
/// private traversal scratch and the batch buffers still fit in
/// `max_working_bytes` beyond the sequential bound.
fn build_threads(
    count: usize,
    config: DiskAnnBuildConfig,
    task: Option<&RuntimeTaskContext>,
    base_bytes: usize,
) -> Result<usize> {
    let mut threads = config
        .max_build_threads
        .get()
        .min(task.map_or(1, |task| task.admitted_parallelism().get()));
    if let Some(limit) = task.and_then(RuntimeTaskContext::executor_thread_limit) {
        threads = threads.min(limit.get());
    }
    let per_thread = scratch_bytes(count, config.build_beam.min(count), count)?
        .checked_add(
            count
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| configuration("DiskANN build memory bound overflow"))?,
        )
        .ok_or_else(|| configuration("DiskANN build memory bound overflow"))?;
    // Several workers also hold the largest batch's forward lists and
    // reverse-link buffers; one sequential worker needs neither.
    let batch = batch_bytes(count, config)
        .ok_or_else(|| configuration("DiskANN build memory bound overflow"))?;
    let spare = config
        .max_working_bytes
        .saturating_sub(base_bytes)
        .saturating_sub(batch);
    let fitting = 1 + spare / per_thread.max(1);
    Ok(if fitting < 2 {
        1
    } else {
        threads.min(fitting).max(1)
    })
}

/// Forward lists and reverse-link buffers of the largest insertion batch.
fn batch_bytes(count: usize, config: DiskAnnBuildConfig) -> Option<usize> {
    (count / MAX_BATCH_DIVISOR)
        .max(1)
        .checked_mul(config.max_degree)?
        .checked_mul(2 * size_of::<(u32, u32)>() + size_of::<u32>() + size_of::<(u32, Vec<u32>)>())
}

fn build_bytes(entries: &Vec<(u64, Vec<f32>)>, config: DiskAnnBuildConfig) -> Result<usize> {
    let overflow = || configuration("DiskANN build memory bound overflow");
    let count = entries.len();
    let mut bytes = entries
        .capacity()
        .checked_mul(size_of::<(u64, Vec<f32>)>())
        .and_then(|bytes| bytes.checked_add(size_of::<DiskAnnIndex>()))
        .ok_or_else(overflow)?;
    for (_, vector) in entries {
        bytes = bytes
            .checked_add(
                vector
                    .capacity()
                    .checked_mul(size_of::<f32>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
    }
    let per_node = config
        .max_degree
        .checked_mul(size_of::<u32>())
        .and_then(|bytes| {
            bytes.checked_add(
                size_of::<Node>() + 3 * size_of::<u32>() + size_of::<f64>() + size_of::<Neighbor>(),
            )
        })
        .ok_or_else(overflow)?;
    bytes = bytes
        .checked_add(count.checked_mul(per_node).ok_or_else(overflow)?)
        .and_then(|bytes| {
            bytes.checked_add(scratch_bytes(count, config.build_beam.min(count), count).ok()?)
        })
        .and_then(|bytes| {
            bytes.checked_add(
                config
                    .max_degree
                    .checked_add(1)?
                    .checked_mul(size_of::<u32>() + size_of::<Neighbor>())?,
            )
        })
        .ok_or_else(overflow)?;
    if let Some(lsg) = config.lsg {
        bytes = bytes
            .checked_add(
                lsg.neighbors
                    .min(count)
                    .checked_mul(size_of::<f64>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

fn sort_entries(entries: &mut [(u64, Vec<f32>)], task: Option<&RuntimeTaskContext>) -> Result<()> {
    fn sift(
        entries: &mut [(u64, Vec<f32>)],
        mut root: usize,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<()> {
        while root < entries.len() / 2 {
            checkpoint(task)?;
            let left = root * 2 + 1;
            let right = left + 1;
            let child = if right < entries.len() && entries[right].0 > entries[left].0 {
                right
            } else {
                left
            };
            if entries[root].0 >= entries[child].0 {
                break;
            }
            entries.swap(root, child);
            root = child;
        }
        Ok(())
    }
    for root in (0..entries.len() / 2).rev() {
        sift(entries, root, task)?;
    }
    for end in (1..entries.len()).rev() {
        checkpoint(task)?;
        entries.swap(0, end);
        sift(&mut entries[..end], 0, task)?;
    }
    Ok(())
}
