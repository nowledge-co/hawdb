// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
}

impl Default for DiskAnnBuildConfig {
    fn default() -> Self {
        Self {
            max_degree: 32,
            build_beam: 128,
            alpha: 1.2,
            lsg: None,
            max_working_bytes: 64 << 20,
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
        require_memory(build_bytes(&entries, config)?, config.max_working_bytes)?;
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
        let mut scratch = Scratch::new(count, config.build_beam.min(count), count)?;
        scratch.collect_visited = true;
        scratch.visited = Vec::with_capacity(count);
        // Vamana first removes redundant short edges with alpha=1, then adds
        // longer alternatives using the requested relaxation.
        for alpha in [1.0, config.alpha] {
            for position in 0..count {
                checkpoint(task)?;
                let ordinal = index.order[position];
                traverse(
                    &index,
                    &mut scratch,
                    DiskAnnSearchOptions {
                        beam_width: config.build_beam,
                        max_visited: count,
                        max_working_bytes: config.max_working_bytes,
                        task,
                    },
                    |_| Ok(true),
                    |other| index.construction_distance(ordinal, other, task),
                )?;
                let neighbors = index.prune(ordinal, &scratch.visited, alpha, task)?;
                index.nodes[ordinal as usize].neighbors = neighbors;
                // Reverse links are pruned under the same metric and degree cap.
                for position in 0..index.nodes[ordinal as usize].neighbors.len() {
                    let other = index.nodes[ordinal as usize].neighbors[position];
                    let mut candidates = Vec::with_capacity(config.max_degree + 1);
                    candidates.extend_from_slice(&index.nodes[other as usize].neighbors);
                    if candidates.contains(&ordinal) {
                        continue;
                    }
                    candidates.push(ordinal);
                    index.nodes[other as usize].neighbors =
                        index.prune(other, &candidates, alpha, task)?;
                }
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
