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

//! An experimental base-only HNSW candidate index over a fixed vector set.
//!
//! `InMemoryProjection`/`FileProjection` exhaustively scan eligible quantized
//! candidates; their RaBitQ scores are approximate, not exact raw-vector scores.
//! `HnswIndex` is an independent in-memory benchmark alternative. It is not
//! selected by embedded search, has no persistence or generation binding, and
//! accepts no candidate filter. Growing its fixed set requires a full rebuild.
//! See the [experimental API roadmap](crate#experimental-apis) before integration.
//!
//! This implementation keeps full-precision vectors for graph traversal. RaBitQ
//! defaults to 1-bit codes; 4-bit is an explicit build option. Keeping raw vectors
//! is this implementation's choice, not evidence that quantized graph traversal
//! is impossible or that HNSW has better recall for every workload. Its memory,
//! latency, and recall trade-offs need separate representative measurements.
//!
//! This implements the standard multi-layer HNSW algorithm (Malkov &
//! Yashunin, 2016): layer assignment by a truncated exponential
//! distribution, greedy single-best descent through upper layers, a
//! bounded beam search (`search_layer`) at each layer down to and
//! including layer 0, and simple (non-heuristic) nearest-M neighbor
//! selection with degree-bounded pruning on insert. The paper's optional
//! neighbor-selection heuristic (diversifying the connected set rather
//! than always keeping the M nearest) is not implemented here, to keep
//! this first version's surface area small; it is available as later
//! follow-up if benchmarks show it is worth the extra complexity.

use crate::error::{ProjectionError, Result};
use crate::scan::ProjectionHit;
use hawdb_core::RuntimeTaskContext;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

/// Matches `DEFAULT_BUILD_MEMORY_BYTES`/`DEFAULT_SEARCH_MEMORY_BYTES`
/// elsewhere in this crate.
const DEFAULT_HNSW_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Fixed per-node overhead (id, `Vec` headers, `Node` struct fields)
/// beyond the vector and neighbor-list payloads, folded into the
/// admission estimate so small corpora with large `m` do not slip under
/// budget on the payload count alone.
const HNSW_NODE_FIXED_BYTES: usize = 64;

/// Build settings for the [experimental HNSW index](crate#experimental-apis).
#[derive(Debug, Clone, Copy)]
pub struct HnswBuildConfig {
    /// Max neighbors per node at layers above 0.
    pub m: usize,
    /// Max neighbors per node at layer 0 (conventionally `2 * m`).
    pub m_max0: usize,
    /// Candidate list size while building each node's connections.
    pub ef_construction: usize,
    /// Seed for the deterministic layer-assignment RNG.
    pub seed: u64,
    /// Upper bound on `approximate_memory_bytes()` (plus fixed per-node
    /// overhead) admitted at build time; see `build`'s admission check.
    pub max_working_bytes: usize,
}

impl HnswBuildConfig {
    pub fn new() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            seed: 0x5eed_5eed_5eed_5eed,
            max_working_bytes: DEFAULT_HNSW_MEMORY_BYTES,
        }
    }

    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self.m_max0 = m.saturating_mul(2);
        self
    }

    pub fn with_ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_max_working_bytes(mut self, max_working_bytes: usize) -> Self {
        self.max_working_bytes = max_working_bytes;
        self
    }
}

impl Default for HnswBuildConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct Node {
    id: u64,
    vector: Vec<f32>,
    norm: f32,
    /// `neighbors[level]` is this node's neighbor ordinals at `level`;
    /// `neighbors.len() - 1` is the node's top level.
    neighbors: Vec<Vec<u32>>,
}

/// An immutable, base-only HNSW graph over a fixed vector set.
///
/// Experimental and in-memory only; see the [roadmap](crate#experimental-apis).
#[derive(Debug, Clone)]
pub struct HnswIndex {
    dimension: usize,
    nodes: Vec<Node>,
    entry_point: Option<u32>,
    config: HnswBuildConfig,
}

impl HnswIndex {
    /// Build a graph over `entries`. Every vector must have `dimension`
    /// finite coordinates and a unique id; build order does not need to be
    /// sorted (unlike `ProjectionBuilder`).
    ///
    /// Fails closed with `ResourceBudgetExceeded` before doing any work if
    /// the estimated graph size (vectors plus a conservative upper bound
    /// on neighbor-list edges) would exceed `config.max_working_bytes` --
    /// mirroring `ProjectionBuildConfig::resource_admission()` for the
    /// quantized base. `task_context`, when supplied, is checkpointed once
    /// per inserted node (and, inside each insert, once per beam-search
    /// step) so a host governor can cancel or time out a large build; this
    /// module never spawns threads of its own, so `admitted_parallelism`
    /// does not apply here.
    pub fn build(
        entries: &[(u64, Vec<f32>)],
        dimension: usize,
        config: HnswBuildConfig,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        if dimension == 0 {
            return Err(ProjectionError::InvalidConfiguration(
                "dimension must be greater than zero".to_string(),
            ));
        }
        if config.m == 0 {
            return Err(ProjectionError::InvalidConfiguration(
                "m must be greater than zero".to_string(),
            ));
        }
        let per_node_bytes = dimension
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(
                config
                    .m
                    .saturating_add(config.m_max0)
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(HNSW_NODE_FIXED_BYTES);
        let required = entries.len().saturating_mul(per_node_bytes);
        if required > config.max_working_bytes {
            return Err(ProjectionError::ResourceBudgetExceeded {
                required,
                available: config.max_working_bytes,
            });
        }
        let mut index = Self {
            dimension,
            nodes: Vec::with_capacity(entries.len()),
            entry_point: None,
            config,
        };
        let mut seen_ids = HashSet::with_capacity(entries.len());
        let mut rng = SplitMix64::new(config.seed);
        for (id, vector) in entries {
            if let Some(context) = task_context {
                context.checkpoint()?;
            }
            if !seen_ids.insert(*id) {
                return Err(ProjectionError::DuplicateId(*id));
            }
            index.insert(*id, vector, &mut rng, task_context)?;
        }
        Ok(index)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Approximate heap bytes retained by this index: full-precision
    /// vectors plus every layer's neighbor list, across all nodes. Does
    /// not include allocator overhead or `Vec` spare capacity.
    pub fn approximate_memory_bytes(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| {
                let vector_bytes = node.vector.len() * std::mem::size_of::<f32>();
                let neighbor_bytes = node
                    .neighbors
                    .iter()
                    .map(|layer| layer.len() * std::mem::size_of::<u32>())
                    .sum::<usize>();
                vector_bytes + neighbor_bytes
            })
            .sum()
    }

    fn insert(
        &mut self,
        id: u64,
        vector: &[f32],
        rng: &mut SplitMix64,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<()> {
        if vector.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected dimension {}, got {} for id {id}",
                self.dimension,
                vector.len()
            )));
        }
        if !vector.iter().all(|value| value.is_finite()) {
            return Err(ProjectionError::InvalidVector(format!(
                "coordinates must be finite for id {id}"
            )));
        }
        let norm = l2_norm(vector);
        let node_level = random_level(rng, self.config.m);
        let ordinal = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            vector: vector.to_vec(),
            norm,
            neighbors: vec![Vec::new(); node_level + 1],
        });

        let Some(entry_point) = self.entry_point else {
            self.entry_point = Some(ordinal);
            return Ok(());
        };
        let top_level = self.nodes[entry_point as usize].neighbors.len() - 1;

        let mut ep = entry_point;
        for level in ((node_level + 1)..=top_level).rev() {
            let found = self.search_layer(vector, norm, &[ep], 1, level, None, task_context)?;
            if let Some(&(_, nearest)) = found.first() {
                ep = nearest;
            }
        }

        let mut entry_points = vec![ep];
        for level in (0..=node_level.min(top_level)).rev() {
            let candidates = self.search_layer(
                vector,
                norm,
                &entry_points,
                self.config.ef_construction,
                level,
                None,
                task_context,
            )?;
            let m_at_level = if level == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let selected = select_nearest(&candidates, m_at_level);
            self.nodes[ordinal as usize].neighbors[level] = selected.clone();
            for &neighbor in &selected {
                self.connect(neighbor, ordinal, level);
                self.prune_if_needed(neighbor, level, m_at_level);
            }
            entry_points = candidates.into_iter().map(|(_, ordinal)| ordinal).collect();
        }

        if node_level > top_level {
            self.entry_point = Some(ordinal);
        }
        Ok(())
    }

    fn connect(&mut self, owner: u32, neighbor: u32, level: usize) {
        self.nodes[owner as usize].neighbors[level].push(neighbor);
    }

    fn prune_if_needed(&mut self, owner: u32, level: usize, m_at_level: usize) {
        if self.nodes[owner as usize].neighbors[level].len() <= m_at_level {
            return;
        }
        let (vector, norm) = {
            let node = &self.nodes[owner as usize];
            (node.vector.clone(), node.norm)
        };
        let scored: Vec<(f32, u32)> = self.nodes[owner as usize].neighbors[level]
            .iter()
            .map(|&candidate| (self.similarity(candidate, &vector, norm), candidate))
            .collect();
        self.nodes[owner as usize].neighbors[level] = select_nearest(&scored, m_at_level);
    }

    fn similarity(&self, ordinal: u32, query: &[f32], query_norm: f32) -> f32 {
        let node = &self.nodes[ordinal as usize];
        if query_norm <= f32::EPSILON || node.norm <= f32::EPSILON {
            return 0.0;
        }
        let dot: f32 = query
            .iter()
            .zip(node.vector.iter())
            .map(|(left, right)| left * right)
            .sum();
        dot / (query_norm * node.norm)
    }

    /// Bounded beam search over one layer, returning up to `ef` best
    /// (similarity, ordinal) pairs reachable from `entry_points`, sorted
    /// by descending similarity. `excluded`, when set, is skipped
    /// entirely (used to keep the node being inserted out of its own
    /// candidate list, which cannot otherwise happen since it is not
    /// connected to anything yet).
    ///
    /// Checkpoints once per call rather than once per node visited: `ef`
    /// already bounds one call's work, and both `insert` and `search`
    /// call this only a handful of times each, so checking at entry gives
    /// a host governor a bounded cancellation window without adding an
    /// atomic load to this function's hot inner loop.
    #[allow(clippy::too_many_arguments)]
    fn search_layer(
        &self,
        query: &[f32],
        query_norm: f32,
        entry_points: &[u32],
        ef: usize,
        level: usize,
        excluded: Option<u32>,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<(f32, u32)>> {
        if let Some(context) = task_context {
            context.checkpoint()?;
        }
        let mut visited: HashSet<u32> = entry_points.iter().copied().collect();
        if let Some(excluded) = excluded {
            visited.insert(excluded);
        }
        let mut to_explore: BinaryHeap<ScoredOrdinal> = BinaryHeap::new();
        let mut found: BinaryHeap<std::cmp::Reverse<ScoredOrdinal>> = BinaryHeap::new();
        for &entry in entry_points {
            if Some(entry) == excluded || entry as usize >= self.nodes.len() {
                continue;
            }
            let similarity = self.similarity(entry, query, query_norm);
            let scored = ScoredOrdinal(similarity, entry);
            to_explore.push(scored);
            found.push(std::cmp::Reverse(scored));
        }
        while let Some(ScoredOrdinal(best_similarity, best)) = to_explore.pop() {
            if found.len() >= ef {
                let std::cmp::Reverse(ScoredOrdinal(worst_found, _)) = *found
                    .peek()
                    .expect("found is non-empty once ef nodes are admitted");
                if best_similarity < worst_found {
                    break;
                }
            }
            let Some(layer_neighbors) = self.nodes[best as usize].neighbors.get(level) else {
                continue;
            };
            for &neighbor in layer_neighbors {
                if !visited.insert(neighbor) {
                    continue;
                }
                let similarity = self.similarity(neighbor, query, query_norm);
                let worst_found = found
                    .peek()
                    .map(|std::cmp::Reverse(ScoredOrdinal(score, _))| *score);
                if found.len() < ef || worst_found.is_none_or(|worst| similarity > worst) {
                    let scored = ScoredOrdinal(similarity, neighbor);
                    to_explore.push(scored);
                    found.push(std::cmp::Reverse(scored));
                    if found.len() > ef {
                        found.pop();
                    }
                }
            }
        }
        let mut result: Vec<(f32, u32)> = found
            .into_iter()
            .map(|std::cmp::Reverse(ScoredOrdinal(score, ordinal))| (score, ordinal))
            .collect();
        result.sort_by(|left, right| right.0.total_cmp(&left.0));
        Ok(result)
    }

    /// Approximate top-`top_k` search. `ef_search` bounds the layer-0
    /// candidate list size and must be at least `top_k` to have any
    /// chance of returning `top_k` results; larger values trade latency
    /// for recall. `task_context`, when supplied, is checkpointed once
    /// per layer descended (see `search_layer`'s docs for why that
    /// granularity, not once per node visited).
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<ProjectionHit>> {
        if query.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected query dimension {}, got {}",
                self.dimension,
                query.len()
            )));
        }
        if !query.iter().all(|value| value.is_finite()) {
            return Err(ProjectionError::InvalidVector(
                "query coordinates must be finite".to_string(),
            ));
        }
        let Some(entry_point) = self.entry_point else {
            return Ok(Vec::new());
        };
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let query_norm = l2_norm(query);
        let top_level = self.nodes[entry_point as usize].neighbors.len() - 1;
        let mut ep = entry_point;
        for level in (1..=top_level).rev() {
            let found =
                self.search_layer(query, query_norm, &[ep], 1, level, None, task_context)?;
            if let Some(&(_, nearest)) = found.first() {
                ep = nearest;
            }
        }
        let ef = ef_search.max(top_k);
        let mut found = self.search_layer(query, query_norm, &[ep], ef, 0, None, task_context)?;
        found.truncate(top_k);
        Ok(found
            .into_iter()
            .map(|(score, ordinal)| ProjectionHit {
                id: self.nodes[ordinal as usize].id,
                score,
            })
            .collect())
    }
}

fn select_nearest(candidates: &[(f32, u32)], m: usize) -> Vec<u32> {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|left, right| right.0.total_cmp(&left.0));
    sorted.truncate(m);
    sorted.into_iter().map(|(_, ordinal)| ordinal).collect()
}

fn l2_norm(vector: &[f32]) -> f32 {
    vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt() as f32
}

/// `level = floor(-ln(u) * mL)`, `mL = 1 / ln(m)`, the truncated
/// exponential layer assignment from the HNSW paper.
fn random_level(rng: &mut SplitMix64, m: usize) -> usize {
    let ml = 1.0 / (m.max(2) as f64).ln();
    let uniform = rng.next_unit_f64();
    let level = (-uniform.ln() * ml).floor();
    if level.is_finite() && level > 0.0 {
        level as usize
    } else {
        0
    }
}

#[derive(Debug, Clone, Copy)]
struct ScoredOrdinal(f32, u32);

impl PartialEq for ScoredOrdinal {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}
impl Eq for ScoredOrdinal {}
impl PartialOrd for ScoredOrdinal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredOrdinal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .total_cmp(&other.0)
            .then_with(|| self.1.cmp(&other.1))
    }
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// A value in `(0, 1]`, never exactly zero so callers can safely take
    /// its logarithm.
    fn next_unit_f64(&mut self) -> f64 {
        let bits = self.next_u64() >> 11;
        ((bits as f64) + 1.0) / ((1u64 << 53) as f64 + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};

    fn axis_vector(dimension: usize, axis: usize) -> Vec<f32> {
        let mut vector = vec![0.0; dimension];
        vector[axis] = 1.0;
        vector
    }

    fn mixed_vector(id: u64, dimension: usize) -> Vec<f32> {
        (0..dimension)
            .map(|offset| {
                let mixed = id
                    .wrapping_mul(0x9e37_79b9)
                    .wrapping_add((offset as u64).wrapping_mul(0x85eb_ca6b));
                ((mixed % 2_001) as f32 - 1_000.0) / 1_000.0
            })
            .collect()
    }

    #[test]
    fn build_rejects_duplicate_ids() {
        let entries = vec![(1, axis_vector(4, 0)), (1, axis_vector(4, 1))];
        let result = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), None);
        assert!(matches!(result, Err(ProjectionError::DuplicateId(1))));
    }

    #[test]
    fn build_rejects_dimension_mismatch() {
        let entries = vec![(1, vec![0.0, 1.0])];
        let result = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), None);
        assert!(matches!(result, Err(ProjectionError::InvalidVector(_))));
    }

    #[test]
    fn build_fails_closed_when_the_estimate_exceeds_the_memory_budget() {
        let dimension = 128;
        let entries: Vec<(u64, Vec<f32>)> = (0..1_000)
            .map(|id| (id, mixed_vector(id, dimension)))
            .collect();
        // 1 KiB is nowhere near enough for 1,000 128-dimensional vectors
        // plus their graph edges; this must be rejected before any work
        // happens, not after partially building and running out of memory.
        let config = HnswBuildConfig::new().with_max_working_bytes(1_024);
        let result = HnswIndex::build(&entries, dimension, config, None);
        assert!(matches!(
            result,
            Err(ProjectionError::ResourceBudgetExceeded { .. })
        ));
    }

    #[test]
    fn build_admits_a_small_corpus_under_the_default_budget() {
        let dimension = 128;
        let entries: Vec<(u64, Vec<f32>)> = (0..1_000)
            .map(|id| (id, mixed_vector(id, dimension)))
            .collect();
        let result = HnswIndex::build(&entries, dimension, HnswBuildConfig::new(), None);
        assert!(result.is_ok());
    }

    #[test]
    fn build_honors_a_cancelled_task_context() {
        let entries = vec![(1, axis_vector(4, 0)), (2, axis_vector(4, 1))];
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        let result = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), Some(&context));
        assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    }

    #[test]
    fn search_honors_a_cancelled_task_context() {
        let entries = vec![(1, axis_vector(4, 0)), (2, axis_vector(4, 1))];
        let index = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), None).unwrap();
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        let result = index.search(&axis_vector(4, 0), 1, 10, Some(&context));
        assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    }

    #[test]
    fn search_rejects_non_finite_query() {
        let entries = vec![(1, axis_vector(4, 0))];
        let index = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), None).unwrap();
        let result = index.search(&[f32::NAN, 0.0, 0.0, 0.0], 1, 10, None);
        assert!(matches!(result, Err(ProjectionError::InvalidVector(_))));
    }

    #[test]
    fn empty_index_returns_no_hits() {
        let index = HnswIndex::build(&[], 4, HnswBuildConfig::new(), None).unwrap();
        let hits = index.search(&axis_vector(4, 0), 5, 50, None).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn single_node_index_finds_itself() {
        let entries = vec![(42, axis_vector(4, 2))];
        let index = HnswIndex::build(&entries, 4, HnswBuildConfig::new(), None).unwrap();
        let hits = index.search(&axis_vector(4, 2), 5, 50, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 42);
        assert!(hits[0].score > 0.99);
    }

    #[test]
    fn small_index_matches_exact_top_k_with_generous_ef() {
        let dimension = 16;
        let entries: Vec<(u64, Vec<f32>)> = (0..200)
            .map(|id| (id, mixed_vector(id, dimension)))
            .collect();
        let index = HnswIndex::build(
            &entries,
            dimension,
            HnswBuildConfig::new().with_m(16).with_ef_construction(200),
            None,
        )
        .unwrap();

        let query = mixed_vector(9_999, dimension);
        let top_k = 10;
        let approximate = index.search(&query, top_k, 256, None).unwrap();

        let mut exact: Vec<ProjectionHit> = entries
            .iter()
            .map(|(id, vector)| ProjectionHit {
                id: *id,
                score: exact_cosine(&query, vector),
            })
            .collect();
        exact.sort_by(|left, right| right.score.total_cmp(&left.score));
        exact.truncate(top_k);

        let exact_ids: HashSet<u64> = exact.iter().map(|hit| hit.id).collect();
        let approximate_ids: HashSet<u64> = approximate.iter().map(|hit| hit.id).collect();
        let overlap = exact_ids.intersection(&approximate_ids).count();
        // A generous ef_search over a small, non-adversarial dataset should
        // recover almost all of the true top-k; allow a little slack rather
        // than asserting bit-for-bit equality, since HNSW is approximate by
        // design even with a large ef.
        assert!(
            overlap >= top_k - 1,
            "expected at least {} of {top_k} exact hits, got {overlap}: exact={exact_ids:?} approx={approximate_ids:?}",
            top_k - 1
        );
    }

    #[test]
    fn recall_at_generous_ef_is_high_over_a_larger_corpus() {
        let dimension = 32;
        let count = 2_000;
        let entries: Vec<(u64, Vec<f32>)> = (0..count)
            .map(|id| (id, mixed_vector(id, dimension)))
            .collect();
        let index = HnswIndex::build(&entries, dimension, HnswBuildConfig::new(), None).unwrap();

        let top_k = 10;
        let mut total_overlap = 0usize;
        let query_count = 20;
        for query_id in 0..query_count {
            let query = mixed_vector(count + query_id, dimension);
            let approximate = index.search(&query, top_k, 200, None).unwrap();
            let mut exact: Vec<ProjectionHit> = entries
                .iter()
                .map(|(id, vector)| ProjectionHit {
                    id: *id,
                    score: exact_cosine(&query, vector),
                })
                .collect();
            exact.sort_by(|left, right| right.score.total_cmp(&left.score));
            exact.truncate(top_k);
            let exact_ids: HashSet<u64> = exact.iter().map(|hit| hit.id).collect();
            let approximate_ids: HashSet<u64> = approximate.iter().map(|hit| hit.id).collect();
            total_overlap += exact_ids.intersection(&approximate_ids).count();
        }
        let recall = total_overlap as f64 / (query_count as usize * top_k) as f64;
        assert!(
            recall >= 0.9,
            "expected recall@{top_k} >= 0.9 over {query_count} queries, got {recall}"
        );
    }

    fn exact_cosine(query: &[f32], vector: &[f32]) -> f32 {
        let query_norm = l2_norm(query);
        let vector_norm = l2_norm(vector);
        if query_norm <= f32::EPSILON || vector_norm <= f32::EPSILON {
            return 0.0;
        }
        let dot: f32 = query.iter().zip(vector.iter()).map(|(a, b)| a * b).sum();
        dot / (query_norm * vector_norm)
    }
}
