// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Immutable Vamana graphs and filter-aware traversal over a pinned provider.
//!
//! Providers own persistence and snapshot identity. A query never edits graph
//! links; replacements must publish a new graph with the canonical source epoch.

use crate::{CandidateSet, ProjectionError, Result};
use hawdb_core::{RuntimeTaskContext, VectorDistance};

mod artifact;
mod bound;
#[cfg(test)]
mod bound_tests;
mod build;
mod exact;
mod feedback;
#[cfg(test)]
mod parallel_build_tests;
#[cfg(test)]
mod tests;

pub use artifact::{DiskAnnArtifactIdentity, DiskAnnArtifactMetadata, FileDiskAnn};
pub use build::{DiskAnnBuildConfig, DiskAnnIndex, LsgBuildConfig};

/// A single immutable graph. Ordinals and links must belong to this same view.
/// Implementations validate links, dimensions and vector payloads before serving.
/// No method may acquire a newer root during a query.
pub trait DiskAnnRead {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn dimension(&self) -> usize;
    fn max_degree(&self) -> usize;
    fn metric(&self) -> VectorDistance;
    fn entry_point(&self) -> Option<u32>;
    /// Whether IDs increase strictly with ordinal in this validated view.
    /// This permits bounded lookup of eligible entry points without vector reads.
    fn ids_are_sorted(&self) -> bool {
        false
    }
    fn id(&self, ordinal: u32) -> Result<u64>;
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>>;
    fn distance(
        &self,
        query: &[f32],
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64>;
    /// Reuses validated query state. Providers with native raw vectors can avoid
    /// recomputing its norm; existing providers retain their distance contract.
    fn distance_prepared(
        &self,
        query: &hawdb_core::vector::PreparedVectorQuery<'_>,
        ordinal: u32,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<f64> {
        self.distance(query.coordinates(), ordinal, task)
    }
    /// Four `distance_prepared` results with the same bits. Providers with a
    /// batched kernel override this.
    fn distances_prepared(
        &self,
        query: &hawdb_core::vector::PreparedVectorQuery<'_>,
        ordinals: [u32; 4],
        task: Option<&RuntimeTaskContext>,
    ) -> Result<[f64; 4]> {
        let mut distances = [0.0; 4];
        for (distance, ordinal) in distances.iter_mut().zip(ordinals) {
            checkpoint(task)?;
            *distance = self.distance_prepared(query, ordinal, task)?;
        }
        Ok(distances)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DiskAnnSearchOptions<'a> {
    /// Maximum retained navigation candidates, independent of the output k.
    pub beam_width: usize,
    /// Bound unique visits, ID lookup steps and eligible-entry selection steps.
    /// Refuse rather than return partial results when this work bound is hit.
    pub max_visited: usize,
    pub max_working_bytes: usize,
    pub task: Option<&'a RuntimeTaskContext>,
    /// Raw distance of the worst result the caller already holds from other
    /// graphs, once it holds as many as it returns. A complete graph search may
    /// then end before its beam is expanded: hits worse than this distance are
    /// complete only up to the shortened traversal. Filtered traversal ignores it.
    pub result_bound: Option<f64>,
}

impl Default for DiskAnnSearchOptions<'_> {
    fn default() -> Self {
        Self {
            beam_width: 128,
            max_visited: 1_000_000,
            max_working_bytes: 64 << 20,
            task: None,
            result_bound: None,
        }
    }
}

impl DiskAnnSearchOptions<'_> {
    /// Complete navigation and result workspace, before any query allocation.
    pub fn working_memory_bound(self, node_count: usize, top_k: usize) -> Result<usize> {
        if node_count > u32::MAX as usize {
            return Err(corrupt("DiskANN provider count exceeds ordinal space"));
        }
        if self.beam_width == 0 || top_k > self.beam_width || self.max_visited == 0 {
            return Err(configuration(
                "DiskANN requires a positive beam/work bound and top-k <= beam width",
            ));
        }
        if node_count == 0 || top_k == 0 {
            return Ok(0);
        }
        scratch_bytes(
            node_count,
            self.beam_width.min(node_count),
            self.max_visited,
        )?
        .checked_add(
            top_k
                .min(node_count)
                .saturating_mul(size_of::<DiskAnnHit>()),
        )
        .ok_or_else(|| configuration("DiskANN query memory bound overflow"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiskAnnHit {
    pub id: u64,
    /// Raw metric distance, never the locally scaled construction distance.
    pub distance: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiskAnnSearchReport {
    pub neighbor_reads: usize,
    pub visited_nodes: usize,
    pub distance_comparisons: usize,
    pub expanded_nodes: usize,
    pub predicate_checks: usize,
    pub bridge_nodes: usize,
    pub id_lookup_steps: usize,
    pub seed_steps: usize,
    /// Traversal switched once to complete evaluation of the eligible domain.
    pub adaptive_exact_scan: bool,
    /// Traversal ended early at `DiskAnnSearchOptions::result_bound`.
    pub stopped_at_result_bound: bool,
}

#[derive(Debug)]
pub struct DiskAnnSearchOutput {
    pub hits: Vec<DiskAnnHit>,
    pub report: DiskAnnSearchReport,
}

/// Restrict scoring and the ANN window to an exact, same-generation allowlist.
/// Rejected vertices remain navigation bridges without reading their vectors.
/// This is multi-hop filtering rather than post-filtering an unfiltered window.
pub fn search_diskann(
    graph: &impl DiskAnnRead,
    query: &[f32],
    top_k: usize,
    candidates: Option<CandidateSet<'_>>,
    options: DiskAnnSearchOptions<'_>,
) -> Result<DiskAnnSearchOutput> {
    checkpoint(options.task)?;
    if graph.len() > u32::MAX as usize {
        return Err(corrupt("DiskANN provider count exceeds ordinal space"));
    }
    let query = validate_query(graph.dimension(), graph.metric(), query, options.task)?;
    if options.beam_width == 0 || top_k > options.beam_width || options.max_visited == 0 {
        return Err(configuration(
            "DiskANN requires a positive beam/work bound and top-k <= beam width",
        ));
    }
    if options
        .result_bound
        .is_some_and(|bound| !bound.is_finite() || bound < 0.0)
    {
        return Err(configuration(
            "DiskANN result bound must be a finite distance",
        ));
    }
    if let Some(candidates) = candidates {
        if !candidates.exact {
            return Err(configuration("DiskANN requires an exact eligible domain"));
        }
        let mut previous = None;
        for chunk in candidates.ids.chunks(4096) {
            checkpoint(options.task)?;
            if previous.is_some_and(|last| last >= chunk[0])
                || chunk.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(configuration("DiskANN allowlist must be sorted and unique"));
            }
            previous = chunk.last().copied();
        }
    }
    if top_k == 0 || graph.is_empty() || candidates.is_some_and(|set| set.ids.is_empty()) {
        return Ok(DiskAnnSearchOutput {
            hits: Vec::new(),
            report: DiskAnnSearchReport::default(),
        });
    }
    let beam_width = options.beam_width.min(graph.len());
    let required = options.working_memory_bound(graph.len(), top_k)?;
    require_memory(required, options.max_working_bytes)?;
    let mut scratch = Scratch::new(graph.len(), beam_width, options.max_visited)?;
    let report = if let Some(candidates) = candidates.filter(|_| graph.ids_are_sorted()) {
        traverse_filtered(graph, &query, top_k, candidates, &mut scratch, options)?
    } else {
        let mut report = traverse(
            graph,
            &mut scratch,
            options,
            // Without sorted IDs, eligible vertices cannot be looked up directly.
            candidates
                .is_none()
                .then(|| exact::SpreadProbe::new(graph, graph.len(), 0, options))
                .flatten(),
            candidates
                .is_none()
                .then(|| bound::ResultBound::new(graph, top_k, options))
                .flatten(),
            |id| Ok(candidates.is_none_or(|set| set.ids.binary_search(&id).is_ok())),
            QueryScorer {
                graph,
                query: &query,
                task: options.task,
            },
        )?;
        if report.adaptive_exact_scan {
            exact::complete(
                graph,
                &query,
                top_k,
                None,
                &mut scratch,
                options,
                &mut report,
            )?;
        }
        report
    };
    let mut hits = Vec::with_capacity(top_k.min(scratch.beam.len()));
    for neighbor in scratch.beam.iter().take(top_k) {
        checkpoint(options.task)?;
        hits.push(DiskAnnHit {
            id: neighbor.id,
            distance: neighbor.distance,
        });
    }
    Ok(DiskAnnSearchOutput { hits, report })
}

#[derive(Debug, Clone, Copy)]
struct Neighbor {
    ordinal: u32,
    id: u64,
    distance: f64,
    expanded: bool,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other).is_eq()
    }
}
impl Eq for Neighbor {}
impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(other)
    }
}

impl Neighbor {
    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct Scratch {
    seen: Seen,
    bridges: Vec<u32>,
    beam: Vec<Neighbor>,
    /// Construction needs all visited candidates, not just the final beam.
    visited: Vec<u32>,
    collect_visited: bool,
}

impl Scratch {
    fn new(count: usize, beam: usize, max_visited: usize) -> Result<Self> {
        Ok(Self {
            seen: Seen::new(count, max_visited)?,
            bridges: Vec::with_capacity(count.min(max_visited)),
            beam: Vec::with_capacity(beam),
            visited: Vec::new(),
            collect_visited: false,
        })
    }

    fn reset(&mut self, task: Option<&RuntimeTaskContext>) -> Result<()> {
        self.seen.reset(task)?;
        self.bridges.clear();
        self.beam.clear();
        self.visited.clear();
        Ok(())
    }
}

enum Seen {
    Dense(Vec<u8>),
    Sparse(Vec<u64>),
}

impl Seen {
    fn new(count: usize, max_visited: usize) -> Result<Self> {
        Ok(if count <= max_visited {
            Self::Dense(vec![0; count])
        } else {
            Self::Sparse(vec![u64::MAX; sparse_slots(max_visited)?])
        })
    }
    fn reset(&mut self, task: Option<&RuntimeTaskContext>) -> Result<()> {
        match self {
            Self::Dense(values) => {
                for chunk in values.chunks_mut(16_384) {
                    checkpoint(task)?;
                    chunk.fill(0);
                }
            }
            Self::Sparse(values) => {
                for chunk in values.chunks_mut(4096) {
                    checkpoint(task)?;
                    chunk.fill(u64::MAX);
                }
            }
        }
        Ok(())
    }
    fn slot(values: &[u64], ordinal: u32) -> usize {
        let mask = values.len() - 1;
        let mut position = ordinal.wrapping_mul(0x9e37_79b9) as usize & mask;
        while values[position] != u64::MAX && values[position] as u32 != ordinal {
            position = (position + 1) & mask;
        }
        position
    }
    fn state(&self, ordinal: u32) -> u8 {
        match self {
            Self::Dense(values) => values[ordinal as usize],
            Self::Sparse(values) => {
                let value = values[Self::slot(values, ordinal)];
                if value == u64::MAX {
                    0
                } else {
                    (value >> 32) as u8
                }
            }
        }
    }
    fn set(&mut self, ordinal: u32, state: u8) {
        match self {
            Self::Dense(values) => values[ordinal as usize] = state,
            Self::Sparse(values) => {
                let position = Self::slot(values, ordinal);
                values[position] = u64::from(ordinal) | (u64::from(state) << 32);
            }
        }
    }
}

fn traverse(
    graph: &impl DiskAnnRead,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    probe: Option<exact::SpreadProbe>,
    bound: Option<bound::ResultBound>,
    allowed: impl FnMut(u64) -> Result<bool>,
    score: impl Scorer,
) -> Result<DiskAnnSearchReport> {
    scratch.reset(options.task)?;
    let mut report = DiskAnnSearchReport::default();
    let Some(entry) = graph.entry_point() else {
        if !graph.is_empty() {
            return Err(corrupt("nonempty DiskANN graph has no entry point"));
        }
        return Ok(report);
    };
    let mut policy = VisitPolicy {
        allowed,
        score,
        expand_rejected: true,
    };
    discover(graph, entry, scratch, options, &mut report, &mut policy)?;
    let decisions = Decisions {
        feedback: None,
        probe,
        bound,
    };
    walk(graph, scratch, options, report, policy, decisions)
}

fn traverse_filtered(
    graph: &impl DiskAnnRead,
    query: &hawdb_core::vector::PreparedVectorQuery<'_>,
    top_k: usize,
    candidates: CandidateSet<'_>,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
) -> Result<DiskAnnSearchReport> {
    scratch.reset(options.task)?;
    let mut report = DiskAnnSearchReport::default();
    let ordinal_ids = lookup_id(graph, 0, options, &mut report)? == 0
        && lookup_id(graph, (graph.len() - 1) as u32, options, &mut report)?
            == (graph.len() - 1) as u64;
    let mut policy = VisitPolicy {
        allowed: |id| Ok(candidates.ids.binary_search(&id).is_ok()),
        score: QueryScorer {
            graph,
            query,
            task: options.task,
        },
        expand_rejected: true,
    };
    let complete_domain = ordinal_ids
        && candidates.ids.len() == graph.len()
        && candidates.ids.first() == Some(&0)
        && candidates.ids.last() == Some(&((graph.len() - 1) as u64));
    if !complete_domain {
        let samples = candidates.ids.len().min(options.beam_width);
        for index in 0..samples {
            charge_query_work(options, &report)?;
            report.seed_steps += 1;
            let id = candidates.ids[seed_position(index, samples, candidates.ids.len())];
            let ordinal = if ordinal_ids {
                (id < graph.len() as u64).then_some(id as u32)
            } else {
                find_id(graph, id, options, &mut report)?
            };
            if let Some(ordinal) = ordinal {
                discover(graph, ordinal, scratch, options, &mut report, &mut policy)?;
            }
        }
        // Every eligible ID has been looked up. Link traversal cannot add a hit.
        if candidates.ids.len() <= options.beam_width {
            return Ok(report);
        }
    }
    let probe = exact::SpreadProbe::new(
        graph,
        candidates.ids.len(),
        report.distance_comparisons,
        options,
    )
    .map(|probe| {
        if complete_domain {
            probe
        } else {
            probe.with_filter(graph.len(), ordinal_ids)
        }
    });
    let entry = graph
        .entry_point()
        .ok_or_else(|| corrupt("nonempty DiskANN graph has no entry point"))?;
    discover(graph, entry, scratch, options, &mut report, &mut policy)?;
    let feedback = (!complete_domain).then(|| {
        feedback::FilteredFeedback::new(graph.len(), candidates.ids.len(), ordinal_ids, &report)
    });
    let bound = complete_domain
        .then(|| bound::ResultBound::new(graph, top_k, options))
        .flatten();
    let decisions = Decisions {
        feedback,
        probe,
        bound,
    };
    let mut report = walk(graph, scratch, options, report, policy, decisions)?;
    if report.adaptive_exact_scan {
        // No results have escaped. Reuse the admitted workspace and cumulative
        // work counters; already scored eligible nodes need no second distance.
        let domain = (!complete_domain).then_some((candidates, ordinal_ids));
        exact::complete(graph, query, top_k, domain, scratch, options, &mut report)?;
    }
    Ok(report)
}

// One independently mixed entry from each disjoint ID-list bucket. Stratifying
// avoids duplicate seeds and requires no allocation or retry loop.
fn seed_position(index: usize, samples: usize, count: usize) -> usize {
    let start = (index as u128 * count as u128 / samples as u128) as usize;
    let end = ((index as u128 + 1) * count as u128 / samples as u128) as usize;
    let mut mixed = (index as u64) ^ (count as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    start + ((mixed ^ (mixed >> 31)) % (end - start) as u64) as usize
}

fn lookup_id(
    graph: &impl DiskAnnRead,
    ordinal: u32,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
) -> Result<u64> {
    charge_query_work(options, report)?;
    report.id_lookup_steps += 1;
    graph.id(ordinal)
}

fn find_id(
    graph: &impl DiskAnnRead,
    id: u64,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
) -> Result<Option<u32>> {
    let (mut left, mut right) = (0, graph.len());
    while left < right {
        let middle = left + (right - left) / 2;
        match lookup_id(graph, middle as u32, options, report)?.cmp(&id) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle as u32)),
        }
    }
    Ok(None)
}

/// When traversal may end before its beam is expanded.
struct Decisions {
    feedback: Option<feedback::FilteredFeedback>,
    probe: Option<exact::SpreadProbe>,
    bound: Option<bound::ResultBound>,
}

fn walk(
    graph: &impl DiskAnnRead,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    mut report: DiskAnnSearchReport,
    mut policy: VisitPolicy<impl FnMut(u64) -> Result<bool>, impl Scorer>,
    decisions: Decisions,
) -> Result<DiskAnnSearchReport> {
    let Decisions {
        feedback,
        probe,
        bound,
    } = decisions;
    let mut eligible_expansions = 0;
    loop {
        checkpoint(options.task)?;
        let (ordinal, bridge) = if let Some(bridge) = scratch.bridges.pop() {
            (bridge, true)
        } else if let Some(rank) = scratch.beam.iter().position(|value| !value.expanded) {
            if probe
                .as_ref()
                .is_some_and(|probe| probe.should_scan(eligible_expansions, &report, options))
            {
                report.adaptive_exact_scan = true;
                return Ok(report);
            }
            if bound
                .as_ref()
                .is_some_and(|bound| bound.should_stop(rank, &scratch.beam))
            {
                report.stopped_at_result_bound = true;
                return Ok(report);
            }
            eligible_expansions += 1;
            let neighbor = &mut scratch.beam[rank];
            neighbor.expanded = true;
            (neighbor.ordinal, false)
        } else {
            break;
        };
        policy.expand_rejected = feedback.is_none() || !bridge;
        report.expanded_nodes += 1;
        // Feedback may stop after any neighbor, so it scores one at a time.
        let batch = if feedback.is_some() { 1 } else { 4 };
        let mut pending = Pending::default();
        for (index, neighbor) in graph.neighbors(ordinal)?.enumerate() {
            report.neighbor_reads += 1;
            if index >= graph.max_degree() {
                return Err(corrupt("DiskANN degree exceeds the provider bound"));
            }
            if let Some(id) = admit(graph, neighbor, scratch, options, &mut report, &mut policy)? {
                pending.push(neighbor, id);
            }
            if pending.len == batch {
                score_pending(
                    graph,
                    &mut pending,
                    scratch,
                    options,
                    &mut report,
                    &mut policy,
                )?;
            }
            if feedback
                .as_ref()
                .is_some_and(|feedback| feedback.should_scan(&report, options.max_visited))
            {
                report.adaptive_exact_scan = true;
                return Ok(report);
            }
        }
        score_pending(
            graph,
            &mut pending,
            scratch,
            options,
            &mut report,
            &mut policy,
        )?;
    }
    Ok(report)
}

/// Discovered vertices whose distances are not yet known.
#[derive(Default)]
struct Pending {
    ordinals: [u32; 4],
    ids: [u64; 4],
    len: usize,
}

impl Pending {
    fn push(&mut self, ordinal: u32, id: u64) {
        self.ordinals[self.len] = ordinal;
        self.ids[self.len] = id;
        self.len += 1;
    }
}

/// Distances of one or four vertices. Four at once must equal four single ones.
trait Scorer {
    fn one(&mut self, ordinal: u32) -> Result<f64>;
    fn four(&mut self, ordinals: [u32; 4]) -> Result<[f64; 4]> {
        let mut distances = [0.0; 4];
        for (distance, ordinal) in distances.iter_mut().zip(ordinals) {
            *distance = self.one(ordinal)?;
        }
        Ok(distances)
    }
}

impl<F: FnMut(u32) -> Result<f64>> Scorer for F {
    fn one(&mut self, ordinal: u32) -> Result<f64> {
        self(ordinal)
    }
}

struct QueryScorer<'a, 'q, G> {
    graph: &'a G,
    query: &'a hawdb_core::vector::PreparedVectorQuery<'q>,
    task: Option<&'a RuntimeTaskContext>,
}

impl<G: DiskAnnRead> Scorer for QueryScorer<'_, '_, G> {
    fn one(&mut self, ordinal: u32) -> Result<f64> {
        self.graph.distance_prepared(self.query, ordinal, self.task)
    }
    fn four(&mut self, ordinals: [u32; 4]) -> Result<[f64; 4]> {
        self.graph
            .distances_prepared(self.query, ordinals, self.task)
    }
}

struct VisitPolicy<F, S> {
    allowed: F,
    score: S,
    expand_rejected: bool,
}

/// Marks and charges a new vertex and returns its ID when it must be scored.
fn admit(
    graph: &impl DiskAnnRead,
    ordinal: u32,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
    policy: &mut VisitPolicy<impl FnMut(u64) -> Result<bool>, impl Scorer>,
) -> Result<Option<u64>> {
    checkpoint(options.task)?;
    if ordinal as usize >= graph.len() {
        return Err(corrupt("DiskANN link is outside its pinned graph"));
    }
    let state = scratch.seen.state(ordinal);
    if state != 0 {
        // A rejected second-hop vertex can later be a first-hop neighbor of an
        // eligible vertex. Promote it once without another predicate/vector read.
        if state == 2 && policy.expand_rejected {
            scratch.seen.set(ordinal, 3);
            scratch.bridges.push(ordinal);
        }
        return Ok(None);
    }
    charge_query_work(options, report)?;
    scratch.seen.set(ordinal, 1);
    report.visited_nodes += 1;
    report.predicate_checks += 1;
    let id = graph.id(ordinal)?;
    if !(policy.allowed)(id)? {
        scratch
            .seen
            .set(ordinal, if policy.expand_rejected { 3 } else { 2 });
        if policy.expand_rejected {
            scratch.bridges.push(ordinal);
        }
        report.bridge_nodes += 1;
        return Ok(None);
    }
    Ok(Some(id))
}

fn discover(
    graph: &impl DiskAnnRead,
    ordinal: u32,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
    policy: &mut VisitPolicy<impl FnMut(u64) -> Result<bool>, impl Scorer>,
) -> Result<()> {
    let mut pending = Pending::default();
    if let Some(id) = admit(graph, ordinal, scratch, options, report, policy)? {
        pending.push(ordinal, id);
    }
    score_pending(graph, &mut pending, scratch, options, report, policy)
}

/// Scores admitted vertices in order. Retention depends only on distance and ID.
fn score_pending(
    graph: &impl DiskAnnRead,
    pending: &mut Pending,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
    policy: &mut VisitPolicy<impl FnMut(u64) -> Result<bool>, impl Scorer>,
) -> Result<()> {
    if pending.len == 4 {
        let distances = policy.score.four(pending.ordinals)?;
        for (slot, distance) in distances.into_iter().enumerate() {
            let (ordinal, id) = (pending.ordinals[slot], pending.ids[slot]);
            retain(graph, ordinal, id, distance, scratch, options, report)?;
        }
    } else {
        for slot in 0..pending.len {
            let (ordinal, id) = (pending.ordinals[slot], pending.ids[slot]);
            checkpoint(options.task)?;
            let distance = policy.score.one(ordinal)?;
            retain(graph, ordinal, id, distance, scratch, options, report)?;
        }
    }
    pending.len = 0;
    Ok(())
}

fn retain(
    graph: &impl DiskAnnRead,
    ordinal: u32,
    id: u64,
    distance: f64,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
) -> Result<()> {
    let distance = checked_distance(distance)?;
    report.distance_comparisons += 1;
    if scratch.collect_visited {
        scratch.visited.push(ordinal);
    }
    let neighbor = Neighbor {
        ordinal,
        id,
        distance,
        expanded: false,
    };
    let position = scratch
        .beam
        .partition_point(|existing| existing.compare(&neighbor).is_lt());
    let capacity = options.beam_width.min(graph.len());
    if position < capacity {
        // Pop before insertion so the allocated beam never exceeds admission.
        if scratch.beam.len() == capacity {
            scratch.beam.pop();
        }
        scratch.beam.insert(position, neighbor);
    }
    Ok(())
}

fn checked_distance(distance: f64) -> Result<f64> {
    if !distance.is_finite() || distance < 0.0 {
        return Err(corrupt("DiskANN provider returned an invalid distance"));
    }
    Ok(distance)
}

fn charge_query_work(
    options: DiskAnnSearchOptions<'_>,
    report: &DiskAnnSearchReport,
) -> Result<()> {
    checkpoint(options.task)?;
    let used = report.visited_nodes + report.id_lookup_steps + report.seed_steps;
    if used >= options.max_visited {
        return Err(ProjectionError::Admission(
            hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::WorkUnits,
                scope: hawdb_core::ResourceBudgetScope::Operator,
                owner: "DiskANN traversal".into(),
                memory_class: None,
                requested: 1,
                used: used as u64,
                budget: options.max_visited as u64,
                budget_option: "DiskAnnSearchOptions.max_visited",
                message: "DiskANN traversal work budget exhausted".into(),
            }
            .into(),
        ));
    }
    if let Some(task) = options.task {
        task.charge_work(1).map_err(ProjectionError::Admission)?;
    }
    Ok(())
}

fn validate_query<'a>(
    dimension: usize,
    metric: VectorDistance,
    vector: &'a [f32],
    task: Option<&RuntimeTaskContext>,
) -> Result<hawdb_core::vector::PreparedVectorQuery<'a>> {
    if vector.len() != dimension {
        return Err(ProjectionError::InvalidVector(
            "DiskANN vector dimension differs from its graph".into(),
        ));
    }
    with_distance_work(task, |checkpoint| {
        hawdb_core::vector::PreparedVectorQuery::for_distance(metric, vector, checkpoint)
    })
}

fn distance(
    metric: VectorDistance,
    left: &[f32],
    right: &[f32],
    task: Option<&RuntimeTaskContext>,
) -> Result<f64> {
    with_distance_work(task, |checkpoint| {
        metric.distance_slices_with_work(left, right, checkpoint)
    })
}

fn with_distance_work<T>(
    task: Option<&RuntimeTaskContext>,
    operation: impl FnOnce(&mut dyn FnMut() -> hawdb_core::Result<()>) -> hawdb_core::Result<T>,
) -> Result<T> {
    let mut cancellation = None;
    let result = operation(&mut || {
        if let Some(task) = task
            && let Err(reason) = task.checkpoint()
        {
            cancellation = Some(reason);
            return Err(hawdb_core::HawDBError::RuntimeTaskStopped(reason));
        }
        Ok(())
    });
    if let Some(reason) = cancellation {
        return Err(ProjectionError::Cancelled(reason));
    }
    result.map_err(|error| ProjectionError::InvalidVector(error.to_string()))
}

fn sparse_slots(max_visited: usize) -> Result<usize> {
    max_visited
        .checked_mul(2)
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(|| configuration("DiskANN visited-set capacity overflow"))
}

fn scratch_bytes(count: usize, beam: usize, max_visited: usize) -> Result<usize> {
    let seen_bytes = if count <= max_visited {
        count
    } else {
        sparse_slots(max_visited)?
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| configuration("DiskANN visited-set memory overflow"))?
    };
    count
        .min(max_visited)
        .checked_mul(size_of::<u32>())
        .and_then(|bytes| bytes.checked_add(seen_bytes))
        .and_then(|bytes| bytes.checked_add(beam.checked_mul(size_of::<Neighbor>())?))
        .ok_or_else(|| configuration("DiskANN scratch memory bound overflow"))
}

fn require_memory(required: usize, available: usize) -> Result<()> {
    if required > available {
        return Err(ProjectionError::ResourceBudgetExceeded {
            required,
            available,
        });
    }
    Ok(())
}

fn checkpoint(task: Option<&RuntimeTaskContext>) -> Result<()> {
    if let Some(task) = task {
        task.checkpoint()?;
    }
    Ok(())
}

fn configuration(reason: &str) -> ProjectionError {
    ProjectionError::InvalidConfiguration(reason.into())
}

fn corrupt(reason: &str) -> ProjectionError {
    ProjectionError::CorruptArtifact(reason.into())
}
