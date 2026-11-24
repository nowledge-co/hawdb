// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Complete evaluation of a small eligible domain after a short traversal.
//!
//! A traversal that spreads over much of a small domain scores most of it
//! anyway, yet can still miss a vertex whose few in-neighbors never enter the
//! beam (#1970). A probe measures that spread during the first expansions and
//! then finishes by evaluating the rest of the domain exactly.
//!
//! The constants are empirical heuristics, not a recall guarantee. A false
//! positive costs at most one evaluation of the capped domain. A false negative
//! leaves the ordinary traversal, with its ordinary recall.

use super::*;
use std::collections::BinaryHeap;

/// The domain may be evaluated only when it is at most a third of
/// `beam * max_degree`, so its size stays in proportion to the admitted beam.
const CAP_DIVISOR: usize = 3;
/// The probe decides after `beam / PROBE_DIVISOR` eligible expansions.
const PROBE_DIVISOR: usize = 16;
/// It switches once traversal discovered at least `1 / SPREAD_DIVISOR` of the
/// domain that filter seeding had not already evaluated.
const SPREAD_DIVISOR: usize = 14;

/// One decision that depends only on the pinned graph, the query, the filter
/// and the admitted options.
pub(super) struct SpreadProbe {
    expansions: usize,
    eligible: usize,
    initial_scores: usize,
    filtered_work: Option<usize>,
}

impl SpreadProbe {
    pub(super) fn new(
        graph: &impl DiskAnnRead,
        eligible: usize,
        initial_scores: usize,
        options: DiskAnnSearchOptions<'_>,
    ) -> Option<Self> {
        (eligible.saturating_mul(CAP_DIVISOR)
            <= options.beam_width.saturating_mul(graph.max_degree()))
        .then_some(Self {
            expansions: (options.beam_width / PROBE_DIVISOR).max(1),
            eligible,
            initial_scores,
            filtered_work: None,
        })
    }

    /// Accounts for the ID reads of a filtered completion.
    pub(super) fn with_filter(mut self, nodes: usize, ordinal_ids: bool) -> Self {
        self.filtered_work = Some(filtered_work(nodes, self.eligible, ordinal_ids));
        self
    }

    pub(super) fn should_scan(
        &self,
        eligible_expansions: usize,
        report: &DiskAnnSearchReport,
        options: DiskAnnSearchOptions<'_>,
    ) -> bool {
        if eligible_expansions != self.expansions {
            return false;
        }
        let discovered = report
            .distance_comparisons
            .saturating_sub(self.initial_scores);
        if discovered.saturating_mul(SPREAD_DIVISOR)
            < self.eligible.saturating_sub(self.initial_scores)
        {
            return false;
        }
        // Never replace a traversal that may fit the admitted work with a scan
        // whose worst case does not.
        let used = report.visited_nodes + report.id_lookup_steps + report.seed_steps;
        let remaining = options
            .task
            .and_then(RuntimeTaskContext::remaining_work)
            .unwrap_or(usize::MAX)
            .min(options.max_visited.saturating_sub(used));
        let required = self
            .filtered_work
            .unwrap_or_else(|| self.eligible.saturating_sub(report.visited_nodes));
        required <= remaining
    }
}

/// One list step, at most log2(N)+1 ID reads and one new visit per ID.
pub(super) fn filtered_work(nodes: usize, eligible: usize, ordinal_ids: bool) -> usize {
    let lookup_steps = if ordinal_ids {
        0
    } else {
        nodes.checked_ilog2().unwrap_or(0) as usize + 1
    };
    eligible.saturating_mul(lookup_steps + 2)
}

/// Evaluates every eligible vertex (all of them, or a filter with whether its
/// IDs equal ordinals) that traversal did not score and leaves the
/// exact nearest `top_k` in `scratch.beam`. Scored vertices that the beam has
/// already dropped are worse than `top_k` retained ones.
pub(super) fn complete(
    graph: &impl DiskAnnRead,
    query: &hawdb_core::vector::PreparedVectorQuery<'_>,
    top_k: usize,
    filter: Option<(CandidateSet<'_>, bool)>,
    scratch: &mut Scratch,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
) -> Result<()> {
    let limit = top_k.min(graph.len());
    // Reuses the admitted beam allocation, which holds at least `limit` entries.
    let mut retained = std::mem::take(&mut scratch.beam);
    retained.truncate(limit);
    let mut nearest = BinaryHeap::from(retained);
    if let Some((candidates, ordinal_ids)) = filter {
        for &id in candidates.ids {
            charge_query_work(options, report)?;
            report.seed_steps += 1;
            let ordinal = if ordinal_ids {
                (id < graph.len() as u64).then_some(id as u32)
            } else {
                find_id(graph, id, options, report)?
            };
            if let Some(ordinal) = ordinal
                && scratch.seen.state(ordinal) == 0
            {
                evaluate(graph, query, ordinal, &mut nearest, limit, options, report)?;
            }
        }
    } else {
        for ordinal in 0..graph.len() as u32 {
            if scratch.seen.state(ordinal) == 0 {
                evaluate(graph, query, ordinal, &mut nearest, limit, options, report)?;
            }
        }
    }
    scratch.beam = nearest.into_sorted_vec();
    Ok(())
}

fn evaluate(
    graph: &impl DiskAnnRead,
    query: &hawdb_core::vector::PreparedVectorQuery<'_>,
    ordinal: u32,
    nearest: &mut BinaryHeap<Neighbor>,
    limit: usize,
    options: DiskAnnSearchOptions<'_>,
    report: &mut DiskAnnSearchReport,
) -> Result<()> {
    charge_query_work(options, report)?;
    report.visited_nodes += 1;
    report.predicate_checks += 1;
    let candidate = Neighbor {
        ordinal,
        id: graph.id(ordinal)?,
        distance: checked_distance(graph.distance_prepared(query, ordinal, options.task)?)?,
        expanded: false,
    };
    report.distance_comparisons += 1;
    if nearest.len() < limit {
        nearest.push(candidate);
    } else if let Some(mut worst) = nearest.peek_mut()
        && candidate < *worst
    {
        *worst = candidate;
    }
    Ok(())
}
