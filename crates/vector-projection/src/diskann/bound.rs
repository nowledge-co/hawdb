// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Early end of a graph search whose frontier cannot improve known results.
//!
//! Independent graphs of one generation are searched in order. Once earlier
//! graphs hold enough results, a later graph need not expand its whole beam
//! (#1918). The constants are empirical heuristics, not a recall guarantee: a
//! closer vertex behind a worse frontier is missed, as behind a full beam.

use super::{DiskAnnRead, DiskAnnSearchOptions, Neighbor};
use hawdb_core::VectorDistance;

/// How much worse than the bound, as a ratio of lengths, the frontier must be.
const SLACK: f64 = 1.2;
/// The nearest `beam / RANK_DIVISOR` candidates, and at least `MIN_RANK`, are
/// always expanded. That is later than any spread-probe decision.
const RANK_DIVISOR: usize = 4;
const MIN_RANK: usize = 64;

pub(super) struct ResultBound {
    distance: f64,
    rank: usize,
    hits: usize,
}

impl ResultBound {
    pub(super) fn new(
        graph: &impl DiskAnnRead,
        top_k: usize,
        options: DiskAnnSearchOptions<'_>,
    ) -> Option<Self> {
        let slack = match graph.metric() {
            VectorDistance::Euclidean => SLACK,
            // Cosine distance is half the squared length of the chord.
            VectorDistance::Cosine => SLACK * SLACK,
        };
        options.result_bound.map(|bound| Self {
            distance: bound * slack,
            rank: (options.beam_width / RANK_DIVISOR).max(MIN_RANK),
            hits: top_k.min(graph.len()),
        })
    }

    /// `rank` is the position of the nearest unexpanded candidate.
    ///
    /// The nearest candidate must itself be within reach of the bound. Otherwise
    /// the traversal may not have arrived in the right region yet, so a distant
    /// frontier says nothing about what lies beyond it.
    pub(super) fn should_stop(&self, rank: usize, beam: &[Neighbor]) -> bool {
        rank >= self.rank
            && beam.len() >= self.hits
            && beam[rank].distance > self.distance
            && beam[0].distance <= self.distance
    }
}
