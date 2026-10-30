// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::DiskAnnSearchReport;

pub(super) struct FilteredFeedback {
    initial_visits: usize,
    initial_scores: usize,
    eligible_count: usize,
    exact_work: usize,
}

impl FilteredFeedback {
    pub fn new(
        nodes: usize,
        eligible_count: usize,
        ordinal_ids: bool,
        report: &DiskAnnSearchReport,
    ) -> Self {
        // One iteration, at most log2(N)+1 ID reads, and one new visit per ID.
        // Missing IDs and repeated seeds only reduce this conservative bound.
        let lookup_steps = if ordinal_ids {
            0
        } else {
            nodes.checked_ilog2().unwrap_or(0) as usize + 1
        };
        Self {
            initial_visits: report.visited_nodes,
            initial_scores: report.distance_comparisons,
            eligible_count,
            exact_work: eligible_count.saturating_mul(lookup_steps + 2),
        }
    }

    pub fn should_scan(&self, report: &DiskAnnSearchReport, max_work: usize) -> bool {
        let visits = report.visited_nodes.saturating_sub(self.initial_visits);
        let scores = report
            .distance_comparisons
            .saturating_sub(self.initial_scores);
        let used = report
            .visited_nodes
            .saturating_add(report.id_lookup_steps)
            .saturating_add(report.seed_steps);
        // Require actual poor traversal yield, not merely a selective predicate.
        // Avoid turning a short traversal into an arbitrarily large exact scan.
        visits >= 32
            && scores.saturating_mul(8) <= visits
            && self.eligible_count <= visits.saturating_mul(4)
            && self.exact_work <= max_work.saturating_sub(used)
    }
}
