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
        Self {
            initial_visits: report.visited_nodes,
            initial_scores: report.distance_comparisons,
            eligible_count,
            // Missing IDs and repeated seeds only reduce this conservative bound.
            exact_work: super::exact::filtered_work(nodes, eligible_count, ordinal_ids),
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
