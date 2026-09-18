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

//! `IndexAdvisor` / `AutoIndexPolicy` / `IndexPlanner`: a skeleton for
//! turning observed query workload into index-maintenance recommendations.
//!
//! This module is explain/recommend-only. Nothing in it builds, rebuilds,
//! or discards an index; `IndexAdvisor::recommend` only ever returns a
//! recommendation plus the reasoning behind it, `AutoIndexPolicy` only
//! ever decides whether a recommendation clears a confidence bar, and
//! `IndexPlanner` only ever picks among indexes the caller says already
//! exist. Auto-apply -- actually executing a recommendation -- requires a
//! shadow-build-and-rollback safety net this crate does not implement yet;
//! wiring one up is expected to be a separate, later change once this
//! skeleton's recommendations have been observed against real workloads.
//! The exports are retained as experiments under the
//! [experimental API roadmap](crate#experimental-apis); they do not certify
//! artifact identity, recall, resource admission, or production readiness.
//!
//! `WorkloadSample` is a plain, dependency-free summary a caller
//! populates from whatever signals it already has -- a `DeltaBuffer`'s
//! `len()`/`fraction_of()`, aggregated `ProjectionSearchReport`s, and
//! whether an `HnswIndex` has been built -- rather than this module
//! depending on those types directly, so the advisor works the same way
//! regardless of which of those a given caller has adopted.

/// Aggregated signals about how a projection has actually been queried,
/// fed to `IndexAdvisor::recommend`.
/// See the [experimental API roadmap](crate#experimental-apis).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkloadSample {
    /// Total document count across base and any un-indexed delta.
    pub document_count: usize,
    /// Fraction of `document_count` living in an un-indexed delta, e.g.
    /// `DeltaBuffer::fraction_of`. Zero if there is no delta buffer.
    pub delta_fraction: f64,
    /// Observed p95 query latency for the current path (exact scan unless
    /// `hnsw_available`).
    pub p95_search_seconds: f64,
    /// Whether an `HnswIndex` has already been built over this document
    /// set.
    pub hnsw_available: bool,
}

impl WorkloadSample {
    pub fn new(document_count: usize) -> Self {
        Self {
            document_count,
            delta_fraction: 0.0,
            p95_search_seconds: 0.0,
            hnsw_available: false,
        }
    }

    pub fn with_delta_fraction(mut self, delta_fraction: f64) -> Self {
        self.delta_fraction = delta_fraction;
        self
    }

    pub fn with_p95_search_seconds(mut self, p95_search_seconds: f64) -> Self {
        self.p95_search_seconds = p95_search_seconds;
        self
    }

    pub fn with_hnsw_available(mut self, hnsw_available: bool) -> Self {
        self.hnsw_available = hnsw_available;
        self
    }
}

/// An [experimental recommendation](crate#experimental-apis), not a work request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAction {
    /// Current index shape already matches the observed workload.
    NoActionNeeded,
    /// The un-indexed delta has grown past its cost-effective size; fold
    /// it into a fresh base rebuild (see `DeltaBuffer::should_optimize`).
    FoldDeltaIntoBase,
    /// The corpus and observed latency suggest an `HnswIndex` would pay
    /// for its build cost and memory (see the recall/latency/memory
    /// trade-off measured in `benches/hnsw_vs_exact_scan.rs`).
    ConsiderBuildingHnsw,
}

/// Advice and heuristic confidence; see the [roadmap](crate#experimental-apis).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexRecommendation {
    pub action: IndexAction,
    pub reason: String,
    /// How clearly the sample cleared the relevant threshold, in `[0, 1]`.
    /// Not a probability -- just how far past the trigger point the
    /// sample sits, clipped to 1.0. `NoActionNeeded` always reports 0.0.
    pub confidence: f32,
}

/// Offline/explain: given one workload sample, decide what -- if anything
/// -- about the index shape should change, and why.
/// See the [experimental API roadmap](crate#experimental-apis); fixture agreement
/// is not evidence that the advice improves an embedded workload.
#[derive(Debug, Clone, Copy)]
pub struct IndexAdvisor {
    /// Delta fraction at or above which folding is recommended. Defaults
    /// to `DeltaBuffer`'s own calibrated default (see its module docs).
    delta_fold_threshold: f64,
    /// Corpus size at or above which HNSW starts being considered.
    hnsw_document_count_threshold: usize,
    /// p95 latency at or above which HNSW starts being considered.
    hnsw_latency_threshold_seconds: f64,
}

impl IndexAdvisor {
    pub fn new() -> Self {
        Self {
            delta_fold_threshold: 0.02,
            hnsw_document_count_threshold: 5_000,
            hnsw_latency_threshold_seconds: 0.001,
        }
    }

    pub fn with_delta_fold_threshold(mut self, delta_fold_threshold: f64) -> Self {
        self.delta_fold_threshold = delta_fold_threshold;
        self
    }

    pub fn with_hnsw_document_count_threshold(mut self, threshold: usize) -> Self {
        self.hnsw_document_count_threshold = threshold;
        self
    }

    pub fn with_hnsw_latency_threshold_seconds(mut self, threshold: f64) -> Self {
        self.hnsw_latency_threshold_seconds = threshold;
        self
    }

    pub fn recommend(&self, sample: &WorkloadSample) -> IndexRecommendation {
        if sample.delta_fraction >= self.delta_fold_threshold {
            let confidence = (sample.delta_fraction / self.delta_fold_threshold.max(f64::EPSILON)
                - 1.0)
                .clamp(0.0, 1.0) as f32;
            return IndexRecommendation {
                action: IndexAction::FoldDeltaIntoBase,
                reason: format!(
                    "delta is {:.1}% of the combined document count, at or above the {:.1}% fold threshold",
                    sample.delta_fraction * 100.0,
                    self.delta_fold_threshold * 100.0
                ),
                confidence,
            };
        }

        if !sample.hnsw_available
            && sample.document_count >= self.hnsw_document_count_threshold
            && sample.p95_search_seconds >= self.hnsw_latency_threshold_seconds
        {
            let document_ratio =
                sample.document_count as f64 / self.hnsw_document_count_threshold as f64;
            let latency_ratio =
                sample.p95_search_seconds / self.hnsw_latency_threshold_seconds.max(f64::EPSILON);
            let confidence =
                ((document_ratio.min(latency_ratio) - 1.0) / 2.0).clamp(0.0, 1.0) as f32;
            return IndexRecommendation {
                action: IndexAction::ConsiderBuildingHnsw,
                reason: format!(
                    "{} documents and p95 {:.2}ms exceed the {}-document / {:.2}ms threshold where HnswIndex's recall and latency advantage was measured to outweigh its build cost and memory",
                    sample.document_count,
                    sample.p95_search_seconds * 1000.0,
                    self.hnsw_document_count_threshold,
                    self.hnsw_latency_threshold_seconds * 1000.0
                ),
                confidence,
            };
        }

        IndexRecommendation {
            action: IndexAction::NoActionNeeded,
            reason: "delta fraction and query latency are both within the current index shape's \
                     comfortable range"
                .to_string(),
            confidence: 0.0,
        }
    }
}

impl Default for IndexAdvisor {
    fn default() -> Self {
        Self::new()
    }
}

/// Online decision: whether a recommendation clears the bar for even
/// being surfaced as actionable. Never applies anything itself -- a
/// future auto-apply phase would gate on `is_eligible` returning true
/// *and* a shadow-build-and-rollback step this skeleton does not
/// implement.
/// See the [experimental API roadmap](crate#experimental-apis).
#[derive(Debug, Clone, Copy)]
pub struct AutoIndexPolicy {
    min_confidence: f32,
}

impl AutoIndexPolicy {
    pub fn new(min_confidence: f32) -> Self {
        Self { min_confidence }
    }

    pub fn is_eligible(&self, recommendation: &IndexRecommendation) -> bool {
        recommendation.action != IndexAction::NoActionNeeded
            && recommendation.confidence >= self.min_confidence
    }
}

impl Default for AutoIndexPolicy {
    fn default() -> Self {
        Self::new(0.1)
    }
}

/// Query-time path selection among indexes the caller says already exist.
/// Never builds or rebuilds anything.
/// See the [experimental API roadmap](crate#experimental-apis). `ExactScan`
/// names exhaustive candidate scanning, not a guarantee of raw-vector scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPath {
    ExactScan,
    Hnsw,
}

/// An experimental availability selector, not the embedded query planner.
/// See the [roadmap](crate#experimental-apis) for integration prerequisites.
#[derive(Debug, Clone, Copy)]
pub struct IndexPlanner;

impl IndexPlanner {
    /// `HnswIndex::search` (as of this crate's first HNSW implementation)
    /// takes no candidate filter, so a query that needs one can only be
    /// answered precisely by the exact scan's allowlist pushdown -- this
    /// is a real capability gap, not a tunable preference.
    pub fn choose(hnsw_available: bool, candidate_filter_required: bool) -> QueryPath {
        if hnsw_available && !candidate_filter_required {
            QueryPath::Hnsw
        } else {
            QueryPath::ExactScan
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommends_no_action_for_a_small_healthy_workload() {
        let advisor = IndexAdvisor::new();
        let sample = WorkloadSample::new(500).with_p95_search_seconds(0.0002);
        let recommendation = advisor.recommend(&sample);
        assert_eq!(recommendation.action, IndexAction::NoActionNeeded);
        assert_eq!(recommendation.confidence, 0.0);
    }

    #[test]
    fn recommends_folding_the_delta_once_past_threshold() {
        let advisor = IndexAdvisor::new();
        let sample = WorkloadSample::new(10_000).with_delta_fraction(0.05);
        let recommendation = advisor.recommend(&sample);
        assert_eq!(recommendation.action, IndexAction::FoldDeltaIntoBase);
        assert!(recommendation.confidence > 0.0);
        assert!(recommendation.reason.contains("5.0%"));
    }

    #[test]
    fn recommends_hnsw_for_a_large_slow_workload_without_one_already() {
        let advisor = IndexAdvisor::new();
        let sample = WorkloadSample::new(20_000).with_p95_search_seconds(0.003);
        let recommendation = advisor.recommend(&sample);
        assert_eq!(recommendation.action, IndexAction::ConsiderBuildingHnsw);
        assert!(recommendation.confidence > 0.0);
    }

    #[test]
    fn does_not_recommend_hnsw_again_once_one_is_already_available() {
        let advisor = IndexAdvisor::new();
        let sample = WorkloadSample::new(20_000)
            .with_p95_search_seconds(0.003)
            .with_hnsw_available(true);
        let recommendation = advisor.recommend(&sample);
        assert_eq!(recommendation.action, IndexAction::NoActionNeeded);
    }

    #[test]
    fn delta_fold_is_checked_before_hnsw_consideration() {
        // A sample that would trigger both should still report the delta
        // fold, since an oversized delta degrades every query regardless
        // of which index answers it, and folding is cheaper than a build.
        let advisor = IndexAdvisor::new();
        let sample = WorkloadSample::new(20_000)
            .with_delta_fraction(0.1)
            .with_p95_search_seconds(0.003);
        let recommendation = advisor.recommend(&sample);
        assert_eq!(recommendation.action, IndexAction::FoldDeltaIntoBase);
    }

    #[test]
    fn policy_rejects_low_confidence_recommendations() {
        let policy = AutoIndexPolicy::new(0.5);
        let recommendation = IndexRecommendation {
            action: IndexAction::FoldDeltaIntoBase,
            reason: "test".to_string(),
            confidence: 0.2,
        };
        assert!(!policy.is_eligible(&recommendation));
    }

    #[test]
    fn policy_accepts_high_confidence_actionable_recommendations() {
        let policy = AutoIndexPolicy::new(0.5);
        let recommendation = IndexRecommendation {
            action: IndexAction::FoldDeltaIntoBase,
            reason: "test".to_string(),
            confidence: 0.8,
        };
        assert!(policy.is_eligible(&recommendation));
    }

    #[test]
    fn policy_never_treats_no_action_as_eligible() {
        let policy = AutoIndexPolicy::new(0.0);
        let recommendation = IndexRecommendation {
            action: IndexAction::NoActionNeeded,
            reason: "test".to_string(),
            confidence: 1.0,
        };
        assert!(!policy.is_eligible(&recommendation));
    }

    #[test]
    fn planner_prefers_hnsw_only_when_available_and_unfiltered() {
        assert_eq!(IndexPlanner::choose(true, false), QueryPath::Hnsw);
        assert_eq!(IndexPlanner::choose(true, true), QueryPath::ExactScan);
        assert_eq!(IndexPlanner::choose(false, false), QueryPath::ExactScan);
        assert_eq!(IndexPlanner::choose(false, true), QueryPath::ExactScan);
    }
}
