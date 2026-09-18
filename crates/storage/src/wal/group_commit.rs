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

//! WAL group-commit policy, admission evidence, and runtime snapshots.
//!
//! The storage layer owns the durable-write contract. Host runtimes may own the
//! queueing coordinator, but consume these types without redefining policy.
use hawdb_core::{HawDBError, Result};
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

pub const DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES: NonZeroUsize =
    NonZeroUsize::new(16).expect("WAL group commit entry bound is non-zero");
pub const DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES: NonZeroU64 =
    NonZeroU64::new(1024 * 1024).expect("WAL group commit byte bound is non-zero");
pub const DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(250);
const MAX_WAL_GROUP_COMMIT_ENTRIES: usize = 256;
const MAX_WAL_GROUP_COMMIT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_WAL_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(10);
/// A paired median and its dispersion are both unstable over a handful of
/// rounds, so timing evidence must carry enough rounds to be gated on.
const MIN_MEASUREMENT_ROUNDS: usize = 9;
/// Dispersion above this multiple of the regression budget means the rounds do
/// not agree closely enough for the median to decide admission.
const SIGNAL_QUALITY_MAD_BUDGET_MULTIPLE: u64 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalGroupCommitActivation {
    #[default]
    Disabled,
    BenchmarkCandidate,
    EvidenceValidated,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalGroupCommitDelayPolicy {
    #[default]
    Fixed,
    AdaptiveFsync,
}

impl WalGroupCommitDelayPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::AdaptiveFsync => "adaptive_fsync",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalGroupCommitWaitDecision {
    #[default]
    NotEvaluated,
    MaxEntriesBound,
    SingleRequest,
    FixedDelay,
    AdaptiveFallbackDelay,
    BelowUsefulDelay,
    AdaptiveDelay,
}

impl WalGroupCommitWaitDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotEvaluated => "not_evaluated",
            Self::MaxEntriesBound => "max_entries_bound",
            Self::SingleRequest => "single_request",
            Self::FixedDelay => "fixed_delay",
            Self::AdaptiveFallbackDelay => "adaptive_fallback_delay",
            Self::BelowUsefulDelay => "below_useful_delay",
            Self::AdaptiveDelay => "adaptive_delay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitTailLatencyEvidence {
    pub commit_count: usize,
    /// Median candidate-minus-baseline p95 latency across paired rounds.
    pub paired_p95_regression_micros: i64,
    /// Median absolute deviation of paired p95 regressions.
    pub paired_p95_mad_micros: u64,
    pub max_accepted_p95_regression_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitAdaptivePolicyEvidence {
    /// Median adaptive-minus-fixed elapsed time across paired rounds.
    pub paired_elapsed_regression_micros: i64,
    /// Median absolute deviation of paired elapsed-time regressions.
    pub paired_elapsed_mad_micros: u64,
    pub max_accepted_elapsed_regression_micros: u64,
    pub tail_latency: WalGroupCommitTailLatencyEvidence,
}

/// Cold-start evidence is behavioral rather than temporal.
///
/// Before the fsync window holds enough completed samples, the adaptive policy
/// deliberately reuses the bounded fixed fallback delay, so both arms of a
/// cold-start comparison are meant to behave the same. Their elapsed-time
/// difference is therefore variance, not signal, and gating on it rejects
/// admissible candidates. What must be proven instead is that the fallback
/// keeps coalescing alive before any measurement exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitAdaptiveColdStartEvidence {
    pub commit_count: usize,
    /// Smallest per-round count of contended decisions that used the fallback.
    pub min_fallback_delay_count: u64,
    /// Smallest per-round count of coalescing waits.
    pub min_coalescing_wait_count: u64,
    /// Smallest per-round maximum group size.
    pub min_observed_group_entries: usize,
}

/// Steady-state evidence is behavioral, with timing kept only as a coarse
/// guard.
///
/// A host whose measured fsync baseline derives a window close to the fixed
/// default runs two policies that are indistinguishable in principle: the
/// derived and fixed windows differ by tens of microseconds against a tail
/// latency of milliseconds, so paired timing cannot resolve the difference and
/// a tight budget samples noise. What is worth proving is that the derived path
/// is the one being exercised — the baseline exists, the delay comes from it
/// rather than from the fallback, and coalescing still groups. Whether the
/// derived window is better than a hand-tuned constant is a question about the
/// device, answered by `adaptive_delay_matrix_tracks_fast_and_slow_fsync_baselines`
/// over the derivation itself, not by wall-clock rounds on one disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitAdaptiveSteadyStateEvidence {
    pub commit_count: usize,
    /// Largest per-round count of decisions that fell back to the fixed delay.
    /// A warm window must never fall back.
    pub max_fallback_delay_count: u64,
    /// Smallest per-round count of completed fsync samples behind the baseline.
    pub min_fsync_baseline_sample_count: u64,
    /// Smallest per-round count of coalescing waits.
    pub min_coalescing_wait_count: u64,
    /// Smallest per-round maximum group size.
    pub min_observed_group_entries: usize,
    /// Gross-regression guard. Its budget is deliberately wide because it
    /// catches a broken derivation, not a small difference.
    pub safety_net: WalGroupCommitAdaptivePolicyEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitEvidence {
    /// Number of alternating baseline/candidate measurement rounds.
    pub measurement_rounds: usize,
    pub commit_count: usize,
    pub baseline_elapsed_micros: u64,
    pub baseline_fsync_count: u64,
    pub grouped_elapsed_micros: u64,
    pub grouped_fsync_count: u64,
    pub concurrent_tail_latency: WalGroupCommitTailLatencyEvidence,
    pub single_writer_tail_latency: WalGroupCommitTailLatencyEvidence,
    pub single_writer_max_coalescing_wait_count: u64,
    pub single_writer_max_observed_group_entries: usize,
    /// Behavior observed before an fsync baseline exists.
    pub adaptive_cold_start_behavior: Option<WalGroupCommitAdaptiveColdStartEvidence>,
    /// Behavior observed after the fsync window is ready.
    pub adaptive_steady_state_behavior: Option<WalGroupCommitAdaptiveSteadyStateEvidence>,
    pub strict_recovery_verified: bool,
    pub wal_order_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGroupCommitConfig {
    activation: WalGroupCommitActivation,
    delay_policy: WalGroupCommitDelayPolicy,
    /// Exact upper bound on commit requests sharing one durability barrier.
    max_entries: NonZeroUsize,
    /// Coalescing target checked after each unchanged, individually bounded WAL record.
    /// The hard group byte bound is this target plus one configured WAL record.
    max_bytes: NonZeroU64,
    /// Exact upper bound on the initial coalescing wait.
    max_delay: Duration,
}

impl WalGroupCommitConfig {
    pub const fn disabled() -> Self {
        Self {
            activation: WalGroupCommitActivation::Disabled,
            delay_policy: WalGroupCommitDelayPolicy::Fixed,
            max_entries: DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
            max_bytes: DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES,
            max_delay: DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
        }
    }

    /// Enables the candidate path only for collecting benchmark evidence.
    /// Production callers should use `enabled_after_evidence`.
    pub fn benchmark_candidate(
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        Self::with_activation(
            WalGroupCommitActivation::BenchmarkCandidate,
            WalGroupCommitDelayPolicy::Fixed,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    /// Enables the measured-delay candidate only for collecting benchmark evidence.
    pub fn benchmark_adaptive_candidate(
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        Self::with_activation(
            WalGroupCommitActivation::BenchmarkCandidate,
            WalGroupCommitDelayPolicy::AdaptiveFsync,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    pub fn enabled_after_evidence(
        evidence: WalGroupCommitEvidence,
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        validate_evidence(evidence)?;
        Self::with_activation(
            WalGroupCommitActivation::EvidenceValidated,
            WalGroupCommitDelayPolicy::Fixed,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    pub fn adaptive_enabled_after_evidence(
        evidence: WalGroupCommitEvidence,
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        validate_evidence(evidence)?;
        validate_adaptive_cold_start_evidence(evidence.adaptive_cold_start_behavior)?;
        validate_adaptive_steady_state_evidence(evidence.adaptive_steady_state_behavior)?;
        Self::with_activation(
            WalGroupCommitActivation::EvidenceValidated,
            WalGroupCommitDelayPolicy::AdaptiveFsync,
            max_entries,
            max_bytes,
            max_delay,
        )
    }

    fn with_activation(
        activation: WalGroupCommitActivation,
        delay_policy: WalGroupCommitDelayPolicy,
        max_entries: NonZeroUsize,
        max_bytes: NonZeroU64,
        max_delay: Duration,
    ) -> Result<Self> {
        if max_entries.get() > MAX_WAL_GROUP_COMMIT_ENTRIES {
            return Err(HawDBError::Execution(format!(
                "WAL group commit max_entries must be <= {MAX_WAL_GROUP_COMMIT_ENTRIES}"
            )));
        }
        if max_bytes.get() > MAX_WAL_GROUP_COMMIT_BYTES {
            return Err(HawDBError::Execution(format!(
                "WAL group commit max_bytes must be <= {MAX_WAL_GROUP_COMMIT_BYTES}"
            )));
        }
        if max_delay > MAX_WAL_GROUP_COMMIT_DELAY {
            return Err(HawDBError::Execution(format!(
                "WAL group commit max_delay must be <= {} ms",
                MAX_WAL_GROUP_COMMIT_DELAY.as_millis()
            )));
        }
        Ok(Self {
            activation,
            delay_policy,
            max_entries,
            max_bytes,
            max_delay,
        })
    }

    pub const fn activation(self) -> WalGroupCommitActivation {
        self.activation
    }

    pub const fn is_enabled(self) -> bool {
        !matches!(self.activation, WalGroupCommitActivation::Disabled)
    }

    pub const fn delay_policy(self) -> WalGroupCommitDelayPolicy {
        self.delay_policy
    }

    pub const fn max_entries(self) -> NonZeroUsize {
        self.max_entries
    }

    pub const fn max_bytes(self) -> NonZeroU64 {
        self.max_bytes
    }

    pub const fn max_delay(self) -> Duration {
        self.max_delay
    }
}

impl Default for WalGroupCommitConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalGroupCommitSnapshot {
    pub activation: WalGroupCommitActivation,
    pub delay_policy: WalGroupCommitDelayPolicy,
    pub last_wait_decision: WalGroupCommitWaitDecision,
    pub effective_delay_micros: u64,
    pub max_observed_effective_delay_micros: u64,
    pub fsync_baseline_micros: Option<u64>,
    pub fsync_baseline_sample_count: u64,
    /// Number of contended decisions that used the bounded fixed-delay fallback.
    pub adaptive_fallback_count: u64,
    pub adaptive_delay_clamp_count: u64,
    pub submitted_commits: u64,
    pub completed_commits: u64,
    pub group_count: u64,
    pub coalescing_wait_count: u64,
    pub shared_sync_count: u64,
    pub grouped_wal_entries: u64,
    pub grouped_wal_bytes: u64,
    pub max_observed_group_entries: usize,
    pub max_observed_group_bytes: u64,
    pub total_fsync_micros: u64,
}

fn validate_evidence(evidence: WalGroupCommitEvidence) -> Result<()> {
    let mut blockers = Vec::new();
    if evidence.measurement_rounds < MIN_MEASUREMENT_ROUNDS {
        blockers.push("insufficient_measurement_rounds");
    }
    if evidence.commit_count < 2 {
        blockers.push("insufficient_commit_count");
    }
    if evidence.grouped_fsync_count >= evidence.baseline_fsync_count
        || evidence.grouped_fsync_count >= evidence.commit_count as u64
    {
        blockers.push("fsync_reduction_not_proven");
    }
    if evidence.grouped_elapsed_micros >= evidence.baseline_elapsed_micros {
        blockers.push("throughput_improvement_not_proven");
    }
    if evidence.concurrent_tail_latency.commit_count < 2 {
        blockers.push("concurrent_tail_latency_evidence_missing");
    }
    push_tail_latency_blocker(
        &mut blockers,
        evidence.concurrent_tail_latency,
        "tail_latency_budget_exceeded",
        "tail_latency_insufficient_signal_quality",
    );
    if evidence.single_writer_tail_latency.commit_count == 0 {
        blockers.push("single_writer_evidence_missing");
    }
    push_tail_latency_blocker(
        &mut blockers,
        evidence.single_writer_tail_latency,
        "single_writer_tail_latency_budget_exceeded",
        "single_writer_tail_latency_insufficient_signal_quality",
    );
    if evidence.single_writer_max_coalescing_wait_count > 0 {
        blockers.push("single_writer_coalescing_wait_observed");
    }
    if evidence.single_writer_max_observed_group_entries > 1 {
        blockers.push("single_writer_grouping_observed");
    }
    if !evidence.strict_recovery_verified {
        blockers.push("strict_recovery_not_verified");
    }
    if !evidence.wal_order_verified {
        blockers.push("wal_order_not_verified");
    }
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "WAL group commit evidence rejected: {}",
            blockers.join(",")
        )))
    }
}

fn validate_adaptive_cold_start_evidence(
    evidence: Option<WalGroupCommitAdaptiveColdStartEvidence>,
) -> Result<()> {
    let Some(evidence) = evidence else {
        return Err(HawDBError::Execution(
            "WAL group commit adaptive evidence rejected: cold_start_behavior_missing".to_string(),
        ));
    };
    let mut blockers = Vec::new();
    if evidence.commit_count == 0 {
        blockers.push("cold_start_evidence_missing");
    }
    if evidence.min_fallback_delay_count == 0 {
        blockers.push("cold_start_fallback_not_exercised");
    }
    if evidence.min_coalescing_wait_count == 0 {
        blockers.push("cold_start_coalescing_disabled");
    }
    if evidence.min_observed_group_entries < 2 {
        blockers.push("cold_start_grouping_not_observed");
    }
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "WAL group commit adaptive evidence rejected: {}",
            blockers.join(",")
        )))
    }
}

fn validate_adaptive_steady_state_evidence(
    evidence: Option<WalGroupCommitAdaptiveSteadyStateEvidence>,
) -> Result<()> {
    let Some(evidence) = evidence else {
        return Err(HawDBError::Execution(
            "WAL group commit adaptive evidence rejected: steady_state_behavior_missing"
                .to_string(),
        ));
    };
    let mut blockers = Vec::new();
    if evidence.commit_count == 0 {
        blockers.push("steady_state_evidence_missing");
    }
    if evidence.min_fsync_baseline_sample_count == 0 {
        blockers.push("steady_state_baseline_not_established");
    }
    if evidence.max_fallback_delay_count > 0 {
        blockers.push("steady_state_fell_back_to_fixed_delay");
    }
    if evidence.min_coalescing_wait_count == 0 {
        blockers.push("steady_state_coalescing_disabled");
    }
    if evidence.min_observed_group_entries < 2 {
        blockers.push("steady_state_grouping_not_observed");
    }
    if !blockers.is_empty() {
        return Err(HawDBError::Execution(format!(
            "WAL group commit adaptive evidence rejected: {}",
            blockers.join(",")
        )));
    }
    validate_adaptive_policy_evidence("steady_state", Some(evidence.safety_net))
}

fn validate_adaptive_policy_evidence(
    shape: &'static str,
    evidence: Option<WalGroupCommitAdaptivePolicyEvidence>,
) -> Result<()> {
    let Some(evidence) = evidence else {
        return Err(HawDBError::Execution(format!(
            "WAL group commit adaptive evidence rejected: {shape}_fixed_policy_comparison_missing"
        )));
    };
    let mut blockers = Vec::new();
    if evidence.tail_latency.commit_count == 0 {
        blockers.push(format!("{shape}_adaptive_policy_evidence_missing"));
    }
    match assess_regression(
        evidence.paired_elapsed_regression_micros,
        evidence.paired_elapsed_mad_micros,
        evidence.max_accepted_elapsed_regression_micros,
    ) {
        RegressionAssessment::WithinBudget => {}
        RegressionAssessment::BudgetExceeded => {
            blockers.push(format!("{shape}_adaptive_policy_elapsed_budget_exceeded"));
        }
        RegressionAssessment::InsufficientSignalQuality => {
            blockers.push(format!(
                "{shape}_adaptive_policy_elapsed_insufficient_signal_quality"
            ));
        }
    }
    match assess_tail_latency(evidence.tail_latency) {
        RegressionAssessment::WithinBudget => {}
        RegressionAssessment::BudgetExceeded => {
            blockers.push(format!(
                "{shape}_adaptive_policy_tail_latency_budget_exceeded"
            ));
        }
        RegressionAssessment::InsufficientSignalQuality => {
            blockers.push(format!(
                "{shape}_adaptive_policy_tail_latency_insufficient_signal_quality"
            ));
        }
    }
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "WAL group commit adaptive evidence rejected: {}",
            blockers.join(",")
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegressionAssessment {
    WithinBudget,
    BudgetExceeded,
    InsufficientSignalQuality,
}

fn assess_tail_latency(evidence: WalGroupCommitTailLatencyEvidence) -> RegressionAssessment {
    assess_regression(
        evidence.paired_p95_regression_micros,
        evidence.paired_p95_mad_micros,
        evidence.max_accepted_p95_regression_micros,
    )
}

fn assess_regression(median: i64, mad: u64, budget: u64) -> RegressionAssessment {
    // A candidate whose paired median is at least as fast as the baseline is
    // accepted without a dispersion bound. Requiring tight rounds to accept a
    // measured improvement rejects good candidates on noise alone.
    if median <= 0 {
        return RegressionAssessment::WithinBudget;
    }
    let median = i128::from(median);
    let budget = i128::from(budget);
    if median > budget {
        return RegressionAssessment::BudgetExceeded;
    }
    // Dispersion decides whether the median carries signal; it is not a penalty
    // added to the median. Adding it compares the upper edge of the spread with
    // a budget that was calibrated for the central estimate.
    if i128::from(mad) > budget.saturating_mul(i128::from(SIGNAL_QUALITY_MAD_BUDGET_MULTIPLE)) {
        return RegressionAssessment::InsufficientSignalQuality;
    }
    RegressionAssessment::WithinBudget
}

fn push_tail_latency_blocker(
    blockers: &mut Vec<&'static str>,
    evidence: WalGroupCommitTailLatencyEvidence,
    budget_blocker: &'static str,
    signal_blocker: &'static str,
) {
    match assess_tail_latency(evidence) {
        RegressionAssessment::WithinBudget => {}
        RegressionAssessment::BudgetExceeded => blockers.push(budget_blocker),
        RegressionAssessment::InsufficientSignalQuality => blockers.push(signal_blocker),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assess_regression, RegressionAssessment, WalGroupCommitConfig,
        DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    };
    use std::num::NonZeroUsize;

    #[test]
    fn policy_rejects_runtime_bounds_before_admission() {
        let error = WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(257).unwrap(),
            DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES,
            DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "execution error: WAL group commit max_entries must be <= 256"
        );
    }

    #[test]
    fn regression_assessment_accepts_improvements_and_rejects_noise() {
        assert_eq!(
            assess_regression(-1, u64::MAX, 0),
            RegressionAssessment::WithinBudget
        );
        assert_eq!(
            assess_regression(101, 0, 100),
            RegressionAssessment::BudgetExceeded
        );
        assert_eq!(
            assess_regression(100, 201, 100),
            RegressionAssessment::InsufficientSignalQuality
        );
    }
}
