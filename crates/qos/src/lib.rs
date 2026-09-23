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

#![deny(unsafe_code)]

mod device;
mod process_memory;
mod resource;
mod runtime;

use hawdb_core::time::Instant;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

pub use device::{StorageDeviceDiscoverySource, StorageDeviceProfile, StorageMediaKind};
pub use hawdb_core::{
    RuntimeCancellationReason, RuntimeCancellationToken, RuntimeIoWaveController,
    RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire, RuntimeTaskContext,
};
pub use process_memory::{
    ProcessMemoryCapabilities, ProcessMemoryPolicy, ProcessMemoryPolicyConfig,
    ProcessMemoryPolicySnapshot, ProcessMemoryProfile, ProcessMemorySnapshot,
};
pub use resource::{
    IoConcurrencyBudget, RuntimeMemoryPressure, RuntimeMemorySnapshot, RuntimeResourceBudget,
    RuntimeResourceSnapshot,
};
pub use runtime::{
    RuntimeAdmissionCode, RuntimeAdmissionError, RuntimeAdmissionWaiter, RuntimeGovernor,
    RuntimeGovernorConfig, RuntimeGovernorLimits, RuntimeGovernorSnapshot,
    RuntimeIoReservationScope, RuntimePermit, RuntimeTelemetryEvent, RuntimeTelemetryEventKind,
    RuntimeTelemetrySink, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkPriority {
    Foreground,
    Background,
}

// Keep every class-indexed array exhaustive by generating it with the enum.
macro_rules! define_work_classes {
    ($($class:ident => $name:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum WorkClass {
            $($class),+
        }

        impl WorkClass {
            pub const ALL: [Self; [$($name),+].len()] = [$(Self::$class),+];

            pub const fn as_index(self) -> usize {
                self as usize
            }

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$class => $name),+
                }
            }
        }

        impl FromStr for WorkClass {
            type Err = &'static str;

            fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
                match value {
                    $($name => Ok(Self::$class)),+,
                    _ => Err("unknown work class"),
                }
            }
        }
    };
}

define_work_classes! {
    Query => "query",
    Mutation => "mutation",
    Projection => "projection",
    Import => "import",
    Analytics => "analytics",
    Shadow => "shadow",
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosTelemetryPhase {
    Admission,
    Completion,
}

impl QosTelemetryPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Completion => "completion",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosTelemetryOutcome {
    Admitted,
    Deferred,
    Rejected,
    Completed,
    Failed,
}

impl QosTelemetryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Deferred => "deferred",
            Self::Rejected => "rejected",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosTelemetryEvent {
    pub phase: QosTelemetryPhase,
    pub outcome: QosTelemetryOutcome,
    pub class: WorkClass,
    pub estimated_operations: usize,
    pub elapsed_micros: u64,
    pub admission_code: Option<QosAdmissionCode>,
}

pub trait QosTelemetrySink: Debug + Send + Sync {
    fn record_qos(&self, event: QosTelemetryEvent);
}

impl WorkPriority {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkPriority::Foreground => "foreground",
            WorkPriority::Background => "background",
        }
    }
}

impl FromStr for WorkPriority {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "foreground" => Ok(WorkPriority::Foreground),
            "background" => Ok(WorkPriority::Background),
            _ => Err("unknown work priority"),
        }
    }
}

pub const WORK_CLASS_COUNT: usize = WorkClass::ALL.len();

const _: () = {
    let mut index = 0;
    while index < WORK_CLASS_COUNT {
        assert!(WorkClass::ALL[index].as_index() == index);
        index += 1;
    }
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkRequest {
    pub class: WorkClass,
    pub priority: WorkPriority,
    pub estimated_operations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundWorkHint {
    pub active_topic: bool,
    pub recent_delta_operations: usize,
    pub source_graph_commit_lag: u64,
    pub query_probability_per_million: u32,
    pub staleness_millis: u64,
    pub staleness_ttl_millis: Option<u64>,
    pub freshness_slo_millis: Option<u64>,
    pub tenant_budget_remaining_operations: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundWorkPlan {
    pub request: WorkRequest,
    pub hint: BackgroundWorkHint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundWorkDecision {
    pub admission: QosAdmission,
    pub score: u64,
    pub reason_codes: Vec<BackgroundWorkReasonCode>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedBackgroundWork {
    pub index: usize,
    pub decision: BackgroundWorkDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalQosSnapshot {
    pub ready: bool,
    pub foreground_admitted: bool,
    pub background_enabled: bool,
    pub background_bounded: bool,
    pub running_background_operations: usize,
    pub max_total_background_operations: Option<usize>,
    pub remaining_total_background_operations: Option<usize>,
    pub total_background_over_budget: bool,
    pub class_snapshots: [LocalQosClassSnapshot; WORK_CLASS_COUNT],
    pub blocker_codes: Vec<QosSnapshotBlockerCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalQosClassSnapshot {
    pub class: WorkClass,
    pub running_background_operations: usize,
    pub max_background_operations: Option<usize>,
    pub remaining_background_operations: Option<usize>,
    pub over_budget: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosSnapshotBlockerCode {
    ForegroundAdmissionBlocked,
    BackgroundDisabled,
    BackgroundUnbounded,
    TotalBackgroundOverBudget,
    ClassBackgroundOverBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundWorkReasonCode {
    ForegroundNotRanked,
    TenantBudgetBelowEstimate,
    ActiveTopic,
    QueryProbability,
    RecentDeltaOperations,
    SourceGraphCommitLag,
    StalenessTtl,
    FreshnessSlo,
    AdmissionDeferred,
    AdmissionRejected,
}

impl BackgroundWorkReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            BackgroundWorkReasonCode::ForegroundNotRanked => "foreground_not_ranked",
            BackgroundWorkReasonCode::TenantBudgetBelowEstimate => "tenant_budget_below_estimate",
            BackgroundWorkReasonCode::ActiveTopic => "active_topic",
            BackgroundWorkReasonCode::QueryProbability => "query_probability",
            BackgroundWorkReasonCode::RecentDeltaOperations => "recent_delta_operations",
            BackgroundWorkReasonCode::SourceGraphCommitLag => "source_graph_commit_lag",
            BackgroundWorkReasonCode::StalenessTtl => "staleness_ttl",
            BackgroundWorkReasonCode::FreshnessSlo => "freshness_slo",
            BackgroundWorkReasonCode::AdmissionDeferred => "admission_deferred",
            BackgroundWorkReasonCode::AdmissionRejected => "admission_rejected",
        }
    }
}

impl QosSnapshotBlockerCode {
    pub fn as_str(self) -> &'static str {
        match self {
            QosSnapshotBlockerCode::ForegroundAdmissionBlocked => "foreground_admission_blocked",
            QosSnapshotBlockerCode::BackgroundDisabled => "background_disabled",
            QosSnapshotBlockerCode::BackgroundUnbounded => "background_unbounded",
            QosSnapshotBlockerCode::TotalBackgroundOverBudget => "total_background_over_budget",
            QosSnapshotBlockerCode::ClassBackgroundOverBudget => "class_background_over_budget",
        }
    }
}

impl FromStr for BackgroundWorkReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "foreground_not_ranked" => Ok(BackgroundWorkReasonCode::ForegroundNotRanked),
            "tenant_budget_below_estimate" => {
                Ok(BackgroundWorkReasonCode::TenantBudgetBelowEstimate)
            }
            "active_topic" => Ok(BackgroundWorkReasonCode::ActiveTopic),
            "query_probability" => Ok(BackgroundWorkReasonCode::QueryProbability),
            "recent_delta_operations" => Ok(BackgroundWorkReasonCode::RecentDeltaOperations),
            "source_graph_commit_lag" => Ok(BackgroundWorkReasonCode::SourceGraphCommitLag),
            "staleness_ttl" => Ok(BackgroundWorkReasonCode::StalenessTtl),
            "freshness_slo" => Ok(BackgroundWorkReasonCode::FreshnessSlo),
            "admission_deferred" => Ok(BackgroundWorkReasonCode::AdmissionDeferred),
            "admission_rejected" => Ok(BackgroundWorkReasonCode::AdmissionRejected),
            _ => Err("unknown background work reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalQosPolicy {
    pub max_background_operations: Option<usize>,
    pub max_total_background_operations: Option<usize>,
    pub max_background_operations_by_class: [Option<usize>; WORK_CLASS_COUNT],
    pub background_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalQosState {
    pub running_background_operations: usize,
    pub running_background_operations_by_class: [usize; WORK_CLASS_COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QosAdmission {
    Admit,
    Defer {
        code: QosAdmissionCode,
        reason: String,
    },
    Reject {
        code: QosAdmissionCode,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosAdmissionCode {
    BackgroundDisabled,
    PerWorkLimitExceeded,
    TotalBackgroundLimitExceeded,
    ClassBackgroundLimitExceeded,
    TenantBudgetExceeded,
}

impl QosAdmissionCode {
    pub fn as_str(self) -> &'static str {
        match self {
            QosAdmissionCode::BackgroundDisabled => "background_disabled",
            QosAdmissionCode::PerWorkLimitExceeded => "per_work_limit_exceeded",
            QosAdmissionCode::TotalBackgroundLimitExceeded => "total_background_limit_exceeded",
            QosAdmissionCode::ClassBackgroundLimitExceeded => "class_background_limit_exceeded",
            QosAdmissionCode::TenantBudgetExceeded => "tenant_budget_exceeded",
        }
    }
}

impl FromStr for QosAdmissionCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "background_disabled" => Ok(QosAdmissionCode::BackgroundDisabled),
            "per_work_limit_exceeded" => Ok(QosAdmissionCode::PerWorkLimitExceeded),
            "total_background_limit_exceeded" => Ok(QosAdmissionCode::TotalBackgroundLimitExceeded),
            "class_background_limit_exceeded" => Ok(QosAdmissionCode::ClassBackgroundLimitExceeded),
            "tenant_budget_exceeded" => Ok(QosAdmissionCode::TenantBudgetExceeded),
            _ => Err("unknown qos admission code"),
        }
    }
}

#[derive(Debug)]
pub struct LocalQosPermit {
    scheduler: Arc<LocalQosSchedulerInner>,
    request: WorkRequest,
    started_at: Instant,
    released: bool,
}

#[derive(Debug, Clone)]
pub struct LocalQosScheduler {
    inner: Arc<LocalQosSchedulerInner>,
}

#[derive(Debug)]
struct LocalQosSchedulerInner {
    policy: LocalQosPolicy,
    state: Mutex<LocalQosState>,
    telemetry: RwLock<Option<Arc<dyn QosTelemetrySink>>>,
}

impl PartialEq for LocalQosPermit {
    fn eq(&self, other: &Self) -> bool {
        self.request == other.request
    }
}

impl Eq for LocalQosPermit {}

impl PartialEq for LocalQosScheduler {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
            || (self.policy() == other.policy() && self.state() == other.state())
    }
}

impl Eq for LocalQosScheduler {}

impl Default for LocalQosPolicy {
    fn default() -> Self {
        Self {
            max_background_operations: Some(1024),
            max_total_background_operations: Some(4096),
            max_background_operations_by_class: [None; WORK_CLASS_COUNT],
            background_enabled: true,
        }
    }
}

impl WorkRequest {
    pub fn foreground(class: WorkClass, estimated_operations: usize) -> Self {
        Self {
            class,
            priority: WorkPriority::Foreground,
            estimated_operations,
        }
    }

    pub fn background(class: WorkClass, estimated_operations: usize) -> Self {
        Self {
            class,
            priority: WorkPriority::Background,
            estimated_operations,
        }
    }
}

impl BackgroundWorkHint {
    /// Score this hint for the same estimate used by background admission.
    /// A tenant budget below the estimate produces zero, even for active topics.
    pub fn expected_value_score(&self, estimated_operations: usize) -> u64 {
        self.score_with_reasons(estimated_operations).0
    }

    fn score_with_reasons(
        &self,
        estimated_operations: usize,
    ) -> (u64, Vec<BackgroundWorkReasonCode>, Vec<String>) {
        let mut score = 0u64;
        let mut reason_codes = Vec::new();
        let mut reasons = Vec::new();

        if let Some(remaining) = self.tenant_budget_remaining_operations
            && remaining < estimated_operations
        {
            reason_codes.push(BackgroundWorkReasonCode::TenantBudgetBelowEstimate);
            reasons.push(format!(
                "tenant budget remaining {remaining} below estimated operations {estimated_operations}"
            ));
            return (0, reason_codes, reasons);
        }

        if self.active_topic {
            score = score.saturating_add(1_000_000);
            reason_codes.push(BackgroundWorkReasonCode::ActiveTopic);
            reasons.push("active topic".to_string());
        }

        let query_probability = u64::from(self.query_probability_per_million).min(1_000_000);
        if query_probability > 0 {
            score = score.saturating_add(query_probability);
            reason_codes.push(BackgroundWorkReasonCode::QueryProbability);
            reasons.push(format!("query probability {query_probability} per million"));
        }

        let recent_delta_score = (self.recent_delta_operations as u64).min(1_000_000);
        if recent_delta_score > 0 {
            score = score.saturating_add(recent_delta_score);
            reason_codes.push(BackgroundWorkReasonCode::RecentDeltaOperations);
            reasons.push(format!(
                "recent delta operations {}",
                self.recent_delta_operations
            ));
        }

        let source_graph_commit_lag_score = self.source_graph_commit_lag.min(1_000_000);
        if source_graph_commit_lag_score > 0 {
            score = score.saturating_add(source_graph_commit_lag_score);
            reason_codes.push(BackgroundWorkReasonCode::SourceGraphCommitLag);
            reasons.push(format!(
                "source graph commit lag {}",
                self.source_graph_commit_lag
            ));
        }

        if let Some(ttl) = self.staleness_ttl_millis {
            let staleness_score = scaled_staleness_score(self.staleness_millis, ttl);
            if staleness_score > 0 {
                score = score.saturating_add(staleness_score);
                reason_codes.push(BackgroundWorkReasonCode::StalenessTtl);
                if self.staleness_millis >= ttl {
                    reasons.push(format!(
                        "staleness ttl reached at {} ms",
                        self.staleness_millis
                    ));
                } else {
                    reasons.push(format!(
                        "staleness {} of ttl {} ms",
                        self.staleness_millis, ttl
                    ));
                }
            }
        }

        if let Some(slo) = self.freshness_slo_millis {
            let freshness_score = scaled_staleness_score(self.staleness_millis, slo);
            if freshness_score > 0 {
                score = score.saturating_add(freshness_score);
                reason_codes.push(BackgroundWorkReasonCode::FreshnessSlo);
                if self.staleness_millis >= slo {
                    reasons.push(format!(
                        "freshness slo missed at {} ms",
                        self.staleness_millis
                    ));
                } else {
                    reasons.push(format!(
                        "freshness age {} of slo {} ms",
                        self.staleness_millis, slo
                    ));
                }
            }
        }

        (score, reason_codes, reasons)
    }
}

impl BackgroundWorkPlan {
    pub fn background(
        class: WorkClass,
        estimated_operations: usize,
        hint: BackgroundWorkHint,
    ) -> Self {
        Self {
            request: WorkRequest::background(class, estimated_operations),
            hint,
        }
    }
}

impl LocalQosPolicy {
    pub fn snapshot(&self, state: &LocalQosState) -> LocalQosSnapshot {
        let foreground_admission = self.foreground_admission();
        let foreground_admitted = matches!(foreground_admission, QosAdmission::Admit);
        let background_bounded = self.max_background_operations.is_some()
            || self.max_total_background_operations.is_some()
            || self
                .max_background_operations_by_class
                .iter()
                .any(Option::is_some);
        let total_background_over_budget = self
            .max_total_background_operations
            .is_some_and(|limit| state.running_background_operations > limit);
        let class_snapshots = local_qos_class_snapshots(self, state);
        let class_background_over_budget =
            class_snapshots.iter().any(|snapshot| snapshot.over_budget);
        let mut blocker_codes = Vec::new();
        if !foreground_admitted {
            blocker_codes.push(QosSnapshotBlockerCode::ForegroundAdmissionBlocked);
        }
        if !self.background_enabled {
            blocker_codes.push(QosSnapshotBlockerCode::BackgroundDisabled);
        }
        if !background_bounded {
            blocker_codes.push(QosSnapshotBlockerCode::BackgroundUnbounded);
        }
        if total_background_over_budget {
            blocker_codes.push(QosSnapshotBlockerCode::TotalBackgroundOverBudget);
        }
        if class_background_over_budget {
            blocker_codes.push(QosSnapshotBlockerCode::ClassBackgroundOverBudget);
        }
        LocalQosSnapshot {
            ready: blocker_codes.is_empty(),
            foreground_admitted,
            background_enabled: self.background_enabled,
            background_bounded,
            running_background_operations: state.running_background_operations,
            max_total_background_operations: self.max_total_background_operations,
            remaining_total_background_operations: self
                .max_total_background_operations
                .map(|limit| limit.saturating_sub(state.running_background_operations)),
            total_background_over_budget,
            class_snapshots,
            blocker_codes,
        }
    }

    /// Foreground work bypasses this local background budget policy.
    /// Runtime resource admission is enforced separately by the governor.
    pub fn foreground_admission(&self) -> QosAdmission {
        QosAdmission::Admit
    }

    pub fn admit(&self, state: &LocalQosState, request: &WorkRequest) -> QosAdmission {
        match request.priority {
            WorkPriority::Foreground => self.foreground_admission(),
            WorkPriority::Background => self.admit_background(state, request),
        }
    }

    pub fn evaluate_background_work(
        &self,
        state: &LocalQosState,
        plan: &BackgroundWorkPlan,
    ) -> BackgroundWorkDecision {
        let policy_admission = self.admit(state, &plan.request);
        if plan.request.priority != WorkPriority::Background {
            return BackgroundWorkDecision {
                admission: policy_admission,
                score: 0,
                reason_codes: vec![BackgroundWorkReasonCode::ForegroundNotRanked],
                reasons: vec!["foreground work is not background-ranked".to_string()],
            };
        }

        let admission = match (
            policy_admission,
            plan.hint
                .tenant_budget_defer_reason(plan.request.estimated_operations),
        ) {
            (QosAdmission::Admit, Some(reason)) => QosAdmission::Defer {
                code: QosAdmissionCode::TenantBudgetExceeded,
                reason,
            },
            (admission, _) => admission,
        };
        let (score, mut reason_codes, mut reasons) = plan
            .hint
            .score_with_reasons(plan.request.estimated_operations);
        match &admission {
            QosAdmission::Admit => {}
            QosAdmission::Defer { reason, .. } => {
                reason_codes.push(BackgroundWorkReasonCode::AdmissionDeferred);
                reasons.push(format!("admission deferred: {reason}"));
            }
            QosAdmission::Reject { reason, .. } => {
                reason_codes.push(BackgroundWorkReasonCode::AdmissionRejected);
                reasons.push(format!("admission rejected: {reason}"));
            }
        }

        BackgroundWorkDecision {
            admission,
            score,
            reason_codes,
            reasons,
        }
    }

    pub fn rank_background_work(
        &self,
        state: &LocalQosState,
        plans: &[BackgroundWorkPlan],
    ) -> Vec<RankedBackgroundWork> {
        let mut ranked = plans
            .iter()
            .enumerate()
            .map(|(index, plan)| RankedBackgroundWork {
                index,
                decision: self.evaluate_background_work(state, plan),
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            admission_rank(&left.decision.admission)
                .cmp(&admission_rank(&right.decision.admission))
                .then_with(|| right.decision.score.cmp(&left.decision.score))
                .then_with(|| left.index.cmp(&right.index))
        });
        ranked
    }

    fn admit_background(&self, state: &LocalQosState, request: &WorkRequest) -> QosAdmission {
        if !self.background_enabled {
            return QosAdmission::Defer {
                code: QosAdmissionCode::BackgroundDisabled,
                reason: "background work is disabled".to_string(),
            };
        }
        if let Some(limit) = self.max_background_operations
            && request.estimated_operations > limit
        {
            return QosAdmission::Defer {
                code: QosAdmissionCode::PerWorkLimitExceeded,
                reason: format!(
                    "background {:?} estimated operations {} exceeded per-work limit {limit}",
                    request.class, request.estimated_operations
                ),
            };
        }
        if let Some(limit) = self.max_total_background_operations {
            let total = state
                .running_background_operations
                .saturating_add(request.estimated_operations);
            if total > limit {
                return QosAdmission::Defer {
                    code: QosAdmissionCode::TotalBackgroundLimitExceeded,
                    reason: format!(
                        "background {:?} would raise running operations to {total}, above limit {limit}",
                        request.class
                    ),
                };
            }
        }
        if let Some(limit) = self.max_background_operations_by_class[request.class.as_index()] {
            let class_total = state.running_background_operations_by_class
                [request.class.as_index()]
            .saturating_add(request.estimated_operations);
            if class_total > limit {
                return QosAdmission::Defer {
                    code: QosAdmissionCode::ClassBackgroundLimitExceeded,
                    reason: format!(
                        "background {:?} would raise class running operations to {class_total}, above class limit {limit}",
                        request.class
                    ),
                };
            }
        }
        QosAdmission::Admit
    }
}

impl BackgroundWorkHint {
    fn tenant_budget_defer_reason(&self, estimated_operations: usize) -> Option<String> {
        self.tenant_budget_remaining_operations
            .filter(|remaining| *remaining < estimated_operations)
            .map(|remaining| {
                format!(
                    "tenant budget remaining {remaining} below estimated operations {estimated_operations}"
                )
            })
    }
}

impl QosAdmission {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Admit => "admit",
            Self::Defer { .. } => "defer",
            Self::Reject { .. } => "reject",
        }
    }

    pub fn code(&self) -> Option<QosAdmissionCode> {
        match self {
            QosAdmission::Admit => None,
            QosAdmission::Defer { code, .. } | QosAdmission::Reject { code, .. } => Some(*code),
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            QosAdmission::Admit => None,
            QosAdmission::Defer { reason, .. } | QosAdmission::Reject { reason, .. } => {
                Some(reason)
            }
        }
    }
}

fn admission_rank(admission: &QosAdmission) -> u8 {
    match admission {
        QosAdmission::Admit => 0,
        QosAdmission::Defer { .. } => 1,
        QosAdmission::Reject { .. } => 2,
    }
}

fn scaled_staleness_score(age_millis: u64, limit_millis: u64) -> u64 {
    if age_millis == 0 || limit_millis == 0 {
        return 0;
    }
    if age_millis >= limit_millis {
        return 1_000_000;
    }
    age_millis.saturating_mul(1_000_000) / limit_millis
}

impl LocalQosPermit {
    pub fn request(&self) -> &WorkRequest {
        &self.request
    }

    pub fn finish(mut self) {
        self.release(true);
    }

    pub fn finish_with_outcome(mut self, success: bool) {
        self.release(success);
    }

    fn release(&mut self, success: bool) {
        if self.released {
            return;
        }
        self.released = true;
        self.scheduler
            .release(&self.request, self.started_at, success);
    }
}

impl Drop for LocalQosPermit {
    fn drop(&mut self) {
        self.release(false);
    }
}

impl LocalQosScheduler {
    pub fn new(policy: LocalQosPolicy) -> Self {
        Self {
            inner: Arc::new(LocalQosSchedulerInner {
                policy,
                state: Mutex::new(LocalQosState::default()),
                telemetry: RwLock::new(None),
            }),
        }
    }

    pub fn set_telemetry_sink(&self, telemetry: Option<Arc<dyn QosTelemetrySink>>) {
        *self
            .inner
            .telemetry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = telemetry;
    }

    pub fn policy(&self) -> &LocalQosPolicy {
        &self.inner.policy
    }

    pub fn state(&self) -> LocalQosState {
        self.inner.state()
    }

    pub fn admit(&self, request: &WorkRequest) -> QosAdmission {
        self.inner.policy.admit(&self.inner.state(), request)
    }

    pub fn evaluate_background_work(&self, plan: &BackgroundWorkPlan) -> BackgroundWorkDecision {
        self.inner
            .policy
            .evaluate_background_work(&self.inner.state(), plan)
    }

    pub fn rank_background_work(&self, plans: &[BackgroundWorkPlan]) -> Vec<RankedBackgroundWork> {
        self.inner
            .policy
            .rank_background_work(&self.inner.state(), plans)
    }

    pub fn snapshot(&self) -> LocalQosSnapshot {
        self.inner.policy.snapshot(&self.inner.state())
    }

    pub fn try_start(
        &self,
        request: WorkRequest,
    ) -> std::result::Result<LocalQosPermit, QosAdmission> {
        let admission = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let admission = self.inner.policy.admit(&state, &request);
            if matches!(admission, QosAdmission::Admit)
                && request.priority == WorkPriority::Background
            {
                state.running_background_operations = state
                    .running_background_operations
                    .saturating_add(request.estimated_operations);
                let class_index = request.class.as_index();
                state.running_background_operations_by_class[class_index] = state
                    .running_background_operations_by_class[class_index]
                    .saturating_add(request.estimated_operations);
            }
            admission
        };
        match admission {
            QosAdmission::Admit => {
                self.inner.record_background_event(
                    &request,
                    QosTelemetryPhase::Admission,
                    QosTelemetryOutcome::Admitted,
                    None,
                    0,
                );
                Ok(LocalQosPermit {
                    scheduler: Arc::clone(&self.inner),
                    request,
                    started_at: Instant::now(),
                    released: false,
                })
            }
            admission => {
                let outcome = match admission {
                    QosAdmission::Admit => unreachable!("admitted work returns a permit"),
                    QosAdmission::Defer { .. } => QosTelemetryOutcome::Deferred,
                    QosAdmission::Reject { .. } => QosTelemetryOutcome::Rejected,
                };
                self.inner.record_background_event(
                    &request,
                    QosTelemetryPhase::Admission,
                    outcome,
                    admission.code(),
                    0,
                );
                Err(admission)
            }
        }
    }
}

impl LocalQosSchedulerInner {
    fn state(&self) -> LocalQosState {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn release(&self, request: &WorkRequest, started_at: Instant, success: bool) {
        if request.priority == WorkPriority::Background {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.running_background_operations = state
                .running_background_operations
                .saturating_sub(request.estimated_operations);
            let class_index = request.class.as_index();
            state.running_background_operations_by_class[class_index] = state
                .running_background_operations_by_class[class_index]
                .saturating_sub(request.estimated_operations);
        }
        let elapsed_micros = started_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.record_background_event(
            request,
            QosTelemetryPhase::Completion,
            if success {
                QosTelemetryOutcome::Completed
            } else {
                QosTelemetryOutcome::Failed
            },
            None,
            elapsed_micros,
        );
    }

    fn record_background_event(
        &self,
        request: &WorkRequest,
        phase: QosTelemetryPhase,
        outcome: QosTelemetryOutcome,
        admission_code: Option<QosAdmissionCode>,
        elapsed_micros: u64,
    ) {
        if request.priority != WorkPriority::Background {
            return;
        }
        let telemetry = self
            .telemetry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(telemetry) = telemetry {
            telemetry.record_qos(QosTelemetryEvent {
                phase,
                outcome,
                class: request.class,
                estimated_operations: request.estimated_operations,
                elapsed_micros,
                admission_code,
            });
        }
    }
}

fn local_qos_class_snapshots(
    policy: &LocalQosPolicy,
    state: &LocalQosState,
) -> [LocalQosClassSnapshot; WORK_CLASS_COUNT] {
    WorkClass::ALL.map(|class| local_qos_class_snapshot(policy, state, class))
}

fn local_qos_class_snapshot(
    policy: &LocalQosPolicy,
    state: &LocalQosState,
    class: WorkClass,
) -> LocalQosClassSnapshot {
    let running_background_operations =
        state.running_background_operations_by_class[class.as_index()];
    let max_background_operations = policy.max_background_operations_by_class[class.as_index()]
        .or(policy.max_background_operations);
    LocalQosClassSnapshot {
        class,
        running_background_operations,
        max_background_operations,
        remaining_background_operations: max_background_operations
            .map(|limit| limit.saturating_sub(running_background_operations)),
        over_budget: max_background_operations
            .is_some_and(|limit| running_background_operations > limit),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode, LocalQosPolicy,
        LocalQosScheduler, LocalQosState, QosAdmission, QosAdmissionCode, QosSnapshotBlockerCode,
        QosTelemetryEvent, QosTelemetryOutcome, QosTelemetryPhase, QosTelemetrySink, WorkClass,
        WorkPriority, WorkRequest,
    };
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[derive(Debug, Default)]
    struct RecordingQosTelemetry {
        events: Mutex<Vec<QosTelemetryEvent>>,
    }

    impl QosTelemetrySink for RecordingQosTelemetry {
        fn record_qos(&self, event: QosTelemetryEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[test]
    fn work_priorities_have_stable_string_encodings() {
        assert_eq!(WorkPriority::Foreground.as_str(), "foreground");
        assert_eq!(WorkPriority::Background.as_str(), "background");
        assert_eq!(
            "foreground".parse::<WorkPriority>(),
            Ok(WorkPriority::Foreground)
        );
        assert_eq!(
            "background".parse::<WorkPriority>(),
            Ok(WorkPriority::Background)
        );
        assert!("foreground_work".parse::<WorkPriority>().is_err());
    }

    #[test]
    fn background_scheduler_records_admission_and_completion() {
        let telemetry = Arc::new(RecordingQosTelemetry::default());
        let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
        scheduler.set_telemetry_sink(Some(telemetry.clone()));

        let permit = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();
        permit.finish_with_outcome(false);

        let events = telemetry.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, QosTelemetryPhase::Admission);
        assert_eq!(events[0].outcome, QosTelemetryOutcome::Admitted);
        assert_eq!(events[0].class, WorkClass::Projection);
        assert_eq!(events[0].estimated_operations, 3);
        assert_eq!(events[1].phase, QosTelemetryPhase::Completion);
        assert_eq!(events[1].outcome, QosTelemetryOutcome::Failed);
        assert_eq!(events[1].admission_code, None);
    }

    #[test]
    fn background_scheduler_records_bounded_defer_code() {
        let telemetry = Arc::new(RecordingQosTelemetry::default());
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        });
        scheduler.set_telemetry_sink(Some(telemetry.clone()));

        let admission = scheduler
            .try_start(WorkRequest::background(WorkClass::Import, 2))
            .unwrap_err();

        assert!(matches!(admission, QosAdmission::Defer { .. }));
        let events = telemetry.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase, QosTelemetryPhase::Admission);
        assert_eq!(events[0].outcome, QosTelemetryOutcome::Deferred);
        assert_eq!(
            events[0].admission_code,
            Some(QosAdmissionCode::PerWorkLimitExceeded)
        );
    }

    #[test]
    fn foreground_scheduler_does_not_emit_background_metrics() {
        let telemetry = Arc::new(RecordingQosTelemetry::default());
        let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
        scheduler.set_telemetry_sink(Some(telemetry.clone()));

        let permit = scheduler
            .try_start(WorkRequest::foreground(WorkClass::Query, 1))
            .unwrap();
        permit.finish();

        assert!(telemetry.events.lock().unwrap().is_empty());
    }

    #[test]
    fn work_class_snapshots_follow_the_exhaustive_index_order() {
        let state = LocalQosState {
            running_background_operations: (1..=super::WORK_CLASS_COUNT).sum(),
            running_background_operations_by_class: std::array::from_fn(|index| index + 1),
        };
        let policy = LocalQosPolicy {
            max_background_operations_by_class: std::array::from_fn(|index| Some(index + 3)),
            ..LocalQosPolicy::default()
        };
        let snapshot = policy.snapshot(&state);
        for (index, class) in WorkClass::ALL.into_iter().enumerate() {
            assert_eq!(class.as_index(), index);
            assert_eq!(class.as_str().parse::<WorkClass>(), Ok(class));
            let entry = snapshot.class_snapshots[index];
            assert_eq!(entry.class, class);
            assert_eq!(entry.running_background_operations, index + 1);
            assert_eq!(entry.max_background_operations, Some(index + 3));
            assert_eq!(entry.remaining_background_operations, Some(2));
            assert!(!entry.over_budget);
        }
    }

    #[test]
    fn foreground_probe_and_admission_agree_under_background_pressure() {
        let state = LocalQosState {
            running_background_operations: usize::MAX,
            running_background_operations_by_class: [usize::MAX; super::WORK_CLASS_COUNT],
        };
        for background_enabled in [false, true] {
            let policy = LocalQosPolicy {
                background_enabled,
                max_background_operations: Some(0),
                max_total_background_operations: Some(0),
                max_background_operations_by_class: [Some(0); super::WORK_CLASS_COUNT],
            };
            assert_eq!(policy.foreground_admission(), QosAdmission::Admit);
            assert!(policy.snapshot(&state).foreground_admitted);
            for class in WorkClass::ALL {
                for estimate in [0, 1, usize::MAX] {
                    assert_eq!(
                        policy.admit(&state, &WorkRequest::foreground(class, estimate)),
                        policy.foreground_admission()
                    );
                }
            }
        }
    }

    #[test]
    fn expected_score_matches_admission_scoring_at_tenant_budget_boundaries() {
        for remaining in [None, Some(0), Some(1), Some(8), Some(usize::MAX)] {
            for estimate in [0, 1, 7, 8, 9, usize::MAX] {
                for background_enabled in [false, true] {
                    for active_topic in [false, true] {
                        let hint = BackgroundWorkHint {
                            active_topic,
                            query_probability_per_million: u32::MAX,
                            recent_delta_operations: usize::MAX,
                            source_graph_commit_lag: u64::MAX,
                            staleness_millis: u64::MAX,
                            staleness_ttl_millis: Some(1),
                            freshness_slo_millis: Some(0),
                            tenant_budget_remaining_operations: remaining,
                        };
                        let policy = LocalQosPolicy {
                            background_enabled,
                            ..LocalQosPolicy::default()
                        };
                        let plan = BackgroundWorkPlan::background(
                            WorkClass::Projection,
                            estimate,
                            hint.clone(),
                        );
                        let decision =
                            policy.evaluate_background_work(&LocalQosState::default(), &plan);
                        assert_eq!(hint.expected_value_score(estimate), decision.score);
                        let tenant_exhausted = remaining.is_some_and(|budget| budget < estimate);
                        assert_eq!(decision.score == 0, tenant_exhausted);
                        assert_eq!(
                            decision
                                .reason_codes
                                .contains(&BackgroundWorkReasonCode::TenantBudgetBelowEstimate),
                            tenant_exhausted
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn work_classes_have_stable_string_encodings() {
        assert_eq!(WorkClass::Query.as_str(), "query");
        assert_eq!(WorkClass::Mutation.as_str(), "mutation");
        assert_eq!(WorkClass::Projection.as_str(), "projection");
        assert_eq!(WorkClass::Import.as_str(), "import");
        assert_eq!(WorkClass::Analytics.as_str(), "analytics");
        assert_eq!(WorkClass::Shadow.as_str(), "shadow");
        assert_eq!("query".parse::<WorkClass>(), Ok(WorkClass::Query));
        assert_eq!("mutation".parse::<WorkClass>(), Ok(WorkClass::Mutation));
        assert_eq!("projection".parse::<WorkClass>(), Ok(WorkClass::Projection));
        assert_eq!("import".parse::<WorkClass>(), Ok(WorkClass::Import));
        assert_eq!("analytics".parse::<WorkClass>(), Ok(WorkClass::Analytics));
        assert_eq!("shadow".parse::<WorkClass>(), Ok(WorkClass::Shadow));
        assert!("maintenance".parse::<WorkClass>().is_err());
    }

    #[test]
    fn qos_admission_codes_have_stable_string_encodings() {
        assert_eq!(
            QosAdmissionCode::BackgroundDisabled.as_str(),
            "background_disabled"
        );
        assert_eq!(
            QosAdmissionCode::PerWorkLimitExceeded.as_str(),
            "per_work_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::TotalBackgroundLimitExceeded.as_str(),
            "total_background_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::ClassBackgroundLimitExceeded.as_str(),
            "class_background_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::TenantBudgetExceeded.as_str(),
            "tenant_budget_exceeded"
        );
        assert_eq!(
            "background_disabled".parse::<QosAdmissionCode>(),
            Ok(QosAdmissionCode::BackgroundDisabled)
        );
        assert_eq!(
            "per_work_limit_exceeded".parse::<QosAdmissionCode>(),
            Ok(QosAdmissionCode::PerWorkLimitExceeded)
        );
        assert_eq!(
            "total_background_limit_exceeded".parse::<QosAdmissionCode>(),
            Ok(QosAdmissionCode::TotalBackgroundLimitExceeded)
        );
        assert_eq!(
            "class_background_limit_exceeded".parse::<QosAdmissionCode>(),
            Ok(QosAdmissionCode::ClassBackgroundLimitExceeded)
        );
        assert_eq!(
            "tenant_budget_exceeded".parse::<QosAdmissionCode>(),
            Ok(QosAdmissionCode::TenantBudgetExceeded)
        );
        assert!("background_paused".parse::<QosAdmissionCode>().is_err());
    }

    #[test]
    fn qos_admissions_have_stable_string_encodings() {
        assert_eq!(QosAdmission::Admit.as_str(), "admit");
        assert_eq!(
            QosAdmission::Defer {
                code: QosAdmissionCode::BackgroundDisabled,
                reason: "disabled".to_string(),
            }
            .as_str(),
            "defer"
        );
        assert_eq!(
            QosAdmission::Reject {
                code: QosAdmissionCode::TenantBudgetExceeded,
                reason: "budget".to_string(),
            }
            .as_str(),
            "reject"
        );
    }

    #[test]
    fn background_work_reason_codes_have_stable_string_encodings() {
        let cases = [
            (
                BackgroundWorkReasonCode::ForegroundNotRanked,
                "foreground_not_ranked",
            ),
            (
                BackgroundWorkReasonCode::TenantBudgetBelowEstimate,
                "tenant_budget_below_estimate",
            ),
            (BackgroundWorkReasonCode::ActiveTopic, "active_topic"),
            (
                BackgroundWorkReasonCode::QueryProbability,
                "query_probability",
            ),
            (
                BackgroundWorkReasonCode::RecentDeltaOperations,
                "recent_delta_operations",
            ),
            (
                BackgroundWorkReasonCode::SourceGraphCommitLag,
                "source_graph_commit_lag",
            ),
            (BackgroundWorkReasonCode::StalenessTtl, "staleness_ttl"),
            (BackgroundWorkReasonCode::FreshnessSlo, "freshness_slo"),
            (
                BackgroundWorkReasonCode::AdmissionDeferred,
                "admission_deferred",
            ),
            (
                BackgroundWorkReasonCode::AdmissionRejected,
                "admission_rejected",
            ),
        ];

        for (code, name) in cases {
            assert_eq!(code.as_str(), name);
            assert_eq!(name.parse::<BackgroundWorkReasonCode>(), Ok(code));
        }
        assert!("cron_reason".parse::<BackgroundWorkReasonCode>().is_err());
    }

    #[test]
    fn admits_foreground_work_without_budget_gate() {
        let policy = LocalQosPolicy::default();
        let request = WorkRequest::foreground(WorkClass::Query, usize::MAX);

        assert_eq!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Admit
        );
    }

    #[test]
    fn defers_background_when_disabled() {
        let policy = LocalQosPolicy {
            background_enabled: false,
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 1);

        let admission = policy.admit(&LocalQosState::default(), &request);

        assert_eq!(admission.code(), Some(QosAdmissionCode::BackgroundDisabled));
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("background_disabled")
        );
        assert_eq!(
            "background_disabled".parse::<QosAdmissionCode>(),
            admission.code().ok_or("missing admission code")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("disabled")
        ));
    }

    #[test]
    fn foreground_work_evaluation_reports_not_ranked_code() {
        let decision = LocalQosPolicy::default().evaluate_background_work(
            &LocalQosState::default(),
            &BackgroundWorkPlan {
                request: WorkRequest::foreground(WorkClass::Query, 1),
                hint: BackgroundWorkHint::default(),
            },
        );

        assert_eq!(decision.admission, QosAdmission::Admit);
        assert_eq!(decision.score, 0);
        assert_eq!(
            decision.reason_codes,
            vec![BackgroundWorkReasonCode::ForegroundNotRanked]
        );
        assert_eq!(
            decision.reasons,
            vec!["foreground work is not background-ranked".to_string()]
        );
    }

    #[test]
    fn defers_background_over_per_work_budget() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(4),
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 5);

        let admission = policy.admit(&LocalQosState::default(), &request);

        assert_eq!(
            admission.code(),
            Some(QosAdmissionCode::PerWorkLimitExceeded)
        );
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("per_work_limit_exceeded")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("per-work limit")
        ));
    }

    #[test]
    fn defers_background_when_running_budget_is_exhausted() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(12),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 8,
            ..LocalQosState::default()
        };
        let request = WorkRequest::background(WorkClass::Analytics, 5);

        let admission = policy.admit(&state, &request);

        assert_eq!(
            admission.code(),
            Some(QosAdmissionCode::TotalBackgroundLimitExceeded)
        );
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("total_background_limit_exceeded")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 12")
        ));
    }

    #[test]
    fn background_work_hint_scores_expected_value_signals() {
        let low = BackgroundWorkHint {
            query_probability_per_million: 10_000,
            recent_delta_operations: 2,
            staleness_millis: 100,
            staleness_ttl_millis: Some(1_000),
            ..BackgroundWorkHint::default()
        };
        let high = BackgroundWorkHint {
            active_topic: true,
            query_probability_per_million: 800_000,
            recent_delta_operations: 20,
            source_graph_commit_lag: 3,
            staleness_millis: 6_000,
            staleness_ttl_millis: Some(1_000),
            freshness_slo_millis: Some(5_000),
            ..BackgroundWorkHint::default()
        };

        assert!(high.expected_value_score(1) > low.expected_value_score(1));
        let decision = LocalQosPolicy::default().evaluate_background_work(
            &LocalQosState::default(),
            &BackgroundWorkPlan::background(WorkClass::Projection, 1, high),
        );

        assert_eq!(decision.admission, QosAdmission::Admit);
        assert!(decision.score > 0);
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::ActiveTopic));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::QueryProbability));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::RecentDeltaOperations));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::SourceGraphCommitLag));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::StalenessTtl));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::FreshnessSlo));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("active topic")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("source graph commit lag 3")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("staleness ttl reached")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("freshness slo missed")));
    }

    #[test]
    fn background_work_evaluation_keeps_value_when_budget_defers() {
        let policy = LocalQosPolicy {
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 3,
            ..LocalQosState::default()
        };
        let decision = policy.evaluate_background_work(
            &state,
            &BackgroundWorkPlan::background(
                WorkClass::Analytics,
                2,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 500_000,
                    ..BackgroundWorkHint::default()
                },
            ),
        );

        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::AdmissionDeferred));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("admission deferred")));
    }

    #[test]
    fn tenant_budget_defers_background_work_without_rejecting_it() {
        let decision = LocalQosPolicy::default().evaluate_background_work(
            &LocalQosState::default(),
            &BackgroundWorkPlan::background(
                WorkClass::Import,
                8,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 1_000_000,
                    tenant_budget_remaining_operations: Some(4),
                    ..BackgroundWorkHint::default()
                },
            ),
        );

        assert_eq!(
            decision.admission.code(),
            Some(QosAdmissionCode::TenantBudgetExceeded)
        );
        assert_eq!(
            decision.admission.code().map(QosAdmissionCode::as_str),
            Some("tenant_budget_exceeded")
        );
        assert_eq!(
            decision.admission.reason(),
            Some("tenant budget remaining 4 below estimated operations 8")
        );
        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { ref reason, .. } if reason.contains("tenant budget remaining 4")
        ));
        assert_eq!(decision.score, 0);
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::TenantBudgetBelowEstimate));
        assert!(decision
            .reason_codes
            .contains(&BackgroundWorkReasonCode::AdmissionDeferred));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("tenant budget remaining 4")));
    }

    #[test]
    fn scheduler_evaluates_background_work_against_running_state() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();

        let decision = scheduler.evaluate_background_work(&BackgroundWorkPlan::background(
            WorkClass::Import,
            2,
            BackgroundWorkHint {
                active_topic: true,
                ..BackgroundWorkHint::default()
            },
        ));

        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);

        running.finish();
    }

    #[test]
    fn background_work_ranking_prefers_admitted_high_value_plans_stably() {
        let policy = LocalQosPolicy {
            max_total_background_operations: Some(3),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 1,
            ..LocalQosState::default()
        };
        let plans = vec![
            BackgroundWorkPlan::background(
                WorkClass::Projection,
                3,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 1_000_000,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Import,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 10,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Analytics,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 100_000,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Shadow,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 100_000,
                    ..BackgroundWorkHint::default()
                },
            ),
        ];

        let ranked = policy.rank_background_work(&state, &plans);

        assert_eq!(
            ranked.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![2, 3, 1, 0]
        );
        assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
        assert!(matches!(ranked[1].decision.admission, QosAdmission::Admit));
        assert!(matches!(ranked[2].decision.admission, QosAdmission::Admit));
        assert!(matches!(
            ranked[3].decision.admission,
            QosAdmission::Defer { .. }
        ));
        assert!(ranked[0].decision.score >= ranked[1].decision.score);
    }

    #[test]
    fn scheduler_ranks_background_work_against_current_state() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(2),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 1))
            .unwrap();
        let plans = vec![
            BackgroundWorkPlan::background(
                WorkClass::Import,
                2,
                BackgroundWorkHint {
                    active_topic: true,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Import,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 1,
                    ..BackgroundWorkHint::default()
                },
            ),
        ];

        let ranked = scheduler.rank_background_work(&plans);

        assert_eq!(ranked[0].index, 1);
        assert_eq!(ranked[1].index, 0);
        assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
        assert!(matches!(
            ranked[1].decision.admission,
            QosAdmission::Defer { .. }
        ));

        running.finish();
    }

    #[test]
    fn scheduler_tracks_background_running_operations() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(12),
            ..LocalQosPolicy::default()
        };
        let scheduler = LocalQosScheduler::new(policy);

        let first = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 8))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 8);

        let second = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 5))
            .unwrap_err();
        assert!(matches!(
            second,
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 12")
        ));
        assert_eq!(scheduler.state().running_background_operations, 8);

        first.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);

        let second = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 5))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 5);
        second.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn scheduler_clones_share_one_background_budget() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let shared = scheduler.clone();
        let permit = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();

        let admission = shared
            .try_start(WorkRequest::background(WorkClass::Analytics, 2))
            .unwrap_err();
        assert_eq!(
            admission.code(),
            Some(QosAdmissionCode::TotalBackgroundLimitExceeded)
        );

        drop(permit);
        assert_eq!(shared.state().running_background_operations, 0);
        shared
            .try_start(WorkRequest::background(WorkClass::Analytics, 2))
            .unwrap()
            .finish();
    }

    #[test]
    fn concurrent_scheduler_clones_never_over_admit_shared_budget() {
        const WORKER_COUNT: usize = 8;
        const BACKGROUND_LIMIT: usize = 4;

        let scheduler = Arc::new(LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(BACKGROUND_LIMIT),
            ..LocalQosPolicy::default()
        }));
        let start = Arc::new(Barrier::new(WORKER_COUNT + 1));
        let settled = Arc::new(Barrier::new(WORKER_COUNT + 1));
        let release = Arc::new(Barrier::new(WORKER_COUNT + 1));

        let workers = (0..WORKER_COUNT)
            .map(|_| {
                let scheduler = Arc::clone(&scheduler);
                let start = Arc::clone(&start);
                let settled = Arc::clone(&settled);
                let release = Arc::clone(&release);
                thread::spawn(move || {
                    start.wait();
                    let permit = scheduler
                        .try_start(WorkRequest::background(WorkClass::Projection, 1))
                        .ok();
                    settled.wait();
                    let admitted = permit.is_some();
                    release.wait();
                    drop(permit);
                    admitted
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        settled.wait();
        assert_eq!(
            scheduler.state().running_background_operations,
            BACKGROUND_LIMIT
        );
        release.wait();
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|admitted| *admitted)
                .count(),
            BACKGROUND_LIMIT
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn permit_drop_releases_background_budget_after_error_and_panic() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(3),
            ..LocalQosPolicy::default()
        });
        let fail_after_admission = || -> std::result::Result<(), &'static str> {
            let _permit = scheduler
                .try_start(WorkRequest::background(WorkClass::Projection, 3))
                .unwrap();
            Err("operation failed")
        };

        assert_eq!(fail_after_admission(), Err("operation failed"));
        assert_eq!(scheduler.state().running_background_operations, 0);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _permit = scheduler
                .try_start(WorkRequest::background(WorkClass::Projection, 3))
                .unwrap();
            panic!("operation panicked");
        }));
        assert!(panic.is_err());
        assert_eq!(scheduler.state().running_background_operations, 0);
        scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap()
            .finish();
    }

    #[test]
    fn scheduler_does_not_charge_foreground_work() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(1),
            ..LocalQosPolicy::default()
        });

        let permit = scheduler
            .try_start(WorkRequest::foreground(WorkClass::Query, usize::MAX))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 0);

        permit.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn scheduler_tracks_background_running_operations_by_class() {
        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(4);
        class_limits[WorkClass::Analytics.as_index()] = Some(10);
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(20),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });

        let projection = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Projection.as_index()],
            3
        );

        let same_class = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 2))
            .unwrap_err();
        assert_eq!(
            same_class.code(),
            Some(QosAdmissionCode::ClassBackgroundLimitExceeded)
        );
        assert_eq!(
            same_class.code().map(QosAdmissionCode::as_str),
            Some("class_background_limit_exceeded")
        );
        assert!(matches!(
            same_class,
            QosAdmission::Defer { reason, .. } if reason.contains("class limit 4")
        ));

        let analytics = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 2))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 5);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Analytics.as_index()],
            2
        );

        projection.finish();
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Projection.as_index()],
            0
        );
        assert_eq!(scheduler.state().running_background_operations, 2);

        analytics.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn class_budget_does_not_gate_foreground_work() {
        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(0);
        let policy = LocalQosPolicy {
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::foreground(WorkClass::Projection, usize::MAX);

        assert_eq!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Admit
        );
    }

    #[test]
    fn qos_snapshot_reports_foreground_first_bounded_background_state() {
        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(4);
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(12),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });
        let permit = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();

        let snapshot = scheduler.snapshot();

        assert!(snapshot.ready);
        assert!(snapshot.foreground_admitted);
        assert!(snapshot.background_enabled);
        assert!(snapshot.background_bounded);
        assert_eq!(snapshot.running_background_operations, 3);
        assert_eq!(snapshot.remaining_total_background_operations, Some(9));
        assert!(!snapshot.total_background_over_budget);
        assert!(snapshot.blocker_codes.is_empty());
        let projection = snapshot.class_snapshots[WorkClass::Projection.as_index()];
        assert_eq!(projection.class, WorkClass::Projection);
        assert_eq!(projection.running_background_operations, 3);
        assert_eq!(projection.max_background_operations, Some(4));
        assert_eq!(projection.remaining_background_operations, Some(1));
        assert!(!projection.over_budget);

        permit.finish();
    }

    #[test]
    fn qos_snapshot_fails_closed_for_unbounded_or_over_budget_background() {
        let unbounded = LocalQosPolicy {
            max_background_operations: None,
            max_total_background_operations: None,
            max_background_operations_by_class: [None; super::WORK_CLASS_COUNT],
            background_enabled: true,
        }
        .snapshot(&LocalQosState::default());
        assert!(!unbounded.ready);
        assert_eq!(
            unbounded.blocker_codes,
            vec![QosSnapshotBlockerCode::BackgroundUnbounded]
        );
        assert_eq!(unbounded.blocker_codes[0].as_str(), "background_unbounded");

        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(2);
        let over_budget = LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            background_enabled: true,
        }
        .snapshot(&LocalQosState {
            running_background_operations: 5,
            running_background_operations_by_class: [0, 0, 3, 0, 0, 0],
        });

        assert!(!over_budget.ready);
        assert!(over_budget.total_background_over_budget);
        assert!(over_budget.class_snapshots[WorkClass::Projection.as_index()].over_budget);
        assert_eq!(
            over_budget.blocker_codes,
            vec![
                QosSnapshotBlockerCode::TotalBackgroundOverBudget,
                QosSnapshotBlockerCode::ClassBackgroundOverBudget
            ]
        );
    }
}

#[cfg(all(test, feature = "loom-tests"))]
mod loom_tests {
    use super::{
        LocalQosPolicy, LocalQosScheduler, QosAdmissionCode, WorkClass, WorkRequest,
        WORK_CLASS_COUNT,
    };
    use loom::sync::Arc;
    use loom::thread;

    #[test]
    fn scheduler_preserves_background_budget_under_modeled_concurrent_start_finish() {
        loom::model(|| {
            let mut class_limits = [None; WORK_CLASS_COUNT];
            class_limits[WorkClass::Projection.as_index()] = Some(4);
            let scheduler = Arc::new(LocalQosScheduler::new(LocalQosPolicy {
                max_background_operations: Some(4),
                max_total_background_operations: Some(4),
                max_background_operations_by_class: class_limits,
                background_enabled: true,
            }));

            let first = spawn_projection_worker(Arc::clone(&scheduler));
            let second = spawn_projection_worker(Arc::clone(&scheduler));

            let first_admitted = first.join().unwrap();
            let second_admitted = second.join().unwrap();

            assert!(first_admitted || second_admitted);
            assert!(scheduler.state().running_background_operations <= 4);
            assert_eq!(scheduler.state().running_background_operations, 0);
            assert_eq!(
                scheduler.state().running_background_operations_by_class
                    [WorkClass::Projection.as_index()],
                0
            );
        });
    }

    fn spawn_projection_worker(scheduler: Arc<LocalQosScheduler>) -> thread::JoinHandle<bool> {
        thread::spawn(move || {
            let admission = scheduler.try_start(WorkRequest::background(WorkClass::Projection, 3));
            assert!(scheduler.state().running_background_operations <= 4);
            assert!(
                scheduler.state().running_background_operations_by_class
                    [WorkClass::Projection.as_index()]
                    <= 4
            );

            thread::yield_now();

            match admission {
                Ok(permit) => {
                    assert!(scheduler.state().running_background_operations <= 4);
                    permit.finish();
                    assert!(scheduler.state().running_background_operations <= 4);
                    true
                }
                Err(admission) => {
                    assert!(matches!(
                        admission.code(),
                        Some(
                            QosAdmissionCode::TotalBackgroundLimitExceeded
                                | QosAdmissionCode::ClassBackgroundLimitExceeded
                        )
                    ));
                    false
                }
            }
        })
    }
}
