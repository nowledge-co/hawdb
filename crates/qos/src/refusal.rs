// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Typed caller-facing errors for refused local QoS background admissions.
//!
//! A deferred or rejected background admission ran and wrote nothing, so it
//! is never a storage failure (#1926). The QoS operation budget behaves like a
//! background task budget: every budget refusal is reported as
//! `HawDBError::RuntimeAdmissionRefused` with the governor's stable
//! `background_task_saturated` code, in units of estimated background
//! operations. Disabled background work is a configuration state, not
//! transient load, and is reported as
//! `CapabilityUnavailable(BackgroundMaintenance)`.

use hawdb_core::{HawDBError, RuntimeAdmissionRefusal, RuntimeCapability};

use crate::{
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission,
    QosAdmissionCode, RuntimeAdmissionCode, WorkRequest,
};

impl QosAdmissionCode {
    /// The stable `RuntimeAdmissionRefusal::code` reported for a refusal with
    /// this code, or `None` for [`Self::BackgroundDisabled`], which is reported
    /// as `CapabilityUnavailable(BackgroundMaintenance)` instead.
    pub const fn runtime_admission_code(self) -> Option<RuntimeAdmissionCode> {
        match self {
            Self::BackgroundDisabled => None,
            Self::PerWorkLimitExceeded
            | Self::TotalBackgroundLimitExceeded
            | Self::ClassBackgroundLimitExceeded
            | Self::TenantBudgetExceeded => Some(RuntimeAdmissionCode::BackgroundTaskSaturated),
        }
    }
}

impl QosAdmission {
    /// The typed error a caller receives for this admission of `request`, or
    /// `None` when it was admitted.
    ///
    /// `state` is the background state the admission was decided against.
    /// A budget refusal is `RuntimeAdmissionRefused` with code
    /// `background_task_saturated`: `requested` is the request's estimated
    /// operations and `available` the operations the refusing limit had left
    /// (the limit itself for a per-work limit). A tenant budget refusal has
    /// `available` 0 because the remaining tenant budget belongs to the
    /// caller's hint, not to the policy. `retryable` is true for a deferral
    /// whose unchanged request fits every configured per-work, total and
    /// class limit at idle, so it clears once running background work
    /// finishes. It is false for a rejection, for any per-work limit refusal,
    /// and for a request larger than any configured limit, which can never be
    /// admitted unchanged.
    pub fn refusal_error(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        request: &WorkRequest,
    ) -> Option<HawDBError> {
        let (code, deferred) = match self {
            QosAdmission::Admit => return None,
            QosAdmission::Defer { code, .. } => (*code, true),
            QosAdmission::Reject { code, .. } => (*code, false),
        };
        let Some(runtime_code) = code.runtime_admission_code() else {
            return Some(HawDBError::CapabilityUnavailable {
                capability: RuntimeCapability::BackgroundMaintenance,
            });
        };
        let class = request.class.as_index();
        let running = match code {
            QosAdmissionCode::TotalBackgroundLimitExceeded => state.running_background_operations,
            QosAdmissionCode::ClassBackgroundLimitExceeded => {
                state.running_background_operations_by_class[class]
            }
            _ => 0,
        };
        // The refusing limit, when the policy owns it. A per-work limit never
        // counts running work, so its whole limit is what is available.
        let limit = match code {
            QosAdmissionCode::PerWorkLimitExceeded => policy.max_background_operations,
            QosAdmissionCode::TotalBackgroundLimitExceeded => {
                policy.max_total_background_operations
            }
            QosAdmissionCode::ClassBackgroundLimitExceeded => {
                policy.max_background_operations_by_class[class]
            }
            QosAdmissionCode::BackgroundDisabled | QosAdmissionCode::TenantBudgetExceeded => None,
        };
        // `retryable` asks whether the unchanged request could ever be
        // admitted, so it is checked against every configured per-work,
        // total and class limit at idle, not only the limit that refused
        // this attempt: a temporary total shortage must not hide a class
        // limit the request can never fit (and vice versa). A per-work limit
        // refusal can never succeed unchanged.
        // A tenant budget deferral is reported as retryable when the request
        // fits every policy limit, because the policy does not own the tenant
        // budget and cannot tell whether it will recover. No caller supplies
        // a tenant budget hint today, so that path is unreachable outside
        // tests; revisit `retryable` if one does.
        let within =
            |limit: Option<usize>| limit.is_none_or(|limit| request.estimated_operations <= limit);
        let fits = code != QosAdmissionCode::PerWorkLimitExceeded
            && within(policy.max_background_operations)
            && within(policy.max_total_background_operations)
            && within(policy.max_background_operations_by_class[class]);
        Some(HawDBError::RuntimeAdmissionRefused(
            RuntimeAdmissionRefusal {
                code: runtime_code.as_str(),
                requested: operations(request.estimated_operations),
                available: limit.map_or(0, |limit| operations(limit.saturating_sub(running))),
                retryable: deferred && fits,
                waited: None,
            },
        ))
    }
}

impl LocalQosPolicy {
    /// Admit `request` against `state`, returning the typed refusal described
    /// by [`QosAdmission::refusal_error`] when it is deferred or rejected.
    pub fn admit_or_refuse(
        &self,
        state: &LocalQosState,
        request: &WorkRequest,
    ) -> hawdb_core::Result<()> {
        match self
            .admit(state, request)
            .refusal_error(self, state, request)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl LocalQosScheduler {
    /// [`Self::try_start`] with the typed refusal described by
    /// [`QosAdmission::refusal_error`], reported against the charged state the
    /// admission was decided against.
    pub fn try_start_or_refuse(&self, request: WorkRequest) -> hawdb_core::Result<LocalQosPermit> {
        self.try_start_with_state(request.clone())
            .map_err(|(admission, state)| {
                admission
                    .refusal_error(self.policy(), &state, &request)
                    .expect("a refused admission has a typed error")
            })
    }
}

fn operations(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkClass, WORK_CLASS_COUNT};

    fn refusal(error: HawDBError) -> RuntimeAdmissionRefusal {
        match error {
            HawDBError::RuntimeAdmissionRefused(refusal) => refusal,
            other => panic!("expected a typed admission refusal, got {other:?}"),
        }
    }

    #[test]
    fn deferred_and_rejected_admissions_map_to_typed_refusals() {
        let policy = LocalQosPolicy {
            max_total_background_operations: Some(5),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 3,
            ..LocalQosState::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 4);
        for (admission, retryable) in [
            (
                QosAdmission::Defer {
                    code: QosAdmissionCode::TotalBackgroundLimitExceeded,
                    reason: "busy".into(),
                },
                true,
            ),
            (
                QosAdmission::Reject {
                    code: QosAdmissionCode::TotalBackgroundLimitExceeded,
                    reason: "never".into(),
                },
                false,
            ),
        ] {
            let refusal = refusal(admission.refusal_error(&policy, &state, &request).unwrap());
            assert_eq!(
                refusal,
                RuntimeAdmissionRefusal {
                    code: "background_task_saturated",
                    requested: 4,
                    available: 2,
                    retryable,
                    waited: None,
                }
            );
        }
        assert!(QosAdmission::Admit
            .refusal_error(&policy, &state, &request)
            .is_none());
    }

    #[test]
    fn every_budget_code_reports_its_limit_headroom() {
        let mut by_class = [None; WORK_CLASS_COUNT];
        by_class[WorkClass::Import.as_index()] = Some(16);
        let policy = LocalQosPolicy {
            max_background_operations: Some(12),
            max_total_background_operations: Some(20),
            max_background_operations_by_class: by_class,
            background_enabled: true,
        };
        let mut running_by_class = [0; WORK_CLASS_COUNT];
        running_by_class[WorkClass::Import.as_index()] = 15;
        let state = LocalQosState {
            running_background_operations: 19,
            running_background_operations_by_class: running_by_class,
        };
        let request = WorkRequest::background(WorkClass::Import, 12);
        for (code, available, retryable) in [
            (QosAdmissionCode::PerWorkLimitExceeded, 12, false),
            (QosAdmissionCode::TotalBackgroundLimitExceeded, 1, true),
            (QosAdmissionCode::ClassBackgroundLimitExceeded, 1, true),
            (QosAdmissionCode::TenantBudgetExceeded, 0, true),
        ] {
            let admission = QosAdmission::Defer {
                code,
                reason: String::new(),
            };
            let refusal = refusal(admission.refusal_error(&policy, &state, &request).unwrap());
            assert_eq!(refusal.code, "background_task_saturated", "{code:?}");
            assert_eq!(refusal.requested, 12, "{code:?}");
            assert_eq!(refusal.available, available, "{code:?}");
            assert_eq!(refusal.retryable, retryable, "{code:?}");
        }
        // A deferral the limit itself can never satisfy is not retryable.
        let oversized = WorkRequest::background(WorkClass::Import, 21);
        for code in [
            QosAdmissionCode::TotalBackgroundLimitExceeded,
            QosAdmissionCode::ClassBackgroundLimitExceeded,
        ] {
            let admission = QosAdmission::Defer {
                code,
                reason: String::new(),
            };
            let refusal = refusal(
                admission
                    .refusal_error(&policy, &state, &oversized)
                    .unwrap(),
            );
            assert!(!refusal.retryable, "{code:?}");
        }
        // The real policy reports an oversized request as non-retryable.
        let error = policy
            .admit_or_refuse(&LocalQosState::default(), &oversized)
            .unwrap_err();
        assert!(!refusal(error).retryable);
    }

    #[test]
    fn disabled_background_work_is_an_unavailable_capability() {
        let policy = LocalQosPolicy {
            background_enabled: false,
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 1);
        assert!(matches!(
            policy.admit_or_refuse(&LocalQosState::default(), &request),
            Err(HawDBError::CapabilityUnavailable {
                capability: RuntimeCapability::BackgroundMaintenance
            })
        ));
        assert!(policy
            .admit_or_refuse(
                &LocalQosState::default(),
                &WorkRequest::foreground(WorkClass::Projection, 1)
            )
            .is_ok());
    }

    #[test]
    fn scheduler_refusal_reports_the_state_it_was_decided_against() {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(3),
            ..LocalQosPolicy::default()
        });
        let held = scheduler
            .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 2))
            .unwrap();
        let error = scheduler
            .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 2))
            .unwrap_err();
        assert_eq!(
            refusal(error),
            RuntimeAdmissionRefusal {
                code: "background_task_saturated",
                requested: 2,
                available: 1,
                retryable: true,
                waited: None,
            }
        );
        held.finish();
        scheduler
            .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 2))
            .unwrap()
            .finish();
        assert_eq!(scheduler.state(), LocalQosState::default());
    }

    #[test]
    fn retryable_requires_fitting_every_limit_at_idle() {
        // A temporary total shortage must not hide a class limit the request
        // can never fit (#1935 review).
        let mut by_class = [None; WORK_CLASS_COUNT];
        by_class[WorkClass::Projection.as_index()] = Some(2);
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: None,
            max_total_background_operations: Some(10),
            max_background_operations_by_class: by_class,
            background_enabled: true,
        });
        let held = scheduler
            .try_start_or_refuse(WorkRequest::background(WorkClass::Import, 8))
            .unwrap();
        let busy = refusal(
            scheduler
                .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 3))
                .unwrap_err(),
        );
        assert_eq!(busy.requested, 3);
        assert!(!busy.retryable, "{busy:?}");
        held.finish();
        let idle = refusal(
            scheduler
                .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 3))
                .unwrap_err(),
        );
        assert_eq!(idle.requested, 3);
        assert!(!idle.retryable, "{idle:?}");
        // A request that fits every limit stays retryable while busy.
        let held = scheduler
            .try_start_or_refuse(WorkRequest::background(WorkClass::Import, 9))
            .unwrap();
        let fits = refusal(
            scheduler
                .try_start_or_refuse(WorkRequest::background(WorkClass::Projection, 2))
                .unwrap_err(),
        );
        assert!(fits.retryable, "{fits:?}");
        held.finish();
        assert_eq!(scheduler.state(), LocalQosState::default());
    }
}
