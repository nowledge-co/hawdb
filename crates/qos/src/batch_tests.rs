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

//! Batched background permits (#1616).

use super::{
    LocalQosPolicy, LocalQosScheduler, QosAdmission, QosAdmissionCode, QosTelemetryEvent,
    QosTelemetryOutcome, QosTelemetryPhase, QosTelemetrySink, WorkClass, WorkRequest,
};
use hawdb_core::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug, Default)]
struct Events(Mutex<Vec<QosTelemetryEvent>>);

impl QosTelemetrySink for Events {
    fn record_qos(&self, event: QosTelemetryEvent) {
        self.0.lock().unwrap().push(event);
    }
}

impl Events {
    fn phases(&self) -> Vec<(QosTelemetryPhase, QosTelemetryOutcome)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|event| (event.phase, event.outcome))
            .collect()
    }
}

fn one_operation() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn unit() -> WorkRequest {
    WorkRequest::background(WorkClass::Mutation, 1)
}

#[test]
fn batch_charges_only_while_running_and_reports_one_admission_and_completion() {
    let scheduler = one_operation();
    let events = Arc::new(Events::default());
    scheduler.set_telemetry_sink(Some(events.clone()));
    let started = Instant::now();
    let batch = scheduler.try_start_batch_at(unit(), started).unwrap();
    assert_eq!(batch.request(), &unit());
    assert_eq!(scheduler.state().running_background_operations, 1);
    for _ in 0..100 {
        batch.idle();
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Mutation.as_index()],
            0
        );
        assert!(scheduler.snapshot().ready);
        assert!(batch.resume());
        assert_eq!(scheduler.state().running_background_operations, 1);
        // Resuming a running batch is idempotent.
        assert!(batch.resume());
    }
    batch.finish_with_outcome_at(true, started + Duration::from_millis(7));
    assert_eq!(scheduler.state().running_background_operations, 0);
    let recorded = events.0.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "{recorded:?}");
    assert_eq!(recorded[0].phase, QosTelemetryPhase::Admission);
    assert_eq!(recorded[0].outcome, QosTelemetryOutcome::Admitted);
    assert_eq!(recorded[1].phase, QosTelemetryPhase::Completion);
    assert_eq!(recorded[1].outcome, QosTelemetryOutcome::Completed);
    assert_eq!(recorded[1].elapsed_micros, 7_000);
    assert!(recorded.iter().all(|event| event.estimated_operations == 1));
}

#[test]
fn running_batch_is_never_reclaimed_and_refuses_competitors() {
    let scheduler = one_operation();
    let batch = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    let denied = scheduler.try_start(unit()).unwrap_err();
    assert_eq!(
        denied.code(),
        Some(QosAdmissionCode::TotalBackgroundLimitExceeded)
    );
    assert!(!batch.is_reclaimed());
    assert!(batch.resume());
    batch.finish_with_outcome(true);
    scheduler.try_start(unit()).unwrap().finish();
}

#[test]
fn competitor_reclaims_an_idle_batch_whose_owner_must_admit_again() {
    let scheduler = one_operation();
    let events = Arc::new(Events::default());
    scheduler.set_telemetry_sink(Some(events.clone()));
    let batch = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    batch.idle();
    // The idle batch would refuse this admission, so it is reclaimed.
    let competitor = scheduler.try_start(unit()).unwrap();
    assert!(batch.is_reclaimed());
    assert!(!batch.resume());
    assert_eq!(scheduler.state().running_background_operations, 1);
    // The owner's next admission is deferred while the competitor runs.
    let deferred = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap_err();
    assert!(matches!(deferred, QosAdmission::Defer { .. }));
    // Finishing a reclaimed batch neither refunds again nor reports twice.
    batch.finish_with_outcome(false);
    assert_eq!(scheduler.state().running_background_operations, 1);
    competitor.finish();
    assert_eq!(scheduler.state().running_background_operations, 0);
    let again = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    drop(again);
    assert_eq!(scheduler.state().running_background_operations, 0);
    use QosTelemetryOutcome::*;
    use QosTelemetryPhase::*;
    assert_eq!(
        events.phases(),
        vec![
            (Admission, Admitted),
            (Completion, Completed), // reclaimed idle batch
            (Admission, Admitted),   // competitor
            (Admission, Deferred),
            (Completion, Completed), // competitor
            (Admission, Admitted),
            (Completion, Failed), // dropped batch
        ]
    );
}

#[test]
fn idle_batch_is_kept_when_free_budget_admits_the_competitor() {
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(2),
        ..LocalQosPolicy::default()
    });
    let batch = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    batch.idle();
    let competitor = scheduler.try_start(unit()).unwrap();
    assert!(batch.resume());
    assert_eq!(scheduler.state().running_background_operations, 2);
    competitor.finish();
    batch.finish_with_outcome(true);
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
fn telemetry_change_reclaims_idle_batches_into_the_previous_sink() {
    let scheduler = one_operation();
    let first = Arc::new(Events::default());
    let second = Arc::new(Events::default());
    scheduler.set_telemetry_sink(Some(first.clone()));
    let idle = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    idle.idle();
    scheduler.set_telemetry_sink(Some(second.clone()));
    assert!(!idle.resume());
    drop(idle);
    use QosTelemetryOutcome::*;
    use QosTelemetryPhase::*;
    assert_eq!(
        first.phases(),
        vec![(Admission, Admitted), (Completion, Completed)]
    );
    assert!(second.phases().is_empty());
    // A running batch is not reclaimed by a sink change; its completion is
    // reported to whichever sink is installed when it finishes.
    let running = scheduler
        .try_start_batch_at(unit(), Instant::now())
        .unwrap();
    scheduler.set_telemetry_sink(None);
    assert!(running.resume());
    assert_eq!(scheduler.state().running_background_operations, 1);
    running.finish_with_outcome(true);
    assert_eq!(second.phases(), vec![(Admission, Admitted)]);
}

/// The owner's resume and a competitor's reclaim race on one atomic slot:
/// at most one of them may run against a one-operation budget.
#[test]
fn concurrent_resume_and_reclaim_never_exceed_the_budget() {
    let scheduler = one_operation();
    let running = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let competitor_admitted = Arc::new(AtomicUsize::new(0));
    let competitor = {
        let scheduler = scheduler.clone();
        let running = running.clone();
        let stop = stop.clone();
        let admitted = competitor_admitted.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                if let Ok(permit) = scheduler.try_start(unit()) {
                    assert_eq!(running.fetch_add(1, Ordering::SeqCst), 0);
                    admitted.fetch_add(1, Ordering::SeqCst);
                    thread::yield_now();
                    running.fetch_sub(1, Ordering::SeqCst);
                    permit.finish();
                }
            }
        })
    };
    let mut batch = None;
    let mut owner_units = 0;
    let mut admissions = 0;
    let started = Instant::now();
    let minimum = started + Duration::from_millis(200);
    let limit = started + Duration::from_secs(20);
    while Instant::now() < limit
        && (Instant::now() < minimum
            || owner_units < 1_000
            || competitor_admitted.load(Ordering::SeqCst) == 0)
    {
        let resumed = batch
            .as_ref()
            .is_some_and(super::LocalQosBatchPermit::resume);
        if !resumed {
            if let Some(old) = batch.take() {
                old.finish_with_outcome(true);
            }
            match scheduler.try_start_batch_at(unit(), Instant::now()) {
                Ok(admitted) => {
                    admissions += 1;
                    batch = Some(admitted);
                }
                Err(_) => continue,
            }
        }
        assert_eq!(running.fetch_add(1, Ordering::SeqCst), 0);
        owner_units += 1;
        running.fetch_sub(1, Ordering::SeqCst);
        batch.as_ref().unwrap().idle();
        thread::yield_now();
    }
    stop.store(true, Ordering::SeqCst);
    competitor.join().unwrap();
    drop(batch);
    assert!(admissions >= 1);
    assert!(owner_units >= 1_000);
    assert!(competitor_admitted.load(Ordering::SeqCst) >= 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
}
