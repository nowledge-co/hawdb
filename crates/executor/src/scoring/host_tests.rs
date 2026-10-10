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

use super::*;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::RuntimeCancellationToken;
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
enum Behavior {
    Complete,
    Incomplete,
    Nonfinite,
    Error,
    Cancel,
    ChangeIdentity,
    Panic,
}

struct CohortScorer {
    behavior: Behavior,
    calls: usize,
    changed: bool,
    cancellation: Option<RuntimeCancellationToken>,
    scratch_bytes: usize,
}

impl CohortScorer {
    fn new(behavior: Behavior) -> Self {
        Self {
            behavior,
            calls: 0,
            changed: false,
            cancellation: None,
            scratch_bytes: 0,
        }
    }
}

impl HostScorer for CohortScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new(
            "cohort",
            if self.changed { "v2" } else { "v1" },
            NonZeroU64::MIN,
        )
        .unwrap()
    }

    fn score_batch(&mut self, request: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls += 1;
        assert_eq!(request.reference_time_millis, 1_234);
        assert_eq!(scores.len(), request.features.len());
        assert!(scores.iter().all(|score| score.is_nan()));
        let _scratch = request.scratch_account.reserve(self.scratch_bytes)?;
        let maximum = request
            .features
            .iter()
            .map(|feature| feature.numeric_property("authority").unwrap())
            .fold(0.0, f64::max);
        for (index, (feature, score)) in request.features.iter().zip(scores.iter_mut()).enumerate()
        {
            if matches!(self.behavior, Behavior::Incomplete) && index == 1 {
                continue;
            }
            *score = feature.numeric_property("authority").unwrap() / maximum;
        }
        match self.behavior {
            Behavior::Nonfinite => scores[1] = f64::INFINITY,
            Behavior::Error => {
                return Err(HawDBError::Execution("synthetic scorer failure".into()))
            }
            Behavior::Cancel => {
                assert!(self.cancellation.as_ref().unwrap().cancel());
            }
            Behavior::ChangeIdentity => self.changed = true,
            Behavior::Panic => panic!("synthetic scorer panic"),
            Behavior::Complete | Behavior::Incomplete => {}
        }
        Ok(())
    }
}

fn rows() -> Vec<Binding> {
    [1.0, 4.0, 100.0]
        .into_iter()
        .map(|authority| {
            Binding::values(BTreeMap::from([(
                "authority".into(),
                Value::Float(authority),
            )]))
        })
        .collect()
}

fn request<'a>(
    features: &'a [&'a dyn ScoringFeatureSource],
    account: &'a crate::QueryMemoryAccount,
    task_context: Option<&'a RuntimeTaskContext>,
) -> HostScorerBatch<'a> {
    HostScorerBatch {
        features,
        reference_time_millis: 1_234,
        task_context,
        scratch_account: account,
    }
}

#[test]
fn callback_observes_the_full_cohort_and_scores_keep_their_reservation() {
    let rows = rows();
    let values: Vec<_> = rows
        .iter()
        .map(|row| BindingScoreFeatures::new(row, "score"))
        .collect();
    let features: Vec<&dyn ScoringFeatureSource> = values.iter().map(|value| value as _).collect();
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(40).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "host",
        NonZeroUsize::new(40).unwrap(),
    );
    let mut scorer = CohortScorer::new(Behavior::Complete);
    scorer.scratch_bytes = 8;
    let output = execute_host_scorer(
        &mut scorer,
        request(&features, &account, None),
        NonZeroUsize::new(3).unwrap(),
    )
    .unwrap();
    // The last candidate establishes the cohort maximum for all earlier rows.
    assert_eq!(output.scores(), &[0.01, 0.04, 1.0]);
    assert_eq!(scorer.calls, 1);
    assert_eq!(ledger.snapshot().peak_bytes, 40);
    assert_eq!(ledger.snapshot().used_bytes, 24);
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn candidate_and_output_limits_reject_before_callback_and_release_admission() {
    let rows = rows();
    let values: Vec<_> = rows
        .iter()
        .map(|row| BindingScoreFeatures::new(row, "score"))
        .collect();
    let features: Vec<&dyn ScoringFeatureSource> = values.iter().map(|value| value as _).collect();
    for (bytes, max_rows, expected) in [(32, 2, "candidate limit"), (31, 3, "memory")] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(bytes).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "host",
            NonZeroUsize::new(bytes).unwrap(),
        );
        let mut scorer = CohortScorer::new(Behavior::Complete);
        let error = execute_host_scorer(
            &mut scorer,
            request(&features, &account, None),
            NonZeroUsize::new(max_rows).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(scorer.calls, 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn incomplete_nonfinite_callback_error_and_identity_change_emit_no_scores() {
    let rows = rows();
    let values: Vec<_> = rows
        .iter()
        .map(|row| BindingScoreFeatures::new(row, "score"))
        .collect();
    let features: Vec<&dyn ScoringFeatureSource> = values.iter().map(|value| value as _).collect();
    for (behavior, expected) in [
        (Behavior::Incomplete, "candidate 1"),
        (Behavior::Nonfinite, "candidate 1"),
        (Behavior::Error, "synthetic scorer failure"),
        (Behavior::ChangeIdentity, "identity changed"),
    ] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(32).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "host",
            NonZeroUsize::new(32).unwrap(),
        );
        let mut scorer = CohortScorer::new(behavior);
        let error = execute_host_scorer(
            &mut scorer,
            request(&features, &account, None),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(scorer.calls, 1);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn cancellation_before_and_during_callback_releases_all_scores() {
    let rows = rows();
    let values: Vec<_> = rows
        .iter()
        .map(|row| BindingScoreFeatures::new(row, "score"))
        .collect();
    let features: Vec<&dyn ScoringFeatureSource> = values.iter().map(|value| value as _).collect();
    for precancelled in [false, true] {
        let token = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        if precancelled {
            assert!(token.cancel());
        }
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(32).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "host",
            NonZeroUsize::new(32).unwrap(),
        );
        let mut scorer = CohortScorer::new(Behavior::Cancel);
        scorer.cancellation = Some(token);
        let result = execute_host_scorer(
            &mut scorer,
            request(&features, &account, Some(&task)),
            NonZeroUsize::new(3).unwrap(),
        );
        assert!(result.is_err());
        assert_eq!(scorer.calls, usize::from(!precancelled));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn callback_unwind_releases_owned_score_and_identity_buffers() {
    let rows = rows();
    let values: Vec<_> = rows
        .iter()
        .map(|row| BindingScoreFeatures::new(row, "score"))
        .collect();
    let features: Vec<&dyn ScoringFeatureSource> = values.iter().map(|value| value as _).collect();
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(32).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "host",
        NonZeroUsize::new(32).unwrap(),
    );
    let mut scorer = CohortScorer::new(Behavior::Panic);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        execute_host_scorer(
            &mut scorer,
            request(&features, &account, None),
            NonZeroUsize::new(3).unwrap(),
        )
    }));
    assert!(result.is_err());
    assert_eq!(scorer.calls, 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn empty_cohort_allocates_no_scores_and_skips_callback() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::MIN);
    let account = ledger.account(QueryMemoryClass::BlockingState, "host", NonZeroUsize::MIN);
    let mut scorer = CohortScorer::new(Behavior::Complete);
    let output =
        execute_host_scorer(&mut scorer, request(&[], &account, None), NonZeroUsize::MIN).unwrap();
    assert!(output.scores().is_empty());
    assert_eq!(scorer.calls, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
