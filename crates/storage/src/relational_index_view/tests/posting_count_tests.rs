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
use crate::relational::{
    relational_index_shadow_artifact_file, RelationalIndexRecoveryBuilder,
    RelationalIndexRecoveryConfig, RelationalRecoveryFence, RelationalRecoverySourceIdentity,
};
use std::io::{Read, Seek, SeekFrom, Write};

fn count(view: &RelationalIndexReadView, values: &[i64]) -> (u64, RelationalIndexReadViewReport) {
    view.count_exact_postings(
        TABLE,
        INDEX,
        &key(values),
        RelationalIndexReadLimits::default(),
    )
    .unwrap()
    .unwrap()
}

pub(super) fn corrupt_base_root(fixture: &Fixture, base: &RelationalIndexReadView) {
    let RelationalIndexReadBackend::Base(reader) = &base.backend else {
        unreachable!();
    };
    let root = reader.manifest().root(TABLE, INDEX).unwrap();
    let offset = (root.root_page_id.get() - 1) * reader.manifest().page_bytes;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.0.join(relational_index_shadow_artifact_file(1)))
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
}

fn assert_identity(report: &RelationalIndexReadViewReport, view: &RelationalIndexReadView) {
    let identity = view.identity();
    assert_eq!(report.base_generation, identity.base_generation);
    assert_eq!(report.delta_generation, identity.delta_generation);
    assert_eq!(report.base_commit_epoch, identity.base_commit_epoch);
    assert_eq!(report.visible_commit_epoch, identity.visible_commit_epoch);
    assert_eq!(report.root_set_digest, identity.root_set_digest.to_string());
    assert_eq!(report.rows_visited, 0);
    assert_eq!(report.live_batches_visited, 0);
    assert_eq!(report.live_entries_visited, 0);
    assert_eq!(report.live_bytes_visited, 0);
    let RelationalIndexReadViewBackendReport::Base(backend) = &report.backend else {
        panic!("count must report eligible checkpoint metadata");
    };
    assert_eq!(backend.rows_visited, 0);
    assert!(backend.pages_read > 0);
}

#[test]
fn selected_posting_count_keeps_pinned_identity_and_declines_matching_live_changes() {
    let (_fixture, base, mut oracle, mut state) = Fixture::open();
    let limits = RelationalIndexReadLimits::default();
    let (rows, report) = count(&base, &[0, 0]);
    assert_eq!(rows, 1);
    assert_identity(&report, &base);
    assert_eq!(count(&base, &[99, 99]).0, 0);
    assert!(base
        .count_exact_postings(TABLE, INDEX, &key(&[0]), limits)
        .is_none());
    assert!(matches!(
        base.count_exact_postings(TABLE, "missing_required_root", &key(&[0]), limits),
        Some(Err(RelationalIndexShadowError::MissingIndex { .. }))
    ));

    let graph_only = base
        .advance(41, None, RelationalIndexChangeCaptureLimits::default())
        .unwrap();
    let (rows, report) = count(&graph_only, &[0, 0]);
    assert_eq!(rows, 1);
    assert_identity(&report, &graph_only);
    assert_eq!(report.base_commit_epoch, 40);
    assert_eq!(report.visible_commit_epoch, 41);

    // This captured primary-root change leaves the secondary partition untouched.
    let unrelated_change = RelationalIndexChange {
        table: TABLE.into(),
        index: crate::relational::RELATIONAL_PRIMARY_INDEX_NAME.into(),
        index_key: key(&[9]),
        primary_key: key(&[9]),
        kind: RelationalIndexChangeKind::Insert,
    };
    let encoded_bytes = unrelated_change.estimated_encoded_bytes().unwrap();
    let unrelated = base
        .advance(
            41,
            Some(RelationalIndexChangeCapture::Captured {
                changes: vec![unrelated_change],
                encoded_bytes,
            }),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .unwrap();
    let (rows, report) = count(&unrelated, &[0, 0]);
    assert_eq!(rows, 1);
    assert_identity(&report, &unrelated);
    assert_eq!(unrelated.live_entry_count(), 1);

    let capture = replace(&mut state, &mut oracle, 0, None);
    let changed = base
        .advance(
            41,
            Some(capture),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .unwrap();
    assert!(changed
        .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
        .is_none());
    let mut actual = Vec::new();
    changed
        .visit_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits, |row| {
            actual.push(row.clone());
            true
        })
        .unwrap();
    assert!(
        actual.is_empty(),
        "the selected view really removed this posting"
    );
    assert_eq!(
        state.index_prefix_cardinality_at_most(TABLE, INDEX, &key(&[0, 0]), 100),
        Some(0)
    );
    assert_eq!(
        count(&base, &[0, 0]).0,
        1,
        "the pinned earlier view stays unchanged"
    );
}

#[test]
fn recovered_posting_count_declines_the_checkpoint_count() {
    let (fixture, base, mut oracle, mut state) = Fixture::open();
    let capture = replace(&mut state, &mut oracle, 0, None);
    let config = RelationalIndexRecoveryConfig::default();
    let mut builder = RelationalIndexRecoveryBuilder::new(&fixture.0, 1, 40, config).unwrap();
    builder.record(41, capture).unwrap();
    builder.finish(41).unwrap();
    let reader = RelationalIndexRecoveryReader::open_latest(
        &fixture.0,
        RelationalRecoveryFence::new(41, RelationalRecoverySourceIdentity::for_test(40, 41)),
        RelationalIndexShadowConfig::default(),
        config,
    )
    .unwrap();
    let recovered = RelationalIndexReadView::from_recovered(reader);
    assert_eq!(recovered.kind(), RelationalIndexReadViewKind::Recovered);
    assert!(recovered
        .count_exact_postings(
            TABLE,
            INDEX,
            &key(&[0, 0]),
            RelationalIndexReadLimits::default()
        )
        .is_none());
    let mut actual = Vec::new();
    recovered
        .visit_exact_postings(
            TABLE,
            INDEX,
            &key(&[0, 0]),
            RelationalIndexReadLimits::default(),
            |row| {
                actual.push(row.clone());
                true
            },
        )
        .unwrap();
    assert!(actual.is_empty());
    assert_eq!(count(&base, &[0, 0]).0, 1);
    corrupt_base_root(&fixture, &base);
    let limits = RelationalIndexReadLimits::default();
    assert!(recovered
        .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
        .is_none());
    assert!(
        !recovered.is_poisoned(),
        "ineligible count does not inspect payload"
    );
    assert!(matches!(
        recovered.visit_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits, |_| true),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(recovered.is_poisoned());
    assert!(matches!(
        recovered.count_exact_postings(TABLE, INDEX, &key(&[0]), limits),
        Some(Err(RelationalIndexShadowError::Corrupt(_)))
    ));
    drop(recovered);
    drop(base);
}

#[test]
fn transaction_posting_count_shares_row_probe_admission_and_declines_private_changes() {
    let (_fixture, base, mut oracle, mut state) = Fixture::open();
    let (_, report) = count(&base, &[0, 0]);
    let RelationalIndexReadViewBackendReport::Base(backend) = report.backend else {
        unreachable!();
    };
    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(2 * backend.pages_read).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let mut transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    let (rows, report) = transaction
        .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
        .unwrap()
        .unwrap();
    assert_eq!(rows, 1);
    assert_identity(&report, &base);
    let mut actual = Vec::new();
    let report = transaction
        .visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, row| {
            actual.push(row.clone());
            true
        })
        .unwrap();
    assert_eq!(actual, vec![key(&[0])]);
    assert_eq!(report.rows_visited, 1);
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits),
        Some(Err(RelationalIndexShadowError::Admission(_)))
    ));
    assert!(
        transaction
            .count_exact_postings(TABLE, INDEX, &key(&[0]), limits)
            .is_none(),
        "ineligible partial keys do not consume a new budget"
    );
    assert!(!base.is_poisoned());

    let capture = replace(&mut state, &mut oracle, 0, None);
    let mut private = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        RelationalIndexReadLimits::default(),
    );
    private.append(capture.clone()).unwrap();
    assert!(private
        .count_exact_postings(
            TABLE,
            INDEX,
            &key(&[0, 0]),
            RelationalIndexReadLimits::default()
        )
        .is_none());
    let mut private_rows = Vec::new();
    private
        .visit_prefix_entries(
            TABLE,
            INDEX,
            &key(&[0, 0]),
            RelationalIndexReadLimits::default(),
            |_, row| {
                private_rows.push(row.clone());
                true
            },
        )
        .unwrap();
    assert!(
        private_rows.is_empty(),
        "private row probes reflect the deletion"
    );
    transaction.append(capture).unwrap();
    assert!(transaction
        .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
        .is_none());
    assert_eq!(count(&base, &[0, 0]).0, 1);
}

#[test]
fn known_poison_fails_before_live_and_private_posting_count_eligibility() {
    let (fixture, base, mut oracle, mut state) = Fixture::open();
    let capture = replace(&mut state, &mut oracle, 0, None);
    let live = base
        .advance(
            41,
            Some(capture.clone()),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .unwrap();
    let mut transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        RelationalIndexReadLimits::default(),
    );
    transaction.append(capture).unwrap();
    corrupt_base_root(&fixture, &base);
    let limits = RelationalIndexReadLimits::default();
    assert!(matches!(
        base.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits),
        Some(Err(RelationalIndexShadowError::Corrupt(_)))
    ));
    assert!(base.is_poisoned());
    for view in [&*base, &live] {
        assert!(matches!(
            view.count_exact_postings(TABLE, INDEX, &key(&[0]), limits),
            Some(Err(RelationalIndexShadowError::Corrupt(_)))
        ));
    }
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0]), limits),
        Some(Err(RelationalIndexShadowError::Corrupt(_)))
    ));
}

fn failed_count_retry(retry_rows: bool) {
    let (_fixture, base, _, _) = Fixture::open();
    let (_, report) = count(&base, &[0, 0]);
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        unreachable!();
    };
    assert!(
        report.pages_read >= 2,
        "one-page admission fails after reading the root"
    );
    let limits = RelationalIndexReadLimits::default();
    let limited = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(1).unwrap(),
        ..limits
    };
    let transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limited),
        Some(Err(RelationalIndexShadowError::Admission(_)))
    ));
    let retry = if retry_rows {
        transaction
            .visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, _| true)
            .map(|_| ())
    } else {
        transaction
            .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
            .unwrap()
            .map(|_| ())
    };
    assert!(
        matches!(retry, Err(RelationalIndexShadowError::Admission(_))),
        "an admitted failed count must prevent further unaccounted transaction reads: {retry:?}"
    );
    assert!(transaction
        .count_exact_postings(TABLE, INDEX, &key(&[0]), limits)
        .is_none());
    assert!(
        !base.is_poisoned(),
        "a budget failure does not poison healthy backing pages"
    );
    assert_eq!(
        count(&base, &[0, 0]).0,
        1,
        "closure belongs only to the transaction ledger"
    );
}

#[test]
fn failed_count_closes_transaction_count_admission() {
    failed_count_retry(false);
}

#[test]
fn failed_count_closes_transaction_row_admission() {
    failed_count_retry(true);
}

#[derive(Clone, Copy)]
enum FailedProbeKind {
    Batch,
    Range,
    TransactionConstraint,
    AuthoritativeConstraint,
}

fn failed_probe_retry(kind: FailedProbeKind) {
    let (fixture, base, _, _) = Fixture::open();
    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(1).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    let authoritative = AuthoritativeRelationalConstraintIndex::new(base.clone(), limits);
    let probe = || -> Result<(), RelationalError> {
        match kind {
            FailedProbeKind::Batch => transaction
                .visit_prefix_entries_many(TABLE, INDEX, &[key(&[0, 0])], limits, |_, _| true)
                .map(|_| ())
                .map_err(authoritative::map_constraint_read_error),
            FailedProbeKind::Range => transaction
                .visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, _| true)
                .map(|_| ())
                .map_err(authoritative::map_constraint_read_error),
            FailedProbeKind::TransactionConstraint => {
                transaction.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |_| true)
            }
            FailedProbeKind::AuthoritativeConstraint => {
                authoritative.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |_| true)
            }
        }
    };
    assert!(matches!(probe(), Err(RelationalError::Admission(_))));
    assert!(!base.is_poisoned());
    // Unknown payload corruption makes a second backing read observable: a
    // closed ledger must reject before touching it, leaving reader health intact.
    corrupt_base_root(&fixture, &base);
    let retry = probe();
    assert!(matches!(retry, Err(RelationalError::Admission(_))),
        "failed row/constraint read must close its shared admission before another payload read: {retry:?}");
    assert!(
        !base.is_poisoned(),
        "retry must not inspect the unknown damaged page"
    );
}

#[test]
fn failed_batch_probe_closes_transaction_admission() {
    failed_probe_retry(FailedProbeKind::Batch);
}

#[test]
fn failed_range_probe_closes_transaction_admission() {
    failed_probe_retry(FailedProbeKind::Range);
}

#[test]
fn failed_transaction_constraint_probe_closes_admission() {
    failed_probe_retry(FailedProbeKind::TransactionConstraint);
}

#[test]
fn failed_authoritative_constraint_probe_closes_admission() {
    failed_probe_retry(FailedProbeKind::AuthoritativeConstraint);
}

#[test]
fn unwound_index_probe_closes_only_transaction_admission() {
    let (_fixture, base, _, _) = Fixture::open();
    let limits = RelationalIndexReadLimits::default();
    let transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transaction.visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, _| {
            panic!("owned fixture callback unwinds after payload admission");
        })
    }));
    assert!(unwind.is_err());
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits),
        Some(Err(RelationalIndexShadowError::Admission(_)))
    ));
    assert!(!base.is_poisoned());
    assert_eq!(count(&base, &[0, 0]).0, 1);
}

#[derive(Clone, Copy)]
enum NestedProbeKind {
    Range,
    Batch,
    TransactionConstraint,
    AuthoritativeConstraint,
}

fn nested_probe_admission(
    kind: NestedProbeKind,
    allow_nested: bool,
    file_budget: bool,
    partial_nested: bool,
) {
    let (fixture, base, _, _) = Fixture::open();
    let (_, report) = count(&base, &[0, 0]);
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        unreachable!();
    };
    assert!(report.pages_read >= 2);
    let probes = if allow_nested { 2 } else { 1 };
    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(if file_budget {
            RelationalIndexReadLimits::default().max_pages.get()
        } else {
            probes * report.pages_read + usize::from(partial_nested)
        })
        .unwrap(),
        max_file_bytes: if file_budget {
            probes * report.file_bytes_read
        } else {
            RelationalIndexReadLimits::default().max_file_bytes
        },
        ..RelationalIndexReadLimits::default()
    };
    assert!(
        report.file_bytes_read > 0,
        "owned fixture uses cold file reads"
    );
    let transaction = RelationalTransactionIndexView::new(
        base.clone(),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    let authoritative = AuthoritativeRelationalConstraintIndex::new(base.clone(), limits);
    let mut visited = Vec::new();
    let mut callback = |primary_key: &RelationalKey| {
        visited.push(primary_key.clone());
        if !allow_nested && !partial_nested {
            // The outer path has already consumed its full allowance. Make an
            // unadmitted second payload read distinguishable from rejection.
            corrupt_base_root(&fixture, &base);
        }
        let nested = match kind {
            NestedProbeKind::Range => transaction
                .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits)
                .unwrap()
                .map(|(rows, _)| assert_eq!(rows, 1))
                .map_err(authoritative::map_constraint_read_error),
            NestedProbeKind::Batch => transaction
                .visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, _| true)
                .map(|_| ())
                .map_err(authoritative::map_constraint_read_error),
            NestedProbeKind::TransactionConstraint => {
                transaction.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |_| true)
            }
            NestedProbeKind::AuthoritativeConstraint => {
                authoritative.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |_| true)
            }
        };
        if allow_nested {
            assert!(
                nested.is_ok(),
                "valid nested reads retain streaming semantics: {nested:?}"
            );
        } else {
            assert!(
                matches!(nested, Err(RelationalError::Admission(_))),
                "in-flight work must consume allowance before a nested payload read: {nested:?}"
            );
        }
        true
    };
    let outer = match kind {
        NestedProbeKind::Range => transaction
            .visit_prefix_entries(TABLE, INDEX, &key(&[0, 0]), limits, |_, row| callback(row))
            .map(|_| ())
            .map_err(authoritative::map_constraint_read_error),
        NestedProbeKind::Batch => transaction
            .visit_prefix_entries_many(TABLE, INDEX, &[key(&[0, 0])], limits, |_, row| {
                callback(row)
            })
            .map(|_| ())
            .map_err(authoritative::map_constraint_read_error),
        NestedProbeKind::TransactionConstraint => {
            transaction.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut callback)
        }
        NestedProbeKind::AuthoritativeConstraint => {
            authoritative.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut callback)
        }
    };
    assert_eq!(visited, vec![key(&[0])]);
    if allow_nested {
        assert!(
            outer.is_ok(),
            "complete reports must avoid double charging: {outer:?}"
        );
    } else if file_budget || partial_nested {
        assert!(matches!(outer, Err(RelationalError::Admission(_))),
            "an outer attempt cannot settle successfully after nested failure closes the ledger: {outer:?}");
    } else {
        assert!(
            outer.is_ok(),
            "rejection before a nested attempt preserves the admitted outer read: {outer:?}"
        );
    }
    assert!(
        !base.is_poisoned(),
        "rejected nested reads never inspect payload"
    );
}

#[test]
fn nested_count_observes_inflight_page_admission() {
    nested_probe_admission(NestedProbeKind::Range, false, false, false);
}

#[test]
fn nested_count_observes_inflight_file_admission() {
    nested_probe_admission(NestedProbeKind::Range, false, true, false);
}

#[test]
fn nested_batch_row_probe_observes_inflight_admission() {
    nested_probe_admission(NestedProbeKind::Batch, false, false, false);
}

#[test]
fn nested_transaction_constraint_observes_inflight_admission() {
    nested_probe_admission(NestedProbeKind::TransactionConstraint, false, false, false);
}

#[test]
fn nested_authoritative_constraint_observes_inflight_admission() {
    nested_probe_admission(
        NestedProbeKind::AuthoritativeConstraint,
        false,
        false,
        false,
    );
}

#[test]
fn partial_nested_failure_prevents_outer_settlement() {
    nested_probe_admission(NestedProbeKind::Range, false, false, true);
}

#[test]
fn nested_index_reads_with_enough_allowance_keep_complete_results() {
    for kind in [
        NestedProbeKind::Range,
        NestedProbeKind::Batch,
        NestedProbeKind::TransactionConstraint,
        NestedProbeKind::AuthoritativeConstraint,
    ] {
        nested_probe_admission(kind, true, false, false);
    }
}
