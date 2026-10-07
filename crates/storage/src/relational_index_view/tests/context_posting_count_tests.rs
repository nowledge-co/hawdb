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

use super::posting_count_tests::corrupt_base_root;
use super::*;
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};

fn uncached_view(fixture: &Fixture) -> Arc<RelationalIndexReadView> {
    Arc::new(RelationalIndexReadView::from_base(
        RelationalIndexShadowReader::open(
            &fixture.0,
            1,
            40,
            RelationalIndexShadowConfig::default(),
        )
        .unwrap(),
    ))
}

fn count(
    context: &RelationalIndexReadContext,
    target: RelationalIndexReadTarget<'_>,
) -> Result<(u64, RelationalIndexReadViewReport), RelationalIndexShadowError> {
    context
        .count_exact_postings(target, TABLE, INDEX, &key(&[0, 0]), Default::default())
        .unwrap()
}

#[test]
fn metadata_cache_charges_once_and_rechecks_selected_epoch() {
    let (fixture, _, _, _) = Fixture::open();
    let base = uncached_view(&fixture);
    let context = RelationalIndexReadContext::new(Default::default());
    let target = RelationalIndexReadTarget::View(&base);
    let (rows, first) = count(&context, target).unwrap();
    assert_eq!(rows, 1);
    let remaining = context.remaining_limits().unwrap();
    let (rows, cached) = count(&context, target).unwrap();
    assert_eq!(rows, 1);
    assert_eq!(context.remaining_limits().unwrap(), remaining);
    assert_eq!(cached.base_generation, first.base_generation);
    assert_eq!(cached.visible_commit_epoch, first.visible_commit_epoch);
    let RelationalIndexReadViewBackendReport::Base(cached) = cached.backend else {
        panic!("base")
    };
    assert_eq!(cached.pages_read, 0);
    assert_eq!(cached.file_bytes_read, 0);
    let next = base.advance(41, None, Default::default()).unwrap();
    let (rows, next_report) = count(&context, RelationalIndexReadTarget::View(&next)).unwrap();
    assert_eq!(rows, 1);
    assert_eq!(next_report.visible_commit_epoch, 41);
    let RelationalIndexReadViewBackendReport::Base(next_report) = next_report.backend else {
        panic!("base")
    };
    assert!(
        next_report.pages_read > 0,
        "an earlier identity cannot serve the new view"
    );
    let remaining = context.remaining_limits().unwrap();
    assert_eq!(
        count(&context, RelationalIndexReadTarget::View(&next))
            .unwrap()
            .0,
        1
    );
    assert_eq!(context.remaining_limits().unwrap(), remaining);
}

#[test]
fn cancellation_before_metadata_admits_no_read_of_unknown_damage() {
    let (fixture, _, _, _) = Fixture::open();
    let base = uncached_view(&fixture);
    corrupt_base_root(&fixture, &base);
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let context = RelationalIndexReadContext::with_task(
        Default::default(),
        RuntimeTaskContext::without_deadline(cancellation),
    );
    assert!(
        matches!(count(&context, RelationalIndexReadTarget::View(&base)),
        Err(RelationalIndexShadowError::Admission(message)) if message.contains("cancelled"))
    );
    assert!(!base.is_poisoned());
    assert_eq!(
        context.remaining_limits().unwrap(),
        RelationalIndexReadLimits::default()
    );
    assert!(matches!(
        base.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), Default::default()),
        Some(Err(RelationalIndexShadowError::Corrupt(_)))
    ));
}

#[test]
fn cached_metadata_rechecks_cancellation_and_known_poison() {
    let (fixture, _, _, _) = Fixture::open();
    let base = uncached_view(&fixture);
    let cancellation = RuntimeCancellationToken::new();
    let context = RelationalIndexReadContext::with_task(
        Default::default(),
        RuntimeTaskContext::without_deadline(cancellation.clone()),
    );
    let target = RelationalIndexReadTarget::View(&base);
    assert_eq!(count(&context, target).unwrap().0, 1);
    let remaining = context.remaining_limits().unwrap();
    cancellation.cancel();
    assert!(matches!(count(&context, target),
        Err(RelationalIndexShadowError::Admission(message)) if message.contains("cancelled")));
    assert_eq!(context.remaining_limits().unwrap(), remaining);
    assert!(!base.is_poisoned());
    let healthy_task = RelationalIndexReadContext::new(Default::default());
    assert_eq!(count(&healthy_task, target).unwrap().0, 1);
    corrupt_base_root(&fixture, &base);
    assert!(matches!(
        base.visit_prefix_entries(TABLE, INDEX, &key(&[0]), Default::default(), |_, _| true),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(base.is_poisoned());
    for key in [key(&[0, 0]), key(&[0])] {
        assert!(matches!(
            context.count_exact_postings(target, TABLE, INDEX, &key, Default::default()),
            Some(Err(RelationalIndexShadowError::Corrupt(_)))
        ));
        assert!(matches!(
            healthy_task.count_exact_postings(target, TABLE, INDEX, &key, Default::default()),
            Some(Err(RelationalIndexShadowError::Corrupt(_)))
        ));
    }
}

#[test]
fn transaction_metadata_cache_and_rows_share_both_ledgers() {
    let (fixture, _, _, _) = Fixture::open();
    let base = uncached_view(&fixture);
    let (_, report) = base
        .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), Default::default())
        .unwrap()
        .unwrap();
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        panic!("base")
    };
    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(2 * report.pages_read).unwrap(),
        ..Default::default()
    };
    let transaction = RelationalTransactionIndexView::new(base.clone(), Default::default(), limits);
    let context = RelationalIndexReadContext::new(limits);
    let target = RelationalIndexReadTarget::Transaction(&transaction);
    assert_eq!(count(&context, target).unwrap().0, 1);
    let remaining = context.remaining_limits().unwrap();
    assert_eq!(count(&context, target).unwrap().0, 1);
    assert_eq!(context.remaining_limits().unwrap(), remaining);
    let mut rows = Vec::new();
    context
        .visit_prefix_entries(target, TABLE, INDEX, &key(&[0, 0]), limits, |_, row| {
            rows.push(row.clone());
            true
        })
        .unwrap();
    assert_eq!(rows, vec![key(&[0])]);
    assert!(context.remaining_limits().is_err());
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits),
        Some(Err(RelationalIndexShadowError::Admission(_)))
    ));
    assert!(!base.is_poisoned());
}

#[test]
fn metadata_cache_binds_equal_identity_to_the_actual_reader() {
    let (fixture, _, _, state) = Fixture::open();
    let base = uncached_view(&fixture);
    let directory = fixture.0.join("independent-base");
    std::fs::create_dir(&directory).unwrap();
    let state = state
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: TABLE.into(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::BigInt(1000),
                        RelationalValue::BigInt(0),
                        RelationalValue::BigInt(0),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            Default::default(),
            Default::default(),
        )
        .unwrap();
    RelationalIndexShadowWriter::new(Default::default())
        .publish(&directory, &state, 1, 40, None)
        .unwrap();
    let other = RelationalIndexReadView::from_base(
        RelationalIndexShadowReader::open(&directory, 1, 40, Default::default()).unwrap(),
    );
    assert_eq!(base.identity(), other.identity());
    assert_eq!(
        other
            .count_exact_postings(TABLE, INDEX, &key(&[0, 0]), Default::default())
            .unwrap()
            .unwrap()
            .0,
        2,
        "the independently published source has two matching row locators"
    );
    let context = RelationalIndexReadContext::new(Default::default());
    assert_eq!(
        count(&context, RelationalIndexReadTarget::View(&base))
            .unwrap()
            .0,
        1
    );
    let (rows, report) = count(&context, RelationalIndexReadTarget::View(&other)).unwrap();
    assert_eq!(
        rows, 2,
        "equal schema/epoch identities cannot authorize cross-reader reuse"
    );
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        panic!("base")
    };
    assert!(report.pages_read > 0);
    let remaining = context.remaining_limits().unwrap();
    assert_eq!(
        count(&context, RelationalIndexReadTarget::View(&other))
            .unwrap()
            .0,
        2
    );
    assert_eq!(context.remaining_limits().unwrap(), remaining);
    let (rows, report) = count(&context, RelationalIndexReadTarget::View(&base)).unwrap();
    assert_eq!(rows, 1);
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        panic!("base")
    };
    assert!(
        report.pages_read > 0,
        "switching sources drops the previous cache"
    );
}
