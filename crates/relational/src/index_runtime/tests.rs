use super::*;
use skein_storage::{
    RelationalIndexRangeScan, RelationalIndexReadReport, RelationalIndexRecoveryReadReport,
    RelationalIndexScanDirection, RelationalValue,
};
use std::sync::Arc;

mod fixtures;
use fixtures::{expected, key, replace, report, Fixture, Outcome, Reader, INDEX, TABLE};

type Mode<'a> = RelationalIndexReadMode<'a, Reader>;
type Runtime<'a> = RelationalIndexRuntime<'a, Reader>;
type Entry = (RelationalKey, RelationalKey);

fn scan(prefix: &[i64], backward: bool, bound: Option<&[i64]>) -> RelationalIndexRangeScan {
    RelationalIndexRangeScan {
        prefix: key(prefix),
        exclusive_bound: bound.map(key),
        direction: if backward {
            RelationalIndexScanDirection::Backward
        } else {
            RelationalIndexScanDirection::Forward
        },
    }
}

fn visit(
    runtime: &Runtime<'_>,
    state: &RelationalState,
    kind: usize,
    mut callback: impl FnMut(&RelationalKey, &RelationalKey) -> Result<bool>,
) -> Result<bool> {
    match kind {
        0 => runtime.visit_prefix_entries(state, TABLE, INDEX, &key(&[1]), callback),
        1 => runtime.visit_prefix_entries_many(
            state,
            TABLE,
            INDEX,
            &[key(&[1]), key(&[1])],
            |_, index, primary| callback(index, primary),
        ),
        2 => runtime.visit_range_entries(state, TABLE, INDEX, &scan(&[1], false, None), callback),
        _ => unreachable!(),
    }
}

#[test]
fn all_modes_preserve_prefix_batch_range_order_and_stop() {
    let fixture = Fixture::new();
    for mode in 0..6 {
        for backward in [false, true] {
            for bound in [None, Some(&[1, 2][..])] {
                for stop in [false, true] {
                    let transaction = fixture.transaction();
                    let runtime =
                        Runtime::new(fixture.mode(mode, &transaction), Default::default());
                    let scan = scan(&[1], backward, bound);
                    let mut want = expected(&fixture.oracle, &scan);
                    if stop {
                        want.truncate(1);
                    }
                    let mut actual = Vec::new();
                    let complete = runtime
                        .visit_range_entries(
                            &fixture.state,
                            TABLE,
                            INDEX,
                            &scan,
                            |index, primary| {
                                actual.push((index.clone(), primary.clone()));
                                Ok(!stop)
                            },
                        )
                        .unwrap();
                    assert_eq!(actual, want, "mode={mode}, scan={scan:?}");
                    assert_eq!(complete, !stop || want.is_empty());
                    let evidence = runtime.evidence();
                    match mode {
                        0 | 1 => assert!(evidence.is_empty()),
                        4 => assert_eq!(
                            evidence[0].fallback_reasons,
                            BTreeSet::from(["transaction_workspace"])
                        ),
                        _ => {
                            assert_eq!(evidence[0].range_lookups, 1);
                            assert_eq!(evidence[0].backward_lookups, usize::from(backward));
                            assert_eq!(
                                evidence[0].exclusive_seek_lookups,
                                usize::from(bound.is_some())
                            );
                            assert_eq!(
                                evidence[0].early_stop_lookups,
                                usize::from(stop && !want.is_empty())
                            );
                            assert_eq!(
                                evidence[0].runtime_path(),
                                match mode {
                                    2 => "demand_paged",
                                    3 => "authoritative",
                                    _ => "transaction_workspace",
                                }
                            );
                        }
                    }
                }
            }
        }
        for kind in 0..2 {
            let transaction = fixture.transaction();
            let runtime = Runtime::new(fixture.mode(mode, &transaction), Default::default());
            let mut rows = Vec::new();
            assert!(visit(&runtime, &fixture.state, kind, |index, primary| {
                rows.push((index.clone(), primary.clone()));
                Ok(true)
            })
            .unwrap());
            assert_eq!(rows, expected(&fixture.oracle, &scan(&[1], false, None)));
            if mode == 4 && kind == 1 {
                assert!(runtime.evidence().is_empty());
            }
        }
        let transaction = fixture.transaction();
        let runtime = Runtime::new(fixture.mode(mode, &transaction), Default::default());
        let mut primary = Vec::new();
        runtime
            .visit_prefix(&fixture.state, TABLE, INDEX, &key(&[1]), |key| {
                primary.push(key.clone());
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            primary,
            expected(&fixture.oracle, &scan(&[1], false, None))
                .into_iter()
                .map(|(_, key)| key)
                .collect::<Vec<_>>()
        );
    }
    fixture.remove();
}

#[test]
fn batch_prefixes_validate_width_deduplicate_and_reject_outside_keys() {
    let fixture = Fixture::new();
    for mode in 0..6 {
        let transaction = fixture.transaction();
        let runtime = Runtime::new(fixture.mode(mode, &transaction), Default::default());
        let before = fixture.reader.limits.borrow().len();
        assert!(runtime
            .visit_prefix_entries_many(&fixture.state, TABLE, INDEX, &[], |_, _, _| panic!(
                "empty batch callback"
            ))
            .unwrap());
        assert!(matches!(
            runtime.visit_prefix_entries_many(
                &fixture.state,
                TABLE,
                INDEX,
                &[key(&[1]), key(&[1, 2])],
                |_, _, _| panic!("invalid batch callback")
            ),
            Err(SkeinError::Execution(_))
        ));
        assert_eq!(fixture.reader.limits.borrow().len(), before);
        let mut actual = Vec::new();
        runtime
            .visit_prefix_entries_many(
                &fixture.state,
                TABLE,
                INDEX,
                &[key(&[2]), key(&[1]), key(&[1]), key(&[9])],
                |prefix, index, primary| {
                    assert_eq!(prefix.0, index.0[..1]);
                    actual.push((index.clone(), primary.clone()));
                    Ok(true)
                },
            )
            .unwrap();
        let want = [1, 2, 9]
            .into_iter()
            .flat_map(|bucket| expected(&fixture.oracle, &scan(&[bucket], false, None)))
            .collect::<Vec<_>>();
        assert_eq!(actual, want);
        if matches!(mode, 2 | 3) {
            assert_eq!(
                fixture.reader.batches.borrow().last().unwrap(),
                &vec![key(&[1]), key(&[2]), key(&[9])]
            );
            assert_eq!(runtime.evidence()[0].lookups, 1);
        }
    }
    let reader = Reader::script(Outcome::Success, true);
    let runtime = Runtime::new(Mode::DemandPaged(&reader), Default::default());
    assert!(matches!(
        runtime.visit_prefix_entries_many(
            &fixture.state,
            TABLE,
            INDEX,
            &[key(&[9])],
            |_, _, _| panic!("outside key callback")
        ),
        Err(SkeinError::StorageIntegrity(_))
    ));
    assert!(runtime.evidence().is_empty());
    fixture.remove();
}

#[test]
fn fallback_matrix_never_rescans_after_provisional_output() {
    let fixture = Fixture::new();
    for kind in 0..3 {
        for authoritative in [false, true] {
            for outcome in [
                Outcome::Unavailable,
                Outcome::Admission,
                Outcome::Missing,
                Outcome::Corrupt,
                Outcome::Durability,
                Outcome::Stale,
            ] {
                for emit in [false, true] {
                    if emit && matches!(outcome, Outcome::Unavailable) {
                        continue;
                    }
                    let reader = Reader::script(outcome, emit);
                    let mode = if authoritative {
                        Mode::Authoritative(&reader)
                    } else {
                        Mode::DemandPaged(&reader)
                    };
                    let runtime = Runtime::new(mode, Default::default());
                    let mut rows = Vec::new();
                    let result = visit(&runtime, &fixture.state, kind, |index, primary| {
                        rows.push((index.clone(), primary.clone()));
                        Ok(true)
                    });
                    let fallback = !authoritative
                        && !emit
                        && matches!(
                            outcome,
                            Outcome::Unavailable | Outcome::Admission | Outcome::Missing
                        );
                    if fallback {
                        assert!(result.unwrap());
                        assert_eq!(rows, expected(&fixture.oracle, &scan(&[1], false, None)));
                        let reason = match outcome {
                            Outcome::Unavailable => "read_view_unavailable",
                            Outcome::Admission => "admission_rejected",
                            _ => "missing_index",
                        };
                        assert_eq!(
                            runtime.evidence()[0].fallback_reasons,
                            BTreeSet::from([reason])
                        );
                    } else {
                        match outcome {
                            Outcome::Admission => {
                                assert!(matches!(result, Err(SkeinError::Execution(_))))
                            }
                            _ => assert!(matches!(result, Err(SkeinError::StorageIntegrity(_)))),
                        }
                        assert_eq!(rows.len(), usize::from(emit));
                        assert!(runtime.evidence().is_empty());
                    }
                    assert_eq!(reader.limits.borrow().len(), 1);
                }
            }
        }
    }
    fixture.remove();
}

#[test]
fn callback_errors_precede_backend_results_on_every_probe() {
    let fixture = Fixture::new();
    for kind in 0..3 {
        for outcome in [
            Outcome::Success,
            Outcome::Admission,
            Outcome::Missing,
            Outcome::Corrupt,
            Outcome::Durability,
            Outcome::Stale,
        ] {
            let reader = Reader::script(outcome, true);
            for mode in [Mode::DemandPaged(&reader), Mode::Authoritative(&reader)] {
                let runtime = Runtime::new(mode, Default::default());
                let mut calls = 0;
                let error = visit(&runtime, &fixture.state, kind, |_, _| {
                    calls += 1;
                    Err(SkeinError::Semantic("callback sentinel".into()))
                })
                .unwrap_err();
                assert!(
                    matches!(error, SkeinError::Semantic(message) if message == "callback sentinel")
                );
                assert_eq!(calls, 1);
                assert!(runtime.evidence().is_empty());
            }
        }
        for mode in 0..6 {
            let transaction = fixture.transaction();
            let runtime = Runtime::new(fixture.mode(mode, &transaction), Default::default());
            assert!(
                matches!(visit(&runtime, &fixture.state, kind, |_, _| Err(SkeinError::Semantic("real callback".into()))), Err(SkeinError::Semantic(message)) if message == "real callback")
            );
        }
    }
    fixture.remove();
}

#[test]
fn cumulative_admission_is_shared_across_indexes_and_allows_zero_file_budget() {
    let reader = Reader::script(Outcome::Success, false);
    let runtime = Runtime::new(
        Mode::DemandPaged(&reader),
        RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(5).unwrap(),
            max_rows: NonZeroUsize::new(9).unwrap(),
            max_bytes: NonZeroUsize::new(24).unwrap(),
            max_file_bytes: 4,
            ..Default::default()
        },
    );
    let mut report = report();
    report.backend = RelationalIndexReadViewBackendReport::Base(RelationalIndexReadReport {
        pages_read: 1,
        bytes_read: 3,
        file_bytes_read: 2,
        ..Default::default()
    });
    report.live_bytes_visited = 1;
    report.rows_visited = 2;
    for index in ["z_index", "a_index"] {
        runtime
            .record_success(
                TABLE,
                index,
                &report,
                RelationalIndexProbeSelector::Prefix(&key(&[1])),
            )
            .unwrap();
    }
    let remaining = runtime.remaining_limits().unwrap();
    assert_eq!(
        (
            remaining.max_pages.get(),
            remaining.max_rows.get(),
            remaining.max_bytes.get(),
            remaining.max_file_bytes
        ),
        (3, 5, 16, 0)
    );
    assert_eq!(remaining.max_tree_height, runtime.limits.max_tree_height);
    let evidence = runtime.evidence();
    assert_eq!(
        evidence
            .iter()
            .map(|item| item.index.as_str())
            .collect::<Vec<_>>(),
        vec!["a_index", "z_index"]
    );
    visit(&runtime, &RelationalState::default(), 0, |_, _| {
        panic!("empty script callback")
    })
    .unwrap();
    assert_eq!(reader.limits.borrow().last().unwrap(), &remaining);
    report.rows_visited = 10;
    assert!(matches!(
        runtime.record_success(
            TABLE,
            "a_index",
            &report,
            RelationalIndexProbeSelector::Prefix(&key(&[1]))
        ),
        Err(SkeinError::Execution(_))
    ));
}

#[test]
fn exhausted_query_budget_falls_back_only_for_non_authoritative_modes() {
    let fixture = Fixture::new();
    for kind in 0..3 {
        for resource in 0..4 {
            for authoritative in [false, true] {
                let reader = Reader::script(Outcome::Success, false);
                let mut runtime = Runtime::new(
                    if authoritative {
                        Mode::Authoritative(&reader)
                    } else {
                        Mode::DemandPaged(&reader)
                    },
                    Default::default(),
                );
                let used = runtime.state.get_mut();
                match resource {
                    0 => used.logical_pages = runtime.limits.max_pages.get(),
                    1 => used.rows_visited = runtime.limits.max_rows.get(),
                    2 => used.logical_bytes = runtime.limits.max_bytes.get(),
                    _ => used.file_bytes = runtime.limits.max_file_bytes + 1,
                }
                let mut calls = 0;
                let result = visit(&runtime, &fixture.state, kind, |_, _| {
                    calls += 1;
                    Ok(true)
                });
                if authoritative {
                    assert!(matches!(result, Err(SkeinError::Execution(_))));
                    assert_eq!(calls, 0);
                } else {
                    assert!(result.unwrap());
                    assert_eq!(calls, 8);
                    assert_eq!(
                        runtime.evidence()[0].fallback_reasons,
                        BTreeSet::from(["query_index_budget_exhausted"])
                    );
                }
                assert!(reader.limits.borrow().is_empty());
            }
        }
    }
    fixture.remove();
}

#[test]
fn identity_fence_checks_every_field_after_the_first_success() {
    let reader = Reader::script(Outcome::Success, false);
    let runtime = Runtime::new(Mode::DemandPaged(&reader), Default::default());
    let original = report();
    runtime
        .record_success(
            TABLE,
            INDEX,
            &original,
            RelationalIndexProbeSelector::Prefix(&key(&[])),
        )
        .unwrap();
    let evidence = runtime.evidence();
    for field in 0..5 {
        let mut changed = original.clone();
        match field {
            0 => changed.base_generation += 1,
            1 => changed.delta_generation = Some(1),
            2 => changed.base_commit_epoch += 1,
            3 => changed.visible_commit_epoch += 1,
            _ => changed.root_set_digest.push('x'),
        }
        assert!(matches!(
            runtime.record_success(
                TABLE,
                INDEX,
                &changed,
                RelationalIndexProbeSelector::Prefix(&key(&[]))
            ),
            Err(SkeinError::StorageIntegrity(_))
        ));
        assert_eq!(runtime.evidence(), evidence);
    }
}

#[test]
fn recovered_metrics_preserve_all_counters_and_overflow_errors() {
    let reader = Reader::script(Outcome::Success, false);
    let runtime = Runtime::new(Mode::Authoritative(&reader), Default::default());
    let mut report = report();
    report.delta_generation = Some(2);
    report.visible_commit_epoch = 43;
    report.backend =
        RelationalIndexReadViewBackendReport::Recovered(RelationalIndexRecoveryReadReport {
            base: RelationalIndexReadReport {
                pages_read: 1,
                bytes_read: 2,
                file_pages_read: 3,
                file_bytes_read: 4,
                cache_hits: 5,
                cache_misses: 6,
                cache_admission_rejections: 7,
                ..Default::default()
            },
            delta_pages_read: 8,
            delta_pages_skipped: 9,
            delta_bytes_read: 10,
            delta_file_pages_read: 11,
            delta_file_bytes_read: 12,
            delta_cache_hits: 13,
            delta_cache_misses: 14,
            delta_cache_admission_rejections: 15,
            delta_entries_visited: 16,
            rows_visited: 17,
            stopped_early: true,
        });
    report.live_batches_visited = 18;
    report.live_entries_visited = 19;
    report.live_entries_matched = 20;
    report.live_bytes_visited = 21;
    report.rows_visited = 22;
    report.stopped_early = true;
    for _ in 0..2 {
        runtime
            .record_success(
                TABLE,
                INDEX,
                &report,
                RelationalIndexProbeSelector::Range(&scan(&[1], true, Some(&[1, 2]))),
            )
            .unwrap();
    }
    let e = &runtime.evidence()[0];
    assert_eq!(
        (e.logical_pages, e.logical_bytes, e.file_pages, e.file_bytes),
        (18, 66, 28, 32)
    );
    assert_eq!(
        (e.cache_hits, e.cache_misses, e.cache_admission_rejections),
        (36, 40, 44)
    );
    assert_eq!((e.delta_pages_skipped, e.delta_entries_visited), (18, 32));
    assert_eq!(
        (
            e.live_batches_visited,
            e.live_entries_visited,
            e.live_entries_matched,
            e.live_bytes_visited,
            e.rows_visited
        ),
        (36, 38, 40, 42, 44)
    );
    assert_eq!(
        (
            e.lookups,
            e.authoritative_lookups,
            e.range_lookups,
            e.backward_lookups,
            e.exclusive_seek_lookups,
            e.early_stop_lookups
        ),
        (2, 2, 2, 2, 2, 2)
    );
    let RelationalIndexReadViewBackendReport::Recovered(ref mut recovered) = report.backend else {
        unreachable!()
    };
    recovered.base.pages_read = usize::MAX;
    assert!(matches!(
        IndexReadMetrics::from_report(&report),
        Err(SkeinError::StorageIntegrity(_))
    ));
    assert!(matches!(
        checked_add(usize::MAX, 1, "test"),
        Err(SkeinError::StorageIntegrity(_))
    ));
}

#[test]
fn runtime_path_preserves_all_source_combinations() {
    for mask in 0..16 {
        let evidence = RelationalIndexExecutionEvidence {
            demand_paged_lookups: usize::from(mask & 1 != 0),
            authoritative_lookups: usize::from(mask & 2 != 0),
            transaction_workspace_lookups: usize::from(mask & 4 != 0),
            canonical_fallback_lookups: usize::from(mask & 8 != 0),
            ..Default::default()
        };
        assert_eq!(
            evidence.runtime_path(),
            match mask {
                0 => "not_executed",
                1 => "demand_paged",
                2 => "authoritative",
                4 => "transaction_workspace",
                8 => "canonical_fallback",
                _ => "mixed",
            }
        );
    }
    fn copy_mode<T: Copy>() {}
    copy_mode::<Mode<'_>>();
}

#[test]
fn pinned_and_transaction_views_preserve_read_your_writes_and_statistics() {
    let fixture = Fixture::new();
    let base = Arc::clone(fixture.reader.view());
    assert!(Mode::Shadow(&fixture.reader)
        .probe_statistics(TABLE, INDEX, 1)
        .is_some());
    assert_eq!(Mode::Materialized.probe_statistics(TABLE, INDEX, 1), None);
    assert_eq!(
        Mode::TransactionWorkspace.probe_statistics(TABLE, INDEX, 1),
        None
    );
    let mut state = fixture.state.clone();
    let mut oracle = fixture.oracle.clone();
    let live = Arc::new(
        base.advance(
            41,
            Some(replace(&mut state, &mut oracle, 1, Some((2, 9)))),
            Default::default(),
        )
        .unwrap(),
    );
    let reader = Reader::pinned(Arc::clone(&live));
    assert_eq!(
        Mode::Authoritative(&reader).probe_statistics(TABLE, INDEX, 1),
        None
    );
    let mut transaction = RelationalTransactionIndexView::new(
        Arc::clone(&live),
        Default::default(),
        Default::default(),
    );
    let mut private_state = state.clone();
    let mut private = oracle.clone();
    for (id, next) in [(1, None), (4, Some((1, 8))), (25, Some((2, 3)))] {
        transaction
            .append(replace(&mut private_state, &mut private, id, next))
            .unwrap();
    }
    assert_eq!(
        Mode::AuthoritativeTransaction(&transaction).probe_statistics(TABLE, INDEX, 1),
        None
    );
    for (mode, state, oracle) in [
        (
            Mode::Authoritative(&fixture.reader),
            &fixture.state,
            &fixture.oracle,
        ),
        (Mode::Authoritative(&reader), &state, &oracle),
        (
            Mode::AuthoritativeTransaction(&transaction),
            &private_state,
            &private,
        ),
    ] {
        let runtime = Runtime::new(mode, Default::default());
        for backward in [false, true] {
            for bucket in 0..3 {
                let scan = scan(&[bucket], backward, None);
                let mut got = Vec::new();
                runtime
                    .visit_range_entries(state, TABLE, INDEX, &scan, |index, primary| {
                        got.push((index.clone(), primary.clone()));
                        Ok(true)
                    })
                    .unwrap();
                let want = expected(oracle, &scan);
                assert_eq!(
                    got.iter().map(|entry| &entry.0).collect::<Vec<_>>(),
                    want.iter().map(|entry| &entry.0).collect::<Vec<_>>()
                );
                // The SQL contract orders index keys; overlay locators tied on one key
                // need not use the base posting list's tie order, but none may be lost.
                got.sort();
                let mut want = want;
                want.sort();
                assert_eq!(got, want);
            }
        }
    }
    drop((transaction, reader, live, base));
    fixture.remove();
}

#[test]
fn recovered_live_and_transaction_layers_preserve_postings_and_fences() {
    let fixture = Fixture::new();
    let (recovered, state, oracle) = fixture.recovered();
    let mut live_state = state.clone();
    let mut live_oracle = oracle.clone();
    let live = Reader::pinned(Arc::new(
        recovered
            .view()
            .advance(
                44,
                Some(replace(&mut live_state, &mut live_oracle, 5, Some((1, 7)))),
                Default::default(),
            )
            .unwrap(),
    ));
    let mut transaction = RelationalTransactionIndexView::new(
        Arc::clone(live.view()),
        Default::default(),
        Default::default(),
    );
    let mut private_state = live_state.clone();
    let mut private = live_oracle.clone();
    transaction
        .append(replace(&mut private_state, &mut private, 7, None))
        .unwrap();
    transaction
        .append(replace(&mut private_state, &mut private, 26, Some((2, 8))))
        .unwrap();
    for (mode, state, oracle, epoch) in [
        (Mode::Authoritative(&recovered), &state, &oracle, 43),
        (Mode::Authoritative(&live), &live_state, &live_oracle, 44),
        (
            Mode::AuthoritativeTransaction(&transaction),
            &private_state,
            &private,
            44,
        ),
    ] {
        let runtime = Runtime::new(mode, Default::default());
        for backward in [false, true] {
            for bucket in 0..3 {
                let scan = scan(&[bucket], backward, None);
                let mut actual = Vec::new();
                runtime
                    .visit_range_entries(state, TABLE, INDEX, &scan, |index, primary| {
                        actual.push((index.clone(), primary.clone()));
                        Ok(true)
                    })
                    .unwrap();
                let mut want = expected(oracle, &scan);
                assert_eq!(
                    actual.iter().map(|entry| &entry.0).collect::<Vec<_>>(),
                    want.iter().map(|entry| &entry.0).collect::<Vec<_>>()
                );
                actual.sort();
                want.sort();
                assert_eq!(actual, want);
            }
        }
        let evidence = runtime.evidence();
        assert!(evidence[0].delta_generation.is_some());
        assert_eq!(evidence[0].visible_commit_epoch, Some(epoch));
        assert!(evidence[0].delta_entries_visited > 0);
    }
    drop((transaction, live, recovered));
    fixture.remove();
}

fn differential_campaign(seeds: u64, cases: usize) {
    let fixture = Fixture::new();
    for seed in 0..seeds {
        let mut random = seed.wrapping_add(1);
        for case in 0..cases {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let width = (random % 3) as usize;
            let bucket = ((random >> 8) % 5) as i64 - 1;
            let rank = ((random >> 16) % 7) as i64 - 1;
            let parts = [bucket, rank];
            let prefix = &parts[..width];
            let scan = scan(
                prefix,
                random & 8 != 0,
                (random & 16 != 0).then_some(&parts),
            );
            let stop = random & 32 != 0;
            let mut want = expected(&fixture.oracle, &scan);
            if stop {
                want.truncate(1);
            }
            for mode in 0..6 {
                let transaction = fixture.transaction();
                let runtime = Runtime::new(fixture.mode(mode, &transaction), Default::default());
                let mut got = Vec::new();
                let result = runtime.visit_range_entries(
                    &fixture.state,
                    TABLE,
                    INDEX,
                    &scan,
                    |index, primary| {
                        got.push((index.clone(), primary.clone()));
                        Ok(!stop)
                    },
                );
                let rejected = width == 0 && matches!(mode, 3 | 5);
                assert_outcome(
                    result,
                    &got,
                    &want,
                    rejected,
                    !stop || want.is_empty(),
                    (seed, case, mode),
                );
                let mut got = Vec::new();
                let result = runtime.visit_prefix_entries(
                    &fixture.state,
                    TABLE,
                    INDEX,
                    &key(prefix),
                    |index, primary| {
                        got.push((index.clone(), primary.clone()));
                        Ok(true)
                    },
                );
                let prefix_want = expected(&fixture.oracle, &self::scan(prefix, false, None));
                assert_outcome(
                    result,
                    &got,
                    &prefix_want,
                    rejected,
                    true,
                    (seed, case, mode),
                );
                let mut got = Vec::new();
                let result = runtime.visit_prefix_entries_many(
                    &fixture.state,
                    TABLE,
                    INDEX,
                    &[key(prefix), key(prefix)],
                    |requested, index, primary| {
                        assert_eq!(requested, &key(prefix));
                        got.push((index.clone(), primary.clone()));
                        Ok(true)
                    },
                );
                assert_outcome(
                    result,
                    &got,
                    &prefix_want,
                    rejected,
                    true,
                    (seed, case, mode),
                );
            }
        }
    }
    fixture.remove();
}

#[test]
fn index_runtime_differential_smoke() {
    differential_campaign(4, 16);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn index_runtime_differential_campaign() {
    differential_campaign(128, 64);
}

fn assert_outcome(
    result: Result<bool>,
    rows: &[Entry],
    expected: &[Entry],
    rejected: bool,
    complete: bool,
    context: (u64, usize, usize),
) {
    if rejected {
        // Persistent key encoding rejects empty prefixes. Derived reads retain
        // their canonical fallback; authoritative reads must remain fail-closed.
        assert!(
            matches!(result, Err(SkeinError::Execution(_))),
            "seed/case/mode={context:?}"
        );
        assert!(rows.is_empty(), "seed/case/mode={context:?}");
    } else {
        assert_eq!(result.unwrap(), complete, "seed/case/mode={context:?}");
        assert_eq!(rows, expected, "seed/case/mode={context:?}");
    }
}
