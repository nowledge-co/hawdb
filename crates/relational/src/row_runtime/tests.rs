use super::*;
use skein_core::{RuntimeCancellationToken, Value};
use skein_storage::{
    ProjectionGenerationReadReport, RelationalRowPageDemandReadReport, RelationalValue,
};

mod fixtures;
use fixtures::{apply, fields, key, state, Fixture};

const SHAPES: [(&str, &[usize], &[usize]); 6] = [
    ("SELECT * FROM docs", &[0, 1, 2], &[0, 1, 2]),
    ("SELECT id FROM docs", &[0], &[0]),
    ("SELECT body FROM docs WHERE bucket >= 0", &[1, 2], &[1, 2]),
    ("SELECT body FROM docs ORDER BY bucket", &[1], &[1, 2]),
    ("SELECT count(body) FROM docs", &[2], &[2]),
    ("SELECT count(*) FROM docs", &[], &[]),
];

fn values(row: &RelationalReadRow, ordinals: &[usize]) -> Vec<RelationalValue> {
    for ordinal in 0..3 {
        assert_eq!(row.value(ordinal).is_ok(), ordinals.contains(&ordinal));
    }
    ordinals
        .iter()
        .map(|ordinal| row.value(*ordinal).unwrap().clone())
        .collect()
}

fn expected(
    state: &RelationalState,
    key: &RelationalKey,
    ordinals: &[usize],
) -> Option<Vec<RelationalValue>> {
    state.row_entry("docs", key).map(|(_, row)| {
        ordinals
            .iter()
            .map(|ordinal| row.values()[*ordinal].clone())
            .collect()
    })
}

#[test]
fn pinned_sources_preserve_point_batch_and_owned_and_borrowed_scan_projections() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for mode in 0..3 {
        for (sql, scanned, output) in SHAPES {
            let runtime = fixture.runtime(mode, sql, &task);
            for id in [-1, 0, 2, 7, 8] {
                let point = runtime.read_point("docs", &key(id)).unwrap();
                assert_eq!(
                    point.as_ref().map(|row| values(row, scanned)),
                    expected(&fixture.state, &key(id), scanned)
                );
                if let Some(point) = point {
                    assert_eq!(point.primary_key(), &key(id));
                    assert!(point.resident_bytes() >= std::mem::size_of::<RelationalReadRow>());
                }
                let point = runtime.read_output_point("docs", &key(id)).unwrap();
                assert_eq!(
                    point.as_ref().map(|row| values(row, output)),
                    expected(&fixture.state, &key(id), output)
                );
            }
            let keys = [key(7), key(2), key(2), key(0), key(9)];
            let points = runtime.read_points("docs", &keys).unwrap();
            assert_eq!(
                points.keys().cloned().collect::<Vec<_>>(),
                vec![key(0), key(2), key(7)]
            );
            for (key, row) in points {
                assert_eq!(
                    Some(values(&row, scanned)),
                    expected(&fixture.state, &key, scanned)
                );
            }
            let expected_rows = fixture
                .state
                .rows("docs")
                .map(|(key, _)| expected(&fixture.state, key, scanned).unwrap())
                .collect::<Vec<_>>();
            let mut owned = Vec::new();
            assert!(runtime
                .visit_all("docs", |row| {
                    owned.push(values(&row, scanned));
                    Ok(true)
                })
                .unwrap());
            assert_eq!(owned, expected_rows, "owned: mode={mode}, sql={sql}");
            let mut borrowed = Vec::new();
            assert!(runtime
                .visit_all_ref("docs", |row| {
                    borrowed.push(
                        scanned
                            .iter()
                            .map(|ordinal| row.value(*ordinal).unwrap().to_owned_value())
                            .collect::<Vec<_>>(),
                    );
                    Ok(true)
                })
                .unwrap());
            assert_eq!(borrowed, expected_rows, "borrowed: mode={mode}, sql={sql}");
            assert_eq!(
                runtime.evidence().runtime_path,
                ["canonical_memory", "snapshot_rows", "projection_generation"][mode]
            );
            if mode == 1 {
                assert!(runtime.evidence().borrowed_rows_visited > 0);
            }
        }
    }
    fixture.remove();
}

#[test]
fn duplicate_points_charge_each_distinct_key_once() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for mode in 0..3 {
        let runtime = fixture.runtime(mode, "SELECT * FROM docs", &task);
        let single = fixture.runtime(mode, "SELECT * FROM docs", &task);
        let rows = runtime
            .read_points("docs", &[key(1), key(1), key(1)])
            .unwrap();
        single.read_points("docs", &[key(1)]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(runtime.evidence(), single.evidence());
        assert_eq!(runtime.hydration(), single.hydration());
    }
    fixture.remove();
}

#[test]
fn scan_stop_and_callback_errors_are_preserved() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for mode in 0..3 {
        for borrowed in [false, true] {
            for fail in [false, true] {
                let runtime = fixture.runtime(mode, "SELECT * FROM docs", &task);
                let mut calls = 0;
                let mut callback = || {
                    calls += 1;
                    if fail {
                        Err(SkeinError::Semantic("callback sentinel".into()))
                    } else {
                        Ok(false)
                    }
                };
                let result = if borrowed {
                    runtime.visit_all_ref("docs", |_| callback())
                } else {
                    runtime.visit_all("docs", |_| callback())
                };
                assert_eq!(calls, 1);
                if fail {
                    assert!(
                        matches!(result, Err(SkeinError::Semantic(message)) if message == "callback sentinel")
                    );
                } else {
                    assert!(!result.unwrap());
                }
                assert!(runtime.evidence().rows_visited >= 1);
            }
        }
    }
    fixture.remove();
}

#[test]
fn cancelled_scans_do_not_invoke_callbacks() {
    let fixture = Fixture::new();
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task = RuntimeTaskContext::new(token, None);
    for mode in 0..3 {
        let runtime = fixture.runtime(mode, "SELECT * FROM docs", &task);
        assert!(matches!(
            runtime.visit_all_ref("docs", |_| panic!("cancelled callback")),
            Err(SkeinError::Execution(_))
        ));
        assert!(matches!(
            runtime.visit_all("docs", |_| panic!("cancelled callback")),
            Err(SkeinError::Execution(_))
        ));
        assert_eq!(runtime.evidence().rows_visited, 0);
    }
    fixture.remove();
}

#[test]
fn cumulative_row_budget_applies_across_calls() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for mode in 0..3 {
        let mut runtime = fixture.runtime(mode, "SELECT * FROM docs", &task);
        runtime.limits.demand.max_rows = NonZeroUsize::new(1).unwrap();
        assert!(runtime.read_point("docs", &key(0)).unwrap().is_some());
        assert!(matches!(
            runtime.read_point("docs", &key(1)),
            Err(SkeinError::Execution(_))
        ));
        assert_eq!(runtime.evidence().rows_visited, 1);
    }
    fixture.remove();
}

#[test]
fn metadata_reads_preserve_overflow_until_output_hydration() {
    let mut state = state();
    let body = "overflow".repeat(2048);
    apply(
        &mut state,
        "UPDATE docs SET body = $1 WHERE id = 0",
        &[Value::String(body.clone())],
    );
    let task = RuntimeTaskContext::default();
    let mut runtime = RelationalRowRuntime::new(
        &state,
        None,
        None,
        fields("SELECT count(body) FROM docs", &state),
        Default::default(),
        Default::default(),
        &task,
    );
    runtime.hydration.get_mut().max_decompressed_bytes = 1;
    let scanned = runtime.read_point("docs", &key(0)).unwrap().unwrap();
    assert!(matches!(
        scanned.value(2).unwrap(),
        RelationalValue::Overflow(_)
    ));
    assert_eq!(runtime.hydration().hydrated_rows, 0);
    assert!(matches!(
        runtime.read_output_point("docs", &key(0)),
        Err(SkeinError::Execution(_))
    ));
    let runtime = RelationalRowRuntime::new(
        &state,
        None,
        None,
        fields("SELECT count(body) FROM docs", &state),
        Default::default(),
        Default::default(),
        &task,
    );
    assert_eq!(
        runtime
            .read_output_point("docs", &key(0))
            .unwrap()
            .unwrap()
            .value(2)
            .unwrap(),
        &RelationalValue::Text(body.clone())
    );
    assert_eq!(runtime.hydration().decompressed_bytes, body.len());
    assert_eq!(runtime.hydration().hydrated_rows, 1);
}

#[test]
fn index_coverage_binds_snapshot_without_reading_row_pages() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let runtime = fixture.runtime(1, "SELECT id, bucket FROM docs", &task);
    let row = runtime
        .read_index_covered("docs", &["bucket".into()], &key(1), &key(4))
        .unwrap()
        .unwrap();
    assert_eq!(
        values(&row, &[0, 1]),
        vec![RelationalValue::BigInt(4), RelationalValue::BigInt(1)]
    );
    let evidence = runtime.evidence();
    assert_eq!(evidence.base_generation, Some(1));
    assert_eq!(evidence.visible_commit_epoch, Some(10));
    assert_eq!(evidence.index_covered_rows, 1);
    assert_eq!(evidence.rows_visited, 0);
    assert_eq!(evidence.logical_pages, 0);
    assert!(matches!(
        runtime.read_index_covered("docs", &["bucket".into()], &RelationalKey(vec![]), &key(4)),
        Err(SkeinError::StorageIntegrity(_))
    ));
    assert!(matches!(
        runtime.read_index_covered("docs", &["bucket".into()], &key(1), &RelationalKey(vec![])),
        Err(SkeinError::StorageIntegrity(_))
    ));
    let uncovered = fixture.runtime(1, "SELECT body FROM docs", &task);
    assert!(uncovered
        .read_index_covered("docs", &["bucket".into()], &key(1), &key(4))
        .unwrap()
        .is_none());
    assert_eq!(uncovered.evidence().base_generation, None);
    drop((runtime, uncovered));
    fixture.remove();
}

#[test]
fn recovery_and_live_layers_preserve_precedence_and_tombstones() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let mut oracle = fixture
        .state
        .rows("docs")
        .map(|(key, row)| (key.clone(), row.values().to_vec()))
        .collect::<BTreeMap<_, _>>();
    oracle.remove(&key(2));
    oracle.remove(&key(3));
    for (id, body) in [(1, "live-one"), (5, "recovery-five"), (8, "live-eight")] {
        oracle.insert(
            key(id),
            vec![
                RelationalValue::BigInt(id),
                RelationalValue::BigInt(id % 3),
                RelationalValue::Text(body.into()),
            ],
        );
    }
    let runtime = RelationalRowRuntime::new(
        &fixture.state,
        Some(fixture.recovered_snapshot()),
        None,
        fields("SELECT * FROM docs", &fixture.state),
        Default::default(),
        Default::default(),
        &task,
    );
    for id in -1..10 {
        assert_eq!(
            runtime
                .read_point("docs", &key(id))
                .unwrap()
                .map(|row| values(&row, &[0, 1, 2])),
            oracle.get(&key(id)).cloned()
        );
    }
    let keys = (-1..10)
        .map(key)
        .chain([key(1), key(2)])
        .collect::<Vec<_>>();
    let batch = runtime
        .read_points("docs", &keys)
        .unwrap()
        .into_iter()
        .map(|(key, row)| (key, values(&row, &[0, 1, 2])))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(batch, oracle);
    let mut owned = BTreeMap::new();
    runtime
        .visit_all("docs", |row| {
            owned.insert(row.primary_key().clone(), values(&row, &[0, 1, 2]));
            Ok(true)
        })
        .unwrap();
    assert_eq!(owned, oracle);
    let mut borrowed = Vec::new();
    runtime
        .visit_all_ref("docs", |row| {
            borrowed.push(
                (0..3)
                    .map(|ordinal| row.value(ordinal).unwrap().to_owned_value())
                    .collect::<Vec<_>>(),
            );
            Ok(true)
        })
        .unwrap();
    assert_eq!(borrowed, oracle.into_values().collect::<Vec<_>>());
    let evidence = runtime.evidence();
    assert_eq!(evidence.base_generation, Some(1));
    assert_eq!(evidence.delta_generation, Some(1));
    assert_eq!(evidence.visible_commit_epoch, Some(12));
    assert!(evidence.overlay_entries > 0);
    assert!(evidence.overlay_resident_bytes > 0);
    drop(runtime);
    fixture.remove();
}

#[test]
fn nested_output_hydration_shares_scan_budget_even_on_callback_error() {
    let fixture = Fixture::new();
    let mut state = fixture.state.clone();
    let body = "nested-overflow".repeat(2048);
    let changes = fixtures::capture(
        &mut state,
        &[
            (
                "UPDATE docs SET body = $1 WHERE id = 0",
                &[Value::String(body.clone())],
            ),
            (
                "UPDATE docs SET body = $1 WHERE id = 1",
                &[Value::String(body.clone())],
            ),
        ],
    );
    let task = RuntimeTaskContext::default();
    for snapshot in [false, true] {
        for borrowed in [false, true] {
            let runtime = RelationalRowRuntime::new(
                &state,
                snapshot.then(|| fixture.live_snapshot(changes.clone())),
                None,
                fields("SELECT count(body) FROM docs", &state),
                Default::default(),
                RelationalHydrationBudget {
                    max_rows: 1,
                    ..Default::default()
                },
                &task,
            );
            let mut calls = 0;
            let mut callback = || {
                let id = calls;
                calls += 1;
                runtime.read_output_point("docs", &key(id))?;
                Ok(true)
            };
            let result = if borrowed {
                runtime.visit_all_ref("docs", |_| callback())
            } else {
                runtime.visit_all("docs", |_| callback())
            };
            assert!(matches!(result, Err(SkeinError::Execution(_))));
            assert_eq!(calls, 2);
            assert_eq!(runtime.hydration().hydrated_rows, 1);
            assert_eq!(runtime.hydration().decompressed_bytes, body.len());
        }
    }
    fixture.remove();
}

#[test]
fn snapshot_identity_and_all_cumulative_counters_are_preserved() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let runtime = fixture.runtime(1, "SELECT * FROM docs", &task);
    let identity = fixture.snapshot().identity();
    let report = RelationalRowPageDemandReadReport {
        descriptor_reads: 1,
        pages_read: 2,
        bytes_read: 3,
        file_pages_read: 4,
        file_bytes_read: 5,
        cache_hits: 6,
        cache_misses: 7,
        cache_admission_rejections: 8,
        rows_emitted: 9,
        borrowed_rows_emitted: 4,
        owned_rows_emitted: 5,
        ..Default::default()
    };
    for _ in 0..2 {
        runtime.record(identity, &report, 10, 11).unwrap();
    }
    let evidence = runtime.evidence();
    assert_eq!(
        (
            evidence.descriptor_reads,
            evidence.logical_pages,
            evidence.logical_bytes
        ),
        (2, 4, 6)
    );
    assert_eq!(
        (
            evidence.file_pages,
            evidence.file_bytes,
            evidence.cache_hits,
            evidence.cache_misses,
            evidence.cache_admission_rejections
        ),
        (8, 10, 12, 14, 16)
    );
    assert_eq!(
        (
            evidence.rows_visited,
            evidence.borrowed_rows_visited,
            evidence.owned_rows_visited
        ),
        (18, 8, 10)
    );
    assert_eq!(
        (evidence.overlay_entries, evidence.overlay_resident_bytes),
        (20, 22)
    );
    let remaining = runtime.remaining_limits().unwrap();
    assert_eq!(
        remaining.demand.max_pages.get(),
        runtime.limits.demand.max_pages.get() - 4
    );
    assert_eq!(
        remaining.demand.max_rows.get(),
        runtime.limits.demand.max_rows.get() - 18
    );
    assert_eq!(
        remaining.demand.max_bytes.get(),
        runtime.limits.demand.max_bytes.get() - 6
    );
    assert_eq!(
        remaining.max_overlay_entries.get(),
        runtime.limits.max_overlay_entries.get() - 20
    );
    assert_eq!(
        remaining.max_overlay_bytes.get(),
        runtime.limits.max_overlay_bytes.get() - 22
    );
    assert_eq!(remaining.demand.max_pins, runtime.limits.demand.max_pins);
    assert_eq!(
        remaining.demand.max_tree_height,
        runtime.limits.demand.max_tree_height
    );
    for field in 0..5 {
        let mut changed = identity;
        match field {
            0 => changed.base_generation += 1,
            1 => changed.delta_generation = Some(1),
            2 => changed.base_commit_epoch += 1,
            3 => changed.visible_commit_epoch += 1,
            _ => changed.root_set_digest = "00".repeat(32).parse().unwrap(),
        }
        assert_ne!(changed, identity);
        assert!(matches!(
            runtime.record(changed, &report, 10, 11),
            Err(SkeinError::StorageIntegrity(_))
        ));
        assert_eq!(runtime.evidence(), evidence);
    }
    drop(runtime);
    fixture.remove();
}

#[test]
fn exhausted_resource_limits_and_counter_overflow_fail_closed() {
    let state = state();
    let task = RuntimeTaskContext::default();
    for resource in 0..5 {
        let mut runtime = RelationalRowRuntime::new(
            &state,
            None,
            None,
            fields("SELECT * FROM docs", &state),
            Default::default(),
            Default::default(),
            &task,
        );
        let evidence = runtime.evidence.get_mut();
        match resource {
            0 => evidence.logical_pages = runtime.limits.demand.max_pages.get(),
            1 => evidence.rows_visited = runtime.limits.demand.max_rows.get(),
            2 => evidence.logical_bytes = runtime.limits.demand.max_bytes.get(),
            3 => evidence.overlay_entries = runtime.limits.max_overlay_entries.get(),
            _ => evidence.overlay_resident_bytes = runtime.limits.max_overlay_bytes.get(),
        }
        assert!(matches!(
            runtime.remaining_limits(),
            Err(SkeinError::Execution(_))
        ));
    }
    let mut counter = usize::MAX;
    assert!(matches!(
        add_counter(&mut counter, 1, "test"),
        Err(SkeinError::StorageIntegrity(_))
    ));
    assert_eq!(counter, usize::MAX);
}

#[test]
fn projection_publication_does_not_replace_a_pinned_reader() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let old = fixture.runtime(2, "SELECT * FROM docs", &task);
    let reader = fixture.replace_projection();
    let new = RelationalRowRuntime::new(
        &fixture.state,
        None,
        Some((&reader, &fixture.tables)),
        fields("SELECT * FROM docs", &fixture.state),
        Default::default(),
        Default::default(),
        &task,
    );
    assert_eq!(
        values(
            &old.read_point("docs", &key(4)).unwrap().unwrap(),
            &[0, 1, 2]
        ),
        expected(&fixture.state, &key(4), &[0, 1, 2]).unwrap()
    );
    assert_eq!(
        new.read_point("docs", &key(4))
            .unwrap()
            .unwrap()
            .value(2)
            .unwrap(),
        &RelationalValue::Text("replacement".into())
    );
    assert_eq!(
        old.evidence().projection_generation.as_deref(),
        Some("fixture-v1")
    );
    assert_eq!(
        new.evidence().projection_generation.as_deref(),
        Some("fixture-next")
    );
    assert!(old.read_point("docs", &key(1)).unwrap().is_some());
    assert!(new.read_point("docs", &key(1)).unwrap().is_none());
    drop((old, new));
    drop(reader);
    fixture.remove();
}

#[test]
fn projection_identity_and_table_selection_are_fail_closed() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let runtime = fixture.runtime(2, "SELECT * FROM docs", &task);
    let report = ProjectionGenerationReadReport {
        generation: "fixture-v1".into(),
        source_watermark: 10,
        projection_version: 1,
        publication_commit_epoch: fixture.projection.publication_commit_epoch(),
        rows_returned: 2,
        payload_bytes: 3,
        complete: false,
    };
    runtime.record_projection_page(&report).unwrap();
    let evidence = runtime.evidence();
    assert_eq!(
        (
            evidence.logical_pages,
            evidence.logical_bytes,
            evidence.rows_visited,
            evidence.owned_rows_visited
        ),
        (1, 3, 2, 2)
    );
    for field in 0..4 {
        let mut changed = report.clone();
        match field {
            0 => changed.generation.push('x'),
            1 => changed.source_watermark += 1,
            2 => changed.projection_version += 1,
            _ => changed.publication_commit_epoch += 1,
        }
        assert!(matches!(
            runtime.record_projection_page(&changed),
            Err(SkeinError::StorageIntegrity(_))
        ));
        assert_eq!(runtime.evidence(), evidence);
    }
    let unrelated = BTreeSet::from(["unrelated".into()]);
    let fallback = RelationalRowRuntime::new(
        &fixture.state,
        Some(fixture.snapshot()),
        Some((&fixture.projection, &unrelated)),
        fields("SELECT * FROM docs", &fixture.state),
        Default::default(),
        Default::default(),
        &task,
    );
    assert_eq!(fallback.evidence().runtime_path, "snapshot_rows");
    assert_eq!(fallback.evidence().projection_generation, None);
    assert!(fallback.read_point("docs", &key(4)).unwrap().is_some());
    drop((runtime, fallback));
    fixture.remove();
}

fn differential_campaign(seeds: u64, cases: usize) {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for seed in 0..seeds {
        let mut random = seed.wrapping_add(1);
        for case in 0..cases {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let (sql, scanned, output) = SHAPES[(random as usize) % SHAPES.len()];
            let ids = [
                ((random >> 8) % 12) as i64 - 2,
                ((random >> 16) % 12) as i64 - 2,
                ((random >> 24) % 12) as i64 - 2,
            ];
            let keys = [key(ids[0]), key(ids[1]), key(ids[2]), key(ids[0])];
            let expected_batch = keys
                .iter()
                .filter_map(|key| {
                    expected(&fixture.state, key, scanned).map(|row| (key.clone(), row))
                })
                .collect::<BTreeMap<_, _>>();
            for mode in 0..3 {
                let runtime = fixture.runtime(mode, sql, &task);
                let actual = runtime
                    .read_points("docs", &keys)
                    .unwrap()
                    .into_iter()
                    .map(|(key, row)| (key, values(&row, scanned)))
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(
                    actual, expected_batch,
                    "seed={seed}, case={case}, mode={mode}, sql={sql}"
                );
                for key in &keys {
                    let actual = runtime
                        .read_output_point("docs", key)
                        .unwrap()
                        .map(|row| values(&row, output));
                    assert_eq!(
                        actual,
                        expected(&fixture.state, key, output),
                        "seed={seed}, case={case}, mode={mode}, sql={sql}"
                    );
                }
            }
        }
    }
    fixture.remove();
}

#[test]
fn row_runtime_differential_smoke() {
    differential_campaign(4, 16);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn row_runtime_differential_campaign() {
    differential_campaign(128, 64);
}
