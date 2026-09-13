use super::*;
use crate::{
    RelationalColumnSchema, RelationalConstraintIndex, RelationalError, RelationalIndexSchema,
    RelationalIndexShadowConfig, RelationalIndexShadowWriter, RelationalInsertMode,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalRow, RelationalScalarType,
    RelationalState, RelationalTableSchema, RelationalTransaction, RelationalWrite,
};
use crate::{RelationalIndexChange, RelationalValue};

const TABLE: &str = "items";
const INDEX: &str = "items_bucket_rank";
type Oracle = BTreeMap<i64, (i64, i64)>;

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn open() -> (Self, Arc<RelationalIndexReadView>, Oracle, RelationalState) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "skein-index-view-owner-{}-{nonce}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).expect("create a unique owned fixture directory");
        let fixture = Self(directory.clone());
        let oracle: Oracle = (0..8).map(|id| (id, (id % 3, id * 7 % 11))).collect();
        let state = RelationalState::default()
            .stage_transaction(
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(RelationalTableSchema {
                            name: TABLE.into(),
                            columns: ["id", "bucket", "rank"]
                                .into_iter()
                                .map(|name| RelationalColumnSchema {
                                    name: name.into(),
                                    scalar_type: RelationalScalarType::BigInt,
                                    nullable: false,
                                    default: None,
                                })
                                .collect(),
                            primary_key: vec!["id".into()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: vec![RelationalIndexSchema {
                                name: INDEX.into(),
                                columns: vec!["bucket".into(), "rank".into()],
                                unique: false,
                            }],
                        }),
                        RelationalWrite::Insert {
                            table: TABLE.into(),
                            rows: oracle
                                .iter()
                                .map(|(&id, &(bucket, rank))| {
                                    RelationalRow::new(vec![
                                        RelationalValue::BigInt(id),
                                        RelationalValue::BigInt(bucket),
                                        RelationalValue::BigInt(rank),
                                    ])
                                })
                                .collect(),
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .unwrap();
        let config = RelationalIndexShadowConfig::default();
        RelationalIndexShadowWriter::new(config)
            .publish(&directory, &state, 1, 40, None)
            .unwrap();
        let reader = RelationalIndexShadowReader::open(&directory, 1, 40, config).unwrap();
        (
            fixture,
            Arc::new(RelationalIndexReadView::from_base(reader)),
            oracle,
            state,
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove the owned index-view fixture");
    }
}

fn key(values: &[i64]) -> RelationalKey {
    RelationalKey(
        values
            .iter()
            .copied()
            .map(RelationalValue::BigInt)
            .collect(),
    )
}

fn replace(
    state: &mut RelationalState,
    oracle: &mut Oracle,
    id: i64,
    next: Option<(i64, i64)>,
) -> RelationalIndexChangeCapture {
    let mut writes = vec![RelationalWrite::DeleteByPrimaryKey {
        table: TABLE.into(),
        keys: vec![key(&[id])],
    }];
    oracle.remove(&id);
    if let Some((bucket, rank)) = next {
        writes.push(RelationalWrite::Insert {
            table: TABLE.into(),
            rows: vec![RelationalRow::new(vec![
                RelationalValue::BigInt(id),
                RelationalValue::BigInt(bucket),
                RelationalValue::BigInt(rank),
            ])],
            mode: RelationalInsertMode::Error,
        });
        oracle.insert(id, (bucket, rank));
    }
    let (next_state, captured) = state
        .stage_transaction_with_index_changes(
            RelationalTransaction { writes },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .unwrap();
    *state = next_state;
    captured
}

fn expected(
    oracle: &Oracle,
    scan: &RelationalIndexRangeScan,
) -> Vec<(RelationalKey, RelationalKey)> {
    let mut entries: Vec<_> = oracle
        .iter()
        .map(|(&id, &(bucket, rank))| (key(&[bucket, rank]), key(&[id])))
        .filter(|(index_key, _)| {
            index_key.0.starts_with(&scan.prefix.0)
                && scan
                    .exclusive_bound
                    .as_ref()
                    .is_none_or(|bound| match scan.direction {
                        RelationalIndexScanDirection::Forward => index_key > bound,
                        RelationalIndexScanDirection::Backward => index_key < bound,
                    })
        })
        .collect();
    entries.sort();
    if scan.direction == RelationalIndexScanDirection::Backward {
        entries.reverse();
    }
    entries
}

fn assert_view(view: &RelationalIndexReadView, oracle: &Oracle) {
    for direction in [
        RelationalIndexScanDirection::Forward,
        RelationalIndexScanDirection::Backward,
    ] {
        for bucket in [0, 1, 2, 9] {
            let prefix = key(&[bucket]);
            for exclusive_bound in [None, Some(key(&[bucket, 4]))] {
                let scan = RelationalIndexRangeScan {
                    prefix: prefix.clone(),
                    exclusive_bound,
                    direction,
                };
                let want = expected(oracle, &scan);
                for early_stop in [false, true] {
                    let mut got = Vec::new();
                    let report = view
                        .visit_range_entries(
                            TABLE,
                            INDEX,
                            &scan,
                            RelationalIndexReadLimits::default(),
                            |index, primary| {
                                got.push((index.clone(), primary.clone()));
                                !early_stop
                            },
                        )
                        .unwrap();
                    assert_eq!(
                        got,
                        if early_stop {
                            want.iter().take(1).cloned().collect::<Vec<_>>()
                        } else {
                            want.clone()
                        },
                        "identity={:?}, scan={scan:?}, early_stop={early_stop}, oracle={oracle:?}",
                        view.identity()
                    );
                    assert_eq!(report.rows_visited, got.len());
                    assert_eq!(report.stopped_early, early_stop && !want.is_empty());
                    assert_eq!(
                        report.visible_commit_epoch,
                        view.identity().visible_commit_epoch
                    );
                }
            }
        }
    }
}

fn run_view_campaign(seeds: impl IntoIterator<Item = u64>) {
    let (_fixture, base, initial, initial_state) = Fixture::open();
    for seed in seeds {
        let mut live = Arc::clone(&base);
        let mut committed = initial.clone();
        let mut state = initial_state.clone();
        let mut random = seed.wrapping_add(1);
        for step in 0..8 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = (random % 12) as i64;
            let next = (random & 8 != 0).then_some(((random % 3) as i64, (random % 11) as i64));
            let pinned = Arc::clone(&live);
            let previous = committed.clone();
            let change = replace(&mut state, &mut committed, id, next);
            live = Arc::new(
                live.advance(
                    41 + step,
                    Some(change),
                    RelationalIndexChangeCaptureLimits::default(),
                )
                .unwrap(),
            );
            assert_view(&pinned, &previous);
            assert_view(&live, &committed);

            let mut transaction = RelationalTransactionIndexView::new(
                Arc::clone(&live),
                RelationalIndexChangeCaptureLimits::default(),
                RelationalIndexReadLimits::default(),
            );
            let mut private = committed.clone();
            let mut private_state = state.clone();
            // Multiple statements, including cancellation and reinsertion, share one overlay.
            for next in [Some((1, 5)), None, Some((2, 3))] {
                transaction
                    .append(replace(&mut private_state, &mut private, id, next))
                    .unwrap();
            }
            for direction in [
                RelationalIndexScanDirection::Forward,
                RelationalIndexScanDirection::Backward,
            ] {
                for bucket in 0..3 {
                    let scan = RelationalIndexRangeScan {
                        prefix: key(&[bucket]),
                        exclusive_bound: None,
                        direction,
                    };
                    let mut got = Vec::new();
                    transaction
                        .visit_range_entries(
                            TABLE,
                            INDEX,
                            &scan,
                            RelationalIndexReadLimits::default(),
                            |index, primary| {
                                got.push((index.clone(), primary.clone()));
                                true
                            },
                        )
                        .unwrap();
                    assert_eq!(got, expected(&private, &scan));
                }
            }
            let mut exact = Vec::new();
            transaction
                .visit_exact_primary_keys(TABLE, INDEX, &key(&[2, 3]), &mut |primary| {
                    exact.push(primary.clone());
                    true
                })
                .unwrap();
            let mut want: Vec<_> = private
                .iter()
                .filter(|(_, value)| **value == (2, 3))
                .map(|(&id, _)| key(&[id]))
                .collect();
            exact.sort();
            want.sort();
            assert_eq!(exact, want);
            assert!(transaction
                .fresh_probe_statistics(TABLE, INDEX, 1)
                .is_none());
            assert_view(&live, &committed);
        }
        assert_view(&base, &initial);
    }
}

#[test]
fn pinned_and_transaction_index_views_match_canonical_rows() {
    run_view_campaign([0, 1, 0xdead_beef]);
}

#[test]
#[ignore = "complete local index-view differential campaign"]
fn relational_index_view_differential_campaign() {
    run_view_campaign(0..128);
}

#[test]
fn view_epoch_and_capture_admission_leave_the_pinned_reader_unchanged() {
    let (_fixture, base, oracle, _) = Fixture::open();
    assert!(base
        .advance(42, None, RelationalIndexChangeCaptureLimits::default())
        .unwrap_err()
        .contains("expected commit epoch 41"));
    let limits = RelationalIndexChangeCaptureLimits::default();
    assert!(base
        .advance(
            41,
            Some(RelationalIndexChangeCapture::Invalidated {
                reason: "schema changed".into()
            }),
            limits
        )
        .is_err());
    assert!(base
        .advance(
            41,
            Some(RelationalIndexChangeCapture::Captured {
                changes: vec![RelationalIndexChange {
                    table: TABLE.into(),
                    index: INDEX.into(),
                    index_key: key(&[0, 0]),
                    primary_key: key(&[0]),
                    kind: RelationalIndexChangeKind::Delete,
                }],
                encoded_bytes: 1
            }),
            limits
        )
        .is_err());
    let advanced = base.advance(41, None, limits).unwrap();
    assert_eq!(base.identity().visible_commit_epoch, 40);
    assert_eq!(advanced.identity().visible_commit_epoch, 41);
    assert_eq!(
        base.identity().root_set_digest,
        advanced.identity().root_set_digest
    );
    assert_eq!(advanced.residency_report().live_batches, 0);
    assert!(advanced.fresh_probe_statistics(TABLE, INDEX, 1).is_some());
    assert_view(&base, &oracle);
    assert_view(&advanced, &oracle);
}

#[test]
fn authoritative_and_transaction_probes_share_cumulative_row_admission() {
    let (_fixture, base, _, _) = Fixture::open();
    let limits = RelationalIndexReadLimits {
        max_rows: NonZeroUsize::new(2).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let authoritative = AuthoritativeRelationalConstraintIndex::new(Arc::clone(&base), limits);
    let transaction = RelationalTransactionIndexView::new(
        Arc::clone(&base),
        RelationalIndexChangeCaptureLimits::default(),
        limits,
    );
    for reader in [
        &authoritative as &dyn RelationalConstraintIndex,
        &transaction,
    ] {
        for _ in 0..2 {
            let mut rows = Vec::new();
            reader
                .visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |primary| {
                    rows.push(primary.clone());
                    true
                })
                .unwrap();
            assert_eq!(rows, vec![key(&[0])]);
        }
        let error = reader
            .visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |_| {
                panic!("exhausted admission must precede callback")
            })
            .unwrap_err();
        assert!(
            matches!(error, RelationalError::Admission(message) if message.contains("row budget is exhausted"))
        );
    }
}

#[test]
fn authoritative_ledger_counts_base_recovery_and_live_resources() {
    use super::authoritative::AuthoritativeReadLedger;

    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(10).unwrap(),
        max_rows: NonZeroUsize::new(10).unwrap(),
        max_bytes: NonZeroUsize::new(100).unwrap(),
        max_file_bytes: 100,
        ..RelationalIndexReadLimits::default()
    };
    let base = RelationalIndexReadReport {
        pages_read: 1,
        bytes_read: 10,
        file_bytes_read: 10,
        ..RelationalIndexReadReport::default()
    };
    for recovered in [false, true] {
        let backend = if recovered {
            RelationalIndexReadViewBackendReport::Recovered(RelationalIndexRecoveryReadReport {
                base,
                delta_pages_read: 2,
                delta_pages_skipped: 0,
                delta_bytes_read: 20,
                delta_file_pages_read: 2,
                delta_file_bytes_read: 20,
                delta_cache_hits: 0,
                delta_cache_misses: 0,
                delta_cache_admission_rejections: 0,
                delta_entries_visited: 0,
                rows_visited: 2,
                stopped_early: false,
            })
        } else {
            RelationalIndexReadViewBackendReport::Base(base)
        };
        let report = RelationalIndexReadViewReport {
            base_generation: 1,
            delta_generation: recovered.then_some(2),
            base_commit_epoch: 40,
            visible_commit_epoch: 42,
            root_set_digest: String::new(),
            backend,
            live_batches_visited: 1,
            live_entries_visited: 1,
            live_entries_matched: 1,
            live_bytes_visited: 5,
            rows_visited: 2,
            stopped_early: false,
        };
        let ledger = AuthoritativeReadLedger::new(limits);
        for probes in 1..=2 {
            ledger.record(&report).unwrap();
            let remaining = ledger.remaining_limits().unwrap();
            assert_eq!(
                remaining.max_pages.get(),
                10 - probes * if recovered { 3 } else { 1 }
            );
            assert_eq!(remaining.max_rows.get(), 10 - probes * 2);
            assert_eq!(
                remaining.max_bytes.get(),
                100 - probes * if recovered { 35 } else { 15 }
            );
            assert_eq!(
                remaining.max_file_bytes,
                100 - probes * if recovered { 30 } else { 10 }
            );
            assert_eq!(remaining.max_tree_height, limits.max_tree_height);
        }
        if recovered {
            assert!(matches!(
                ledger.record(&report),
                Err(RelationalError::Admission(_))
            ));
            assert!(ledger.remaining_limits().is_err());
        }
    }
}

#[test]
fn live_relational_index_overlay_fails_closed_at_its_cumulative_budget() {
    let change = RelationalIndexChange {
        table: "documents".to_string(),
        index: "documents_owner_idx".to_string(),
        index_key: RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]),
        primary_key: RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]),
        kind: RelationalIndexChangeKind::Insert,
    };
    let limits = RelationalIndexChangeCaptureLimits {
        max_entries: std::num::NonZeroUsize::new(1).unwrap(),
        max_bytes: std::num::NonZeroUsize::new(1_024).unwrap(),
    };

    let result = RelationalIndexLiveOverlay::empty().append(
        1,
        RelationalIndexChangeCapture::Captured {
            changes: vec![change.clone(), change],
            encoded_bytes: 128,
        },
        limits,
    );

    assert!(matches!(
        result,
        Err(reason) if reason.contains("max_entries=1")
    ));
}

#[test]
fn live_relational_index_overlay_selects_only_matching_partition_and_key() {
    let change = |table: &str, index: &str, key: &str, primary_key: &str| RelationalIndexChange {
        table: table.to_string(),
        index: index.to_string(),
        index_key: RelationalKey(vec![RelationalValue::Text(key.to_string())]),
        primary_key: RelationalKey(vec![RelationalValue::Text(primary_key.to_string())]),
        kind: RelationalIndexChangeKind::Insert,
    };
    let changes = vec![
        change("documents", "documents_owner_idx", "owner-a", "doc-1"),
        change("documents", "documents_owner_idx", "owner-b", "doc-2"),
        change("documents", "documents_rank_idx", "rank-1", "doc-1"),
        change("accounts", "accounts_owner_idx", "owner-a", "account-1"),
    ];
    let encoded_bytes = changes
        .iter()
        .map(|change| {
            change
                .estimated_encoded_bytes()
                .expect("encodable live change")
        })
        .sum();
    let overlay = RelationalIndexLiveOverlay::empty()
        .append(
            1,
            RelationalIndexChangeCapture::Captured {
                changes,
                encoded_bytes,
            },
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("partition live changes");
    let batches = overlay
        .batches("documents", "documents_owner_idx")
        .expect("matching partition");
    assert_eq!(batches.len(), 1);
    assert!(overlay.touches("documents", "documents_owner_idx"));
    assert!(overlay
        .batches("documents", "documents_missing_idx")
        .is_none());
    assert!(!overlay.touches("documents", "documents_missing_idx"));
    let owner_b = RelationalKey(vec![RelationalValue::Text("owner-b".to_string())]);
    let mut primary_keys = Vec::new();
    let report = batches[0]
        .visit_selector(RelationalIndexReadSelector::Exact(&owner_b), |change| {
            primary_keys.push(change.primary_key.clone());
            Ok(())
        })
        .expect("select one live key");

    assert_eq!(
        primary_keys,
        vec![RelationalKey(vec![RelationalValue::Text(
            "doc-2".to_string()
        )])]
    );
    assert_eq!(report.entries_visited, 1);
    assert!(report.bytes_visited < overlay.encoded_bytes);
}
