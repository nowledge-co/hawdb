use super::*;
use crate::{
    RelationalColumnSchema, RelationalComparisonOp, RelationalForeignKeySchema,
    RelationalInsertMode, RelationalKey, RelationalMutationLimits, RelationalOverflowConfig,
    RelationalOverflowPublicationConfig, RelationalOverflowPublisher, RelationalOverflowRootReader,
    RelationalPredicate, RelationalProjectedField, RelationalReferentialAction,
    RelationalRowChangeCapture, RelationalRowPageDemandReadReport,
    RelationalRowPagePublicationConfig, RelationalRowPagePublisher, RelationalRowPageReadView,
    RelationalRowPageRootReader, RelationalScalarType, RelationalSparseLiveStage,
    RelationalTableSchema, RelationalUpdateAssignment, RelationalUpdateValue, RelationalValue,
    RelationalWrite, SegmentCache, StoreId,
};
use skein_core::RuntimeTaskContext;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-row-workspace-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create exclusive fixture directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove owned fixture directory");
    }
}

struct Fixture {
    materialized: RelationalState,
    metadata: RelationalState,
    view: Arc<RelationalRowPageReadView>,
    overflow: Arc<RelationalOverflowRootReader>,
    // Drop immutable readers before removing their files, including on Windows.
    _directory: TestDirectory,
}

impl Fixture {
    fn new(seed: u64) -> Self {
        Self::with_foreign_keys(seed, true)
    }

    fn with_foreign_keys(seed: u64, foreign_keys: bool) -> Self {
        let count = 8 + (seed % 9) as i64;
        let rows = (1..=count)
            .map(|id| {
                let parent = (id > 1).then(|| 1 + ((seed + id as u64) % (id - 1) as u64) as i64);
                row(id, parent, seed)
            })
            .collect();
        let mut table = schema();
        if !foreign_keys {
            table.foreign_keys.clear();
        }
        let materialized = RelationalState::default()
            .stage_transaction(
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(table), insert(rows)],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .expect("create materialized reference state");
        let directory = TestDirectory::new();
        let overflow_config = RelationalOverflowPublicationConfig::default();
        RelationalOverflowPublisher::new(overflow_config)
            .publish(&directory.0, 1, 10, None, Vec::new())
            .unwrap();
        let overflow = Arc::new(
            RelationalOverflowRootReader::open_latest(&directory.0, overflow_config)
                .unwrap()
                .unwrap(),
        );
        let config = RelationalRowPagePublicationConfig::default();
        RelationalRowPagePublisher::new(config)
            .publish_with_overflow_root(
                &directory.0,
                1,
                10,
                None,
                materialized
                    .row_page_snapshot_deltas(1, 10, config)
                    .unwrap(),
                &overflow,
            )
            .unwrap();
        let root = Arc::new(
            RelationalRowPageRootReader::open_latest(&directory.0, config)
                .unwrap()
                .unwrap(),
        );
        let metadata = RelationalState::from_canonical_row_root(root.manifest()).unwrap();
        let view = Arc::new(RelationalRowPageReadView::from_base(root));
        Self {
            materialized,
            metadata,
            view,
            overflow,
            _directory: directory,
        }
    }

    fn reader(&self, view: Arc<RelationalRowPageReadView>) -> RelationalRowPageSnapshotReader {
        RelationalRowPageSnapshotReader::new(
            view,
            Arc::clone(&self.overflow),
            None,
            Arc::new(SegmentCache::new(64 * 1024)),
            StoreId(903),
        )
        .unwrap()
    }

    fn assert_rows(&self, view: Arc<RelationalRowPageReadView>, expected: &RelationalState) {
        let reader = self.reader(view);
        for id in 0..=20 {
            let (projected, _) = reader
                .point_projected(
                    "nodes",
                    &key(id),
                    &[0, 1, 2],
                    RelationalRowPageSnapshotReadLimits::default(),
                    &mut RelationalHydrationBudget::default(),
                    &RuntimeTaskContext::default(),
                )
                .unwrap();
            let actual = projected
                .map(|row| complete_sparse_projected_row("nodes", &[0, 1, 2], row).unwrap());
            assert_eq!(actual.as_ref(), expected.row("nodes", &key(id)), "id={id}");
        }
    }
}

// This oracle uses the fully materialized path, never sparse workspace state.
struct MaterializedIndex<'a>(&'a RelationalState);

impl RelationalConstraintIndex for MaterializedIndex<'_> {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        if index == RELATIONAL_PRIMARY_INDEX_NAME {
            if self.0.row(table, key).is_some() {
                visit(key);
            }
        } else if let Some(posting) = self.0.index_lookup(table, index, key) {
            for primary_key in posting.iter() {
                if !visit(primary_key) {
                    break;
                }
            }
        }
        Ok(())
    }
}

fn options(fast_path: bool) -> RelationalSparseLiveHydrationOptions {
    RelationalSparseLiveHydrationOptions {
        mutation_limits: RelationalMutationLimits::default(),
        overflow_config: RelationalOverflowConfig::default(),
        index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
        row_capture_limits: RelationalRowChangeCaptureLimits::default(),
        monotonic_append_fast_path_enabled: fast_path,
    }
}

fn run_sequence(fixture: &Fixture, seed: u64, fast_path: bool, foreign_keys: bool) {
    let mut reference = fixture.materialized.clone();
    let mut metadata = fixture.metadata.clone();
    let policy = options(fast_path);
    let metrics = Arc::new(RelationalMonotonicAppendMetrics::default());
    let mut rows =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits);
    let transactions = [
        vec![delete(2)],
        vec![insert(vec![row(17, Some(1), seed), row(18, Some(1), seed)])],
        vec![delete(18), insert(vec![row(18, Some(1), seed + 1)])],
        vec![RelationalWrite::UpdateWhere {
            table: "nodes".into(),
            assignments: vec![RelationalUpdateAssignment {
                column: "body".into(),
                value: RelationalUpdateValue::Value(RelationalValue::Text(format!(
                    "updated-{seed}"
                ))),
            }],
            predicate: RelationalPredicate::Compare {
                column: "id".into(),
                op: RelationalComparisonOp::Gte,
                value: RelationalValue::BigInt(1),
            },
        }],
        vec![delete(1)],
        vec![insert(vec![row(19, None, seed), row(20, None, seed)])],
    ];
    for (step, writes) in transactions.into_iter().enumerate() {
        let transaction = RelationalTransaction { writes };
        let (expected, expected_indexes, expected_rows) = reference
            .stage_transaction_with_index_and_row_changes(
                transaction.clone(),
                policy.mutation_limits,
                policy.overflow_config,
                policy.index_capture_limits,
                policy.row_capture_limits,
            )
            .unwrap();
        let index = MaterializedIndex(&reference);
        let (workspace, report, proven_absent) = hydrate_sparse_relational_workspace(
            &metadata,
            fixture.reader(Arc::clone(rows.read_view())),
            &transaction,
            &index,
            policy,
            Arc::clone(&metrics),
        )
        .unwrap_or_else(|error| panic!("seed={seed} fast={fast_path} step={step}: {error}"));
        assert!(report.point_reads + report.range_reads + report.monotonic_append_hits > 0);
        assert_eq!(report.range_reads, usize::from(step == 3));
        let constraint = RelationalProvenAbsenceConstraintIndex::new(&index, &proven_absent);
        let (next_metadata, actual_indexes, actual_rows, replay, _) = metadata
            .stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
                RelationalSparseLiveStage {
                    transaction,
                    hydrated_workspace: workspace.clone(),
                    mutation_limits: policy.mutation_limits,
                    overflow_config: policy.overflow_config,
                    index_capture_limits: policy.index_capture_limits,
                    row_capture_limits: policy.row_capture_limits,
                    constraint_index: &constraint,
                },
            )
            .unwrap();
        assert_eq!(
            actual_indexes, expected_indexes,
            "index capture seed={seed} step={step}"
        );
        assert_eq!(
            actual_rows, expected_rows,
            "row capture seed={seed} step={step}"
        );
        for access in replay.entries() {
            assert!(workspace.iter().any(
                |entry| entry.table == access.table && entry.primary_key == access.primary_key
            ));
        }
        let previous_view = Arc::clone(rows.read_view());
        let previous_identity = previous_view.identity();
        rows = rows.stage_advance(actual_rows).unwrap();
        assert_eq!(previous_view.identity(), previous_identity);
        assert_eq!(
            rows.read_view().identity().visible_commit_epoch,
            previous_identity.visible_commit_epoch + 1
        );
        fixture.assert_rows(previous_view, &reference);
        fixture.assert_rows(Arc::clone(rows.read_view()), &expected);
        assert!(next_metadata.canonical_row_metadata_only());
        assert_eq!(next_metadata.materialized_row_count(), 0);
        assert_eq!(
            next_metadata.row_count("nodes"),
            expected.row_count("nodes")
        );
        metadata = next_metadata;
        reference = expected;
    }
    fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
    if fast_path && !foreign_keys {
        assert!(metrics.hits() > 0);
        assert_eq!(metrics.attempts(), metrics.hits() + metrics.fallbacks());
    } else {
        assert_eq!(metrics.attempts(), 0);
    }
}

fn run_seed(seed: u64) {
    for foreign_keys in [false, true] {
        // Both policies start from the same immutable files. Each sequence
        // independently forks its metadata, row view, oracle and metrics.
        let fixture = Fixture::with_foreign_keys(seed, foreign_keys);
        for fast_path in [false, true] {
            run_sequence(&fixture, seed, fast_path, foreign_keys);
        }
    }
}

#[test]
fn row_workspace_differential_smoke() {
    for seed in [0, 7, 127] {
        run_seed(seed);
    }
}

#[test]
#[ignore = "complete local differential campaign"]
fn row_workspace_differential_campaign() {
    for seed in 0..128 {
        run_seed(seed);
        if (seed + 1) % 32 == 0 {
            eprintln!("row workspace campaign: {} / 128 seeds complete", seed + 1);
        }
    }
}

#[test]
fn workspace_budget_failure_does_not_advance_the_pinned_view() {
    let fixture = Fixture::new(7);
    let mut policy = options(false);
    policy.row_capture_limits.max_entries = NonZeroUsize::new(1).unwrap();
    let identity = fixture.view.identity();
    let result = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &RelationalTransaction {
            writes: vec![delete(1)],
        },
        &MaterializedIndex(&fixture.materialized),
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    );
    assert!(matches!(result, Err(RelationalError::Admission(_))));
    assert_eq!(fixture.view.identity(), identity);
    fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
}

#[test]
fn transaction_checkpoint_and_invalidation_fail_without_advancing() {
    let fixture = Fixture::new(0);
    let rows = RelationalTransactionRowView::new(
        Arc::clone(&fixture.view),
        options(false).row_capture_limits,
    );
    let identity = rows.read_view().identity();
    assert!(matches!(
        rows.stage_advance(RelationalRowChangeCapture::RequiresCheckpoint { tables: vec!["nodes".into()] }),
        Err(RelationalError::Admission(message)) if message.contains("canonical checkpoint for tables nodes")
    ));
    assert!(matches!(
        rows.stage_advance(RelationalRowChangeCapture::Invalidated { reason: "capture unavailable".into() }),
        Err(RelationalError::Admission(message)) if message.contains("capture unavailable")
    ));
    assert_eq!(rows.read_view().identity(), identity);
    fixture.assert_rows(Arc::clone(rows.read_view()), &fixture.materialized);
}

#[test]
fn hydration_accounting_rejects_each_cumulative_limit_and_identity_change() {
    let fixture = Fixture::new(0);
    for budget in ["page", "row", "read-byte", "overlay-entry", "overlay-byte"] {
        let mut hydrator = RelationalSparseLiveHydrator::new(
            &fixture.metadata,
            fixture.reader(Arc::clone(&fixture.view)),
            options(false).row_capture_limits,
            Arc::new(RelationalMonotonicAppendMetrics::default()),
        );
        let limit = NonZeroUsize::new(2).unwrap();
        hydrator.limits.demand.max_pages = limit;
        hydrator.limits.demand.max_rows = limit;
        hydrator.limits.demand.max_bytes = limit;
        hydrator.limits.max_overlay_entries = limit;
        hydrator.limits.max_overlay_bytes = limit;
        let demand = RelationalRowPageDemandReadReport {
            pages_read: usize::from(budget == "page"),
            rows_decoded: usize::from(budget == "row"),
            bytes_read: usize::from(budget == "read-byte"),
            ..RelationalRowPageDemandReadReport::default()
        };
        let identity = fixture.view.identity();
        for _ in 0..2 {
            hydrator
                .record_snapshot_read(
                    identity,
                    &demand,
                    0,
                    usize::from(budget == "overlay-entry"),
                    usize::from(budget == "overlay-byte"),
                )
                .unwrap();
        }
        assert!(
            matches!(hydrator.remaining_limits(), Err(RelationalError::Admission(message)) if message.contains(budget))
        );
        assert!(matches!(
            hydrator.record_snapshot_read(identity, &demand, 0, usize::from(budget == "overlay-entry"), usize::from(budget == "overlay-byte")),
            Err(RelationalError::Admission(message)) if message.contains(budget)
        ));
        let mut changed = identity;
        changed.visible_commit_epoch += 1;
        assert!(matches!(
            hydrator.record_snapshot_read(changed, &RelationalRowPageDemandReadReport::default(), 0, 0, 0),
            Err(RelationalError::Corruption(message)) if message.contains("identity changed")
        ));
    }
}

#[test]
fn proven_absence_applies_only_to_the_exact_table_and_primary_index() {
    struct RecordingIndex(std::cell::Cell<usize>);
    impl RelationalConstraintIndex for RecordingIndex {
        fn visit_exact_primary_keys(
            &self,
            _: &str,
            _: &str,
            key: &RelationalKey,
            visit: &mut dyn FnMut(&RelationalKey) -> bool,
        ) -> Result<(), RelationalError> {
            self.0.set(self.0.get() + 1);
            visit(key);
            Ok(())
        }
    }
    let inner = RecordingIndex(std::cell::Cell::new(0));
    let absent = BTreeSet::from([RelationalReplayAccess {
        table: "nodes".into(),
        primary_key: key(1),
    }]);
    let index = RelationalProvenAbsenceConstraintIndex::new(&inner, &absent);
    for (table, name, id, expected) in [
        ("nodes", RELATIONAL_PRIMARY_INDEX_NAME, 1, 0),
        ("nodes", "secondary", 1, 1),
        ("other", RELATIONAL_PRIMARY_INDEX_NAME, 1, 1),
        ("nodes", RELATIONAL_PRIMARY_INDEX_NAME, 2, 1),
    ] {
        let mut count = 0;
        index
            .visit_exact_primary_keys(table, name, &key(id), &mut |_| {
                count += 1;
                false
            })
            .unwrap();
        assert_eq!(count, expected);
    }
    assert_eq!(inner.0.get(), 3);
}

#[test]
fn partial_and_reordered_projected_rows_fail_closed() {
    for ordinals in [vec![0], vec![1, 0], vec![0, 0], vec![0, 1, 2]] {
        let projected = RelationalProjectedRow {
            primary_key: key(1),
            fields: ordinals
                .into_iter()
                .map(|ordinal| RelationalProjectedField {
                    ordinal,
                    value: RelationalValue::BigInt(1),
                })
                .collect(),
        };
        assert!(matches!(
            complete_sparse_projected_row("nodes", &[0, 1], projected),
            Err(RelationalError::Corruption(_))
        ));
    }
}

fn schema() -> RelationalTableSchema {
    RelationalTableSchema {
        name: "nodes".into(),
        columns: [
            ("id", RelationalScalarType::BigInt, false),
            ("parent", RelationalScalarType::BigInt, true),
            ("body", RelationalScalarType::Text, false),
        ]
        .into_iter()
        .map(|(name, scalar_type, nullable)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable,
            default: None,
        })
        .collect(),
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: vec![RelationalForeignKeySchema {
            columns: vec!["parent".into()],
            referenced_table: "nodes".into(),
            referenced_columns: vec!["id".into()],
            on_delete: RelationalReferentialAction::Cascade,
            on_update: RelationalReferentialAction::NoAction,
        }],
        indexes: Vec::new(),
    }
}

fn row(id: i64, parent: Option<i64>, revision: u64) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        parent.map_or(RelationalValue::Null, RelationalValue::BigInt),
        RelationalValue::Text(format!("row-{id}-{revision}")),
    ])
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn insert(rows: Vec<RelationalRow>) -> RelationalWrite {
    RelationalWrite::Insert {
        table: "nodes".into(),
        rows,
        mode: RelationalInsertMode::Error,
    }
}

fn delete(id: i64) -> RelationalWrite {
    RelationalWrite::DeleteByPrimaryKey {
        table: "nodes".into(),
        keys: vec![key(id)],
    }
}
