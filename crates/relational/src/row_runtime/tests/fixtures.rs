use super::*;
use skein_storage::{
    encode_projection_relational_member, ImmutableRelationalRowPage,
    ProjectionGenerationBatchLimits, ProjectionGenerationBegin, ProjectionGenerationDigestBuilder,
    ProjectionGenerationIdentity, ProjectionGenerationStore, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRootReader, RelationalRecoveryFence,
    RelationalRecoverySourceIdentity, RelationalRowChangeCapture, RelationalRowDeltaBuilder,
    RelationalRowDeltaConfig, RelationalRowDeltaReader, RelationalRowDeltaTableMetadata,
    RelationalRowPageEntry, RelationalRowPageId, RelationalRowPagePublicationConfig,
    RelationalRowPagePublisher, RelationalRowPageReadView, RelationalRowPageRootReader,
    RelationalRowPageTableDelta, RelationalTransaction, SegmentCache, StoreId,
};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

pub(super) fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

pub(super) fn row(id: i64, body: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::BigInt(id % 3),
        RelationalValue::Text(body.into()),
    ])
}

pub(super) fn state() -> RelationalState {
    let mut state = RelationalState::default();
    apply(
        &mut state,
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, bucket BIGINT, body TEXT)",
        &[],
    );
    for id in 0..8 {
        apply(
            &mut state,
            "INSERT INTO docs (id, bucket, body) VALUES ($1, $2, $3)",
            &[
                Value::Int(id),
                Value::Int(id % 3),
                if id == 2 {
                    Value::Null
                } else {
                    Value::String(format!("body-{id}-\u{e9}"))
                },
            ],
        );
    }
    state
}

pub(super) fn apply(state: &mut RelationalState, sql: &str, parameters: &[Value]) {
    let transaction = crate::compile_relational_statement_sql(sql, parameters, state).unwrap();
    *state = state
        .stage_transaction(transaction, Default::default(), Default::default())
        .unwrap();
}

pub(super) fn fields(sql: &str, state: &RelationalState) -> RelationalFieldPlan {
    let skein_sql::SqlStatement::Select(select) =
        skein_sql::prepare_postgres_sql(sql).unwrap().statement
    else {
        panic!("expected SELECT");
    };
    crate::field_plan::plan_relational_field_plan(&select, state).unwrap()
}

pub(super) struct Fixture {
    pub directory: PathBuf,
    pub state: RelationalState,
    pub root: Arc<RelationalRowPageRootReader>,
    overflow: Arc<RelationalOverflowRootReader>,
    projections: ProjectionGenerationStore,
    pub projection: ProjectionGenerationReader,
    pub tables: BTreeSet<String>,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "skein-row-runtime-{}",
            skein_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let state = state();
        let overflow_config = RelationalOverflowPublicationConfig::default();
        RelationalOverflowPublisher::new(overflow_config)
            .publish(&directory, 1, 10, None, Vec::new())
            .unwrap();
        let overflow = Arc::new(
            RelationalOverflowRootReader::open_latest(&directory, overflow_config)
                .unwrap()
                .unwrap(),
        );
        let schema = state.table_schema("docs").unwrap();
        let schema_digest = state.table_schema_digest("docs").unwrap().unwrap();
        let row_config = RelationalRowPagePublicationConfig::default();
        RelationalRowPagePublisher::new(row_config)
            .publish_with_overflow_root(
                &directory,
                1,
                10,
                None,
                vec![RelationalRowPageTableDelta {
                    table: "docs".into(),
                    schema: Some(schema.clone()),
                    schema_digest,
                    column_count: NonZeroU32::new(3).unwrap(),
                    next_page_id: NonZeroU64::new(2).unwrap(),
                    dirty_pages: vec![ImmutableRelationalRowPage {
                        generation: 1,
                        source_commit_epoch: 10,
                        page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                        schema_digest,
                        column_count: 3,
                        rows: state
                            .rows("docs")
                            .map(|(key, row)| RelationalRowPageEntry {
                                primary_key: key.clone(),
                                row: row.clone(),
                            })
                            .collect(),
                    }],
                    deleted_page_ids: Vec::new(),
                }],
                &overflow,
            )
            .unwrap();
        let root = Arc::new(
            RelationalRowPageRootReader::open_latest(&directory, row_config)
                .unwrap()
                .unwrap(),
        );
        let projections = ProjectionGenerationStore::open(directory.join("projections")).unwrap();
        let projection = publish(
            &projections,
            &state,
            "fixture-v1",
            None,
            state.rows("docs").map(|(_, row)| row.clone()).collect(),
        );
        Self {
            directory,
            state,
            root,
            overflow,
            projections,
            projection,
            tables: BTreeSet::from(["docs".into()]),
        }
    }

    pub fn snapshot(&self) -> RelationalRowPageSnapshotReader {
        self.snapshot_from(RelationalRowPageReadView::from_base(Arc::clone(&self.root)))
    }

    pub fn snapshot_from(
        &self,
        view: RelationalRowPageReadView,
    ) -> RelationalRowPageSnapshotReader {
        RelationalRowPageSnapshotReader::new(
            Arc::new(view),
            Arc::clone(&self.overflow),
            None,
            Arc::new(SegmentCache::new(64 * 1024)),
            StoreId(418),
        )
        .unwrap()
    }

    pub fn live_snapshot(
        &self,
        capture: RelationalRowChangeCapture,
    ) -> RelationalRowPageSnapshotReader {
        let view = RelationalRowPageReadView::from_base(Arc::clone(&self.root))
            .advance(11, Some(capture), Default::default())
            .unwrap();
        self.snapshot_from(view)
    }

    pub fn recovered_snapshot(&self) -> RelationalRowPageSnapshotReader {
        let config = RelationalRowDeltaConfig::default();
        let mut builder = RelationalRowDeltaBuilder::new(
            &self.directory,
            &self.root,
            1,
            None,
            vec![RelationalRowDeltaTableMetadata {
                table: "docs".into(),
                schema_digest: self.state.table_schema_digest("docs").unwrap().unwrap(),
                column_count: NonZeroU32::new(3).unwrap(),
                row_count: 7,
            }],
            config,
        )
        .unwrap();
        let mut recovered = self.state.clone();
        let recovery = capture(
            &mut recovered,
            &[
                ("UPDATE docs SET body = 'recovery-one' WHERE id = 1", &[]),
                ("DELETE FROM docs WHERE id = 2", &[]),
                ("UPDATE docs SET body = 'recovery-five' WHERE id = 5", &[]),
            ],
        );
        builder.record(11, recovery).unwrap();
        // The fixture supplies a synthetic but stable recovery fence, not a WAL replay claim.
        let source = RelationalRecoverySourceIdentity {
            wal_generation: 1,
            start_lsn: 10,
            end_lsn: 11,
            record_sequence_sha256: "42".repeat(32).parse().unwrap(),
        };
        builder
            .finish_with_state(11, source, None, &recovered)
            .unwrap();
        let delta = Arc::new(
            RelationalRowDeltaReader::open_latest_with_recovery_fence(
                &self.directory,
                &self.root,
                RelationalRecoveryFence::new(11, source),
                config,
            )
            .unwrap()
            .unwrap(),
        );
        let live = capture(
            &mut recovered,
            &[
                ("UPDATE docs SET body = 'live-one' WHERE id = 1", &[]),
                ("DELETE FROM docs WHERE id = 3", &[]),
                (
                    "INSERT INTO docs (id, bucket, body) VALUES (8, 2, 'live-eight')",
                    &[],
                ),
            ],
        );
        let view = RelationalRowPageReadView::from_recovery_delta(Arc::clone(&self.root), delta)
            .unwrap()
            .advance(12, Some(live), Default::default())
            .unwrap();
        self.snapshot_from(view)
    }

    pub fn runtime<'a>(
        &'a self,
        mode: usize,
        sql: &str,
        task: &'a RuntimeTaskContext,
    ) -> RelationalRowRuntime<'a> {
        RelationalRowRuntime::new(
            &self.state,
            (mode == 1).then(|| self.snapshot()),
            (mode == 2).then_some((&self.projection, &self.tables)),
            fields(sql, &self.state),
            Default::default(),
            Default::default(),
            task,
        )
    }

    pub fn replace_projection(&self) -> ProjectionGenerationReader {
        publish(
            &self.projections,
            &self.state,
            "fixture-next",
            Some("fixture-v1"),
            vec![row(4, "replacement")],
        )
    }

    pub fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

pub(super) fn capture(
    state: &mut RelationalState,
    statements: &[(&str, &[Value])],
) -> RelationalRowChangeCapture {
    let mut transaction = RelationalTransaction::default();
    for (sql, parameters) in statements {
        transaction.writes.extend(
            crate::compile_relational_statement_sql(sql, parameters, state)
                .unwrap()
                .writes,
        );
    }
    let (updated, captured) = state
        .stage_transaction_with_row_changes(
            transaction,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
    *state = updated;
    captured
}

fn publish(
    store: &ProjectionGenerationStore,
    state: &RelationalState,
    generation: &str,
    expected_head: Option<&str>,
    rows: Vec<RelationalRow>,
) -> ProjectionGenerationReader {
    let mut writer = store
        .begin_candidate(
            ProjectionGenerationBegin {
                identity: ProjectionGenerationIdentity {
                    projection: "row-runtime".into(),
                    owner_key: b"fixture".to_vec(),
                    generation: generation.into(),
                },
                source_watermark: 10,
                projection_version: 1,
                expected_head: expected_head.map(str::to_owned),
            },
            ProjectionGenerationBatchLimits::default(),
        )
        .unwrap();
    let members = rows
        .into_iter()
        .map(|row| {
            encode_projection_relational_member(state.table_schema("docs").unwrap(), row).unwrap()
        })
        .collect::<Vec<_>>();
    let mut digest = ProjectionGenerationDigestBuilder::default();
    for member in &members {
        digest.update(member).unwrap();
    }
    writer.append_batch(&members).unwrap();
    let sealed = writer.seal(digest.finish()).unwrap();
    store.publish(&sealed).unwrap();
    store.open_active("row-runtime", b"fixture").unwrap()
}
