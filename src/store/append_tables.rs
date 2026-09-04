use super::*;

impl GraphStore {
    pub(crate) fn append_state(&self) -> &skein_storage::AppendState {
        &self.append_state
    }

    pub fn append_transaction(&mut self, transaction: AppendTransaction) -> Result<u64> {
        let mut catalog = Catalog::default();
        self.commit_kernel_write_batch(
            &mut catalog,
            KernelWriteBatch {
                append: transaction,
                ..KernelWriteBatch::default()
            },
            MutationLimits::default(),
        )?;
        Ok(self.commit_epoch)
    }

    pub fn read_append_partition(
        &self,
        table: &str,
        partition: &skein_storage::RelationalKey,
        after: Option<&skein_storage::RelationalKey>,
        max_rows: usize,
    ) -> Result<AppendSegmentReadOutput> {
        self.read_append_partition_bounded(table, partition, after, max_rows, usize::MAX)
    }

    pub fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &skein_storage::RelationalKey,
        after: Option<&skein_storage::RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput> {
        self.read_append_partition_from_state_bounded(
            &self.append_state,
            table,
            partition,
            after,
            max_rows,
            max_payload_bytes,
        )
    }

    pub(crate) fn read_append_partition_from_state_bounded(
        &self,
        append_state: &skein_storage::AppendState,
        table: &str,
        partition: &skein_storage::RelationalKey,
        after: Option<&skein_storage::RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput> {
        self.ensure_usable()?;
        if append_state.schema(table).is_none() {
            return Err(SkeinError::Storage(format!("unknown append table {table}")));
        }
        let mut output = match self.append_generation_reader.as_ref() {
            Some(reader) => reader
                .read_partition_bounded(table, partition, after, max_rows, max_payload_bytes)
                .map_err(|error| SkeinError::Storage(error.to_string()))?,
            None => AppendSegmentReadOutput {
                rows: Vec::new(),
                report: Default::default(),
            },
        };
        if output.rows.len() == max_rows {
            return Ok(output);
        }
        let remaining = max_rows - output.rows.len();
        let live = append_state
            .visit_live_rows(table, partition, after, remaining, |row| {
                let row_payload_bytes = append_row_payload_bytes(&row.row)?;
                let next_payload_bytes = output
                    .report
                    .output_payload_bytes
                    .checked_add(row_payload_bytes)
                    .ok_or_else(|| {
                        skein_storage::AppendTableError::Admission(
                            "append read payload size overflow".to_string(),
                        )
                    })?;
                if next_payload_bytes > max_payload_bytes {
                    return Err(skein_storage::AppendTableError::Admission(format!(
                        "append read produced {next_payload_bytes} payload bytes, exceeding limit {max_payload_bytes}"
                    )));
                }
                output.rows.push(row.clone());
                output.report.output_payload_bytes = next_payload_bytes;
                Ok(())
            })
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        merge_live_read_report(
            &mut output.report,
            live.rows_returned,
            live.batches_examined,
            live.batches_pruned,
            live.rows_examined,
        );
        Ok(output)
    }

    pub fn append_table_schema(&self, table: &str) -> Option<&AppendTableSchema> {
        self.append_state.schema(table)
    }

    pub fn append_storage_residency_report(&self) -> AppendStorageResidencyReport {
        let (
            canonical_segment_count,
            canonical_segment_bytes,
            resident_segment_payload_bytes,
            resident_descriptor_count,
        ) = self
            .append_generation_reader
            .as_ref()
            .map_or((0, 0, 0, 0), |reader| {
                (
                    reader.segment_bindings().len(),
                    reader
                        .segment_bindings()
                        .iter()
                        .map(|binding| binding.artifact.encoded_len)
                        .fold(0, u64::saturating_add),
                    reader.segment_payload_resident_bytes(),
                    reader.descriptor_count(),
                )
            });
        AppendStorageResidencyReport {
            canonical_segment_count,
            canonical_segment_bytes,
            resident_segment_payload_bytes,
            resident_descriptor_count,
            live_rows: self.append_state.live_rows(),
            live_payload_bytes: self.append_state.live_payload_bytes(),
        }
    }
}

fn append_row_payload_bytes(
    row: &skein_storage::RelationalRow,
) -> std::result::Result<usize, skein_storage::AppendTableError> {
    row.values().iter().try_fold(0usize, |total, value| {
        let value_bytes = value
            .estimated_payload_bytes()
            .checked_add(1)
            .ok_or_else(append_read_payload_overflow)?;
        total
            .checked_add(value_bytes)
            .ok_or_else(append_read_payload_overflow)
    })
}

fn append_read_payload_overflow() -> skein_storage::AppendTableError {
    skein_storage::AppendTableError::Admission(
        "append read payload size overflows usize".to_string(),
    )
}

fn merge_live_read_report(
    report: &mut skein_storage::AppendSegmentReadReport,
    rows_returned: usize,
    batches_examined: usize,
    batches_pruned: usize,
    rows_examined: usize,
) {
    report.rows_returned = report.rows_returned.saturating_add(rows_returned);
    report.live_batches_examined = report
        .live_batches_examined
        .saturating_add(batches_examined);
    report.live_batches_pruned = report.live_batches_pruned.saturating_add(batches_pruned);
    report.live_rows_examined = report.live_rows_examined.saturating_add(rows_examined);
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::{
        RelationalColumnSchema, RelationalInsertMode, RelationalKey, RelationalRow,
        RelationalScalarType, RelationalTableSchema, RelationalValue, RelationalWrite,
    };
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_dir(name: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "skein-append-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn schema() -> AppendTableSchema {
        AppendTableSchema {
            name: "events".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "stream".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "sequence".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
            ],
            partition_key: vec!["stream".to_string()],
            order_key: vec!["sequence".to_string()],
            order_mode: Default::default(),
        }
    }

    fn row(sequence: i64) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::Text("alpha".to_string()),
            RelationalValue::BigInt(sequence),
        ])
    }

    fn relational_schema() -> RelationalTableSchema {
        RelationalTableSchema {
            name: "metadata".to_string(),
            columns: vec![RelationalColumnSchema {
                name: "id".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            }],
            primary_key: vec!["id".to_string()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn large_value_schema() -> AppendTableSchema {
        AppendTableSchema {
            name: "large_events".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "stream".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "sequence".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "payload".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
            ],
            partition_key: vec!["stream".to_string()],
            order_key: vec!["sequence".to_string()],
            order_mode: Default::default(),
        }
    }

    fn unified_batch(sequence: i64, id: &str, create_schemas: bool) -> KernelWriteBatch {
        let mut relational_writes = Vec::new();
        let mut append_writes = Vec::new();
        if create_schemas {
            relational_writes.push(RelationalWrite::CreateTable(relational_schema()));
            append_writes.push(AppendWrite::CreateTable { schema: schema() });
        }
        relational_writes.push(RelationalWrite::Insert {
            table: "metadata".to_string(),
            rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                id.to_string(),
            )])],
            mode: RelationalInsertMode::Error,
        });
        append_writes.push(AppendWrite::Append {
            table: "events".to_string(),
            rows: vec![row(sequence)],
        });
        KernelWriteBatch {
            graph: vec![GraphMutation::CreateNode {
                label: "Marker".to_string(),
                properties: BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]),
            }],
            relational: RelationalTransaction {
                writes: relational_writes,
            },
            append: AppendTransaction {
                writes: append_writes,
            },
        }
    }

    #[test]
    fn typed_append_api_reads_live_rows_in_order() {
        let mut store = GraphStore::default();
        store
            .append_transaction(AppendTransaction {
                writes: vec![
                    AppendWrite::CreateTable { schema: schema() },
                    AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row(1), row(2)],
                    },
                ],
            })
            .expect("append transaction");
        let read = store
            .read_append_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(1)])),
                10,
            )
            .expect("read append partition");

        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0].order_key,
            RelationalKey(vec![RelationalValue::BigInt(2)])
        );
        assert_eq!(read.report.blocks_read, 0);
        assert_eq!(read.report.rows_returned, 1);
    }

    #[test]
    fn checkpoint_reopen_wal_tail_and_backup_restore_preserve_append_rows() {
        let path = test_dir("checkpoint-recovery");
        let backup = test_dir("checkpoint-backup");
        let restored = test_dir("checkpoint-restored");
        let partition = RelationalKey(vec![RelationalValue::Text("alpha".to_string())]);

        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open append store");
            store
                .append_transaction(AppendTransaction {
                    writes: vec![
                        AppendWrite::CreateTable { schema: schema() },
                        AppendWrite::Append {
                            table: "events".to_string(),
                            rows: vec![row(1), row(2)],
                        },
                    ],
                })
                .expect("append checkpoint rows");
            store.checkpoint(&catalog).expect("checkpoint append rows");
            assert!(path.join("append-1.segment.skein").exists());
            assert!(path.join("append-1.manifest.skein").exists());
            store.scrub_storage().expect("scrub append checkpoint");
            store
                .backup_to(&catalog, &backup)
                .expect("back up append checkpoint");
        }

        {
            let mut catalog = Catalog::default();
            let mut store =
                GraphStore::open(&path, &mut catalog).expect("reopen append checkpoint");
            let checkpoint_rows = store
                .read_append_partition("events", &partition, None, 10)
                .expect("read checkpoint append rows");
            assert_eq!(checkpoint_rows.rows.len(), 2);
            assert!(checkpoint_rows.report.blocks_read > 0);
            store
                .append_transaction(AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row(3)],
                    }],
                })
                .expect("append WAL tail row");
        }

        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).expect("replay append WAL tail");
            let rows = store
                .read_append_partition("events", &partition, None, 10)
                .expect("read checkpoint and WAL append rows");
            assert_eq!(
                rows.rows
                    .iter()
                    .map(|row| row.order_key.clone())
                    .collect::<Vec<_>>(),
                vec![
                    RelationalKey(vec![RelationalValue::BigInt(1)]),
                    RelationalKey(vec![RelationalValue::BigInt(2)]),
                    RelationalKey(vec![RelationalValue::BigInt(3)]),
                ]
            );
        }

        restore_storage_backup(&backup, &restored).expect("restore append backup");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&restored, &mut catalog).expect("open append backup");
            let rows = store
                .read_append_partition("events", &partition, None, 10)
                .expect("read restored append rows");
            assert_eq!(rows.rows.len(), 2);
        }

        fs::remove_dir_all(path).expect("remove append store");
        fs::remove_dir_all(backup).expect("remove append backup");
        fs::remove_dir_all(restored).expect("remove restored append store");
    }

    #[test]
    fn pinned_generation_retains_only_its_referenced_append_segments() {
        let path = test_dir("pinned-generation-reclamation");
        let partition = RelationalKey(vec![RelationalValue::Text("alpha".to_string())]);
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).expect("open append store");
        store.append_publication_config.compact_after_segments = 2;
        store
            .append_transaction(AppendTransaction {
                writes: vec![
                    AppendWrite::CreateTable { schema: schema() },
                    AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row(1)],
                    },
                ],
            })
            .expect("append pinned row");
        store
            .checkpoint(&catalog)
            .expect("checkpoint generation one");
        let pinned = store.snapshot();
        let pinned_generations = BTreeSet::from([1]);

        for sequence in 2..=4 {
            store
                .append_transaction(AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row(sequence)],
                    }],
                })
                .expect("append newer row");
            let prepared = store
                .prepare_checkpoint(&catalog)
                .expect("prepare checkpoint")
                .expect("durable checkpoint is available");
            store
                .publish_prepared_checkpoint_with_reader_generations(
                    prepared,
                    Some(pinned.commit_epoch()),
                    &pinned_generations,
                    None,
                )
                .expect("publish checkpoint with pinned generation");
        }

        assert!(path.join("append-1.segment.skein").exists());
        assert!(!path.join("append-2.segment.skein").exists());
        assert!(path.join("append-3.segment.skein").exists());
        assert!(path.join("append-4.segment.skein").exists());
        let pinned_rows = pinned
            .read_append_partition("events", &partition, None, 10)
            .expect("read pinned append generation");
        assert_eq!(pinned_rows.rows.len(), 1);
        assert_eq!(
            pinned_rows.rows[0].order_key,
            RelationalKey(vec![RelationalValue::BigInt(1)])
        );

        drop(pinned);
        store
            .checkpoint(&catalog)
            .expect("checkpoint after pin drop");
        assert!(!path.join("append-1.segment.skein").exists());
        fs::remove_dir_all(path).expect("remove append store");
    }

    #[test]
    fn checkpoint_failpoints_recover_append_from_one_canonical_generation() {
        for (stage, checkpoint_published) in [
            (CheckpointPublishStage::CheckpointPersisted, false),
            (CheckpointPublishStage::WalPrepared, false),
            (CheckpointPublishStage::ManifestPublished, true),
        ] {
            let path = test_dir(&format!("checkpoint-failpoint-{stage:?}"));
            {
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&path, &mut catalog).expect("open append store");
                store
                    .append_transaction(AppendTransaction {
                        writes: vec![
                            AppendWrite::CreateTable { schema: schema() },
                            AppendWrite::Append {
                                table: "events".to_string(),
                                rows: vec![row(1)],
                            },
                        ],
                    })
                    .expect("append before checkpoint failure");
                set_checkpoint_failpoint(Some(stage));
                let error = store
                    .checkpoint(&catalog)
                    .expect_err("checkpoint failpoint must fire");
                set_checkpoint_failpoint(None);
                assert!(error.to_string().contains("injected checkpoint failure"));
            }

            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).expect("recover failed checkpoint");
            let rows = store
                .read_append_partition(
                    "events",
                    &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                    None,
                    10,
                )
                .expect("read recovered append row");
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(
                store.storage_recovery_report().checkpoint_epoch == Some(1),
                checkpoint_published
            );
            assert_eq!(
                path.join("append-1.segment.skein").exists(),
                checkpoint_published
            );
            assert_eq!(
                path.join("append-1.manifest.skein").exists(),
                checkpoint_published
            );
            drop(store);
            fs::remove_dir_all(path).expect("remove failed checkpoint store");
        }
    }

    #[test]
    fn generation_reclamation_retains_cumulatively_referenced_append_segments() {
        let path = test_dir("generation-reclamation");
        let partition = RelationalKey(vec![RelationalValue::Text("alpha".to_string())]);
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open append store");
            for sequence in 1..=3 {
                let writes = if sequence == 1 {
                    vec![
                        AppendWrite::CreateTable { schema: schema() },
                        AppendWrite::Append {
                            table: "events".to_string(),
                            rows: vec![row(sequence)],
                        },
                    ]
                } else {
                    vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row(sequence)],
                    }]
                };
                store
                    .append_transaction(AppendTransaction { writes })
                    .expect("append generation row");
                store
                    .checkpoint(&catalog)
                    .expect("publish append generation");
            }
            assert!(path.join("append-1.segment.skein").exists());
            assert!(!path.join("append-1.manifest.skein").exists());
            assert!(path.join("append-2.segment.skein").exists());
            assert!(path.join("append-2.manifest.skein").exists());
            assert!(path.join("append-3.segment.skein").exists());
            assert!(path.join("append-3.manifest.skein").exists());
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("reopen reclaimed append store");
        let rows = store
            .read_append_partition("events", &partition, None, 10)
            .expect("read across retained append segments");
        assert_eq!(rows.rows.len(), 3);
        drop(store);
        fs::remove_dir_all(path).expect("remove reclaimed append store");
    }

    #[test]
    fn unified_batch_commits_and_recovers_graph_relational_and_append_at_one_epoch() {
        let path = test_dir("unified-batch");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open unified store");
            store
                .commit_kernel_write_batch(
                    &mut catalog,
                    unified_batch(1, "one", true),
                    MutationLimits::default(),
                )
                .expect("commit unified batch");
            assert_eq!(store.commit_epoch(), 1);
            assert_eq!(store.nodes.len(), 1);
            assert_eq!(store.relational_state().row_count("metadata"), 1);
            assert_eq!(
                store
                    .read_append_partition(
                        "events",
                        &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                        None,
                        10,
                    )
                    .expect("read unified append")
                    .rows
                    .len(),
                1
            );
            store
                .checkpoint(&catalog)
                .expect("checkpoint unified batch");
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("recover unified batch");
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert_eq!(store.relational_state().row_count("metadata"), 1);
        assert_eq!(
            store
                .read_append_partition(
                    "events",
                    &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                    None,
                    10,
                )
                .expect("read recovered append")
                .rows
                .len(),
            1
        );
        drop(store);
        fs::remove_dir_all(path).expect("remove unified store");
    }

    #[test]
    fn unified_batch_append_failure_publishes_nothing() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        store
            .commit_kernel_write_batch(
                &mut catalog,
                unified_batch(2, "seed", true),
                MutationLimits::default(),
            )
            .expect("seed unified state");
        let error = store
            .commit_kernel_write_batch(
                &mut catalog,
                unified_batch(1, "rejected", false),
                MutationLimits::default(),
            )
            .expect_err("regressing append key must reject unified batch");
        assert!(error.to_string().contains("order key must increase"));
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert_eq!(store.relational_state().row_count("metadata"), 1);
    }

    #[test]
    fn unified_batch_recovers_all_states_after_post_wal_apply_failure() {
        let path = test_dir("unified-apply-failure");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open unified store");
            set_wal_apply_failpoint(Some(0));
            let error = store
                .commit_kernel_write_batch(
                    &mut catalog,
                    unified_batch(1, "one", true),
                    MutationLimits::default(),
                )
                .expect_err("post-WAL apply failure must surface");
            set_wal_apply_failpoint(None);
            assert!(
                error
                    .to_string()
                    .contains("injected failure while applying a durable WAL batch"),
                "unexpected apply error: {error}"
            );
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("replay unified WAL batch");
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert_eq!(store.relational_state().row_count("metadata"), 1);
        assert_eq!(
            store
                .read_append_partition(
                    "events",
                    &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                    None,
                    10,
                )
                .expect("read replayed append")
                .rows
                .len(),
            1
        );
        drop(store);
        fs::remove_dir_all(path).expect("remove unified recovery store");
    }

    #[test]
    fn large_append_value_round_trips_through_checkpoint_with_hydration_metrics() {
        let path = test_dir("large-value");
        let payload = "payload".repeat(10_000);
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open append store");
            store
                .append_transaction(AppendTransaction {
                    writes: vec![
                        AppendWrite::CreateTable {
                            schema: large_value_schema(),
                        },
                        AppendWrite::Append {
                            table: "large_events".to_string(),
                            rows: vec![RelationalRow::new(vec![
                                RelationalValue::Text("alpha".to_string()),
                                RelationalValue::BigInt(1),
                                RelationalValue::Text(payload.clone()),
                            ])],
                        },
                    ],
                })
                .expect("append large value");
            store.checkpoint(&catalog).expect("checkpoint large value");
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("reopen large value store");
        let read = store
            .read_append_partition(
                "large_events",
                &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                None,
                10,
            )
            .expect("read large append value");
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0].row.values()[2],
            RelationalValue::Text(payload.clone())
        );
        assert_eq!(read.report.overflow_values_hydrated, 1);
        assert_eq!(read.report.overflow_decompressed_bytes, payload.len());
        drop(store);
        fs::remove_dir_all(path).expect("remove large value store");
    }
}
