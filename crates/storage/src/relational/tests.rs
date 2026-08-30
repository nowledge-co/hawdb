use super::*;

#[test]
fn relational_types_map_to_shared_logical_types() {
    assert_eq!(
        RelationalScalarType::Boolean.logical_type(),
        LogicalType::Boolean
    );
    assert_eq!(
        RelationalScalarType::BigInt.logical_type(),
        LogicalType::Int64
    );
    assert_eq!(
        RelationalScalarType::DoublePrecision.logical_type(),
        LogicalType::Float64
    );
    assert_eq!(RelationalScalarType::Text.logical_type(), LogicalType::Text);
    assert_eq!(
        RelationalScalarType::Bytea.logical_type(),
        LogicalType::Binary
    );
    assert_eq!(RelationalValue::Null.logical_type(), None);
    assert_eq!(
        RelationalValue::Text("payload".into()).logical_type(),
        Some(LogicalType::Text)
    );
}
use std::cell::RefCell;

#[derive(Default)]
struct TestConstraintIndex {
    postings: BTreeMap<(String, String, RelationalKey), Vec<RelationalKey>>,
    failure: Option<RelationalError>,
    lookups: RefCell<Vec<(String, String, RelationalKey)>>,
    visited: RefCell<BTreeMap<(String, String, RelationalKey), usize>>,
}

impl TestConstraintIndex {
    fn from_state(state: &RelationalState) -> Self {
        let mut postings = BTreeMap::<_, Vec<_>>::new();
        for schema in state.table_schemas() {
            for definition in schema.required_index_definitions() {
                let positions = column_positions(schema, &definition.columns)
                    .expect("fixture index columns must exist");
                for (primary_key, row) in state.rows(&schema.name) {
                    let index_key = row_key(row, &positions);
                    if !index_includes_key(&definition, &index_key) {
                        continue;
                    }
                    postings
                        .entry((schema.name.clone(), definition.name.clone(), index_key))
                        .or_default()
                        .push(primary_key.clone());
                }
            }
        }
        Self {
            postings,
            failure: None,
            lookups: RefCell::new(Vec::new()),
            visited: RefCell::new(BTreeMap::new()),
        }
    }

    fn failing(error: RelationalError) -> Self {
        Self {
            failure: Some(error),
            ..Self::default()
        }
    }

    fn looked_up(&self, table: &str, index: &str, key: &RelationalKey) -> bool {
        self.lookups
            .borrow()
            .iter()
            .any(|lookup| lookup.0 == table && lookup.1 == index && lookup.2 == *key)
    }

    fn visited(&self, table: &str, index: &str, key: &RelationalKey) -> usize {
        self.visited
            .borrow()
            .get(&(table.to_string(), index.to_string(), key.clone()))
            .copied()
            .unwrap_or(0)
    }
}

impl RelationalConstraintIndex for TestConstraintIndex {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        self.lookups
            .borrow_mut()
            .push((table.to_string(), index.to_string(), key.clone()));
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if let Some(postings) =
            self.postings
                .get(&(table.to_string(), index.to_string(), key.clone()))
        {
            for primary_key in postings {
                *self
                    .visited
                    .borrow_mut()
                    .entry((table.to_string(), index.to_string(), key.clone()))
                    .or_default() += 1;
                if !visit(primary_key) {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[test]
fn sparse_workspace_builder_admits_entries_and_bytes_atomically() {
    let first = RelationalSparseRecoveryRow {
        table: "documents".to_string(),
        primary_key: RelationalKey(vec![RelationalValue::Text("first".to_string())]),
        row: Some(document_row("first")),
    };
    let first_access = RelationalReplayAccess {
        table: first.table.clone(),
        primary_key: first.primary_key.clone(),
    };
    let first_bytes = relational_sparse_recovery_entry_bytes(&first)
        .unwrap()
        .checked_add(relational_replay_access_resident_bytes(&first_access).unwrap())
        .unwrap();
    let limits = RelationalRowChangeCaptureLimits {
        max_entries: NonZeroUsize::new(2).unwrap(),
        max_bytes: NonZeroUsize::new(first_bytes).unwrap(),
    };
    let mut workspace = RelationalSparseWorkspaceBuilder::new(limits);
    assert!(workspace.insert(first.clone()).unwrap());
    let resident_bytes = workspace.resident_bytes();

    let second = RelationalSparseRecoveryRow {
        table: "documents".to_string(),
        primary_key: RelationalKey(vec![RelationalValue::Text("second".to_string())]),
        row: Some(document_row("second")),
    };
    let error = workspace.insert(second.clone()).unwrap_err();
    assert!(matches!(error, RelationalError::Admission(message) if message.contains("bytes")));
    assert_eq!(workspace.len(), 1);
    assert_eq!(workspace.resident_bytes(), resident_bytes);
    assert!(!workspace.contains(&RelationalReplayAccess {
        table: second.table,
        primary_key: second.primary_key,
    }));
    assert_eq!(workspace.snapshot(), vec![first]);
}

#[test]
fn sparse_workspace_builder_rejects_conflicting_duplicate_hydration() {
    let limits = RelationalRowChangeCaptureLimits {
        max_entries: NonZeroUsize::new(1).unwrap(),
        max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
    };
    let mut workspace = RelationalSparseWorkspaceBuilder::new(limits);
    let missing = RelationalSparseRecoveryRow {
        table: "documents".to_string(),
        primary_key: RelationalKey(vec![RelationalValue::Text("same".to_string())]),
        row: None,
    };
    assert!(workspace.insert(missing.clone()).unwrap());
    assert!(!workspace.insert(missing.clone()).unwrap());
    let error = workspace
        .insert(RelationalSparseRecoveryRow {
            row: Some(document_row("same")),
            ..missing.clone()
        })
        .unwrap_err();
    assert!(
        matches!(error, RelationalError::Corruption(message) if message.contains("conflicting"))
    );
    assert_eq!(workspace.snapshot(), vec![missing]);
}

#[test]
fn snapshots_share_untouched_segments_and_keep_old_rows_visible() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let before = store.snapshot().expect("snapshot before insert");

    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_documents".to_string(),
                    rows: vec![document_row("doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert document");
    let after = store.snapshot().expect("snapshot after insert");

    assert_eq!(before.value().row_count("content_documents"), 0);
    assert_eq!(after.value().row_count("content_documents"), 1);
    assert!(Arc::ptr_eq(
        before.value().segments.get("content_anchors").unwrap(),
        after.value().segments.get("content_anchors").unwrap()
    ));
}

#[test]
fn durability_failure_does_not_publish_relational_rows() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");

    let result = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "content_documents".to_string(),
                rows: vec![document_row("doc-1")],
                mode: RelationalInsertMode::Error,
            }],
        },
        |_, _| Err(RelationalError::Durability("fsync failed".to_string())),
    );

    assert!(matches!(result, Err(SnapshotCommitError::Durability(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("published snapshot")
            .value()
            .row_count("content_documents"),
        0
    );
}

#[test]
fn table_without_primary_key_is_rejected_before_catalog_publication() {
    let store = RelationalStore::default();
    let error = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: Vec::new(),
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            |_, _| Ok(()),
        )
        .expect_err("primary-key-free tables must not enter the catalog");

    assert!(matches!(
        error,
        SnapshotCommitError::Stage(RelationalError::Schema(message))
            if message == "table documents must declare a primary key"
    ));
    assert!(store
        .snapshot()
        .expect("empty snapshot")
        .value()
        .table_schema("documents")
        .is_none());
}

#[test]
fn foreign_key_is_checked_against_final_atomic_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");

    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("target in the same batch is visible");

    assert_eq!(
        store
            .snapshot()
            .expect("snapshot")
            .value()
            .row_count("content_anchors"),
        1
    );
}

#[test]
fn referenced_delete_is_restricted_unless_child_is_removed_in_same_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("seed referenced rows");
    let document_key = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
    let anchor_key = RelationalKey(vec![RelationalValue::Text("anchor-1".to_string())]);

    let rejected = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "content_documents".to_string(),
                keys: vec![document_key.clone()],
            }],
        },
        |_, _| Ok(()),
    );
    assert!(matches!(rejected, Err(SnapshotCommitError::Stage(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot after rejected delete")
            .value()
            .row_count("content_documents"),
        1
    );

    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_documents".to_string(),
                        keys: vec![document_key],
                    },
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_anchors".to_string(),
                        keys: vec![anchor_key],
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("delete parent and child atomically");
    let snapshot = store.snapshot().expect("snapshot after atomic delete");
    assert_eq!(snapshot.value().row_count("content_documents"), 0);
    assert_eq!(snapshot.value().row_count("content_anchors"), 0);
}

#[test]
fn unique_constraint_rejects_complete_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let result = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "content_documents".to_string(),
                rows: vec![document_row("doc-1"), document_row("doc-1")],
                mode: RelationalInsertMode::Error,
            }],
        },
        |_, _| Ok(()),
    );
    assert!(matches!(result, Err(SnapshotCommitError::Stage(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot")
            .value()
            .row_count("content_documents"),
        0
    );
}

#[test]
fn authoritative_constraint_staging_derives_index_and_row_batches_once() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create upsert table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed upsert row");
    let index = TestConstraintIndex::from_state(&base);
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);

    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Upsert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-2", "owner-1", "new")],
            conflict_columns: vec!["owner".to_string()],
            action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                column: "payload".to_string(),
                value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
            }]),
        }],
    };
    let (next, index_capture, row_capture, replay_access) = base
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &index,
        )
        .expect("persistent unique lookup resolves the upsert target");

    let id_1 = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let id_2 = RelationalKey(vec![RelationalValue::Text("id-2".to_string())]);
    assert_eq!(
        replay_access.entries(),
        &[RelationalReplayAccess {
            table: "documents".to_string(),
            primary_key: id_1.clone(),
        }]
    );
    let encoded =
        encode_relational_wal_batch_with_replay_access(3, &transaction, Some(&replay_access))
            .expect("encode authoritative WAL access set");
    let decoded = decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal())
        .expect("decode authoritative WAL access set");
    assert_eq!(decoded.epoch, 3);
    assert_eq!(decoded.transaction, transaction);
    assert_eq!(decoded.replay_access.as_ref(), Some(&replay_access));

    let (recovered, _, _) = base
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            decoded.transaction,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            decoded
                .replay_access
                .as_ref()
                .expect("decoded WAL retains replay access"),
        )
        .expect("recovery accepts the authenticated access set");
    assert_eq!(
        recovered.row("documents", &id_1),
        next.row("documents", &id_1)
    );

    let drifted = RelationalReplayAccessSet::from_decoded_entries(Vec::new())
        .expect("empty access set has canonical ordering");
    let drift = base
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &drifted,
        )
        .expect_err("recovery rejects access-set drift");
    assert!(matches!(
        drift,
        RelationalError::Corruption(message) if message.contains("does not match")
    ));
    assert_eq!(next.row_count("documents"), 1);
    assert_eq!(
        next.row("documents", &id_1)
            .expect("upsert preserves the conflicting primary key")
            .values()[2],
        RelationalValue::Text("new".to_string())
    );
    assert!(next.row("documents", &id_2).is_none());
    assert!(index.looked_up("documents", &relational_unique_index_name(0), &owner));
    assert!(matches!(
        index_capture,
        RelationalIndexChangeCapture::Captured { .. }
    ));
    assert!(matches!(
        row_capture,
        RelationalRowChangeCapture::Captured {
            changes,
            encoded_bytes,
        } if changes.len() == 1
            && changes[0].table == "documents"
            && changes[0].primary_key == id_1
            && changes[0].row.as_ref() == next.row("documents", &id_1)
            && encoded_bytes > 0
    ));
}

#[test]
fn relational_wal_round_trips_bounded_primary_key_changes_without_row_payloads() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create table");
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-1", "owner-1", &"x".repeat(32 * 1024))],
            mode: RelationalInsertMode::Error,
        }],
    };
    let staged = base
        .stage_transaction_with_primary_key_changes(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            None,
            None,
            RelationalPrimaryKeyChangeCaptureLimits::default(),
            None,
        )
        .expect("stage transaction with primary-key changes");
    let RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    } = &staged.primary_key_changes
    else {
        panic!("small primary-key change set must remain incremental");
    };
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table, "documents");
    assert_eq!(tables[0].primary_keys.len(), 1);
    assert!(*encoded_bytes < 128);

    let encoded = encode_relational_wal_batch_with_captures(
        2,
        &transaction,
        None,
        &staged.primary_key_changes,
    )
    .expect("encode relational WAL captures");
    assert_eq!(encoded.primary_key_changes, staged.primary_key_changes);
    let decoded = decode_relational_wal_batch(&encoded.record, RelationalDecodeLimits::wal())
        .expect("decode relational WAL captures");
    assert_eq!(
        decoded.primary_key_changes,
        Some(staged.primary_key_changes)
    );
}

#[test]
fn primary_key_change_capture_reports_exact_net_identity_changes() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create primary-key change capture table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed primary-key change capture table");

    let staged = base
        .stage_transaction_with_primary_key_changes(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::UpdateWhere {
                        table: "documents".to_string(),
                        assignments: vec![RelationalUpdateAssignment {
                            column: "id".to_string(),
                            value: RelationalUpdateValue::Value(RelationalValue::Text(
                                "id-3".to_string(),
                            )),
                        }],
                        predicate: RelationalPredicate::Compare {
                            column: "id".to_string(),
                            op: RelationalComparisonOp::Eq,
                            value: RelationalValue::Text("id-1".to_string()),
                        },
                    },
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-2", "owner-2", "transient")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "documents".to_string(),
                        keys: vec![RelationalKey(vec![RelationalValue::Text(
                            "id-2".to_string(),
                        )])],
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            None,
            None,
            RelationalPrimaryKeyChangeCaptureLimits::default(),
            None,
        )
        .expect("capture exact net primary-key changes");

    assert!(matches!(
        staged.primary_key_changes,
        RelationalPrimaryKeyChangeCapture::Captured { tables, .. }
            if tables
                == vec![RelationalTablePrimaryKeyChanges {
                    table: "documents".to_string(),
                    primary_keys: vec![
                        RelationalKey(vec![RelationalValue::Text("id-1".to_string())]),
                        RelationalKey(vec![RelationalValue::Text("id-3".to_string())]),
                    ],
                }]
    ));
}

#[test]
fn sparse_authoritative_recovery_matches_materialized_predicate_replay() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create upsert table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "keep"),
                        upsert_row("id-2", "owner-2", "drop"),
                        upsert_row("id-3", "owner-3", "keep"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed rows");
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::DeleteWhere {
            table: "documents".to_string(),
            predicate: RelationalPredicate::Compare {
                column: "payload".to_string(),
                op: RelationalComparisonOp::Eq,
                value: RelationalValue::Text("drop".to_string()),
            },
        }],
    };
    let index = TestConstraintIndex::from_state(&base);
    let (materialized, expected_index_capture, expected_row_capture, access) = base
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &index,
        )
        .expect("materialized staging derives the authenticated access set");
    assert_eq!(access.entries().len(), 3);

    let hydrated = access
        .entries()
        .iter()
        .map(|entry| RelationalSparseRecoveryRow {
            table: entry.table.clone(),
            primary_key: entry.primary_key.clone(),
            row: base.row(&entry.table, &entry.primary_key).cloned(),
        })
        .collect::<Vec<_>>();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-sparse-relational-recovery-{}-{nonce}",
        std::process::id()
    ));
    let row_page_config = RelationalRowPagePublicationConfig::default();
    let deltas = base
        .row_page_snapshot_deltas(1, 1, row_page_config)
        .expect("pack canonical row pages");
    RelationalRowPagePublisher::new(row_page_config)
        .publish(&directory, 1, 1, None, deltas)
        .expect("publish canonical row root");
    let row_root = RelationalRowPageRootReader::open_generation(&directory, 1, row_page_config)
        .expect("open canonical row root");
    let metadata = RelationalState::from_canonical_row_root(row_root.manifest())
        .expect("mount canonical row metadata");
    let (recovered, index_capture, row_capture) = metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
            RelationalSparseRecoveryStage {
                transaction: transaction.clone(),
                hydrated_access: hydrated.clone(),
                mutation_limits: RelationalMutationLimits::default(),
                overflow_config: RelationalOverflowConfig::default(),
                index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
                row_capture_limits: RelationalRowChangeCaptureLimits::default(),
                expected_replay_access: &access,
            },
        )
        .expect("sparse recovery replays the exact authenticated workspace");
    assert!(!recovered.materialized_rows_resident());
    assert!(recovered.canonical_row_metadata_only());
    assert_eq!(recovered.materialized_row_count(), 0);
    assert_eq!(
        recovered.row_count("documents"),
        materialized.row_count("documents")
    );
    assert_eq!(recovered.total_row_count(), materialized.total_row_count());
    assert_eq!(index_capture, expected_index_capture);
    assert_eq!(row_capture, expected_row_capture);

    let missing = metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
            RelationalSparseRecoveryStage {
                transaction: transaction.clone(),
                hydrated_access: hydrated[..2].to_vec(),
                mutation_limits: RelationalMutationLimits::default(),
                overflow_config: RelationalOverflowConfig::default(),
                index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
                row_capture_limits: RelationalRowChangeCaptureLimits::default(),
                expected_replay_access: &access,
            },
        )
        .expect_err("sparse recovery rejects an incomplete hydration set");
    assert!(matches!(
        missing,
        RelationalError::Corruption(message) if message.contains("does not exactly cover")
    ));

    let over_budget = metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
            RelationalSparseRecoveryStage {
                transaction,
                hydrated_access: hydrated,
                mutation_limits: RelationalMutationLimits::default(),
                overflow_config: RelationalOverflowConfig::default(),
                index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
                row_capture_limits: RelationalRowChangeCaptureLimits {
                    max_entries: std::num::NonZeroUsize::new(3).unwrap(),
                    max_bytes: std::num::NonZeroUsize::new(1).unwrap(),
                },
                expected_replay_access: &access,
            },
        )
        .expect_err("sparse recovery enforces its resident-byte budget");
    assert!(matches!(
        over_budget,
        RelationalError::Admission(message) if message.contains("workspace requires")
    ));
    std::fs::remove_dir_all(directory).expect("remove sparse recovery fixture");
}

#[test]
fn sparse_live_staging_validates_constraints_without_counting_support_rows() {
    let base = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create foreign-key tables")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_documents".to_string(),
                    rows: vec![document_row("doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed foreign-key target");
    let constraint_index = TestConstraintIndex::from_state(&base);
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "content_anchors".to_string(),
            rows: vec![anchor_row("anchor-1", "doc-1")],
            mode: RelationalInsertMode::Error,
        }],
    };
    let (materialized, expected_index_capture, expected_row_capture, expected_access) = base
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &constraint_index,
        )
        .expect("materialized oracle validates the foreign key");

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-sparse-relational-live-{}-{nonce}",
        std::process::id()
    ));
    let row_page_config = RelationalRowPagePublicationConfig::default();
    let deltas = base
        .row_page_snapshot_deltas(1, 1, row_page_config)
        .expect("pack canonical row pages");
    RelationalRowPagePublisher::new(row_page_config)
        .publish(&directory, 1, 1, None, deltas)
        .expect("publish canonical row root");
    let row_root = RelationalRowPageRootReader::open_generation(&directory, 1, row_page_config)
        .expect("open canonical row root");
    let metadata = RelationalState::from_canonical_row_root(row_root.manifest())
        .expect("mount canonical row metadata");
    let anchor_key = RelationalKey(vec![RelationalValue::Text("anchor-1".to_string())]);
    let document_key = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
    let hydrated_workspace = vec![
        RelationalSparseRecoveryRow {
            table: "content_anchors".to_string(),
            primary_key: anchor_key.clone(),
            row: None,
        },
        RelationalSparseRecoveryRow {
            table: "content_documents".to_string(),
            primary_key: document_key.clone(),
            row: base.row("content_documents", &document_key).cloned(),
        },
    ];
    let (staged, index_capture, row_capture, replay_access) = metadata
        .stage_sparse_transaction_with_authoritative_replay_access(RelationalSparseLiveStage {
            transaction: transaction.clone(),
            hydrated_workspace: hydrated_workspace.clone(),
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
            constraint_index: &constraint_index,
        })
        .expect("sparse live staging validates from a support row");

    assert!(!staged.materialized_rows_resident());
    assert!(staged.canonical_row_metadata_only());
    assert_eq!(staged.materialized_row_count(), 0);
    assert_eq!(staged.total_row_count(), materialized.total_row_count());
    assert_eq!(staged.row_count("content_documents"), 1);
    assert_eq!(staged.row_count("content_anchors"), 1);
    assert_eq!(index_capture, expected_index_capture);
    assert_eq!(row_capture, expected_row_capture);
    assert_eq!(replay_access, expected_access);
    assert_eq!(
        replay_access.entries(),
        &[RelationalReplayAccess {
            table: "content_anchors".to_string(),
            primary_key: anchor_key.clone(),
        }]
    );

    let missing_constraint_row = metadata
        .stage_sparse_transaction_with_authoritative_replay_access(RelationalSparseLiveStage {
            transaction: transaction.clone(),
            hydrated_workspace: hydrated_workspace[..1].to_vec(),
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
            constraint_index: &constraint_index,
        })
        .expect_err("sparse live staging rejects an unhydrated foreign-key target");
    assert!(
        matches!(
            &missing_constraint_row,
            RelationalError::Corruption(message) if message.contains("missing row")
        ),
        "unexpected error: {missing_constraint_row:?}"
    );

    let missing_mutation_key = metadata
        .stage_sparse_transaction_with_authoritative_replay_access(RelationalSparseLiveStage {
            transaction,
            hydrated_workspace: hydrated_workspace[1..].to_vec(),
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
            constraint_index: &constraint_index,
        })
        .expect_err("sparse live staging rejects replay access outside the workspace");
    assert!(matches!(
        missing_mutation_key,
        RelationalError::Corruption(message) if message.contains("did not hydrate replay access")
    ));
    assert_eq!(metadata.row_count("content_documents"), 1);
    assert_eq!(metadata.row_count("content_anchors"), 0);
    std::fs::remove_dir_all(directory).expect("remove sparse live fixture");
}

#[test]
fn sparse_live_staging_hydrates_authoritative_unique_conflicts() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create unique table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed unique row");
    let constraint_index = TestConstraintIndex::from_state(&base);
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Upsert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-2", "owner-1", "new")],
            conflict_columns: vec!["owner".to_string()],
            action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                column: "payload".to_string(),
                value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
            }]),
        }],
    };
    let (materialized, expected_index_capture, expected_row_capture, expected_access) = base
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &constraint_index,
        )
        .expect("materialized oracle resolves the unique conflict");

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-sparse-relational-unique-live-{}-{nonce}",
        std::process::id()
    ));
    let row_page_config = RelationalRowPagePublicationConfig::default();
    let deltas = base
        .row_page_snapshot_deltas(1, 1, row_page_config)
        .expect("pack canonical row pages");
    RelationalRowPagePublisher::new(row_page_config)
        .publish(&directory, 1, 1, None, deltas)
        .expect("publish canonical row root");
    let row_root = RelationalRowPageRootReader::open_generation(&directory, 1, row_page_config)
        .expect("open canonical row root");
    let metadata = RelationalState::from_canonical_row_root(row_root.manifest())
        .expect("mount canonical row metadata");
    let id_1 = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let id_2 = RelationalKey(vec![RelationalValue::Text("id-2".to_string())]);
    let hydrated_workspace = vec![
        RelationalSparseRecoveryRow {
            table: "documents".to_string(),
            primary_key: id_1.clone(),
            row: base.row("documents", &id_1).cloned(),
        },
        RelationalSparseRecoveryRow {
            table: "documents".to_string(),
            primary_key: id_2,
            row: None,
        },
    ];
    let (staged, index_capture, row_capture, replay_access) = metadata
        .stage_sparse_transaction_with_authoritative_replay_access(RelationalSparseLiveStage {
            transaction: transaction.clone(),
            hydrated_workspace: hydrated_workspace.clone(),
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
            constraint_index: &constraint_index,
        })
        .expect("sparse live staging resolves the unique conflict");
    assert_eq!(
        staged.row_count("documents"),
        materialized.row_count("documents")
    );
    assert_eq!(staged.total_row_count(), materialized.total_row_count());
    assert_eq!(index_capture, expected_index_capture);
    assert_eq!(row_capture, expected_row_capture);
    assert_eq!(replay_access, expected_access);
    assert_eq!(
        replay_access.entries(),
        &[RelationalReplayAccess {
            table: "documents".to_string(),
            primary_key: id_1,
        }]
    );

    let missing_conflict = metadata
        .stage_sparse_transaction_with_authoritative_replay_access(RelationalSparseLiveStage {
            transaction,
            hydrated_workspace: hydrated_workspace[1..].to_vec(),
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
            constraint_index: &constraint_index,
        })
        .expect_err("sparse live staging rejects an unhydrated unique conflict");
    assert!(matches!(
        missing_conflict,
        RelationalError::Corruption(message) if message.contains("stale primary key")
    ));
    std::fs::remove_dir_all(directory).expect("remove sparse unique live fixture");
}

#[test]
fn sparse_mutation_hydration_plan_separates_points_scans_and_upsert_probes() {
    let state = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create content tables")
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create upsert table");
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::Insert {
                table: "content_anchors".to_string(),
                rows: vec![anchor_row("anchor-1", "doc-1")],
                mode: RelationalInsertMode::Error,
            },
            RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-2", "owner-1", "new")],
                conflict_columns: vec!["owner".to_string()],
                action: RelationalConflictAction::DoNothing,
            },
            RelationalWrite::UpdateWhere {
                table: "documents".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "payload".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text(
                        "updated".to_string(),
                    )),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "owner".to_string(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("owner-1".to_string()),
                },
            },
        ],
    };

    let plan = state
        .plan_sparse_transaction_hydration(&transaction)
        .expect("derive sparse mutation hydration plan");
    assert_eq!(
        plan.point_access(),
        &[RelationalReplayAccess {
            table: "content_anchors".to_string(),
            primary_key: RelationalKey(vec![RelationalValue::Text("anchor-1".to_string())]),
        }]
    );
    assert_eq!(plan.scan_tables(), &["documents".to_string()]);
    assert_eq!(
        plan.index_probes(),
        &[RelationalSparseIndexProbe {
            table: "documents".to_string(),
            index: relational_unique_index_name(0),
            index_key: RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]),
        }]
    );
}

#[test]
fn sparse_mutation_hydration_plan_groups_monotonic_primary_keys_by_partition() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
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
                        RelationalColumnSchema {
                            name: "payload".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                    ],
                    primary_key: vec!["stream".to_string(), "sequence".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create partitioned event table");
    let event = |stream: &str, sequence: i64| {
        RelationalRow::new(vec![
            RelationalValue::Text(stream.to_string()),
            RelationalValue::BigInt(sequence),
            RelationalValue::Text(format!("event-{sequence}")),
        ])
    };
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "events".to_string(),
            rows: vec![event("a", 2), event("a", 3), event("b", 8), event("b", 9)],
            mode: RelationalInsertMode::Error,
        }],
    };

    let plan = state
        .plan_sparse_transaction_hydration(&transaction)
        .expect("derive monotonic append hydration plan");

    assert!(plan.point_access().is_empty());
    assert_eq!(plan.monotonic_appends().len(), 2);
    assert_eq!(
        plan.monotonic_appends()[0],
        RelationalMonotonicAppendHydration {
            table: "events".to_string(),
            partition_prefix: RelationalKey(vec![RelationalValue::Text("a".to_string())]),
            primary_keys: vec![
                RelationalKey(vec![
                    RelationalValue::Text("a".to_string()),
                    RelationalValue::BigInt(2),
                ]),
                RelationalKey(vec![
                    RelationalValue::Text("a".to_string()),
                    RelationalValue::BigInt(3),
                ]),
            ],
        }
    );
}

#[test]
fn sparse_mutation_hydration_plan_falls_back_for_out_of_order_insert() {
    let state = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create table");
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "content_documents".to_string(),
            rows: vec![document_row("id-2"), document_row("id-1")],
            mode: RelationalInsertMode::Error,
        }],
    };

    let plan = state
        .plan_sparse_transaction_hydration(&transaction)
        .expect("derive fallback hydration plan");

    assert!(plan.monotonic_appends().is_empty());
    assert_eq!(plan.point_access().len(), 2);
}

#[test]
fn sparse_mutation_hydration_plan_falls_back_for_unique_and_foreign_key_probes() {
    let state = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create content tables")
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create unique table");
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![
                    upsert_row("id-1", "owner-1", "one"),
                    upsert_row("id-2", "owner-2", "two"),
                ],
                mode: RelationalInsertMode::Error,
            },
            RelationalWrite::Insert {
                table: "content_anchors".to_string(),
                rows: vec![
                    anchor_row("anchor-1", "doc-1"),
                    anchor_row("anchor-2", "doc-2"),
                ],
                mode: RelationalInsertMode::Error,
            },
        ],
    };

    let plan = state
        .plan_sparse_transaction_hydration(&transaction)
        .expect("derive constrained fallback hydration plan");

    assert!(plan.monotonic_appends().is_empty());
    assert_eq!(plan.point_access().len(), 4);
}

#[test]
fn sparse_live_preparation_discovers_foreign_key_constraint_probes() {
    let base = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create content tables")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed content rows");
    let (directory, metadata) = canonical_metadata_state(&base, "live-preparation-fk");
    let document_key = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
    let delete = RelationalTransaction {
        writes: vec![RelationalWrite::DeleteByPrimaryKey {
            table: "content_documents".to_string(),
            keys: vec![document_key.clone()],
        }],
    };
    let delete_preparation = metadata
        .prepare_sparse_transaction_for_authoritative_live(RelationalSparseLivePreparationStage {
            transaction: delete,
            hydrated_workspace: vec![RelationalSparseRecoveryRow {
                table: "content_documents".to_string(),
                primary_key: document_key.clone(),
                row: base.row("content_documents", &document_key).cloned(),
            }],
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
        })
        .expect("prepare sparse parent deletion");
    assert_eq!(
        delete_preparation.replay_access().entries(),
        &[RelationalReplayAccess {
            table: "content_documents".to_string(),
            primary_key: document_key.clone(),
        }]
    );
    assert_eq!(
        delete_preparation.constraint_probes(),
        &[
            RelationalSparseIndexProbe {
                table: "content_anchors".to_string(),
                index: relational_foreign_key_index_name(0),
                index_key: document_key.clone(),
            },
            RelationalSparseIndexProbe {
                table: "content_documents".to_string(),
                index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                index_key: document_key.clone(),
            },
        ]
    );

    let anchor_2 = RelationalKey(vec![RelationalValue::Text("anchor-2".to_string())]);
    let insert_preparation = metadata
        .prepare_sparse_transaction_for_authoritative_live(RelationalSparseLivePreparationStage {
            transaction: RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_anchors".to_string(),
                    rows: vec![anchor_row("anchor-2", "doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            hydrated_workspace: vec![RelationalSparseRecoveryRow {
                table: "content_anchors".to_string(),
                primary_key: anchor_2,
                row: None,
            }],
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
        })
        .expect("prepare sparse child insertion");
    assert!(insert_preparation
        .constraint_probes()
        .contains(&RelationalSparseIndexProbe {
            table: "content_documents".to_string(),
            index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
            index_key: document_key,
        }));
    std::fs::remove_dir_all(directory).expect("remove live preparation fixture");
}

#[test]
fn sparse_live_preparation_exposes_new_primary_key_hydration() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed row");
    let (directory, metadata) = canonical_metadata_state(&base, "live-preparation-primary-key");
    let id_1 = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let id_2 = RelationalKey(vec![RelationalValue::Text("id-2".to_string())]);
    let preparation = metadata
        .prepare_sparse_transaction_for_authoritative_live(RelationalSparseLivePreparationStage {
            transaction: RelationalTransaction {
                writes: vec![RelationalWrite::UpdateWhere {
                    table: "documents".to_string(),
                    assignments: vec![RelationalUpdateAssignment {
                        column: "id".to_string(),
                        value: RelationalUpdateValue::Value(RelationalValue::Text(
                            "id-2".to_string(),
                        )),
                    }],
                    predicate: RelationalPredicate::Compare {
                        column: "owner".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("owner-1".to_string()),
                    },
                }],
            },
            hydrated_workspace: vec![RelationalSparseRecoveryRow {
                table: "documents".to_string(),
                primary_key: id_1.clone(),
                row: base.row("documents", &id_1).cloned(),
            }],
            mutation_limits: RelationalMutationLimits::default(),
            overflow_config: RelationalOverflowConfig::default(),
            index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
            row_capture_limits: RelationalRowChangeCaptureLimits::default(),
        })
        .expect("prepare primary-key update");
    assert_eq!(
        preparation.replay_access().entries(),
        &[
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: id_1,
            },
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: id_2,
            },
        ]
    );
    std::fs::remove_dir_all(directory).expect("remove primary-key fixture");
}

#[test]
fn authoritative_constraint_staging_merges_transaction_local_unique_changes() {
    let empty = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create upsert table");
    let empty_index = TestConstraintIndex::from_state(&empty);
    let duplicate = empty.stage_transaction_with_authoritative_index(
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![
                    upsert_row("id-1", "owner-1", "first"),
                    upsert_row("id-2", "owner-1", "second"),
                ],
                mode: RelationalInsertMode::Error,
            }],
        },
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &empty_index,
    );
    assert!(matches!(duplicate, Err(RelationalError::Constraint(_))));

    let (upserted, _) = empty
        .stage_transaction_with_authoritative_index(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-1", "owner-1", "old")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Upsert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-2", "owner-1", "new")],
                        conflict_columns: vec!["owner".to_string()],
                        action: RelationalConflictAction::Update(vec![
                            RelationalUpsertAssignment {
                                column: "payload".to_string(),
                                value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                            },
                        ]),
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            &empty_index,
        )
        .expect("upsert sees a row inserted earlier in the transaction");
    let upserted_key = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    assert_eq!(upserted.row_count("documents"), 1);
    assert_eq!(
        upserted
            .row("documents", &upserted_key)
            .expect("transaction-local upsert target")
            .values()[2],
        RelationalValue::Text("new".to_string())
    );

    let seeded = empty
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed unique row");
    let seeded_index = TestConstraintIndex::from_state(&seeded);
    let id_1 = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let (replaced, _) = seeded
        .stage_transaction_with_authoritative_index(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "documents".to_string(),
                        keys: vec![id_1],
                    },
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-2", "owner-1", "new")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            &seeded_index,
        )
        .expect("delete plus insert reuses one unique key atomically");
    let id_2 = RelationalKey(vec![RelationalValue::Text("id-2".to_string())]);
    assert_eq!(replaced.row_count("documents"), 1);
    assert!(replaced.row("documents", &id_2).is_some());
}

#[test]
fn authoritative_constraint_staging_merges_foreign_key_changes() {
    let base = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create foreign-key tables")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed foreign-key rows");
    let index = TestConstraintIndex::from_state(&base);
    let document_1 = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
    let restricted = base.stage_transaction_with_authoritative_index(
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "content_documents".to_string(),
                keys: vec![document_1.clone()],
            }],
        },
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &index,
    );
    assert!(matches!(restricted, Err(RelationalError::Constraint(_))));

    let anchor_1 = RelationalKey(vec![RelationalValue::Text("anchor-1".to_string())]);
    let (removed, _) = base
        .stage_transaction_with_authoritative_index(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_documents".to_string(),
                        keys: vec![document_1],
                    },
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_anchors".to_string(),
                        keys: vec![anchor_1],
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            &index,
        )
        .expect("parent and child deletion is atomic");
    assert_eq!(removed.row_count("content_documents"), 0);
    assert_eq!(removed.row_count("content_anchors"), 0);

    let (inserted, _) = base
        .stage_transaction_with_authoritative_index(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-2", "doc-2")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-2")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            &index,
        )
        .expect("same-transaction target is visible to the foreign key");
    assert_eq!(inserted.row_count("content_documents"), 2);
    assert_eq!(inserted.row_count("content_anchors"), 2);
}

#[test]
fn authoritative_foreign_key_restriction_stops_after_one_visible_referrer() {
    let base = RelationalState::default()
        .stage_transaction(
            create_content_tables(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create foreign-key tables")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: (0..128)
                            .map(|ordinal| anchor_row(&format!("anchor-{ordinal:03}"), "doc-1"))
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed high-fanout foreign-key rows");
    let index = TestConstraintIndex::from_state(&base);
    let document = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);

    let result = base.stage_transaction_with_authoritative_index(
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "content_documents".to_string(),
                keys: vec![document.clone()],
            }],
        },
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &index,
    );

    assert!(matches!(result, Err(RelationalError::Constraint(_))));
    assert_eq!(
        index.visited(
            "content_anchors",
            &relational_foreign_key_index_name(0),
            &document,
        ),
        1
    );
}

#[test]
fn authoritative_constraint_staging_fails_closed_before_publication() {
    let state = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create authoritative fixture");
    let insert = || RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".to_string(),
            rows: vec![upsert_row("id-1", "owner-1", "payload")],
            mode: RelationalInsertMode::Error,
        }],
    };

    let lookup_failure = state.stage_transaction_with_authoritative_index(
        insert(),
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &TestConstraintIndex::failing(RelationalError::Corruption(
            "persistent constraint view is poisoned".to_string(),
        )),
    );
    assert!(matches!(
        lookup_failure,
        Err(RelationalError::Corruption(message)) if message.contains("poisoned")
    ));

    let capture_failure = state.stage_transaction_with_authoritative_index(
        insert(),
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(1024).unwrap(),
        },
        &TestConstraintIndex::from_state(&state),
    );
    assert!(matches!(
        capture_failure,
        Err(RelationalError::Admission(message)) if message.contains("capture limits")
    ));

    let ddl_failure = state.stage_transaction_with_authoritative_index(
        RelationalTransaction {
            writes: vec![RelationalWrite::CreateIndex {
                table: "documents".to_string(),
                index: RelationalIndexSchema {
                    name: "documents_payload_idx".to_string(),
                    columns: vec!["payload".to_string()],
                    unique: false,
                },
            }],
        },
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &TestConstraintIndex::from_state(&state),
    );
    assert!(matches!(
        ddl_failure,
        Err(RelationalError::Admission(message)) if message.contains("schema-changing")
    ));

    let seeded = state
        .stage_transaction(
            insert(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed malformed lookup fixture");
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);
    let primary_key = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let mut malformed = TestConstraintIndex::from_state(&seeded);
    malformed
        .postings
        .get_mut(&(
            "documents".to_string(),
            relational_unique_index_name(0),
            owner,
        ))
        .expect("seeded unique posting")
        .push(primary_key);
    let malformed_failure = seeded.stage_transaction_with_authoritative_index(
        RelationalTransaction {
            writes: vec![RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-2", "owner-1", "new")],
                conflict_columns: vec!["owner".to_string()],
                action: RelationalConflictAction::DoNothing,
            }],
        },
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
        RelationalIndexChangeCaptureLimits::default(),
        &malformed,
    );
    assert!(matches!(
        malformed_failure,
        Err(RelationalError::Corruption(message))
            if message.contains("unordered or duplicate")
    ));
}

#[test]
fn large_payload_is_externalized_and_hydrated_with_explicit_budgets() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 16,
            compression_level: 3,
            max_value_bytes: 1024 * 1024,
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let payload = "compressible-content-".repeat(128);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text(payload.clone()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");

    let snapshot = store.snapshot().expect("snapshot");
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let stored = snapshot.value().row("messages", &key).expect("stored row");
    assert!(matches!(stored.values()[1], RelationalValue::Overflow(_)));
    assert_eq!(snapshot.value().overflow_segment_count(), 1);

    let mut budget = RelationalHydrationBudget::default();
    let hydrated = snapshot
        .value()
        .hydrate_row("messages", &key, &mut budget)
        .expect("bounded hydration")
        .expect("hydrated row");
    assert_eq!(hydrated.values()[1], RelationalValue::Text(payload.clone()));
    assert_eq!(budget.hydrated_rows, 1);
    assert_eq!(budget.decompressed_bytes, payload.len());

    let reference = match stored.values()[1] {
        RelationalValue::Overflow(reference) => reference,
        _ => panic!("stored payload must remain an overflow reference"),
    };
    let mut projected = RelationalProjectedRow {
        primary_key: key.clone(),
        fields: vec![RelationalProjectedField {
            ordinal: 1,
            value: RelationalValue::Overflow(reference),
        }],
    };
    let mut projected_budget = RelationalHydrationBudget::default();
    snapshot
        .value()
        .hydrate_projected_row_with_context("messages", &mut projected, &mut projected_budget, None)
        .expect("hydrate selected overflow only");
    assert_eq!(
        projected.fields[0].value,
        RelationalValue::Text(payload.clone())
    );
    assert_eq!(projected_budget.hydrated_rows, 1);
    assert_eq!(projected_budget.decompressed_bytes, payload.len());

    let mut metadata_only = RelationalProjectedRow {
        primary_key: key.clone(),
        fields: vec![RelationalProjectedField {
            ordinal: 1,
            value: RelationalValue::Overflow(reference),
        }],
    };
    let mut metadata_budget = RelationalHydrationBudget {
        max_rows: 0,
        max_compressed_bytes: 0,
        max_decompressed_bytes: 0,
        max_memory_bytes: 0,
        ..RelationalHydrationBudget::default()
    };
    snapshot
        .value()
        .hydrate_projected_row_fields_with_context(
            "messages",
            &mut metadata_only,
            &[],
            &mut metadata_budget,
            None,
        )
        .expect("validate metadata-only overflow without hydration");
    assert_eq!(
        metadata_only.fields[0].value,
        RelationalValue::Overflow(reference)
    );
    assert_eq!(metadata_budget.hydrated_rows, 0);
    assert_eq!(metadata_budget.decompressed_bytes, 0);

    let mut mismatched = RelationalProjectedRow {
        primary_key: key.clone(),
        fields: vec![RelationalProjectedField {
            ordinal: 1,
            value: RelationalValue::Overflow(RelationalOverflowRef {
                uncompressed_bytes: reference.uncompressed_bytes + 1,
                ..reference
            }),
        }],
    };
    let initial_mismatch_budget = RelationalHydrationBudget::default();
    let mut mismatch_budget = initial_mismatch_budget;
    assert!(matches!(
        snapshot.value().hydrate_projected_row_with_context(
            "messages",
            &mut mismatched,
            &mut mismatch_budget,
            None,
        ),
        Err(RelationalError::Corruption(_))
    ));
    assert_eq!(mismatch_budget, initial_mismatch_budget);

    let mut metadata_mismatch_budget = RelationalHydrationBudget::default();
    assert!(matches!(
        snapshot.value().hydrate_projected_row_fields_with_context(
            "messages",
            &mut mismatched,
            &[],
            &mut metadata_mismatch_budget,
            None,
        ),
        Err(RelationalError::Corruption(_))
    ));
    assert_eq!(
        metadata_mismatch_budget,
        RelationalHydrationBudget::default()
    );

    let mut rejected_budget = RelationalHydrationBudget {
        max_decompressed_bytes: payload.len() - 1,
        ..RelationalHydrationBudget::default()
    };
    let error = snapshot
        .value()
        .hydrate_row("messages", &key, &mut rejected_budget)
        .expect_err("decompressed-byte budget must fail before allocation");
    assert!(matches!(error, RelationalError::Admission(_)));
    assert_eq!(rejected_budget.hydrated_rows, 0);
    assert_eq!(rejected_budget.decompressed_bytes, 0);
}

#[test]
fn add_column_rewrite_is_admitted_by_existing_resident_bytes() {
    let store = RelationalStore::new(RelationalMutationLimits {
        max_rows: NonZeroUsize::new(10).unwrap(),
        max_payload_bytes: NonZeroUsize::new(128).unwrap(),
    });
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("m1".to_string()),
                        RelationalValue::Text("payload".repeat(16)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert admitted payload");

    let error = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::AddColumn {
                    table: "messages".to_string(),
                    column: text_column("kind", true),
                }],
            },
            |_, _| Ok(()),
        )
        .expect_err("resident rewrite must honor the mutation byte budget");
    assert!(matches!(
        error,
        SnapshotCommitError::Stage(RelationalError::Admission(message))
            if message.contains("resident bytes")
    ));
    assert!(store
        .snapshot()
        .unwrap()
        .value()
        .table_schema("messages")
        .unwrap()
        .column_position("kind")
        .is_none());
}

#[test]
fn add_column_externalizes_one_shared_default_for_all_rewritten_rows() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 16,
            compression_level: 3,
            max_value_bytes: 1024 * 1024,
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![
                        RelationalRow::new(vec![
                            RelationalValue::Text("m1".to_string()),
                            RelationalValue::Text("first".to_string()),
                        ]),
                        RelationalRow::new(vec![
                            RelationalValue::Text("m2".to_string()),
                            RelationalValue::Text("second".to_string()),
                        ]),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert rows");

    let default = "shared-default-".repeat(128);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::AddColumn {
                    table: "messages".to_string(),
                    column: RelationalColumnSchema {
                        name: "kind".to_string(),
                        scalar_type: RelationalScalarType::Text,
                        nullable: false,
                        default: Some(RelationalValue::Text(default.clone())),
                    },
                }],
            },
            |_, _| Ok(()),
        )
        .expect("add column");

    let snapshot = store.snapshot().expect("snapshot");
    assert_eq!(snapshot.value().overflow_segment_count(), 1);
    for id in ["m1", "m2"] {
        let key = RelationalKey(vec![RelationalValue::Text(id.to_string())]);
        let row = snapshot
            .value()
            .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
            .expect("hydrate rewritten row")
            .expect("rewritten row");
        assert_eq!(row.values()[2], RelationalValue::Text(default.clone()));
    }
}

#[test]
fn row_mutation_clones_only_the_affected_cow_page() {
    let store = RelationalStore::default();
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let rows = (0..768)
        .map(|index| {
            RelationalRow::new(vec![
                RelationalValue::Text(format!("message-{index:04}")),
                RelationalValue::Text(format!("payload-{index}")),
            ])
        })
        .collect::<Vec<_>>();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed paged rows");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-9999".to_string()),
                        RelationalValue::Text("new payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("append one row");

    let old_rows = &pinned.value().segments["messages"].rows;
    let new_rows = &current.value().segments["messages"].rows;
    assert!(old_rows.page_count() >= 3);
    assert!(old_rows.shared_page_count(new_rows) >= old_rows.page_count() - 1);
    assert_eq!(old_rows.len(), 768);
    assert_eq!(new_rows.len(), 769);
    assert!(
        !old_rows.contains_key(&RelationalKey(vec![RelationalValue::Text(
            "message-9999".to_string()
        )]))
    );
}

#[test]
fn append_updates_only_the_affected_posting_page() {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![text_column("id", false), text_column("thread_id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_thread".to_string(),
                            columns: vec!["thread_id".to_string()],
                            unique: false,
                        },
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("create indexed table");
    let rows = (0..768)
        .map(|index| {
            RelationalRow::new(vec![
                RelationalValue::Text(format!("message-{index:04}")),
                RelationalValue::Text("thread-1".to_string()),
            ])
        })
        .collect::<Vec<_>>();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed posting pages");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-9999".to_string()),
                        RelationalValue::Text("thread-1".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("append indexed row");

    let index_key = RelationalKey(vec![RelationalValue::Text("thread-1".to_string())]);
    let old_postings = pinned.value().segments["messages"].indexes["idx_messages_thread"]
        .get(&index_key)
        .expect("old postings");
    let new_postings = current.value().segments["messages"].indexes["idx_messages_thread"]
        .get(&index_key)
        .expect("new postings");
    assert!(old_postings.page_count() >= 3);
    assert!(old_postings.shared_page_count(new_postings) >= old_postings.page_count() - 1);
    assert_eq!(old_postings.len, 768);
    assert_eq!(new_postings.len, 769);
}

#[test]
fn composite_index_prefix_lookup_is_bounded_and_preserves_leading_column_order() {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("space_id", false),
                            text_column("thread_id", false),
                            RelationalColumnSchema {
                                name: "order_index".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_space_thread_order".to_string(),
                            columns: vec![
                                "space_id".to_string(),
                                "thread_id".to_string(),
                                "order_index".to_string(),
                            ],
                            unique: false,
                        },
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("create composite index");
    let rows = [
        ("message-1", "default", "thread-1", 1),
        ("message-2", "default", "thread-1", 2),
        ("message-3", "default", "thread-2", 1),
        ("message-4", "private", "thread-1", 1),
    ]
    .into_iter()
    .map(|(id, space_id, thread_id, order_index)| {
        RelationalRow::new(vec![
            RelationalValue::Text(id.to_string()),
            RelationalValue::Text(space_id.to_string()),
            RelationalValue::Text(thread_id.to_string()),
            RelationalValue::BigInt(order_index),
        ])
    })
    .collect();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed composite index");

    let snapshot = store.snapshot().expect("composite index snapshot");
    let prefix = RelationalKey(vec![
        RelationalValue::Text("default".to_string()),
        RelationalValue::Text("thread-1".to_string()),
    ]);
    assert_eq!(
        snapshot.value().index_prefix_cardinality(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
        ),
        Some(2)
    );
    assert_eq!(
        snapshot.value().index_prefix_cardinality_at_most(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            1,
        ),
        Some(1)
    );
    let keys = snapshot
        .value()
        .index_prefix_lookup("messages", "idx_messages_space_thread_order", &prefix, 1)
        .expect("prefix lookup");
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0],
        &RelationalKey(vec![RelationalValue::Text("message-1".to_string())])
    );
    assert_eq!(
        snapshot.value().index_prefix_lookup(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            0,
        ),
        Some(Vec::new())
    );

    let mut visited = Vec::new();
    snapshot
        .value()
        .visit_index_prefix_rows(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            |primary_key, row| {
                visited.push((primary_key.clone(), row.values()[3].clone()));
                false
            },
        )
        .expect("streaming prefix lookup");
    assert_eq!(
        visited,
        vec![(
            RelationalKey(vec![RelationalValue::Text("message-1".to_string())]),
            RelationalValue::BigInt(1),
        )]
    );
}

#[test]
fn overflow_gc_prunes_new_snapshot_without_invalidating_old_snapshot() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "messages".to_string(),
                    keys: vec![key],
                }],
            },
            |_, _| Ok(()),
        )
        .expect("delete payload");

    assert_eq!(pinned.value().overflow_segment_count(), 1);
    assert_eq!(current.value().overflow_segment_count(), 0);
}

#[test]
fn overflow_digest_mismatch_fails_closed() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");
    let snapshot = store.snapshot().expect("snapshot");
    let mut corrupt = snapshot.value().clone();
    let segment = corrupt
        .overflow_segments
        .values_mut()
        .next()
        .expect("overflow envelope");
    let RelationalOverflowSegment::Inline(envelope) = segment else {
        panic!("newly staged overflow must be inline");
    };
    let bytes = Arc::make_mut(envelope);
    bytes[bytes.len() - 1] ^= 0xff;
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);

    let error = corrupt
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect_err("corrupt overflow must fail closed");
    assert!(matches!(error, RelationalError::Corruption(_)));

    let mut checkpoint = std::io::Cursor::new(Vec::new());
    let error = encode_relational_checkpoint_to_writer(
        &mut checkpoint,
        snapshot.epoch(),
        &corrupt,
        RelationalDecodeLimits::checkpoint().max_record_bytes,
    )
    .expect_err("checkpoint publication must reject a corrupt overflow segment");
    assert!(matches!(error, RelationalError::Corruption(_)));
}

#[test]
fn wal_round_trip_replays_only_complete_epoch_sequence() {
    let source = RelationalStore::default();
    let mut schema_record = Vec::new();
    source
        .commit_with_wal(create_content_tables(), |_, bytes| {
            schema_record = bytes.to_vec();
            Ok(())
        })
        .expect("durable schema commit");
    let mut insert_record = Vec::new();
    source
        .commit_with_wal(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_documents".to_string(),
                    rows: vec![document_row("doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, bytes| {
                insert_record = bytes.to_vec();
                Ok(())
            },
        )
        .expect("durable insert commit");

    let replay = RelationalStore::default();
    replay
        .replay_wal_record(&schema_record, RelationalDecodeLimits::wal())
        .expect("replay schema epoch");
    let snapshot = replay
        .replay_wal_record(&insert_record, RelationalDecodeLimits::wal())
        .expect("replay insert epoch");
    assert_eq!(snapshot.epoch(), 2);
    assert_eq!(snapshot.value().row_count("content_documents"), 1);

    let gap = RelationalStore::default();
    let error = gap
        .replay_wal_record(&insert_record, RelationalDecodeLimits::wal())
        .expect_err("epoch gaps must fail closed");
    assert!(matches!(error, SnapshotCommitError::Stage(_)));
    assert_eq!(gap.snapshot().expect("empty snapshot").epoch(), 0);
}

#[test]
fn torn_or_modified_wal_record_is_rejected_before_replay() {
    let record = encode_relational_wal_batch(1, &create_content_tables()).expect("encoded WAL");
    let torn = &record[..record.len() - 1];
    assert!(matches!(
        decode_relational_wal_batch(torn, RelationalDecodeLimits::wal()),
        Err(RelationalError::Corruption(_))
    ));

    let mut modified = record;
    let last = modified.last_mut().expect("payload byte");
    *last ^= 0xff;
    assert!(matches!(
        decode_relational_wal_batch(&modified, RelationalDecodeLimits::wal()),
        Err(RelationalError::Corruption(_))
    ));
}

#[test]
fn wal_encoder_rejects_replay_access_above_decoder_limit() {
    let limits = RelationalDecodeLimits::wal();
    let entries = (0..=limits.max_rows)
        .map(|ordinal| RelationalReplayAccess {
            table: "documents".to_string(),
            primary_key: RelationalKey(vec![RelationalValue::BigInt(
                i64::try_from(ordinal).expect("test ordinal fits i64"),
            )]),
        })
        .collect();
    let replay_access = RelationalReplayAccessSet { entries };
    let error = encode_relational_wal_batch_with_replay_access(
        1,
        &RelationalTransaction::default(),
        Some(&replay_access),
    )
    .expect_err("encoder must not emit access sets rejected by its decoder");
    assert!(matches!(
        error,
        RelationalError::Admission(message) if message.contains("decoder entry limit")
    ));
}

#[test]
fn replay_access_decoder_rejects_noncanonical_order() {
    let entry = |id: i64| RelationalReplayAccess {
        table: "documents".to_string(),
        primary_key: RelationalKey(vec![RelationalValue::BigInt(id)]),
    };
    for entries in [vec![entry(2), entry(1)], vec![entry(1), entry(1)]] {
        assert!(matches!(
            RelationalReplayAccessSet::from_decoded_entries(entries),
            Err(RelationalError::Corruption(message))
                if message.contains("not strictly ordered")
        ));
    }
}

#[test]
fn checkpoint_restores_catalog_rows_overflow_and_epoch() {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: 1,
        ..RelationalOverflowConfig::default()
    };
    let source =
        RelationalStore::with_overflow_config(RelationalMutationLimits::default(), overflow_config);
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create table");
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert row");

    let checkpoint = source.encode_checkpoint().expect("encode checkpoint");
    let restored = RelationalStore::from_checkpoint(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalMutationLimits::default(),
        overflow_config,
    )
    .expect("restore checkpoint");
    let snapshot = restored.snapshot().expect("restored snapshot");
    assert_eq!(snapshot.epoch(), 2);
    assert_eq!(snapshot.value().row_count("messages"), 1);
    assert_eq!(snapshot.value().overflow_segment_count(), 1);

    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let hydrated = snapshot
        .value()
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect("hydrate restored row")
        .expect("restored row");
    assert_eq!(
        hydrated.values()[1],
        RelationalValue::Text("payload".repeat(64))
    );
}

#[test]
fn checkpoint_file_keeps_overflow_out_of_resident_state_and_checks_size_before_read() {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: 1,
        ..RelationalOverflowConfig::default()
    };
    let source =
        RelationalStore::with_overflow_config(RelationalMutationLimits::default(), overflow_config);
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload row");
    let checkpoint = source.encode_checkpoint().expect("encode checkpoint");
    let snapshot = source.snapshot().expect("checkpoint source snapshot");
    let mut undersized_writer = std::io::Cursor::new(Vec::new());
    let error = encode_relational_checkpoint_to_writer(
        &mut undersized_writer,
        snapshot.epoch(),
        snapshot.value(),
        checkpoint.len() - 1,
    )
    .expect_err("streaming checkpoint writer must enforce its byte admission");
    assert!(matches!(error, RelationalError::Admission(_)));
    let path = std::env::temp_dir().join(format!(
        "skein-relational-checkpoint-{}-{}.skein",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::write(&path, &checkpoint).expect("write checkpoint fixture");

    let decoded = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .expect("decode file-backed checkpoint");
    assert_eq!(decoded.state.file_backed_overflow_segment_count(), 1);
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let hydrated = decoded
        .state
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect("hydrate file-backed overflow")
        .expect("payload row");
    assert_eq!(
        hydrated.values()[1],
        RelationalValue::Text("payload".repeat(64))
    );

    let mut limits = RelationalDecodeLimits::checkpoint();
    limits.max_record_bytes = checkpoint.len() - 1;
    let error = decode_relational_checkpoint_file(&path, limits)
        .expect_err("oversized checkpoint must fail before allocation");
    assert!(matches!(error, RelationalError::Admission(_)));

    let mut corrupt = checkpoint;
    *corrupt.last_mut().expect("checkpoint payload byte") ^= 0xff;
    std::fs::write(&path, corrupt).expect("write corrupted checkpoint fixture");
    let error = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .expect_err("streaming file decoder must reject corrupted payloads");
    assert!(matches!(error, RelationalError::Corruption(_)));
    std::fs::remove_file(path).expect("remove checkpoint fixture");
}

#[test]
fn checkpoint_decode_can_omit_materialized_postings_and_recovery_keeps_them_omitted() {
    let seeded = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .and_then(|state| {
            state.stage_transaction(
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-1", "owner-1", "payload-1")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
        })
        .expect("seed indexed checkpoint state");
    let checkpoint = encode_relational_checkpoint(2, &seeded).expect("encode indexed checkpoint");
    let materialized =
        decode_relational_checkpoint(&checkpoint, RelationalDecodeLimits::checkpoint())
            .expect("decode materialized checkpoint");
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);
    assert!(materialized.state.materialized_index_postings_resident());
    assert!(materialized
        .state
        .index_lookup("documents", &relational_unique_index_name(0), &owner)
        .is_some());

    let omitted = decode_relational_checkpoint_with_index_load(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalCheckpointIndexLoad::OmitMaterializedPostings,
    )
    .expect("decode checkpoint without materialized postings");
    assert!(!omitted.state.materialized_index_postings_resident());
    assert!(omitted
        .state
        .index_lookup("documents", &relational_unique_index_name(0), &owner)
        .is_none());
    assert!(matches!(
        omitted.state.stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-2", "owner-2", "payload-2")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        ),
        Err(RelationalError::Corruption(message))
            if message.contains("postings were omitted")
    ));

    let (recovered, capture) = omitted
        .state
        .stage_transaction_for_authoritative_recovery(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-2", "owner-2", "payload-2")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("replay authoritative transaction without rebuilding postings");
    assert!(!recovered.materialized_index_postings_resident());
    assert!(matches!(
        capture,
        RelationalIndexChangeCapture::Captured { .. }
    ));
}

#[test]
fn authoritative_recovery_does_not_revalidate_durable_non_primary_foreign_keys() {
    let schema = RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "accounts".to_string(),
                columns: vec![text_column("id", false), text_column("owner", false)],
                primary_key: vec!["id".to_string()],
                unique_constraints: vec![vec!["owner".to_string()]],
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
            }),
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "sessions".to_string(),
                columns: vec![text_column("id", false), text_column("owner", false)],
                primary_key: vec!["id".to_string()],
                unique_constraints: Vec::new(),
                foreign_keys: vec![RelationalForeignKeySchema {
                    columns: vec!["owner".to_string()],
                    referenced_table: "accounts".to_string(),
                    referenced_columns: vec!["owner".to_string()],
                    on_delete: RelationalReferentialAction::Restrict,
                    on_update: RelationalReferentialAction::Restrict,
                }],
                indexes: Vec::new(),
            }),
            RelationalWrite::Insert {
                table: "accounts".to_string(),
                rows: vec![RelationalRow::new(vec![
                    RelationalValue::Text("account-1".to_string()),
                    RelationalValue::Text("owner-1".to_string()),
                ])],
                mode: RelationalInsertMode::Error,
            },
        ],
    };
    let seeded = RelationalState::default()
        .stage_transaction(
            schema,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed non-primary foreign-key target");
    let checkpoint = encode_relational_checkpoint(1, &seeded).expect("encode recovery fixture");
    let omitted = decode_relational_checkpoint_with_index_load(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalCheckpointIndexLoad::OmitMaterializedPostings,
    )
    .expect("decode authoritative recovery fixture");

    let (recovered, _) = omitted
        .state
        .stage_transaction_for_authoritative_recovery(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "sessions".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("session-1".to_string()),
                        RelationalValue::Text("owner-1".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("replay already-validated foreign-key WAL without materialized postings");

    assert_eq!(recovered.row_count("sessions"), 1);
    assert!(!recovered.materialized_index_postings_resident());
}

#[test]
fn checkpoint_corruption_is_rejected_without_partial_state() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let mut checkpoint = store.encode_checkpoint().expect("encode checkpoint");
    let last = checkpoint.last_mut().expect("payload byte");
    *last ^= 0xff;

    let result = RelationalStore::from_checkpoint(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
    );
    assert!(matches!(result, Err(RelationalError::Corruption(_))));
}

#[test]
fn unique_target_upsert_and_bounded_delete_where_are_atomic() {
    let store = RelationalStore::default();
    store
        .commit(create_upsert_table(), |_, _| Ok(()))
        .expect("create upsert table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert initial row");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Upsert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-2", "owner-1", "new")],
                    conflict_columns: vec!["owner".to_string()],
                    action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                        column: "payload".to_string(),
                        value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                    }]),
                }],
            },
            |_, _| Ok(()),
        )
        .expect("upsert by unique owner");

    let snapshot = store.snapshot().expect("snapshot after upsert");
    assert_eq!(snapshot.value().row_count("documents"), 1);
    let key = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    assert_eq!(
        snapshot
            .value()
            .row("documents", &key)
            .expect("preserved primary key")
            .values()[2],
        RelationalValue::Text("new".to_string())
    );

    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteWhere {
                    table: "documents".to_string(),
                    predicate: RelationalPredicate::Compare {
                        column: "payload".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("new".to_string()),
                    },
                }],
            },
            |_, _| Ok(()),
        )
        .expect("delete matching row");
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot after delete")
            .value()
            .row_count("documents"),
        0
    );
}

#[test]
fn wal_codec_preserves_upsert_and_delete_predicates() {
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-1", "owner-1", "payload")],
                conflict_columns: vec!["owner".to_string()],
                action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                    column: "payload".to_string(),
                    value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                }]),
            },
            RelationalWrite::DeleteWhere {
                table: "documents".to_string(),
                predicate: RelationalPredicate::And(
                    Box::new(RelationalPredicate::Compare {
                        column: "owner".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("owner-1".to_string()),
                    }),
                    Box::new(RelationalPredicate::IsNull {
                        column: "payload".to_string(),
                        negated: true,
                    }),
                ),
            },
            RelationalWrite::AddColumn {
                table: "documents".to_string(),
                column: RelationalColumnSchema {
                    name: "kind".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: Some(RelationalValue::Text("text".to_string())),
                },
            },
        ],
    };
    let encoded = encode_relational_wal_batch(9, &transaction).expect("encoded WAL");
    let decoded =
        decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).expect("decoded WAL");
    assert_eq!(decoded.epoch, 9);
    assert_eq!(decoded.transaction, transaction);
}

#[test]
fn relational_index_shadow_publishes_generation_fenced_cold_pages() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false), text_column("owner", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..8)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal}")),
                                    RelationalValue::Text("shared-owner".to_string()),
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
        .expect("build relational shadow source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-shadow-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    let writer = RelationalIndexShadowWriter::new(config);
    let first = writer
        .publish(&directory, &state, 1, 40, None)
        .expect("publish first shadow generation");
    assert_eq!(first.index_roots, 2);
    assert!(first.pages_written > first.index_roots as u64);
    assert_eq!(first.artifact_bytes, first.pages_written * 4096);
    assert_eq!(first.generation_artifacts.generation, 1);
    assert_eq!(first.generation_artifacts.source_commit_epoch, 40);
    assert_eq!(
        first.generation_artifacts.page_artifact.encoded_len,
        first.artifact_bytes
    );
    assert_eq!(
        first.generation_artifacts.manifest_artifact.encoded_len,
        first.manifest_bytes
    );
    assert_relational_index_artifact_metadata(
        &directory.join(relational_index_shadow_artifact_file(1)),
        first.generation_artifacts.page_artifact,
    );
    assert_relational_index_artifact_metadata(
        &directory.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE),
        first.generation_artifacts.manifest_artifact,
    );
    let first_reader = RelationalIndexShadowReader::open(&directory, 1, 40, config)
        .expect("open the first bound generation");
    assert_eq!(
        first.generation_artifacts.catalog_schema_digest,
        first_reader.manifest().catalog_schema_digest
    );
    assert_eq!(
        first.generation_artifacts.root_set_digest,
        first_reader.manifest().root_set_digest
    );

    let stale = writer
        .publish(&directory, &state, 2, 41, None)
        .expect_err("stale publisher must not replace the selected root");
    assert!(matches!(
        stale,
        RelationalIndexShadowError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(1),
        }
    ));
    assert!(!directory
        .join(relational_index_shadow_artifact_file(2))
        .exists());

    let second = writer
        .publish(&directory, &state, 2, 41, Some(1))
        .expect("publish next shadow generation");
    let reader = RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("open manifest without reading page payloads");
    assert_eq!(reader.manifest().page_count, second.pages_written);
    for root in &reader.manifest().roots {
        reader.read_root(root).expect("read and verify root page");
    }
    assert!(RelationalIndexShadowReader::open(&directory, 2, 42, config).is_err());

    std::fs::write(
        directory.join(relational_index_shadow_artifact_file(99)),
        b"orphan",
    )
    .expect("write orphan candidate");
    RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("orphan generation must not affect selected manifest");

    let artifact = directory.join(relational_index_shadow_artifact_file(2));
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .expect("open selected page artifact");
    use std::io::{Seek, Write};
    file.seek(std::io::SeekFrom::Start(4095))
        .expect("seek cold page padding");
    file.write_all(&[1]).expect("corrupt cold page padding");
    file.sync_all().expect("sync page corruption");
    let cold_reader = RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("cold open must not scan page payloads");
    let first_page = crate::IndexPageId::new(std::num::NonZeroU64::new(1).unwrap());
    assert!(matches!(
        cold_reader.read_page(first_page),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(cold_reader.is_poisoned());
    let second_page = crate::IndexPageId::new(std::num::NonZeroU64::new(2).unwrap());
    assert!(matches!(
        cold_reader.read_page(second_page),
        Err(RelationalIndexShadowError::Corrupt(message)) if message.contains("poisoned")
    ));

    std::fs::remove_dir_all(directory).expect("remove relational index shadow fixture");
}

#[test]
fn relational_index_shadow_publishes_skew_aware_leading_prefix_statistics() {
    let rows = [
        ("row-1", "A", Some("x")),
        ("row-2", "A", Some("x")),
        ("row-3", "A", Some("x")),
        ("row-4", "A", Some("x")),
        ("row-5", "A", Some("y")),
        ("row-6", "A", None),
        ("row-7", "A", None),
        ("row-8", "B", Some("x")),
    ];
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "events".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("tenant", false),
                            text_column("category", true),
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "events_tenant_category_idx".to_string(),
                            columns: vec!["tenant".to_string(), "category".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "events".to_string(),
                        rows: rows
                            .into_iter()
                            .map(|(id, tenant, category)| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(id.to_string()),
                                    RelationalValue::Text(tenant.to_string()),
                                    category.map_or(RelationalValue::Null, |category| {
                                        RelationalValue::Text(category.to_string())
                                    }),
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
        .expect("build skewed relational index source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-statistics-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig::default();
    let report = RelationalIndexShadowWriter::new(config)
        .publish_generation(&directory, &state, 1, 40)
        .expect("publish relational index statistics");
    let reader = RelationalIndexShadowReader::open_bound_generation(
        &directory,
        report.generation_artifacts,
        config,
    )
    .expect("open relational index statistics");
    let statistics = &reader
        .manifest()
        .root("events", "events_tenant_category_idx")
        .expect("composite index root")
        .statistics;

    assert_eq!(
        statistics.leading_prefixes,
        [
            RelationalIndexPrefixStatistics {
                distinct_non_null_values: 2,
                non_null_rows: 8,
                fanout: 7,
            },
            RelationalIndexPrefixStatistics {
                distinct_non_null_values: 3,
                non_null_rows: 6,
                fanout: 4,
            },
        ]
    );

    std::fs::remove_dir_all(directory).expect("remove relational index statistics fixture");
}

#[test]
fn relational_index_bound_open_verifies_one_canonical_manifest_image() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false), text_column("owner", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text("doc-1".to_string()),
                            RelationalValue::Text("owner-1".to_string()),
                        ])],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build bound relational index source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-bound-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig::default();
    let report = RelationalIndexShadowWriter::new(config)
        .publish_generation(&directory, &state, 1, 40)
        .expect("publish bound relational index generation");

    let reader = RelationalIndexShadowReader::open_bound_generation(
        &directory,
        report.generation_artifacts,
        config,
    )
    .expect("open exact canonical index binding");
    assert_eq!(reader.manifest().generation, 1);

    let mut wrong_manifest = report.generation_artifacts;
    wrong_manifest.manifest_artifact.encoded_crc32c ^= 1;
    assert!(matches!(
        RelationalIndexShadowReader::open_bound_generation(
            &directory,
            wrong_manifest,
            config,
        ),
        Err(RelationalIndexShadowError::Corrupt(message))
            if message.contains("canonical binding")
    ));

    let mut wrong_page_length = report.generation_artifacts;
    wrong_page_length.page_artifact.encoded_len += 1;
    assert!(matches!(
        RelationalIndexShadowReader::open_bound_generation(
            &directory,
            wrong_page_length,
            config,
        ),
        Err(RelationalIndexShadowError::Corrupt(message))
            if message.contains("identity")
    ));

    std::fs::remove_dir_all(directory).expect("remove bound relational index fixture");
}

#[test]
fn relational_index_shadow_streams_rows_without_materialized_postings() {
    let mut state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false), text_column("owner", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..1024)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal:03}")),
                                    RelationalValue::Text(format!("owner-{}", ordinal % 4)),
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
        .expect("build row-stream index source");
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);
    let expected = state
        .index_lookup("documents", "documents_owner_idx", &owner)
        .expect("materialized differential oracle")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    for segment in state.segments.values_mut() {
        Arc::make_mut(segment).indexes.clear();
    }
    assert!(state
        .index_lookup("documents", "documents_owner_idx", &owner)
        .is_none());

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-row-stream-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        max_sort_memory_bytes: std::num::NonZeroUsize::new(32 * 1024).unwrap(),
        max_sort_runs: std::num::NonZeroUsize::new(128).unwrap(),
        max_sort_merge_fan_in: std::num::NonZeroUsize::new(2).unwrap(),
        ..RelationalIndexShadowConfig::default()
    };
    std::fs::create_dir_all(&directory).expect("create row-stream index directory");
    let stale_run = directory.join(".relational-index.0.0.run.0.tmp");
    std::fs::write(&stale_run, b"stale").expect("write stale relational index sort run");
    let report = RelationalIndexShadowWriter::new(config)
        .publish(&directory, &state, 1, 9, None)
        .expect("stream index generation from canonical rows");
    assert!(!stale_run.exists());
    assert!(report.sort_spill_run_count > 1);
    assert!(report.sort_spill_bytes > 0);
    assert!(report.peak_sort_memory_bytes <= config.max_sort_memory_bytes.get());

    let reader = RelationalIndexShadowReader::open(&directory, 1, 9, config)
        .expect("open row-stream index generation");
    let mut actual = Vec::new();
    reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                actual.push(key.clone());
                true
            },
        )
        .expect("read externally sorted postings");
    assert_eq!(actual, expected);
    assert!(std::fs::read_dir(&directory)
        .expect("list relational index directory")
        .all(|entry| !entry
            .expect("read relational index directory entry")
            .file_name()
            .to_string_lossy()
            .contains(".run.")));
    std::fs::remove_dir_all(directory).expect("remove row-stream index fixture");
}

#[test]
fn relational_index_shadow_rejects_and_cleans_excess_spill_runs() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false), text_column("owner", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..32)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal:03}")),
                                    RelationalValue::Text(format!("owner-{ordinal:03}")),
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
        .expect("build spill rejection source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-spill-rejection-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        max_sort_memory_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
        max_sort_runs: std::num::NonZeroUsize::new(1).unwrap(),
        ..RelationalIndexShadowConfig::default()
    };
    let error = RelationalIndexShadowWriter::new(config)
        .publish(&directory, &state, 1, 1, None)
        .expect_err("spill run budget must reject the build");
    assert!(matches!(
        error,
        RelationalIndexShadowError::Admission(message)
            if message.contains("spill runs") && message.contains("exceeding limit 1")
    ));
    assert!(!directory
        .join(relational_index_shadow_artifact_file(1))
        .exists());
    assert!(std::fs::read_dir(&directory)
        .expect("list rejected relational index directory")
        .all(|entry| !entry
            .expect("read rejected relational index directory entry")
            .file_name()
            .to_string_lossy()
            .contains(".run.")));

    let spill_byte_config = RelationalIndexShadowConfig {
        max_sort_memory_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
        max_sort_spill_bytes: std::num::NonZeroU64::new(1).unwrap(),
        ..RelationalIndexShadowConfig::default()
    };
    let error = RelationalIndexShadowWriter::new(spill_byte_config)
        .publish(&directory, &state, 1, 1, None)
        .expect_err("spill byte budget must reject the build");
    assert!(matches!(
        error,
        RelationalIndexShadowError::Admission(message)
            if message.contains("spill bytes") && message.contains("exceeding limit 1")
    ));
    assert!(std::fs::read_dir(&directory)
        .expect("list spill-byte rejection directory")
        .all(|entry| !entry
            .expect("read spill-byte rejection directory entry")
            .file_name()
            .to_string_lossy()
            .contains(".run.")));
    std::fs::remove_dir_all(directory).expect("remove spill rejection fixture");
}

#[test]
fn required_relational_index_roots_cover_constraints_and_foreign_keys() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "accounts".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("email", false),
                            text_column("handle", false),
                            text_column("status", false),
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: vec![vec!["email".to_string()]],
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "accounts_handle_idx".to_string(),
                                columns: vec!["handle".to_string()],
                                unique: true,
                            },
                            RelationalIndexSchema {
                                name: "accounts_status_idx".to_string(),
                                columns: vec!["status".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "sessions".to_string(),
                        columns: vec![text_column("id", false), text_column("account_id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: vec![RelationalForeignKeySchema {
                            columns: vec!["account_id".to_string()],
                            referenced_table: "accounts".to_string(),
                            referenced_columns: vec!["id".to_string()],
                            on_delete: RelationalReferentialAction::Restrict,
                            on_update: RelationalReferentialAction::Restrict,
                        }],
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::Insert {
                        table: "accounts".to_string(),
                        rows: vec![
                            RelationalRow::new(vec![
                                RelationalValue::Text("account-a".to_string()),
                                RelationalValue::Text("a@example.test".to_string()),
                                RelationalValue::Text("alice".to_string()),
                                RelationalValue::Text("active".to_string()),
                            ]),
                            RelationalRow::new(vec![
                                RelationalValue::Text("account-b".to_string()),
                                RelationalValue::Text("b@example.test".to_string()),
                                RelationalValue::Text("bob".to_string()),
                                RelationalValue::Text("active".to_string()),
                            ]),
                        ],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "sessions".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text("session-1".to_string()),
                            RelationalValue::Text("account-a".to_string()),
                        ])],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build required-root source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-required-roots-{}-{nonce}",
        std::process::id()
    ));
    let shadow_config = RelationalIndexShadowConfig::default();
    let report = RelationalIndexShadowWriter::new(shadow_config)
        .publish(&directory, &state, 1, 1, None)
        .expect("publish all required roots");
    assert_eq!(report.index_roots, 6);

    let reader = RelationalIndexShadowReader::open(&directory, 1, 1, shadow_config)
        .expect("open required-root fixture");
    reader
        .validate_required_roots(&state)
        .expect("manifest exactly covers the source schema");
    let roles = reader
        .manifest()
        .roots
        .iter()
        .map(|root| {
            (
                (root.identity.namespace.clone(), root.identity.name.clone()),
                root.role,
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        roles.get(&(
            "accounts".to_string(),
            RELATIONAL_PRIMARY_INDEX_NAME.to_string()
        )),
        Some(&RelationalIndexRole::Primary)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), relational_unique_index_name(0))),
        Some(&RelationalIndexRole::UniqueConstraint)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), "accounts_handle_idx".to_string())),
        Some(&RelationalIndexRole::DeclaredUnique)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), "accounts_status_idx".to_string())),
        Some(&RelationalIndexRole::Secondary)
    );
    let foreign_key_index = relational_foreign_key_index_name(0);
    assert_eq!(
        roles.get(&("sessions".to_string(), foreign_key_index.clone())),
        Some(&RelationalIndexRole::ForeignKeySupport)
    );

    let limited_config = RelationalIndexShadowConfig {
        max_roots: std::num::NonZeroUsize::new(5).unwrap(),
        ..shadow_config
    };
    assert!(matches!(
        RelationalIndexShadowWriter::new(limited_config)
            .publish_generation(&directory, &state, 2, 1),
        Err(RelationalIndexShadowError::Admission(message))
            if message.contains("required index root count 6 exceeds limit 5")
    ));
    assert!(!directory
        .join(relational_index_shadow_artifact_file(2))
        .exists());

    let account_a = RelationalKey(vec![RelationalValue::Text("account-a".to_string())]);
    let session_1 = RelationalKey(vec![RelationalValue::Text("session-1".to_string())]);
    let mut base_sessions = Vec::new();
    reader
        .visit_exact_postings(
            "sessions",
            &foreign_key_index,
            &account_a,
            RelationalIndexReadLimits::default(),
            |key| {
                base_sessions.push(key.clone());
                true
            },
        )
        .expect("read foreign-key support root");
    assert_eq!(base_sessions, vec![session_1.clone()]);

    let (next, capture) = state
        .stage_transaction_with_index_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::UpdateWhere {
                    table: "sessions".to_string(),
                    assignments: vec![RelationalUpdateAssignment {
                        column: "account_id".to_string(),
                        value: RelationalUpdateValue::Value(RelationalValue::Text(
                            "account-b".to_string(),
                        )),
                    }],
                    predicate: RelationalPredicate::Compare {
                        column: "id".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("session-1".to_string()),
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("capture a foreign-key support update");
    let RelationalIndexChangeCapture::Captured { changes, .. } = &capture else {
        panic!("foreign-key update must remain incrementally capturable");
    };
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.index == foreign_key_index)
            .count(),
        2
    );

    let recovery_config = RelationalIndexRecoveryConfig::default();
    let mut builder = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, recovery_config)
        .expect("create foreign-key recovery delta");
    builder
        .record(2, capture)
        .expect("record foreign-key recovery delta");
    builder.finish(2).expect("publish recovery delta");
    let recovered = RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(2, RelationalRecoverySourceIdentity::for_test(1, 2)),
        shadow_config,
        recovery_config,
    )
    .expect("open recovered foreign-key root");
    recovered
        .validate_required_roots(&next)
        .expect("recovered root identity remains schema-complete");
    let account_b = RelationalKey(vec![RelationalValue::Text("account-b".to_string())]);
    let mut recovered_sessions = Vec::new();
    recovered
        .visit_exact_postings(
            "sessions",
            &foreign_key_index,
            &account_b,
            RelationalIndexReadLimits::default(),
            |key| {
                recovered_sessions.push(key.clone());
                true
            },
        )
        .expect("read recovered foreign-key support root");
    assert_eq!(recovered_sessions, vec![session_1]);

    let changed_schema = next
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateIndex {
                    table: "accounts".to_string(),
                    index: RelationalIndexSchema {
                        name: "accounts_email_lookup_idx".to_string(),
                        columns: vec!["email".to_string()],
                        unique: false,
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build a different schema root set");
    assert!(matches!(
        reader.validate_required_roots(&changed_schema),
        Err(RelationalIndexShadowError::Corrupt(message))
            if message.contains("required relational index roots")
    ));

    std::fs::remove_dir_all(directory).expect("remove required-root fixture");
}

#[test]
fn relational_index_shadow_demand_reads_match_materialized_oracle() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("owner", false),
                            RelationalColumnSchema {
                                name: "rank".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "documents_owner_idx".to_string(),
                                columns: vec!["owner".to_string()],
                                unique: false,
                            },
                            RelationalIndexSchema {
                                name: "documents_owner_rank_idx".to_string(),
                                columns: vec!["owner".to_string(), "rank".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..60)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal:03}")),
                                    RelationalValue::Text(format!("owner-{}", ordinal % 3)),
                                    RelationalValue::BigInt(ordinal),
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
        .expect("build demand-read source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-demand-read-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    RelationalIndexShadowWriter::new(config)
        .publish(&directory, &state, 1, 80, None)
        .expect("publish demand-read fixture");
    let reader = RelationalIndexShadowReader::open(&directory, 1, 80, config)
        .expect("open demand-read fixture cold");
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);

    let expected_exact = state
        .index_lookup("documents", "documents_owner_idx", &owner)
        .expect("materialized exact posting")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut exact = Vec::new();
    let exact_report = reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                exact.push(key.clone());
                true
            },
        )
        .expect("demand-read exact posting");
    assert_eq!(exact, expected_exact);
    assert_eq!(exact_report.rows_visited, expected_exact.len());
    assert!(exact_report.pages_read < reader.manifest().page_count as usize);

    let expected_prefix = state
        .index_prefix_lookup("documents", "documents_owner_rank_idx", &owner, usize::MAX)
        .expect("materialized prefix posting")
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut prefix = Vec::new();
    let prefix_report = reader
        .visit_prefix_postings(
            "documents",
            "documents_owner_rank_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                prefix.push(key.clone());
                true
            },
        )
        .expect("demand-read composite prefix");
    assert_eq!(prefix, expected_prefix);
    assert_eq!(prefix_report.matched_index_keys, expected_prefix.len());
    assert!(prefix_report.pages_read < reader.manifest().page_count as usize);

    let primary_key = RelationalKey(vec![RelationalValue::Text("doc-031".to_string())]);
    let mut primary = Vec::new();
    let primary_report = reader
        .visit_exact_postings(
            "documents",
            RELATIONAL_PRIMARY_INDEX_NAME,
            &primary_key,
            RelationalIndexReadLimits::default(),
            |key| {
                primary.push(key.clone());
                true
            },
        )
        .expect("demand-read primary key");
    assert_eq!(primary, vec![primary_key]);
    assert_eq!(primary_report.rows_visited, 1);

    let mut early = Vec::new();
    let early_report = reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                early.push(key.clone());
                early.len() < 2
            },
        )
        .expect("bounded early posting stop");
    assert_eq!(early.len(), 2);
    assert!(early_report.stopped_early);
    assert_eq!(early_report.rows_visited, 2);
    assert!(early_report.pages_read < exact_report.pages_read);

    let low_page_limit = RelationalIndexReadLimits {
        max_pages: std::num::NonZeroUsize::new(1).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let mut provisional_rows = 0usize;
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_page_limit,
            |_| {
                provisional_rows += 1;
                true
            },
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert_eq!(provisional_rows, 0);
    assert!(!reader.is_poisoned());

    let low_byte_limit = RelationalIndexReadLimits {
        max_bytes: std::num::NonZeroUsize::new(512).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_byte_limit,
            |_| true,
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));

    let low_row_limit = RelationalIndexReadLimits {
        max_rows: std::num::NonZeroUsize::new(3).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let mut provisional_rows = 0usize;
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_row_limit,
            |_| {
                provisional_rows += 1;
                true
            },
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert_eq!(provisional_rows, 3);

    let low_height_limit = RelationalIndexReadLimits {
        max_tree_height: std::num::NonZeroU32::new(1).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_height_limit,
            |_| true,
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert!(!reader.is_poisoned());

    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "missing_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::MissingIndex { .. })
    ));
    assert!(!reader.is_poisoned());

    let page_cache = std::sync::Arc::new(crate::SegmentCache::new(16 * 1024));
    let cached_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&page_cache),
        crate::StoreId(41),
    )
    .expect("open demand-read fixture with an empty page cache");
    assert_eq!(page_cache.snapshot().resident_bytes, 0);
    let mut cold = Vec::new();
    let cold_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                cold.push(key.clone());
                true
            },
        )
        .expect("cold cached index lookup");
    assert_eq!(cold, expected_exact);
    assert_eq!(cold_report.cache_hits, 0);
    assert_eq!(cold_report.cache_misses, cold_report.pages_read);
    assert_eq!(cold_report.file_pages_read, cold_report.pages_read);
    assert_eq!(cold_report.file_bytes_read, cold_report.bytes_read);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let mut warm = Vec::new();
    let warm_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                warm.push(key.clone());
                true
            },
        )
        .expect("warm cached index lookup");
    assert_eq!(warm, expected_exact);
    assert_eq!(warm_report.cache_hits, warm_report.pages_read);
    assert_eq!(warm_report.cache_misses, 0);
    assert_eq!(warm_report.file_pages_read, 0);
    assert_eq!(warm_report.file_bytes_read, 0);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let callback_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = cached_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| panic!("stop the cached cursor"),
        );
    }));
    assert!(callback_panic.is_err());
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);
    assert!(!cached_reader.is_poisoned());

    let evicting_cache = std::sync::Arc::new(crate::SegmentCache::new(2 * 1024));
    let evicting_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&evicting_cache),
        crate::StoreId(43),
    )
    .expect("open demand-read fixture with an evicting cache");
    let mut evicted = Vec::new();
    evicting_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                evicted.push(key.clone());
                true
            },
        )
        .expect("index traversal remains correct while cold pages are evicted");
    assert_eq!(evicted, expected_exact);
    let eviction = evicting_cache.snapshot();
    assert!(eviction.eviction_count > 0);
    assert!(eviction.resident_bytes <= eviction.capacity_bytes);
    assert_eq!(eviction.pinned_bytes, 0);

    let undersized_cache = std::sync::Arc::new(crate::SegmentCache::new(512));
    let uncached_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&undersized_cache),
        crate::StoreId(42),
    )
    .expect("open demand-read fixture with an undersized cache");
    let mut uncached = Vec::new();
    let uncached_report = uncached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                uncached.push(key.clone());
                true
            },
        )
        .expect("cache admission rejection falls back to bounded positioned reads");
    assert_eq!(uncached, expected_exact);
    assert_eq!(
        uncached_report.cache_admission_rejections,
        uncached_report.pages_read
    );
    assert_eq!(undersized_cache.snapshot().resident_bytes, 0);

    let root_descriptor = reader
        .manifest()
        .root("documents", "documents_owner_idx")
        .expect("owner root descriptor");
    let root = reader
        .read_root(root_descriptor)
        .expect("read owner root before semantic corruption");
    let mut interior_page = reader
        .read_page(root.child)
        .expect("read owner interior before semantic corruption");
    let crate::ImmutableIndexPageBody::Interior(interior) = &mut interior_page.body else {
        panic!("small-page fixture must build an interior owner page");
    };
    interior
        .entries
        .first_mut()
        .expect("owner interior entry")
        .upper_bound
        .push(0);
    let corrupt_slot = interior_page
        .encode_slot(config.page_limits)
        .expect("re-encode checksummed but inconsistent separator");
    let artifact = directory.join(relational_index_shadow_artifact_file(1));
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .expect("open demand-read artifact");
    use std::io::{Seek, Write};
    let corrupt_offset = root
        .child
        .get()
        .checked_sub(1)
        .and_then(|ordinal| ordinal.checked_mul(1024))
        .expect("interior page offset");
    file.seek(std::io::SeekFrom::Start(corrupt_offset))
        .expect("seek owner interior");
    file.write_all(&corrupt_slot)
        .expect("replace owner interior with inconsistent separator");
    file.sync_all().expect("sync semantic corruption");
    let corrupt_reader = RelationalIndexShadowReader::open(&directory, 1, 80, config)
        .expect("cold open must not traverse the inconsistent separator");
    assert!(matches!(
        corrupt_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(corrupt_reader.is_poisoned());

    std::fs::remove_dir_all(directory).expect("remove demand-read fixture");
}

#[test]
fn relational_index_wal_deltas_merge_with_cold_base_and_stay_bounded() {
    let base = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("owner", false),
                            RelationalColumnSchema {
                                name: "rank".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "documents_owner_idx".to_string(),
                                columns: vec!["owner".to_string()],
                                unique: false,
                            },
                            RelationalIndexSchema {
                                name: "documents_owner_rank_idx".to_string(),
                                columns: vec!["owner".to_string(), "rank".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![
                            recovery_document_row("doc-001", "owner-a", 1),
                            recovery_document_row("doc-002", "owner-b", 2),
                            recovery_document_row("doc-003", "owner-a", 3),
                        ],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build recovery base");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-recovery-{}-{nonce}",
        std::process::id()
    ));
    let shadow_config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    RelationalIndexShadowWriter::new(shadow_config)
        .publish(&directory, &base, 1, 1, None)
        .expect("publish recovery base");
    let recovery_config = RelationalIndexRecoveryConfig {
        max_dirty_entries: std::num::NonZeroUsize::new(5).unwrap(),
        max_dirty_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
        max_delta_pages: std::num::NonZeroUsize::new(16).unwrap(),
        max_manifest_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
    };
    let mut builder = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, recovery_config)
        .expect("create recovery delta builder");
    let mut state = base;
    let transactions = [
        RelationalTransaction {
            writes: vec![RelationalWrite::UpdateWhere {
                table: "documents".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "owner".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text(
                        "owner-b".to_string(),
                    )),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "id".to_string(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("doc-001".to_string()),
                },
            }],
        },
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "documents".to_string(),
                keys: vec![RelationalKey(vec![RelationalValue::Text(
                    "doc-002".to_string(),
                )])],
            }],
        },
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![recovery_document_row("doc-004", "owner-a", 4)],
                mode: RelationalInsertMode::Error,
            }],
        },
    ];
    for (offset, transaction) in transactions.into_iter().enumerate() {
        let epoch = 2 + offset as u64;
        let (next, capture) = state
            .stage_transaction_with_index_changes(
                transaction,
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
                builder.capture_limits(),
            )
            .expect("stage recovered relational transaction");
        builder
            .record(epoch, capture)
            .expect("record bounded recovery delta");
        state = next;
    }
    let report = builder.finish(4).expect("publish recovery delta manifest");
    assert!(report.delta_pages >= 2);
    assert!(report.delta_entries >= 7);
    assert!(report.peak_dirty_entries <= recovery_config.max_dirty_entries.get());
    assert!(report.peak_dirty_bytes <= recovery_config.max_dirty_bytes.get());

    let recovery_source = RelationalRecoverySourceIdentity::for_test(1, 4);
    let reader = RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(4, recovery_source),
        shadow_config,
        recovery_config,
    )
    .expect("open fenced recovery reader");
    for (index, key) in [
        (
            "documents_owner_idx",
            RelationalKey(vec![RelationalValue::Text("owner-a".to_string())]),
        ),
        (
            "documents_owner_idx",
            RelationalKey(vec![RelationalValue::Text("owner-b".to_string())]),
        ),
    ] {
        let expected = state
            .index_lookup("documents", index, &key)
            .map(|posting| posting.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut actual = Vec::new();
        let read_report = reader
            .visit_exact_postings(
                "documents",
                index,
                &key,
                RelationalIndexReadLimits::default(),
                |primary_key| {
                    actual.push(primary_key.clone());
                    true
                },
            )
            .expect("merge exact base and recovery deltas");
        assert_eq!(actual, expected);
        assert_eq!(read_report.rows_visited, expected.len());
        assert_eq!(read_report.delta_pages_read, report.delta_pages);
    }

    let owner_prefix = RelationalKey(vec![RelationalValue::Text("owner-a".to_string())]);
    let expected_prefix = state
        .index_prefix_lookup(
            "documents",
            "documents_owner_rank_idx",
            &owner_prefix,
            usize::MAX,
        )
        .expect("materialized recovery prefix")
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut actual_prefix = Vec::new();
    reader
        .visit_prefix_postings(
            "documents",
            "documents_owner_rank_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                actual_prefix.push(primary_key.clone());
                true
            },
        )
        .expect("merge prefix base and recovery deltas");
    assert_eq!(actual_prefix, expected_prefix);

    let page_cache = std::sync::Arc::new(crate::SegmentCache::new(64 * 1024));
    let cached_reader = RelationalIndexRecoveryReader::open_latest_with_cache(
        &directory,
        RelationalRecoveryFence::new(4, recovery_source),
        shadow_config,
        recovery_config,
        std::sync::Arc::clone(&page_cache),
        crate::StoreId(73),
    )
    .expect("open recovery reader with a shared empty page cache");
    assert_eq!(page_cache.snapshot().resident_bytes, 0);
    let mut cold_rows = Vec::new();
    let cold_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                cold_rows.push(primary_key.clone());
                true
            },
        )
        .expect("cold recovery read uses positioned base and delta reads");
    let expected_cold_rows = state
        .index_lookup("documents", "documents_owner_idx", &owner_prefix)
        .expect("materialized owner-a posting")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(cold_rows, expected_cold_rows);
    assert_eq!(cold_report.base.cache_misses, cold_report.base.pages_read);
    assert_eq!(cold_report.delta_cache_misses, report.delta_pages);
    assert_eq!(cold_report.delta_file_pages_read, report.delta_pages);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let mut warm_rows = Vec::new();
    let warm_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                warm_rows.push(primary_key.clone());
                true
            },
        )
        .expect("warm recovery read reuses base and delta pages");
    assert_eq!(warm_rows, expected_cold_rows);
    assert_eq!(warm_report.base.cache_hits, warm_report.base.pages_read);
    assert_eq!(warm_report.base.file_pages_read, 0);
    assert_eq!(warm_report.delta_cache_hits, report.delta_pages);
    assert_eq!(warm_report.delta_file_pages_read, 0);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    assert!(RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(5, RelationalRecoverySourceIdentity::for_test(1, 5)),
        shadow_config,
        recovery_config,
    )
    .is_err());
    let wrong_source = RelationalRecoverySourceIdentity {
        record_sequence_sha256: skein_integrity::Sha256Digest::from_bytes([0x5a; 32]),
        ..recovery_source
    };
    assert!(RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(4, wrong_source),
        shadow_config,
        recovery_config,
    )
    .is_err());

    let crash_config = RelationalIndexRecoveryConfig {
        max_dirty_entries: std::num::NonZeroUsize::new(1).unwrap(),
        ..recovery_config
    };
    let mut abandoned = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, crash_config)
        .expect("create abandoned recovery builder");
    abandoned
        .record(
            5,
            RelationalIndexChangeCapture::Captured {
                changes: vec![
                    RelationalIndexChange {
                        table: "documents".to_string(),
                        index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                        index_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-1".to_string(),
                        )]),
                        primary_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-1".to_string(),
                        )]),
                        kind: RelationalIndexChangeKind::Insert,
                    },
                    RelationalIndexChange {
                        table: "documents".to_string(),
                        index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                        index_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-2".to_string(),
                        )]),
                        primary_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-2".to_string(),
                        )]),
                        kind: RelationalIndexChangeKind::Insert,
                    },
                ],
                encoded_bytes: 0,
            },
        )
        .expect("flush one immutable but unpublished delta generation");
    drop(abandoned);
    let old_reader = RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(4, recovery_source),
        shadow_config,
        recovery_config,
    )
    .expect("abandoned delta generation must not replace the old manifest");
    let mut old_rows = Vec::new();
    old_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |key| {
                old_rows.push(key.clone());
                true
            },
        )
        .expect("old manifest remains readable after candidate crash");
    assert_eq!(
        old_rows,
        state
            .index_lookup("documents", "documents_owner_idx", &owner_prefix)
            .expect("materialized old owner posting")
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    );

    let first_delta = directory.join(relational_index_recovery_delta_file(
        1,
        report.delta_generation,
        0,
    ));
    let mut encoded = std::fs::read(&first_delta).expect("read first recovery delta");
    let last = encoded.last_mut().expect("recovery delta is not empty");
    *last ^= 1;
    std::fs::write(&first_delta, encoded).expect("corrupt recovery delta");
    let corrupt_reader = RelationalIndexRecoveryReader::open_latest(
        &directory,
        RelationalRecoveryFence::new(4, recovery_source),
        shadow_config,
        recovery_config,
    )
    .expect("cold recovery open does not read delta pages");
    assert!(matches!(
        corrupt_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(corrupt_reader.is_poisoned());

    std::fs::remove_dir_all(directory).expect("remove recovery delta fixture");
}

#[test]
fn schema_changing_relational_wal_invalidates_incremental_index_capture() {
    let (state, capture) = RelationalState::default()
        .stage_transaction_with_index_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("schema transaction remains canonically valid");
    assert!(state.table_schema("documents").is_some());
    assert!(matches!(
        capture,
        RelationalIndexChangeCapture::Invalidated { reason }
            if reason.contains("schema-changing WAL")
    ));
}

#[test]
fn relational_row_change_capture_reports_exact_net_primary_key_changes() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create row-change capture table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed row-change capture table");

    let (next, capture) = base
        .stage_transaction_with_row_changes(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::UpdateWhere {
                        table: "documents".to_string(),
                        assignments: vec![RelationalUpdateAssignment {
                            column: "id".to_string(),
                            value: RelationalUpdateValue::Value(RelationalValue::Text(
                                "id-3".to_string(),
                            )),
                        }],
                        predicate: RelationalPredicate::Compare {
                            column: "id".to_string(),
                            op: RelationalComparisonOp::Eq,
                            value: RelationalValue::Text("id-1".to_string()),
                        },
                    },
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![upsert_row("id-2", "owner-2", "transient")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "documents".to_string(),
                        keys: vec![RelationalKey(vec![RelationalValue::Text(
                            "id-2".to_string(),
                        )])],
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalRowChangeCaptureLimits::default(),
        )
        .expect("capture exact net row changes");

    let old_key = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    let transient_key = RelationalKey(vec![RelationalValue::Text("id-2".to_string())]);
    let new_key = RelationalKey(vec![RelationalValue::Text("id-3".to_string())]);
    assert!(next.row("documents", &old_key).is_none());
    assert!(next.row("documents", &transient_key).is_none());
    assert_eq!(
        next.row("documents", &new_key),
        Some(&upsert_row("id-3", "owner-1", "old"))
    );

    let RelationalRowChangeCapture::Captured {
        changes,
        encoded_bytes,
    } = capture
    else {
        panic!("row-only transaction must remain incrementally capturable");
    };
    assert!(encoded_bytes > 0);
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].primary_key, old_key);
    assert!(changes[0].row.is_none());
    assert_eq!(changes[1].primary_key, new_key);
    assert_eq!(changes[1].row, Some(upsert_row("id-3", "owner-1", "old")));
}

#[test]
fn relational_row_change_capture_limit_invalidates_without_rejecting_canonical_state() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create row-change limit table");
    let (next, capture) = base
        .stage_transaction_with_row_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "one"),
                        upsert_row("id-2", "owner-2", "two"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalRowChangeCaptureLimits {
                max_entries: NonZeroUsize::new(1).unwrap(),
                max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            },
        )
        .expect("canonical state remains valid when shadow capture is invalidated");

    assert_eq!(next.row_count("documents"), 2);
    assert!(matches!(
        capture,
        RelationalRowChangeCapture::Invalidated { reason }
            if reason.contains("max_entries=1")
    ));
}

#[test]
fn replay_access_retains_transient_and_primary_key_working_set() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create replay-access table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed replay-access table");
    let index = TestConstraintIndex::from_state(&base);
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::UpdateWhere {
                table: "documents".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "id".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text("id-3".to_string())),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "id".to_string(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("id-1".to_string()),
                },
            },
            RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-2", "owner-2", "transient")],
                mode: RelationalInsertMode::Error,
            },
            RelationalWrite::DeleteByPrimaryKey {
                table: "documents".to_string(),
                keys: vec![RelationalKey(vec![RelationalValue::Text(
                    "id-2".to_string(),
                )])],
            },
        ],
    };

    let (_, _, row_capture, replay_access) = base
        .stage_transaction_with_authoritative_replay_access(
            transaction,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &index,
        )
        .expect("capture durable replay access");

    let RelationalRowChangeCapture::Captured { changes, .. } = row_capture else {
        panic!("row changes remain capturable");
    };
    assert_eq!(changes.len(), 2);
    assert_eq!(
        replay_access.entries(),
        &[
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::Text("id-1".to_string())]),
            },
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::Text("id-2".to_string())]),
            },
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::Text("id-3".to_string())]),
            },
        ]
    );
}

#[test]
fn replay_access_retains_predicate_non_matches() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create replay-access table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "one"),
                        upsert_row("id-2", "owner-2", "two"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed replay-access table");
    let index = TestConstraintIndex::from_state(&base);
    let (_, _, row_capture, replay_access) = base
        .stage_transaction_with_authoritative_replay_access(
            RelationalTransaction {
                writes: vec![RelationalWrite::UpdateWhere {
                    table: "documents".to_string(),
                    assignments: vec![RelationalUpdateAssignment {
                        column: "payload".to_string(),
                        value: RelationalUpdateValue::Value(RelationalValue::Text(
                            "updated".to_string(),
                        )),
                    }],
                    predicate: RelationalPredicate::Compare {
                        column: "id".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("id-1".to_string()),
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &index,
        )
        .expect("capture the complete predicate-read set");

    assert!(matches!(
        row_capture,
        RelationalRowChangeCapture::Captured { changes, .. } if changes.len() == 1
    ));
    assert_eq!(
        replay_access.entries(),
        &[
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::Text("id-1".to_string())]),
            },
            RelationalReplayAccess {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::Text("id-2".to_string())]),
            },
        ]
    );
}

#[test]
fn replay_access_retains_noop_upsert_conflict_reads() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create replay-access table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed replay-access table");
    let index = TestConstraintIndex::from_state(&base);
    let (_, _, row_capture, replay_access) = base
        .stage_transaction_with_authoritative_replay_access(
            RelationalTransaction {
                writes: vec![RelationalWrite::Upsert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-2", "owner-1", "ignored")],
                    conflict_columns: vec!["owner".to_string()],
                    action: RelationalConflictAction::DoNothing,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &index,
        )
        .expect("capture no-op conflict read");

    assert!(matches!(
        row_capture,
        RelationalRowChangeCapture::Captured { changes, .. } if changes.is_empty()
    ));
    assert_eq!(
        replay_access.entries(),
        &[RelationalReplayAccess {
            table: "documents".to_string(),
            primary_key: RelationalKey(vec![RelationalValue::Text("id-1".to_string())]),
        }]
    );
}

#[test]
fn replay_access_limit_rejects_before_durable_staging() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create replay-access table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "one"),
                        upsert_row("id-2", "owner-2", "two"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed replay-access table");
    let index = TestConstraintIndex::from_state(&base);
    let error = base
        .stage_transaction_with_authoritative_replay_access(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "documents".to_string(),
                    keys: vec![
                        RelationalKey(vec![RelationalValue::Text("id-1".to_string())]),
                        RelationalKey(vec![RelationalValue::Text("id-2".to_string())]),
                    ],
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits {
                max_entries: NonZeroUsize::new(1).unwrap(),
                max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            },
            &index,
        )
        .expect_err("oversized replay access must reject staging");
    assert!(matches!(
        error,
        RelationalError::Admission(message) if message.contains("replay access set")
    ));
    assert_eq!(base.row_count("documents"), 2);
}

#[test]
fn replay_access_limit_rejects_unbounded_predicate_scan_without_changes() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create replay-access table")
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "one"),
                        upsert_row("id-2", "owner-2", "two"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("seed replay-access table");
    let index = TestConstraintIndex::from_state(&base);
    let error = base
        .stage_transaction_with_authoritative_replay_access(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteWhere {
                    table: "documents".to_string(),
                    predicate: RelationalPredicate::Compare {
                        column: "id".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("absent".to_string()),
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits {
                max_entries: NonZeroUsize::new(1).unwrap(),
                max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            },
            &index,
        )
        .expect_err("an unbounded predicate read set must reject before WAL");
    assert!(matches!(
        error,
        RelationalError::Admission(message) if message.contains("replay access set")
    ));
    assert_eq!(base.row_count("documents"), 2);
}

#[test]
fn relational_schema_change_requires_a_canonical_row_checkpoint() {
    let base = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("create schema-change table");
    let (next, capture) = base
        .stage_transaction_with_row_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::AddColumn {
                    table: "documents".to_string(),
                    column: RelationalColumnSchema {
                        name: "kind".to_string(),
                        scalar_type: RelationalScalarType::Text,
                        nullable: false,
                        default: Some(RelationalValue::Text("text".to_string())),
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalRowChangeCaptureLimits::default(),
        )
        .expect("schema change remains a valid canonical mutation");

    assert_eq!(next.table_schema("documents").unwrap().columns.len(), 4);
    assert_eq!(
        capture,
        RelationalRowChangeCapture::RequiresCheckpoint {
            tables: vec!["documents".to_string()]
        }
    );
}

#[test]
fn relational_schema_rejects_reserved_and_duplicate_index_names() {
    for name in [
        "",
        RELATIONAL_PRIMARY_INDEX_NAME,
        "__unique_0",
        "__foreign_key_0",
    ] {
        let error = RelationalState::default()
            .stage_transaction(
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: name.to_string(),
                            columns: vec!["id".to_string()],
                            unique: false,
                        }],
                    })],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .expect_err("reserved index identity must be rejected");
        assert!(matches!(error, RelationalError::Schema(_)));
    }

    let error = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: vec![
                        RelationalIndexSchema {
                            name: "documents_id_idx".to_string(),
                            columns: vec!["id".to_string()],
                            unique: false,
                        },
                        RelationalIndexSchema {
                            name: "documents_id_idx".to_string(),
                            columns: vec!["id".to_string()],
                            unique: true,
                        },
                    ],
                })],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect_err("duplicate declared index identities must be rejected");
    assert!(matches!(error, RelationalError::Schema(_)));
}

fn recovery_document_row(id: &str, owner: &str, rank: i64) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(id.to_string()),
        RelationalValue::Text(owner.to_string()),
        RelationalValue::BigInt(rank),
    ])
}

fn create_content_tables() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "content_documents".to_string(),
                columns: vec![text_column("content_doc_id", false)],
                primary_key: vec!["content_doc_id".to_string()],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
            }),
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "content_anchors".to_string(),
                columns: vec![
                    text_column("anchor_id", false),
                    text_column("content_doc_id", false),
                ],
                primary_key: vec!["anchor_id".to_string()],
                unique_constraints: Vec::new(),
                foreign_keys: vec![RelationalForeignKeySchema {
                    columns: vec!["content_doc_id".to_string()],
                    referenced_table: "content_documents".to_string(),
                    referenced_columns: vec!["content_doc_id".to_string()],
                    on_delete: RelationalReferentialAction::NoAction,
                    on_update: RelationalReferentialAction::NoAction,
                }],
                indexes: Vec::new(),
            }),
        ],
    }
}

fn create_payload_table() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
            name: "messages".to_string(),
            columns: vec![text_column("id", false), text_column("payload", false)],
            primary_key: vec!["id".to_string()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        })],
    }
}

fn create_upsert_table() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
            name: "documents".to_string(),
            columns: vec![
                text_column("id", false),
                text_column("owner", false),
                text_column("payload", false),
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![vec!["owner".to_string()]],
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        })],
    }
}

fn text_column(name: &str, nullable: bool) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::Text,
        nullable,
        default: None,
    }
}

fn canonical_metadata_state(
    state: &RelationalState,
    fixture: &str,
) -> (std::path::PathBuf, RelationalState) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("skein-{fixture}-{}-{nonce}", std::process::id()));
    let config = RelationalRowPagePublicationConfig::default();
    let deltas = state
        .row_page_snapshot_deltas(1, 1, config)
        .expect("pack canonical row pages");
    RelationalRowPagePublisher::new(config)
        .publish(&directory, 1, 1, None, deltas)
        .expect("publish canonical row root");
    let root = RelationalRowPageRootReader::open_generation(&directory, 1, config)
        .expect("open canonical row root");
    let metadata = RelationalState::from_canonical_row_root(root.manifest())
        .expect("mount canonical row metadata");
    (directory, metadata)
}

fn document_row(id: &str) -> RelationalRow {
    RelationalRow::new(vec![RelationalValue::Text(id.to_string())])
}

fn anchor_row(anchor_id: &str, document_id: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(anchor_id.to_string()),
        RelationalValue::Text(document_id.to_string()),
    ])
}

fn upsert_row(id: &str, owner: &str, payload: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(id.to_string()),
        RelationalValue::Text(owner.to_string()),
        RelationalValue::Text(payload.to_string()),
    ])
}

fn assert_relational_index_artifact_metadata(
    path: &std::path::Path,
    expected: RelationalIndexArtifactMetadata,
) {
    let encoded = std::fs::read(path).expect("read relational index generation artifact");
    let actual = skein_integrity::integrity_digest(&encoded);
    assert_eq!(expected.encoded_len, encoded.len() as u64);
    assert_eq!(expected.encoded_crc32c, actual.crc32c.as_u64());
    assert_eq!(expected.encoded_sha256, actual.sha256);
}
