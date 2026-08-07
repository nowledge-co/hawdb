use super::*;

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
        ],
    };
    let encoded = encode_relational_wal_batch(9, &transaction).expect("encoded WAL");
    let decoded =
        decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).expect("decoded WAL");
    assert_eq!(decoded.epoch, 9);
    assert_eq!(decoded.transaction, transaction);
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
