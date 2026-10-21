// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.
use hawdb::{
    Database, DurabilityPolicy, ProjectionFeedError, ProjectionFeedLimits,
    ProjectionFullSourceItem, ProjectionFullSourceLimits, ProjectionPublicationError, Value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
#[path = "support/mvcc_fixture.rs"]
mod support;
use support::{fixture, limits, seal, TestRoot};
fn budget(work: usize, items: usize, bytes: usize) -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(work).unwrap(),
        items: NonZeroUsize::new(items).unwrap(),
        bytes: NonZeroUsize::new(bytes).unwrap(),
    }
}
fn setup(cold: bool) -> (TestRoot, Database) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query("MATCH (n:Other) DELETE n").unwrap();
    db.query_sql("CREATE TABLE records (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("CREATE TABLE empty_records (id TEXT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO records (id,body) VALUES ('a','before'),('z','zed')")
        .unwrap();
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &root.0,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
    }
    (root, db)
}
#[test]
fn full_source_pins_every_table_key_and_graph_candidate_across_writes_and_checkpoint() {
    for cold in [false, true] {
        let (_root, mut db) = setup(cold);
        let mut source = db
            .begin_prepared_full_projection_source(
                limits(),
                budget(1000, 100, 8192),
                ProjectionFullSourceLimits::default(),
                Default::default(),
            )
            .unwrap();
        let epoch = source.source_epoch();
        let mut expected_keys = BTreeSet::from([
            ("records".to_string(), "a".to_string()),
            ("records".to_string(), "z".to_string()),
        ]);
        for row in db
            .query_sql("SELECT migration_id FROM hawdb_schema_migrations")
            .unwrap()
            .rows
            .iter()
        {
            let Value::String(id) = &row["migration_id"] else {
                panic!("registry key")
            };
            expected_keys.insert(("hawdb_schema_migrations".into(), id.clone()));
        }

        db.query("CREATE (:Memory {id:'late'})").unwrap();
        db.query_sql("UPDATE records SET body='after' WHERE id='a'")
            .unwrap();
        db.query_sql("DELETE FROM records WHERE id='z'").unwrap();
        db.query_sql("INSERT INTO records (id,body) VALUES ('b','later')")
            .unwrap();
        db.checkpoint().unwrap();
        let mut nodes = BTreeSet::new();
        let mut tables = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut ends = 0;
        let mut steps = 0;
        loop {
            let step = source
                .visit_next(budget(8, 2, 256), |read, item| {
                    assert_eq!(read.source_epoch(), epoch);
                    match item {
                        ProjectionFullSourceItem::GraphNodeCandidate { node_id } => {
                            assert!(nodes.insert(node_id));
                            let result = read.query_with_params(
                                "MATCH (n:Memory) WHERE id(n)=$id RETURN n.id AS id",
                                &BTreeMap::from([("id".into(), Value::Int(node_id as i64))]),
                            )?;
                            assert_eq!(result.output.rows.len(), usize::from(node_id == 0));
                        }
                        ProjectionFullSourceItem::RelationalTable { table } => {
                            assert!(tables.insert(table.to_string()));
                        }
                        ProjectionFullSourceItem::RelationalKey { table, key } => {
                            let hawdb::RelationalValue::Text(key) = &key.0[0] else {
                                panic!("text fixture key")
                            };
                            assert!(keys.insert((table.to_string(), key.clone())));
                            if table == "records" {
                                let result=read.query_sql_with_params("SELECT body FROM records WHERE id=$1",&[Value::String(key.clone())])?;
                                assert_eq!(result.rows.len(),1,"cold={cold} key={key} epoch={epoch}");
                                assert_eq!(result.rows[0]["body"],Value::String(if key=="a" {"before"} else {"zed"}.into()));
                            } else {
                                assert_eq!(table,"hawdb_schema_migrations");
                                let result=read.query_sql_with_params("SELECT owner FROM hawdb_schema_migrations WHERE migration_id=$1",&[Value::String(key.clone())])?;
                                assert_eq!(result.rows[0]["owner"],Value::String("hawdb.engine".into()));
                            }
                        }
                        ProjectionFullSourceItem::End { source_epoch } => {
                            assert_eq!(source_epoch, epoch);
                            ends += 1;
                        }
                    };
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("cold={cold}: {e}"));
            steps += 1;
            assert!(steps < 30);
            assert!(step.usage.work > 0);
            if step.complete {
                break;
            }
        }
        assert_eq!(nodes, BTreeSet::from([0, 1]));
        assert_eq!(
            tables,
            BTreeSet::from([
                "empty_records".into(),
                "records".into(),
                "hawdb_schema_migrations".into()
            ])
        );
        assert_eq!(keys, expected_keys);
        assert_eq!(ends, 1);
        assert!(steps > 2);
        let deps = source.finish().unwrap();
        assert!(deps.requires_unchanged_database_epoch());
        let candidate = seal(
            &db.projection_generation_store().unwrap(),
            "full_source_stale",
            epoch,
            None,
        );
        assert!(matches!(
            db.publish_prepared_projection(&deps, &candidate),
            Err(ProjectionPublicationError::Dependency(_))
        ));
    }
}
#[test]
fn full_source_requires_explicit_end_and_poisoned_callbacks_cannot_resume() {
    for ignored in [false, true] {
        let (_root, db) = setup(false);
        let source = db
            .begin_prepared_full_projection_source(
                limits(),
                budget(100, 100, 8192),
                ProjectionFullSourceLimits::default(),
                Default::default(),
            )
            .unwrap();
        assert!(matches!(
            source.finish(),
            Err(ProjectionFeedError::Incomplete)
        ));
        let mut source = db
            .begin_prepared_full_projection_source(
                limits(),
                budget(100, 100, 8192),
                ProjectionFullSourceLimits::default(),
                Default::default(),
            )
            .unwrap();
        let result = source.visit_next(budget(8, 2, 256), |read, _| {
            if ignored {
                let _ = read.query_sql_with_params("SELECT missing FROM unknown_table", &[]);
                Ok(())
            } else {
                Err(hawdb::HawDBError::Execution("mapper refused".into()))
            }
        });
        assert!(result.is_err());
        assert!(matches!(
            source.visit_next(budget(8, 2, 256), |_, _| Ok(())),
            Err(ProjectionFeedError::Failed)
        ));
        assert!(source.finish().is_err());
    }
}
#[test]
fn full_source_admission_counts_holes_and_refuses_oversized_keys_before_completion() {
    let (_root, mut db) = setup(false);
    let config = ProjectionFullSourceLimits {
        metadata_bytes: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    };
    assert!(db
        .begin_prepared_full_projection_source(
            limits(),
            budget(100, 100, 8192),
            config,
            Default::default()
        )
        .is_err());
    let mut source = db
        .begin_prepared_full_projection_source(
            limits(),
            budget(1, 100, 8192),
            ProjectionFullSourceLimits::default(),
            Default::default(),
        )
        .unwrap();
    let mut seen = 0;
    assert!(source
        .visit_next(budget(8, 8, 8192), |_, _| {
            seen += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(seen, 1);
    assert!(!source.is_complete());
    db.query_sql_with_params(
        "INSERT INTO records (id,body) VALUES ($1,'large')",
        &[Value::String("x".repeat(5000))],
    )
    .unwrap();
    let mut source = db
        .begin_prepared_full_projection_source(
            limits(),
            budget(100, 100, 8192),
            ProjectionFullSourceLimits::default(),
            Default::default(),
        )
        .unwrap();
    assert!(source
        .visit_next(budget(100, 100, 8192), |_, _| Ok(()))
        .is_err());
    assert!(!source.is_complete());
    assert!(source.finish().is_err());
}

#[test]
fn full_source_byte_yields_retain_measured_keys_without_reset_or_prefix_rescan() {
    for cold in [false, true] {
        let (_root, db) = setup(cold);
        let mut traces = Vec::new();
        let mut usage = Vec::new();
        let mut frames = Vec::new();
        for bytes in [128, 8192] {
            let mut source = db
                .begin_prepared_full_projection_source(
                    limits(),
                    budget(1000, 100, 8192),
                    ProjectionFullSourceLimits::default(),
                    Default::default(),
                )
                .unwrap();
            let mut trace = Vec::new();
            let mut count = 0;
            loop {
                let step = source
                    .visit_next(budget(64, 10, bytes), |_, item| {
                        trace.push(format!("{item:?}"));
                        Ok(())
                    })
                    .unwrap();
                assert!(step.usage.work > 0);
                assert!(step.usage.bytes <= bytes);
                count += 1;
                assert!(count < 30);
                if step.complete {
                    break;
                }
            }
            usage.push(source.usage());
            frames.push(count);
            source.finish().unwrap();
            traces.push(trace);
        }
        assert_eq!(traces[0], traces[1]);
        assert_eq!(usage[0], usage[1]);
        assert!(frames[0] > frames[1]);
    }
}
#[test]
fn full_source_cancel_and_panic_cannot_produce_completion_or_reusable_progress() {
    let (_root, db) = setup(false);
    for mode in ["before", "during", "end", "after", "panic"] {
        let token = hawdb::RuntimeCancellationToken::new();
        if mode == "before" {
            token.cancel();
        }
        let result = db.begin_prepared_full_projection_source(
            limits(),
            budget(1000, 100, 8192),
            ProjectionFullSourceLimits::default(),
            hawdb::RuntimeTaskContext::without_deadline(token.clone()),
        );
        if mode == "before" {
            assert!(result.is_err());
            continue;
        }
        let mut source = result.unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            source.visit_next(budget(1000, 100, 8192), |_, item| {
                if mode == "panic" {
                    panic!("injected baseline mapper panic");
                }
                if mode == "during"
                    || (mode == "end" && matches!(item, ProjectionFullSourceItem::End { .. }))
                {
                    token.cancel();
                }
                Ok(())
            })
        }));
        if mode == "after" {
            assert!(result.unwrap().unwrap().complete);
            token.cancel();
            assert!(source.finish().is_err());
        } else {
            if mode == "panic" {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(!source.is_complete());
            assert!(matches!(
                source.visit_next(budget(8, 2, 256), |_, _| Ok(())),
                Err(ProjectionFeedError::Failed)
            ));
            assert!(source.finish().is_err());
        }
    }
}
#[test]
fn full_source_composite_primary_keys_cross_page_boundaries_and_cold_live_overlays() {
    for cold in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query_sql("CREATE TABLE pages (tenant TEXT, ordinal BIGINT, body TEXT, PRIMARY KEY(tenant,ordinal))").unwrap();
        let values = (0..520)
            .map(|i| format!("('g',{i},'body')"))
            .collect::<Vec<_>>()
            .join(",");
        db.query_sql(&format!(
            "INSERT INTO pages (tenant,ordinal,body) VALUES {values}"
        ))
        .unwrap();
        if cold {
            db.checkpoint().unwrap();
            drop(db);
            db = Database::open_with_config(
                &root.0,
                hawdb::DatabaseConfig {
                    storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        // These become live overlay insertion/deletion/replacement on a cold root.
        db.query_sql("DELETE FROM pages WHERE tenant='g' AND ordinal=255")
            .unwrap();
        db.query_sql("UPDATE pages SET body='updated' WHERE tenant='g' AND ordinal=256")
            .unwrap();
        db.query_sql("INSERT INTO pages (tenant,ordinal,body) VALUES ('z',0,'live')")
            .unwrap();
        let expected = db
            .query_sql("SELECT tenant,ordinal FROM pages ORDER BY tenant,ordinal")
            .unwrap();
        let mut source = db
            .begin_prepared_full_projection_source(
                limits(),
                budget(20000, 2000, 1024 * 1024),
                ProjectionFullSourceLimits::default(),
                Default::default(),
            )
            .unwrap();
        let mut keys = Vec::new();
        loop {
            let step = source
                .visit_next(budget(512, 32, 4096), |_, item| {
                    if let ProjectionFullSourceItem::RelationalKey {
                        table: "pages",
                        key,
                    } = item
                    {
                        keys.push(key.clone());
                    }
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("cold={cold}: {e}"));
            if step.complete {
                break;
            }
        }
        assert_eq!(keys.len(), 520);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (key, row) in keys.iter().zip(expected.rows.iter()) {
            assert_eq!(
                key.0[0],
                hawdb::RelationalValue::Text(match &row["tenant"] {
                    Value::String(v) => v.clone(),
                    _ => panic!("tenant"),
                })
            );
            assert_eq!(
                key.0[1],
                hawdb::RelationalValue::BigInt(match row["ordinal"] {
                    Value::Int(v) => v,
                    _ => panic!("ordinal"),
                })
            );
        }
        source.finish().unwrap();
    }
}
