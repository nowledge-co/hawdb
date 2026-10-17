// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use hawdb::{
    Database, DatabasePublicationOwner, DatabasePublicationState, PreparedReadLimits,
    ProjectionFeedError, ProjectionFeedLimits, ProjectionOwnerBefore, ProjectionOwnerFeedItem,
    ProjectionOwnerId, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn reads() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(64).unwrap(),
        dependency_bytes: NonZeroUsize::new(8192).unwrap(),
        statement_rows: NonZeroUsize::new(16).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(8192).unwrap(),
    }
}
fn budget(items: usize) -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(1024).unwrap(),
        items: NonZeroUsize::new(items).unwrap(),
        bytes: NonZeroUsize::new(8192).unwrap(),
    }
}
#[test]
fn split_graph_and_sql_effects_hydrate_only_the_captured_end_after_later_writes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id:'old'})").unwrap();
    db.query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("INSERT INTO messages (id,body) VALUES (7,'old')")
        .unwrap();
    let c = db.commit_epoch().unwrap();
    db.query("MATCH (n:Thread) SET n.id='renamed'").unwrap();
    db.query_sql("UPDATE messages SET body='intermediate' WHERE id=7")
        .unwrap();
    db.query("MATCH (n:Thread) DELETE n").unwrap();
    db.query_sql("DELETE FROM messages WHERE id=7").unwrap();
    let e = db.commit_epoch().unwrap();
    let mut feed = db
        .begin_prepared_projection_owner_feed(c, reads(), budget(32))
        .unwrap();
    // These relevant writes finish before enumeration, but do not enter E.
    db.query("CREATE (:Thread {id:'later'})").unwrap();
    db.query_sql("INSERT INTO messages (id,body) VALUES (7,'later')")
        .unwrap();
    let mut before_ids = vec![];
    let mut keys = 0;
    let mut ends = 0;
    while !feed.is_complete() {
        let step = feed
            .visit_next(budget(1), |read, item| {
                assert_eq!(read.source_epoch(), e);
                match item {
                    ProjectionOwnerFeedItem::GraphOwner {
                        node_id, before, ..
                    } => {
                        let ProjectionOwnerBefore::Present {
                            labels,
                            id: ProjectionOwnerId::String(id),
                        } = before
                        else {
                            panic!("expected transaction-start identity");
                        };
                        assert_eq!(labels, &[read.graph_label_id("Thread")?.unwrap()]);
                        before_ids.push(id.to_string());
                        let rows = read.query_with_params(
                            "MATCH (n:Thread) WHERE id(n)=$id RETURN n.id AS id",
                            &BTreeMap::from([("id".into(), Value::Int(node_id as i64))]),
                        )?;
                        assert!(rows.output.rows.is_empty());
                    }
                    ProjectionOwnerFeedItem::RelationalKey { table, .. } => {
                        assert_eq!(table, "messages");
                        keys += 1;
                        assert!(read
                            .query_sql_with_params(
                                "SELECT id FROM messages WHERE id=$1",
                                &[Value::Int(7)]
                            )?
                            .rows
                            .is_empty());
                    }
                    ProjectionOwnerFeedItem::End { source_epoch } => {
                        assert_eq!(source_epoch, e);
                        assert_eq!(keys, 2);
                        assert_eq!(before_ids, ["old", "renamed"]);
                        ends += 1;
                    }
                }
                Ok(())
            })
            .unwrap();
        assert!(step.usage.items <= 1);
    }
    assert_eq!((before_ids.len(), keys, ends), (2, 2, 1));
    feed.finish().unwrap();
}
#[test]
fn unfinished_end_callback_and_ignored_query_errors_cannot_finish() {
    let mut db = Database::new();
    let c = db.commit_epoch().unwrap();
    db.query("CREATE (:Thread {id:'a'})").unwrap();
    let feed = db
        .begin_prepared_projection_owner_feed(c, reads(), budget(10))
        .unwrap();
    assert!(matches!(
        feed.finish(),
        Err(ProjectionFeedError::Incomplete)
    ));
    let mut feed = db
        .begin_prepared_projection_owner_feed(c, reads(), budget(10))
        .unwrap();
    assert!(matches!(
        feed.visit_next(budget(10), |read, _| {
            let _ = read.query_sql_with_params("SELECT missing FROM nonexistent", &[]);
            Ok(())
        }),
        Err(ProjectionFeedError::Callback(_))
    ));
    assert!(!feed.is_complete());
    assert!(matches!(feed.finish(), Err(ProjectionFeedError::Failed)));
    let mut end = db
        .begin_prepared_projection_owner_feed(db.commit_epoch().unwrap(), reads(), budget(10))
        .unwrap();
    assert!(matches!(
        end.visit_next(budget(10), |_, item| {
            assert!(matches!(item, ProjectionOwnerFeedItem::End { .. }));
            Err(hawdb::HawDBError::Execution("refuse End".into()))
        }),
        Err(ProjectionFeedError::Callback(_))
    ));
    assert!(matches!(end.finish(), Err(ProjectionFeedError::Failed)));
}
#[test]
fn graph_schema_gap_refuses_owner_resume_before_the_gap() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id:'a'})").unwrap();
    let c = db.commit_epoch().unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    assert!(matches!(
        db.begin_prepared_projection_owner_feed(c, reads(), budget(10)),
        Err(ProjectionFeedError::BehindFloor { .. })
    ));
}
struct State(Database);
impl DatabasePublicationState for State {
    fn database(&self) -> &Database {
        &self.0
    }
}
#[test]
fn competing_writer_commits_through_the_same_owner_while_owner_hydration_is_paused() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id:'a', value:0})").unwrap();
    let c = db.commit_epoch().unwrap();
    db.query("MATCH (n:Memory) SET n.value=1").unwrap();
    let e = db.commit_epoch().unwrap();
    let owner = Arc::new(DatabasePublicationOwner::new(State(db)));
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let worker_owner = Arc::clone(&owner);
    let worker = std::thread::spawn(move || {
        let mut feed = worker_owner
            .with_read(|state| {
                state
                    .0
                    .begin_prepared_projection_owner_feed(c, reads(), budget(10))
                    .map_err(|error| hawdb::HawDBError::Execution(error.to_string()))
            })
            .unwrap();
        let mut visited = 0;
        while !feed.is_complete() {
            feed.visit_next(budget(1), |read, item| {
                if let ProjectionOwnerFeedItem::GraphOwner { node_id, .. } = item {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(30)).unwrap();
                    let output = read.query_with_params(
                        "MATCH (n:Memory) WHERE id(n)=$id RETURN n.value AS value",
                        &BTreeMap::from([("id".into(), Value::Int(node_id as i64))]),
                    )?;
                    assert_eq!(output.output.rows[0]["value"], Value::Int(1));
                    assert_eq!(read.source_epoch(), e);
                    visited += 1;
                }
                Ok(())
            })
            .unwrap();
        }
        feed.finish().unwrap();
        visited
    });
    entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    // The same real outer owner is available while the callback is paused.
    owner
        .with_write(|state| state.0.query("MATCH (n:Memory) SET n.value=2").map(|_| ()))
        .unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), 1);
}

#[test]
fn cold_owner_feed_keeps_preimages_and_queries_after_database_handle_closes() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-owner-feed-cold-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Thread {id:'cold-old'})").unwrap();
    let c = db.commit_epoch().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let open = || {
        Database::open_with_config(
            &path,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let mut db = open();
    db.query("MATCH (n:Thread) SET n.id='cold-renamed'")
        .unwrap();
    db.query("MATCH (n:Thread) DELETE n").unwrap();
    let e = db.commit_epoch().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = open();
    let mut feed = db
        .begin_prepared_projection_owner_feed(c, reads(), budget(32))
        .unwrap();
    drop(db);
    let mut before_ids = vec![];
    let mut end = 0;
    while !feed.is_complete() {
        feed.visit_next(budget(1), |read, item| {
            match item {
                ProjectionOwnerFeedItem::GraphOwner {
                    node_id, before, ..
                } => {
                    let ProjectionOwnerBefore::Present {
                        labels,
                        id: ProjectionOwnerId::String(id),
                    } = before
                    else {
                        panic!("missing cold preimage");
                    };
                    assert_eq!(labels, &[read.graph_label_id("Thread")?.unwrap()]);
                    before_ids.push(id.to_string());
                    assert!(read
                        .query_with_params(
                            "MATCH (n:Thread) WHERE id(n)=$id RETURN n.id AS id",
                            &BTreeMap::from([("id".into(), Value::Int(node_id as i64))])
                        )?
                        .output
                        .rows
                        .is_empty());
                }
                ProjectionOwnerFeedItem::End { source_epoch } => {
                    assert_eq!(source_epoch, e);
                    end += 1;
                }
                _ => panic!("unexpected relational input"),
            }
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(before_ids, ["cold-old", "cold-renamed"]);
    assert_eq!(end, 1);
    drop(feed.finish().unwrap());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn metadata_absence_publication_rejects_implicit_label_creation_but_allows_other_writes() {
    for related in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "hawdb-owner-label-publication-{}-{}-{related}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let mut db = Database::open(root.join("db")).unwrap();
        db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
        let e = db.commit_epoch().unwrap();
        let mut feed = db
            .begin_prepared_projection_owner_feed(e, reads(), budget(10))
            .unwrap();
        feed.visit_next(budget(10), |read, item| {
            assert!(matches!(item, ProjectionOwnerFeedItem::End { .. }));
            assert_eq!(read.graph_label_id("Later")?, None);
            Ok(())
        })
        .unwrap();
        let dependencies = feed.finish().unwrap();
        assert_eq!(dependencies.node_domain_count(), 1);
        assert!(!dependencies.requires_unchanged_database_epoch());
        db.query(if related {
            "CREATE (:Later {id:'b'})"
        } else {
            "MATCH (n:Memory) SET n.value=2"
        })
        .unwrap();
        // This controlled empty candidate uses the same generic source-fenced
        // backend as the standalone regression probe. It is not a coverage test.
        let writer = hawdb::SearchOutOfCoreGenerationWriter::create(
            root.join("search"),
            hawdb::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(e),
                ..Default::default()
            },
        )
        .unwrap();
        let result =
            db.publish_prepared_search(&dependencies, writer.prepare_publication().unwrap());
        if related {
            assert!(matches!(
                result,
                Err(hawdb::ProjectionPublicationError::Dependency(_))
            ));
        } else {
            assert_eq!(result.unwrap().source_graph_commit_epoch, Some(e));
        }
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn endpoint_property_inputs_cross_one_item_steps_at_one_pinned_end() {
    for cold in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "hawdb-endpoint-feed-{}-{}-{cold}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut db = Database::open(&root).unwrap();
        db.query("CREATE (:Parent {id:'a'})").unwrap();
        db.query("CREATE (:Child {id:'b'})").unwrap();
        db.query("MATCH (a:Parent),(b:Child) CREATE (a)-[:DEPENDS_ON {weight:1}]->(b)")
            .unwrap();
        let parent = db
            .query("MATCH (a:Parent) RETURN id(a) AS id")
            .unwrap()
            .rows[0]
            .get("id")
            .unwrap()
            .clone();
        let child = db.query("MATCH (b:Child) RETURN id(b) AS id").unwrap().rows[0]
            .get("id")
            .unwrap()
            .clone();
        if cold {
            db.checkpoint().unwrap();
        }
        let c = db.commit_epoch().unwrap();
        db.query("MATCH (a:Parent)-[r:DEPENDS_ON]->(b:Child) SET r.weight=2")
            .unwrap();
        let e = db.commit_epoch().unwrap();
        let mut config = reads();
        config.dependency_entries = NonZeroUsize::new(3).unwrap();
        let mut feed = db
            .begin_prepared_projection_owner_feed_with_scopes(
                c,
                config,
                budget(8),
                hawdb::ProjectionReadScopes {
                    node_labels: &["Parent", "Child"],
                    relationship_types: &["DEPENDS_ON"],
                    ..Default::default()
                },
            )
            .unwrap();
        db.query("MATCH (a:Parent)-[r:DEPENDS_ON]->(b:Child) SET r.weight=3")
            .unwrap();
        let mut observed = vec![];
        let mut ended = 0;
        let mut steps = 0;
        while !feed.is_complete() {
            steps += 1;
            assert!(steps <= 16, "one-item feed did not progress");
            let result=feed.visit_next(budget(1),|read,item| {
                assert_eq!(read.source_epoch(),e);
                match item {
                    ProjectionOwnerFeedItem::GraphOwner {epoch,node_id,before} => {
                        assert_eq!(epoch,e);assert_eq!(before,ProjectionOwnerBefore::NotRecorded);
                        observed.push(Value::Int(i64::try_from(node_id).unwrap()));
                        let rows=read.query_with_params("MATCH (a:Parent)-[r:DEPENDS_ON]->(b:Child) WHERE id(a)=$id RETURN r.weight AS weight",&BTreeMap::from([("id".into(),parent.clone())]))?;
                        assert_eq!(rows.output.rows.len(),1);
                        assert_eq!(rows.output.rows[0].get("weight"),Some(&Value::Int(2)));
                    }
                    ProjectionOwnerFeedItem::End {source_epoch} => {assert_eq!(source_epoch,e);ended+=1;}
                    ProjectionOwnerFeedItem::RelationalKey {..} => panic!("unexpected relational input"),
                }
                Ok(())
            }).unwrap();
            assert!(result.usage.items <= 1);
        }
        assert_eq!(observed, vec![parent, child]);
        assert_eq!(ended, 1);
        let deps = feed.finish().unwrap();
        assert_eq!(deps.node_domain_count(), 2);
        assert_eq!(deps.relationship_domain_count(), 1);
        // Enumeration and dependencies only: no stage or watermark receipt.
        drop(deps);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
