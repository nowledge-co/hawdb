// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use hawdb::{
    Database, DatabasePublicationOwner, DatabasePublicationState, PreparedReadLimits,
    ProjectionPublicationError, SearchDocument, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Duration;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-mvcc-owner-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        // Timestamp resolution alone does not reserve a unique parallel-test
        // namespace. Reserve it before returning any database/Search handle.
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct State(Database);
impl DatabasePublicationState for State {
    fn database(&self) -> &Database {
        &self.0
    }
}
fn database(path: &std::path::Path) -> Database {
    let mut db = Database::open(path).unwrap();
    db.query("CREATE (:Memory {id: 'a', value: 1})").unwrap();
    db.query("CREATE (:Other {id: 'b', value: 1})").unwrap();
    db.query("CREATE (:Binding {incarnation: 'current'})")
        .unwrap();
    db
}
fn limits() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(32).unwrap(),
        dependency_bytes: NonZeroUsize::new(4096).unwrap(),
        statement_rows: NonZeroUsize::new(8).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(4096).unwrap(),
    }
}

#[test]
fn owner_composes_private_preparation_with_source_and_binding_validation() {
    for mutation in ["disjoint", "related", "binding_aba", "replace"] {
        let root = Directory::new();
        let owner = Arc::new(DatabasePublicationOwner::new(State(database(&root.0))));
        let search = root.0.join("search");
        let (captured_tx, captured_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let worker_owner = Arc::clone(&owner);
        let worker_search = search.clone();
        let preparation = std::thread::spawn(move || {
            let mut read = worker_owner
                .with_read(|state| state.0.begin_prepared_read(limits()))
                .unwrap();
            // The binding is a host-owned graph row in this fixture. Its
            // captured dependency, rather than an equal latest value, fences ABA.
            let binding = read
                .query_with_params(
                    "MATCH (b:Binding) WHERE id(b) = $id RETURN b.incarnation AS incarnation",
                    &BTreeMap::from([("id".into(), Value::Int(2))]),
                )
                .unwrap();
            assert_eq!(
                binding.output.rows[0]["incarnation"],
                Value::String("current".into())
            );
            captured_tx.send(read.source_epoch()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            // Querying and artifact construction both happen after capture has
            // released the actual public owner, against the original snapshot.
            let rows = read
                .query_with_params(
                    "MATCH (n:Memory) WHERE id(n) = $id RETURN n.value AS value",
                    &BTreeMap::from([("id".into(), Value::Int(0))]),
                )
                .unwrap();
            assert_eq!(rows.output.rows[0]["value"], Value::Int(1));
            let epoch = read.source_epoch();
            let dependencies = read.finish().unwrap();
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &worker_search,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(epoch),
                    ..Default::default()
                },
            )
            .unwrap();
            writer
                .push(SearchDocument {
                    id: "a".into(),
                    title: "a".into(),
                    content: "1".into(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
            (dependencies, writer.prepare_publication().unwrap())
        });
        let captured = captured_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let (committed_tx, committed_rx) = mpsc::sync_channel(1);
        let writer_owner = Arc::clone(&owner);
        let replacement = root.0.join("replacement");
        let foreground = std::thread::spawn(move || {
            let epoch = writer_owner
                .with_write(|state| {
                    match mutation {
                        "disjoint" => {
                            state.0.query("MATCH (n:Other) SET n.value = 2")?;
                        }
                        "related" => {
                            state.0.query("MATCH (n:Memory) SET n.value = 2")?;
                        }
                        "binding_aba" => {
                            state
                                .0
                                .query("MATCH (b:Binding) SET b.incarnation = 'away'")?;
                            state
                                .0
                                .query("MATCH (b:Binding) SET b.incarnation = 'current'")?;
                        }
                        "replace" => state.0 = database(&replacement),
                        _ => unreachable!(),
                    }
                    state.0.commit_epoch()
                })
                .unwrap();
            committed_tx.send(epoch).unwrap();
        });
        // Completion, not just thread creation or a try-lock, proves that a
        // foreground write finishes while preparation remains paused.
        let committed = committed_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        if mutation == "replace" {
            assert_eq!(committed, captured);
        } else {
            assert!(committed > captured);
        }
        assert!(!search
            .join("search_projection.out_of_core.manifest.hawdb")
            .exists());
        resume_tx.send(()).unwrap();
        foreground.join().unwrap();
        let (dependencies, candidate) = preparation.join().unwrap();
        let result = owner
            .with_validated_publication(
                |snapshot| {
                    let result = snapshot
                        .query("MATCH (b:Binding) RETURN b.incarnation AS incarnation LIMIT 2")?;
                    assert_eq!(result.rows.len(), 1);
                    assert_eq!(
                        result.rows[0]["incarnation"],
                        Value::String("current".into())
                    );
                    Ok(())
                },
                |state| Ok(state.0.publish_prepared_search(&dependencies, candidate)),
            )
            .unwrap();
        match mutation {
            "disjoint" => {
                assert_eq!(result.unwrap().source_graph_commit_epoch, Some(captured));
                let reader = SearchOutOfCoreReader::open(&search).unwrap();
                assert_eq!(reader.source_graph_commit_epoch(), Some(captured));
                assert_eq!(reader.document_count(), 1);
            }
            "replace" => assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceChanged)
            )),
            _ => assert!(matches!(
                result,
                Err(ProjectionPublicationError::Dependency(_))
            )),
        }
        if mutation != "disjoint" {
            assert!(!search
                .join("search_projection.out_of_core.manifest.hawdb")
                .exists());
            assert!(SearchOutOfCoreReader::open(&search).is_err());
            assert!(!std::fs::read_dir(&search).unwrap().any(|entry| entry
                .unwrap()
                .file_type()
                .unwrap()
                .is_dir()));
        }
    }
}

fn feed_limits(work: usize, items: usize, bytes: usize) -> hawdb::ProjectionFeedLimits {
    hawdb::ProjectionFeedLimits {
        work: NonZeroUsize::new(work).unwrap(),
        items: NonZeroUsize::new(items).unwrap(),
        bytes: NonZeroUsize::new(bytes).unwrap(),
    }
}

#[test]
fn prepared_feed_pins_graph_relational_inputs_across_trim_and_later_writes() {
    use hawdb::{ProjectionFeedError, ProjectionFeedItem, RelationalValue};
    for cold in [false, true] {
        for related in [false, true] {
            let root = Directory::new();
            let config = hawdb::DatabaseConfig {
                max_search_projection_change_log_entries: Some(3),
                storage_residency_mode: if cold {
                    hawdb::StorageResidencyMode::OutOfCore
                } else {
                    hawdb::StorageResidencyMode::Materialized
                },
                ..Default::default()
            };
            let mut db = Database::open_with_config(&root.0, config.clone()).unwrap();
            db.query("CREATE (:Memory {id: 'a', title: 'old'})")
                .unwrap();
            db.query("CREATE (:Other {value: 1})").unwrap();
            db.query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
                .unwrap();
            db.query_sql("INSERT INTO records (id, body) VALUES (2, 'unrelated')")
                .unwrap();
            let start = db.commit_epoch().unwrap();
            db.query("MATCH (n:Memory) SET n.title = 'new'").unwrap();
            db.query_sql("INSERT INTO records (id, body) VALUES (1, 'at-E')")
                .unwrap();
            db.query("MATCH (n:Memory) DETACH DELETE n").unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(&root.0, config).unwrap();
            }
            let source = db.commit_epoch().unwrap();
            let mut feed = db
                .begin_prepared_projection_feed(start, limits(), feed_limits(1000, 100, 10000))
                .unwrap();
            assert_eq!(feed.source_epoch(), source);
            assert_eq!(feed.start_epoch(), start);
            db.query_sql(if related {
                "UPDATE records SET body = 'later' WHERE id = 1"
            } else {
                "UPDATE records SET body = 'later' WHERE id = 2"
            })
            .unwrap();
            db.query("MATCH (n:Other) SET n.value = 9").unwrap();
            assert!(matches!(
                db.begin_prepared_projection_feed(start, limits(), feed_limits(1000, 100, 10000)),
                Err(ProjectionFeedError::BehindFloor { .. })
            ));
            let mut nodes = Vec::new();
            let mut deletes = Vec::new();
            let mut body = None;
            let mut ends = 0;
            while !feed.is_complete() {
                let report = feed
                    .visit_next(feed_limits(5, 1, 1024), |read, item| {
                        assert_eq!(read.source_epoch(), source);
                        match item {
                            ProjectionFeedItem::GraphNode { node_id, .. } => {
                                nodes.push(node_id);
                                assert!(read
                                    .hydrate_search_projection_nodes(
                                        &[node_id],
                                        NonZeroUsize::new(1024).unwrap()
                                    )?
                                    .is_empty());
                            }
                            ProjectionFeedItem::DeleteDocument { document_id, .. } => {
                                deletes.push(document_id.to_owned())
                            }
                            ProjectionFeedItem::RelationalKey { table, key, .. } => {
                                assert_eq!(table, "records");
                                let RelationalValue::BigInt(id) = key.0[0] else {
                                    panic!("expected integer key")
                                };
                                assert_eq!(id, 1);
                                let output = read.query_sql_with_params(
                                    "SELECT body FROM records WHERE id = $1",
                                    &[Value::Int(id)],
                                )?;
                                assert_eq!(output.rows[0]["body"], Value::String("at-E".into()));
                                body = Some("at-E".to_string());
                            }
                            ProjectionFeedItem::End { source_epoch } => {
                                assert_eq!(source_epoch, source);
                                ends += 1;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                assert!(
                    report.usage.work <= 5 && report.usage.items <= 1 && report.usage.bytes <= 1024
                );
            }
            assert_eq!(nodes, [0]);
            assert_eq!(deletes, ["memory:a"]);
            assert_eq!(ends, 1);
            let dependencies = feed.finish().unwrap();
            assert_eq!(dependencies.exact_relational_row_count(), 1);
            assert!(!dependencies.requires_unchanged_database_epoch());
            let search = root.0.join("search");
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &search,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(source),
                    ..Default::default()
                },
            )
            .unwrap();
            writer
                .push(SearchDocument {
                    id: "record:1".into(),
                    title: "record:1".into(),
                    content: body.unwrap(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
            let result =
                db.publish_prepared_search(&dependencies, writer.prepare_publication().unwrap());
            if related {
                assert!(matches!(
                    result,
                    Err(ProjectionPublicationError::Dependency(_))
                ));
                assert!(SearchOutOfCoreReader::open(&search).is_err());
            } else {
                assert_eq!(result.unwrap().source_graph_commit_epoch, Some(source));
                let reader = SearchOutOfCoreReader::open(&search).unwrap();
                assert_eq!(
                    reader
                        .hydrate_documents(&["record:1".into()])
                        .unwrap()
                        .documents[0]
                        .content,
                    "at-E"
                );
            }
        }
    }
}

#[test]
fn prepared_feed_refuses_incomplete_ignored_error_and_panicked_callbacks() {
    use hawdb::ProjectionFeedError;
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'a'})").unwrap();
    let source = db.commit_epoch().unwrap();
    let feed = db
        .begin_prepared_projection_feed(source, limits(), feed_limits(100, 100, 1000))
        .unwrap();
    assert!(matches!(
        feed.finish(),
        Err(ProjectionFeedError::Incomplete)
    ));
    let mut feed = db
        .begin_prepared_projection_feed(source, limits(), feed_limits(100, 100, 1000))
        .unwrap();
    assert!(matches!(
        feed.visit_next(feed_limits(100, 100, 1000), |read, _| {
            assert!(read
                .query_with_params("CREATE (:Forbidden)", &BTreeMap::new())
                .is_err());
            Ok(())
        }),
        Err(ProjectionFeedError::Callback(_))
    ));
    assert!(!feed.is_complete());
    assert!(matches!(feed.finish(), Err(ProjectionFeedError::Failed)));
    let mut feed = db
        .begin_prepared_projection_feed(source, limits(), feed_limits(100, 100, 1000))
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = feed.visit_next(feed_limits(100, 100, 1000), |_, _| panic!("mapping panic"));
    }));
    assert!(panic.is_err());
    assert!(matches!(feed.finish(), Err(ProjectionFeedError::Failed)));
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn prepared_feed_splits_one_canonical_transaction_and_counts_repeated_commits() {
    use hawdb::{ProjectionFeedError, ProjectionFeedItem, ProjectionFeedResource};
    let mut db = Database::new();
    let start = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for id in 0..64 {
        transaction
            .query_with_params(
                "CREATE (:Memory {id: $id})",
                &BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    assert_eq!(db.commit_epoch().unwrap(), start + 1);
    let mut feed = db
        .begin_prepared_projection_feed(start, limits(), feed_limits(1000, 1000, 10000))
        .unwrap();
    let mut nodes = Vec::new();
    let mut ends = 0;
    let mut steps = 0;
    while !feed.is_complete() {
        let report = feed
            .visit_next(feed_limits(7, 3, 64), |_, item| {
                match item {
                    ProjectionFeedItem::GraphNode { node_id, .. } => nodes.push(node_id),
                    ProjectionFeedItem::End { .. } => ends += 1,
                    _ => panic!("unexpected input in node-only transaction"),
                }
                Ok(())
            })
            .unwrap();
        assert!(report.usage.items <= 3 && report.usage.work <= 7 && report.usage.bytes <= 64);
        steps += 1;
    }
    assert_eq!(nodes, (0..64).collect::<Vec<_>>());
    assert_eq!(ends, 1);
    assert!(steps > 20);
    feed.finish().unwrap();
    let start = db.commit_epoch().unwrap();
    for value in 0..16 {
        db.query_with_params(
            "MATCH (n:Memory) WHERE id(n) = 0 SET n.version = $value",
            &BTreeMap::from([("value".into(), Value::Int(value))]),
        )
        .unwrap();
    }
    let mut feed = db
        .begin_prepared_projection_feed(start, limits(), feed_limits(50, 1000, 10000))
        .unwrap();
    let mut repeated = 0;
    loop {
        let result = feed.visit_next(feed_limits(7, 3, 64), |_, item| {
            if let ProjectionFeedItem::GraphNode { node_id, .. } = item {
                assert_eq!(node_id, 0);
                repeated += 1;
            }
            Ok(())
        });
        if let Err(error) = result {
            assert!(matches!(
                error,
                ProjectionFeedError::BudgetExceeded(ProjectionFeedResource::Work)
            ));
            break;
        }
    }
    assert!(repeated > 3 && repeated < 16);
    assert!(!feed.is_complete());
    assert!(feed.finish().is_err());
}

#[test]
fn pinned_feed_and_external_sort_preserve_the_complete_final_graph_and_sql_set() {
    use hawdb::{
        ProjectionFeedItem, RelationalValue, RuntimeTaskContext, SearchUnorderedMutationLimits,
    };
    for cold in [false, true] {
        for related in [false, true] {
            let root = Directory::new();
            let graph = root.0.join("graph");
            let search = root.0.join("search");
            let config = hawdb::DatabaseConfig {
                storage_residency_mode: if cold {
                    hawdb::StorageResidencyMode::OutOfCore
                } else {
                    hawdb::StorageResidencyMode::Materialized
                },
                ..Default::default()
            };
            let mut db = Database::open_with_config(&graph, config.clone()).unwrap();
            db.query("CREATE (:Memory {id: 'a', title: 'old'})")
                .unwrap();
            db.query("CREATE (:Other {value: 1})").unwrap();
            db.query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
                .unwrap();
            db.query_sql("INSERT INTO records (id, body) VALUES (1, 'old')")
                .unwrap();
            let start = db.commit_epoch().unwrap();
            let document = |id: &str, body: &str| SearchDocument {
                id: id.into(),
                title: id.into(),
                content: body.into(),
                embedding: None,
                metadata: BTreeMap::new(),
            };
            let mut baseline = SearchOutOfCoreGenerationWriter::create(
                &search,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(start),
                    ..Default::default()
                },
            )
            .unwrap();
            baseline.push(document("memory:a", "old")).unwrap();
            baseline.push(document("record:1", "old")).unwrap();
            baseline.finish().unwrap();
            db.query("MATCH (n:Memory) SET n.title = 'intermediate'")
                .unwrap();
            db.query_sql("UPDATE records SET body = 'intermediate' WHERE id = 1")
                .unwrap();
            db.query("MATCH (n:Memory) DETACH DELETE n").unwrap();
            db.query_sql("UPDATE records SET body = 'at-E' WHERE id = 1")
                .unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(&graph, config).unwrap();
            }
            let source = db.commit_epoch().unwrap();
            let owner = Arc::new(DatabasePublicationOwner::new(State(db)));
            let mut feed = owner
                .with_read(|state| {
                    state
                        .0
                        .begin_prepared_projection_feed(
                            start,
                            limits(),
                            feed_limits(1000, 100, 10000),
                        )
                        .map_err(|error| hawdb::HawDBError::Execution(error.to_string()))
                })
                .unwrap();
            let reader = SearchOutOfCoreReader::open(&search).unwrap();
            let mut sorting = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &reader,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(source),
                    ..Default::default()
                },
                SearchUnorderedMutationLimits {
                    max_buffer_records: NonZeroUsize::new(1).unwrap(),
                    ..Default::default()
                },
                RuntimeTaskContext::default(),
            )
            .unwrap();
            // The actual public owner's writer completes before any hydration
            // or sort resumes. All mapped data must still come from pinned E.
            let foreground = Arc::clone(&owner);
            let (tx, rx) = mpsc::sync_channel(1);
            let worker = std::thread::spawn(move || {
                foreground
                    .with_write(|state| {
                        if related {
                            state
                                .0
                                .query_sql("UPDATE records SET body = 'later' WHERE id = 1")?;
                        } else {
                            state.0.query("MATCH (n:Other) SET n.value = 2")?;
                        }
                        Ok(())
                    })
                    .unwrap();
                tx.send(()).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(30)).unwrap();
            worker.join().unwrap();
            let mut callbacks = 0;
            let mut end = false;
            while !feed.is_complete() {
                let step = feed
                    .visit_next(feed_limits(8, 1, 2048), |read, item| {
                        callbacks += 1;
                        match item {
                            ProjectionFeedItem::GraphNode { node_id, .. } => {
                                assert!(read
                                    .hydrate_search_projection_nodes(
                                        &[node_id],
                                        NonZeroUsize::new(2048).unwrap(),
                                    )?
                                    .is_empty());
                            }
                            ProjectionFeedItem::DeleteDocument { document_id, .. } => {
                                sorting.queue_delete(document_id)?
                            }
                            ProjectionFeedItem::RelationalKey { table, key, .. } => {
                                assert_eq!(table, "records");
                                let RelationalValue::BigInt(id) = key.0[0] else {
                                    panic!("expected integer key")
                                };
                                let result = read.query_sql_with_params(
                                    "SELECT body FROM records WHERE id = $1",
                                    &[Value::Int(id)],
                                )?;
                                assert_eq!(result.rows[0]["body"], Value::String("at-E".into()));
                                sorting.queue_upsert(document(&format!("record:{id}"), "at-E"))?;
                            }
                            ProjectionFeedItem::End { source_epoch } => {
                                assert_eq!(source_epoch, source);
                                end = true;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                assert!(step.usage.items <= 1);
                while !sorting.ready_for_input() {
                    let step = sorting
                        .advance_ordering(hawdb::SearchMutationSortStepLimits {
                            work: NonZeroUsize::new(16).unwrap(),
                            read_bytes: std::num::NonZeroU64::new(1024).unwrap(),
                            spill_bytes: std::num::NonZeroU64::new(1024).unwrap(),
                        })
                        .unwrap();
                    assert!(
                        step.usage.work <= 16
                            && step.usage.read_bytes <= 1024
                            && step.usage.spill_bytes <= 1024
                    );
                }
                assert_eq!(
                    SearchOutOfCoreReader::open(&search).unwrap().generation(),
                    reader.generation()
                );
            }
            assert!(end && callbacks >= 4);
            let dependencies = feed.finish().unwrap();
            sorting.finish_input().unwrap();
            while !sorting.ordering_complete() {
                let step = sorting
                    .advance_ordering(hawdb::SearchMutationSortStepLimits {
                        work: NonZeroUsize::new(16).unwrap(),
                        read_bytes: std::num::NonZeroU64::new(1024).unwrap(),
                        spill_bytes: std::num::NonZeroU64::new(1024).unwrap(),
                    })
                    .unwrap();
                assert!(
                    step.usage.work <= 16
                        && step.usage.read_bytes <= 1024
                        && step.usage.spill_bytes <= 1024
                );
                assert_eq!(
                    SearchOutOfCoreReader::open(&search).unwrap().generation(),
                    reader.generation()
                );
            }
            let (report, candidate) = sorting.prepare_publication().unwrap();
            assert_eq!(report.output_operations, 2);
            // This composes the primitives; callback correctness is a fixture
            // premise. It does not mint an artifact-bound coverage receipt.
            let result = owner
                .with_write(|state| Ok(state.0.publish_prepared_search(&dependencies, candidate)))
                .unwrap();
            let current = SearchOutOfCoreReader::open(&search).unwrap();
            if related {
                assert!(matches!(
                    result,
                    Err(ProjectionPublicationError::Dependency(_))
                ));
                assert_eq!(current.generation(), reader.generation());
                assert_eq!(current.source_graph_commit_epoch(), Some(start));
                assert_eq!(current.document_count(), 2);
            } else {
                result.unwrap();
                assert_eq!(current.source_graph_commit_epoch(), Some(source));
                assert_eq!(current.document_count(), 1);
                assert_eq!(
                    current
                        .hydrate_documents(&["record:1".into()])
                        .unwrap()
                        .documents[0]
                        .content,
                    "at-E"
                );
                assert!(current.hydrate_documents(&["memory:a".into()]).is_err());
            }
        }
    }
}
