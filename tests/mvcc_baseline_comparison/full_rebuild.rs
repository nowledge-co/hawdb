// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.
use super::*;
fn rebuild_config() -> ProjectionFullRebuildConfig {
    let c = config();
    ProjectionFullRebuildConfig {
        reads: c.reads,
        feed: c.feed,
        source: c.source,
        mapping: c.mapping,
        sorting: c.sorting,
        build: c.build,
        task: c.task,
    }
}
fn advance(
    s: &mut ProjectionFullRebuildSession,
) -> std::result::Result<ProjectionFullRebuildStep, ProjectionFeedError> {
    s.advance(
        ProjectionFeedLimits {
            work: nz(512),
            items: nz(1),
            bytes: nz(4096),
        },
        SearchMutationSortStepLimits {
            work: nz(64),
            read_bytes: n64(8192),
            spill_bytes: n64(8192),
        },
    )
}
fn drain(s: &mut ProjectionFullRebuildSession) {
    for _ in 0..10000 {
        if advance(s).unwrap() == ProjectionFullRebuildStep::Complete {
            return;
        }
    }
    panic!("full rebuild did not complete")
}
fn released(g: &RuntimeGovernor) {
    assert_eq!(g.snapshot().active_background_tasks, 0);
    assert_eq!(g.snapshot().admitted_memory_bytes, 0);
}
fn require_complete_baseline(db: &Database, reader: &SearchOutOfCoreReader, g: &RuntimeGovernor) {
    let mut s = db
        .begin_projection_baseline_comparison(reader, config(), mapped, permit(g))
        .unwrap();
    complete(&mut s);
    assert_eq!(
        s.finish().unwrap().report().compared_documents,
        reader.document_count()
    );
    released(g);
}
#[test]
fn full_rebuild_unknown_lagged_extra_and_partitioned_heads_hot_cold() {
    for cold in [false, true] {
        for kind in ["unknown_epoch", "extra", "vector"] {
            let (root, mut db, reader) = setup(cold, kind);
            db.query("MATCH (n:Memory) SET n.value=88").unwrap();
            db.query_sql("UPDATE records SET body='updated' WHERE id='a'")
                .unwrap();
            let g = governor();
            let mut cfg = rebuild_config();
            cfg.build.max_content_documents = nz(1);
            let mut s = db
                .begin_projection_full_rebuild(&reader, cfg, mapped, permit(&g))
                .unwrap();
            let c = s.source_epoch();
            drain(&mut s);
            let p = s.finish().unwrap();
            assert_eq!(p.source_epoch(), c);
            assert_eq!(p.expected_active_generation(), Some(reader.generation()));
            assert_eq!(p.sorting_report().input_operations, 3);
            assert_eq!(g.snapshot().active_background_tasks, 1);
            unchanged(&root, &reader);
            let report = db
                .publish_projection_full_rebuild(p, ProjectionSourceDurability::RequireKnownDurable)
                .unwrap();
            assert_eq!(report.document_count, 3);
            assert!(report.published_content_segments >= 3);
            released(&g);
            let rebuilt = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
            assert_eq!(rebuilt.document_count(), 3);
            assert_eq!(rebuilt.source_graph_commit_epoch(), Some(c));
            require_complete_baseline(&db, &rebuilt, &g);
        }
    }
}
#[test]
fn full_rebuild_empty_source_removes_old_documents_and_reopens() {
    let (root, mut db, reader) = setup(true, "match");
    db.query("MATCH (n:Memory) DELETE n").unwrap();
    db.query_sql("DELETE FROM records WHERE id IN ('a','z')")
        .unwrap();
    let g = governor();
    let mut s = db
        .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
        .unwrap();
    let c = s.source_epoch();
    drain(&mut s);
    let p = s.finish().unwrap();
    assert_eq!(p.sorting_report().input_operations, 0);
    db.publish_projection_full_rebuild(p, ProjectionSourceDurability::RequireKnownDurable)
        .unwrap();
    released(&g);
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(root.0.join("graph")).unwrap();
    let rebuilt = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
    assert_eq!(rebuilt.document_count(), 0);
    assert_eq!(rebuilt.source_graph_commit_epoch(), Some(c));
    require_complete_baseline(&db, &rebuilt, &g);
}
#[test]
fn full_rebuild_source_drift_and_instance_replacement_refuse_before_visibility() {
    for change in ["relevant", "unrelated", "instance"] {
        let (root, mut db, reader) = setup(false, "extra");
        let g = governor();
        let mut s = db
            .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
            .unwrap();
        match change {
            "relevant" => {
                db.query("MATCH (n:Memory) SET n.value=99").unwrap();
            }
            "unrelated" => {
                db.query("CREATE (:Unrelated {id:1})").unwrap();
            }
            "instance" => {
                db = Database::new();
            }
            _ => unreachable!(),
        }
        drain(&mut s);
        let p = s.finish().unwrap();
        assert!(db
            .publish_projection_full_rebuild(p, ProjectionSourceDurability::RequireKnownDurable)
            .is_err());
        released(&g);
        unchanged(&root, &reader);
    }
}
#[test]
fn full_rebuild_search_cas_rejects_a_newer_head_and_retains_it() {
    let (root, db, reader) = setup(false, "extra");
    let g = governor();
    let mut s = db
        .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
        .unwrap();
    drain(&mut s);
    let p = s.finish().unwrap();
    let mut other =
        SearchOutOfCoreGenerationWriter::create(root.0.join("search"), Default::default()).unwrap();
    other.push(document("competitor", "newer")).unwrap();
    let report = other.finish().unwrap();
    let mut db = db;
    assert!(db
        .publish_projection_full_rebuild(p, ProjectionSourceDurability::RequireKnownDurable)
        .is_err());
    released(&g);
    let held = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
    assert_eq!(held.generation(), report.generation);
    assert_eq!(held.document_count(), 1);
}
#[test]
fn full_rebuild_failed_intake_and_incomplete_finish_preserve_permit_custody() {
    for mapper in [bad_read as ProjectionBaselineMappingFunction, boundary_doc] {
        let (root, db, reader) = setup(false, "match");
        let g = governor();
        let mut s = db
            .begin_projection_full_rebuild(&reader, rebuild_config(), mapper, permit(&g))
            .unwrap();
        let mut refused = false;
        for _ in 0..1000 {
            if advance(&mut s).is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused);
        assert!(advance(&mut s).is_err());
        assert_eq!(g.snapshot().active_background_tasks, 1);
        assert!(s.finish().is_err());
        released(&g);
        unchanged(&root, &reader);
    }
    for kind in ["duplicate", "unknown_table", "match"] {
        let (root, db, reader) = setup(false, kind);
        let g = governor();
        let mut s = db
            .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
            .unwrap();
        if kind != "match" {
            let mut refused = false;
            for _ in 0..1000 {
                if advance(&mut s).is_err() {
                    refused = true;
                    break;
                }
            }
            assert!(refused);
        }
        assert!(s.finish().is_err());
        released(&g);
        unchanged(&root, &reader);
    }
}
#[test]
fn full_rebuild_panics_and_cumulative_mapping_refusal_are_sticky() {
    let (root, db, reader) = setup(false, "match");
    let g = governor();
    let mut s = db
        .begin_projection_full_rebuild(&reader, rebuild_config(), panic_map, permit(&g))
        .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| advance(&mut s))).is_err());
    assert!(advance(&mut s).is_err());
    assert_eq!(g.snapshot().active_background_tasks, 1);
    drop(s);
    released(&g);
    let mut cfg = rebuild_config();
    cfg.mapping.calls = nz(1);
    let mut s = db
        .begin_projection_full_rebuild(&reader, cfg, mapped, permit(&g))
        .unwrap();
    assert!(advance(&mut s).is_ok());
    assert!(advance(&mut s).is_err());
    assert!(s.finish().is_err());
    released(&g);
    unchanged(&root, &reader);
}

#[test]
fn full_rebuild_cancel_and_cumulative_query_refusal_preserve_the_old_head() {
    let (root, mut db, reader) = setup(false, "extra");
    let g = governor();
    for kind in [
        "cancel",
        "cancel_done",
        "cancel_prepared",
        "queries",
        "caller_epoch",
    ] {
        let mut cfg = rebuild_config();
        let cancel = cfg.task.cancellation().clone();
        if kind == "queries" {
            cfg.mapping.queries = nz(1);
        }
        if kind == "caller_epoch" {
            cfg.build.source_graph_commit_epoch = Some(db.commit_epoch().unwrap());
            assert!(db
                .begin_projection_full_rebuild(&reader, cfg, mapped, permit(&g))
                .is_err());
            released(&g);
            continue;
        }
        let mut s = db
            .begin_projection_full_rebuild(&reader, cfg, mapped, permit(&g))
            .unwrap();
        match kind {
            "cancel" => {
                cancel.cancel();
                assert!(advance(&mut s).is_err());
            }
            "queries" => {
                let mut refused = false;
                for _ in 0..1000 {
                    if advance(&mut s).is_err() {
                        refused = true;
                        break;
                    }
                }
                assert!(refused);
            }
            _ => {
                drain(&mut s);
            }
        }
        if kind == "cancel_prepared" {
            let p = s.finish().unwrap();
            cancel.cancel();
            assert!(db
                .publish_projection_full_rebuild(p, ProjectionSourceDurability::RequireKnownDurable)
                .is_err());
        } else {
            if kind == "cancel_done" {
                cancel.cancel();
            }
            assert_eq!(g.snapshot().active_background_tasks, 1);
            assert!(s.finish().is_err());
        }
        released(&g);
        unchanged(&root, &reader);
    }
}
#[test]
fn full_rebuild_relaxed_and_recovered_source_need_an_explicit_barrier() {
    for reopen in [false, true] {
        let root = Root::new();
        let graph = root.0.join("graph");
        let mut db =
            Database::open_with_durability(&graph, DurabilityPolicy::SyncOnCheckpoint).unwrap();
        db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            root.0.join("search"),
            SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(db.commit_epoch().unwrap()),
                ..Default::default()
            },
        )
        .unwrap();
        writer.push(document("extra", "old")).unwrap();
        writer.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
        if reopen {
            drop(db);
            db = Database::open(&graph).unwrap();
        }
        let g = governor();
        let mut s = db
            .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
            .unwrap();
        drain(&mut s);
        assert!(matches!(
            db.publish_projection_full_rebuild(
                s.finish().unwrap(),
                ProjectionSourceDurability::RequireKnownDurable
            ),
            Err(ProjectionPublicationError::SourceNotDurable)
        ));
        released(&g);
        unchanged(&root, &reader);
        let mut s = db
            .begin_projection_full_rebuild(&reader, rebuild_config(), mapped, permit(&g))
            .unwrap();
        drain(&mut s);
        db.publish_projection_full_rebuild(
            s.finish().unwrap(),
            ProjectionSourceDurability::Synchronize,
        )
        .unwrap();
        released(&g);
        let rebuilt = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
        assert_eq!(rebuilt.document_count(), 1);
        require_complete_baseline(&db, &rebuilt, &g);
        if !reopen {
            db.query("MATCH (n:Memory) SET n.value=2").unwrap();
            let mut s = db
                .begin_projection_full_rebuild(&rebuilt, rebuild_config(), mapped, permit(&g))
                .unwrap();
            drain(&mut s);
            assert!(matches!(
                db.publish_projection_full_rebuild(
                    s.finish().unwrap(),
                    ProjectionSourceDurability::RequireKnownDurable
                ),
                Err(ProjectionPublicationError::SourceNotDurable)
            ));
            released(&g);
        }
    }
}

struct OwnedStore {
    db: Database,
    reader: Option<SearchOutOfCoreReader>,
}
impl DatabasePublicationState for OwnedStore {
    fn database(&self) -> &Database {
        &self.db
    }
}
#[test]
fn full_rebuild_leaves_the_owner_and_old_reader_before_worker_preparation() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    for write in [false, true] {
        let (root, db, reader) = setup(false, "extra");
        let old_generation = reader.generation();
        let owner = Arc::new(DatabasePublicationOwner::new(OwnedStore {
            db,
            reader: Some(reader),
        }));
        let g = governor();
        let mut session = owner
            .with_read(|state| {
                Ok(state.db.begin_projection_full_rebuild(
                    state.reader.as_ref().unwrap(),
                    rebuild_config(),
                    mapped,
                    permit(&g),
                ))
            })
            .unwrap()
            .unwrap();
        let c = session.source_epoch();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        // Moving both the session and its result across workers also verifies
        // that the owned API does not retain a thread-bound owner/reader borrow.
        let preparation = std::thread::spawn(move || {
            ready_tx.send(session.source_epoch()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            drain(&mut session);
            session.finish()
        });
        assert_eq!(ready_rx.recv_timeout(Duration::from_secs(30)).unwrap(), c);
        let (committed_tx, committed_rx) = mpsc::sync_channel(1);
        let foreground_owner = Arc::clone(&owner);
        let foreground = std::thread::spawn(move || {
            let epoch = foreground_owner.with_write(|state| {
                state.reader.take();
                if write {
                    state.db.query("CREATE (:Other {id:'concurrent'})")?;
                }
                state.db.commit_epoch()
            });
            committed_tx.send(epoch).unwrap();
        });
        let committed = committed_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap()
            .unwrap();
        foreground.join().unwrap();
        assert_eq!(committed > c, write);
        assert_eq!(g.snapshot().active_background_tasks, 1);
        resume_tx.send(()).unwrap();
        let candidate = preparation.join().unwrap().unwrap();
        let result = owner
            .with_write(|state| {
                Ok(state.db.publish_projection_full_rebuild(
                    candidate,
                    ProjectionSourceDurability::RequireKnownDurable,
                ))
            })
            .unwrap();
        let current = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
        if write {
            assert!(matches!(
                result,
                Err(ProjectionPublicationError::Dependency(_))
            ));
            assert_eq!(current.generation(), old_generation);
            assert_eq!(current.document_count(), 4);
        } else {
            assert_eq!(result.unwrap().document_count, 3);
            assert_eq!(current.document_count(), 3);
            let mut check = owner
                .with_read(|state| {
                    Ok(state.db.begin_projection_baseline_comparison(
                        &current,
                        config(),
                        mapped,
                        permit(&g),
                    ))
                })
                .unwrap()
                .unwrap();
            complete(&mut check);
            assert_eq!(check.finish().unwrap().report().compared_documents, 3);
        }
        released(&g);
    }
}
