// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Additive schema commits and non-retiring descriptor states change no record
//! or read result, so the search change log must stay continuous across them:
//! incremental catch-up through each one must equal a full graph rebuild.
//! Retiring transitions (`DELETE_ONLY`, `GC`) keep the conservative gap.

use super::*;
use crate::StorageResidencyMode;

const DOCUMENTS: [&str; 3] = ["memory:m1", "memory:m2", "memory:m3"];
const READ: &str = "MATCH (m:Memory) RETURN m.id AS id, m.title AS title, \
                    m.score AS score ORDER BY id";

fn seed(db: &mut Database) {
    db.query("CREATE (:Memory {id: 'm1', title: 'First', content: 'alpha body'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm2', title: 'Second', content: 'beta body'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l1', name: 'Topic', canonical_name: 'topic'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
}

/// Catches `index` up incrementally and compares it with a full rebuild.
fn assert_catch_up_equals_rebuild(db: &Database, index: &mut SearchIndex, step: &str) {
    let request = db
        .build_search_projection_graph_delta_request_from_freshness(index, None)
        .unwrap_or_else(|error| panic!("{step}: incremental catch-up refused: {error}"));
    if let Some(request) = request {
        db.apply_search_projection_graph_delta(index, request)
            .unwrap();
    }
    let epoch = db.commit_epoch().unwrap();
    assert_eq!(
        index.projection_freshness().source_graph_commit_epoch,
        Some(epoch),
        "{step}: projection cursor did not reach the graph"
    );
    let mut rebuilt = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut rebuilt, SearchRebuildOptions::default())
        .unwrap();
    assert_eq!(index.document_count(), rebuilt.document_count(), "{step}");
    for id in DOCUMENTS {
        assert_eq!(index.document(id), rebuilt.document(id), "{step}: {id}");
    }
}

fn read_rows(db: &mut Database) -> String {
    format!("{:?}", db.query(READ).unwrap().rows)
}

/// Applies `statement`, asserts that reads are unchanged by it, then writes a
/// record change so later catch-up must cross the schema commit.
fn step(db: &mut Database, index: &mut SearchIndex, statement: &str, write: usize) {
    let before = read_rows(db);
    let epoch = db.commit_epoch().unwrap();
    db.query(statement).unwrap();
    assert!(db.commit_epoch().unwrap() > epoch, "{statement}: no commit");
    assert_eq!(read_rows(db), before, "{statement}: changed read results");
    assert_catch_up_equals_rebuild(db, index, statement);
    db.query(&format!(
        "MATCH (m:Memory {{id: 'm2'}}) SET m.title = 'Second {write}'"
    ))
    .unwrap();
    assert_catch_up_equals_rebuild(db, index, &format!("write after {statement}"));
}

const TABLE_STATES: [&str; 4] = ["WRITE_ONLY", "BACKFILL", "VALIDATING", "PUBLIC"];

fn additive_schema_sequence(db: &mut Database, index: &mut SearchIndex) {
    let mut write = 0;
    let mut run = |db: &mut Database, index: &mut SearchIndex, statement: String| {
        write += 1;
        step(db, index, &statement, write);
    };
    run(db, index, "CREATE NODE TABLE Memory".into());
    run(db, index, "CREATE RELATIONSHIP TABLE HAS_LABEL".into());
    run(
        db,
        index,
        "CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING".into(),
    );
    for state in TABLE_STATES {
        run(
            db,
            index,
            format!("ALTER NODE TABLE Memory SET STATE {state}"),
        );
        run(
            db,
            index,
            format!("ALTER RELATIONSHIP TABLE HAS_LABEL SET STATE {state}"),
        );
        run(
            db,
            index,
            format!("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE {state}"),
        );
    }
    run(db, index, "CREATE INDEX ON :Memory(title)".into());
    for statement in [
        "CREATE FULLTEXT INDEX ON :Memory(content)",
        "CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE",
        "CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS",
        "CREATE CONSTRAINT ON -[:HAS_LABEL(weight)]-> ASSERT UNIQUE",
    ] {
        run(db, index, statement.into());
    }
}

#[test]
fn additive_schema_and_state_transitions_keep_incremental_equal_to_rebuild() {
    let mut db = Database::new();
    seed(&mut db);
    let mut index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    additive_schema_sequence(&mut db, &mut index);
    assert_eq!(
        db.search_projection_changefeed_status()
            .unwrap()
            .resume_floor_commit_epoch,
        0
    );
}

#[test]
fn defaulted_property_and_backfill_maintenance_are_captured_as_record_changes() {
    let mut db = Database::new();
    seed(&mut db);
    let mut index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    // The declaration itself changes no record; existing rows read NULL.
    step(
        &mut db,
        &mut index,
        "CREATE PROPERTY ON NODE TABLE Memory(score) TYPE ANY DEFAULT 0.0",
        1,
    );
    // Each maintenance pass commits fills plus state advances. Fills are
    // point writes and must reach the projection as record changes.
    let before = db.commit_epoch().unwrap();
    for _ in 0..8 {
        db.run_schema_maintenance().unwrap();
    }
    assert!(db.commit_epoch().unwrap() > before);
    let batch = db
        .build_search_projection_change_batch_after(before, None)
        .unwrap()
        .unwrap();
    assert!(
        batch.operation_count() >= 2,
        "backfill fills were not captured"
    );
    let missing = db
        .query("MATCH (m:Memory) WHERE m.score IS NULL OR m.score <> 0.0 RETURN count(*) AS c")
        .unwrap();
    assert_eq!(missing.rows[0].get("c"), Some(&Value::Int(0)));
    assert_catch_up_equals_rebuild(&db, &mut index, "backfill");
    // A record created under the default receives it at write time.
    db.query("CREATE (:Memory {id: 'm3', title: 'Third', content: 'gamma'})")
        .unwrap();
    assert_catch_up_equals_rebuild(&db, &mut index, "create under default");
    assert_eq!(
        db.search_projection_changefeed_status()
            .unwrap()
            .resume_floor_commit_epoch,
        0
    );
}

#[test]
fn retiring_descriptor_states_keep_the_rebuild_gap() {
    for statement in [
        "ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE DELETE_ONLY",
        "ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE GC",
        "ALTER NODE TABLE Memory SET STATE DELETE_ONLY",
        "ALTER NODE TABLE Memory SET STATE GC",
    ] {
        let mut db = Database::new();
        seed(&mut db);
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING")
            .unwrap();
        let mut index = SearchIndex::in_memory();
        assert_catch_up_equals_rebuild(&db, &mut index, "initial");
        db.query(statement).unwrap();
        let error = db
            .build_search_projection_graph_delta_request_from_freshness(&index, None)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("full search projection rebuild required"),
            "{statement}: {error}"
        );
        let mut index = SearchIndex::in_memory();
        db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
            .unwrap();
        db.query("MATCH (m:Memory {id: 'm2'}) SET m.title = 'After gap'")
            .unwrap();
        assert_catch_up_equals_rebuild(&db, &mut index, statement);
    }
}

#[test]
fn schema_change_log_continuity_survives_checkpoint_and_reopen() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for checkpoint in [false, true] {
            let root = unique_test_dir("schema_change_log_continuity");
            let config = DatabaseConfig {
                storage_residency_mode: residency,
                ..Default::default()
            };
            let mut index = SearchIndex::in_memory();
            {
                let mut db = Database::open_with_config(&root, config.clone()).unwrap();
                seed(&mut db);
                db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
                    .unwrap();
                db.query("CREATE PROPERTY ON NODE TABLE Memory(score) TYPE ANY DEFAULT 0.0")
                    .unwrap();
                db.query("CREATE NODE TABLE Entity").unwrap();
                db.query("CREATE INDEX ON :Memory(title)").unwrap();
                if checkpoint {
                    db.checkpoint().unwrap();
                }
            }
            let mut db = Database::open_with_config(&root, config).unwrap();
            assert_catch_up_equals_rebuild(&db, &mut index, "reopen");
            for _ in 0..8 {
                db.run_schema_maintenance().unwrap();
            }
            assert_catch_up_equals_rebuild(&db, &mut index, "maintenance after reopen");
            drop(db);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
