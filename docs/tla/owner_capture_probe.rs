// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

// Native observation of legacy feed limits; not a new owner-capture contract.
use hawdb::{Database, PreparedReadLimits, ProjectionFeedItem, ProjectionFeedLimits};
use std::num::NonZeroUsize;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn reads() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: nz(32),
        dependency_bytes: nz(4096),
        statement_rows: nz(32),
        statement_payload_bytes: nz(4096),
    }
}
fn feed() -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: nz(1000),
        items: nz(100),
        bytes: nz(8192),
    }
}
fn main() {
    for label in ["Thread", "Memory"] {
        let mut db = Database::new();
        db.query(&format!("CREATE (:{label} {{id:'owner'}})"))
            .unwrap();
        let held_c = db.begin_prepared_read(reads()).unwrap();
        let c = held_c.source_epoch();
        db.query(&format!("MATCH (n:{label} {{id:'owner'}}) DELETE n"))
            .unwrap();
        assert!(db
            .query(&format!("MATCH (n:{label}) RETURN n.id AS id"))
            .unwrap()
            .rows
            .is_empty());
        let mut changes = db
            .begin_prepared_projection_feed(c, reads(), feed())
            .unwrap();
        assert!(changes.source_epoch() > c);
        let (mut graph_owners, mut doc_deletes, mut ends) = (0, 0, 0);
        loop {
            let step = changes
                .visit_next(feed(), |_, item| {
                    match item {
                        ProjectionFeedItem::GraphNode { .. } => graph_owners += 1,
                        ProjectionFeedItem::DeleteDocument { .. } => doc_deletes += 1,
                        ProjectionFeedItem::End { .. } => ends += 1,
                        ProjectionFeedItem::RelationalKey { .. } => {
                            panic!("unexpected relational key")
                        }
                    }
                    Ok(())
                })
                .unwrap();
            if step.complete {
                break;
            }
        }
        changes.finish().unwrap();
        assert_eq!(graph_owners, 0);
        assert_eq!(doc_deletes, usize::from(label == "Memory"));
        assert_eq!(ends, 1);
        println!(
            "label={label} C={c} deleted_at_E=true physical_owner_events={graph_owners} old_document_deletes={doc_deletes} Ends={ends} held_C=true"
        );
    }
    retention_probe();
}

fn retention_probe() {
    let mut db = Database::new_with_config(hawdb::DatabaseConfig {
        max_search_projection_change_log_entries: Some(1),
        ..Default::default()
    });
    db.query("CREATE (:Memory {id:'owner', value:0})").unwrap();
    let mut c_source = db
        .begin_prepared_full_projection_source(
            reads(),
            feed(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
    let c = c_source.source_epoch();
    for value in 1..=3 {
        db.query(&format!(
            "MATCH (n:Memory {{id:'owner'}}) SET n.value={value}"
        ))
        .unwrap();
    }
    let floor = match db.begin_prepared_projection_feed(c, reads(), feed()) {
        Err(hawdb::ProjectionFeedError::BehindFloor { requested, floor }) => {
            assert_eq!(requested, c);
            assert!(floor > c);
            floor
        }
        other => panic!("expected expired capture interval, got {other:?}"),
    };
    let mut pinned_values = 0;
    loop {
        let step = c_source
            .visit_next(feed(), |read, item| {
                if let hawdb::ProjectionFullSourceItem::GraphNodeCandidate { node_id } = item {
                    assert_eq!(node_id, 0);
                    let output = read.query_with_params(
                        "MATCH (n:Memory) RETURN n.value AS value",
                        &Default::default(),
                    )?;
                    assert_eq!(output.output.rows.len(), 1);
                    assert_eq!(output.output.rows[0]["value"], hawdb::Value::Int(0));
                    pinned_values += 1;
                }
                Ok(())
            })
            .unwrap();
        if step.complete {
            break;
        }
    }
    c_source.finish().unwrap();
    assert_eq!(pinned_values, 1);
    println!(
        "capture_retention C={c} floor={floor} entry_limit=1 commits=3 held_full_C=true pinned_value=0 interval=BehindFloor"
    );
}
