use super::*;
#[cfg(feature = "acl")]
use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

#[test]
fn explains_query_with_optimizer_trace() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    for id in 2..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Extra {id}'}})"
        ))
        .unwrap();
    }
    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert!(output.trace.groups >= 3);
    assert!(output
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(!output.trace.selected_plan.contains("FilterExec"));
    assert_eq!(
        output.trace.selected_plan_fingerprint,
        output.physical_plan.fingerprint()
    );
    assert_eq!(
        output.work_request,
        WorkRequest::foreground(WorkClass::Query, 1)
    );
    assert!(output
        .trace
        .selected_plan_fingerprint
        .contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));
}

#[test]
fn explain_query_reports_effective_resource_hints() {
    let db = Database::new();
    let output = db
        .explain_query(
            "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
             system.estimated_operations = 64 MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap();

    assert_eq!(
        output.work_request,
        WorkRequest::background(WorkClass::Analytics, 64)
    );
    assert!(output
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
}

#[test]
fn explain_analyze_reports_storage_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    db.query("CREATE (:Memory {id: 'mem-analyze-1', kind: 'note', title: 'Analyze'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-analyze-2', kind: 'note', title: 'Profile'})")
        .unwrap();

    let output = db
        .explain_analyze_query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.title AS title")
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert!(output
        .physical_plan
        .explain(0)
        .contains("NodeProjectionScanExec"));
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_filter, 2);
    assert_eq!(scan.output_count, 2);
}

#[test]
fn explain_analyze_reports_durable_source_segment_pruning() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-source-analyze-{nonce}"));
    let mut db = Database::open(&path).unwrap();
    for id in 0..128 {
        db.query(&format!(
            "CREATE (:Source {{id: 'source-beta-{id}', archive_group: 'beta'}})"
        ))
        .unwrap();
    }
    db.query("CREATE (:Source {id: 'source-missing'})").unwrap();
    db.checkpoint().unwrap();

    let output = db
        .explain_analyze_query("MATCH (s:Source) WHERE s.archive_group IS NULL RETURN s.id AS id")
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    let physical_plan = output.physical_plan.explain(0);
    assert!(
        physical_plan.contains("SourceSegmentScan"),
        "expected SourceSegmentScan, got:\n{physical_plan}"
    );
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 129);
    assert_eq!(scan.candidate_count_before_filter, 1);
    assert_eq!(scan.pruned_candidate_count, 128);
    assert_eq!(scan.output_count, 1);
    assert_eq!(scan.strategy, crate::store::ScanPruningStrategy::OrUnion);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn explain_analyze_reports_property_exists_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(confidence)").unwrap();
    db.query("CREATE (:Memory {id: 'mem-confidence-1', confidence: 0.9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-confidence-2'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-confidence-3', confidence: null})")
        .unwrap();

    let output = db
        .explain_analyze_query("MATCH (m:Memory) WHERE m.confidence IS NOT NULL RETURN m.id AS id")
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 3);
    assert_eq!(scan.candidate_count_before_filter, 1);
    assert_eq!(scan.pruned_candidate_count, 2);
    assert_eq!(scan.output_count, 1);
    assert_eq!(
        scan.strategy,
        crate::store::ScanPruningStrategy::PropertyExists {
            property: "confidence".to_string()
        }
    );
}

#[test]
fn explain_analyze_reports_property_missing_or_null_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(latest_at)").unwrap();
    db.query("CREATE (:Memory {id: 'mem-latest-1', latest_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-latest-2'})").unwrap();
    db.query("CREATE (:Memory {id: 'mem-latest-3', latest_at: null})")
        .unwrap();

    let output = db
        .explain_analyze_query("MATCH (m:Memory) WHERE m.latest_at IS NULL RETURN m.id AS id")
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 3);
    assert_eq!(scan.candidate_count_before_filter, 2);
    assert_eq!(scan.pruned_candidate_count, 1);
    assert_eq!(scan.output_count, 2);
    assert_eq!(
        scan.strategy,
        crate::store::ScanPruningStrategy::PropertyMissingOrNull {
            property: "latest_at".to_string()
        }
    );
}

#[test]
fn explain_analyze_reports_default_if_null_eq_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Thread(space_id)").unwrap();
    db.query("CREATE (:Thread {id: 'thread-missing', thread_id: 'logical-1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-empty', thread_id: 'logical-2', space_id: ''})")
        .unwrap();
    db.query(
        "CREATE (:Thread {id: 'thread-default', thread_id: 'logical-3', space_id: 'default'})",
    )
    .unwrap();
    db.query("CREATE (:Thread {id: 'thread-team', thread_id: 'logical-4', space_id: 'team'})")
        .unwrap();

    let output = db
        .explain_analyze_query(
            "MATCH (t:Thread) \
             WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' \
             THEN 'default' ELSE t.space_id END = 'default' \
             RETURN t.id AS id",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 3);
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 4);
    assert_eq!(scan.candidate_count_before_filter, 3);
    assert_eq!(scan.pruned_candidate_count, 1);
    assert_eq!(scan.output_count, 3);
    assert_eq!(
        scan.strategy,
        crate::store::ScanPruningStrategy::PropertyDefaultIfNullEq {
            property: "space_id".to_string()
        }
    );
}

#[test]
fn explain_analyze_reports_parameterized_default_if_null_eq_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Thread(space_id)").unwrap();
    db.query("CREATE (:Thread {id: 'thread-missing', thread_id: 'logical-1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-empty', thread_id: 'logical-2', space_id: ''})")
        .unwrap();
    db.query(
        "CREATE (:Thread {id: 'thread-default', thread_id: 'logical-3', space_id: 'default'})",
    )
    .unwrap();
    db.query("CREATE (:Thread {id: 'thread-team', thread_id: 'logical-4', space_id: 'team'})")
        .unwrap();

    let output = db
        .explain_analyze_query_with_params(
            "MATCH (t:Thread) \
             WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' \
             THEN 'default' ELSE t.space_id END = $source_space_id \
             RETURN t.id AS id",
            &BTreeMap::from([(
                "source_space_id".to_string(),
                Value::String("default".to_string()),
            )]),
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 3);
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 4);
    assert_eq!(scan.candidate_count_before_filter, 3);
    assert_eq!(scan.pruned_candidate_count, 1);
    assert_eq!(scan.output_count, 3);
    assert_eq!(
        scan.strategy,
        crate::store::ScanPruningStrategy::PropertyDefaultIfNullEq {
            property: "space_id".to_string()
        }
    );
}

#[test]
fn explain_analyze_reports_parameterized_default_if_null_not_eq_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Thread(space_id)").unwrap();
    db.query("CREATE (:Thread {id: 'thread-missing', thread_id: 'logical-1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-empty', thread_id: 'logical-2', space_id: ''})")
        .unwrap();
    db.query(
        "CREATE (:Thread {id: 'thread-default', thread_id: 'logical-3', space_id: 'default'})",
    )
    .unwrap();
    db.query("CREATE (:Thread {id: 'thread-team', thread_id: 'logical-4', space_id: 'team'})")
        .unwrap();

    let output = db
        .explain_analyze_query_with_params(
            "MATCH (t:Thread) \
             WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' \
             THEN 'default' ELSE t.space_id END <> $target_space_id \
             RETURN t.id AS id",
            &BTreeMap::from([(
                "target_space_id".to_string(),
                Value::String("default".to_string()),
            )]),
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 1);
    let scan = &output.execution_profile.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 4);
    assert_eq!(scan.candidate_count_before_filter, 1);
    assert_eq!(scan.pruned_candidate_count, 3);
    assert_eq!(scan.output_count, 1);
    assert_eq!(
        scan.strategy,
        crate::store::ScanPruningStrategy::PropertyDefaultIfNullNotEq {
            property: "space_id".to_string()
        }
    );
}

#[test]
fn explain_analyze_reports_relationship_property_scan_pruning_profile() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    db.query("CREATE (:Entity {id: 'active'})").unwrap();
    db.query("CREATE (:Entity {id: 'deleted'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'source'}), (e:Entity {id: 'active'}) \
         CREATE (m)-[:RELATES_TO {lifecycle_state: 'active'}]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'source'}), (e:Entity {id: 'deleted'}) \
         CREATE (m)-[:RELATES_TO {lifecycle_state: 'deleted'}]->(e)",
    )
    .unwrap();

    let output = db
        .explain_analyze_query(
            "MATCH (m:Memory {id: 'source'})-[r:RELATES_TO {lifecycle_state: 'active'}]->(e:Entity) \
             RETURN e.id AS id",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("id"),
        Some(&Value::String("active".to_string()))
    );
    assert_eq!(output.execution_profile.scan_pruning_reports.len(), 2);
    let relationship_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| {
            scan.strategy
                == crate::store::ScanPruningStrategy::PropertyEq {
                    property: "lifecycle_state".to_string(),
                }
        })
        .expect("relationship property pruning report");
    assert!(relationship_scan.pruned);
    assert_eq!(
        relationship_scan.target_kind,
        crate::store::ScanPruningTargetKind::Relationship
    );
    assert_eq!(relationship_scan.label_id, None);
    assert!(relationship_scan.rel_type_id.is_some());
    assert_eq!(relationship_scan.candidate_count_before_pruning, 2);
    assert_eq!(relationship_scan.candidate_count_before_filter, 1);
    assert_eq!(relationship_scan.pruned_candidate_count, 1);
    assert_eq!(relationship_scan.output_count, 1);
}

#[test]
fn explain_analyze_reports_out_of_core_relationship_projection_pruning() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "skein-relationship-projection-profile-{}-{nonce}",
        std::process::id(),
    ));
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    for (id, rank) in [("old", 10), ("selected", 20), ("new", 30)] {
        db.query(&format!("CREATE (:Entity {{id: '{id}'}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (m:Memory {{id: 'source'}}), (e:Entity {{id: '{id}'}}) \
             CREATE (m)-[:LINKS_TO {{rank: {rank}}}]->(e)"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();

    let output = db
        .explain_analyze_query(
            "MATCH (m:Memory {id: 'source'})-[r:LINKS_TO {rank: 20}]->(e:Entity) \
             RETURN e.id AS id",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("id"),
        Some(&Value::String("selected".to_string()))
    );
    let relationship_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| {
            matches!(
                scan.strategy,
                crate::store::ScanPruningStrategy::PropertyEq { ref property }
                    if property == "rank"
            )
        })
        .expect("out-of-core relationship projection pruning report");
    assert!(relationship_scan.pruned);
    assert_eq!(relationship_scan.candidate_count_before_pruning, 3);
    assert_eq!(relationship_scan.output_count, 1);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn explain_analyze_pushes_relationship_where_predicate_to_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    for (id, created_at) in [("old", 7), ("newer", 9), ("newest", 10)] {
        db.query(&format!("CREATE (:Entity {{id: '{id}'}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (m:Memory {{id: 'source'}}), (e:Entity {{id: '{id}'}}) \
             CREATE (m)-[:MENTIONS {{created_at: {created_at}}}]->(e)"
        ))
        .unwrap();
    }

    let output = db
        .explain_analyze_query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) \
             WHERE r.created_at > 8 \
             RETURN e.id AS id ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert_eq!(
        output
            .output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("newer".to_string()),
            Value::String("newest".to_string()),
        ]
    );
    let relationship_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| {
            scan.strategy
                == crate::store::ScanPruningStrategy::PropertyRange {
                    property: "created_at".to_string(),
                }
        })
        .expect("relationship where predicate scan pruning report");
    assert!(relationship_scan.pruned);
    assert_eq!(
        relationship_scan.target_kind,
        crate::store::ScanPruningTargetKind::Relationship
    );
    assert_eq!(relationship_scan.label_id, None);
    assert!(relationship_scan.rel_type_id.is_some());
    assert_eq!(relationship_scan.candidate_count_before_pruning, 3);
    assert_eq!(relationship_scan.candidate_count_before_filter, 2);
    assert_eq!(relationship_scan.pruned_candidate_count, 1);
    assert_eq!(relationship_scan.output_count, 2);
}

#[test]
fn explain_analyze_pushes_relationship_conjunct_from_mixed_where_to_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    db.query("CREATE (:Memory {id: 'other'})").unwrap();
    for (source, id, created_at) in [
        ("source", "old", 7),
        ("source", "newer", 9),
        ("source", "newest", 10),
        ("other", "other_new", 11),
    ] {
        db.query(&format!("CREATE (:Entity {{id: '{id}'}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (m:Memory {{id: '{source}'}}), (e:Entity {{id: '{id}'}}) \
             CREATE (m)-[:MENTIONS {{created_at: {created_at}}}]->(e)"
        ))
        .unwrap();
    }

    let output = db
        .explain_analyze_query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) \
             WHERE m.id = 'source' AND r.created_at > 8 \
             RETURN e.id AS id ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert_eq!(
        output
            .output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("newer".to_string()),
            Value::String("newest".to_string()),
        ]
    );
    let relationship_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| {
            scan.strategy
                == crate::store::ScanPruningStrategy::PropertyRange {
                    property: "created_at".to_string(),
                }
        })
        .expect("mixed where relationship scan pruning report");
    assert!(relationship_scan.pruned);
    assert_eq!(relationship_scan.label_id, None);
    assert_eq!(relationship_scan.candidate_count_before_pruning, 4);
    assert_eq!(relationship_scan.candidate_count_before_filter, 3);
    assert_eq!(relationship_scan.pruned_candidate_count, 1);
    assert_eq!(relationship_scan.output_count, 3);
}

#[test]
fn explain_analyze_pushes_nowledge_status_timestamp_relationship_filter_to_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    for (id, status, created_at, updated_at) in [
        ("stale", "active", 1, 1),
        ("created", "active", 10, 1),
        ("updated", "active", 1, 11),
        ("deleted", "deleted", 12, 12),
        ("archived", "active", 2, 2),
    ] {
        db.query(&format!("CREATE (:Memory {{id: '{id}'}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (source:Memory {{id: 'source'}}), (target:Memory {{id: '{id}'}}) \
             CREATE (source)-[:MEMORY_RELATES_TO {{status: '{status}', created_at: {created_at}, updated_at: {updated_at}}}]->(target)"
        ))
        .unwrap();
    }

    let output = db
        .explain_analyze_query(
            "MATCH (source:Memory)-[r:MEMORY_RELATES_TO]->(target:Memory) \
             WHERE r.status = 'active' AND (r.created_at > 8 OR r.updated_at > 8) \
             RETURN target.id AS id ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert_eq!(
        output
            .output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("created".to_string()),
            Value::String("updated".to_string()),
        ]
    );
    let relationship_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| scan.strategy == crate::store::ScanPruningStrategy::OrUnion)
        .expect("status plus timestamp relationship scan pruning report");
    assert!(relationship_scan.pruned);
    assert_eq!(relationship_scan.label_id, None);
    assert_eq!(relationship_scan.candidate_count_before_pruning, 5);
    assert_eq!(relationship_scan.candidate_count_before_filter, 3);
    assert_eq!(relationship_scan.pruned_candidate_count, 2);
    assert_eq!(relationship_scan.output_count, 2);
}

#[test]
fn explain_analyze_pushes_nowledge_timestamp_node_delta_filter_to_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(created_at)").unwrap();
    db.query("CREATE INDEX ON :Memory(updated_at)").unwrap();
    for (id, created_at, updated_at) in [
        ("stale", 1, 1),
        ("created", 10, 1),
        ("updated", 1, 11),
        ("both", 12, 13),
    ] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', created_at: {created_at}, updated_at: {updated_at}}})"
        ))
        .unwrap();
    }

    let output = db
        .explain_analyze_query(
            "MATCH (m:Memory) \
             WHERE m.created_at > 8 OR m.updated_at > 8 \
             RETURN m.id AS id ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 3);
    assert_eq!(
        output
            .output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("both".to_string()),
            Value::String("created".to_string()),
            Value::String("updated".to_string()),
        ]
    );
    let node_scan = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|scan| scan.strategy == crate::store::ScanPruningStrategy::OrUnion)
        .expect("timestamp node delta scan pruning report");
    assert!(node_scan.pruned);
    assert_eq!(
        node_scan.target_kind,
        crate::store::ScanPruningTargetKind::Node
    );
    assert!(node_scan.label_id.is_some());
    assert_eq!(node_scan.rel_type_id, None);
    assert_eq!(node_scan.candidate_count_before_pruning, 4);
    assert_eq!(node_scan.candidate_count_before_filter, 3);
    assert_eq!(node_scan.pruned_candidate_count, 1);
    assert_eq!(node_scan.output_count, 3);
}

#[test]
fn cypher_explain_returns_structured_plan_row() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Explain row'})")
        .unwrap();
    for id in 2..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Explain extra {id}'}})"
        ))
        .unwrap();
    }

    let output = db
        .query("EXPLAIN MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let row = &output.rows[0];
    assert_eq!(row.get("mode"), Some(&Value::String("explain".to_string())));
    assert_eq!(
        row.get("statement_kind"),
        Some(&Value::String("match_return".to_string()))
    );
    assert!(
        matches!(row.get("plan"), Some(Value::String(plan)) if plan.contains("NodeProjectionScanExec"))
    );
    assert!(matches!(
        row.get("selected_plan_fingerprint"),
        Some(Value::String(fingerprint)) if fingerprint.contains("IndexNodeSeek")
    ));
    assert!(matches!(
        row.get("query_digest"),
        Some(Value::String(digest)) if digest.starts_with("q1:")
    ));
    let Some(Value::Map(selected_plan_cost)) = row.get("selected_plan_cost") else {
        panic!("expected selected plan cost map");
    };
    assert!(matches!(
        selected_plan_cost.get("cost"),
        Some(Value::Int(cost)) if *cost > 0
    ));
    let Some(Value::Map(selected_plan_cost_breakdown)) = row.get("selected_plan_cost_breakdown")
    else {
        panic!("expected selected plan cost breakdown map");
    };
    assert_eq!(
        selected_plan_cost_breakdown.get("estimated_rows"),
        selected_plan_cost.get("estimated_rows")
    );
    assert_eq!(
        selected_plan_cost_breakdown.get("cost"),
        selected_plan_cost.get("cost")
    );
    assert!(selected_plan_cost_breakdown.contains_key("cpu"));
    assert!(selected_plan_cost_breakdown.contains_key("random_io"));
    assert!(selected_plan_cost_breakdown.contains_key("sequential_io"));
    assert!(selected_plan_cost_breakdown.contains_key("output_rows"));
    let Some(Value::List(operator_cardinalities)) = row.get("operator_cardinalities") else {
        panic!("expected operator cardinalities");
    };
    assert!(!operator_cardinalities.is_empty());
    assert!(operator_cardinalities.iter().enumerate().all(|(ordinal, value)| {
        matches!(
            value,
            Value::Map(cardinality)
                if cardinality.get("operator_id") == Some(&Value::Int(ordinal as i64))
                    && matches!(cardinality.get("estimated_rows"), Some(Value::Int(rows)) if *rows > 0)
                    && cardinality.get("actual_rows") == Some(&Value::Null)
        )
    }));
    let Some(Value::Map(selected_plan_properties)) = row.get("selected_plan_properties") else {
        panic!("expected selected plan properties map");
    };
    assert_eq!(
        selected_plan_properties.get("scan_pruning"),
        Some(&Value::String("index".to_string()))
    );
    assert_eq!(
        selected_plan_properties.get("vector_precision"),
        Some(&Value::String("not_vector".to_string()))
    );
    let Some(Value::List(covering_fields)) = selected_plan_properties.get("covering_fields") else {
        panic!("expected covering fields list");
    };
    assert!(covering_fields.contains(&Value::String("Memory.id".to_string())));
    let Some(Value::List(optimizer_stages)) = row.get("optimizer_stages") else {
        panic!("expected optimizer stages list");
    };
    assert!(optimizer_stages.iter().any(|stage| {
        matches!(
            stage,
            Value::Map(stage)
                if stage.get("name")
                    == Some(&Value::String("logical_grouping".to_string()))
        )
    }));
    assert!(optimizer_stages.iter().any(|stage| {
        matches!(
            stage,
            Value::Map(stage)
                if stage.get("name")
                    == Some(&Value::String("physical_search".to_string()))
        )
    }));
    assert!(row.contains_key("work_request"));
    let Some(Value::Map(semantic_checks)) = row.get("semantic_checks") else {
        panic!("expected semantic checks map");
    };
    assert_eq!(
        semantic_checks.get("parse"),
        Some(&Value::String("passed".to_string()))
    );
    assert_eq!(
        semantic_checks.get("semantic_validation"),
        Some(&Value::String("passed".to_string()))
    );
    let Some(Value::Map(fast_path)) = row.get("fast_path") else {
        panic!("expected fast path map");
    };
    assert_eq!(fast_path.get("selected"), Some(&Value::Bool(true)));
    assert_eq!(
        fast_path.get("reason"),
        Some(&Value::String(
            "index_node_seek_without_residual_filter".to_string()
        ))
    );
    let Some(Value::Map(optimizer_budget)) = row.get("optimizer_budget") else {
        panic!("expected optimizer budget map");
    };
    assert_eq!(optimizer_budget.get("max_groups"), Some(&Value::Int(128)));
    assert_eq!(
        optimizer_budget.get("budget_exceeded"),
        Some(&Value::Bool(false))
    );
    let Some(Value::List(chosen_indexes)) = row.get("chosen_indexes") else {
        panic!("expected chosen indexes list");
    };
    assert!(chosen_indexes.iter().any(|index| {
        matches!(
            index,
            Value::Map(index)
                if index.get("operator")
                    == Some(&Value::String("IndexNodeSeek".to_string()))
        )
    }));
    assert_eq!(
        row.get("resource_class"),
        Some(&Value::String("query".to_string()))
    );
}

#[test]
fn cypher_explain_analyze_returns_execution_profile_row() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    db.query("CREATE (:Memory {id: 'mem-cypher-analyze-1', kind: 'note', title: 'Analyze'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-cypher-analyze-2', kind: 'note', title: 'Profile'})")
        .unwrap();

    let output = db
        .query(
            "EXPLAIN ANALYZE MATCH (m:Memory) \
             WHERE m.kind = 'note' RETURN m.title AS title",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let row = &output.rows[0];
    assert_eq!(
        row.get("mode"),
        Some(&Value::String("explain_analyze".to_string()))
    );
    let Some(Value::Map(selected_plan_cost)) = row.get("selected_plan_cost") else {
        panic!("expected selected plan cost map");
    };
    assert!(matches!(
        selected_plan_cost.get("cost"),
        Some(Value::Int(cost)) if *cost > 0
    ));
    let Some(Value::Map(selected_plan_cost_breakdown)) = row.get("selected_plan_cost_breakdown")
    else {
        panic!("expected selected plan cost breakdown map");
    };
    assert_eq!(
        selected_plan_cost_breakdown.get("cost"),
        selected_plan_cost.get("cost")
    );
    let Some(Value::List(optimizer_stages)) = row.get("optimizer_stages") else {
        panic!("expected optimizer stages list");
    };
    assert!(optimizer_stages.iter().any(|stage| {
        matches!(
            stage,
            Value::Map(stage)
                if stage.get("name")
                    == Some(&Value::String("selected_plan_costing".to_string()))
        )
    }));
    assert_eq!(row.get("row_count"), Some(&Value::Int(2)));
    let Some(Value::List(operator_cardinalities)) = row.get("operator_cardinalities") else {
        panic!("expected operator cardinalities");
    };
    let Value::Map(root_cardinality) = &operator_cardinalities[0] else {
        panic!("expected root operator cardinality map");
    };
    assert_eq!(root_cardinality.get("operator_id"), Some(&Value::Int(0)));
    assert!(matches!(
        root_cardinality.get("estimated_rows"),
        Some(Value::Int(rows)) if *rows > 0
    ));
    assert_eq!(root_cardinality.get("actual_rows"), Some(&Value::Int(2)));
    assert_eq!(row.get("scan_pruning_report_count"), Some(&Value::Int(1)));
    let Some(Value::List(scan_reports)) = row.get("scan_pruning_reports") else {
        panic!("expected scan pruning reports");
    };
    assert_eq!(scan_reports.len(), 1);
    let Value::Map(scan_report) = &scan_reports[0] else {
        panic!("expected scan pruning report map");
    };
    assert_eq!(scan_report.get("pruned"), Some(&Value::Bool(true)));
    assert_eq!(
        scan_report.get("candidate_count_before_pruning"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        scan_report.get("pruned_candidate_count"),
        Some(&Value::Int(0))
    );
    assert_eq!(scan_report.get("output_count"), Some(&Value::Int(2)));
    let Some(Value::Map(strategy)) = scan_report.get("strategy") else {
        panic!("expected scan pruning strategy map");
    };
    assert_eq!(
        strategy.get("kind"),
        Some(&Value::String("property_eq".to_string()))
    );
    assert_eq!(
        strategy.get("property"),
        Some(&Value::String("kind".to_string()))
    );
    assert_eq!(
        row.get("operator_row_cap_enabled"),
        Some(&Value::Bool(true))
    );
}

#[test]
fn read_transaction_cypher_explain_analyze_uses_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem-read-explain-1', kind: 'note'})")
        .unwrap();
    let mut read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'mem-read-explain-2', kind: 'note'})")
        .unwrap();

    let output = read_tx
        .query(
            "EXPLAIN ANALYZE MATCH (m:Memory) \
             WHERE m.kind = 'note' RETURN m.id AS id",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("row_count"), Some(&Value::Int(1)));
    assert_eq!(
        output.rows[0].get("scan_pruning_report_count"),
        Some(&Value::Int(1))
    );
}

#[test]
fn cypher_explain_analyze_rejects_mutation() {
    let mut db = Database::new();

    let error = db
        .query("EXPLAIN ANALYZE CREATE (:Memory {id: 1})")
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("EXPLAIN ANALYZE only supports read queries"));
    let output = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn plan_cache_reuses_parameterized_physical_plan_template() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let mut parameters = BTreeMap::new();
    parameters.insert("id".to_string(), Value::Int(1));
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";

    let first = db.explain_query_with_params(query, &parameters).unwrap();
    let second = db.explain_query_with_params(query, &parameters).unwrap();

    assert_eq!(first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(second.plan_cache_lookup, PlanCacheLookup::Hit);
    assert!(first
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    assert!(second
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: parameterized physical plan template"));
    assert_eq!(
        first.trace.selected_plan_fingerprint,
        second.trace.selected_plan_fingerprint
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.admissions, 1);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 0);
    assert_eq!(stats.memory_pressure_events, 0);
}

#[test]
fn plan_cache_rebinds_equality_parameters_without_reoptimizing() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', title: 'Second'})")
        .unwrap();
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";

    let first = db
        .query_with_params(
            query,
            &BTreeMap::from([("id".to_string(), Value::String("first".to_string()))]),
        )
        .unwrap();
    let second = db
        .query_with_params(
            query,
            &BTreeMap::from([("id".to_string(), Value::String("second".to_string()))]),
        )
        .unwrap();

    assert_eq!(
        first.rows[0].get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        second.rows[0].get("title"),
        Some(&Value::String("Second".to_string()))
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.entries, 1);
}

#[test]
fn plan_cache_rebinds_in_list_parameters_with_the_same_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', title: 'Second'})")
        .unwrap();
    let query = "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title";

    let first = db
        .query_with_params(
            query,
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::String("first".to_string())]),
            )]),
        )
        .unwrap();
    let second = db
        .query_with_params(
            query,
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::String("second".to_string())]),
            )]),
        )
        .unwrap();

    assert_eq!(
        first.rows[0].get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        second.rows[0].get("title"),
        Some(&Value::String("Second".to_string()))
    );
    assert_eq!(db.plan_cache_stats().hits, 1);
}

#[test]
fn plan_cache_reuses_a_plan_after_data_commits_when_schema_and_statistics_generation_match() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";
    let first_parameters = BTreeMap::from([("id".to_string(), Value::String("first".to_string()))]);

    let first = db
        .explain_query_with_params(query, &first_parameters)
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', title: 'Second'})")
        .unwrap();
    let second = db
        .explain_query_with_params(
            query,
            &BTreeMap::from([("id".to_string(), Value::String("second".to_string()))]),
        )
        .unwrap();

    assert_eq!(first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(second.plan_cache_lookup, PlanCacheLookup::Hit);
}

#[test]
fn stale_out_of_core_snapshot_refresh_does_not_publish_a_new_statistics_generation() {
    let path = unique_test_dir("plan_cache_stale_statistics_generation");
    let config = DatabaseConfig {
        max_plan_cache_entries: Some(8),
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE (:Memory {id: 'first', title: 'First'})")
            .unwrap();
        db.checkpoint().unwrap();
    }

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";
        let parameters = BTreeMap::from([("id".to_string(), Value::String("first".to_string()))]);
        let first = db.explain_query_with_params(query, &parameters).unwrap();
        assert_eq!(first.plan_cache_lookup, PlanCacheLookup::Miss);

        db.query("MATCH (m:Memory) WHERE m.id = 'first' SET m.note = 'updated'")
            .unwrap();
        let different_plan = db
            .explain_query("MATCH (m:Memory) RETURN count(m) AS count")
            .unwrap();
        assert_eq!(different_plan.plan_cache_lookup, PlanCacheLookup::Miss);
        assert!(different_plan.trace.decisions.iter().any(|decision| {
            decision.starts_with("optimizer statistics cache refresh:")
                && decision.contains("publication_changed=false")
        }));
        assert!(different_plan.trace.decisions.iter().any(|decision| {
            decision.starts_with("optimizer advanced statistics freshness:")
                && decision.contains("status=unavailable")
        }));

        let reused = db.explain_query_with_params(query, &parameters).unwrap();
        assert_eq!(reused.plan_cache_lookup, PlanCacheLookup::Hit);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn optimizer_reuses_statistics_snapshot_within_a_commit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    db.explain_query("MATCH (m:Memory) WHERE m.id = 'first' RETURN m.id AS id")
        .unwrap();
    let output = db
        .explain_query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();

    assert!(output.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer statistics cache hit: statistics_epoch=")
    }));
    assert!(output.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer catalog cache hit: statistics_epoch=")
    }));
}

#[test]
fn pagination_parameters_keep_distinct_plan_variants() {
    let db = Database::new();
    let query = "MATCH (m:Memory) RETURN m.id AS id ORDER BY m.id LIMIT $limit";

    let first = db
        .explain_query_with_params(
            query,
            &BTreeMap::from([("limit".to_string(), Value::Int(1))]),
        )
        .unwrap();
    let second = db
        .explain_query_with_params(
            query,
            &BTreeMap::from([("limit".to_string(), Value::Int(2))]),
        )
        .unwrap();

    assert_eq!(first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(second.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(db.plan_cache_stats().entries, 2);
}

#[test]
fn order_by_limit_uses_top_n_and_preserves_stable_order() {
    let mut db = Database::new();
    for (id, score) in [("a", 3), ("b", 3), ("c", 2), ("d", 1)] {
        db.query(&format!("CREATE (:Memory {{id: '{id}', score: {score}}})"))
            .unwrap();
    }
    let query = "MATCH (m:Memory) RETURN m.id AS id, m.score AS score \
                 ORDER BY m.score DESC, m.id ASC SKIP 1 LIMIT 2";

    let explain = db.explain_query(query).unwrap();
    let output = db.query(query).unwrap();

    assert!(explain.trace.selected_plan.contains("TopNExec"));
    assert!(!explain.trace.selected_plan.contains("SortExec"));
    assert!(!explain.trace.selected_plan.contains("LimitExec"));
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("b".to_string()),
            Value::String("c".to_string())
        ]
    );
}

#[test]
fn sql_reads_plan_cache_virtual_table() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    db.explain_query(query).unwrap();
    db.explain_query(query).unwrap();

    let output = db
        .query_sql(
            "SELECT metric, value FROM system.plan_cache \
             WHERE metric IN ('admissions', 'entries', 'hits', 'memory_pressure_events', 'misses') \
             ORDER BY metric",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([
                (
                    "metric".to_string(),
                    Value::String("admissions".to_string())
                ),
                ("value".to_string(), Value::Int(1)),
            ]),
            BTreeMap::from([
                ("metric".to_string(), Value::String("entries".to_string())),
                ("value".to_string(), Value::Int(1)),
            ]),
            BTreeMap::from([
                ("metric".to_string(), Value::String("hits".to_string())),
                ("value".to_string(), Value::Int(1)),
            ]),
            BTreeMap::from([
                (
                    "metric".to_string(),
                    Value::String("memory_pressure_events".to_string())
                ),
                ("value".to_string(), Value::Int(0)),
            ]),
            BTreeMap::from([
                ("metric".to_string(), Value::String("misses".to_string())),
                ("value".to_string(), Value::Int(1)),
            ]),
        ]
    );
}

#[test]
fn sql_system_table_queries_do_not_use_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    db.explain_query(query).unwrap();
    db.explain_query(query).unwrap();
    let before = db.plan_cache_stats();

    db.query_sql("SELECT * FROM system.plan_cache").unwrap();
    db.query_sql("SELECT * FROM system.slow_queries").unwrap();
    db.query_sql("SELECT * FROM system.statement_summary")
        .unwrap();

    let after = db.plan_cache_stats();
    assert_eq!(after, before);
}

#[test]
fn sql_reads_completed_slow_query_ring() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });

    db.query("CREATE (:Memory {id: 'm1', title: 'Graph foundations'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}) RETURN m.title AS title")
        .unwrap();

    let output = db
        .query_sql(
            "SELECT query_language, row_count, slow_log_candidate \
             FROM system.slow_queries \
             WHERE query_language = 'cypher' AND slow_log_candidate = true \
             ORDER BY sequence DESC LIMIT 1",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([
            (
                "query_language".to_string(),
                Value::String("cypher".to_string())
            ),
            ("row_count".to_string(), Value::Int(1)),
            ("slow_log_candidate".to_string(), Value::Bool(true)),
        ])]
    );
}

#[test]
fn slow_query_jsonl_export_is_redacted_by_default() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });

    db.query("CREATE (:Memory {id: 'secret-id', title: 'Sensitive title'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'secret-id'}) RETURN m.title AS title")
        .unwrap();

    let jsonl = db.slow_query_log_jsonl().unwrap();

    assert!(jsonl.contains("skein-slow-query-log-event-v1"));
    assert!(jsonl.contains("query_digest"));
    assert!(!jsonl.contains("MATCH"));
    assert!(!jsonl.contains("secret-id"));
    assert!(!jsonl.contains("Sensitive title"));
}

#[cfg(feature = "acl")]
#[test]
fn slow_query_log_records_acl_epoch_without_policy_inputs() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });

    db.query("CREATE (:Memory {id: 'visible', title: 'Visible', secret_space_id: 'secret_space'})")
        .unwrap();
    db.query_with_params_access_control(
        "MATCH (m:Memory) RETURN m.id AS id",
        &BTreeMap::new(),
        QueryAccessControlContext::visibility_scope(11, "secret_space_id", "secret_space"),
    )
    .unwrap();

    let acl_record = db
        .slow_query_log_snapshot()
        .into_iter()
        .find(|record| record.access_control_policy_epoch == Some(11))
        .expect("ACL slow-query record");
    assert_eq!(acl_record.access_control_policy_epoch, Some(11));

    let output = db
        .query_sql(
            "SELECT access_control_policy_epoch FROM system.slow_queries \
             WHERE access_control_policy_epoch = 11",
        )
        .unwrap();
    assert_eq!(
        output.rows,
        vec![BTreeMap::from([(
            "access_control_policy_epoch".to_string(),
            Value::Int(11)
        )])]
    );

    let jsonl = db.slow_query_log_jsonl().unwrap();
    assert!(jsonl.contains("\"access_control_policy_epoch\":11"));
    assert!(jsonl.contains("\"access_control_policy_inputs_copied\":false"));
    assert!(!jsonl.contains("secret_space_id"));
    assert!(!jsonl.contains("secret_space"));
}

#[test]
fn failed_cypher_queries_do_not_enter_slow_query_ring() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });

    let error = db
        .query("CREATE (:Memory {id: 'blocked', title: 'Blocked write'})")
        .expect_err("read-only mutation should fail");
    assert!(error.to_string().contains("read-only"));

    let output = db.query_sql("SELECT * FROM system.slow_queries").unwrap();

    assert!(output.rows.is_empty());

    let summary = db
        .query_sql(
            "SELECT statement_kind, execution_count, success_count, error_count, \
             last_success, last_error \
             FROM system.statement_summary \
             WHERE statement_kind = 'create_node'",
        )
        .unwrap();

    assert_eq!(summary.rows.len(), 1);
    assert_eq!(
        summary.rows[0].get("statement_kind"),
        Some(&Value::String("create_node".to_string()))
    );
    assert_eq!(summary.rows[0].get("execution_count"), Some(&Value::Int(1)));
    assert_eq!(summary.rows[0].get("success_count"), Some(&Value::Int(0)));
    assert_eq!(summary.rows[0].get("error_count"), Some(&Value::Int(1)));
    assert_eq!(
        summary.rows[0].get("last_success"),
        Some(&Value::Bool(false))
    );
    assert!(matches!(
        summary.rows[0].get("last_error"),
        Some(Value::String(message)) if message.contains("read-only")
    ));
}

#[test]
fn cypher_queries_below_slow_threshold_do_not_enter_slow_query_ring() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: u128::MAX,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });

    db.query("CREATE (:Memory {id: 'fast', title: 'Fast path'})")
        .unwrap();

    let output = db.query_sql("SELECT * FROM system.slow_queries").unwrap();

    assert!(output.rows.is_empty());
}

#[test]
fn read_transaction_sql_reads_slow_query_snapshot() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });

    db.query("CREATE (:Memory {id: 'before-read-tx', title: 'Before'})")
        .unwrap();
    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'after-read-tx', title: 'After'})")
        .unwrap();

    let output = read_tx
        .query_sql("SELECT sequence FROM system.slow_queries ORDER BY sequence")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("sequence"), Some(&Value::Int(1)));
}

#[test]
fn sql_reads_statement_summary_virtual_table() {
    let mut db = Database::new_with_config(DatabaseConfig {
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory {id: 'summary'}) RETURN m.title AS title";

    db.query("CREATE (:Memory {id: 'summary', title: 'Statement summary'})")
        .unwrap();
    db.query(query).unwrap();
    db.query(query).unwrap();

    let output = db
        .query_sql(
            "SELECT statement_kind, execution_count, success_count, error_count, \
             total_row_count \
             FROM system.statement_summary \
             WHERE statement_kind = 'match_return' \
             ORDER BY execution_count DESC LIMIT 1",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([
            (
                "statement_kind".to_string(),
                Value::String("match_return".to_string())
            ),
            ("execution_count".to_string(), Value::Int(2)),
            ("success_count".to_string(), Value::Int(2)),
            ("error_count".to_string(), Value::Int(0)),
            ("total_row_count".to_string(), Value::Int(2)),
        ])]
    );
}

#[test]
fn statement_summary_groups_normalized_query_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', title: 'Second'})")
        .unwrap();

    db.query("MATCH (m:Memory {id: 'first'}) RETURN m.title AS title")
        .unwrap();
    db.query(" match (m:Memory { id : \"second\" }) return m.title as title; ")
        .unwrap();

    let output = db
        .query_sql(
            "SELECT digest, sample_query_text_hash, execution_count \
             FROM system.statement_summary \
             WHERE statement_kind = 'match_return'",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("execution_count"), Some(&Value::Int(2)));
    assert!(matches!(
        output.rows[0].get("digest"),
        Some(Value::String(digest)) if digest.starts_with("q1:")
    ));
    assert!(matches!(
        output.rows[0].get("sample_query_text_hash"),
        Some(Value::String(hash)) if hash.starts_with("t1:")
    ));
}

#[test]
fn slow_query_and_statement_summary_share_query_identity() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'shared-digest'})").unwrap();
    db.query("MATCH (m:Memory {id: 'shared-digest'}) RETURN m.id AS id")
        .unwrap();

    let slow = db
        .slow_query_log_snapshot()
        .into_iter()
        .find(|record| record.statement_kind == "match_return")
        .unwrap();
    let summary = db
        .query_sql(
            "SELECT digest FROM system.statement_summary \
             WHERE statement_kind = 'match_return'",
        )
        .unwrap();

    assert_eq!(slow.statement_kind, "match_return");
    assert_eq!(
        summary.rows[0].get("digest"),
        Some(&Value::String(slow.query_digest))
    );
}

#[test]
fn plan_cache_misses_after_graph_commit_epoch_changes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    db.explain_query(query).unwrap();
    db.explain_query(query).unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let after_commit = db.explain_query(query).unwrap();

    assert_eq!(after_commit.plan_cache_lookup, PlanCacheLookup::Miss);
    assert!(after_commit
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.admissions, 2);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 1);
    assert_eq!(stats.memory_pressure_events, 0);
}

#[test]
fn plan_cache_misses_after_index_descriptor_changes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    for id in 1..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: {id}, title: 'Memory {id}'}})"
        ))
        .unwrap();
    }
    let query = "MATCH (m:Memory) WHERE m.created_at >= 16 RETURN m.title AS title";

    let before_index = db.explain_query(query).unwrap();
    let cached_before_index = db.explain_query(query).unwrap();
    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();
    let after_index = db.explain_query(query).unwrap();

    assert_eq!(before_index.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(cached_before_index.plan_cache_lookup, PlanCacheLookup::Hit);
    assert_eq!(after_index.plan_cache_lookup, PlanCacheLookup::Miss);
    assert!(before_index
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(!before_index.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(cached_before_index
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: parameterized physical plan template"));
    assert!(after_index
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    assert!(after_index
        .trace
        .selected_plan
        .contains("IndexNodeRangeSeek"));
    assert!(!after_index.trace.selected_plan.contains("SeqNodeScan"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.admissions, 2);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 17);
    assert_eq!(stats.memory_pressure_events, 0);
}

#[test]
fn plan_cache_evicts_least_frequently_used_plan() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(2),
        ..DatabaseConfig::default()
    });
    let q1 = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";
    let q2 = "MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title";
    let q3 = "MATCH (m:Memory) WHERE m.id = 3 RETURN m.title AS title";

    db.explain_query(q1).unwrap();
    db.explain_query(q1).unwrap();
    db.explain_query(q2).unwrap();
    db.explain_query(q3).unwrap();

    let hot = db.explain_query(q1).unwrap();
    let evicted = db.explain_query(q2).unwrap();

    assert_eq!(hot.plan_cache_lookup, PlanCacheLookup::Hit);
    assert_eq!(evicted.plan_cache_lookup, PlanCacheLookup::Miss);
    assert!(hot
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: parameterized physical plan template"));
    assert!(evicted
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 4);
    assert_eq!(stats.admissions, 4);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 0);
    assert_eq!(stats.evictions, 2);
    assert_eq!(stats.memory_pressure_events, 2);
}

#[test]
fn plan_cache_can_be_disabled_with_zero_capacity() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(0),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    let first = db.explain_query(query).unwrap();
    let second = db.explain_query(query).unwrap();

    assert_eq!(first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(second.plan_cache_lookup, PlanCacheLookup::Miss);
    assert!(first
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    assert!(second
        .trace
        .decisions
        .iter()
        .any(|decision| decision
            == "plan cache miss: optimized parameterized physical plan template"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.max_entries, Some(0));
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.admissions, 0);
    assert_eq!(stats.disabled_misses, 2);
    assert_eq!(stats.bypasses, 0);
    assert_eq!(stats.evictions, 0);
    assert_eq!(stats.memory_pressure_events, 0);
}

#[test]
fn plan_cache_records_bypassed_mutation_explain_separately() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });

    let output = db
        .explain_query("CREATE (:Memory {id: 1, title: 'Bypassed'})")
        .unwrap();

    assert_eq!(
        output.plan_cache_lookup,
        PlanCacheLookup::Bypass(PlanCacheBypassReason::StatementNotCacheable)
    );
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache bypass: statement_not_cacheable"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.admissions, 0);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 1);
    assert_eq!(stats.evictions, 0);
    assert_eq!(stats.memory_pressure_events, 0);
}

#[test]
fn explain_uses_scan_without_index_descriptor() {
    let db = Database::new();
    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert!(output
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(!output.trace.selected_plan.contains("FilterExec"));
    assert!(!output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("no equality index descriptor")));
}

#[test]
fn explain_uses_statistics_to_keep_low_selectivity_filter() {
    let mut db = Database::new();
    for id in 1..=5 {
        db.query(&format!("CREATE (:Memory {{id: {id}, kind: 'note'}})"))
            .unwrap();
    }

    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.id AS id")
        .unwrap();

    assert!(output
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(!output.trace.selected_plan.contains("FilterExec"));
    assert!(!output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose SeqNodeScan")));
}

#[test]
fn explain_keeps_full_scan_for_small_indexed_label() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert!(output
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(!output.trace.selected_plan.contains("FilterExec"));
    assert!(!output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose SeqNodeScan for Memory.id")));
}

#[test]
fn plan_fingerprint_is_deterministic_and_changes_with_plan_shape() {
    let mut db = Database::new();
    for id in 1..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: {id}, title: 'Memory {id}'}})"
        ))
        .unwrap();
    }
    let query = "MATCH (m:Memory) WHERE m.created_at >= 16 RETURN m.title AS title";
    let before = db.explain_query(query).unwrap();
    let before_again = db.explain_query(query).unwrap();

    assert_eq!(
        before.trace.selected_plan_fingerprint,
        before_again.trace.selected_plan_fingerprint
    );
    assert!(before
        .trace
        .selected_plan_fingerprint
        .contains("NodeProjectionScanExec"));

    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();
    let after = db.explain_query(query).unwrap();

    assert_ne!(
        before.trace.selected_plan_fingerprint,
        after.trace.selected_plan_fingerprint
    );
    assert!(after
        .trace
        .selected_plan_fingerprint
        .contains("IndexNodeRangeSeek"));
}

#[test]
fn plan_fingerprint_excludes_bound_values_but_instance_fingerprint_retains_them() {
    let mut db = Database::new();
    for id in 1..=16 {
        db.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
    }

    let first = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap();
    let second = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.id AS id")
        .unwrap();

    assert_eq!(
        first.trace.selected_plan_fingerprint,
        second.trace.selected_plan_fingerprint
    );
    assert_ne!(
        first.physical_plan.instance_fingerprint(),
        second.physical_plan.instance_fingerprint()
    );
}
