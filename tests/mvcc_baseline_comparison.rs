// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.
use hawdb::*;
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "hawdb-baseline-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn n64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn governor() -> RuntimeGovernor {
    let r = RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(nz(2), None, None),
        RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
    );
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            background_task_limit: Some(nz(1)),
            memory_budget_bytes: Some(128 << 20),
            ..RuntimeGovernorConfig::shared_host()
        },
        r,
        IoConcurrencyBudget::new(2, 1),
    )
}
fn permit(g: &RuntimeGovernor) -> SearchGenerationAdmission {
    SearchGenerationAdmission::acquire(
        g,
        RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1),
    )
    .unwrap()
}
fn config() -> ProjectionBaselineComparisonConfig {
    ProjectionBaselineComparisonConfig {
        reads: PreparedReadLimits {
            dependency_entries: nz(128),
            dependency_bytes: nz(16384),
            statement_rows: nz(4),
            statement_payload_bytes: nz(4096),
        },
        feed: ProjectionFeedLimits {
            work: nz(100000),
            items: nz(10000),
            bytes: nz(1 << 20),
        },
        source: Default::default(),
        mapping: ProjectionBaselineMappingLimits {
            calls: nz(10000),
            queries: nz(1000),
            reserved_rows: nz(4000),
            reserved_payload_bytes: nz(4 << 20),
            queries_per_call: nz(2),
        },
        sorting: SearchUnorderedMutationLimits {
            max_buffer_records: nz(1),
            ..Default::default()
        },
        build: Default::default(),
        comparison: SearchBaselineComparisonLimits {
            lookups: nz(1000),
            compressed_segment_bytes: n64(65536),
            expanded_segment_bytes: n64(65536),
            total_compressed_bytes: n64(64 << 20),
            total_expanded_bytes: n64(64 << 20),
        },
        task: Default::default(),
    }
}
fn document(id: &str, body: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: id.into(),
        content: body.into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}
fn docs() -> Vec<SearchDocument> {
    vec![
        document("memory:a", "1"),
        document("record:a", "old"),
        document("record:z", "zed"),
    ]
}
fn setup(cold: bool, kind: &str) -> (Root, Database, SearchOutOfCoreReader) {
    let root = Root::new();
    let graph = root.0.join("graph");
    let mut db = Database::open(&graph).unwrap();
    db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
    db.query("CREATE (:Other {id:'hole'})").unwrap();
    db.query("MATCH (n:Other) DELETE n").unwrap();
    db.query_sql("CREATE TABLE records (id TEXT PRIMARY KEY,body TEXT)")
        .unwrap();
    db.query_sql("CREATE TABLE empty_records (id TEXT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO records (id,body) VALUES ('a','old'),('z','zed')")
        .unwrap();
    if kind == "duplicate" {
        db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
    }
    if kind == "unknown_table" {
        db.query_sql("CREATE TABLE unknown_records (id TEXT PRIMARY KEY)")
            .unwrap();
    }
    if kind == "empty" {
        db.query("MATCH (n:Memory) DELETE n").unwrap();
        db.query_sql("DELETE FROM records WHERE id IN ('a','z')")
            .unwrap();
    }
    let c = db.commit_epoch().unwrap();
    let mut expected = docs();
    match kind {
        "empty" => expected.clear(),
        "extra" => expected.push(document("extra", "retained")),
        "missing" => {
            expected.remove(2);
        }
        "body" => expected[0].content = "2".into(),
        "vector" => expected[0].embedding = Some(vec![1.0, 2.0]),
        _ => {}
    }
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        root.0.join("search"),
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: if kind == "unknown_epoch" {
                None
            } else {
                Some(c)
            },
            ..Default::default()
        },
    )
    .unwrap();
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    for d in expected {
        writer.push(d).unwrap();
    }
    writer.finish().unwrap();
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &graph,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
    }
    let reader = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
    (root, db, reader)
}
fn mapped(
    ctx: &mut ProjectionBaselineMappingContext<'_, '_>,
    input: ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>> {
    match input {
        ProjectionFullSourceItem::GraphNodeCandidate { node_id } => {
            let result = ctx.query_with_params(
                "MATCH (n:Memory) WHERE id(n)=$id RETURN n.id AS id,n.value AS value",
                &BTreeMap::from([("id".into(), Value::Int(node_id as i64))]),
            )?;
            let Some(row) = result.output.rows.first() else {
                return Ok(None);
            };
            let Value::String(id) = &row["id"] else {
                panic!("string id")
            };
            let Value::Int(value) = row["value"] else {
                panic!("integer value")
            };
            Ok(Some(document(&format!("memory:{id}"), &value.to_string())))
        }
        ProjectionFullSourceItem::RelationalTable { table }
        | ProjectionFullSourceItem::RelationalKey { table, .. }
            if table != "records" =>
        {
            // Empty application table and operational migration registry are
            // explicit non-document sources for this synthetic contract.
            if !matches!(table, "empty_records" | "hawdb_schema_migrations") {
                return Err(HawDBError::Execution(format!(
                    "unclassified canonical table {table}"
                )));
            }
            Ok(None)
        }
        ProjectionFullSourceItem::RelationalKey { key, .. } => {
            let RelationalValue::Text(id) = &key.0[0] else {
                panic!("text PK")
            };
            let row = ctx.query_sql_with_params(
                "SELECT body FROM records WHERE id=$1",
                &[Value::String(id.clone())],
            )?;
            let Value::String(body) = &row.rows[0]["body"] else {
                panic!("text body")
            };
            Ok(Some(document(&format!("record:{id}"), body)))
        }
        ProjectionFullSourceItem::RelationalTable { .. } => Ok(None),
        ProjectionFullSourceItem::End { source_epoch } => {
            assert_eq!(source_epoch, ctx.source_epoch());
            Ok(None)
        }
    }
}
fn step(
    session: &mut ProjectionBaselineComparisonSession<'_>,
) -> std::result::Result<ProjectionBaselineComparisonStep, ProjectionFeedError> {
    session.advance(
        ProjectionFeedLimits {
            work: nz(8),
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
fn complete(session: &mut ProjectionBaselineComparisonSession<'_>) {
    for _ in 0..10000 {
        if step(session).unwrap() == ProjectionBaselineComparisonStep::Complete {
            return;
        }
    }
    panic!("baseline comparison did not complete")
}
fn unchanged(root: &Root, reader: &SearchOutOfCoreReader) {
    let r = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
    assert_eq!(r.generation(), reader.generation());
    assert_eq!(
        r.source_graph_commit_epoch(),
        reader.source_graph_commit_epoch()
    );
}
#[test]
fn baseline_complete_hot_cold_mapping_remains_at_c_across_later_writes() {
    for cold in [false, true] {
        let (root, mut db, reader) = setup(cold, "match");
        let g = governor();
        let c = db.commit_epoch().unwrap();
        let mut session = db
            .begin_projection_baseline_comparison(&reader, config(), mapped, permit(&g))
            .unwrap();
        db.query("MATCH (n:Memory) SET n.value=99").unwrap();
        db.query("CREATE (:Memory {id:'later',value:2})").unwrap();
        db.query_sql("UPDATE records SET body='later' WHERE id='a'")
            .unwrap();
        db.checkpoint().unwrap();
        complete(&mut session);
        assert_eq!(g.snapshot().active_background_tasks, 1);
        let result = session.finish().unwrap();
        assert_eq!(result.source_epoch(), c);
        assert_eq!(result.generation(), reader.generation());
        assert_eq!(result.report().compared_documents, 3);
        assert_eq!(result.report().reserved_lookups, 3);
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        assert!(result.mapping_usage().queries >= 4);
        unchanged(&root, &reader);
    }
}
#[test]
fn baseline_refuses_extra_missing_changed_vector_and_duplicate_owners() {
    for kind in [
        "extra",
        "missing",
        "body",
        "vector",
        "duplicate",
        "unknown_table",
    ] {
        let (root, db, reader) = setup(false, kind);
        let g = governor();
        let mut session = db
            .begin_projection_baseline_comparison(&reader, config(), mapped, permit(&g))
            .unwrap();
        let mut refused = false;
        for _ in 0..10000 {
            match step(&mut session) {
                Err(_) => {
                    refused = true;
                    break;
                }
                Ok(s) => assert_ne!(s, ProjectionBaselineComparisonStep::Complete),
            }
        }
        assert!(refused, "{kind}");
        assert!(step(&mut session).is_err());
        assert!(session.finish().is_err());
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        unchanged(&root, &reader);
    }
}
fn bad_read(
    ctx: &mut ProjectionBaselineMappingContext<'_, '_>,
    _: ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>> {
    let _ = ctx.query_with_params("MATCH (", &BTreeMap::new());
    Ok(None)
}
fn boundary_doc(
    _: &mut ProjectionBaselineMappingContext<'_, '_>,
    input: ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>> {
    Ok(matches!(
        input,
        ProjectionFullSourceItem::RelationalTable { .. } | ProjectionFullSourceItem::End { .. }
    )
    .then(|| document("bad", "boundary")))
}
fn panic_map(
    _: &mut ProjectionBaselineMappingContext<'_, '_>,
    _: ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>> {
    panic!("baseline mapper panic")
}
#[test]
fn baseline_failed_mapper_and_panic_keep_admission_until_cleanup() {
    let (root, db, reader) = setup(false, "match");
    let g = governor();
    for mapper in [
        bad_read as ProjectionBaselineMappingFunction,
        boundary_doc,
        panic_map,
    ] {
        let mut session = db
            .begin_projection_baseline_comparison(&reader, config(), mapper, permit(&g))
            .unwrap();
        let mut refused = false;
        for _ in 0..10000 {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| step(&mut session)));
            if result.is_err() || result.unwrap().is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused);
        assert_eq!(g.snapshot().active_background_tasks, 1);
        assert!(session.finish().is_err());
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        unchanged(&root, &reader);
    }
}
#[test]
fn baseline_cumulative_mapping_source_admission_cancel_and_premature_finish() {
    let (root, db, reader) = setup(false, "match");
    let g = governor();
    for kind in [
        "queries",
        "calls",
        "lookup",
        "compressed",
        "expanded",
        "partial",
        "cancel",
        "cancel_done",
    ] {
        let mut cfg = config();
        let cancel = cfg.task.cancellation().clone();
        match kind {
            "queries" => cfg.mapping.queries = nz(1),
            "calls" => cfg.mapping.calls = nz(1),
            "lookup" => cfg.comparison.lookups = nz(1),
            "compressed" => cfg.comparison.compressed_segment_bytes = n64(1),
            "expanded" => cfg.comparison.total_expanded_bytes = n64(1),
            _ => {}
        }
        let mut session = db
            .begin_projection_baseline_comparison(&reader, cfg, mapped, permit(&g))
            .unwrap();
        if kind == "cancel_done" {
            complete(&mut session);
            cancel.cancel();
        } else if kind == "cancel" {
            cancel.cancel();
            assert!(step(&mut session).is_err());
        } else if kind != "partial" {
            let mut refused = false;
            for _ in 0..10000 {
                if step(&mut session).is_err() {
                    refused = true;
                    break;
                }
            }
            assert!(refused, "{kind}");
        }
        assert!(session.finish().is_err(), "{kind}");
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        unchanged(&root, &reader);
    }
}
#[test]
fn baseline_unknown_lagged_epoch_and_caller_epoch_refuse_before_mapping() {
    for kind in ["unknown_epoch", "lagged", "caller"] {
        let (root, mut db, reader) = setup(false, kind);
        let g = governor();
        let mut cfg = config();
        if kind == "lagged" {
            db.query("CREATE (:Memory {id:'later'})").unwrap();
        }
        if kind == "caller" {
            cfg.build.source_graph_commit_epoch = reader.source_graph_commit_epoch();
        }
        assert!(db
            .begin_projection_baseline_comparison(&reader, cfg, mapped, permit(&g))
            .is_err());
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        unchanged(&root, &reader);
    }
}

#[test]
fn baseline_empty_source_and_held_old_search_head_are_compared_exactly() {
    let (root, db, reader) = setup(true, "empty");
    let g = governor();
    let mut session = db
        .begin_projection_baseline_comparison(&reader, config(), mapped, permit(&g))
        .unwrap();
    complete(&mut session);
    let result = session.finish().unwrap();
    assert_eq!(result.report().compared_documents, 0);
    assert_eq!(result.report().reserved_lookups, 0);
    unchanged(&root, &reader);
    let (root, db, reader) = setup(false, "match");
    let mut session = db
        .begin_projection_baseline_comparison(&reader, config(), mapped, permit(&g))
        .unwrap();
    let mut external = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
        &reader,
        Default::default(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    external.upsert(document("memory:a", "new-index")).unwrap();
    external.prepare_publication().unwrap().1.publish().unwrap();
    let newer = SearchOutOfCoreReader::open(root.0.join("search")).unwrap();
    assert_ne!(newer.generation(), reader.generation());
    complete(&mut session);
    let result = session.finish().unwrap();
    assert_eq!(result.generation(), reader.generation());
    assert_eq!(result.report().compared_documents, 3);
    assert_ne!(result.generation(), newer.generation());
}

#[test]
fn baseline_cold_pages_keep_one_database_dependency_slot() {
    let (root, mut db, _reader) = setup(true, "match");
    let values = (0..520)
        .map(|i| format!("('p{i:04}','body-{i}')"))
        .collect::<Vec<_>>()
        .join(",");
    db.query_sql(&format!("INSERT INTO records (id,body) VALUES {values}"))
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open_with_config(
        root.0.join("graph"),
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        },
    )
    .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut expected = docs();
    expected.extend((0..520).map(|i| document(&format!("record:p{i:04}"), &format!("body-{i}"))));
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    let path = root.0.join("bulk-search");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(epoch),
            ..Default::default()
        },
    )
    .unwrap();
    for d in expected {
        writer.push(d).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(path).unwrap();
    let g = governor();
    let mut cfg = config();
    cfg.reads.dependency_entries = nz(1);
    cfg.comparison.compressed_segment_bytes = n64(1 << 20);
    cfg.comparison.expanded_segment_bytes = n64(1 << 20);
    cfg.comparison.total_compressed_bytes = n64(1 << 30);
    cfg.comparison.total_expanded_bytes = n64(1 << 30);
    let mut session = db
        .begin_projection_baseline_comparison(&reader, cfg, mapped, permit(&g))
        .unwrap();
    let mut done = false;
    for _ in 0..10000 {
        if session
            .advance(
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
            .unwrap()
            == ProjectionBaselineComparisonStep::Complete
        {
            done = true;
            break;
        }
    }
    assert!(done);
    let matched = session.finish().unwrap();
    assert_eq!(matched.report().compared_documents, 523);
    assert_eq!(matched.report().reserved_lookups, 523);
    assert!(matched.mapping_usage().queries >= 524);
    assert_eq!(g.snapshot().active_background_tasks, 0);
}

#[path = "mvcc_baseline_comparison/full_rebuild.rs"]
mod full_rebuild;
