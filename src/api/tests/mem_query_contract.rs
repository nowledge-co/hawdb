// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Executable consumer query shapes from nowledge-co/mem issue 9338.
use super::*;
use std::collections::BTreeMap;

const SOURCE: &str = "MATCH (t:Thread) WHERE t.source IS NOT NULL WITH LOWER(regexp_replace(t.source, ' ', '_', 'g')) AS normalized_source, COUNT(t) AS thread_count RETURN normalized_source, thread_count ORDER BY thread_count DESC LIMIT 1";
const ENTITIES: &str = "MATCH (e:Entity) UNWIND list_append(list_append(COALESCE(e.aliases, ['']), e.name), e.id) AS entity_search_value WITH e, entity_search_value WHERE lower(entity_search_value) CONTAINS $entity_query WITH DISTINCT e RETURN e.id AS id LIMIT 50";
const SPACE: &str = "MATCH (t:Thread {id: $memory_id}) WHERE CASE WHEN t.space_id IS NULL OR trim(t.space_id) = '' THEN 'default' ELSE t.space_id END = 'default' RETURN t.source AS source, t.metadata AS metadata";
const KEEP_BOTH: &str = "MATCH (older:Memory {id: $older_id}), (newer:Memory {id: $newer_id}) SET older.is_latest = true, newer.is_latest = true";
const THREAD: &str = "MATCH (t:Thread {id: $thread_uuid})
 SET t.message_count = $message_count,
 t.first_message_at = CASE WHEN $first_message_at IS NULL THEN t.first_message_at
 WHEN $preserve_existing_range = true AND t.first_message_at IS NOT NULL
 AND t.first_message_at < $first_message_at THEN t.first_message_at ELSE $first_message_at END,
 t.last_message_at = CASE WHEN $last_message_at IS NULL THEN t.last_message_at
 WHEN $preserve_existing_range = true AND t.last_message_at IS NOT NULL
 AND t.last_message_at > $last_message_at THEN t.last_message_at ELSE $last_message_at END,
 t.updated_at = CASE WHEN $mutation_at IS NULL THEN t.updated_at ELSE $mutation_at END,
 t.metadata = CASE WHEN $source_time_metadata IS NULL THEN t.metadata ELSE $source_time_metadata END";

fn params(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(key, value)| ((*key).into(), value.clone()))
        .collect()
}
fn text(value: &str) -> Value {
    Value::String(value.into())
}
fn fixture(config: DatabaseConfig) -> Database {
    let mut db = Database::new_with_config(config);
    for id in ["old", "new", "unrelated"] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, is_latest: false})",
            &params(&[("id", text(id))]),
        )
        .unwrap();
    }
    db
}
fn state(db: &mut Database) -> Vec<BTreeMap<String, Value>> {
    db.query("MATCH (m:Memory) RETURN m.id AS id, m.is_latest AS latest ORDER BY id")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| (key.to_owned(), value.clone()))
                .collect()
        })
        .collect()
}

#[test]
fn mem_9338_normalized_grouping_uses_the_computed_string() {
    let mut db = Database::new();
    for source in [
        text("Claude Code"),
        text("CLAUDE CODE"),
        text("Other"),
        Value::Null,
    ] {
        db.query_with_params(
            "CREATE (:Thread {source: $source})",
            &params(&[("source", source)]),
        )
        .unwrap();
    }
    let rows = db.query(SOURCE).unwrap().rows;
    assert_eq!(
        rows,
        vec![params(&[
            ("normalized_source", text("claude_code")),
            ("thread_count", Value::Int(2))
        ])]
    );
}

#[test]
fn mem_9338_entity_unwind_includes_alias_name_id_and_deduplicates() {
    let mut db = Database::new();
    for (id, name, aliases) in [
        ("one", "PG", Value::List(vec![text("pg"), text("pg")])),
        ("two", "Other", Value::List(vec![text("pg")])),
        ("pg-id", "Other", Value::Null),
        ("four", "PG", Value::Null),
        ("excluded", "other", Value::List(vec![])),
    ] {
        db.query_with_params(
            "CREATE (:Entity {id: $id, name: $name, aliases: $aliases})",
            &params(&[("id", text(id)), ("name", text(name)), ("aliases", aliases)]),
        )
        .unwrap();
    }
    let mut ids = db
        .query_with_params(ENTITIES, &params(&[("entity_query", text("pg"))]))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row["id"].clone())
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec![text("four"), text("one"), text("pg-id"), text("two")]
    );
    let output = db
        .query("UNWIND list_append([1, 1], [2, 3]) AS x RETURN x")
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(
        output.rows[2]["x"],
        Value::List(vec![Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn mem_9338_trim_case_preserves_space_isolation() {
    let mut db = Database::new();
    for (id, space) in [
        ("null", Value::Null),
        ("empty", text("")),
        ("white", text(" \t\n")),
        ("unicode", text("\u{00a0}\u{2003}")),
        ("zero_width", text("\u{200b}")),
        ("default", text("default")),
        ("private", text("private")),
        ("padded", text(" default ")),
    ] {
        db.query_with_params(
            "CREATE (:Thread {id: $id, space_id: $space, source: 'source', metadata: '{}'})",
            &params(&[("id", text(id)), ("space", space)]),
        )
        .unwrap();
        let rows = db
            .query_with_params(SPACE, &params(&[("memory_id", text(id))]))
            .unwrap()
            .rows;
        assert_eq!(
            rows.len(),
            usize::from(!matches!(id, "private" | "padded" | "zero_width")),
            "{id}"
        );
    }
}

#[test]
fn mem_9338_multi_binding_set_is_atomic_and_missing_endpoint_is_noop() {
    let mut db = fixture(DatabaseConfig::default());
    let before = state(&mut db);
    db.query_with_params(
        KEEP_BOTH,
        &params(&[("older_id", text("old")), ("newer_id", text("absent"))]),
    )
    .unwrap();
    assert_eq!(state(&mut db), before);
    let epoch = db.statistics().unwrap().computed_at_commit_epoch;
    db.query_with_params(
        KEEP_BOTH,
        &params(&[("older_id", text("old")), ("newer_id", text("new"))]),
    )
    .unwrap();
    assert_eq!(db.statistics().unwrap().computed_at_commit_epoch, epoch + 1);
    assert_eq!(
        state(&mut db),
        vec![
            params(&[("id", text("new")), ("latest", Value::Bool(true))]),
            params(&[("id", text("old")), ("latest", Value::Bool(true))]),
            params(&[("id", text("unrelated")), ("latest", Value::Bool(false))])
        ]
    );
}

#[test]
fn mem_9338_multi_binding_set_budget_failure_and_transaction_rollback() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_operations = std::num::NonZeroUsize::new(1).unwrap();
    let mut db = fixture(config);
    let before = state(&mut db);
    let epoch = db.statistics().unwrap().computed_at_commit_epoch;
    assert!(db
        .query_with_params(
            KEEP_BOTH,
            &params(&[("older_id", text("old")), ("newer_id", text("new"))])
        )
        .is_err());
    assert_eq!(state(&mut db), before);
    assert_eq!(db.statistics().unwrap().computed_at_commit_epoch, epoch);
    let mut db = fixture(DatabaseConfig::default());
    let before = state(&mut db);
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_with_params(
            KEEP_BOTH,
            &params(&[("older_id", text("old")), ("newer_id", text("new"))]),
        )
        .unwrap();
    transaction.rollback();
    assert_eq!(state(&mut db), before);
    db.query("MATCH (a:Memory {id:'old'}), (b:Memory {id:'old'}) SET a.is_latest = true, b.is_latest = false, a.is_latest = true").unwrap();
    assert_eq!(state(&mut db)[1]["latest"], Value::Bool(true));
}

#[test]
fn mem_9338_full_thread_case_set_preserves_ranges_and_null_metadata() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id:'thread', first_message_at:10, last_message_at:30, updated_at:1, metadata:'old'})").unwrap();
    let mut parameters = params(&[
        ("thread_uuid", text("thread")),
        ("message_count", Value::Int(2)),
        ("first_message_at", Value::Int(20)),
        ("last_message_at", Value::Int(20)),
        ("preserve_existing_range", Value::Bool(true)),
        ("mutation_at", Value::Null),
        ("source_time_metadata", Value::Null),
    ]);
    db.query_with_params(THREAD, &parameters).unwrap();
    let read="MATCH (t:Thread) RETURN t.first_message_at AS first, t.last_message_at AS last, t.updated_at AS updated, t.metadata AS metadata, t.message_count AS count";
    assert_eq!(
        db.query(read).unwrap().rows,
        vec![params(&[
            ("first", Value::Int(10)),
            ("last", Value::Int(30)),
            ("updated", Value::Int(1)),
            ("metadata", text("old")),
            ("count", Value::Int(2))
        ])]
    );
    parameters.insert("preserve_existing_range".into(), Value::Bool(false));
    parameters.insert("mutation_at".into(), Value::Int(5));
    parameters.insert("source_time_metadata".into(), text("new"));
    db.query_with_params(THREAD, &parameters).unwrap();
    assert_eq!(
        db.query(read).unwrap().rows[0],
        params(&[
            ("first", Value::Int(20)),
            ("last", Value::Int(20)),
            ("updated", Value::Int(5)),
            ("metadata", text("new")),
            ("count", Value::Int(2))
        ])
    );
    parameters.insert("first_message_at".into(), Value::Null);
    parameters.insert("last_message_at".into(), Value::Null);
    db.query_with_params(THREAD, &parameters).unwrap();
    assert_eq!(db.query(read).unwrap().rows[0]["first"], Value::Int(20));
    db.query("CREATE (:Thread {id:'empty'})").unwrap();
    parameters.insert("thread_uuid".into(), text("empty"));
    parameters.insert("first_message_at".into(), Value::Int(20));
    db.query_with_params(THREAD, &parameters).unwrap();
    assert_eq!(
        db.query("MATCH (t:Thread {id:'empty'}) RETURN t.first_message_at AS first")
            .unwrap()
            .rows[0]["first"],
        Value::Int(20)
    );
}

#[test]
fn mem_9338_string_scalar_errors_and_nulls_are_explicit() {
    let mut db = Database::new();
    assert_eq!(db.query("UNWIND [1] AS x RETURN trim(NULL) AS trimmed, regexp_replace(NULL, ' ', '_', 'g') AS replaced, list_append(NULL, 1) AS appended").unwrap().rows[0],params(&[("trimmed",Value::Null),("replaced",Value::Null),("appended",Value::Null)]));
    for expression in [
        "trim(1)",
        "trim('a','b')",
        "list_append(1, 2)",
        "regexp_replace('x', '[', '')",
        "regexp_replace('x', 'x', '', 'unsupported')",
    ] {
        assert!(
            db.query(&format!("UNWIND [1] AS x RETURN {expression} AS result"))
                .is_err(),
            "{expression}"
        );
    }
    let row=db.query("UNWIND [1] AS x RETURN regexp_replace('a a', 'a', '$1') AS first, regexp_replace('a a', 'a', 'b', 'g') AS all").unwrap().rows.into_iter().next().unwrap();
    assert_eq!(row["first"], text("$1 a"));
    assert_eq!(row["all"], text("b b"));
}

#[test]
fn mem_9338_temporal_ranges_and_multi_set_survive_reopen() {
    let path = unique_test_dir("mem_9338_contract");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id:'old', is_latest:false})")
            .unwrap();
        db.query("CREATE (:Memory {id:'new', is_latest:false})")
            .unwrap();
        db.query("CREATE (:Thread {id:'thread', first_message_at:datetime('2026-01-01T00:00:00Z'), last_message_at:datetime('2026-01-03T00:00:00Z')})").unwrap();
        let input = db
            .query("MATCH (t:Thread) RETURN datetime('2026-01-02T00:00:00Z') AS instant")
            .unwrap()
            .rows[0]["instant"]
            .clone();
        let parameters = params(&[
            ("thread_uuid", text("thread")),
            ("message_count", Value::Int(2)),
            ("first_message_at", input.clone()),
            ("last_message_at", input),
            ("preserve_existing_range", Value::Bool(true)),
            ("mutation_at", Value::Null),
            ("source_time_metadata", Value::Null),
        ]);
        db.query_with_params(THREAD, &parameters).unwrap();
        db.query_with_params(
            KEEP_BOTH,
            &params(&[("older_id", text("old")), ("newer_id", text("new"))]),
        )
        .unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    assert!(state(&mut db)
        .iter()
        .all(|row| row["latest"] == Value::Bool(true)));
    let row=db.query("MATCH (t:Thread) RETURN t.first_message_at AS first, t.last_message_at AS last, datetime('2026-01-01T00:00:00Z') AS expected_first, datetime('2026-01-03T00:00:00Z') AS expected_last").unwrap().rows.into_iter().next().unwrap();
    assert_eq!(row["first"], row["expected_first"]);
    assert_eq!(row["last"], row["expected_last"]);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_9338_text_expansion_respects_query_memory_budget() {
    let mut config = DatabaseConfig::default();
    config.execution_memory.query_memory_bytes = std::num::NonZeroUsize::new(32 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {source:'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'})")
        .unwrap();
    let error = db
        .query_with_params(
            "MATCH (t:Thread) RETURN regexp_replace(t.source, 'a', $replacement, 'g') AS value",
            &params(&[("replacement", text(&"x".repeat(2048)))]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(
        db.query("MATCH (t:Thread) RETURN t.source AS source")
            .unwrap()
            .rows[0]["source"],
        text("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
}

#[test]
fn mem_9338_passing_language_controls_and_sql_comparison_scope() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id:'old'})").unwrap();
    let rows=db.query("MATCH (m:Memory) OPTIONAL MATCH (m)-[r:EVOLVES]->(newer:Memory) RETURN m.id AS id, COUNT(r) AS count").unwrap().rows;
    assert_eq!(rows[0]["count"], Value::Int(0));
    assert_eq!(
        db.query("UNWIND [1, 2] AS x RETURN CASE WHEN x = 1 THEN 'one' ELSE 'two' END AS name")
            .unwrap()
            .rows
            .len(),
        2
    );
    db.query_sql("CREATE TABLE memories (id TEXT PRIMARY KEY, is_latest BOOLEAN)")
        .unwrap();
    db.query_sql("INSERT INTO memories (id, is_latest) VALUES ('old', FALSE), ('new', FALSE), ('unrelated', FALSE)").unwrap();
    db.query_sql("UPDATE memories SET is_latest = TRUE WHERE id IN ('old', 'new')")
        .unwrap();
    assert_eq!(
        db.query_sql("SELECT id FROM memories WHERE is_latest = TRUE ORDER BY id")
            .unwrap()
            .rows,
        vec![
            params(&[("id", text("new"))]),
            params(&[("id", text("old"))])
        ]
    );
    for sql in [
        "SELECT regexp_replace(id, ' ', '_', 'g') FROM memories",
        "SELECT TRIM(id) FROM memories",
        "SELECT CASE WHEN id IS NULL THEN 'default' ELSE id END FROM memories",
        "UPDATE memories SET is_latest = CASE WHEN is_latest IS NULL THEN TRUE ELSE is_latest END",
    ] {
        assert!(
            db.query_sql(sql).is_err(),
            "SQL comparison is deliberately unqualified: {sql}"
        );
    }
}

#[test]
fn mem_9338_invariant_regex_rejects_invalid_patterns_even_on_empty_input() {
    let mut db = Database::new();
    assert!(db
        .query("MATCH (t:Thread) RETURN regexp_replace(t.source, '[', '_', 'g') AS value")
        .is_err());
    let query =
        "MATCH (t:Thread) RETURN regexp_replace(t.source, $pattern, $replacement, 'g') AS value";
    assert!(db
        .query_with_params(
            query,
            &params(&[("pattern", text("[")), ("replacement", text("_"))])
        )
        .is_err());
    assert!(db
        .query_with_params(
            query,
            &params(&[("pattern", text(" ")), ("replacement", text("_"))])
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn mem_9338_regex_cache_rebinds_pattern_and_replacement_without_retaining_programs() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {source:'A B'})").unwrap();
    let query =
        "MATCH (t:Thread) RETURN regexp_replace(t.source, $pattern, $replacement, 'g') AS value";
    for (pattern, replacement, expected) in
        [(" ", "_", "A_B"), ("[AB]", "x", "x x"), ("B", "β", "A β")]
    {
        let parameters = params(&[
            ("pattern", text(pattern)),
            ("replacement", text(replacement)),
        ]);
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows[0]["value"],
            text(expected)
        );
    }
    assert!(db
        .query_with_params(
            query,
            &params(&[("pattern", text("[")), ("replacement", text("_"))])
        )
        .is_err());
    assert_eq!(
        db.query_with_params(
            query,
            &params(&[("pattern", text(" ")), ("replacement", text("_"))])
        )
        .unwrap()
        .rows[0]["value"],
        text("A_B")
    );
    let report = db
        .explain_query_with_params(
            query,
            &params(&[("pattern", text(" ")), ("replacement", text("_"))]),
        )
        .unwrap();
    assert_eq!(report.plan_cache_lookup, PlanCacheLookup::Hit);
    db.query("MATCH (t:Thread) SET t.pattern='[AB]'").unwrap();
    assert_eq!(
        db.query("MATCH (t:Thread) RETURN regexp_replace(t.source,t.pattern,'x','g') AS value")
            .unwrap()
            .rows[0]["value"],
        text("x x")
    );
    db.query("MATCH (t:Thread) SET t.pattern='['").unwrap();
    assert!(db
        .query("MATCH (t:Thread) RETURN regexp_replace(t.source,t.pattern,'x','g') AS value")
        .is_err());
    let literal = "MATCH (t:Thread) RETURN regexp_replace(t.source, ' ', '_', 'g') AS value";
    assert_eq!(db.query(literal).unwrap().rows[0]["value"], text("A_B"));
    assert_eq!(
        db.explain_query(literal).unwrap().plan_cache_lookup,
        PlanCacheLookup::Hit
    );
}

#[test]
fn mem_9338_consecutive_set_items_share_the_target_row_budget_and_keep_snapshot_aliases() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(2).unwrap();
    let mut db = fixture(config);
    db.query("MATCH (a:Memory {id:'old'}), (b:Memory {id:'new'}) SET a.x=1, a.y=2, a.z=3, b.x=4")
        .unwrap();
    let row = db
        .query("MATCH (a:Memory {id:'old'}) RETURN a.x AS x,a.y AS y,a.z AS z")
        .unwrap()
        .rows
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        row,
        params(&[
            ("x", Value::Int(1)),
            ("y", Value::Int(2)),
            ("z", Value::Int(3))
        ])
    );
    db.query("MATCH (a:Memory {id:'old'}), (b:Memory {id:'old'}) SET a.id='changed', a.x=5, b.x=6")
        .unwrap();
    assert_eq!(
        db.query("MATCH (a:Memory {id:'changed'}) RETURN a.x AS x")
            .unwrap()
            .rows[0]["x"],
        Value::Int(6)
    );
    assert!(db
        .query("MATCH (a:Memory {id:'old'}) RETURN a.id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn mem_9338_grouped_duplicate_set_properties_preserve_operation_limits_atomically() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(2).unwrap();
    config.mutation_limits.max_operations = std::num::NonZeroUsize::new(3).unwrap();
    let mut db = fixture(config);
    let read =
        "MATCH (m:Memory) RETURN m.id AS id,m.x AS x,m.y AS y,m.is_latest AS latest ORDER BY id";
    let before = db.query(read).unwrap().rows;
    let epoch = db.statistics().unwrap().computed_at_commit_epoch;
    let error = db
        .query("MATCH (a:Memory {id:'old'}), (b:Memory {id:'new'}) SET a.x=1, a.x=2, a.y=3, b.x=4")
        .unwrap_err();
    assert!(
        error.to_string().contains("max_mutation_operations"),
        "{error}"
    );
    assert_eq!(db.query(read).unwrap().rows, before);
    assert_eq!(db.statistics().unwrap().computed_at_commit_epoch, epoch);
}
