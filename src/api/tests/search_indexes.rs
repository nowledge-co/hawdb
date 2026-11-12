// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Engine-owned SQL search indexes: DDL, synchronous maintenance, BM25
//! `text_search`, persistence, recovery and branch isolation, all through
//! SQL only (no host `SearchIndex` or projection mapper).

use super::*;
use hawdb_storage::relational::SearchIndexAnalyzer;

const SCHEMA: &str =
    "CREATE TABLE events (id BIGINT PRIMARY KEY, stream TEXT NOT NULL, seq BIGINT NOT NULL, text TEXT)";
const INDEX: &str =
    "CREATE SEARCH INDEX events_text ON events (text) WITH (analyzer = 'cjk_ngram')";
const SEARCH: &str =
    "SELECT id, score FROM text_search('events_text', $1, top_k => $2) AS e ORDER BY score DESC, id";

fn insert(db: &mut Database, id: i64, stream: &str, text: Option<&str>) {
    db.query_sql_with_params(
        "INSERT INTO events (id, stream, seq, text) VALUES ($1, $2, $3, $4)",
        &[
            Value::Int(id),
            Value::String(stream.into()),
            Value::Int(id * 10),
            text.map_or(Value::Null, |text| Value::String(text.into())),
        ],
    )
    .unwrap();
}

fn search(db: &mut Database, query: &str, top_k: i64) -> Vec<(i64, f64)> {
    db.query_sql_with_params(SEARCH, &[Value::String(query.into()), Value::Int(top_k)])
        .unwrap()
        .rows
        .iter()
        .map(|row| {
            let (Value::Int(id), Value::Float(score)) = (&row["id"], &row["score"]) else {
                panic!("unexpected text_search row {row:?}");
            };
            (*id, *score)
        })
        .collect()
}

fn ids(results: &[(i64, f64)]) -> Vec<i64> {
    results.iter().map(|(id, _)| *id).collect()
}

/// Brute-force BM25 over `SELECT * FROM events` with the same analyzer and
/// constants. Documents with no tokens are not part of the corpus.
fn oracle(db: &mut Database, query: &str, top_k: usize) -> Vec<(i64, f64)> {
    let analyzer = SearchIndexAnalyzer::CjkNgram;
    let rows = db.query_sql("SELECT id, text FROM events").unwrap();
    let documents = rows
        .rows
        .iter()
        .filter_map(|row| {
            let Value::String(text) = &row["text"] else {
                return None;
            };
            let tokens = analyzer.index_tokens(text);
            let Value::Int(id) = row["id"] else {
                unreachable!()
            };
            (!tokens.is_empty()).then_some((id, tokens))
        })
        .collect::<Vec<_>>();
    if documents.is_empty() {
        return Vec::new();
    }
    let n = documents.len() as f64;
    let average = documents
        .iter()
        .map(|(_, tokens)| tokens.len())
        .sum::<usize>() as f64
        / n;
    let terms = analyzer.query_terms(query);
    let mut scores = BTreeMap::<i64, f64>::new();
    for term in &terms {
        let containing = documents
            .iter()
            .filter(|(_, tokens)| tokens.contains(term))
            .count() as f64;
        if containing == 0.0 {
            continue;
        }
        let idf = (1.0 + (n - containing + 0.5) / (containing + 0.5)).ln();
        for (id, tokens) in &documents {
            let tf = tokens.iter().filter(|token| *token == term).count() as f64;
            if tf > 0.0 {
                let norm = 1.2 * (1.0 - 0.75 + 0.75 * tokens.len() as f64 / average);
                *scores.entry(*id).or_insert(0.0) += idf * tf * 2.2 / (tf + norm);
            }
        }
    }
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    // Single-column BIGINT keys encode in numeric order for the ids used here.
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    ranked.truncate(top_k);
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    ranked
}

fn assert_matches_oracle(db: &mut Database, query: &str) {
    let actual = search(db, query, 50);
    let expected = oracle(db, query, 50);
    assert_eq!(ids(&actual), ids(&expected), "query {query}");
    // The index evaluates the same floating-point operations in the same
    // term order as the oracle, so scores are bit-for-bit equal.
    for ((_, actual), (_, expected)) in actual.iter().zip(&expected) {
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "{actual} != {expected}"
        );
    }
}

const QUERIES: &[&str] = &[
    "hawdb search",
    "bm25 index",
    "数据库",
    "搜索",
    "库",
    "release notes",
    "the",
    "东京タワー",
    "missing-term",
];

fn corpus(db: &mut Database) {
    db.query_sql(SCHEMA).unwrap();
    corpus_rows(db);
}

fn corpus_rows(db: &mut Database) {
    for (id, stream, text) in [
        (
            1,
            "a",
            Some("HawDB search index design: BM25 over SQL rows"),
        ),
        (2, "a", Some("我们需要一个支持中文搜索的数据库")),
        (
            3,
            "b",
            Some("Release notes for the BM25 index and the search API"),
        ),
        (4, "b", Some("数据库的全文搜索：BM25 与中文分词")),
        (5, "c", None),
        (6, "c", Some("东京タワー and 서울 are cities")),
        (7, "a", Some("the the the the the")),
        (8, "b", Some("...")),
    ] {
        insert(db, id, stream, text);
    }
}

#[test]
fn search_index_ranks_latin_and_cjk_text_exactly() {
    let mut db = Database::new();
    corpus(&mut db);
    db.query_sql(INDEX).unwrap();
    for query in QUERIES {
        assert_matches_oracle(&mut db, query);
    }
    let latin = search(&mut db, "BM25 search", 10);
    assert_eq!(ids(&latin)[..2], [1, 3]);
    assert!(latin.windows(2).all(|pair| pair[0].1 >= pair[1].1));
    assert!(latin.iter().all(|(_, score)| *score > 0.0));
    let mut chinese = ids(&search(&mut db, "数据库", 10));
    chinese.sort_unstable();
    assert_eq!(chinese, [2, 4]);
    assert_eq!(ids(&search(&mut db, "中文搜索", 1)).len(), 1);
    assert!(search(&mut db, "missing-term", 10).is_empty());
    assert!(search(&mut db, "  ", 10).is_empty());
}

#[test]
fn search_index_tracks_inserts_updates_deletes_and_backfill() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    assert!(search(&mut db, "hawdb", 5).is_empty());
    corpus_rows(&mut db);
    for query in QUERIES {
        assert_matches_oracle(&mut db, query);
    }
    for (statement, parameters) in [
        (
            "UPDATE events SET text = $1 WHERE id = 1",
            vec![Value::String("rewritten: lexical recall over the event log".into())],
        ),
        ("UPDATE events SET text = NULL WHERE id = 2", vec![]),
        (
            "UPDATE events SET text = $1 WHERE id = 5",
            vec![Value::String("新的数据库记录 hawdb".into())],
        ),
        ("UPDATE events SET id = 50 WHERE id = 3", vec![]),
        ("DELETE FROM events WHERE id = 4", vec![]),
        (
            "INSERT INTO events (id, stream, seq, text) VALUES (9, 'a', 90, $1) ON CONFLICT (id) DO UPDATE SET text = EXCLUDED.text",
            vec![Value::String("search search search".into())],
        ),
        ("UPDATE events SET stream = 'z' WHERE id = 6", vec![]),
    ] {
        db.query_sql_with_params(statement, &parameters).unwrap();
        for query in QUERIES.iter().chain(&["lexical recall", "hawdb"]) {
            assert_matches_oracle(&mut db, query);
        }
    }
    assert!(!ids(&search(&mut db, "hawdb", 10)).contains(&1));
    assert_eq!(ids(&search(&mut db, "lexical", 10)), [1]);
    assert!(ids(&search(&mut db, "数据库", 10)).contains(&5));
    assert!(!ids(&search(&mut db, "数据库", 10)).contains(&4));
    assert_eq!(ids(&search(&mut db, "release notes", 10)), [50]);
    // A backfilled index answers exactly like the incrementally maintained one.
    let before = QUERIES
        .iter()
        .map(|query| search(&mut db, query, 50))
        .collect::<Vec<_>>();
    db.query_sql("DROP SEARCH INDEX events_text").unwrap();
    assert!(db
        .query_sql_with_params(SEARCH, &[Value::String("hawdb".into()), Value::Int(1)])
        .is_err());
    db.query_sql(INDEX).unwrap();
    let after = QUERIES
        .iter()
        .map(|query| search(&mut db, query, 50))
        .collect::<Vec<_>>();
    assert_eq!(before, after);
    db.query_sql("DELETE FROM events WHERE id > 0").unwrap();
    for query in QUERIES {
        assert!(search(&mut db, query, 10).is_empty());
    }
}

#[test]
fn search_results_filter_before_top_k_and_compose_with_sql() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    for id in 0..40 {
        let stream = if id % 4 == 0 { "rare" } else { "common" };
        let text = if id % 4 == 0 {
            "deploy log".to_string()
        } else {
            format!("deploy deploy deploy failed step {id}")
        };
        insert(&mut db, id, stream, Some(&text));
    }
    // The ten best "deploy" rows are all in "common"; filtering happens before
    // top_k, so the "rare" stream still yields its own best k rows.
    let rows = db
        .query_sql_with_params(
            "SELECT e.seq, e.stream, score FROM text_search('events_text', $1, top_k => 3) AS e \
             WHERE e.stream = $2 ORDER BY e.seq DESC",
            &[Value::String("deploy".into()), Value::String("rare".into())],
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert!(rows
        .rows
        .iter()
        .all(|row| row["stream"] == Value::String("rare".into())));
    let seqs = rows
        .rows
        .iter()
        .map(|row| row["seq"].clone())
        .collect::<Vec<_>>();
    // All rare rows score equally; ties keep primary-key order (0, 4, 8), then
    // the outer ORDER BY sorts by seq.
    assert_eq!(seqs, [Value::Int(80), Value::Int(40), Value::Int(0)]);
    // Wildcards expose the source columns and the virtual score.
    let all = db
        .query_sql_with_params(
            "SELECT * FROM text_search('events_text', $1, top_k => 2)",
            &[Value::String("failed".into())],
        )
        .unwrap();
    assert_eq!(all.rows.len(), 2);
    assert_eq!(
        all.rows.schema().columns(),
        ["id", "stream", "seq", "text", "score"].map(String::from)
    );
    // LIMIT/OFFSET page the top_k window; aggregation over it is refused.
    let page = db
        .query_sql_with_params(
            "SELECT id FROM text_search('events_text', 'deploy', top_k => 10) ORDER BY score DESC, id LIMIT 2 OFFSET 1",
            &[],
        )
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    for (sql, message) in [
        ("SELECT id FROM text_search('events_text', 'x', top_k => 0)", "top_k"),
        ("SELECT id FROM text_search('events_text', 'x')", "top_k"),
        ("SELECT id FROM text_search('events_text', 1, top_k => 3)", "query must be a string"),
        ("SELECT id FROM text_search('nope', 'x', top_k => 3)", "unknown search index"),
        ("SELECT id FROM text_search('events', 'x', top_k => 3)", "not a search index"),
        ("SELECT COUNT(*) FROM text_search('events_text', 'x', top_k => 3)", "single source"),
        (
            "SELECT e.id FROM text_search('events_text', 'x', top_k => 3) AS e JOIN events o ON o.id = e.id",
            "single source",
        ),
    ] {
        let error = db.query_sql(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
    let plan = db
        .query_sql("EXPLAIN SELECT id FROM text_search('events_text', 'deploy', top_k => 3)")
        .unwrap();
    assert!(plan.rows.iter().any(|row| {
        matches!(row.get("access object"), Some(Value::String(object)) if object.contains("events_text"))
            && matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("row_runtime_path=not_executed"))
    }));
    let error = db
        .query_sql("EXPLAIN SELECT absent FROM text_search('events_text', 'deploy', top_k => 3)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("absent"), "{error}");
    let profile = db
        .query_sql(
            "EXPLAIN ANALYZE SELECT id FROM text_search('events_text', 'deploy', top_k => 3)",
        )
        .unwrap();
    assert!(profile.rows.iter().any(|row| {
        matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("postings_visited="))
    }));
}

#[test]
fn search_index_ddl_and_catalog_guards() {
    let mut db = Database::new();
    corpus(&mut db);
    db.query_sql(INDEX).unwrap();
    db.query_sql("CREATE SEARCH INDEX IF NOT EXISTS events_text ON events (text)")
        .unwrap();
    for (sql, message) in [
        (INDEX, "already exists"),
        ("CREATE SEARCH INDEX bad ON events (seq)", "must be TEXT"),
        (
            "CREATE SEARCH INDEX bad ON events (nope)",
            "unknown materialized view input column",
        ),
        (
            "CREATE SEARCH INDEX bad ON nope (text)",
            "unknown relational table",
        ),
        (
            "CREATE SEARCH INDEX bad ON events (text) WITH (analyzer = 'jieba')",
            "unsupported search index analyzer",
        ),
        (
            "CREATE SEARCH INDEX bad ON events (text) WITH (stemmer = 'x')",
            "unsupported",
        ),
        ("SELECT * FROM events_text", "search index"),
        (
            "INSERT INTO events_text (__hawdb_search_kind) VALUES (0)",
            "read-only",
        ),
        ("DROP MATERIALIZED VIEW events_text", "DROP SEARCH INDEX"),
        ("DROP SEARCH INDEX events", "unknown search index"),
        (
            "CREATE INDEX on_postings ON events_text (__hawdb_search_name)",
            "visible",
        ),
    ] {
        let error = db.query_sql(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
    let tables = db
        .query_sql("SELECT table_name FROM information_schema.tables ORDER BY table_name")
        .unwrap();
    assert_eq!(tables.rows.len(), 1);
    let indexes = db
        .query_sql("SELECT tablename, indexdef FROM pg_indexes WHERE indexname = 'events_text'")
        .unwrap();
    assert_eq!(indexes.rows[0]["tablename"], Value::String("events".into()));
    assert_eq!(
        indexes.rows[0]["indexdef"],
        Value::String(
            "CREATE SEARCH INDEX \"events_text\" ON public.\"events\" (\"text\") WITH (analyzer = 'cjk_ngram')"
                .into()
        )
    );
    // A source column named like the virtual output is refused at query time.
    db.query_sql("CREATE TABLE scored (id BIGINT PRIMARY KEY, score TEXT)")
        .unwrap();
    db.query_sql("CREATE SEARCH INDEX scored_text ON scored (score)")
        .unwrap();
    db.query_sql("INSERT INTO scored (id, score) VALUES (1, 'high')")
        .unwrap();
    assert!(db
        .query_sql("SELECT id FROM text_search('scored_text', 'high', top_k => 1)")
        .unwrap_err()
        .to_string()
        .contains("reserves the score"));
    db.query_sql("DROP SEARCH INDEX IF EXISTS missing").unwrap();
    db.query_sql("DROP SEARCH INDEX events_text").unwrap();
    db.query_sql("DROP SEARCH INDEX scored_text").unwrap();
    db.query_sql("DROP SEARCH INDEX IF EXISTS events_text")
        .unwrap();
    assert!(db.query_sql("SELECT * FROM events_text").is_err());
}

#[test]
fn search_index_transactions_read_their_writes_and_rollback() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    insert(&mut db, 1, "a", Some("committed baseline"));
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.query_sql(
            "INSERT INTO events (id, stream, seq, text) VALUES (2, 'a', 20, 'staged words')",
        )
        .unwrap();
        let staged = tx
            .query_sql("SELECT id FROM text_search('events_text', 'staged', top_k => 5)")
            .unwrap();
        assert_eq!(staged.rows.len(), 1);
        // Dropped without commit: rolled back.
    }
    assert!(search(&mut db, "staged", 5).is_empty());
    let mut tx = db.begin_transaction().unwrap();
    tx.query_sql("INSERT INTO events (id, stream, seq, text) VALUES (2, 'a', 20, 'staged words')")
        .unwrap();
    tx.query_sql("UPDATE events SET text = 'changed baseline' WHERE id = 1")
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(ids(&search(&mut db, "staged", 5)), [2]);
    assert!(search(&mut db, "committed", 5).is_empty());
    assert_matches_oracle(&mut db, "baseline words");
}

#[test]
fn search_index_persists_through_wal_replay_checkpoint_and_reopen() {
    let path = unique_test_dir("search_index_reopen");
    let expected;
    {
        let mut db = Database::open(&path).unwrap();
        corpus(&mut db);
        db.query_sql(INDEX).unwrap();
        expected = QUERIES
            .iter()
            .map(|query| search(&mut db, query, 50))
            .collect::<Vec<_>>();
    }
    for checkpoint in [false, true] {
        let mut db = Database::open(&path).unwrap();
        let actual = QUERIES
            .iter()
            .map(|query| search(&mut db, query, 50))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "checkpointed before reopen: {checkpoint}");
        if !checkpoint {
            db.checkpoint().unwrap();
        }
    }
    {
        // Maintenance continues on a reopened handle, then survives another
        // WAL-only reopen on top of the checkpoint.
        let mut db = Database::open(&path).unwrap();
        insert(&mut db, 20, "a", Some("after reopen: 重新打开以后的数据库"));
        db.query_sql("DELETE FROM events WHERE id = 1").unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    for query in QUERIES.iter().chain(&["reopen", "重新打开"]) {
        assert_matches_oracle(&mut db, query);
    }
    assert_eq!(ids(&search(&mut db, "重新打开", 5)), [20]);
    assert!(!ids(&search(&mut db, "hawdb", 5)).contains(&1));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_index_torn_wal_tail_recovers_only_complete_transactions() {
    let path = unique_test_dir("search_index_torn_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql(SCHEMA).unwrap();
        db.query_sql(INDEX).unwrap();
        insert(&mut db, 1, "a", Some("durable first entry"));
        db.checkpoint().unwrap();
        insert(&mut db, 2, "a", Some("durable second entry"));
        let mut tx = db.begin_transaction().unwrap();
        tx.query_sql(
            "INSERT INTO events (id, stream, seq, text) VALUES (3, 'a', 30, 'torn entry')",
        )
        .unwrap();
        tx.query_sql("UPDATE events SET text = 'torn rewrite' WHERE id = 1")
            .unwrap();
        tx.commit().unwrap();
    }
    let wal = active_wal_path(&path);
    let complete = std::fs::read(&wal).unwrap();
    std::fs::write(&wal, &complete[..complete.len() - 5]).unwrap();
    // Strict recovery refuses the torn tail and keeps the evidence.
    assert!(Database::open_with_config(
        &path,
        crate::DatabaseConfig {
            recovery_mode: crate::RecoveryMode::Strict,
            ..Default::default()
        },
    )
    .and_then(|mut db| db.query_sql("SELECT * FROM events"))
    .is_err());
    // The default repair drops the incomplete transaction as a whole: neither
    // its rows nor its postings exist, and every committed row is searchable.
    let mut db = Database::open(&path).unwrap();
    let rows = db.query_sql("SELECT id FROM events ORDER BY id").unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert!(search(&mut db, "torn", 5).is_empty());
    assert_eq!(ids(&search(&mut db, "durable", 5)).len(), 2);
    assert_eq!(ids(&search(&mut db, "first", 5)), [1]);
    for query in ["durable entry", "torn rewrite", "second"] {
        assert_matches_oracle(&mut db, query);
    }
    insert(&mut db, 3, "a", Some("rewritten after repair"));
    assert_eq!(ids(&search(&mut db, "repair", 5)), [3]);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_index_is_isolated_per_branch() {
    let path = unique_test_dir("search_index_branch");
    let mut db = Database::open(&path).unwrap();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    insert(&mut db, 1, "a", Some("shared history"));
    let epoch = db.commit_epoch().unwrap();
    db.query_sql_with_params(
        "CREATE BRANCH side FROM main AT REVISION $1 REQUEST KEY 'search-branch'",
        &[Value::Int(i64::try_from(epoch).unwrap())],
    )
    .unwrap();
    db.query_sql("USE BRANCH side").unwrap();
    assert_eq!(ids(&search(&mut db, "shared", 5)), [1]);
    insert(&mut db, 2, "a", Some("only on the side branch"));
    assert_eq!(ids(&search(&mut db, "side", 5)), [2]);
    db.query_sql("USE BRANCH main").unwrap();
    assert!(search(&mut db, "side", 5).is_empty());
    insert(&mut db, 3, "a", Some("only on main"));
    assert_matches_oracle(&mut db, "only shared");
    db.query_sql("USE BRANCH side").unwrap();
    assert!(search(&mut db, "main", 5).is_empty());
    assert_matches_oracle(&mut db, "only shared side");
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_index_concurrent_writers_publish_complete_postings() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    let db = db.into_concurrent();
    std::thread::scope(|scope| {
        for worker in 0..4i64 {
            let db = &db;
            scope.spawn(move || {
                for offset in 0..8 {
                    let id = worker * 100 + offset;
                    db.query_sql_with_params(
                        "INSERT INTO events (id, stream, seq, text) VALUES ($1, 'w', $1, $2)",
                        &[
                            Value::Int(id),
                            Value::String(format!("worker{worker} 并发 entry")),
                        ],
                    )
                    .unwrap();
                    let seen = db
                        .query_sql_with_params(
                            "SELECT id FROM text_search('events_text', $1, top_k => 100)",
                            &[Value::String(format!("worker{worker}"))],
                        )
                        .unwrap();
                    assert_eq!(seen.rows.len(), usize::try_from(offset + 1).unwrap());
                }
            });
        }
    });
    let all = db
        .query_sql("SELECT id FROM text_search('events_text', '并发', top_k => 100)")
        .unwrap();
    assert_eq!(all.rows.len(), 32);
}

#[test]
fn search_index_persistent_index_and_residency_modes_reopen_consistently() {
    use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
    for (index_mode, residency) in [
        (RelationalIndexMode::Shadow, StorageResidencyMode::Auto),
        (RelationalIndexMode::DemandPaged, StorageResidencyMode::Auto),
        (
            RelationalIndexMode::Materialized,
            StorageResidencyMode::OutOfCore,
        ),
    ] {
        let path = unique_test_dir("search_index_modes");
        let config = DatabaseConfig {
            relational_index_mode: index_mode,
            storage_residency_mode: residency,
            ..DatabaseConfig::default()
        };
        {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            corpus(&mut db);
            db.query_sql(INDEX).unwrap();
            db.checkpoint().unwrap();
            db.query_sql("UPDATE events SET text = 'checkpoint tail rewrite' WHERE id = 3")
                .unwrap();
        }
        let mut db = Database::open_with_config(&path, config).unwrap();
        for query in QUERIES.iter().chain(&["checkpoint tail"]) {
            assert_matches_oracle(&mut db, query);
        }
        assert_eq!(
            ids(&search(&mut db, "rewrite", 5)),
            [3],
            "{index_mode:?} {residency:?}"
        );
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
    // Authoritative relational indexes require out-of-core rows, which the
    // synchronous derived-relation kernel does not maintain: refuse, never
    // serve stale postings.
    let path = unique_test_dir("search_index_authoritative");
    {
        let mut db = Database::open(&path).unwrap();
        corpus(&mut db);
        db.query_sql(INDEX).unwrap();
        db.checkpoint().unwrap();
    }
    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..DatabaseConfig::default()
        },
    )
    .and_then(|mut db| db.query_sql("SELECT id FROM events"))
    .unwrap_err();
    assert!(error.to_string().contains("materialized views"), "{error}");
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_index_backfill_and_writes_are_admitted_atomically() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    // 2,600 documents x 100 distinct terms exceed the default 250,000-row
    // transaction work budget for a backfill.
    let mut tx = db.begin_transaction().unwrap();
    for id in 0..2_600i64 {
        let text = (0..100)
            .map(|term| format!("t{id}x{term}"))
            .collect::<Vec<_>>()
            .join(" ");
        tx.query_sql_with_params(
            "INSERT INTO events (id, stream, seq, text) VALUES ($1, 's', $1, $2)",
            &[Value::Int(id), Value::String(text)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let error = db.query_sql(INDEX).unwrap_err().to_string();
    assert!(error.contains("budget"), "{error}");
    assert!(db
        .query_sql("SELECT id FROM text_search('events_text', 't1x1', top_k => 1)")
        .is_err());
    db.query_sql("DELETE FROM events WHERE id >= 100").unwrap();
    db.query_sql(INDEX).unwrap();
    assert_eq!(ids(&search(&mut db, "t7x42", 5)), [7]);
    // One oversized document is refused with its source row; nothing commits.
    let huge = (0..300_000)
        .map(|term| format!("w{term}"))
        .collect::<Vec<_>>()
        .join(" ");
    let error = db
        .query_sql_with_params(
            "INSERT INTO events (id, stream, seq, text) VALUES (9999, 's', 1, $1)",
            &[Value::String(huge)],
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("budget"), "{error}");
    assert!(db
        .query_sql("SELECT id FROM events WHERE id = 9999")
        .unwrap()
        .rows
        .is_empty());
    assert!(search(&mut db, "w5", 5).is_empty());
}

#[test]
fn search_index_refuses_large_cjk_document_before_expanding_it() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    insert(&mut db, 1, "s", Some("数据库"));
    // 800,000 pseudo-random ideographs (2.4 MB of TEXT) analyze into about
    // 1.6 million distinct unigrams and bigrams. Their admission exceeds the
    // transaction payload budget part-way through analysis, so the row is
    // refused without materializing every term.
    let mut state = 0x2545_f491_u32;
    let text = (0..800_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            char::from_u32(0x4E00 + state % 0x5200).unwrap()
        })
        .collect::<String>();
    let error = db
        .query_sql_with_params(
            "INSERT INTO events (id, stream, seq, text) VALUES (2, 's', 2, $1)",
            &[Value::String(text)],
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("payload budget"), "{error}");
    assert!(db
        .query_sql("SELECT id FROM events WHERE id = 2")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(ids(&search(&mut db, "数据库", 5)), [1]);
    assert_matches_oracle(&mut db, "数据库");
}

#[test]
fn search_index_coexists_with_materialized_views_on_one_source() {
    let mut db = Database::new();
    corpus(&mut db);
    db.query_sql(INDEX).unwrap();
    db.query_sql(
        "CREATE MATERIALIZED VIEW per_stream AS SELECT stream, COUNT(*) AS n FROM events GROUP BY stream",
    )
    .unwrap();
    insert(&mut db, 30, "a", Some("view and index together"));
    db.query_sql("DELETE FROM events WHERE id = 1").unwrap();
    assert_eq!(ids(&search(&mut db, "together", 5)), [30]);
    let counts = db
        .query_sql("SELECT n FROM per_stream WHERE stream = 'a'")
        .unwrap();
    assert_eq!(counts.rows[0]["n"], Value::Int(3));
    for query in QUERIES {
        assert_matches_oracle(&mut db, query);
    }
}

#[test]
fn search_index_analyzes_overflowed_text_and_survives_schema_extension() {
    let path = unique_test_dir("search_index_overflow");
    let large = format!(
        "alphaword {} 长文本中间 {} omegaword",
        "filler ".repeat(30_000),
        "填充".repeat(20_000)
    );
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql(SCHEMA).unwrap();
        db.query_sql(INDEX).unwrap();
        insert(&mut db, 1, "a", Some(&large));
        insert(&mut db, 2, "a", Some("short omegaword note"));
        db.query_sql("ALTER TABLE events ADD COLUMN extra TEXT")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let mut omega = ids(&search(&mut db, "omegaword", 5));
    omega.sort_unstable();
    assert_eq!(omega, [1, 2]);
    assert_eq!(ids(&search(&mut db, "alphaword", 5)), [1]);
    assert_eq!(ids(&search(&mut db, "长文本", 5)), [1]);
    for query in ["omegaword filler", "填充", "note"] {
        assert_matches_oracle(&mut db, query);
    }
    db.query_sql("UPDATE events SET text = 'now short', extra = 'x' WHERE id = 1")
        .unwrap();
    assert!(search(&mut db, "alphaword", 5).is_empty());
    assert_eq!(ids(&search(&mut db, "short", 5)).len(), 2);
    assert_matches_oracle(&mut db, "short omegaword");
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

/// Hidden index rows by kind: statistics, documents, lengths, term
/// dictionaries and sealed posting blocks.
fn layout_rows(db: &Database) -> [usize; 5] {
    use hawdb_storage::relational::RelationalValue;
    let runtime = db.runtime.get().unwrap();
    let state = runtime.store.relational_state();
    let mut counts = [0; 5];
    for (key, _) in state.rows("events_text") {
        let (RelationalValue::BigInt(kind), RelationalValue::BigInt(slot)) = (&key.0[0], &key.0[2])
        else {
            panic!("unexpected search index key {key:?}");
        };
        let kind = match (*kind, *slot) {
            (3, i64::MAX) => 3,
            (3, _) => 4,
            (kind, _) => usize::try_from(kind).unwrap(),
        };
        counts[kind] += 1;
    }
    counts
}

fn row_data(db: &Database, key: &hawdb_storage::relational::RelationalKey) -> Option<Vec<u8>> {
    use hawdb_storage::relational::RelationalValue;
    let runtime = db.runtime.get().unwrap();
    let state = runtime.store.relational_state();
    let row = state.row("events_text", key)?;
    let RelationalValue::Bytea(data) = &row.values()[3] else {
        panic!("unexpected search index payload");
    };
    Some(data.clone())
}

fn next_ordinal(db: &Database) -> u64 {
    use hawdb_storage::relational::{search_index_statistics_key, SearchIndexStatistics};
    SearchIndexStatistics::decode(&row_data(db, &search_index_statistics_key()).unwrap())
        .unwrap()
        .next_ordinal
}

/// Posting counts of a term's sealed blocks in fence order, then its tail.
fn term_blocks(db: &Database, term: &str) -> (Vec<usize>, usize) {
    use hawdb_storage::relational::{
        decode_search_sealed_block, search_index_block_fence, search_index_dictionary_key,
        RelationalValue, SearchTermDictionary,
    };
    let Some(data) = row_data(db, &search_index_dictionary_key(term)) else {
        return (Vec::new(), 0);
    };
    let dictionary = SearchTermDictionary::decode(&data).unwrap();
    let runtime = db.runtime.get().unwrap();
    let state = runtime.store.relational_state();
    let mut sealed = Vec::new();
    for (key, row) in state.rows("events_text") {
        if key.0[0] != RelationalValue::BigInt(3)
            || key.0[1] != RelationalValue::Bytea(term.as_bytes().to_vec())
            || key.0[2] == RelationalValue::BigInt(i64::MAX)
        {
            continue;
        }
        let RelationalValue::Bytea(data) = &row.values()[3] else {
            panic!("unexpected sealed block payload");
        };
        let mut postings = Vec::new();
        decode_search_sealed_block(
            search_index_block_fence(&key.0[2]).unwrap(),
            data,
            &mut postings,
        )
        .unwrap();
        sealed.push(postings.len());
    }
    assert_eq!(sealed.len() as u64, dictionary.sealed_blocks, "{term}");
    assert_eq!(
        sealed.iter().sum::<usize>() + dictionary.tail.len(),
        usize::try_from(dictionary.document_frequency).unwrap(),
        "{term}"
    );
    (sealed, dictionary.tail.len())
}

#[test]
fn search_index_blocks_seal_split_and_route_older_changes_exactly() {
    use hawdb_storage::relational::{
        SEARCH_INDEX_BLOCK_POSTINGS as BLOCK, SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
    };
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    // "common" is in every document, "odd" in every other one and "sparse" in
    // every fourth; one INSERT per document seals tails one posting at a time.
    let total = 520i64;
    let text = |id: i64| {
        format!(
            "common doc{id}{}{}",
            if id % 2 == 1 { " odd" } else { "" },
            if id % 4 == 0 { " sparse" } else { "" }
        )
    };
    for id in 0..total {
        insert(&mut db, id, "s", Some(&text(id)));
    }
    let [statistics, documents, lengths, dictionaries, sealed] = layout_rows(&db);
    assert_eq!((statistics, documents), (1, 520));
    assert_eq!(
        lengths as u64,
        520u64.div_ceil(SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS)
    );
    assert_eq!(dictionaries, 520 + 3);
    assert_eq!(term_blocks(&db, "common"), (vec![BLOCK; 4], 8));
    assert_eq!(term_blocks(&db, "odd"), (vec![BLOCK; 2], 4));
    assert_eq!(term_blocks(&db, "sparse"), (vec![BLOCK], 2));
    assert_eq!(term_blocks(&db, "doc7"), (vec![], 1));
    assert_eq!(sealed, 7);
    let queries = [
        "common",
        "odd",
        "sparse",
        "doc7",
        "common odd",
        "doc127 doc128 doc511 doc512",
        "sparse odd doc0",
    ];
    for query in queries {
        assert_matches_oracle(&mut db, query);
    }
    // Older documents gain, change and lose terms inside sealed blocks, at
    // and around block boundaries.
    for (id, text) in [
        (0, "common common odd late"),
        (127, "sparse late"),
        (128, "common doc128 odd odd"),
        (255, "doc255"),
        (256, "late"),
        (511, "common sparse sparse"),
    ] {
        db.query_sql_with_params(
            "UPDATE events SET text = $1 WHERE id = $2",
            &[Value::String(text.into()), Value::Int(id)],
        )
        .unwrap();
        for query in queries.iter().chain(&["late"]) {
            assert_matches_oracle(&mut db, query);
        }
    }
    // One transaction adds "sparse" to hundreds of older documents that all
    // belong to its first sealed block: the block splits into full blocks.
    let mut tx = db.begin_transaction().unwrap();
    for id in (1..509).filter(|id| id % 4 != 0) {
        tx.query_sql_with_params(
            "UPDATE events SET text = $1 WHERE id = $2",
            &[
                Value::String(format!("common sparse doc{id}")),
                Value::Int(id),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let (blocks, _) = term_blocks(&db, "sparse");
    assert!(blocks.len() >= 4, "{blocks:?}");
    assert!(blocks.iter().all(|count| *count <= 2 * BLOCK), "{blocks:?}");
    for query in queries.iter().chain(&["late"]) {
        assert_matches_oracle(&mut db, query);
    }
    assert_eq!(next_ordinal(&db), 520, "updates keep their ordinals");
}

#[test]
fn search_index_one_document_updated_many_times_keeps_one_ordinal() {
    let mut db = Database::new();
    corpus(&mut db);
    db.query_sql(INDEX).unwrap();
    let before = layout_rows(&db);
    let ordinal = next_ordinal(&db);
    for revision in 0..300 {
        let text = format!(
            "revision{revision} {} hawdb",
            "search ".repeat(revision % 7 + 1)
        );
        db.query_sql_with_params(
            "UPDATE events SET text = $1 WHERE id = 1",
            &[Value::String(text)],
        )
        .unwrap();
        if revision % 50 == 0 {
            for query in QUERIES.iter().chain(&["revision0 search"]) {
                assert_matches_oracle(&mut db, query);
            }
        }
    }
    assert_eq!(next_ordinal(&db), ordinal);
    assert_eq!(ids(&search(&mut db, "revision299", 5)), [1]);
    assert!(search(&mut db, "revision298", 5).is_empty());
    // Earlier revisions' words left the dictionary; rows stay bounded.
    let after = layout_rows(&db);
    assert_eq!(after[..3], before[..3]);
    assert!(after[3] <= before[3] + 2, "{before:?} {after:?}");
    for query in QUERIES {
        assert_matches_oracle(&mut db, query);
    }
}

#[test]
fn search_index_delete_churn_compacts_blocks_and_empties_the_relation() {
    use hawdb_storage::relational::SEARCH_INDEX_BLOCK_POSTINGS as BLOCK;
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    for id in 0..(4 * BLOCK as i64) {
        insert(&mut db, id, "s", Some(&format!("common doc{id} 共同")));
    }
    assert_eq!(term_blocks(&db, "common"), (vec![BLOCK; 4], 0));
    // Delete seven of every eight documents, one statement each.
    for id in (0..(4 * BLOCK as i64)).filter(|id| id % 8 != 0) {
        db.query_sql_with_params("DELETE FROM events WHERE id = $1", &[Value::Int(id)])
            .unwrap();
    }
    let (blocks, tail) = term_blocks(&db, "common");
    assert_eq!(blocks.iter().sum::<usize>() + tail, BLOCK / 2);
    assert!(blocks.len() <= 2, "{blocks:?}");
    for query in ["common", "共同", "doc8 doc16", "doc9"] {
        assert_matches_oracle(&mut db, query);
    }
    // Re-inserting appends new ordinals after the compacted blocks.
    for id in 1000..1010 {
        insert(&mut db, id, "s", Some("common 共同 again"));
    }
    for query in ["common again", "共同"] {
        assert_matches_oracle(&mut db, query);
    }
    db.query_sql("DELETE FROM events WHERE id >= 0").unwrap();
    assert_eq!(layout_rows(&db), [0; 5]);
    assert!(search(&mut db, "common", 5).is_empty());
    insert(&mut db, 1, "s", Some("fresh start"));
    assert_eq!(next_ordinal(&db), 1);
    assert_matches_oracle(&mut db, "fresh");
}
