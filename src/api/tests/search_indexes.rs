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
    for ((_, actual), (_, expected)) in actual.iter().zip(&expected) {
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
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
        ("EXPLAIN SELECT id FROM text_search('events_text', 'x', top_k => 3)", "text_search"),
    ] {
        let error = db.query_sql(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
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
            "INSERT INTO events_text (__hawdb_search_term) VALUES ('x')",
            "read-only",
        ),
        ("DROP MATERIALIZED VIEW events_text", "DROP SEARCH INDEX"),
        ("DROP SEARCH INDEX events", "unknown search index"),
        (
            "CREATE INDEX on_postings ON events_text (__hawdb_search_term)",
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
