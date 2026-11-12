// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The `Bm25IndexScan` access path: exact Block-Max WAND top-k against the
//! brute-force oracle after shortening churn (and, manually, on generated
//! corpora and churn), block skipping, filters
//! by bounded over-fetch, LIMIT pushdown, EXPLAIN, and the narrow
//! concurrency footprint of a search index source.

use super::*;
use crate::api::ConcurrentTransactionOptions;
use crate::TransactionIsolation;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

const ZH: [&str; 4] = ["数据库", "全文搜索", "任务完成", "上下文"];

/// Zipf-like words, some repeated, varying lengths, sometimes Chinese.
fn random_text(rng: &mut Rng) -> String {
    let mut words = Vec::new();
    for _ in 0..1 + rng.below(24) {
        let spread = 1 + rng.below(30);
        let word = rng.below(spread);
        let repeats = if rng.below(10) == 0 {
            1 + rng.below(4)
        } else {
            1
        };
        for _ in 0..repeats {
            words.push(format!("t{word}"));
        }
    }
    if rng.below(3) == 0 {
        words.push(ZH[rng.below(ZH.len() as u64) as usize].to_owned());
    }
    words.join(" ")
}

fn random_query(rng: &mut Rng) -> String {
    match rng.below(6) {
        0 => ZH[rng.below(ZH.len() as u64) as usize].to_owned(),
        1 => "absent".to_owned(),
        _ => (0..1 + rng.below(3))
            .map(|_| {
                let spread = 1 + rng.below(30);
                format!("t{}", rng.below(spread))
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn insert_rows(db: &mut Database, rows: &[(i64, String, String)]) {
    for chunk in rows.chunks(50) {
        let values = (0..chunk.len())
            .map(|row| {
                let base = row * 3;
                format!(
                    "(${}, ${}, ${}, ${})",
                    base + 1,
                    base + 2,
                    base + 1,
                    base + 3
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let parameters = chunk
            .iter()
            .flat_map(|(id, stream, text)| {
                [
                    Value::Int(*id),
                    Value::String(stream.clone()),
                    Value::String(text.clone()),
                ]
            })
            .collect::<Vec<_>>();
        db.query_sql_with_params(
            &format!("INSERT INTO events (id, stream, seq, text) VALUES {values}"),
            &parameters,
        )
        .unwrap();
    }
}

fn filtered_search(db: &mut Database, query: &str, top_k: i64, stream: &str) -> Vec<(i64, f64)> {
    db.query_sql_with_params(
        "SELECT id, score FROM text_search('events_text', $1, top_k => $2) AS e \
         WHERE e.stream = $3 ORDER BY score DESC, id",
        &[
            Value::String(query.into()),
            Value::Int(top_k),
            Value::String(stream.into()),
        ],
    )
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

fn assert_bit_exact(actual: &[(i64, f64)], expected: &[(i64, f64)], context: &str) {
    assert_eq!(ids(actual), ids(expected), "{context}");
    for ((_, actual), (_, expected)) in actual.iter().zip(expected) {
        assert_eq!(actual.to_bits(), expected.to_bits(), "{context}");
    }
}

/// `key=value` pairs of the `Bm25IndexScan` row of `EXPLAIN ANALYZE sql`.
fn scan_evidence(db: &mut Database, sql: &str, parameters: &[Value]) -> BTreeMap<String, String> {
    let rows = db
        .query_sql_with_params(&format!("EXPLAIN ANALYZE {sql}"), parameters)
        .unwrap()
        .rows;
    let scan = rows
        .iter()
        .find(|row| matches!(&row["id"], Value::String(id) if id.contains("Bm25IndexScan")))
        .expect("Bm25IndexScan row");
    let Value::String(info) = &scan["execution info"] else {
        panic!("no execution info: {scan:?}");
    };
    info.split(", ")
        .map(|pair| {
            let (key, value) = pair.split_once('=').expect("key=value");
            (key.to_owned(), value.to_owned())
        })
        .collect()
}

fn fraction(evidence: &BTreeMap<String, String>, key: &str) -> (u64, u64) {
    let (part, whole) = evidence[key].split_once('/').expect("part/whole");
    (part.parse().unwrap(), whole.parse().unwrap())
}

fn number(evidence: &BTreeMap<String, String>, key: &str) -> u64 {
    evidence[key].parse().unwrap()
}

/// A shortened document keeps some terms with their old frequencies, so its
/// blocks' length bounds must drop: otherwise Block-Max WAND would skip the
/// block that now holds the best (shortest) match.
#[test]
fn search_index_scan_stays_exact_after_documents_shorten() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    let filler = "w ".repeat(40);
    let rows = (0..600)
        .map(|id| {
            (
                id,
                format!("s{}", id % 4),
                format!("deploy failed {filler}"),
            )
        })
        .collect::<Vec<_>>();
    insert_rows(&mut db, &rows);
    let queries = ["deploy", "deploy failed", "failed"];
    for query in queries {
        assert_matches_oracle(&mut db, query);
    }
    // Late, middle and first-block documents shrink to their two terms.
    for id in [599, 300, 129, 3] {
        db.query_sql_with_params(
            "UPDATE events SET text = 'deploy failed' WHERE id = $1",
            &[Value::Int(id)],
        )
        .unwrap();
    }
    check_block_bounds(&db);
    for query in queries {
        for k in [1, 3, 4, 5] {
            assert_bit_exact(
                &search(&mut db, query, k),
                &oracle(&mut db, query, k as usize),
                &format!("{query:?} k={k}"),
            );
        }
        let mut expected = ranked_oracle(&mut db, query, Some("s1"));
        expected.truncate(2);
        assert_bit_exact(
            &filtered_search(&mut db, query, 2, "s1"),
            &expected,
            &format!("{query:?} in s1"),
        );
    }
    assert_eq!(ids(&search(&mut db, "deploy", 4)), [3, 129, 300, 599]);
}

/// The generated differential campaign: random corpora, queries, filters
/// and churn against the brute-force oracle. Manual only, not part of
/// ordinary unit runs or CI: `cargo test --locked -p hawdb --lib
/// search_index_scan_generated_campaign -- --ignored`. The failure message
/// names the corpus seed; `oracle_campaign(seed)` replays it.
#[test]
#[ignore = "explicit local differential campaign"]
fn search_index_scan_generated_campaign() {
    let mut seeds = Rng(0x2545_f491_4f6c_dd1d);
    for corpus in 0..4 {
        oracle_campaign(seeds.next(), corpus);
    }
}

fn oracle_campaign(seed: u64, corpus: i64) {
    let mut rng = Rng(seed | 1);
    {
        let mut db = Database::new();
        db.query_sql(SCHEMA).unwrap();
        db.query_sql(INDEX).unwrap();
        let documents = 300 + 120 * corpus;
        let rows = (0..documents)
            .map(|id| (id, format!("s{}", id % 4), random_text(&mut rng)))
            .collect::<Vec<_>>();
        insert_rows(&mut db, &rows);
        let mut next_id = documents;
        for phase in 0..3 {
            for _ in 0..12 {
                let query = random_query(&mut rng);
                let k = 1 + rng.below(40) as usize;
                let context =
                    format!("seed {seed:#x} corpus {corpus} phase {phase} query {query:?} k={k}");
                assert_bit_exact(
                    &search(&mut db, &query, k as i64),
                    &oracle(&mut db, &query, k),
                    &context,
                );
                let stream = format!("s{}", rng.below(4));
                let mut expected = ranked_oracle(&mut db, &query, Some(&stream));
                expected.truncate(k);
                assert_bit_exact(
                    &filtered_search(&mut db, &query, k as i64, &stream),
                    &expected,
                    &format!("{context} stream {stream}"),
                );
            }
            // Every skip entry still bounds its block.
            layout_rows(&db);
            // Churn: rewrites, shortened documents, deletes and inserts.
            for _ in 0..80 {
                let id = rng.below(next_id as u64) as i64;
                match rng.below(5) {
                    0 => {
                        db.query_sql_with_params(
                            "DELETE FROM events WHERE id = $1",
                            &[Value::Int(id)],
                        )
                        .unwrap();
                    }
                    1 => {
                        // Shorter text keeps some terms with their old
                        // frequencies: their blocks' length bounds must drop.
                        db.query_sql_with_params(
                            "UPDATE events SET text = $1 WHERE id = $2",
                            &[Value::String("t0 t1".into()), Value::Int(id)],
                        )
                        .unwrap();
                    }
                    2 => {
                        insert_rows(
                            &mut db,
                            &[(next_id, format!("s{}", next_id % 4), random_text(&mut rng))],
                        );
                        next_id += 1;
                    }
                    _ => {
                        db.query_sql_with_params(
                            "UPDATE events SET text = $1 WHERE id = $2",
                            &[Value::String(random_text(&mut rng)), Value::Int(id)],
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
}

/// 3,000 documents of varying length share one term; three repeat it, the
/// best early in the corpus. Once the best is retained, a one-row top-k
/// skips every later block that cannot beat it without reading it.
fn dense_corpus(db: &mut Database) {
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    let rows = (0..3_000)
        .map(|id| {
            let text = match id {
                40 => "deploy deploy deploy failed".to_owned(),
                2_200 | 2_900 => "deploy deploy failed".to_owned(),
                _ => {
                    let filler = (0..id % 9)
                        .map(|word| format!("f{}", (id + word) % 101))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!("deploy step w{} {filler}", id % 97)
                }
            };
            (id, format!("s{}", id % 20), text)
        })
        .collect::<Vec<_>>();
    insert_rows(db, &rows);
}

#[test]
fn search_index_scan_one_row_top_k_skips_most_blocks() {
    let mut db = Database::new();
    dense_corpus(&mut db);
    for (query, k) in [("deploy", 3), ("deploy failed", 3), ("failed", 2)] {
        assert_bit_exact(
            &search(&mut db, query, k),
            &oracle(&mut db, query, k as usize),
            query,
        );
    }
    for (query, k) in [("deploy", 1), ("deploy failed", 1)] {
        assert_bit_exact(
            &search(&mut db, query, k),
            &oracle(&mut db, query, k as usize),
            query,
        );
        let evidence = scan_evidence(
            &mut db,
            "SELECT id, score FROM text_search('events_text', $1, top_k => $2)",
            &[Value::String(query.into()), Value::Int(k)],
        );
        let (visited, total) = fraction(&evidence, "postings_visited");
        assert!(visited * 4 < total, "{query} k={k}: {evidence:?}");
        assert!(
            number(&evidence, "blocks_skipped") * 2 > number(&evidence, "blocks_total"),
            "{query} k={k}: {evidence:?}"
        );
        // Pruned by block bounds, or by term bounds once nothing left could
        // reach the floor.
        assert!(
            number(&evidence, "block_skips") > 0 || evidence["terminated_early"] == "true",
            "{evidence:?}"
        );
        assert_eq!(number(&evidence, "rounds"), 1);
    }
    // The top-1 is the one document repeating the term three times.
    assert_eq!(ids(&search(&mut db, "deploy", 1)), [40]);
}

#[test]
fn search_index_scan_filters_by_bounded_over_fetch() {
    let mut db = Database::new();
    dense_corpus(&mut db);
    // Stream s7 holds none of the best documents: hydrated candidates fail
    // WHERE, and each further round ranks a prefix twice as long. The result
    // is the stream's own top-k under the whole corpus's statistics.
    for (stream, k) in [("s7", 5), ("s0", 2), ("s7", 40)] {
        let mut expected = ranked_oracle(&mut db, "deploy failed", Some(stream));
        expected.truncate(k);
        assert_bit_exact(
            &filtered_search(&mut db, "deploy failed", k as i64, stream),
            &expected,
            stream,
        );
    }
    let evidence = scan_evidence(
        &mut db,
        "SELECT id FROM text_search('events_text', 'deploy', top_k => 5) AS e WHERE e.stream = 's7'",
        &[],
    );
    assert!(number(&evidence, "rounds") > 1, "{evidence:?}");
    // Rounds stop as soon as five s7 rows are accepted: far fewer candidates
    // than the 3,000 matches are ranked, and no round ranks everything.
    assert!(number(&evidence, "final_k") < 3_000, "{evidence:?}");
    let hydration = db
        .query_sql(
            "EXPLAIN ANALYZE SELECT id FROM text_search('events_text', 'deploy', top_k => 5) AS e \
             WHERE e.stream = 's7'",
        )
        .unwrap()
        .rows
        .into_iter()
        .find(|row| matches!(&row["id"], Value::String(id) if id.contains("SearchHydrationExec")))
        .unwrap();
    let Value::Int(hydrated) = hydration["actRows"] else {
        panic!("{hydration:?}");
    };
    assert!((5..=200).contains(&hydrated), "{hydration:?}");
    // A filter nothing satisfies exhausts the ranking and returns no rows.
    let none = db
        .query_sql(
            "SELECT id FROM text_search('events_text', 'failed', top_k => 5) AS e \
             WHERE e.stream = 'nowhere'",
        )
        .unwrap();
    assert!(none.rows.is_empty());
    let evidence = scan_evidence(
        &mut db,
        "SELECT id FROM text_search('events_text', 'failed', top_k => 5) AS e WHERE e.stream = 'nowhere'",
        &[],
    );
    assert_eq!(number(&evidence, "ranked_rows"), 3, "{evidence:?}");
}

#[test]
fn search_index_scan_pushes_limit_in_rank_order_only() {
    let mut db = Database::new();
    dense_corpus(&mut db);
    let expected = oracle(&mut db, "deploy failed", 50);
    for (sql, k, pushed, expected) in [
        (
            "SELECT id, score FROM text_search('events_text', 'deploy failed', top_k => 50) LIMIT 3",
            3,
            true,
            expected[..3].to_vec(),
        ),
        (
            "SELECT id, score FROM text_search('events_text', 'deploy failed', top_k => 50) \
             ORDER BY score DESC LIMIT 2 OFFSET 2",
            4,
            true,
            expected[2..4].to_vec(),
        ),
        (
            // Another order needs the whole top_k window.
            "SELECT id, score FROM text_search('events_text', 'deploy failed', top_k => 50) \
             ORDER BY id LIMIT 3",
            50,
            false,
            {
                let mut window = expected.clone();
                window.sort_by_key(|(id, _)| *id);
                window.truncate(3);
                window
            },
        ),
    ] {
        let rows = db
            .query_sql(sql)
            .unwrap()
            .rows
            .iter()
            .map(|row| {
                let (Value::Int(id), Value::Float(score)) = (&row["id"], &row["score"]) else {
                    panic!("{row:?}");
                };
                (*id, *score)
            })
            .collect::<Vec<_>>();
        assert_bit_exact(&rows, &expected, sql);
        let plan = db.query_sql(&format!("EXPLAIN {sql}")).unwrap().rows;
        let scan = plan
            .iter()
            .find(|row| matches!(&row["id"], Value::String(id) if id.contains("Bm25IndexScan")))
            .unwrap();
        let Value::String(info) = &scan["operator info"] else {
            panic!("{scan:?}");
        };
        assert!(
            info.contains(&format!("top_k=50, k={k}, limit_pushdown={pushed}")),
            "{sql}: {info}"
        );
    }
}

#[test]
fn search_index_scan_explains_its_plan() {
    let mut db = Database::new();
    dense_corpus(&mut db);
    let rows = db
        .query_sql(
            "EXPLAIN SELECT e.id, score FROM text_search('events_text', '数据库 deploy', top_k => 7) AS e \
             WHERE e.stream = 's1' ORDER BY e.seq LIMIT 5",
        )
        .unwrap()
        .rows;
    let lines = rows
        .iter()
        .map(|row| {
            let text = |column: &str| match &row[column] {
                Value::String(text) => text.clone(),
                other => panic!("{column}: {other:?}"),
            };
            format!(
                "{} | {} | {}",
                text("id"),
                text("access object"),
                text("operator info")
            )
        })
        .collect::<Vec<_>>();
    let operators = lines
        .iter()
        .map(|line| {
            line.split('_')
                .next()
                .unwrap()
                .trim_start_matches(['└', '─', ' '])
        })
        .collect::<Vec<_>>();
    assert_eq!(
        operators,
        [
            "LimitExec",
            "TopNExec",
            "ProjectionExec",
            "SelectionExec",
            "SearchHydrationExec",
            "Bm25IndexScan",
        ],
        "{lines:#?}"
    );
    let scan = lines.last().unwrap();
    assert!(
        scan.contains("table:events, search_index:events_text(text)")
            && scan.contains("algorithm=block_max_wand, analyzer=cjk_ngram")
            && scan.contains("terms=[deploy,据库,数据], top_k=7, k=7, limit_pushdown=false"),
        "{scan}"
    );
    assert!(
        lines[3].contains("strategy=bounded_over_fetch"),
        "{}",
        lines[3]
    );
    // ANALYZE adds the scan's work; the statement still runs.
    let evidence = scan_evidence(
        &mut db,
        "SELECT e.id FROM text_search('events_text', 'deploy', top_k => 2) AS e",
        &[],
    );
    for key in [
        "rounds",
        "final_k",
        "ranked_rows",
        "blocks_decoded",
        "blocks_read",
        "blocks_skipped",
        "blocks_total",
        "block_skips",
        "documents_scored",
        "lengths_rows_read",
    ] {
        number(&evidence, key);
    }
    fraction(&evidence, "postings_visited");
    fraction(&evidence, "matched_terms");
}

#[test]
fn search_index_source_writers_take_row_footprints_and_readers_the_source_domain() {
    let mut db = Database::new();
    db.query_sql(SCHEMA).unwrap();
    db.query_sql(INDEX).unwrap();
    db.query_sql("CREATE TABLE other (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("CREATE TABLE elsewhere (id BIGINT PRIMARY KEY)")
        .unwrap();
    insert(&mut db, 1, "s", Some("deploy failed"));
    let db = db.into_concurrent();
    // A writer to the indexed table no longer takes a database-wide
    // footprint: a concurrent writer to another table commits.
    let mut other = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    other
        .query_sql("INSERT INTO other (id) VALUES (1)")
        .unwrap();
    db.query_sql("INSERT INTO events (id, stream, seq, text) VALUES (2, 's', 2, 'deploy again')")
        .unwrap();
    other.commit().unwrap();
    // Two writers to the source table with disjoint keys both commit; each
    // commit derives its postings from the latest state.
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    first
        .query_sql("INSERT INTO events (id, stream, seq, text) VALUES (3, 's', 3, 'deploy three')")
        .unwrap();
    db.query_sql("INSERT INTO events (id, stream, seq, text) VALUES (4, 's', 4, 'deploy four')")
        .unwrap();
    first.commit().unwrap();
    let all = db
        .query_sql("SELECT id FROM text_search('events_text', 'deploy', top_k => 10)")
        .unwrap();
    assert_eq!(all.rows.len(), 4);
    // A serializable transaction that ranked the corpus conflicts with a
    // concurrent change to it: every score depends on the corpus statistics.
    let serializable = ConcurrentTransactionOptions::optimistic()
        .with_isolation(TransactionIsolation::Serializable);
    let mut reader = db.begin_transaction(serializable).unwrap();
    reader
        .query_sql("SELECT id FROM text_search('events_text', 'deploy', top_k => 1)")
        .unwrap();
    reader
        .query_sql("INSERT INTO other (id) VALUES (2)")
        .unwrap();
    db.query_sql(
        "INSERT INTO events (id, stream, seq, text) VALUES (5, 's', 5, 'unrelated words')",
    )
    .unwrap();
    assert!(
        reader
            .commit()
            .unwrap_err()
            .is_retryable_transaction_conflict(),
        "a ranking read must be invalidated by a corpus change"
    );
    // A concurrent write elsewhere does not invalidate it.
    let mut reader = db.begin_transaction(serializable).unwrap();
    reader
        .query_sql("SELECT id FROM text_search('events_text', 'deploy', top_k => 1)")
        .unwrap();
    reader
        .query_sql("INSERT INTO other (id) VALUES (3)")
        .unwrap();
    db.query_sql("INSERT INTO elsewhere (id) VALUES (4)")
        .unwrap();
    reader.commit().unwrap();
}
