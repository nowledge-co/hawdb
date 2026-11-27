// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Seeded differential test: an out-of-core database with equality, range,
//! composite and full-text indexes must answer every read exactly like an
//! unindexed twin, across random creates, SETs, MERGEs, deletes, checkpoints
//! and reopens (#2019, #2040). The fixed-seed CI version is small; set
//! `HAWDB_INDEX_TWIN_STEPS` and `HAWDB_INDEX_TWIN_SEEDS` for longer manual runs.

use super::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> i64 {
        (self.next() % bound) as i64
    }
}

const KINDS: [&str; 3] = ["note", "archive", "draft"];
const WORDS: [&str; 5] = ["alpha", "beta", "gamma", "delta", "omega"];

fn config() -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        runtime_capabilities: DatabaseConfig::default()
            .runtime_capabilities
            .with(hawdb_core::RuntimeCapability::BackgroundMaintenance, false),
        ..DatabaseConfig::default()
    }
}

fn params(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn rows(db: &mut Database, query: &str, params: &BTreeMap<String, Value>) -> Vec<Vec<Value>> {
    db.query_with_params(query, params)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect::<Vec<_>>())
        .collect()
}

fn setup(path: &std::path::Path, indexed: bool, base: i64) -> Database {
    let mut db = Database::open_with_config(path, config()).unwrap();
    db.query("CREATE NODE TABLE Item").unwrap();
    for (property, kind) in [("rank", "INT"), ("id", "STRING")] {
        db.query(&format!(
            "CREATE PROPERTY ON NODE TABLE Item({property}) TYPE {kind} NOT NULL"
        ))
        .unwrap();
    }
    let seed = (0..base)
        .map(|n| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::String(format!("i{n:05}"))),
                ("kind".into(), Value::String(KINDS[(n % 3) as usize].into())),
                ("rank".into(), Value::Int(n % 400)),
                (
                    "body".into(),
                    Value::String(format!(
                        "{} {}",
                        WORDS[(n % 5) as usize],
                        WORDS[(n % 3) as usize]
                    )),
                ),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {id: r.id, kind: r.kind, rank: r.rank, body: r.body, v: 0})",
        &params(&[("rows", Value::List(seed))]),
    )
    .unwrap();
    db.checkpoint().unwrap();
    if indexed {
        // The unique constraint lets the planner order by the composite
        // suffix; writes never create a second node with the same id.
        db.query("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")
            .unwrap();
        db.query("CREATE INDEX ON :Item(id)").unwrap();
        db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
        db.query("CREATE INDEX ON :Item(kind, rank, id)").unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Item(body)").unwrap();
        db.checkpoint().unwrap();
    }
    assert!(db.storage_residency_report().unwrap().out_of_core);
    db
}

/// One random write, applied identically to both databases.
fn write(rng: &mut Rng, next_id: &mut i64) -> (String, BTreeMap<String, Value>) {
    let id = |rng: &mut Rng, next_id: i64| {
        Value::String(format!("i{:05}", rng.below(next_id as u64 + 3)))
    };
    let kind = |rng: &mut Rng| Value::String(KINDS[rng.below(3) as usize].into());
    match rng.below(7) {
        0 => {
            let fresh = Value::String(format!("i{:05}", *next_id));
            *next_id += 1;
            (
                "CREATE (:Item {id: $id, kind: $kind, rank: $rank, body: $body, v: 0})".into(),
                params(&[
                    ("id", fresh),
                    ("kind", kind(rng)),
                    ("rank", Value::Int(rng.below(400))),
                    ("body", Value::String(WORDS[rng.below(5) as usize].into())),
                ]),
            )
        }
        1 | 2 => (
            // Materializes a canonical node without touching an index key.
            "MATCH (n:Item {id: $id}) SET n.v = n.v + 1".into(),
            params(&[("id", id(rng, *next_id))]),
        ),
        3 => (
            "MATCH (n:Item {id: $id}) SET n.rank = $rank".into(),
            params(&[("id", id(rng, *next_id)), ("rank", Value::Int(rng.below(400)))]),
        ),
        4 => (
            "MATCH (n:Item {id: $id}) SET n.kind = $kind".into(),
            params(&[("id", id(rng, *next_id)), ("kind", kind(rng))]),
        ),
        5 => (
            "MERGE (n:Item {id: $id}) ON CREATE SET n.kind = $kind, n.rank = $rank, n.body = 'omega', n.v = 0 ON MATCH SET n.v = n.v + 1".into(),
            params(&[
                ("id", id(rng, *next_id)),
                ("kind", kind(rng)),
                ("rank", Value::Int(rng.below(400))),
            ]),
        ),
        _ => (
            "MATCH (n:Item {id: $id}) DETACH DELETE n".into(),
            params(&[("id", id(rng, *next_id))]),
        ),
    }
}

/// The reads both databases must answer identically after every step.
fn reads(rng: &mut Rng, next_id: i64) -> Vec<(&'static str, BTreeMap<String, Value>)> {
    let id = Value::String(format!("i{:05}", rng.below(next_id as u64 + 3)));
    let kind = Value::String(KINDS[rng.below(3) as usize].into());
    let low = rng.below(400);
    vec![
        (
            "MATCH (n:Item {id: $id}) RETURN n.id AS id, n.v AS v, n.rank AS rank, n.kind AS kind",
            params(&[("id", id.clone())]),
        ),
        (
            "MATCH (n:Item) WHERE n.id = $id RETURN count(n) AS c",
            params(&[("id", id)]),
        ),
        (
            "MATCH (n:Item) WHERE n.rank >= $low AND n.rank <= $high RETURN n.id AS id, n.v AS v ORDER BY id",
            params(&[("low", Value::Int(low)), ("high", Value::Int(low + 2))]),
        ),
        (
            "MATCH (n:Item) WHERE n.kind = $kind AND n.rank = $rank RETURN n.id AS id, n.v AS v ORDER BY id",
            params(&[("kind", kind.clone()), ("rank", Value::Int(low))]),
        ),
        (
            "MATCH (n:Item) WHERE n.kind = $kind RETURN n.rank AS rank, n.id AS id ORDER BY rank ASC, id ASC LIMIT 25",
            params(&[("kind", kind.clone())]),
        ),
        (
            "MATCH (n:Item) WHERE n.kind = $kind RETURN n.rank AS rank, n.id AS id ORDER BY rank DESC, id DESC LIMIT 25",
            params(&[("kind", kind)]),
        ),
        (
            "MATCH (n:Item) WHERE n.body CONTAINS $word RETURN n.id AS id ORDER BY id",
            params(&[("word", Value::String(WORDS[rng.below(5) as usize].into()))]),
        ),
        ("MATCH (n:Item) RETURN count(n) AS c, sum(n.v) AS v", BTreeMap::new()),
    ]
}

fn run(seed: u64, steps: usize, base: i64) {
    let indexed_path = unique_test_dir(&format!("index_twin_indexed_{seed}"));
    let twin_path = unique_test_dir(&format!("index_twin_twin_{seed}"));
    let mut dbs = [
        setup(&indexed_path, true, base),
        setup(&twin_path, false, base),
    ];
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut next_id = base;
    assert_index_plans(&mut dbs[0]);
    for step in 0..steps {
        let (query, write_params) = write(&mut rng, &mut next_id);
        let results = dbs.each_mut().map(|db| rows(db, &query, &write_params));
        assert_eq!(
            results[0], results[1],
            "seed {seed} step {step}: {query} {write_params:?}"
        );
        match rng.below(40) {
            0 => dbs.iter_mut().for_each(|db| db.checkpoint().unwrap()),
            1 => {
                dbs = [
                    {
                        drop(std::mem::replace(&mut dbs[0], Database::new()));
                        Database::open_with_config(&indexed_path, config()).unwrap()
                    },
                    {
                        drop(std::mem::replace(&mut dbs[1], Database::new()));
                        Database::open_with_config(&twin_path, config()).unwrap()
                    },
                ];
            }
            _ => {}
        }
        for (query, read_params) in reads(&mut rng, next_id) {
            let indexed = rows(&mut dbs[0], query, &read_params);
            let twin = rows(&mut dbs[1], query, &read_params);
            assert_eq!(
                indexed, twin,
                "seed {seed} step {step}: {query} {read_params:?}"
            );
        }
    }
    drop(dbs);
    std::fs::remove_dir_all(indexed_path).unwrap();
    std::fs::remove_dir_all(twin_path).unwrap();
}

/// The reads above reach the equality, composite ordered and full-text
/// index paths on the indexed database. Range and composite range visitors
/// are pinned directly by
/// `store::tests::out_of_core_index_reads_find_materialized_base_nodes`.
fn assert_index_plans(db: &mut Database) {
    let mut rng = Rng(7);
    for (query, read_params) in reads(&mut rng, 10) {
        let plan = db
            .explain_query_with_params(query, &read_params)
            .unwrap()
            .physical_plan
            .explain(0);
        let expected = if query.contains("{id: $id}") || query.contains("n.id = $id") {
            Some("IndexNodeSeek")
        } else if query.contains("ORDER BY rank") {
            Some("IndexNodeCompositeOrderedSeek")
        } else if query.contains("CONTAINS") {
            Some("IndexNodeTextSeek")
        } else {
            None
        };
        if let Some(expected) = expected {
            assert!(plan.contains(expected), "{query}: {plan}");
        }
    }
}

#[test]
fn indexed_database_reads_match_an_unindexed_twin() {
    let steps = std::env::var("HAWDB_INDEX_TWIN_STEPS")
        .ok()
        .and_then(|steps| steps.parse().ok())
        .unwrap_or(120);
    let seeds = std::env::var("HAWDB_INDEX_TWIN_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(3u64);
    for seed in 1..=seeds {
        run(seed, steps, 500);
    }
}
