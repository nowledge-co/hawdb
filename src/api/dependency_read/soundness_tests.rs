// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::store::GraphReadDependencyError;

struct Random(u64);

impl Random {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

fn limits() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(32).unwrap(),
        dependency_bytes: NonZeroUsize::new(4096).unwrap(),
        statement_rows: NonZeroUsize::new(32).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(4096).unwrap(),
    }
}

#[test]
fn randomized_single_hop_dependencies_preserve_query_results() {
    let mut accepted = [0usize; 3];
    let mut rejected = [0usize; 3];
    for seed in [1130, 0x51ce, 0xdecaf, 0x1234_5678] {
        let mut random = Random(seed);
        for (shape, effect) in ["create", "delete", "property"].into_iter().enumerate() {
            for same_root in [false, true] {
                for same_type in [false, true] {
                    let mut db = Database::new();
                    for key in ["a", "b", "c"] {
                        db.query(&format!("CREATE (:Memory {{key: '{key}'}})"))
                            .unwrap();
                        db.query(&format!("CREATE (:Document {{key: '{key}'}})"))
                            .unwrap();
                    }
                    for root_key in ["a", "b"] {
                        for kind in ["LINK", "OTHER"] {
                            db.query(&format!("MATCH (a:Memory {{key: '{root_key}'}}), (b:Document {{key: 'a'}}) CREATE (a)-[:{kind} {{weight: 1}}]->(b)")).unwrap();
                        }
                    }
                    let read_root = if random.below(2) == 0 { "a" } else { "b" };
                    let other_root = if read_root == "a" { "b" } else { "a" };
                    let root = db
                        .query(&format!(
                            "MATCH (a:Memory {{key: '{read_root}'}}) RETURN id(a) AS id"
                        ))
                        .unwrap()
                        .rows[0]["id"]
                        .clone();
                    let params = BTreeMap::from([("root".into(), root)]);
                    let query = "MATCH (a:Memory)-[r:LINK]->(b:Document) WHERE id(a) = $root RETURN b.key AS key, r.weight AS weight";
                    let mut read = db.begin_prepared_read(limits()).unwrap();
                    let mut before = read
                        .query_with_params(query, &params)
                        .unwrap()
                        .output
                        .rows
                        .into_rows();
                    before.sort();
                    let kind = if same_type { "LINK" } else { "OTHER" };
                    let write_root = if same_root { read_root } else { other_root };
                    let value = 2 + random.below(1000);
                    let write = match effect {
                    "create" => format!("MATCH (a:Memory {{key: '{write_root}'}}), (b:Document {{key: 'b'}}) CREATE (a)-[:{kind} {{weight: {value}}}]->(b)"),
                    "delete" => format!("MATCH (a:Memory {{key: '{write_root}'}})-[r:{kind}]->(:Document {{key: 'a'}}) DELETE r"),
                    "property" => format!("MATCH (a:Memory {{key: '{write_root}'}})-[r:{kind}]->(:Document {{key: 'a'}}) SET r.weight = {value}"),
                    _ => unreachable!(),
                };
                    db.query(&write).unwrap();
                    let mut retained = read
                        .query_with_params(query, &params)
                        .unwrap()
                        .output
                        .rows
                        .into_rows();
                    retained.sort();
                    assert_eq!(retained, before, "seed={seed} effect={effect} same_root={same_root} same_type={same_type}: snapshot changed");
                    let deps = read.finish().unwrap();
                    let mut after = db
                        .query_with_params(query, &params)
                        .unwrap()
                        .rows
                        .into_rows();
                    after.sort();
                    match db.runtime.get().unwrap().store.validate_read_dependencies(&deps.graph) {
                    Ok(()) => {
                        assert_eq!(before, after, "seed={seed} effect={effect} same_root={same_root} same_type={same_type}: accepted stale rows");
                        accepted[shape] += 1;
                    }
                    Err(GraphReadDependencyError::Conflict(_)) => rejected[shape] += 1,
                    Err(error) => panic!("seed={seed} effect={effect} same_root={same_root} same_type={same_type}: {error}"),
                }
                }
            }
        }
    }
    assert_eq!(accepted, [8, 8, 12]);
    assert_eq!(rejected, [8, 8, 4]);
}

#[test]
fn randomized_sql_point_prefix_and_join_dependencies_preserve_query_results() {
    let mut accepted = [0usize; 4];
    let mut rejected = [0usize; 4];
    for seed in [1130, 0x51ce, 0xdecaf, 0x1234_5678] {
        let mut random = Random(seed);
        for (shape, name) in ["point", "prefix", "join", "join_null"]
            .into_iter()
            .enumerate()
        {
            for effect in ["insert", "update", "delete"] {
                if name == "join_null" && effect != "update" {
                    continue;
                }
                for related in [false, true] {
                    let mut db = Database::new();
                    db.query_sql("CREATE TABLE docs (doc_id TEXT PRIMARY KEY, owner_id TEXT, owner_kind TEXT)").unwrap();
                    db.query_sql("CREATE TABLE chunks (chunk_id TEXT PRIMARY KEY, doc_id TEXT, ordinal BIGINT, body TEXT)").unwrap();
                    db.query_sql("CREATE INDEX chunk_doc ON chunks (doc_id, ordinal, chunk_id)")
                        .unwrap();
                    db.query_sql("INSERT INTO docs (doc_id, owner_id, owner_kind) VALUES ('d1','one','source'),('d2','two','source')").unwrap();
                    db.query_sql("INSERT INTO chunks (chunk_id, doc_id, ordinal, body) VALUES ('c1','d1',1,'one'),('c2','d2',1,'two'),('c3',NULL,2,'null'),('c6',NULL,3,'null')").unwrap();
                    let first = random.below(2) == 0;
                    let (
                        doc_id,
                        other_doc,
                        chunk_id,
                        other_chunk,
                        missing_doc,
                        other_missing_doc,
                        null_chunk,
                    ) = if first {
                        ("d1", "d2", "c1", "c2", "d3", "d4", "c3")
                    } else {
                        ("d2", "d1", "c2", "c1", "d4", "d3", "c6")
                    };
                    let query = match name {
                        "point" => "SELECT doc_id, owner_id FROM docs WHERE doc_id = $1",
                        "prefix" => "SELECT chunk_id, body FROM chunks WHERE doc_id = $1 ORDER BY ordinal, chunk_id LIMIT 8",
                        _ => "SELECT c.chunk_id, d.owner_id FROM chunks AS c INNER JOIN docs AS d ON d.doc_id = c.doc_id WHERE c.chunk_id = $1 AND d.owner_kind = 'source'",
                    };
                    let key = match (name, effect) {
                        ("point", "insert") => missing_doc,
                        ("join", "insert") => "c4",
                        ("join_null", _) => null_chunk,
                        ("point", _) => doc_id,
                        ("join", _) => chunk_id,
                        _ => doc_id,
                    };
                    let params = [Value::String(key.into())];
                    let mut read = db.begin_prepared_read(limits()).unwrap();
                    let mut before = read
                        .query_sql_with_params(query, &params)
                        .unwrap()
                        .rows
                        .into_rows();
                    before.sort();
                    let value = 10 + random.below(10_000);
                    let write_doc = if related { doc_id } else { other_doc };
                    let write_chunk = if related { chunk_id } else { other_chunk };
                    let write = match (name, effect, related) {
                        ("point", "insert", _) => format!("INSERT INTO docs (doc_id, owner_id, owner_kind) VALUES ('{}','{value}','source')", if related { missing_doc } else { other_missing_doc }),
                        ("point" | "join", "update", _) => format!("UPDATE docs SET owner_id = '{value}' WHERE doc_id = '{write_doc}'"),
                        ("point", "delete", _) => format!("DELETE FROM docs WHERE doc_id = '{write_doc}'"),
                        ("prefix", "insert", _) => format!("INSERT INTO chunks (chunk_id, doc_id, ordinal, body) VALUES ('c4','{write_doc}',4,'{value}')"),
                        ("prefix", "update", _) => format!("UPDATE chunks SET body = '{value}' WHERE chunk_id = '{write_chunk}'"),
                        ("prefix" | "join", "delete", _) => format!("DELETE FROM chunks WHERE chunk_id = '{write_chunk}'"),
                        ("join", "insert", _) => format!("INSERT INTO chunks (chunk_id, doc_id, ordinal, body) VALUES ('{}','{write_doc}',4,'{value}')", if related { "c4" } else { "c5" }),
                        ("join_null", "update", true) => format!("UPDATE chunks SET doc_id = '{doc_id}' WHERE chunk_id = '{null_chunk}'"),
                        ("join_null", "update", false) => format!("UPDATE chunks SET doc_id = NULL WHERE chunk_id = '{other_chunk}'"),
                        _ => unreachable!(),
                    };
                    let case =
                        format!("seed={seed:#x} shape={name} effect={effect} related={related}");
                    db.query_sql(&write)
                        .unwrap_or_else(|error| panic!("{case}: {error}"));
                    let mut retained = read
                        .query_sql_with_params(query, &params)
                        .unwrap()
                        .rows
                        .into_rows();
                    retained.sort();
                    assert_eq!(retained, before, "{case}: pinned source changed");
                    let deps = read.finish().unwrap();
                    assert!(
                        !deps.requires_unchanged_database_epoch(),
                        "{case}: coarse fallback"
                    );
                    let mut after = db
                        .query_sql_with_params(query, &params)
                        .unwrap()
                        .rows
                        .into_rows();
                    after.sort();
                    match db
                        .runtime
                        .get()
                        .unwrap()
                        .store
                        .validate_read_dependencies(&deps.graph)
                    {
                        Ok(()) => {
                            assert_eq!(before, after, "{case}: accepted stale rows");
                            accepted[shape] += 1;
                        }
                        Err(GraphReadDependencyError::Conflict(_)) => rejected[shape] += 1,
                        Err(error) => panic!("{case}: {error}"),
                    }
                }
            }
        }
    }
    assert_eq!(accepted, [12, 12, 12, 4]);
    assert_eq!(rejected, [12, 12, 12, 4]);
}

#[test]
fn reverted_index_values_still_conflict() {
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for seed in [1130, 0x51ce, 0xdecaf, 0x1234_5678] {
        let mut random = Random(seed);
        let query = "MATCH (n:Document) WHERE n.key = $key RETURN n.key AS key, n.value AS value";
        for related in [false, true] {
            let mut db = Database::new();
            let value = random.below(10_000);
            db.query(&format!("CREATE (:Document {{key: 'v', value: {value}}})"))
                .unwrap();
            db.query(&format!("CREATE (:Memory {{key: 'x', value: {value}}})"))
                .unwrap();
            db.query("CREATE INDEX ON :Document(key)").unwrap();
            let params = BTreeMap::from([("key".into(), Value::String("v".into()))]);
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let mut before = read
                .query_with_params(query, &params)
                .unwrap()
                .output
                .rows
                .into_rows();
            before.sort();
            let target = if related { "v" } else { "x" };
            let label = if related { "Document" } else { "Memory" };
            let intermediate = if related { "w" } else { "y" };
            db.query(&format!(
                "MATCH (n:{label} {{key: '{target}'}}) SET n.key = '{intermediate}'"
            ))
            .unwrap();
            db.query(&format!(
                "MATCH (n:{label} {{key: '{intermediate}'}}) SET n.key = '{target}'"
            ))
            .unwrap();
            let mut retained = read
                .query_with_params(query, &params)
                .unwrap()
                .output
                .rows
                .into_rows();
            retained.sort();
            assert_eq!(
                before, retained,
                "seed={seed} related={related}: snapshot changed"
            );
            let deps = read.finish().unwrap();
            let mut after = db
                .query_with_params(query, &params)
                .unwrap()
                .rows
                .into_rows();
            after.sort();
            match db
                .runtime
                .get()
                .unwrap()
                .store
                .validate_read_dependencies(&deps.graph)
            {
                Ok(()) => {
                    assert_eq!(
                        before, after,
                        "seed={seed} related={related}: accepted stale rows"
                    );
                    accepted += 1;
                }
                Err(GraphReadDependencyError::Conflict(_)) => rejected += 1,
                Err(error) => panic!("seed={seed} related={related}: {error}"),
            }
        }
    }
    assert_eq!(accepted, 4);
    assert_eq!(rejected, 4);
}
