// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cypher `LOAD CSV` through the embedded `Database` API.

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

const PERSONS: &str = "id|firstName|lastName|age\n\
                       1|Ann|Lee|34\n\
                       2|Bo|\"Smith, Jr.\"|28\n\
                       3|Cy||\n";
const KNOWS: &str = "src|dst|creationDate\n\
                     1|2|100\n\
                     2|3|200\n\
                     1|3|300\n\
                     1|9|400\n\
                     |2|500\n";
const CREATE_PERSONS: &str = "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
     CREATE (:Person {id: toInteger(row.id), firstName: row.firstName})";
const CREATE_KNOWS: &str = "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
     MATCH (a:Person {id: toInteger(row.src)}), (b:Person {id: toInteger(row.dst)}) \
     CREATE (a)-[:KNOWS {creationDate: toInteger(row.creationDate)}]->(b)";

/// A unique import directory, removed with its parent on drop.
struct ImportDir {
    base: PathBuf,
    root: PathBuf,
}

impl ImportDir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let base = std::env::temp_dir().join(format!(
            "hawdb-load-csv-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&base);
        let root = base.join("import");
        std::fs::create_dir_all(&root).unwrap();
        Self { base, root }
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> String {
        let path = self.root.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn config(&self) -> DatabaseConfig {
        DatabaseConfig {
            csv_import_root: Some(self.root.clone()),
            ..DatabaseConfig::default()
        }
    }

    fn database(&self) -> Database {
        Database::new_with_config(self.config())
    }
}

impl Drop for ImportDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

fn file(path: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("f".to_string(), Value::String(path.to_string()))])
}

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

fn count(db: &mut Database, query: &str) -> i64 {
    match &db.query(query).unwrap().rows[0]["c"] {
        Value::Int(count) => *count,
        other => panic!("expected count, got {other:?}"),
    }
}

fn load_people(dir: &ImportDir, db: &mut Database) {
    let persons = dir.write("persons.csv", PERSONS);
    db.query_with_params(CREATE_PERSONS, &file(&persons))
        .unwrap();
}

#[test]
fn ldbc_style_node_and_relationship_loads_compose_with_create_and_match() {
    let dir = ImportDir::new("ldbc");
    let mut db = dir.database();
    load_people(&dir, &mut db);
    let people = db
        .query("MATCH (p:Person) RETURN p.id AS id, p.firstName AS name ORDER BY id")
        .unwrap();
    assert_eq!(column(&people, "id"), [1, 2, 3].map(Value::Int));
    assert_eq!(
        column(&people, "name"),
        [string("Ann"), string("Bo"), string("Cy")]
    );

    let knows = dir.write("knows.csv", KNOWS);
    db.query_with_params(CREATE_KNOWS, &file(&knows)).unwrap();
    let edges = db
        .query(
            "MATCH (a:Person)-[k:KNOWS]->(b:Person) \
             RETURN a.id AS src, b.id AS dst, k.creationDate AS created ORDER BY created",
        )
        .unwrap();
    // `1|9` has no target and `|2` has a NULL source, so neither matches.
    assert_eq!(column(&edges, "src"), [1, 2, 1].map(Value::Int));
    assert_eq!(column(&edges, "dst"), [2, 3, 3].map(Value::Int));
    assert_eq!(column(&edges, "created"), [100, 200, 300].map(Value::Int));
}

#[test]
fn empty_header_fields_are_null_and_create_omits_null_properties() {
    let dir = ImportDir::new("nulls");
    let mut db = dir.database();
    let path = dir.write("people.csv", "id,name,nick\n1,,\"\"\n");
    db.query_with_params(
        "LOAD CSV WITH HEADERS FROM $f AS row \
         CREATE (:P {id: toInteger(row.id), name: row.name, nick: row.nick})",
        &file(&path),
    )
    .unwrap();
    let row = db
        .query("MATCH (p:P) RETURN coalesce(p.name, 'none') AS missing, p.nick AS nick")
        .unwrap()
        .rows[0]
        .clone();
    assert_eq!(row["missing"], string("none"));
    assert_eq!(row["nick"], string(""));
}

#[test]
fn list_rows_project_index_size_and_count() {
    let dir = ImportDir::new("list");
    let mut db = dir.database();
    let path = dir.write(
        "lines.csv",
        "\u{feff}a,b,c\r\n\"multi\nline\",\"q\"\"uote\"\r\n\r\nlast\r\n",
    );
    let output = db
        .query_with_params(
            "LOAD CSV FROM $f AS line RETURN line[0] AS first, size(line) AS n",
            &file(&path),
        )
        .unwrap();
    assert_eq!(
        column(&output, "first"),
        [string("a"), string("multi\nline"), string("last")]
    );
    assert_eq!(column(&output, "n"), [3, 2, 1].map(Value::Int));
    let total = db
        .query_with_params(
            "LOAD CSV FROM $f AS line RETURN count(*) AS c",
            &file(&path),
        )
        .unwrap();
    assert_eq!(total.rows[0]["c"], Value::Int(3));

    // Neo4j-style URL: the path is relative to the import directory.
    let file_url = format!(
        "file:///{}",
        std::path::Path::new(&path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
    );
    let literal = db
        .query(&format!(
            "LOAD CSV FROM '{file_url}' AS line RETURN line[1] AS second"
        ))
        .unwrap();
    assert_eq!(
        column(&literal, "second"),
        [string("b"), string("q\"uote"), Value::Null]
    );
}

#[test]
fn header_rows_compose_with_with_where_order_and_conversions() {
    let dir = ImportDir::new("with");
    let mut db = dir.database();
    let path = dir.write("persons.csv", PERSONS);
    let output = db
        .query_with_params(
            "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
             WITH row, toInteger(row.age) AS age WHERE age > 30 OR row.age IS NULL \
             RETURN row.firstName AS name, age, toString(age) AS text, \
             toFloat(row.id) AS id, toBoolean('TRUE') AS flag ORDER BY name",
            &file(&path),
        )
        .unwrap();
    assert_eq!(column(&output, "name"), [string("Ann"), string("Cy")]);
    assert_eq!(column(&output, "age"), [Value::Int(34), Value::Null]);
    assert_eq!(column(&output, "text"), [string("34"), Value::Null]);
    assert_eq!(
        column(&output, "id"),
        [Value::Float(1.0), Value::Float(3.0)]
    );
    assert_eq!(
        column(&output, "flag"),
        [Value::Bool(true), Value::Bool(true)]
    );

    let tabs = dir.write("tabs.tsv", "x\ty\n1\t2\n");
    let output = db
        .query_with_params(
            "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '\\t' \
             RETURN row.y AS y",
            &file(&tabs),
        )
        .unwrap();
    assert_eq!(column(&output, "y"), [string("2")]);
}

#[test]
fn list_rows_feed_merge_with_on_create_conversions() {
    let dir = ImportDir::new("merge");
    let mut db = dir.database();
    let path = dir.write("cities.csv", "10,Paris\n20,Rome\n30,Paris\n");
    db.query_with_params(
        "LOAD CSV FROM $f AS line MERGE (c:City {name: line[1]}) \
         ON CREATE SET c.code = toInteger(line[0])",
        &file(&path),
    )
    .unwrap();
    let output = db
        .query("MATCH (c:City) RETURN c.name AS name, c.code AS code ORDER BY name")
        .unwrap();
    assert_eq!(column(&output, "name"), [string("Paris"), string("Rome")]);
    assert_eq!(column(&output, "code"), [Value::Int(10), Value::Int(20)]);

    let nulls = dir.write("null.csv", "id\n\n,\n");
    let error = db
        .query_with_params(
            "LOAD CSV FROM $f AS line MERGE (c:City {name: line[1]})",
            &file(&nulls),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("NULL value for property 'name'"),
        "{error}"
    );
}

#[test]
fn load_csv_is_refused_without_an_import_root() {
    let dir = ImportDir::new("disabled");
    let path = dir.write("persons.csv", PERSONS);
    let mut db = Database::new();
    for query in [
        "LOAD CSV FROM $f AS line RETURN count(*) AS c",
        CREATE_PERSONS,
    ] {
        let error = db.query_with_params(query, &file(&path)).unwrap_err();
        assert!(error.to_string().contains("csv_import_root"), "{error}");
    }
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);
}

#[test]
fn sources_outside_the_root_and_remote_urls_are_rejected() {
    let dir = ImportDir::new("escape");
    let outside = dir.base.join("outside.csv");
    std::fs::write(&outside, "secret\n").unwrap();
    let mut db = dir.database();
    let query = "LOAD CSV FROM $f AS line RETURN line[0] AS value";
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut rejected = vec![
        (
            "../outside.csv".to_string(),
            "outside the configured import directory",
        ),
        (
            outside.to_str().unwrap().to_string(),
            "outside the configured import directory",
        ),
        (
            "https://example.com/data.csv".to_string(),
            "supports only local files",
        ),
        (
            "http://example.com/data.csv".to_string(),
            "supports only local files",
        ),
    ];
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&outside, dir.root.join("link.csv")).unwrap();
        rejected.push((
            "link.csv".to_string(),
            "outside the configured import directory",
        ));
    }
    for (location, message) in rejected {
        let error = db.query_with_params(query, &file(&location)).unwrap_err();
        assert!(error.to_string().contains(message), "{location}: {error}");
    }
    let error = db
        .query("LOAD CSV FROM 'https://example.com/x.csv' AS line RETURN line")
        .unwrap_err();
    assert!(
        error.to_string().contains("supports only local files"),
        "{error}"
    );
}

#[test]
fn parameter_sources_are_rebound_for_every_execution() {
    let dir = ImportDir::new("rebind");
    let first = dir.write("first.csv", "a\nb\n");
    let second = dir.write("second.csv", "x\ny\nz\n");
    let mut db = dir.database();
    let query = "LOAD CSV FROM $f AS line RETURN line[0] AS value";
    let before = db.plan_cache_stats().unwrap();
    let output = db.query_with_params(query, &file(&first)).unwrap();
    assert_eq!(column(&output, "value"), [string("a"), string("b")]);
    let output = db.query_with_params(query, &file(&second)).unwrap();
    assert_eq!(
        column(&output, "value"),
        [string("x"), string("y"), string("z")]
    );
    // A cached plan never retains file contents: rewriting the file changes
    // the result of an identical query and parameter.
    dir.write("first.csv", "c\n");
    let output = db.query_with_params(query, &file(&first)).unwrap();
    assert_eq!(column(&output, "value"), [string("c")]);
    let after = db.plan_cache_stats().unwrap();
    assert!(after.hits > before.hits, "{before:?} -> {after:?}");
}

#[test]
fn oversized_imports_fail_closed_without_partial_writes() {
    let dir = ImportDir::new("budget");
    let path = dir.write("persons.csv", PERSONS);
    let mut config = dir.config();
    config.mutation_limits.max_operations = std::num::NonZeroUsize::new(2).unwrap();
    let mut db = Database::new_with_config(config);
    let error = db
        .query_with_params(CREATE_PERSONS, &file(&path))
        .unwrap_err();
    assert!(
        error.to_string().contains("max_mutation_operations 2"),
        "{error}"
    );
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);

    let mut config = dir.config();
    config.csv_import_limits.max_record_bytes = 8;
    let mut db = Database::new_with_config(config);
    let error = db
        .query_with_params(CREATE_PERSONS, &file(&path))
        .unwrap_err();
    assert!(error.to_string().contains("max_record_bytes 8"), "{error}");
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);

    let mut config = dir.config();
    config.csv_import_limits.max_file_bytes = 16;
    let mut db = Database::new_with_config(config);
    let error = db
        .query_with_params(
            "LOAD CSV FROM $f AS line RETURN count(*) AS c",
            &file(&path),
        )
        .unwrap_err();
    assert!(error.to_string().contains("max_file_bytes 16"), "{error}");

    let mut config = dir.config();
    config.max_read_result_rows = Some(2);
    let mut db = Database::new_with_config(config);
    let error = db
        .query_with_params("LOAD CSV FROM $f AS line RETURN line", &file(&path))
        .unwrap_err();
    assert!(error.to_string().contains('2'), "{error}");
    // Aggregation consumes every record without returning them.
    assert_eq!(
        db.query_with_params(
            "LOAD CSV FROM $f AS line RETURN count(*) AS c",
            &file(&path)
        )
        .unwrap()
        .rows[0]["c"],
        Value::Int(4)
    );

    let invalid = dir.write("invalid.csv", b"ok\nbad\xFF\n");
    let error = db
        .query_with_params(
            "LOAD CSV FROM $f AS line RETURN count(*) AS c",
            &file(&invalid),
        )
        .unwrap_err();
    assert!(error.to_string().contains("not valid UTF-8"), "{error}");
}

#[test]
fn explicit_transactions_stage_and_roll_back_imports() {
    let dir = ImportDir::new("transaction");
    let path = dir.write("persons.csv", PERSONS);
    let mut db = dir.database();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_with_params(CREATE_PERSONS, &file(&path))
        .unwrap();
    let staged = transaction
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .unwrap();
    assert_eq!(staged.rows[0]["c"], Value::Int(3));
    transaction.rollback();
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);

    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_with_params(CREATE_PERSONS, &file(&path))
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 3);
}

#[test]
fn unsupported_compositions_are_rejected_with_semantic_errors() {
    let dir = ImportDir::new("unsupported");
    let path = dir.write("persons.csv", PERSONS);
    let mut db = dir.database();
    for (query, message) in [
        (
            "MATCH (n:Person) LOAD CSV FROM $f AS line RETURN line",
            "only as the first clause",
        ),
        (
            "LOAD CSV WITH HEADERS FROM $f AS row WITH row WHERE row.id IS NOT NULL \
             CREATE (:Person {id: row.id})",
            "LOAD CSV mutations require exactly one following mutation clause",
        ),
        (
            "LOAD CSV WITH HEADERS FROM $f AS row \
             MATCH (a:Person {id: row.id}), (b:Person {id: row.id}) WHERE a.x = 1 \
             CREATE (a)-[:R]->(b)",
            "does not support WHERE",
        ),
        (
            "LOAD CSV WITH HEADERS FROM $f AS other CREATE (:Person {id: row.id})",
            "does not reference row variable 'other'",
        ),
        (
            "LOAD CSV FROM $f AS line FIELDTERMINATOR ';;' RETURN line",
            "FIELDTERMINATOR must be one ASCII character",
        ),
    ] {
        let error = db.query_with_params(query, &file(&path)).unwrap_err();
        assert!(error.to_string().contains(message), "{query}: {error}");
    }
    let error = db
        .query_with_params(
            "LOAD CSV FROM $f AS line RETURN line",
            &BTreeMap::from([("f".to_string(), Value::Int(1))]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("must be a string"), "{error}");
}

#[test]
fn explain_reports_csv_operators_without_reading_the_file() {
    let dir = ImportDir::new("explain");
    let db = dir.database();
    let missing = file(&dir.root.join("missing.csv").to_string_lossy());
    let read = db
        .explain_query_with_params("LOAD CSV FROM $f AS line RETURN line", &missing)
        .unwrap();
    assert!(
        format!("{read:?}").contains("LoadCsvExec source=$f"),
        "{read:?}"
    );
    let write = db
        .explain_query_with_params(CREATE_KNOWS, &missing)
        .unwrap();
    let text = format!("{write:?}");
    assert!(text.contains("LoadCsvMutationExec"), "{text}");
    assert!(text.contains("CreateMatchedRelationship"), "{text}");
}

#[test]
fn conversion_functions_follow_neo4j_semantics_outside_load_csv() {
    let mut db = Database::new();
    db.query_with_params(
        "CREATE (:T {i: toInteger($i), f: toFloat($f), b: toBoolean('false'), s: toString(12)})",
        &BTreeMap::from([
            ("i".to_string(), string(" 42 ")),
            ("f".to_string(), string("2.5")),
        ]),
    )
    .unwrap();
    let row = db
        .query(
            "MATCH (t:T) RETURN t.i AS i, t.f AS f, t.b AS b, t.s AS s, \
             toInteger('nope') AS bad, toInteger(t.missing) AS missing, \
             toInteger(t.f) AS truncated, toString(t.f) AS text, toFloat(t.i) AS float",
        )
        .unwrap()
        .rows[0]
        .clone();
    assert_eq!(row["i"], Value::Int(42));
    assert_eq!(row["f"], Value::Float(2.5));
    assert_eq!(row["b"], Value::Bool(false));
    assert_eq!(row["s"], string("12"));
    assert_eq!(row["bad"], Value::Null);
    assert_eq!(row["missing"], Value::Null);
    assert_eq!(row["truncated"], Value::Int(2));
    assert_eq!(row["text"], string("2.5"));
    assert_eq!(row["float"], Value::Float(42.0));
    let error = db
        .query("MATCH (t:T) RETURN toBoolean(t.f) AS invalid")
        .unwrap_err();
    assert!(error.to_string().contains("toBoolean()"), "{error}");
}

#[test]
fn csv_fields_compose_with_temporal_constructors_and_to_string() {
    let dir = ImportDir::new("temporal");
    let mut db = dir.database();
    let path = dir.write(
        "events.csv",
        "id,day,at\n1,2026-10-30,2026-10-30T12:34:56.789+01:00\n2,,2026-01-02T03:04:05Z\n",
    );
    let output = db
        .query_with_params(
            "LOAD CSV WITH HEADERS FROM $f AS row \
             WITH toInteger(row.id) AS id, date(row.day) AS day, datetime(row.at) AS at \
             RETURN id, day.year AS year, toString(day) AS day_text, toString(at) AS at_text \
             ORDER BY id",
            &file(&path),
        )
        .unwrap();
    assert_eq!(column(&output, "year"), [Value::Int(2026), Value::Null]);
    assert_eq!(
        column(&output, "day_text"),
        [string("2026-10-30"), Value::Null]
    );
    assert_eq!(
        column(&output, "at_text"),
        [
            string("2026-10-30T12:34:56.789+01:00"),
            string("2026-01-02T03:04:05Z")
        ]
    );

    db.query("CREATE (:E {day: date('2026-10-30'), span: duration('P1Y2M3DT5H')})")
        .unwrap();
    let row = db
        .query("MATCH (e:E) RETURN toString(e.day) AS day, toString(e.span) AS span")
        .unwrap()
        .rows[0]
        .clone();
    assert_eq!(row["day"], string("2026-10-30"));
    assert_eq!(row["span"], string("P1Y2M3DT5H"));
    let error = db
        .query("MATCH (e:E) RETURN toInteger(e.day) AS invalid")
        .unwrap_err();
    assert!(
        error.to_string().contains("cannot convert a DATE value"),
        "{error}"
    );
    // Write property values do not yet evaluate temporal constructors per
    // record; the statement is refused before any write.
    let error = db
        .query_with_params(
            "LOAD CSV WITH HEADERS FROM $f AS row CREATE (:W {day: date(row.day)})",
            &file(&path),
        )
        .unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
    assert_eq!(count(&mut db, "MATCH (w:W) RETURN count(w) AS c"), 0);
}

#[test]
fn unwind_rows_share_the_matched_relationship_mutation() {
    let mut db = Database::new();
    for id in 1..=3 {
        db.query(&format!("CREATE (:Person {{id: {id}}})")).unwrap();
    }
    let edge = |src: i64, dst: i64| {
        Value::Map(BTreeMap::from([
            ("src".to_string(), Value::Int(src)),
            ("dst".to_string(), Value::Int(dst)),
        ]))
    };
    db.query_with_params(
        "UNWIND $rows AS r MATCH (a:Person {id: r.src}), (b:Person {id: r.dst}) \
         CREATE (a)-[:KNOWS {since: toString(r.src)}]->(b)",
        &BTreeMap::from([(
            "rows".to_string(),
            Value::List(vec![edge(1, 2), edge(2, 3), edge(3, 9)]),
        )]),
    )
    .unwrap();
    let edges = db
        .query(
            "MATCH (a:Person)-[k:KNOWS]->(b:Person) \
             RETURN a.id AS src, b.id AS dst, k.since AS since ORDER BY src",
        )
        .unwrap();
    assert_eq!(column(&edges, "src"), [1, 2].map(Value::Int));
    assert_eq!(column(&edges, "dst"), [2, 3].map(Value::Int));
    assert_eq!(column(&edges, "since"), [string("1"), string("2")]);
}

#[test]
fn persistent_imports_respect_wal_batch_limits_atomically() {
    let dir = ImportDir::new("wal");
    let path = dir.write("persons.csv", PERSONS);
    let database_path = dir.base.join("db");
    let config = DatabaseConfig {
        max_wal_batch_operations: Some(2),
        ..dir.config()
    };
    {
        let mut db = Database::open_with_config(&database_path, config.clone()).unwrap();
        let error = db
            .query_with_params(CREATE_PERSONS, &file(&path))
            .unwrap_err();
        assert!(
            error.to_string().contains("max_wal_batch_operations=2"),
            "{error}"
        );
        assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);
    }
    let mut db = Database::open_with_config(&database_path, dir.config()).unwrap();
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);
    db.query_with_params(CREATE_PERSONS, &file(&path)).unwrap();
    drop(db);
    let mut db = Database::open_with_config(&database_path, dir.config()).unwrap();
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 3);
}

/// `n` people with `id` 0..n, plus a second `id: 7`, as a CSV body.
fn people_csv(n: i64) -> String {
    let mut csv = String::from("id\n");
    for id in 0..n {
        csv.push_str(&format!("{id}\n"));
    }
    csv.push_str("7\n");
    csv
}

/// Edges `i -> (7i + 1) mod n`, a duplicate-endpoint row, a missing endpoint
/// and a NULL endpoint, with the number of relationships they create.
fn knows_csv(n: i64) -> (String, i64) {
    let mut csv = String::from("src,dst,w\n");
    let matches = |id: i64| match id {
        7 => 2,
        id if (0..n).contains(&id) => 1,
        _ => 0,
    };
    let mut expected = 0;
    for src in 0..n {
        let dst = (src * 7 + 1) % n;
        csv.push_str(&format!("{src},{dst},{src}\n"));
        expected += matches(src) * matches(dst);
    }
    csv.push_str(&format!("7,7,-1\n1,{},-2\n,2,-3\n", n + 5));
    expected += 4;
    (csv, expected)
}

const LOAD_KNOWS: &str = "LOAD CSV WITH HEADERS FROM $f AS row \
     MATCH (a:Person {id: toInteger(row.src)}), (b:Person {id: toInteger(row.dst)}) \
     CREATE (a)-[:KNOWS {w: toInteger(row.w)}]->(b)";
const LOAD_PEOPLE: &str =
    "LOAD CSV WITH HEADERS FROM $f AS row CREATE (:Person {id: toInteger(row.id)})";

fn knows_count(db: &mut Database) -> i64 {
    count(
        db,
        "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS c",
    )
}

#[test]
fn relationship_loads_resolve_endpoints_before_the_write_guard() {
    const PEOPLE: i64 = 3_000;
    let dir = ImportDir::new("endpoints");
    let people = dir.write("people.csv", people_csv(PEOPLE));
    let (knows, expected) = knows_csv(PEOPLE);
    let knows = dir.write("knows.csv", knows);
    for indexed in [false, true] {
        let mut db = dir.database();
        if indexed {
            db.query("CREATE INDEX ON :Person(id)").unwrap();
        }
        db.query_with_params(LOAD_PEOPLE, &file(&people)).unwrap();
        // The counter observes the per-request scan a property filter needs.
        let scans = hawdb_storage::store::matched_node_label_scans_for_test();
        db.query("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:SEED]->(b)")
            .unwrap();
        assert!(hawdb_storage::store::matched_node_label_scans_for_test() > scans);

        let scans = hawdb_storage::store::matched_node_label_scans_for_test();
        db.query_with_params(LOAD_KNOWS, &file(&knows)).unwrap();
        // The commit resolved every endpoint by internal id: no label scan
        // per row and endpoint under the write guard.
        assert_eq!(
            hawdb_storage::store::matched_node_label_scans_for_test(),
            scans
        );
        assert_eq!(knows_count(&mut db), expected, "indexed={indexed}");
        let weights = db
            .query(
                "MATCH (a:Person {id: 7})-[k:KNOWS]->(b:Person {id: 7}) \
                 RETURN k.w AS w ORDER BY w",
            )
            .unwrap();
        // The duplicate `id: 7` keeps the Cartesian product: 2 x 2 pairs.
        assert_eq!(column(&weights, "w"), [-1, -1, -1, -1].map(Value::Int));

        // The same form fed by UNWIND takes the same path.
        let rows = (0..PEOPLE)
            .map(|src| {
                Value::Map(BTreeMap::from([
                    ("src".to_string(), Value::Int(src)),
                    ("dst".to_string(), Value::Int((src + 1) % PEOPLE)),
                ]))
            })
            .collect();
        let scans = hawdb_storage::store::matched_node_label_scans_for_test();
        db.query_with_params(
            "UNWIND $rows AS r MATCH (a:Person {id: r.src}), (b:Person {id: r.dst}) \
             CREATE (a)-[:NEXT]->(b)",
            &BTreeMap::from([("rows".to_string(), Value::List(rows))]),
        )
        .unwrap();
        assert_eq!(
            hawdb_storage::store::matched_node_label_scans_for_test(),
            scans
        );
        // Rows touching `id: 7` match both copies.
        assert_eq!(
            count(
                &mut db,
                "MATCH (:Person)-[n:NEXT]->(:Person) RETURN count(n) AS c"
            ),
            PEOPLE + 2
        );
    }
}

#[test]
fn unwind_relationship_create_and_merge_resolve_endpoints_on_every_path() {
    const PEOPLE: i64 = 200;
    let rows = Value::List(
        (0..PEOPLE)
            .map(|src| {
                Value::Map(BTreeMap::from([
                    ("src".to_string(), Value::Int(src)),
                    ("dst".to_string(), Value::Int((src + 1) % PEOPLE)),
                ]))
            })
            .collect(),
    );
    let parameters = BTreeMap::from([("rows".to_string(), rows)]);
    let create = "UNWIND $rows AS r MATCH (a:Person {id: r.src}), (b:Person {id: r.dst}) \
         CREATE (a)-[:NEXT]->(b)";
    let merge = "UNWIND $rows AS r MATCH (a:Person {id: r.src}), (b:Person {id: r.dst}) \
         MERGE (a)-[:LINK]->(b)";
    for explicit in [false, true] {
        let mut db = Database::new();
        db.query_with_params(
            "UNWIND $ids AS i CREATE (:Person {id: i.id})",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(
                    (0..PEOPLE)
                        .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
                        .collect(),
                ),
            )]),
        )
        .unwrap();
        // CREATE goes through the statement-level CREATE resolver and MERGE
        // through the relationship MERGE resolver; either one missing would
        // fall back to a label scan per row and endpoint under the guard.
        let scans = hawdb_storage::store::matched_node_label_scans_for_test();
        if explicit {
            let mut transaction = db.begin_transaction().unwrap();
            for query in [create, merge, merge] {
                transaction.query_with_params(query, &parameters).unwrap();
            }
            transaction.commit().unwrap();
        } else {
            for query in [create, merge, merge] {
                db.query_with_params(query, &parameters).unwrap();
            }
        }
        assert_eq!(
            hawdb_storage::store::matched_node_label_scans_for_test(),
            scans,
            "explicit={explicit}"
        );
        for rel in ["NEXT", "LINK"] {
            assert_eq!(
                count(
                    &mut db,
                    &format!("MATCH (:Person)-[r:{rel}]->(:Person) RETURN count(r) AS c")
                ),
                PEOPLE,
                "explicit={explicit} {rel}"
            );
        }
    }
}

#[test]
fn relationship_loads_see_nodes_staged_earlier_in_the_transaction() {
    let dir = ImportDir::new("pending");
    let people = dir.write("people.csv", people_csv(50));
    let (knows, expected) = knows_csv(50);
    let knows = dir.write("knows.csv", knows);
    let mut db = dir.database();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_with_params(LOAD_PEOPLE, &file(&people))
        .unwrap();
    transaction
        .query_with_params(LOAD_KNOWS, &file(&knows))
        .unwrap();
    transaction
        .query_with_params(
            "UNWIND $rows AS r MATCH (a:Person {id: r.src}), (b:Person {id: r.dst}) \
             CREATE (a)-[:NEXT]->(b)",
            &BTreeMap::from([(
                "rows".to_string(),
                Value::List(vec![Value::Map(BTreeMap::from([
                    ("src".to_string(), Value::Int(7)),
                    ("dst".to_string(), Value::Int(8)),
                ]))]),
            )]),
        )
        .unwrap();
    let staged = transaction
        .query("MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS c")
        .unwrap();
    assert_eq!(staged.rows[0]["c"], Value::Int(expected));
    transaction.commit().unwrap();
    assert_eq!(knows_count(&mut db), expected);
    assert_eq!(
        count(
            &mut db,
            "MATCH (:Person)-[n:NEXT]->(:Person) RETURN count(n) AS c"
        ),
        2
    );
}

#[test]
fn staged_imports_are_charged_to_the_query_memory_budget() {
    let dir = ImportDir::new("ledger");
    let people = dir.write("people.csv", people_csv(3_000));
    let mut config = dir.config();
    config.execution_memory.query_memory_bytes = std::num::NonZeroUsize::new(256 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    let error = db
        .query_with_params(LOAD_PEOPLE, &file(&people))
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::ResourceBudgetExceeded(_)),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("staged graph mutations"),
        "{error}"
    );
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 0);
    let small = dir.write("small.csv", people_csv(20));
    db.query_with_params(LOAD_PEOPLE, &file(&small)).unwrap();
    assert_eq!(count(&mut db, "MATCH (p:Person) RETURN count(p) AS c"), 21);
}

#[test]
fn fan_out_beyond_the_affected_row_limit_fails_before_the_commit() {
    const PEOPLE: usize = 60;
    let dir = ImportDir::new("fan-out");
    let countries = dir.write("countries.csv", "c\nnl\nnl\nnl\n");
    let mut config = dir.config();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(100).unwrap();
    let mut db = Database::new_with_config(config);
    db.query_with_params(
        "UNWIND $people AS p CREATE (:Person {id: p.id, country: p.country})",
        &BTreeMap::from([(
            "people".to_string(),
            Value::List(
                (0..PEOPLE as i64)
                    .map(|id| {
                        Value::Map(BTreeMap::from([
                            ("id".to_string(), Value::Int(id)),
                            ("country".to_string(), string("nl")),
                        ]))
                    })
                    .collect(),
            ),
        )]),
    )
    .unwrap();
    db.query("CREATE (:Country {code: 'nl'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    // Every row pairs all 60 people with the country: the second row would
    // exceed the 100 affected rows the commit admits.
    let load = "LOAD CSV WITH HEADERS FROM $f AS row \
         MATCH (p:Person {country: row.c}), (c:Country {code: row.c}) \
         CREATE (p)-[:IN]->(c)";
    let unwind = "UNWIND $rows AS row \
         MATCH (p:Person {country: row.c}), (c:Country {code: row.c}) \
         CREATE (p)-[:IN]->(c)";
    let rows = BTreeMap::from([(
        "rows".to_string(),
        Value::List(vec![
            Value::Map(BTreeMap::from([(
                "c".to_string(),
                string("nl")
            )]));
            3
        ]),
    )]);
    for (query, parameters) in [(load, file(&countries)), (unwind, rows)] {
        let error = db.query_with_params(query, &parameters).unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        assert!(
            error
                .to_string()
                .contains("mutation would exceed max_mutation_affected_rows"),
            "{error}"
        );
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(
            count(&mut db, "MATCH ()-[r:IN]->() RETURN count(r) AS c"),
            0
        );
    }
    let one = dir.write("one.csv", "c\nnl\n");
    db.query_with_params(load, &file(&one)).unwrap();
    assert_eq!(
        count(&mut db, "MATCH ()-[r:IN]->() RETURN count(r) AS c"),
        PEOPLE as i64
    );
}

#[test]
fn missing_sources_are_execution_errors_and_bad_locations_are_semantic() {
    let dir = ImportDir::new("classes");
    let mut db = dir.database();
    let query = "LOAD CSV FROM $f AS line RETURN line";
    for location in ["missing.csv", "file:///missing.csv"] {
        let error = db.query_with_params(query, &file(location)).unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        assert!(error.to_string().contains("cannot access"), "{error}");
        let error = db
            .query_with_params(CREATE_PERSONS, &file(location))
            .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    }
    std::fs::create_dir(dir.root.join("folder")).unwrap();
    let error = db.query_with_params(query, &file("folder")).unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    assert!(error.to_string().contains("not a regular file"), "{error}");
    for location in [
        "../missing.csv",
        "https://example.com/a.csv",
        "file://host/a.csv",
    ] {
        let error = db.query_with_params(query, &file(location)).unwrap_err();
        assert!(
            matches!(error, HawDBError::Semantic(_)),
            "{location}: {error:?}"
        );
    }
    let error = Database::new()
        .query_with_params(query, &file("missing.csv"))
        .unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)), "{error:?}");
}

#[cfg(unix)]
#[test]
fn fifo_sources_fail_without_blocking() {
    let dir = ImportDir::new("fifo");
    let fifo = dir.root.join("pipe.csv");
    let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    // SAFETY: `path` is a valid NUL-terminated string for the duration of the call.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let (sender, receiver) = std::sync::mpsc::channel();
    let config = dir.config();
    let location = fifo.to_str().unwrap().to_string();
    std::thread::spawn(move || {
        let mut db = Database::new_with_config(config);
        let read = db
            .query_with_params("LOAD CSV FROM $f AS line RETURN line", &file(&location))
            .map(|_| ());
        let write = db
            .query_with_params(CREATE_PERSONS, &file(&location))
            .map(|_| ());
        let _ = sender.send((read, write));
    });
    // Opening a FIFO for reading would block until a writer appeared.
    let (read, write) = receiver
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("LOAD CSV must refuse a FIFO without opening it");
    for result in [read, write] {
        let error = result.unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        assert!(error.to_string().contains("not a regular file"), "{error}");
    }
}
