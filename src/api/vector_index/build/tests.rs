// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::Vector;
use hawdb_qos::RuntimeWorkRequest;
use std::num::NonZeroU64;
use std::path::PathBuf;

mod diskann;
mod eligibility;
mod incremental;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-vector-source-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn database(&self) -> Database {
        Database::open(self.0.join("database")).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn registration() -> SearchProjectionConsumerOptions {
    SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap())
}

fn sql_field() -> VectorIndexField {
    VectorIndexField::SqlColumn {
        table: "vectors".into(),
        column: "embedding".into(),
    }
}

fn distances(db: &mut Database, consumer: &SearchProjectionConsumer) -> Vec<f64> {
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(consumer, &permit)
        .unwrap();
    let output = read
        .query_candidates(
            &permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();
    output.hits().iter().map(|hit| hit.distance).collect()
}

#[test]
fn vector_index_builds_both_canonical_source_families_and_reopens() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (tenant TEXT, id BIGINT, embedding VECTOR(2), PRIMARY KEY (tenant, id))").unwrap();
    db.query("CREATE NODE TABLE Document").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
        .unwrap();
    for (id, coordinates) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
        let vector = Value::Vector(Vector::new(&coordinates).unwrap());
        db.query_sql_with_params(
            "INSERT INTO vectors (tenant, id, embedding) VALUES ($1, $2, $3)",
            &[Value::String("a\0b".into()), Value::Int(id), vector.clone()],
        )
        .unwrap();
        db.query_with_params(
            "CREATE (:Document {id: $id, embedding: $embedding})",
            &BTreeMap::from([("id".into(), Value::Int(id)), ("embedding".into(), vector)]),
        )
        .unwrap();
    }
    db.query_sql("INSERT INTO vectors (tenant, id, embedding) VALUES ('null', 4, NULL)")
        .unwrap();
    db.query("CREATE (:Document {id: 4})").unwrap();
    db.query("CREATE (:Other {id: 5})").unwrap();
    let targets = [
        ("sql", sql_field()),
        (
            "graph",
            VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            },
        ),
    ];
    for (name, field) in &targets {
        let consumer = db
            .create_vector_index(
                name,
                field,
                fixture.0.join(name),
                registration(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(consumer.document_count().unwrap(), 3);
        assert_eq!(distances(&mut db, &consumer), [0.0, 1.0, 2.0]);
        assert_eq!(
            consumer
                .vector_index_descriptor()
                .unwrap()
                .unwrap()
                .dimension()
                .get(),
            2
        );
    }
    drop(db);
    let mut db = fixture.database();
    for (name, _) in &targets {
        let consumer = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new(*name).unwrap(),
                fixture.0.join(name),
            )
            .unwrap();
        assert_eq!(distances(&mut db, &consumer), [0.0, 1.0, 2.0]);
    }
}

#[test]
fn vector_index_build_refuses_incomplete_input_and_invalid_nonnull_vectors() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    for id in 0..3 {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[
                Value::Int(id),
                Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
            ],
        )
        .unwrap();
    }
    let limited = VectorIndexBuildOptions {
        max_documents: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    };
    assert!(matches!(
        db.create_vector_index(
            "limited",
            &sql_field(),
            fixture.0.join("limited"),
            registration(),
            limited
        ),
        Err(VectorIndexBuildError::Source(
            ProjectionFeedError::Callback(HawDBError::ResourceBudgetExceeded(_))
        ))
    ));
    let mut limited = VectorIndexBuildOptions::default();
    limited.feed.work = NonZeroUsize::new(1).unwrap();
    assert!(matches!(
        db.create_vector_index(
            "feed",
            &sql_field(),
            fixture.0.join("feed"),
            registration(),
            limited
        ),
        Err(VectorIndexBuildError::Source(
            ProjectionFeedError::BudgetExceeded(_)
        ))
    ));
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
        &[
            Value::Int(3),
            Value::Vector(Vector::new(&[0.0, 0.0]).unwrap()),
        ],
    )
    .unwrap();
    assert!(db
        .create_vector_index(
            "zero",
            &sql_field(),
            fixture.0.join("zero"),
            registration(),
            Default::default()
        )
        .is_err());
    db.query_sql("DELETE FROM vectors WHERE id = 3").unwrap();
    let consumer = db
        .create_vector_index(
            "valid",
            &sql_field(),
            fixture.0.join("valid"),
            registration(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(distances(&mut db, &consumer), [0.0, 0.0, 0.0]);
}

#[test]
fn vector_index_build_preserves_empty_and_null_only_dimensions() {
    for with_null in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        if with_null {
            db.query_sql("INSERT INTO vectors (id, embedding) VALUES (1, NULL)")
                .unwrap();
        }
        let consumer = db
            .create_vector_index(
                "empty",
                &sql_field(),
                fixture.0.join("empty"),
                registration(),
                Default::default(),
            )
            .unwrap();
        assert!(distances(&mut db, &consumer).is_empty());
        drop(consumer);
        drop(db);
        let mut db = fixture.database();
        let consumer = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new("empty").unwrap(),
                fixture.0.join("empty"),
            )
            .unwrap();
        assert_eq!(
            consumer
                .vector_index_descriptor()
                .unwrap()
                .unwrap()
                .dimension()
                .get(),
            2
        );
        assert!(distances(&mut db, &consumer).is_empty());
    }
}

#[test]
fn vector_index_read_domain_rejects_new_source_and_preserves_old_snapshot() {
    use crate::VectorIndexReadError;

    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
        &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "index",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let old = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    db.query_sql("DELETE FROM vectors WHERE id = 1").unwrap();
    assert!(matches!(
        db.begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit),
        Err(VectorIndexReadError::SourceEpochMismatch { index, source })
            if index == old.source_epoch() && source > index
    ));
    let foreign_fixture = Fixture::new();
    let foreign = foreign_fixture.database();
    assert!(matches!(
        foreign
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit),
        Err(VectorIndexReadError::DatabaseMismatch)
    ));
    drop(consumer);
    drop(db);
    let result = old
        .query_candidates(
            &permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
        )
        .unwrap();
    assert_eq!(result.hits().len(), 1);
    assert_eq!(result.hits()[0].distance, 0.0);
    assert_eq!(result.report().source_epoch, old.source_epoch());
    assert_eq!(result.report().generation, old.generation());
    assert_eq!(
        old.source.query_sql("SELECT id FROM vectors").unwrap().rows[0]["id"],
        Value::Int(1)
    );
}

#[test]
fn vector_index_read_domain_rejects_a_forged_schema_definition() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let consumer = db
        .create_search_projection_consumer(
            SearchProjectionConsumerId::new("forged").unwrap(),
            fixture.0.join("forged"),
            registration(),
            |source, index| {
                let definition = source.resolve_vector_index(
                    "forged",
                    &sql_field(),
                    VectorDistance::Cosine,
                    VectorIndexBuildOptions::default().backend,
                )?;
                let mut digest = *definition.schema_digest();
                digest[0] ^= 1;
                index.bind_vector_index(VectorIndexDescriptor::new(
                    definition.name().into(),
                    definition.target().clone(),
                    definition.dimension(),
                    definition.distance(),
                    definition.backend(),
                    digest,
                )?)
            },
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    assert!(matches!(
        db.begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit),
        Err(crate::VectorIndexReadError::SchemaMismatch)
    ));
}

#[test]
fn vector_index_rebuild_replaces_updates_deletes_and_nulls_without_mixing_old_pins() {
    fn language_rows(
        read: &mut crate::VectorIndexReadTransaction,
        permit: &hawdb_qos::RuntimePermit,
        graph: bool,
    ) -> Vec<(Value, Value)> {
        use crate::{
            QueryRequest, QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexSqlRequest,
        };
        let budget = VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        };
        let mut expected = None;
        for mode in [
            hawdb_core::VectorIndexQueryMode::AnnWindow,
            hawdb_core::VectorIndexQueryMode::Exact,
            hawdb_core::VectorIndexQueryMode::FilteredAnn,
        ] {
            let sql = match mode {
                hawdb_core::VectorIndexQueryMode::AnnWindow => "SELECT id, distance FROM vector_index_search('index', $1, candidates => 10, top_k => 10) ORDER BY distance, id",
                hawdb_core::VectorIndexQueryMode::Exact => "SELECT id, distance FROM vector_index_search('index', $1, mode => 'exact', top_k => 10, max_work => 100) ORDER BY distance, id",
                hawdb_core::VectorIndexQueryMode::FilteredAnn => "SELECT id, distance FROM vector_index_search('index', $1, mode => 'filtered_ann', candidates => 10, top_k => 10, max_work => 100) ORDER BY distance, id",
            };
            let cypher = match mode {
                hawdb_core::VectorIndexQueryMode::AnnWindow => "CALL vector_index_search('index', $q, candidates := 10, topK := 10) YIELD node AS n, distance RETURN n.id AS id, distance ORDER BY distance, id",
                hawdb_core::VectorIndexQueryMode::Exact => "MATCH (d:Document) CALL vector_index_search('index', $q, mode := 'exact', eligible := d, topK := 10, maxWork := 100) YIELD node AS n, distance RETURN n.id AS id, distance ORDER BY distance, id",
                hawdb_core::VectorIndexQueryMode::FilteredAnn => "MATCH (d:Document) CALL vector_index_search('index', $q, mode := 'filtered_ann', eligible := d, candidates := 10, topK := 10, maxWork := 100) YIELD node AS n, distance RETURN n.id AS id, distance ORDER BY distance, id",
            };
            let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
            let mut rows = Vec::new();
            let mut collect = |row: crate::executor::Row| {
                rows.push((row["id"].clone(), row["distance"].clone()));
                Ok(())
            };
            let (epoch, generation) = if graph {
                let parameters = BTreeMap::from([("q".into(), embedding)]);
                let report = read
                    .query_cypher_streaming(
                        permit,
                        budget,
                        QueryRequest::new(cypher).with_params(&parameters),
                        &mut collect,
                    )
                    .unwrap();
                let report = report.index().unwrap();
                (report.source_epoch, report.generation)
            } else {
                let report = read
                    .query_sql_streaming(
                        permit,
                        budget,
                        VectorIndexSqlRequest {
                            sql,
                            parameters: &[embedding],
                            output_limits: QueryStreamOptions::default(),
                            task: Default::default(),
                        },
                        &mut collect,
                    )
                    .unwrap();
                (report.index().source_epoch, report.index().generation)
            };
            assert_eq!(epoch, read.source_epoch());
            assert_eq!(generation, read.generation());
            if let Some(expected) = &expected {
                assert_eq!(&rows, expected);
            } else {
                expected = Some(rows);
            }
        }
        expected.unwrap()
    }
    for graph in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let field = if graph {
            db.query("CREATE NODE TABLE Document").unwrap();
            db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
                .unwrap();
            VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            }
        } else {
            db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
                .unwrap();
            sql_field()
        };
        for (id, coordinates) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
            let vector = Value::Vector(Vector::new(&coordinates).unwrap());
            if graph {
                db.query_with_params(
                    "CREATE (:Document {id: $id, embedding: $q})",
                    &BTreeMap::from([("id".into(), Value::Int(id)), ("q".into(), vector)]),
                )
                .unwrap();
            } else {
                db.query_sql_with_params(
                    "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                    &[Value::Int(id), vector],
                )
                .unwrap();
            }
        }
        let mut consumer = db
            .create_vector_index(
                "index",
                &field,
                fixture.0.join("index"),
                registration(),
                Default::default(),
            )
            .unwrap();
        let permit = db
            .search_projection_consumer_read_permit(
                RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
            )
            .unwrap();
        let mut old = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit)
            .unwrap();
        let orthogonal = Value::Vector(Vector::new(&[0.0, 1.0]).unwrap());
        let opposite = Value::Vector(Vector::new(&[-1.0, 0.0]).unwrap());
        if graph {
            db.query_with_params(
                "MATCH (n:Document) WHERE n.id = 1 SET n.embedding = $q",
                &BTreeMap::from([("q".into(), orthogonal)]),
            )
            .unwrap();
            db.query("MATCH (n:Document) WHERE n.id = 2 DELETE n")
                .unwrap();
            db.query("MATCH (n:Document) WHERE n.id = 3 SET n.embedding = NULL")
                .unwrap();
            db.query_with_params(
                "CREATE (:Document {id: 4, embedding: $q})",
                &BTreeMap::from([("q".into(), opposite)]),
            )
            .unwrap();
        } else {
            db.query_sql_with_params(
                "UPDATE vectors SET embedding = $1 WHERE id = 1",
                &[orthogonal],
            )
            .unwrap();
            db.query_sql("DELETE FROM vectors WHERE id = 2").unwrap();
            db.query_sql("UPDATE vectors SET embedding = NULL WHERE id = 3")
                .unwrap();
            db.query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES (4, $1)",
                &[opposite],
            )
            .unwrap();
            // A compatible schema change still requires a new schema digest.
            db.query_sql("ALTER TABLE vectors ADD COLUMN category TEXT")
                .unwrap();
        }
        assert!(db
            .catch_up_search_projection_consumer(&mut consumer, 10, 10, 10, |_, _| panic!(
                "legacy mapping must not run for a vector field"
            ))
            .is_err());
        let cancelled = VectorIndexBuildOptions::default();
        cancelled.task.cancellation().cancel();
        assert!(db.rebuild_vector_index(&mut consumer, cancelled).is_err());
        db.rebuild_vector_index(&mut consumer, Default::default())
            .unwrap();
        let mut current = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit)
            .unwrap();
        assert!(current.source_epoch() > old.source_epoch());
        assert!(current.generation() > old.generation());
        if !graph {
            assert_ne!(
                current.definition().schema_digest(),
                old.definition().schema_digest()
            );
        }
        for (read, expected) in [(&old, vec![0.0, 1.0, 2.0]), (&current, vec![1.0, 2.0])] {
            let result = read
                .query_candidates(
                    &permit,
                    Default::default(),
                    &[1.0, 0.0],
                    NonZeroUsize::new(10).unwrap(),
                    NonZeroUsize::new(10).unwrap(),
                )
                .unwrap();
            assert_eq!(
                result
                    .hits()
                    .iter()
                    .map(|hit| hit.distance)
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(result.report().generation, read.generation());
            assert_eq!(result.report().source_epoch, read.source_epoch());
        }
        assert_eq!(
            language_rows(&mut old, &permit, graph),
            vec![
                (Value::Int(1), Value::Float(0.0)),
                (Value::Int(2), Value::Float(1.0)),
                (Value::Int(3), Value::Float(2.0)),
            ]
        );
        let expected_current = vec![
            (Value::Int(1), Value::Float(1.0)),
            (Value::Int(4), Value::Float(2.0)),
        ];
        assert_eq!(
            language_rows(&mut current, &permit, graph),
            expected_current
        );
        let definition = current.definition().clone();
        let query_vector = Vector::new(&[1.0, 0.0]).unwrap();
        let memory = hawdb_executor::ExecutionMemoryConfig::default();
        let ledger = hawdb_executor::QueryMemoryLedger::for_query(&memory, None).unwrap();
        let account = ledger.account(
            hawdb_executor::QueryMemoryClass::BlockingState,
            "same-epoch vector domain",
            memory.blocking_operator_bytes,
        );
        let mut domain = hawdb_executor::vector_eligibility::VectorEligibleDomain::new(
            &definition,
            current.pin.vector_index_read_identity(),
            query_vector.as_ref(),
            hawdb_core::VectorIndexQueryMode::FilteredAnn,
            0,
            &account,
            None,
        )
        .unwrap();
        domain.finish().unwrap();
        db.rebuild_vector_index(&mut consumer, Default::default())
            .unwrap();
        let mut same_epoch = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit)
            .unwrap();
        assert_eq!(same_epoch.source_epoch(), current.source_epoch());
        assert!(same_epoch.generation() > current.generation());
        let error = same_epoch
            .pin
            .query_vector_index(
                &permit,
                Default::default(),
                hawdb_search::VectorIndexQuery {
                    descriptor: &definition,
                    embedding: &[1.0, 0.0],
                    candidate_limit: NonZeroUsize::MIN,
                    top_k: NonZeroUsize::MIN,
                    mode: hawdb_core::VectorIndexQueryMode::FilteredAnn,
                    eligible: Some(&domain),
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("another source or publication"),
            "{error}"
        );
        drop(domain);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(
            language_rows(&mut current, &permit, graph),
            expected_current
        );
        assert_eq!(
            language_rows(&mut same_epoch, &permit, graph),
            expected_current
        );
        let generation = same_epoch.generation();
        drop(same_epoch);
        drop(current);
        drop(old);
        drop(consumer);
        drop(permit);
        drop(db);
        let mut db = fixture.database();
        let consumer = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new("index").unwrap(),
                fixture.0.join("index"),
            )
            .unwrap();
        assert_eq!(distances(&mut db, &consumer), [1.0, 2.0]);
        let permit = db
            .search_projection_consumer_read_permit(
                RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
            )
            .unwrap();
        assert_eq!(
            consumer.pin(&permit).unwrap().publication_generation(),
            generation
        );
        let mut reopened = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit)
            .unwrap();
        assert_eq!(
            language_rows(&mut reopened, &permit, graph),
            expected_current
        );
    }
}

#[test]
fn vector_index_cypher_pipeline_preserves_distance_window_and_canonical_traversal() {
    use crate::{QueryRequest, VectorIndexLanguageBudget};
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query("CREATE NODE TABLE Document").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
        .unwrap();
    for (id, coordinates) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
        db.query_with_params(
            "CREATE (:Document {id: $id, embedding: $q})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "q".into(),
                    Value::Vector(Vector::new(&coordinates).unwrap()),
                ),
            ]),
        )
        .unwrap();
    }
    db.query(
        "MATCH (a:Document), (b:Document) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:RELATED]->(b)",
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "docs",
            &VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            },
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let mut read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let budget = VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    let mut parameters = BTreeMap::from([
        ("q".into(), Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())),
        ("k".into(), Value::Int(3)),
    ]);
    let query = "CALL vector_index_search('docs', $q, candidates := 8, topK := $k) YIELD node AS m, distance RETURN m.id AS id, distance ORDER BY distance, id";
    let mut rows = Vec::new();
    let report = read
        .query_cypher_streaming(
            &permit,
            budget,
            QueryRequest::new(query).with_params(&parameters),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row["distance"].clone())
            .collect::<Vec<_>>(),
        [Value::Float(0.0), Value::Float(1.0), Value::Float(2.0)]
    );
    assert_eq!(report.index().unwrap().generation, read.generation());
    assert_eq!(report.index().unwrap().top_k, 3);
    drop(report);
    let filtered = "CALL vector_index_search('docs', $q, candidates := 8, topK := $k) YIELD node AS m, distance WITH m, distance WHERE m.id = 3 RETURN m.id AS id, distance";
    parameters.insert("k".into(), Value::Int(2));
    let mut filtered_rows = Vec::new();
    read.query_cypher_streaming(
        &permit,
        budget,
        QueryRequest::new(filtered).with_params(&parameters),
        |row| {
            filtered_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert!(
        filtered_rows.is_empty(),
        "filtering must not refill the declared ANN window"
    );
    parameters.insert("k".into(), Value::Int(3));
    read.query_cypher_streaming(
        &permit,
        budget,
        QueryRequest::new(filtered).with_params(&parameters),
        |row| {
            filtered_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(filtered_rows.len(), 1);
    assert_eq!(filtered_rows[0]["distance"], Value::Float(2.0));
    let traverse = "CALL vector_index_search('docs', $q, candidates := 8, topK := 1) YIELD node AS m, distance MATCH (m)-[:RELATED]->(n:Document) RETURN n.id AS id, distance";
    let mut related = Vec::new();
    read.query_cypher_streaming(
        &permit,
        budget,
        QueryRequest::new(traverse).with_params(&parameters),
        |row| {
            related.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0]["id"], Value::Int(3));
    assert_eq!(related[0]["distance"], Value::Float(0.0));
}

#[test]
fn vector_index_sql_pipeline_preserves_composite_keys_distance_and_post_filtering() {
    use crate::{QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexSqlRequest};
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (tenant TEXT, id BIGINT, embedding VECTOR(2), PRIMARY KEY (tenant, id))").unwrap();
    for (tenant, id, coordinates) in [
        ("a\0b", 1, [1.0, 0.0]),
        ("other", 1, [0.0, 1.0]),
        ("a\0b", 2, [-1.0, 0.0]),
    ] {
        db.query_sql_with_params(
            "INSERT INTO vectors (tenant, id, embedding) VALUES ($1, $2, $3)",
            &[
                Value::String(tenant.into()),
                Value::Int(id),
                Value::Vector(Vector::new(&coordinates).unwrap()),
            ],
        )
        .unwrap();
    }
    db.query_sql("INSERT INTO vectors (tenant, id, embedding) VALUES ('null', 3, NULL)")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let budget = VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    let mut parameters = vec![
        Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
        Value::Int(3),
    ];
    let query = "SELECT v.tenant, v.id, distance FROM vector_index_search('rows', $1, candidates => 8, top_k => $2) AS v ORDER BY distance, v.id";
    let mut rows = Vec::new();
    let report = read
        .query_sql_streaming(
            &permit,
            budget,
            VectorIndexSqlRequest {
                sql: query,
                parameters: &parameters,
                output_limits: QueryStreamOptions::default(),
                task: Default::default(),
            },
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row["distance"].clone())
            .collect::<Vec<_>>(),
        [Value::Float(0.0), Value::Float(1.0), Value::Float(2.0)]
    );
    assert_eq!(rows[0]["tenant"], Value::String("a\0b".into()));
    assert_eq!(rows[1]["tenant"], Value::String("other".into()));
    assert_eq!(report.index().generation, read.generation());
    assert_eq!(report.output_rows, 3);
    drop(report);
    let filtered = "SELECT v.id, distance FROM vector_index_search('rows', $1, candidates => 8, top_k => $2) AS v WHERE v.id = 2 ORDER BY distance LIMIT 1";
    for (k, expected) in [(2, 0), (3, 1)] {
        parameters[1] = Value::Int(k);
        let mut filtered_rows = Vec::new();
        read.query_sql_streaming(
            &permit,
            budget,
            VectorIndexSqlRequest {
                sql: filtered,
                parameters: &parameters,
                output_limits: QueryStreamOptions::default(),
                task: Default::default(),
            },
            |row| {
                filtered_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(filtered_rows.len(), expected);
        if expected == 1 {
            assert_eq!(filtered_rows[0]["distance"], Value::Float(2.0));
        }
    }
    assert!(
        db.begin_read_transaction()
            .unwrap()
            .query_sql_with_params(query, &parameters)
            .is_err(),
        "ordinary SQL execution cannot silently consume an unbound vector index"
    );
}

#[test]
fn vector_index_sql_denies_wide_canonical_rows_before_delivery_and_can_retry() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2), body TEXT)")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding, body) VALUES ($1, $2, $3)",
        &[
            Value::Int(1),
            Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
            Value::String("x".repeat(256 * 1024)),
        ],
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let parameters = [Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())];
    for (working, succeeds) in [(32 * 1024 * 1024 + 64 * 1024, false), (128 << 20, true)] {
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(working, 4 << 20));
        let mut delivered = 0;
        let result = read.query_sql_streaming(&permit, VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        }, VectorIndexSqlRequest {
            sql: "SELECT body, distance FROM vector_index_search('rows', $1, candidates => 8, top_k => 1)",
            parameters: &parameters, output_limits: QueryStreamOptions::default(), task,
        }, |row| { assert_eq!(row["body"], Value::String("x".repeat(256 * 1024))); delivered += 1; Ok(()) });
        if succeeds {
            assert_eq!(result.unwrap().output_rows, 1);
            assert_eq!(delivered, 1);
        } else {
            assert!(
                matches!(
                    result,
                    Err(VectorIndexReadError::Query(
                        HawDBError::ResourceBudgetExceeded(_)
                    ))
                ),
                "{result:?}"
            );
            assert_eq!(delivered, 0);
        }
    }
    let repeated = (0..100)
        .map(|ordinal| format!("body AS body{ordinal}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sort = (0..100)
        .map(|ordinal| format!("body{ordinal}"))
        .collect::<Vec<_>>()
        .join(", ");
    let repeated = format!("SELECT {repeated} FROM vector_index_search('rows', $1, candidates => 8, top_k => 1) ORDER BY {sort}");
    let result = read.query_sql_streaming(
        &permit,
        VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        },
        VectorIndexSqlRequest {
            sql: &repeated,
            parameters: &parameters,
            output_limits: QueryStreamOptions::default(),
            task: RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(40 << 20, 4 << 20)),
        },
        |_| panic!("unadmitted projection must not deliver"),
    );
    let Err(VectorIndexReadError::Query(HawDBError::ResourceBudgetExceeded(error))) = result else {
        panic!("projection must exceed its grant");
    };
    assert_eq!(error.owner, "VectorIndexSqlScan source");
    assert!(error.requested >= 100 * 256 * 1024, "{error:?}");
}

#[test]
fn vector_index_rebuild_producer_budget_refusal_keeps_selected_generation() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    // Even an empty projection must fund its fixed compression workspace.
    let path = fixture.0.join("index");
    let mut consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            &path,
            registration(),
            Default::default(),
        )
        .unwrap();
    let result = db.rebuild_vector_index(
        &mut consumer,
        VectorIndexBuildOptions {
            memory_bytes: NonZeroUsize::new(4 << 20).unwrap(),
            ..Default::default()
        },
    );
    assert!(
        matches!(
            result,
            Err(VectorIndexBuildError::Consumer(
                crate::SearchProjectionConsumerError::Database(HawDBError::ResourceBudgetExceeded(
                    _
                ))
            ))
        ),
        "{result:?}"
    );
    assert!(distances(&mut db, &consumer).is_empty());
    let before = consumer.projection().receipt();
    db.rebuild_vector_index(&mut consumer, Default::default())
        .unwrap();
    let after = consumer.projection().receipt();
    assert_eq!(before.source_epoch, after.source_epoch);
    assert_ne!(
        before.binding.checkpoint_uuid,
        after.binding.checkpoint_uuid
    );
    drop(consumer);
    drop(db);
    let mut db = fixture.database();
    let consumer = db
        .open_search_projection_consumer(&SearchProjectionConsumerId::new("rows").unwrap(), path)
        .unwrap();
    assert!(distances(&mut db, &consumer).is_empty());
}

#[test]
fn vector_index_read_domain_rejects_another_branch_of_the_same_database() {
    use crate::{BranchCreateRequest, BranchSelector, VectorIndexReadError};
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let child = db
        .create_branch(BranchCreateRequest {
            name: Some("vector-child".into()),
            parent: BranchSelector::Name("main".into()),
            expected_source_commit_epoch: db.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "vector-child".into(),
        })
        .unwrap();
    db.use_branch(BranchSelector::Id(child.id)).unwrap();
    assert!(matches!(
        db.begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit),
        Err(VectorIndexReadError::BranchMismatch)
    ));
}

#[test]
fn vector_index_language_rejects_foreign_governor_before_skipped_execution() {
    use crate::{QueryRequest, VectorIndexLanguageBudget, VectorIndexReadError};
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query("CREATE NODE TABLE Document").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "docs",
            &VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            },
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let work = RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1);
    let permit = db.search_projection_consumer_read_permit(work).unwrap();
    let other_fixture = Fixture::new();
    let mut other = other_fixture.database();
    let foreign = other.search_projection_consumer_read_permit(work).unwrap();
    let mut read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let budget = VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    let parameters =
        BTreeMap::from([("q".into(), Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()))]);
    for query in [
        "CALL vector_index_search('docs', $q, candidates := 8, topK := 1) YIELD node AS m, distance RETURN m.id AS id LIMIT 0",
        "MATCH (m:Document) RETURN m.id AS id",
    ] {
        let mut delivered = 0;
        let rejected = read.query_cypher_streaming(&foreign, budget, QueryRequest::new(query).with_params(&parameters), |_| { delivered += 1; Ok(()) });
        assert!(matches!(rejected, Err(VectorIndexReadError::Query(HawDBError::Execution(_)))), "{rejected:?}");
        assert_eq!(delivered, 0);
        let accepted = read.query_cypher_streaming(&permit, budget, QueryRequest::new(query).with_params(&parameters), |_| panic!("empty input"));
        assert!(accepted.is_ok(), "{accepted:?}");
    }
}

#[test]
fn vector_index_sql_admits_empty_projection_literals_and_parameters_before_copy() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
    let wide = "x".repeat(256 << 10);
    let literal = format!("SELECT '{wide}' AS body FROM vector_index_search('rows', $1, candidates => 8, top_k => 1) LIMIT 0");
    for (sql, parameters) in [
        (literal.as_str(), vec![embedding.clone()]),
        ("SELECT coalesce($2, NULL) AS body FROM vector_index_search('rows', $1, candidates => 8, top_k => 1) LIMIT 0", vec![embedding, Value::String(wide)]),
    ] {
        for (working, succeeds) in [(32 * 1024 * 1024 + 64 * 1024, false), (128 << 20, true)] {
            let result = read.query_sql_streaming(&permit, VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(), index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            }, VectorIndexSqlRequest { sql, parameters: &parameters, output_limits: QueryStreamOptions::default(), task: RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(working, 4 << 20)) }, |_| panic!("LIMIT 0 must not deliver"));
            if succeeds { assert_eq!(result.unwrap().output_rows, 0); }
            else { assert!(matches!(result, Err(VectorIndexReadError::Query(HawDBError::ResourceBudgetExceeded(_)))), "{result:?}"); }
        }
    }
}

#[test]
fn vector_index_sql_admits_like_matcher_scratch_before_evaluation() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2), body TEXT)")
        .unwrap();
    let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding, body) VALUES (1, $1, $2)",
        &[embedding.clone(), Value::String("Straße_ﬃ".into())],
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    for (operator, suffix) in [
        ("LIKE", ""),
        ("LIKE", "Straße!_ﬃ"),
        ("ILIKE", "STRASSE!_FFI"),
    ] {
        let pattern = format!("{}{suffix}", "%_".repeat(128 << 10));
        let parameters = [embedding.clone(), Value::String(pattern)];
        let sql = format!("SELECT id FROM vector_index_search('rows', $1, candidates => 8, top_k => 1) WHERE body {operator} $2 ESCAPE '!'");
        for (canonical, succeeds) in [(2 << 20, false), (64 << 20, true)] {
            let mut ids = Vec::new();
            let result = read.query_sql_streaming(
                &permit,
                VectorIndexLanguageBudget {
                    index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                    index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
                },
                VectorIndexSqlRequest {
                    sql: &sql,
                    parameters: &parameters,
                    output_limits: QueryStreamOptions::default(),
                    task: RuntimeTaskContext::default().with_memory_reservation(
                        RuntimeMemoryReservation::new((32 << 20) + canonical, 4 << 20),
                    ),
                },
                |row| {
                    ids.push(row["id"].clone());
                    Ok(())
                },
            );
            if succeeds {
                assert_eq!(result.unwrap().output_rows, 0);
                assert!(ids.is_empty());
            } else {
                assert!(ids.is_empty(), "unadmitted matcher must not deliver");
                assert!(
                    matches!(
                        result,
                        Err(VectorIndexReadError::Query(
                            HawDBError::ResourceBudgetExceeded(_)
                        ))
                    ),
                    "{result:?}"
                );
            }
        }
        let mut ids = Vec::new();
        let compact = [embedding.clone(), Value::String(format!("%{suffix}"))];
        read.query_sql_streaming(
            &permit,
            VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            },
            VectorIndexSqlRequest {
                sql: &sql,
                parameters: &compact,
                output_limits: QueryStreamOptions::default(),
                task: RuntimeTaskContext::default(),
            },
            |row| {
                ids.push(row["id"].clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(ids, [Value::Int(1)]);
    }

    for (predicate, pattern, expected_rows) in [
        (
            "id = 2 AND body LIKE $2 ESCAPE '!'",
            Value::String("dangling!".into()),
            Some(0),
        ),
        (
            "id = 1 OR body LIKE $2 ESCAPE '!'",
            Value::String("dangling!".into()),
            Some(1),
        ),
        (
            "id = 1 AND body LIKE $2 ESCAPE '!'",
            Value::String("dangling!".into()),
            None,
        ),
        ("body LIKE $2 ESCAPE '!'", Value::Null, Some(0)),
    ] {
        let sql = format!("SELECT id FROM vector_index_search('rows', $1, candidates => 8, top_k => 1) WHERE {predicate}");
        let parameters = [embedding.clone(), pattern];
        let mut delivered = 0;
        let result = read.query_sql_streaming(
            &permit,
            VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            },
            VectorIndexSqlRequest {
                sql: &sql,
                parameters: &parameters,
                output_limits: QueryStreamOptions::default(),
                task: RuntimeTaskContext::default(),
            },
            |_| {
                delivered += 1;
                Ok(())
            },
        );
        match expected_rows {
            Some(rows) => {
                assert_eq!(result.unwrap().output_rows, rows);
                assert_eq!(delivered, rows);
            }
            None => {
                assert_eq!(delivered, 0);
                assert!(
                    matches!(result, Err(VectorIndexReadError::Query(HawDBError::Semantic(message))) if message.contains("ends with its escape"))
                );
            }
        }
    }
}

#[test]
fn vector_index_sql_rejects_foreign_governor_before_execution() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let parameters = [Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())];
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
        &parameters,
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let work = RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1);
    let permit = db.search_projection_consumer_read_permit(work).unwrap();
    let other_fixture = Fixture::new();
    let mut other = other_fixture.database();
    let foreign = other.search_projection_consumer_read_permit(work).unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let budget = VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    for suffix in ["", " LIMIT 0"] {
        let sql = format!(
            "SELECT id FROM vector_index_search('rows', $1, candidates => 8, top_k => 1){suffix}"
        );
        let request = || VectorIndexSqlRequest {
            sql: &sql,
            parameters: &parameters,
            output_limits: QueryStreamOptions::default(),
            task: RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(128 << 20, 4 << 20)),
        };
        let mut delivered = 0;
        let rejected = read.query_sql_streaming(&foreign, budget, request(), |_| {
            delivered += 1;
            Ok(())
        });
        assert!(
            matches!(&rejected, Err(VectorIndexReadError::Query(HawDBError::Execution(message))) if message == "vector query permit belongs to another governor"),
            "{rejected:?}"
        );
        assert_eq!(delivered, 0);
        let accepted = read
            .query_sql_streaming(&permit, budget, request(), |_| {
                delivered += 1;
                Ok(())
            })
            .unwrap();
        let expected = usize::from(suffix.is_empty());
        assert_eq!(delivered, expected);
        assert_eq!(accepted.output_rows, expected);
    }
}

#[test]
fn vector_index_sql_rejects_unsupported_shapes_and_bindings() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
    let source = "vector_index_search('rows', $1, candidates => 8, top_k => 1)";
    let mut cases = vec![
        (
            format!("SELECT DISTINCT id FROM {source}"),
            vec![embedding.clone()],
            "single source",
        ),
        (
            format!("SELECT id FROM {source} GROUP BY id"),
            vec![embedding.clone()],
            "single source",
        ),
        (
            format!("SELECT id FROM {source} FOR UPDATE"),
            vec![embedding.clone()],
            "single source",
        ),
        (
            format!("SELECT v.id FROM {source} AS v JOIN vectors AS x ON v.id = x.id"),
            vec![embedding.clone()],
            "single source",
        ),
        (
            "SELECT id FROM vector_index_search('rows', 'bad', candidates => 8, top_k => 1)".into(),
            vec![],
            "typed vector parameter",
        ),
        (
            format!("SELECT id FROM {source}"),
            vec![Value::String("bad".into())],
            "typed vector parameter",
        ),
    ];
    for (candidates, top_k, diagnostic) in [
        (0, 1, "positive integers"),
        (-1, 1, "positive integers"),
        (8, 0, "positive integers"),
        (8, -1, "positive integers"),
        (1, 2, "top_k exceeds candidates"),
    ] {
        cases.push((format!("SELECT id FROM vector_index_search('rows', $1, candidates => {candidates}, top_k => {top_k})"), vec![embedding.clone()], diagnostic));
    }
    for (sql, parameters, diagnostic) in cases {
        let rejected = read.query_sql_streaming(
            &permit,
            VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            },
            VectorIndexSqlRequest {
                sql: &sql,
                parameters: &parameters,
                output_limits: QueryStreamOptions::default(),
                task: RuntimeTaskContext::default()
                    .with_memory_reservation(RuntimeMemoryReservation::new(128 << 20, 4 << 20)),
            },
            |_| panic!("rejected query delivered a row"),
        );
        assert!(
            matches!(&rejected, Err(VectorIndexReadError::Query(HawDBError::Semantic(message))) if message.contains(diagnostic)),
            "{sql}: {rejected:?}"
        );
    }
}

#[test]
fn vector_index_cypher_rejects_invalid_bindings_and_yields() {
    use crate::{QueryRequest, VectorIndexLanguageBudget, VectorIndexReadError};
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query("CREATE NODE TABLE Document").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
        .unwrap();
    let consumer = db
        .create_vector_index(
            "docs",
            &VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            },
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let mut read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let parameters = BTreeMap::from([
        ("q".into(), Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())),
        ("bad".into(), Value::String("bad".into())),
    ]);
    for (arguments, yields, diagnostic) in [
        (
            "'docs', $q, candidates := 8, topK := -1",
            "node",
            "topK must be a non-negative integer",
        ),
        (
            "'docs', $q, candidates := 8, topK := 0",
            "node",
            "0 < topK <= candidates",
        ),
        (
            "'docs', $q, candidates := 0, topK := 1",
            "node",
            "0 < topK <= candidates",
        ),
        (
            "'docs', $q, candidates := 1, topK := 2",
            "node",
            "0 < topK <= candidates",
        ),
        (
            "'docs', 'bad', candidates := 8, topK := 1",
            "node",
            "typed vector parameter",
        ),
        (
            "'docs', $bad, candidates := 8, topK := 1",
            "node",
            "typed vector parameter",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1",
            "node, node AS other",
            "YIELD columns must be distinct",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1",
            "node AS same, distance AS same",
            "YIELD aliases must be distinct",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1",
            "score",
            "unknown vector index YIELD column",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1",
            "distance AS node",
            "output names must be distinct",
        ),
    ] {
        let query =
            format!("CALL vector_index_search({arguments}) YIELD {yields} RETURN 1 AS value");
        let rejected = read.query_cypher_streaming(
            &permit,
            VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            },
            QueryRequest::new(&query).with_params(&parameters),
            |_| panic!("rejected query delivered a row"),
        );
        assert!(
            matches!(&rejected, Err(VectorIndexReadError::Query(HawDBError::Semantic(message))) if message.contains(diagnostic)),
            "{query}: {rejected:?}"
        );
    }
}

#[test]
fn vector_index_sql_rejects_reserved_distance_column() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql(
        "CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2), distance BIGINT)",
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let parameters = [Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())];
    let rejected = read.query_sql_streaming(
        &permit,
        VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        },
        VectorIndexSqlRequest {
            sql: "SELECT id FROM vector_index_search('rows', $1, candidates => 8, top_k => 1)",
            parameters: &parameters,
            output_limits: QueryStreamOptions::default(),
            task: RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(128 << 20, 4 << 20)),
        },
        |_| panic!("rejected query delivered a row"),
    );
    assert!(
        matches!(&rejected, Err(VectorIndexReadError::Query(HawDBError::Semantic(message))) if message == "vector index SQL reserves the distance output column"),
        "{rejected:?}"
    );
}

#[test]
fn vector_index_sql_observes_cancellation_from_last_result_callback() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let parameters = [Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())];
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
        &parameters,
    )
    .unwrap();
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    for cancel in [true, false] {
        let task = RuntimeTaskContext::default();
        let cancellation = task.cancellation().clone();
        let mut delivered = 0;
        let result = read.query_sql_streaming(
            &permit,
            VectorIndexLanguageBudget {
                index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
                index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            },
            VectorIndexSqlRequest {
                sql: "SELECT id FROM vector_index_search('rows', $1, candidates => 8, top_k => 1)",
                parameters: &parameters,
                output_limits: QueryStreamOptions::default(),
                task,
            },
            |row| {
                assert_eq!(row.get("id"), Some(&Value::Int(1)));
                delivered += 1;
                if cancel {
                    cancellation.cancel();
                }
                Ok(())
            },
        );
        assert_eq!(delivered, 1);
        if cancel {
            assert!(
                matches!(
                    &result,
                    Err(VectorIndexReadError::Query(HawDBError::RuntimeTaskStopped(
                        crate::RuntimeCancellationReason::Cancelled
                    )))
                ),
                "{result:?}"
            );
        } else {
            assert_eq!(result.unwrap().output_rows, 1);
        }
    }
}

#[test]
fn vector_index_sql_checks_callback_cancellation_and_preserves_consumer_errors() {
    use crate::{
        QueryStreamOptions, VectorIndexLanguageBudget, VectorIndexReadError, VectorIndexSqlRequest,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    for (id, coordinates) in [(1, [1.0, 0.0]), (2, [0.0, 1.0])] {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[
                Value::Int(id),
                Value::Vector(Vector::new(&coordinates).unwrap()),
            ],
        )
        .unwrap();
    }
    let consumer = db
        .create_vector_index(
            "rows",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let budget = VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    let parameters = [Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())];
    for (top_k, cancel_after, consumer_error) in
        [(1, 1, false), (2, 1, false), (2, 2, false), (2, 2, true)]
    {
        let sql = format!(
            "SELECT id FROM vector_index_search('rows', $1, candidates => 2, top_k => {top_k})"
        );
        let request = |task| VectorIndexSqlRequest {
            sql: &sql,
            parameters: &parameters,
            output_limits: QueryStreamOptions::default(),
            task,
        };
        let task = RuntimeTaskContext::default();
        let mut delivered = Vec::new();
        let rejected = read.query_sql_streaming(&permit, budget, request(task.clone()), |row| {
            delivered.push(row["id"].clone());
            if delivered.len() == cancel_after {
                task.cancellation().cancel();
                if consumer_error {
                    return Err(HawDBError::Execution("host consumer failed".into()));
                }
            }
            Ok(())
        });
        let expected = [Value::Int(1), Value::Int(2)];
        assert_eq!(delivered, expected[..cancel_after]);
        assert!(
            match &rejected {
                Err(VectorIndexReadError::Query(HawDBError::Execution(message))) => {
                    consumer_error && message == "host consumer failed"
                }
                Err(VectorIndexReadError::Query(HawDBError::RuntimeTaskStopped(reason))) => {
                    !consumer_error && *reason == crate::RuntimeCancellationReason::Cancelled
                }
                _ => false,
            },
            "top_k={top_k}, cancel_after={cancel_after}, consumer_error={consumer_error}: {rejected:?}"
        );

        let mut retried = Vec::new();
        let report = read
            .query_sql_streaming(
                &permit,
                budget,
                request(RuntimeTaskContext::default()),
                |row| {
                    retried.push(row["id"].clone());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(report.output_rows, top_k);
        assert_eq!(retried, expected[..top_k]);
    }
}
