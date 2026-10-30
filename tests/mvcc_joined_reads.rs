// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::{Database, DurabilityPolicy, ProjectionPublicationError, Value};
use std::num::NonZeroUsize;
#[path = "support/mvcc_fixture.rs"]
mod support;
use support::{fixture, limits, seal, TestRoot};

const JOIN_POINT: &str = "SELECT c.chunk_id, d.owner_id FROM chunks AS c INNER JOIN docs AS d ON d.doc_id = c.doc_id WHERE c.chunk_id = $1 AND d.owner_kind = 'source'";
const JOIN_PAGE: &str = "SELECT c.chunk_id, d.owner_id FROM chunks AS c INNER JOIN docs AS d ON d.doc_id = c.doc_id WHERE c.doc_id = $1 AND d.owner_kind = 'source' AND (c.ordinal > $2 OR (c.ordinal = $2 AND c.chunk_id > $3)) ORDER BY c.ordinal, c.chunk_id LIMIT $4";
fn joined_fixture(cold: bool) -> (TestRoot, Database) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query_sql("CREATE TABLE docs (doc_id TEXT PRIMARY KEY, owner_id TEXT, owner_kind TEXT)")
        .unwrap();
    db.query_sql(
        "CREATE TABLE chunks (chunk_id TEXT PRIMARY KEY, doc_id TEXT, ordinal BIGINT, body TEXT)",
    )
    .unwrap();
    db.query_sql("CREATE INDEX chunk_doc ON chunks (doc_id, ordinal, chunk_id)")
        .unwrap();
    db.query_sql("INSERT INTO docs (doc_id,owner_id,owner_kind) VALUES ('d1','one','source'),('d2','other','source')").unwrap();
    db.query_sql("INSERT INTO chunks (chunk_id,doc_id,ordinal,body) VALUES ('a','d1',1,'one'),('b','d1',2,'two'),('c','d2',1,'other')").unwrap();
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &root.0,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
    }
    (root, db)
}
fn joined_query(read: &mut hawdb::PreparedRead, page: bool, key: &str) -> hawdb::QueryOutput {
    if page {
        read.query_sql_with_params(
            JOIN_PAGE,
            &[
                Value::String(key.into()),
                Value::Int(0),
                Value::String("".into()),
                Value::Int(1),
            ],
        )
        .unwrap()
    } else {
        read.query_sql_with_params(JOIN_POINT, &[Value::String(key.into())])
            .unwrap()
    }
}
#[test]
fn joined_reads_capture_parent_before_query_and_allow_disjoint_writes() {
    for cold in [false, true] {
        for page in [false, true] {
            for related in [false, true] {
                let (_root, mut db) = joined_fixture(cold);
                let mut read = db.begin_prepared_read(limits()).unwrap();
                db.query_sql(if related {
                    "UPDATE docs SET owner_id = 'new' WHERE doc_id = 'd1'"
                } else {
                    "UPDATE docs SET owner_id = 'new' WHERE doc_id = 'd2'"
                })
                .unwrap();
                let output = joined_query(&mut read, page, if page { "d1" } else { "a" });
                assert_eq!(output.rows.len(), 1);
                assert_eq!(output.rows[0]["owner_id"], Value::String("one".into()));
                let deps = read.finish().unwrap();
                assert!(!deps.requires_unchanged_database_epoch());
                assert_eq!(deps.exact_relational_row_count(), if page { 1 } else { 2 });
                assert_eq!(deps.relational_index_prefix_count(), usize::from(page));
                let candidate = seal(
                    &db.projection_generation_store().unwrap(),
                    "joined_capture",
                    deps.source_epoch(),
                    None,
                );
                let result = db.publish_prepared_projection(&deps, &candidate);
                if related {
                    assert!(
                        matches!(result, Err(ProjectionPublicationError::Dependency(_))),
                        "cold={cold} page={page}"
                    );
                } else {
                    result.unwrap();
                }
            }
        }
    }
}
mod joined_parent_effects {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Effect {
        MissingParent,
        FilteredParent,
        DeleteParent,
        OffPage,
    }

    fn run(cold: bool, page: bool, effect: Effect) {
        let (_root, mut db) = joined_fixture(cold);
        if effect == Effect::MissingParent {
            db.query_sql("DELETE FROM docs WHERE doc_id = 'd1'")
                .unwrap();
        }
        if effect == Effect::FilteredParent {
            db.query_sql("UPDATE docs SET owner_kind = 'other' WHERE doc_id = 'd1'")
                .unwrap();
        }
        let mut read = db.begin_prepared_read(limits()).unwrap();
        let output = joined_query(&mut read, page, if page { "d1" } else { "a" });
        assert_eq!(
            output.rows.len(),
            usize::from(!matches!(
                effect,
                Effect::MissingParent | Effect::FilteredParent
            ))
        );
        let deps = read.finish().unwrap();
        assert!(!deps.requires_unchanged_database_epoch());
        assert_eq!(deps.exact_relational_row_count(), if page { 1 } else { 2 });
        db.query_sql(match effect {
            Effect::MissingParent => {
                "INSERT INTO docs (doc_id,owner_id,owner_kind) VALUES ('d1','returned','source')"
            }
            Effect::FilteredParent => "UPDATE docs SET owner_kind = 'source' WHERE doc_id = 'd1'",
            Effect::DeleteParent => "DELETE FROM docs WHERE doc_id = 'd1'",
            Effect::OffPage => "UPDATE chunks SET body = 'changed' WHERE chunk_id = 'b'",
        })
        .unwrap();
        let candidate = seal(
            &db.projection_generation_store().unwrap(),
            "joined_effect",
            deps.source_epoch(),
            None,
        );
        let result = db.publish_prepared_projection(&deps, &candidate);
        if effect == Effect::OffPage && !page {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(ProjectionPublicationError::Dependency(_))),
                "{effect:?} page={page} cold={cold}"
            );
        }
    }

    // Separate libtest cases let Bazel distribute persistent fixtures across shards.
    macro_rules! scenarios {
        ($($name:ident => ($cold:expr, $page:expr, $effect:ident)),+ $(,)?) => {
            $(
                #[test]
                fn $name() {
                    run($cold, $page, Effect::$effect);
                }
            )+
        };
    }

    scenarios! {
        cold_page_missing_parent => (true, true, MissingParent),
        cold_page_filtered_parent => (true, true, FilteredParent),
        cold_page_delete_parent => (true, true, DeleteParent),
        cold_page_off_page => (true, true, OffPage),
        cold_point_missing_parent => (true, false, MissingParent),
        cold_point_filtered_parent => (true, false, FilteredParent),
        cold_point_delete_parent => (true, false, DeleteParent),
        cold_point_off_page => (true, false, OffPage),
        warm_page_missing_parent => (false, true, MissingParent),
        warm_page_filtered_parent => (false, true, FilteredParent),
        warm_page_delete_parent => (false, true, DeleteParent),
        warm_page_off_page => (false, true, OffPage),
        warm_point_missing_parent => (false, false, MissingParent),
        warm_point_filtered_parent => (false, false, FilteredParent),
        warm_point_delete_parent => (false, false, DeleteParent),
        warm_point_off_page => (false, false, OffPage),
    }
}
#[test]
fn joined_reads_protect_absent_children_and_null_foreign_keys() {
    for cold in [false, true] {
        for null in [false, true] {
            let (_root, mut db) = joined_fixture(cold);
            if null {
                db.query_sql("UPDATE chunks SET doc_id = NULL WHERE chunk_id = 'a'")
                    .unwrap();
            }
            let mut read = db.begin_prepared_read(limits()).unwrap();
            assert!(
                joined_query(&mut read, false, if null { "a" } else { "absent" })
                    .rows
                    .is_empty()
            );
            let deps = read.finish().unwrap();
            assert!(!deps.requires_unchanged_database_epoch());
            assert_eq!(deps.exact_relational_row_count(), 1);
            db.query_sql("UPDATE docs SET owner_id = 'changed' WHERE doc_id = 'd1'")
                .unwrap();
            let first = seal(
                &db.projection_generation_store().unwrap(),
                "joined_absent_parent",
                deps.source_epoch(),
                None,
            );
            db.publish_prepared_projection(&deps, &first).unwrap();
            db.query_sql(if null {"UPDATE chunks SET doc_id = 'd1' WHERE chunk_id = 'a'"} else {"INSERT INTO chunks (chunk_id,doc_id,ordinal,body) VALUES ('absent','d1',3,'appeared')"}).unwrap();
            let second = seal(
                &db.projection_generation_store().unwrap(),
                "joined_child_changed",
                deps.source_epoch(),
                None,
            );
            assert!(matches!(
                db.publish_prepared_projection(&deps, &second),
                Err(ProjectionPublicationError::Dependency(_))
            ));
        }
    }
}
#[test]
fn joined_reads_keep_errors_sticky_and_enforce_guard_and_dependency_admission() {
    let (_root, mut db) = joined_fixture(false);
    for query in [
        "SELECT c.chunk_id FROM chunks c LEFT JOIN docs d ON d.doc_id=c.doc_id WHERE c.chunk_id='a'",
        "SELECT c.chunk_id FROM chunks c JOIN docs d ON (d.doc_id=c.doc_id OR d.doc_id=c.body) WHERE c.chunk_id='a'",
        "SELECT c.chunk_id FROM chunks c JOIN docs d ON NOT(d.doc_id=c.doc_id) WHERE c.chunk_id='a'",
        "SELECT c.chunk_id FROM chunks c JOIN docs d ON d.owner_id=c.doc_id WHERE c.chunk_id='a'",
        "SELECT c.chunk_id FROM chunks c JOIN docs d ON missing.doc_id=c.doc_id WHERE c.chunk_id='a'",
        "SELECT c.chunk_id FROM chunks c JOIN docs d ON d.doc_id=c.doc_id AND lower(d.owner_kind)='source' WHERE c.chunk_id='a'",
    ] {
        let mut read=db.begin_prepared_read(limits()).unwrap();
        assert!(read.query_sql_with_params(query,&[]).is_err(),"{query}");assert!(read.finish().is_err());
    }
    let mut budget = limits();
    budget.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut read = db.begin_prepared_read(budget).unwrap();
    assert!(read
        .query_sql_with_params(JOIN_POINT, &[Value::String("a".into())])
        .is_err());
    assert!(read.finish().is_err());
    db.query_sql_with_params(
        "UPDATE chunks SET doc_id=$1 WHERE chunk_id='a'",
        &[Value::String("x".repeat(5000))],
    )
    .unwrap();
    let mut budget = limits();
    budget.statement_payload_bytes = NonZeroUsize::new(64 * 1024).unwrap();
    let mut read = db.begin_prepared_read(budget).unwrap();
    assert!(read
        .query_sql_with_params(JOIN_POINT, &[Value::String("a".into())])
        .is_err());
    assert!(read.finish().is_err());
}

#[test]
fn joined_reads_bind_complete_composite_keys_and_keep_foreign_predicates_broad() {
    for cold in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query_sql(
            "CREATE TABLE parents (tenant TEXT, id BIGINT, body TEXT, PRIMARY KEY (tenant,id))",
        )
        .unwrap();
        db.query_sql(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, tenant TEXT, parent_id BIGINT)",
        )
        .unwrap();
        db.query_sql("CREATE INDEX child_tenant ON children (tenant,parent_id)")
            .unwrap();
        db.query_sql(
            "INSERT INTO parents (tenant,id,body) VALUES ('t',1,'one'),('other',1,'other')",
        )
        .unwrap();
        db.query_sql("INSERT INTO children (id,tenant,parent_id) VALUES (1,'t',1),(2,'other',1)")
            .unwrap();
        if cold {
            db.checkpoint().unwrap();
            drop(db);
            db = Database::open_with_config(
                &root.0,
                hawdb::DatabaseConfig {
                    storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let sql="SELECT p.body FROM children c JOIN parents p ON c.parent_id=p.id AND p.tenant=c.tenant WHERE c.id=$1";
        let mut read = db.begin_prepared_read(limits()).unwrap();
        assert_eq!(
            read.query_sql_with_params(sql, &[Value::Int(1)])
                .unwrap()
                .rows[0]["body"],
            Value::String("one".into())
        );
        let deps = read.finish().unwrap();
        assert!(!deps.requires_unchanged_database_epoch());
        assert_eq!(deps.exact_relational_row_count(), 2);
        db.query_sql("UPDATE parents SET body='changed' WHERE tenant='other' AND id=1")
            .unwrap();
        let first = seal(
            &db.projection_generation_store().unwrap(),
            "joined_composite",
            deps.source_epoch(),
            None,
        );
        db.publish_prepared_projection(&deps, &first).unwrap();
        db.query_sql("UPDATE parents SET body='changed' WHERE tenant='t' AND id=1")
            .unwrap();
        let second = seal(
            &db.projection_generation_store().unwrap(),
            "joined_composite_stale",
            deps.source_epoch(),
            None,
        );
        assert!(matches!(
            db.publish_prepared_projection(&deps, &second),
            Err(ProjectionPublicationError::Dependency(_))
        ));
        for sql in [
            "SELECT c.id FROM children c JOIN parents p ON p.tenant=c.tenant AND p.id=c.parent_id WHERE p.id=1",
            "SELECT c.id FROM children c JOIN parents p ON p.tenant=c.tenant AND p.id=c.parent_id WHERE c.id=1 OR c.id=2",
        ] {
            let reference=db.query_sql(sql).unwrap();
            let mut read=db.begin_prepared_read(limits()).unwrap();
            assert_eq!(read.query_sql_with_params(sql,&[]).unwrap().rows,reference.rows);
            assert!(read.finish().unwrap().requires_unchanged_database_epoch(),"{sql}");
        }
        for sql in [
            // Table-name/alias collisions must never yield a source proof.
            "SELECT c.id FROM children c JOIN parents children ON children.tenant=c.tenant AND children.id=c.parent_id WHERE children.id=1",
            "SELECT c.id FROM children c JOIN parents p ON p.id=c.parent_id WHERE c.id=1",
            "SELECT c.id FROM children c JOIN parents p ON p.id=c.parent_id AND p.id=c.id WHERE c.id=1",
            "SELECT c.id FROM children c JOIN parents p ON p.tenant=c.tenant AND p.id=c.parent_id WHERE c.tenant='t'",
        ] {
            let mut read=db.begin_prepared_read(limits()).unwrap();
            assert!(read.query_sql_with_params(sql,&[]).is_err(),"{sql}");assert!(read.finish().is_err());
        }
    }
}

#[test]
fn joined_reads_quote_internal_guard_identifiers_and_do_not_project_unused_bodies() {
    for cold in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query_sql("CREATE TABLE \"child\"\"table\" (\"key\"\"id\" TEXT PRIMARY KEY, \"parent\"\"id\" TEXT, unused TEXT)").unwrap();
        db.query_sql(
            "CREATE TABLE \"parent\"\"table\" (\"key\"\"id\" TEXT PRIMARY KEY, body TEXT)",
        )
        .unwrap();
        db.query_sql("INSERT INTO \"parent\"\"table\" (\"key\"\"id\",body) VALUES ('p','one')")
            .unwrap();
        db.query_sql_with_params("INSERT INTO \"child\"\"table\" (\"key\"\"id\",\"parent\"\"id\",unused) VALUES ('a','p',$1)",&[Value::String("x".repeat(1024*1024))]).unwrap();
        if cold {
            db.checkpoint().unwrap();
            drop(db);
            db = Database::open_with_config(
                &root.0,
                hawdb::DatabaseConfig {
                    storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let mut read = db.begin_prepared_read(limits()).unwrap();
        let output=read.query_sql_with_params("SELECT p.body FROM \"child\"\"table\" c JOIN \"parent\"\"table\" p ON p.\"key\"\"id\"=c.\"parent\"\"id\" WHERE c.\"key\"\"id\"=$1",&[Value::String("a".into())]).unwrap();
        assert_eq!(output.rows[0]["body"], Value::String("one".into()));
        let deps = read.finish().unwrap();
        assert!(!deps.requires_unchanged_database_epoch());
        assert_eq!(deps.exact_relational_row_count(), 2);
    }
}

#[test]
fn joined_reads_canonicalize_uuid_parent_keys_and_keep_float_joins_broad() {
    for cold in [false, true] {
        for uuid in [false, true] {
            let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
            db.query_sql(if uuid {
                "CREATE TABLE scalar_parents (id UUID PRIMARY KEY, body TEXT)"
            } else {
                "CREATE TABLE scalar_parents (id DOUBLE PRECISION PRIMARY KEY, body TEXT)"
            })
            .unwrap();
            db.query_sql(if uuid {
                "CREATE TABLE scalar_children (id BIGINT PRIMARY KEY, parent_id UUID)"
            } else {
                "CREATE TABLE scalar_children (id BIGINT PRIMARY KEY, parent_id DOUBLE PRECISION)"
            })
            .unwrap();
            let id = if uuid {
                Value::String("018f22d2-7d34-7000-8000-000000000001".into())
            } else {
                Value::Float(1.0)
            };
            db.query_sql_with_params(
                "INSERT INTO scalar_parents (id,body) VALUES ($1,'one')",
                std::slice::from_ref(&id),
            )
            .unwrap();
            db.query_sql_with_params(
                "INSERT INTO scalar_children (id,parent_id) VALUES (1,$1)",
                std::slice::from_ref(&id),
            )
            .unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(
                    &root.0,
                    hawdb::DatabaseConfig {
                        storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let output=read.query_sql_with_params("SELECT p.body FROM scalar_children c JOIN scalar_parents p ON p.id=c.parent_id WHERE c.id=1",&[]).unwrap();
            assert_eq!(output.rows[0]["body"], Value::String("one".into()));
            let deps = read.finish().unwrap();
            assert_eq!(deps.requires_unchanged_database_epoch(), !uuid);
            if uuid {
                assert_eq!(deps.exact_relational_row_count(), 2);
            }
            db.query_sql_with_params(
                "UPDATE scalar_parents SET body='changed' WHERE id=$1",
                &[id],
            )
            .unwrap();
            let candidate = seal(
                &db.projection_generation_store().unwrap(),
                "joined_scalar",
                deps.source_epoch(),
                None,
            );
            assert!(matches!(
                db.publish_prepared_projection(&deps, &candidate),
                Err(ProjectionPublicationError::Dependency(_))
            ));
        }
    }
}

// Audited Mem a78432d: exact SourceChunk SQL; reduced synthetic projection schema.
const SELECT_SOURCE_CHUNK_PROJECTION_ROW_SQL: &str = "SELECT c.chunk_id, \
c.content_doc_id, d.owner_id AS source_id, d.space_id, c.chunk_index, c.text, \
c.token_count, c.metadata_json, c.created_at \
FROM content_chunks AS c INNER JOIN content_documents AS d \
ON d.content_doc_id = c.content_doc_id \
WHERE c.chunk_id = $1 AND d.owner_kind = 'source'";

const SELECT_SOURCE_CHUNK_PROJECTION_PAGE_SQL: &str = "SELECT c.chunk_id, \
c.content_doc_id, d.owner_id AS source_id, d.space_id, c.chunk_index, c.text, \
c.token_count, c.metadata_json, c.created_at \
FROM content_chunks AS c INNER JOIN content_documents AS d \
ON d.content_doc_id = c.content_doc_id \
WHERE c.content_doc_id = $1 AND d.owner_kind = 'source' AND \
(c.chunk_index > $2 OR (c.chunk_index = $2 AND c.chunk_id > $3)) \
ORDER BY c.chunk_index ASC, c.chunk_id ASC LIMIT $4";

#[test]
fn joined_reads_execute_audited_source_chunk_statements() {
    for cold in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query_sql("CREATE TABLE content_documents (content_doc_id TEXT PRIMARY KEY, owner_kind TEXT, owner_id TEXT, space_id TEXT)").unwrap();
        db.query_sql("CREATE TABLE content_chunks (chunk_id TEXT PRIMARY KEY, content_doc_id TEXT, chunk_index BIGINT, text TEXT, token_count BIGINT, metadata_json TEXT, created_at TEXT)").unwrap();
        db.query_sql("CREATE INDEX chunks_by_document ON content_chunks (content_doc_id,chunk_index,chunk_id)").unwrap();
        db.query_sql("INSERT INTO content_documents (content_doc_id,owner_kind,owner_id,space_id) VALUES ('doc','source','source1','space1')").unwrap();
        db.query_sql("INSERT INTO content_chunks (chunk_id,content_doc_id,chunk_index,text,token_count,metadata_json,created_at) VALUES ('chunk','doc',0,'body',1,'{}','2026-10-16T00:00:00Z')").unwrap();
        if cold {
            db.checkpoint().unwrap();
            drop(db);
            db = Database::open_with_config(
                &root.0,
                hawdb::DatabaseConfig {
                    storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        for (sql, params, prefixes) in [
            (
                SELECT_SOURCE_CHUNK_PROJECTION_ROW_SQL,
                vec![Value::String("chunk".into())],
                0,
            ),
            (
                SELECT_SOURCE_CHUNK_PROJECTION_PAGE_SQL,
                vec![
                    Value::String("doc".into()),
                    Value::Int(-1),
                    Value::String("".into()),
                    Value::Int(10),
                ],
                1,
            ),
        ] {
            let reference = db.query_sql_with_params(sql, &params).unwrap();
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let result = read.query_sql_with_params(sql, &params).unwrap();
            assert_eq!(result.rows, reference.rows);
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0]["source_id"], Value::String("source1".into()));
            let deps = read.finish().unwrap();
            assert!(!deps.requires_unchanged_database_epoch());
            assert_eq!(deps.relational_index_prefix_count(), prefixes);
            assert_eq!(
                deps.exact_relational_row_count(),
                if prefixes == 0 { 2 } else { 1 }
            );
        }
    }
}
