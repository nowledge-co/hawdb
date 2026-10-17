// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use hawdb::{
    Database, PreparedReadLimits, ProjectionFeedLimits, ProjectionPublicationError,
    ProjectionReadScopes, Value,
};
use std::num::NonZeroUsize;
fn limits() -> PreparedReadLimits {
    let n = |x| NonZeroUsize::new(x).unwrap();
    PreparedReadLimits {
        dependency_entries: n(32),
        dependency_bytes: n(8192),
        statement_rows: n(8),
        statement_payload_bytes: n(8192),
    }
}
fn budget() -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(128).unwrap(),
        items: NonZeroUsize::new(16).unwrap(),
        bytes: NonZeroUsize::new(8192).unwrap(),
    }
}

#[test]
fn relationship_scope_detects_unselected_owner_edges_and_allows_other_types() {
    for (related, effect) in [false, true]
        .into_iter()
        .flat_map(|related| ["create", "property", "delete"].map(|effect| (related, effect)))
    {
        let root = std::env::temp_dir().join(format!(
            "hawdb-rel-scope-{}-{}-{related}-{effect}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let mut db = Database::open(root.join("db")).unwrap();
        for query in [
            "CREATE (:Memory {id:'a'})",
            "CREATE (:Memory {id:'b'})",
            "CREATE (:Label {id:'l'})",
        ] {
            db.query(query).unwrap();
        }
        let kind = if related { "HAS_LABEL" } else { "OTHER" };
        if effect != "create" {
            db.query(&format!(
                "MATCH (m:Memory {{id:'b'}}),(l:Label {{id:'l'}}) CREATE (m)-[:{kind}]->(l)"
            ))
            .unwrap();
        }
        let e = db.commit_epoch().unwrap();
        let mut feed = db
            .begin_prepared_projection_owner_feed_with_scopes(
                e,
                limits(),
                budget(),
                ProjectionReadScopes {
                    node_labels: &["Memory", "Label"],
                    relationship_types: &["HAS_LABEL"],
                    ..Default::default()
                },
            )
            .unwrap();
        let mutation = match effect {
            "create" => format!(
                "MATCH (m:Memory {{id:'b'}}),(l:Label {{id:'l'}}) CREATE (m)-[:{kind}]->(l)"
            ),
            "property" => {
                format!("MATCH (m:Memory {{id:'b'}})-[r:{kind}]->(l:Label) SET r.weight = 2")
            }
            "delete" => format!("MATCH (m:Memory {{id:'b'}})-[r:{kind}]->(l:Label) DELETE r"),
            _ => unreachable!(),
        };
        db.query(&mutation).unwrap();
        feed.visit_next(budget(), |read, _| {
            assert_eq!(
                read.query_with_params(
                    "MATCH (m:Memory {id:'a'}) RETURN m.id AS id",
                    &Default::default()
                )?
                .output
                .rows
                .len(),
                1
            );
            Ok(())
        })
        .unwrap();
        let deps = feed.finish().unwrap();
        assert_eq!(deps.relationship_domain_count(), 1);
        assert!(!deps.requires_unchanged_database_epoch());
        let writer = hawdb::SearchOutOfCoreGenerationWriter::create(
            root.join("search"),
            hawdb::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(e),
                ..Default::default()
            },
        )
        .unwrap();
        // Controlled source-validation candidate, not complete mapping coverage.
        let result = db.publish_prepared_search(&deps, writer.prepare_publication().unwrap());
        if related {
            let Err(ProjectionPublicationError::Dependency(error)) = result else {
                panic!("expected relationship-scope conflict")
            };
            assert!(
                error.to_string().contains("ReadRelationshipDomain"),
                "{error}"
            );
        } else {
            result.unwrap();
        }
        drop(deps);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn eager_table_scope_keeps_one_entry_across_many_point_reads() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE rows (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("INSERT INTO rows (id,body) VALUES (1,'one'),(2,'two')")
        .unwrap();
    let mut config = limits();
    config.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut feed = db
        .begin_prepared_projection_owner_feed_with_scopes(
            db.commit_epoch().unwrap(),
            config,
            budget(),
            ProjectionReadScopes {
                relational_tables: &["rows"],
                ..Default::default()
            },
        )
        .unwrap();
    feed.visit_next(budget(), |read, _| {
        for id in 0..64 {
            let output =
                read.query_sql_with_params("SELECT body FROM rows WHERE id=$1", &[Value::Int(id)])?;
            assert_eq!(output.rows.len(), usize::from(id == 1 || id == 2));
        }
        Ok(())
    })
    .unwrap();
    let deps = feed.finish().unwrap();
    assert_eq!(deps.relational_table_domain_count(), 1);
    assert_eq!(deps.exact_relational_row_count(), 0);
    assert!(!deps.requires_unchanged_database_epoch());
}

#[test]
fn eager_label_scope_keeps_one_entry_across_many_scalar_index_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id:'a'})").unwrap();
    db.query("CREATE INDEX ON :Thread(id)").unwrap();
    let mut config = limits();
    config.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut feed = db
        .begin_prepared_projection_owner_feed_with_scopes(
            db.commit_epoch().unwrap(),
            config,
            budget(),
            ProjectionReadScopes {
                node_labels: &["Thread"],
                ..Default::default()
            },
        )
        .unwrap();
    feed.visit_next(budget(), |read, _| {
        for id in ["a", "missing"].into_iter().cycle().take(64) {
            let output = read.query_with_params(
                "MATCH (t:Thread) WHERE t.id=$id RETURN t.id AS id",
                &std::collections::BTreeMap::from([("id".into(), Value::String(id.into()))]),
            )?;
            assert_eq!(output.output.rows.len(), usize::from(id == "a"));
        }
        Ok(())
    })
    .unwrap();
    let deps = feed.finish().unwrap();
    assert_eq!(deps.node_domain_count(), 1);
    assert_eq!(deps.exact_node_property_count(), 0);
    assert!(!deps.requires_unchanged_database_epoch());
}

#[test]
fn scope_constructor_refuses_unknown_tables_empty_names_and_unadmitted_input() {
    let db = Database::new();
    for scopes in [
        ProjectionReadScopes {
            relational_tables: &["missing"],
            ..Default::default()
        },
        ProjectionReadScopes {
            relationship_types: &[""],
            ..Default::default()
        },
    ] {
        assert!(db
            .begin_prepared_projection_owner_feed_with_scopes(
                db.commit_epoch().unwrap(),
                limits(),
                budget(),
                scopes
            )
            .is_err());
    }
    let mut config = limits();
    config.dependency_entries = NonZeroUsize::new(1).unwrap();
    assert!(db
        .begin_prepared_projection_owner_feed_with_scopes(
            db.commit_epoch().unwrap(),
            config,
            budget(),
            ProjectionReadScopes {
                node_labels: &["Memory", "Thread"],
                ..Default::default()
            }
        )
        .is_err());
    config = limits();
    config.dependency_bytes = NonZeroUsize::new(1).unwrap();
    assert!(db
        .begin_prepared_projection_owner_feed_with_scopes(
            db.commit_epoch().unwrap(),
            config,
            budget(),
            ProjectionReadScopes {
                relationship_types: &["HAS_LABEL"],
                ..Default::default()
            }
        )
        .is_err());
}

#[test]
fn eager_label_scope_bounds_qualified_physical_id_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id:'a'})").unwrap();
    let mut config = limits();
    config.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut feed = db
        .begin_prepared_projection_owner_feed_with_scopes(
            db.commit_epoch().unwrap(),
            config,
            budget(),
            ProjectionReadScopes {
                node_labels: &["Memory"],
                ..Default::default()
            },
        )
        .unwrap();
    feed.visit_next(budget(), |read, _| {
        let mut seen = 0;
        for id in 0..64 {
            let result = read.query_with_params(
                "MATCH (m:Memory) WHERE id(m)=$id RETURN m.id AS id",
                &std::collections::BTreeMap::from([("id".into(), Value::Int(id))]),
            )?;
            assert!(result.output.rows.len() <= 1);
            seen += result.output.rows.len();
        }
        assert_eq!(seen, 1, "fixture must read the existing physical owner");
        Ok(())
    })
    .unwrap();
    let deps = feed.finish().unwrap();
    assert_eq!(deps.node_domain_count(), 1);
    assert!(!deps.requires_unchanged_database_epoch());
}

#[test]
fn complete_hop_scopes_bound_identity_reads_and_partial_scopes_refuse() {
    for missing in [
        "none",
        "root",
        "target",
        "kind",
        "wrong_root",
        "unlabeled",
        "untyped",
    ] {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id:'a'})").unwrap();
        db.query("CREATE (:Label {id:'l'})").unwrap();
        db.query("MATCH (m:Memory),(l:Label) CREATE (m)-[:HAS_LABEL]->(l)")
            .unwrap();
        let labels: &[&str] = match missing {
            "root" => &["Label"],
            "target" => &["Memory"],
            "wrong_root" => &["Other", "Label"],
            _ => &["Memory", "Label"],
        };
        let kinds: &[&str] = if missing == "kind" {
            &[]
        } else {
            &["HAS_LABEL"]
        };
        let mut config = limits();
        config.dependency_entries = NonZeroUsize::new(labels.len() + kinds.len()).unwrap();
        let mut feed = db
            .begin_prepared_projection_owner_feed_with_scopes(
                db.commit_epoch().unwrap(),
                config,
                budget(),
                ProjectionReadScopes {
                    node_labels: labels,
                    relationship_types: kinds,
                    ..Default::default()
                },
            )
            .unwrap();
        let result = feed.visit_next(budget(), |read, _| {
            let mut seen = 0;
            for id in 0..64 {
                let text = match missing {
                    "unlabeled" => {
                        "MATCH (m)-[:HAS_LABEL]->(l:Label) WHERE id(m)=$id RETURN l.id AS id"
                    }
                    "untyped" => "MATCH (m:Memory)-[]->(l:Label) WHERE id(m)=$id RETURN l.id AS id",
                    _ => {
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE id(m)=$id RETURN l.id AS id"
                    }
                };
                let output = read.query_with_params(
                    text,
                    &std::collections::BTreeMap::from([("id".into(), Value::Int(id))]),
                )?;
                assert!(output.output.rows.len() <= 1);
                seen += output.output.rows.len();
            }
            assert_eq!(seen, 1, "fixture must traverse the existing relationship");
            Ok(())
        });
        if missing == "none" {
            result.unwrap();
            let deps = feed.finish().unwrap();
            assert_eq!(deps.node_domain_count(), 2);
            assert_eq!(deps.relationship_domain_count(), 1);
            assert!(!deps.requires_unchanged_database_epoch());
        } else {
            assert!(result.is_err(), "incomplete domains passed: {missing}");
            assert!(feed.finish().is_err(), "ignored refusal passed: {missing}");
        }
    }
}

#[test]
fn complete_hop_guards_validate_hot_and_cold_pinned_values() {
    for (cold, change) in [false, true]
        .into_iter()
        .flat_map(|cold| ["root", "target", "edge", "unrelated"].map(|change| (cold, change)))
    {
        let root = std::env::temp_dir().join(format!(
            "hawdb-covered-hop-{}-{}-{cold}-{change}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir(&root).unwrap();
        let mut db = Database::open(root.join("db")).unwrap();
        db.query("CREATE (:Memory {id:'a'})").unwrap();
        db.query("CREATE (:Label {id:'l'})").unwrap();
        db.query("MATCH (m:Memory),(l:Label) CREATE (m)-[:HAS_LABEL {weight:1}]->(l)")
            .unwrap();
        let output = db.query("MATCH (m:Memory) RETURN id(m) AS id").unwrap();
        let node_id = output.rows[0].get("id").unwrap().clone();
        if cold {
            db.checkpoint().unwrap();
        }
        let e = db.commit_epoch().unwrap();
        let mut config = limits();
        config.dependency_entries = NonZeroUsize::new(3).unwrap();
        let mut feed = db
            .begin_prepared_projection_owner_feed_with_scopes(
                e,
                config,
                budget(),
                ProjectionReadScopes {
                    node_labels: &["Memory", "Label"],
                    relationship_types: &["HAS_LABEL"],
                    ..Default::default()
                },
            )
            .unwrap();
        db.query(match change {
            "root" => "MATCH (m:Memory) SET m.id='new'",
            "target" => "MATCH (l:Label) SET l.id='new'",
            "edge" => "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) SET r.weight=2",
            "unrelated" => "CREATE (:Other {id:'other'})",
            _ => unreachable!(),
        })
        .unwrap();
        feed.visit_next(budget(),|read,_| {
            let result=read.query_with_params(
                "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) WHERE id(m)=$id RETURN m.id AS root,l.id AS label,r.weight AS weight",
                &std::collections::BTreeMap::from([("id".into(),node_id.clone())]),
            )?;
            assert_eq!(result.output.rows.len(),1);
            assert_eq!(result.output.rows[0].get("root"),Some(&Value::String("a".into())));
            assert_eq!(result.output.rows[0].get("label"),Some(&Value::String("l".into())));
            assert_eq!(result.output.rows[0].get("weight"),Some(&Value::Int(1)));
            Ok(())
        }).unwrap();
        let deps = feed.finish().unwrap();
        assert_eq!(deps.node_domain_count(), 2);
        assert_eq!(deps.relationship_domain_count(), 1);
        // Deliberately empty candidate exercises source validation only; this
        // test does not authorize output completeness or a C..E watermark.
        let writer = hawdb::SearchOutOfCoreGenerationWriter::create(
            root.join("search"),
            hawdb::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(e),
                ..Default::default()
            },
        )
        .unwrap();
        let result = db.publish_prepared_search(&deps, writer.prepare_publication().unwrap());
        if change == "unrelated" {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(ProjectionPublicationError::Dependency(_))),
                "stale {change} passed, cold={cold}"
            );
        }
        drop(deps);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
