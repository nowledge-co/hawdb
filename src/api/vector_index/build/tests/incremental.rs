// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::VectorIndexReadTransaction;

#[test]
fn vector_index_incremental_capture_capacity_is_explicit_and_fails_closed() {
    use crate::DatabaseConfig;
    use hawdb_storage::relational::RelationalPrimaryKeyChangeCaptureLimits;
    use hawdb_storage::store::ProjectionFeedError;
    for capacity in [1, 2] {
        let fixture = Fixture::new();
        let mut db = Database::open_with_config(
            fixture.0.join("database"),
            DatabaseConfig {
                search_projection_relational_change_limits:
                    RelationalPrimaryKeyChangeCaptureLimits {
                        max_entries: NonZeroUsize::new(capacity).unwrap(),
                        ..Default::default()
                    },
                automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
        )
        .unwrap();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        for id in 1..=2 {
            db.query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                &[
                    Value::Int(id),
                    Value::Vector(Vector::new(&[1.0, id as f32]).unwrap()),
                ],
            )
            .unwrap();
        }
        let mut consumer = db
            .create_vector_index(
                "capacity",
                &sql_field(),
                fixture.0.join("capacity"),
                registration(),
                Default::default(),
            )
            .unwrap();
        let before = consumer.canonical_vector_source_epoch().unwrap();
        let generation = consumer
            .projection()
            .stationary_receipt()
            .unwrap()
            .generation_name()
            .to_owned();
        let mut transaction = db.begin_transaction().unwrap();
        for id in 1..=2 {
            transaction
                .query_sql_with_params(
                    "UPDATE vectors SET embedding = NULL WHERE id = $1",
                    &[Value::Int(id)],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        let result = db.catch_up_vector_index(&mut consumer, Default::default());
        if capacity == 1 {
            assert!(
                matches!(
                    result,
                    Err(VectorIndexBuildError::Source(
                        ProjectionFeedError::RequiresRebuild
                    ))
                ),
                "{result:?}"
            );
            assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), before);
            assert_eq!(
                consumer
                    .projection()
                    .stationary_receipt()
                    .unwrap()
                    .generation_name(),
                generation
            );
            assert_eq!(consumer.document_count().unwrap(), 2);
        } else {
            let report = result.unwrap();
            assert_eq!(report.mapped_candidates, 2);
            assert_eq!(report.retraction_candidates, 2);
            assert_eq!(consumer.document_count().unwrap(), 0);
        }
    }
}

#[cfg(feature = "background-maintenance")]
#[test]
fn vector_index_incremental_admission_is_typed_and_refuses_before_publication() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeAdmissionCode, RuntimeGovernor, RuntimeGovernorConfig,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            background_task_limit: NonZeroUsize::new(1),
            memory_budget_bytes: Some(1 << 30),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    db.set_runtime_governor(governor.clone());
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    suspension.wait_idle().unwrap();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    for id in 1..=2 {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[
                Value::Int(id),
                Value::Vector(Vector::new(&[1.0, id as f32]).unwrap()),
            ],
        )
        .unwrap();
    }
    let mut consumer = db
        .create_vector_index(
            "admission",
            &sql_field(),
            fixture.0.join("admission"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let before = consumer.canonical_vector_source_epoch().unwrap();
    let generation = consumer
        .projection()
        .stationary_receipt()
        .unwrap()
        .generation_name()
        .to_owned();
    let busy = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let noop = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .expect("a no-op needs no publication admission");
    assert_eq!(noop.mapped_candidates, 0);
    db.query_sql("UPDATE vectors SET embedding = NULL WHERE id = 1")
        .unwrap();
    for rebuild in [false, true] {
        let result = if rebuild {
            db.rebuild_vector_index(&mut consumer, Default::default())
        } else {
            db.catch_up_vector_index(&mut consumer, Default::default())
                .map(|_| ())
        };
        let error = result.unwrap_err();
        assert!(
            matches!(error, VectorIndexBuildError::Admission(error)
                if error.code == RuntimeAdmissionCode::BackgroundTaskSaturated && error.is_retryable()),
            "expected typed retryable admission: {error:?}"
        );
        assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), before);
        assert_eq!(
            consumer
                .projection()
                .stationary_receipt()
                .unwrap()
                .generation_name(),
            generation
        );
    }
    drop(busy);
    let metadata = hawdb_storage::branch_catalog::CatalogMetadataLease::acquire(
        &fixture.0.join("database/branches"),
    )
    .unwrap();
    let error = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap_err();
    assert!(
        matches!(
            error,
            VectorIndexBuildError::Consumer(crate::SearchProjectionConsumerError::Database(
                crate::HawDBError::BranchBusy { .. }
            ))
        ),
        "expected typed metadata contention: {error:?}"
    );
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), before);
    assert_eq!(
        consumer
            .projection()
            .stationary_receipt()
            .unwrap()
            .generation_name(),
        generation
    );
    drop(metadata);
    let report = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(report.mapped_candidates, 1);
    assert_eq!(report.retraction_candidates, 1);
    assert_eq!(consumer.document_count().unwrap(), 1);
}

#[cfg(feature = "background-maintenance")]
#[test]
fn vector_index_incremental_feed_survives_automatic_checkpoint_with_one_background_slot() {
    use crate::{DatabaseConfig, StorageResidencyMode};
    use hawdb_qos::{IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig};
    use std::time::{Duration, Instant};

    let fixture = Fixture::new();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            background_task_limit: NonZeroUsize::new(1),
            memory_budget_bytes: Some(1 << 30),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let mut db = Database::open_with_config(
        fixture.0.join("database"),
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .unwrap();
    db.set_runtime_governor(governor.clone());
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for id in 1..=32 {
        transaction
            .query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                &[
                    Value::Int(id),
                    Value::Vector(Vector::new(&[1.0, id as f32 / 100.0]).unwrap()),
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.checkpoint().unwrap();
    let mut consumer = db
        .create_vector_index(
            "checkpoint-feed",
            &sql_field(),
            fixture.0.join("checkpoint-feed"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let base = consumer.canonical_vector_source_epoch().unwrap().unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    suspension.wait_idle().unwrap();
    let before = db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints;
    db.query_sql("UPDATE vectors SET embedding = NULL WHERE id = 3")
        .unwrap();
    let changes = db
        .runtime
        .get()
        .unwrap()
        .store
        .search_projection_graph_changes_after(base);
    assert_eq!(changes.len(), 1);
    assert!(!changes[0].relational_primary_key_changes.requires_rebuild());
    drop(suspension);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let report = db.automatic_checkpoint_report().unwrap().unwrap();
        if report.completed_checkpoints > before && governor.snapshot().active_background_tasks == 0
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint did not finish: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    // The completed checkpoint is the observation under test. Stop the owner
    // from reacquiring the sole background slot during the subsequent checks.
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    suspension.wait_idle().unwrap();
    let recovered = db
        .runtime
        .get()
        .unwrap()
        .store
        .search_projection_graph_changes_after(base);
    assert_eq!(recovered, changes);
    let report = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(report.mapped_candidates, 1);
    assert_eq!(report.retraction_candidates, 1);
    assert_eq!(consumer.document_count().unwrap(), 31);
}

fn query(read: &VectorIndexReadTransaction, permit: &hawdb_qos::RuntimePermit) -> Vec<f64> {
    let output = read
        .query_candidates(
            permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::new(64).unwrap(),
            NonZeroUsize::new(64).unwrap(),
        )
        .unwrap();
    assert_eq!(output.report().backend, "diskann");
    output.hits().iter().map(|hit| hit.distance).collect()
}

fn current_manifest(fixture: &Fixture, epoch: u64) -> (PathBuf, serde_json::Value) {
    let mut matches = Vec::new();
    for entry in std::fs::read_dir(&fixture.0).unwrap() {
        let root = entry.unwrap().path();
        let path = root.join("search_projection.out_of_core.manifest.hawdb");
        if !path.is_file() {
            continue;
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        if manifest["body"]["source_graph_commit_epoch"].as_u64() == Some(epoch) {
            matches.push((root, manifest));
        }
    }
    assert_eq!(matches.len(), 1);
    matches.pop().unwrap()
}

fn expected() -> Vec<f64> {
    let mut vectors = (4..=32)
        .map(|id| vec![1.0, id as f32 / 100.0])
        .collect::<Vec<_>>();
    vectors.extend([vec![0.1, 1.0], vec![1.0, 0.0]]);
    let mut distances = vectors
        .into_iter()
        .map(|vector| {
            VectorDistance::Cosine
                .distance_slices_with_work(&[1.0, 0.0], &vector, || Ok(()))
                .unwrap()
        })
        .collect::<Vec<_>>();
    distances.sort_by(f64::total_cmp);
    distances
}

fn filtered_language(
    db: &mut Database,
    consumer: &SearchProjectionConsumer,
    graph: bool,
    permit: &hawdb_qos::RuntimePermit,
) {
    for mode in ["exact", "filtered_ann"] {
        let mut read = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(consumer, permit)
            .unwrap();
        let mut rows = Vec::new();
        let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
        let budget = crate::VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        };
        let collect = |row: crate::executor::Row| {
            rows.push(row["id"].clone());
            Ok(())
        };
        if graph {
            let candidates = if mode == "filtered_ann" {
                ", candidates := 4"
            } else {
                ""
            };
            let statement = format!("MATCH (d:Document) WHERE d.id >= 1 AND d.id <= 5 CALL vector_index_search('incremental', $q, mode := '{mode}', eligible := d{candidates}, topK := 4, maxWork := 1000) YIELD node AS n, distance RETURN n.id AS id, distance ORDER BY distance, id");
            let parameters = BTreeMap::from([("q".into(), embedding)]);
            let report = read
                .query_cypher_streaming(
                    permit,
                    budget,
                    crate::QueryRequest::new(&statement).with_params(&parameters),
                    collect,
                )
                .unwrap();
            if mode == "filtered_ann" {
                assert_eq!(report.index().unwrap().backend, "diskann");
            }
        } else {
            let candidates = if mode == "filtered_ann" {
                ", candidates => 4"
            } else {
                ""
            };
            let statement = format!("SELECT v.id, distance FROM vector_index_search('incremental', $1, mode => '{mode}'{candidates}, top_k => 4, max_work => 1000) AS v WHERE v.id >= 1 AND v.id <= 5 ORDER BY distance, v.id");
            let report = read
                .query_sql_streaming(
                    permit,
                    budget,
                    crate::VectorIndexSqlRequest {
                        sql: &statement,
                        parameters: &[embedding],
                        output_limits: Default::default(),
                        task: Default::default(),
                    },
                    collect,
                )
                .unwrap();
            if mode == "filtered_ann" {
                assert_eq!(report.index().backend, "diskann");
            }
        }
        assert_eq!(
            rows,
            vec![Value::Int(4), Value::Int(5), Value::Int(1)],
            "{mode}"
        );
    }
}

#[test]
fn vector_index_incremental_native_publication_reuses_graphs_and_preserves_old_domains() {
    for graph in [false, true] {
        for lsg in [false, true] {
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
            for id in 1..=32 {
                let embedding = Value::Vector(Vector::new(&[1.0, id as f32 / 100.0]).unwrap());
                if graph {
                    db.query_with_params(
                        "CREATE (:Document {id: $id, embedding: $embedding})",
                        &BTreeMap::from([
                            ("id".into(), Value::Int(id)),
                            ("embedding".into(), embedding),
                        ]),
                    )
                    .unwrap();
                } else {
                    db.query_sql_with_params(
                        "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                        &[Value::Int(id), embedding],
                    )
                    .unwrap();
                }
            }
            let options = super::diskann::options(lsg);
            let target = fixture.0.join("incremental");
            let mut consumer = db
                .create_vector_index(
                    "incremental",
                    &field,
                    &target,
                    registration(),
                    options.clone(),
                )
                .unwrap();
            let permit = db
                .search_projection_consumer_read_permit(
                    RuntimeWorkRequest::foreground_query(128 << 20, 2 << 20).with_io_wave_slots(1),
                )
                .unwrap();
            let old = db
                .begin_read_transaction()
                .unwrap()
                .bind_vector_index(&consumer, &permit)
                .unwrap();
            let before = query(&old, &permit);
            assert_eq!(before.len(), 32);
            let old_graph = target.join("search_rabitq.1.hawdb");
            let original = std::fs::read(&old_graph).unwrap();
            for vector in [[0.2, 1.0], [0.1, 1.0]] {
                let embedding = Value::Vector(Vector::new(&vector).unwrap());
                if graph {
                    db.query_with_params(
                        "MATCH (d:Document {id: 1}) SET d.embedding = $embedding",
                        &BTreeMap::from([("embedding".into(), embedding)]),
                    )
                    .unwrap();
                } else {
                    db.query_sql_with_params(
                        "UPDATE vectors SET embedding = $1 WHERE id = 1",
                        &[embedding],
                    )
                    .unwrap();
                }
            }
            if graph {
                db.query("MATCH (d:Document {id: 2}) DELETE d").unwrap();
                db.query("MATCH (d:Document {id: 3}) SET d.embedding = NULL")
                    .unwrap();
                db.query_with_params(
                    "CREATE (:Document {id: 33, embedding: $embedding})",
                    &BTreeMap::from([(
                        "embedding".into(),
                        Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
                    )]),
                )
                .unwrap();
            } else {
                db.query_sql("DELETE FROM vectors WHERE id = 2").unwrap();
                db.query_sql("UPDATE vectors SET embedding = NULL WHERE id = 3")
                    .unwrap();
                db.query_sql_with_params(
                    "INSERT INTO vectors (id, embedding) VALUES (33, $1)",
                    &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
                )
                .unwrap();
            }
            let report = db
                .catch_up_vector_index(&mut consumer, options.clone())
                .unwrap();
            assert_eq!(report.mapped_candidates, 4);
            assert_eq!(report.upserted_vectors, 2);
            assert_eq!(report.retraction_candidates, 2);
            assert!(report.feed_items > report.mapped_candidates);
            assert_eq!(consumer.document_count().unwrap(), 31);
            assert_eq!(
                consumer.canonical_vector_source_epoch().unwrap(),
                Some(report.source_epoch)
            );
            let current = db
                .begin_read_transaction()
                .unwrap()
                .bind_vector_index(&consumer, &permit)
                .unwrap();
            assert_eq!(query(&current, &permit), expected());
            let generation = current.generation();
            drop(current);
            filtered_language(&mut db, &consumer, graph, &permit);
            let (root, manifest) = current_manifest(&fixture, report.source_epoch);
            let segments = manifest["body"]["segments"].as_array().unwrap();
            assert_eq!(segments.len(), 2);
            assert_eq!(
                segments[1]["rabitq_vector_document_count"].as_u64(),
                Some(2)
            );
            let reused = root.join("search_rabitq.1.hawdb");
            assert_eq!(std::fs::read(&reused).unwrap(), original);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                assert_eq!(
                    std::fs::metadata(&old_graph).unwrap().ino(),
                    std::fs::metadata(&reused).unwrap().ino()
                );
            }
            let noop = db
                .catch_up_vector_index(&mut consumer, options.clone())
                .unwrap();
            assert_eq!(noop.mapped_candidates, 0);
            let current = db
                .begin_read_transaction()
                .unwrap()
                .bind_vector_index(&consumer, &permit)
                .unwrap();
            assert_eq!(current.generation(), generation);
            drop(current);
            db.checkpoint().unwrap();
            if graph {
                db.query("MATCH (d:Document) WHERE d.id >= 1 AND d.id <= 33 DELETE d")
                    .unwrap();
            } else {
                db.query_sql("DELETE FROM vectors WHERE id >= 1 AND id <= 33")
                    .unwrap();
            }
            let empty = db
                .catch_up_vector_index(&mut consumer, options.clone())
                .unwrap();
            assert_eq!(empty.upserted_vectors, 0);
            assert_eq!(consumer.document_count().unwrap(), 0);
            let (root, manifest) = current_manifest(&fixture, empty.source_epoch);
            assert_eq!(manifest["body"]["segments"].as_array().unwrap().len(), 2);
            assert!(!root.join("search_rabitq.3.hawdb").exists());
            assert_eq!(query(&old, &permit), before);
            if graph {
                db.query_with_params(
                    "CREATE (:Document {id: 100, embedding: $embedding})",
                    &BTreeMap::from([(
                        "embedding".into(),
                        Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
                    )]),
                )
                .unwrap();
            } else {
                db.query_sql_with_params(
                    "INSERT INTO vectors (id, embedding) VALUES (100, $1)",
                    &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
                )
                .unwrap();
            }
            db.catch_up_vector_index(&mut consumer, options).unwrap();
            assert_eq!(distances(&mut db, &consumer), [0.0]);
            assert_eq!(query(&old, &permit), before);
            drop(old);
            drop(permit);
            drop(consumer);
            drop(db);
            let mut db = fixture.database();
            let consumer = db
                .open_search_projection_consumer(
                    &SearchProjectionConsumerId::new("incremental").unwrap(),
                    &target,
                )
                .unwrap();
            assert_eq!(consumer.document_count().unwrap(), 1);
            assert_eq!(distances(&mut db, &consumer), [0.0]);
        }
    }
}

#[test]
fn vector_index_incremental_feed_refusal_preserves_the_completed_base_and_allows_retry() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
        &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
    )
    .unwrap();
    let mut consumer = db
        .create_vector_index(
            "retry",
            &sql_field(),
            fixture.0.join("retry"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let base = consumer.canonical_vector_source_epoch().unwrap();
    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query_sql_with_params(
                "UPDATE vectors SET embedding = $1 WHERE id = 1",
                &[Value::Vector(Vector::new(&[-1.0, 0.0]).unwrap())],
            )
            .unwrap();
        transaction.rollback();
    }
    let unchanged = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(unchanged.mapped_candidates, 0);
    assert_eq!(Some(unchanged.source_epoch), base);
    db.query_sql_with_params(
        "UPDATE vectors SET embedding = $1 WHERE id = 1",
        &[Value::Vector(Vector::new(&[0.0, 1.0]).unwrap())],
    )
    .unwrap();
    let mut limited = VectorIndexBuildOptions::default();
    limited.feed.work = NonZeroUsize::MIN;
    assert!(matches!(
        db.catch_up_vector_index(&mut consumer, limited),
        Err(VectorIndexBuildError::Source(
            ProjectionFeedError::BudgetExceeded(_)
        ))
    ));
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    consumer.projection().require_completed_view().unwrap();
    let limited = VectorIndexBuildOptions {
        memory_bytes: NonZeroUsize::new(1024).unwrap(),
        ..Default::default()
    };
    assert!(matches!(
        db.catch_up_vector_index(&mut consumer, limited),
        Err(VectorIndexBuildError::Query(
            HawDBError::ResourceBudgetExceeded(_)
        ))
    ));
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    let cancelled = VectorIndexBuildOptions::default();
    cancelled.task.cancellation().cancel();
    assert!(db.catch_up_vector_index(&mut consumer, cancelled).is_err());
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    let report = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(report.mapped_candidates, 1);
    assert_eq!(distances(&mut db, &consumer), [1.0]);
}

#[test]
fn vector_index_incremental_refuses_unproven_baselines_and_schema_drift() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
        &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
    )
    .unwrap();
    let mut arbitrary = db
        .create_search_projection_consumer(
            SearchProjectionConsumerId::new("arbitrary").unwrap(),
            fixture.0.join("arbitrary"),
            registration(),
            |source, index| {
                let binding = source.resolve_vector_index(
                    "arbitrary",
                    &sql_field(),
                    VectorDistance::Cosine,
                    VectorIndexBackend::default(),
                )?;
                index.bind_vector_index(binding)
            },
        )
        .unwrap();
    assert_eq!(arbitrary.canonical_vector_source_epoch().unwrap(), None);
    assert!(db
        .catch_up_vector_index(&mut arbitrary, Default::default())
        .is_err());
    arbitrary.projection().require_completed_view().unwrap();
    let mut consumer = db
        .create_vector_index(
            "canonical",
            &sql_field(),
            fixture.0.join("canonical"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let base = consumer.canonical_vector_source_epoch().unwrap();
    let changed_algorithm = super::diskann::options(true);
    assert!(db
        .catch_up_vector_index(&mut consumer, changed_algorithm)
        .is_err());
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    db.query_sql("ALTER TABLE vectors ADD COLUMN payload TEXT")
        .unwrap();
    assert!(db
        .catch_up_vector_index(&mut consumer, Default::default())
        .is_err());
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    consumer.projection().require_completed_view().unwrap();
}

#[test]
fn vector_index_canonical_completion_rejects_late_builder_mutation() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let spoof = db.create_search_projection_consumer(
        SearchProjectionConsumerId::new("spoof").unwrap(),
        fixture.0.join("spoof"),
        registration(),
        |source, index| {
            index.bind_vector_index(source.resolve_vector_index(
                "spoof",
                &sql_field(),
                VectorDistance::Cosine,
                VectorIndexBackend::default(),
            )?)?;
            index.finish_canonical_vector_source(source.commit_epoch(), None, Vec::new())
        },
    );
    assert!(spoof.err().unwrap().to_string().contains("projector"));
    let id = SearchProjectionConsumerId::new("late").unwrap();
    let result = db.create_consumer_with_initializer::<{ hawdb_search::projection_consumer::MAX_CONSUMERS }, _>(
        id,
        &fixture.0.join("late-failed"),
        registration(),
        Default::default(),
        hawdb_search::consumer::RegisteredConsumerProjector::CanonicalVectorField,
        |source, index, _permit| {
            index.bind_vector_index(source.resolve_vector_index(
                "late",
                &sql_field(),
                VectorDistance::Cosine,
                VectorIndexBackend::default(),
            )?)?;
            index.finish_canonical_vector_source(source.commit_epoch(), None, Vec::new())?;
            index.delete("r:00");
            Ok(())
        },
    );
    assert!(result.is_err());
    let consumer = db
        .create_vector_index(
            "late",
            &sql_field(),
            fixture.0.join("late-fresh"),
            registration(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(consumer.document_count().unwrap(), 0);
}

#[test]
fn vector_index_incremental_retains_explicit_rabitq_and_euclidean_zero_semantics() {
    for (euclidean, rabitq_bits) in [
        (false, Some(1)),
        (true, None),
        (true, Some(1)),
        (true, Some(4)),
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        let options = VectorIndexBuildOptions {
            distance: if euclidean {
                VectorDistance::Euclidean
            } else {
                VectorDistance::Cosine
            },
            backend: rabitq_bits.map_or_else(VectorIndexBackend::default, |bit_width| {
                VectorIndexBackend::RaBitQ {
                    bit_width,
                    segment_rows: 64,
                    transform_seed: 42,
                }
            }),
            ..Default::default()
        };
        let first = if euclidean { [0.0, 0.0] } else { [1.0, 0.0] };
        for (id, vector) in [(1, first), (2, [0.0, 1.0])] {
            db.query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                &[Value::Int(id), Value::Vector(Vector::new(&vector).unwrap())],
            )
            .unwrap();
        }
        let target = fixture.0.join("other-backend");
        let mut consumer = db
            .create_vector_index(
                "other-backend",
                &sql_field(),
                &target,
                registration(),
                options.clone(),
            )
            .unwrap();
        db.query_sql("DELETE FROM vectors WHERE id = 1").unwrap();
        let replacement = if euclidean { [-2.0, 0.0] } else { [-1.0, 0.0] };
        db.query_sql_with_params(
            "UPDATE vectors SET embedding = $1 WHERE id = 2",
            &[Value::Vector(Vector::new(&replacement).unwrap())],
        )
        .unwrap();
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES (3, $1)",
            &[Value::Vector(Vector::new(&first).unwrap())],
        )
        .unwrap();
        db.catch_up_vector_index(&mut consumer, options).unwrap();
        let expected = if euclidean {
            vec![1.0, 3.0]
        } else {
            vec![0.0, 2.0]
        };
        assert_eq!(distances(&mut db, &consumer), expected);
        drop(consumer);
        drop(db);
        let mut db = fixture.database();
        let consumer = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new("other-backend").unwrap(),
                &target,
            )
            .unwrap();
        assert_eq!(distances(&mut db, &consumer), expected);
    }
}

#[test]
fn vector_index_incremental_admits_borrowed_keys_before_encoding_and_retries() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id TEXT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let mut consumer = db
        .create_vector_index(
            "keys",
            &sql_field(),
            fixture.0.join("keys"),
            registration(),
            Default::default(),
        )
        .unwrap();
    let base = consumer.canonical_vector_source_epoch().unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
        &[
            Value::String("x".repeat(256)),
            Value::Vector(Vector::new(&[1.0, 0.0]).unwrap()),
        ],
    )
    .unwrap();
    let mut limited = VectorIndexBuildOptions::default();
    limited.source.key_bytes = NonZeroUsize::new(64).unwrap();
    let refusal = db.catch_up_vector_index(&mut consumer, limited);
    assert!(
        matches!(refusal, Err(VectorIndexBuildError::Source(ProjectionFeedError::Callback(HawDBError::ResourceBudgetExceeded(ref error)))) if error.budget_option == "VectorIndexBuildOptions.source.key_bytes")
    );
    assert_eq!(consumer.canonical_vector_source_epoch().unwrap(), base);
    let report = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(report.mapped_candidates, 1);
    assert_eq!(distances(&mut db, &consumer), [0.0]);
}
