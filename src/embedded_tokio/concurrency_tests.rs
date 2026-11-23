// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::api::AutocommitMutationPhase;
use crate::{Database, OwnedSqlQueryRequest};
use hawdb_qos::{RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct Fixture {
    engine: HawDBTokioEmbedded,
    path: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-tokio-writers-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(4 << 30), None, None, None),
        );
        let engine = HawDBTokioEmbedded::open_owned(
            HawDBEmbeddedOpenOptions::new(&path).with_resource_snapshot(resources),
        )
        .unwrap();
        engine.with_embedded_mut(|embedded| {
            let database = embedded.database_mut();
            database
                .query("CREATE (:Counter {id: 1, counter: 10})")
                .unwrap();
            database
                .query("CREATE (:Counter {id: 2, counter: 20})")
                .unwrap();
            database
                .query("CREATE CONSTRAINT ON :Counter(id) ASSERT UNIQUE")
                .unwrap();
            database.query("CREATE INDEX ON :Counter(id)").unwrap();
            database
                .query_sql("CREATE TABLE counters (id BIGINT PRIMARY KEY, counter BIGINT NOT NULL)")
                .unwrap();
            database
                .query_sql("INSERT INTO counters (id, counter) VALUES (1, 10), (2, 20)")
                .unwrap();
        });
        Self { engine, path }
    }

    fn close(self) {
        let snapshot = self.engine.runtime_snapshot();
        assert_eq!(snapshot.admitted_memory_bytes, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.active_blocking_tasks, 0);
        drop(self.engine);
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

fn write(
    engine: &HawDBTokioEmbedded,
    sql: bool,
    id: i64,
    value: i64,
) -> Result<QueryOutput, HawDBTokioEmbeddedError> {
    engine.runtime().block_on(async {
        if sql {
            engine.query_sql_request(OwnedSqlQueryRequest::new("UPDATE counters SET counter = $2 WHERE id = $1")
                .with_params(vec![Value::Int(id), Value::Int(value)])).await
        } else {
            engine.query_request(OwnedQueryRequest::new("MATCH (n:Counter) WHERE n.id = $id SET n.counter = $value RETURN count(n) AS total")
                .with_params(BTreeMap::from([("id".into(), Value::Int(id)), ("value".into(), Value::Int(value))]))).await
        }
    }).unwrap()
}

fn read(engine: &HawDBTokioEmbedded, sql: bool, id: i64) -> Value {
    let output = engine
        .runtime()
        .block_on(async {
            if sql {
                engine
                    .query_sql_request(
                        OwnedSqlQueryRequest::new("SELECT counter FROM counters WHERE id = $1")
                            .with_params(vec![Value::Int(id)]),
                    )
                    .await
            } else {
                engine
                    .query_request(
                        OwnedQueryRequest::new(
                            "MATCH (n:Counter) WHERE n.id = $id RETURN n.counter AS counter",
                        )
                        .with_params(BTreeMap::from([("id".into(), Value::Int(id))])),
                    )
                    .await
            }
        })
        .unwrap()
        .unwrap();
    output.rows[0]["counter"].clone()
}

#[test]
fn sql_autocommit_clock_advances_without_a_write_on_tokio_workers() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    let query = || {
        engine
            .runtime()
            .block_on(engine.query_sql_request(OwnedSqlQueryRequest::new(
                "SELECT CURRENT_TIMESTAMP AS t, now() AS n",
            )))
            .unwrap()
            .unwrap()
    };
    let first = query();
    assert_eq!(first.rows[0]["t"], first.rows[0]["n"]);
    thread::sleep(Duration::from_millis(20));
    let second = query();
    assert_eq!(second.rows[0]["t"], second.rows[0]["n"]);
    assert_ne!(first.rows[0]["t"], second.rows[0]["t"]);
    fixture.close();
}

#[test]
fn replacement_keeps_tokio_and_inner_admission_on_one_governor() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    engine.with_embedded_mut(|embedded| *embedded = HawDBEmbedded::open_in_memory());
    let governor = engine.runtime().governor();
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::new(RuntimeWorkPriority::Foreground, RuntimeWorkKind::Control)
                .with_cpu_slots(governor.snapshot().limits.effective_cpu_slots.get()),
        )
        .unwrap();
    let result = engine.with_embedded_mut(|embedded| {
        embedded.query_sql_request_admitted(crate::SqlQueryRequest::new("SELECT 1"))
    });
    assert!(
        matches!(result, Err(crate::EmbeddedQueryError::Admission(ref error))
        if error.code == hawdb_qos::RuntimeAdmissionCode::CpuSaturated),
        "{result:?}"
    );
    drop(permit);
    engine
        .runtime()
        .block_on(engine.query_sql_request(OwnedSqlQueryRequest::new("SELECT 1")))
        .unwrap()
        .unwrap();
    fixture.close();
}

#[test]
fn ordinary_sql_and_cypher_writers_overlap_with_reads_and_refund_admission() {
    for sql in [false, true] {
        let fixture = Fixture::new();
        let engine = &fixture.engine;
        let concurrent = engine.embedded.concurrent(&engine.concurrent).unwrap();
        let (ready, resume) =
            concurrent.pause_next_autocommit_mutation(AutocommitMutationPhase::BeforeExecution);
        let first_engine = engine.clone();
        let first = thread::spawn(move || write(&first_engine, sql, 1, 99));
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        let other_engine = engine.clone();
        let (done, completed) = mpsc::channel();
        let other = thread::spawn(move || {
            let result = (|| {
                for value in 1..=8 {
                    write(&other_engine, sql, 2, value)?;
                }
                Ok::<_, HawDBTokioEmbeddedError>((
                    read(&other_engine, sql, 1),
                    read(&other_engine, sql, 2),
                ))
            })();
            done.send(result).unwrap();
        });
        let progress = completed.recv_timeout(Duration::from_secs(10));
        resume.send(()).unwrap();
        first.join().unwrap().unwrap();
        other.join().unwrap();
        assert_eq!(
            progress.unwrap().unwrap(),
            (Value::Int(10), Value::Int(8)),
            "sql={sql}"
        );
        assert_eq!(read(engine, sql, 1), Value::Int(99));
        drop(concurrent);
        fixture.close();
    }
}

#[test]
fn new_sql_cypher_and_stream_reads_bypass_the_held_commit_owner() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    assert_eq!(read(engine, false, 1), Value::Int(10));
    let concurrent = engine.embedded.concurrent(&engine.concurrent).unwrap();
    let (held, acquired) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let writer = thread::spawn(move || {
        concurrent.with_autocommit_exclusive(|database| {
            database.query_sql("UPDATE counters SET counter = 99 WHERE id = 1")?;
            held.send(()).unwrap();
            resume.recv().unwrap();
            database.query_sql("SELECT counter FROM counters WHERE id = 1")
        })
    });
    acquired.recv_timeout(Duration::from_secs(10)).unwrap();
    let reader = engine.clone();
    let (done, completed) = mpsc::channel();
    let reads = thread::spawn(move || {
        let sql = read(&reader, true, 1);
        let graph = read(&reader, false, 1);
        let rows = reader
            .runtime()
            .block_on(async {
                let mut stream = reader
                    .query_stream(
                        "MATCH (n:Counter) RETURN n.id",
                        RuntimeTaskContext::default(),
                    )
                    .await
                    .unwrap();
                let mut rows = 0;
                while let Some(batch) = stream.next_batch().await.unwrap() {
                    rows += batch.len();
                }
                rows
            })
            .unwrap();
        done.send((sql, graph, rows)).unwrap();
    });
    let progress = completed.recv_timeout(Duration::from_secs(10));
    release.send(()).unwrap();
    writer.join().unwrap().unwrap();
    reads.join().unwrap();
    assert_eq!(progress.unwrap(), (Value::Int(10), Value::Int(10), 2));
    assert_eq!(read(engine, true, 1), Value::Int(99));
    fixture.close();
}

#[test]
fn data_commits_during_scope_wait_reuse_one_cypher_preparation() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    let concurrent = engine.embedded.concurrent(&engine.concurrent).unwrap();
    let (ready, resume) =
        concurrent.pause_next_autocommit_mutation(AutocommitMutationPhase::BeforeLocks);
    let writer_engine = engine.clone();
    let writer = thread::spawn(move || {
        writer_engine.runtime().block_on(writer_engine.query(
            "MATCH (n:Counter) WHERE n.id = 1 SET n.counter = n.counter + 1 RETURN n.counter AS counter",
            RuntimeTaskContext::default(),
        )).unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    write(engine, false, 1, 40).unwrap();
    let cache = engine.with_embedded(|embedded| embedded.database().plan_cache_stats().unwrap());
    resume.send(()).unwrap();
    assert_eq!(
        writer.join().unwrap().unwrap().rows[0]["counter"],
        Value::Int(41)
    );
    let after = engine.with_embedded(|embedded| embedded.database().plan_cache_stats().unwrap());
    assert_eq!(
        after, cache,
        "execution must consume the admitted preparation"
    );
    drop(concurrent);
    fixture.close();
}

#[test]
fn direct_host_mutation_waits_for_private_writers_and_refreshes_publication() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    let concurrent = engine.embedded.concurrent(&engine.concurrent).unwrap();
    let (ready, resume) =
        concurrent.pause_next_autocommit_mutation(AutocommitMutationPhase::BeforeExecution);
    let writer_engine = engine.clone();
    let writer = thread::spawn(move || write(&writer_engine, true, 1, 99));
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let direct_engine = engine.clone();
    let (started, starting) = mpsc::channel();
    let (done, completed) = mpsc::channel();
    let direct = thread::spawn(move || {
        started.send(()).unwrap();
        direct_engine.with_embedded_mut(|embedded| {
            done.send(
                embedded
                    .database_mut()
                    .query_sql("UPDATE counters SET counter = counter + 1 WHERE id = 1"),
            )
            .unwrap();
        });
    });
    starting.recv_timeout(Duration::from_secs(10)).unwrap();
    let premature = completed.recv_timeout(Duration::from_millis(50));
    resume.send(()).unwrap();
    writer.join().unwrap().unwrap();
    direct.join().unwrap();
    assert!(matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)));
    completed
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    assert_eq!(read(engine, true, 1), Value::Int(100));
    drop(concurrent);
    fixture.close();
}

#[test]
fn source_replacement_after_preparation_rejects_the_old_request() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    engine
        .runtime()
        .block_on(async {
            let input = Arc::new(RuntimeQueryInput::new(OwnedQueryRequest::new(
                "CREATE (:Marker {id: 1})",
            )));
            let prepared = engine
                .prepare_query(
                    Arc::clone(&input),
                    RuntimeWorkPriority::Foreground,
                    RuntimeTaskContext::default(),
                )
                .await
                .unwrap();
            let admission = prepared.admission().clone();
            engine.with_embedded_mut(|embedded| *embedded.database_mut() = Database::new());
            let snapshot = engine.runtime_snapshot();
            let request = admission.runtime_work_request_for_snapshot(0, snapshot);
            let result = engine
                .execute_query_with_request_factory(
                    input,
                    prepared,
                    request,
                    move |_| request,
                    RuntimeTaskContext::default(),
                )
                .await;
            assert!(result.unwrap_err().to_string().contains("instance changed"));
            engine
                .query("CREATE (:Marker {id: 2})", RuntimeTaskContext::default())
                .await
                .unwrap();
            let rows = engine
                .query(
                    "MATCH (n:Marker) RETURN n.id AS id",
                    RuntimeTaskContext::default(),
                )
                .await
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["id"], Value::Int(2));
        })
        .unwrap();
    fixture.close();
}

#[test]
fn cancelled_sql_staging_releases_owner_and_runtime_capacity() {
    let fixture = Fixture::new();
    let engine = &fixture.engine;
    let concurrent = engine.embedded.concurrent(&engine.concurrent).unwrap();
    let (ready, resume) =
        concurrent.pause_next_autocommit_mutation(AutocommitMutationPhase::Staged);
    let task = RuntimeTaskContext::default();
    let writer_task = task.clone();
    let writer_engine = engine.clone();
    let writer = thread::spawn(move || {
        writer_engine
            .runtime()
            .block_on(
                writer_engine.query_sql_request(
                    OwnedSqlQueryRequest::new("UPDATE counters SET counter = 99 WHERE id = 1")
                        .with_task_context(writer_task),
                ),
            )
            .unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    task.cancellation().cancel();
    resume.send(()).unwrap();
    assert!(writer.join().unwrap().is_err());
    assert_eq!(read(engine, true, 1), Value::Int(10));
    write(engine, true, 1, 11).unwrap();
    drop(concurrent);
    fixture.close();
}
