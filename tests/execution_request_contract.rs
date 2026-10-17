use hawdb::executor::{
    execute_with_request, execute_with_request_consumer, ExecutionMemoryConfig, ExecutionRequest,
    ExecutionResources, NoExternalReadOperator,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::schema::Catalog;
use hawdb::store::GraphStore;
use hawdb::value::Value;
use std::collections::BTreeMap;

struct LimitArrowStream(hawdb::ArrowArrayStream);

impl Drop for LimitArrowStream {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            // This guard owns the sole raw stream release obligation.
            unsafe { release(&mut self.0) };
        }
    }
}

struct LimitArrowArray(hawdb::ArrowArray);

impl Drop for LimitArrowArray {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            // Each successful pull transfers this array's release obligation.
            unsafe { release(&mut self.0) };
        }
    }
}

fn consume_limit_arrow(cursor: hawdb::RetainedQueryCursor) -> Result<usize, (usize, String)> {
    let mut stream = LimitArrowStream(cursor.into_arrow_stream(0).unwrap().into_raw());
    let mut rows = 0usize;
    loop {
        let mut array = LimitArrowArray(hawdb::ArrowArray::default());
        // Descriptors remain owned by their guards through callbacks and reads.
        let code = unsafe { stream.0.get_next.unwrap()(&mut stream.0, &mut array.0) };
        if code != 0 {
            assert!(array.0.release.is_none());
            let error = unsafe { stream.0.get_last_error.unwrap()(&mut stream.0) };
            assert!(!error.is_null());
            let message = unsafe { std::ffi::CStr::from_ptr(error) }
                .to_str()
                .unwrap()
                .to_owned();
            assert!(!message.is_empty());
            let repeated = unsafe { stream.0.get_next.unwrap()(&mut stream.0, &mut array.0) };
            assert_eq!(repeated, code, "terminal refusal must not turn into EOF");
            assert!(array.0.release.is_none());
            return Err((rows, message));
        }
        if array.0.release.is_none() {
            return Ok(rows);
        }
        assert_eq!(array.0.n_children, 1);
        let count = usize::try_from(array.0.length).unwrap();
        // The fixture exports one non-null signed-int column in insertion order.
        let child = unsafe { &**array.0.children };
        assert_eq!(child.null_count, 0);
        let offset = usize::try_from(child.offset).unwrap();
        let values = unsafe {
            std::slice::from_raw_parts((*child.buffers.add(1)).cast::<i64>(), offset + count)
        };
        for value in &values[offset..] {
            assert_eq!(*value, i64::try_from(rows).unwrap());
            rows += 1;
        }
    }
}

#[test]
fn default_unlimited_and_explicit_read_row_limits_preserve_complete_delivery() {
    use hawdb::{
        DatabaseConfig, EmbeddedQueryError, HawDBEmbedded, HawDBError, ReadBudgetResource,
        RetainedColumnValues, RetainedQueryError, RetainedQueryOptions, RetainedQueryStatus,
    };

    assert_eq!(DatabaseConfig::default().max_read_result_rows, None);
    assert_eq!(hawdb::SessionConfig::default().max_result_rows, 0);
    let cap = 100_000;
    let directory = (0..1000)
        .find_map(|attempt| {
            let path = std::env::temp_dir().join(format!(
                "hawdb-default-read-cap-{}-{attempt}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => Some(IdentityDirectory(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                Err(error) => panic!("reserve read-limit fixture: {error}"),
            }
        })
        .expect("a uniquely reserved fixture directory");
    for (persistent, configured) in [
        (false, None),
        (true, None),
        (false, Some(0)),
        (true, Some(0)),
        (false, Some(cap)),
        (true, Some(cap)),
    ] {
        let bounded = configured.is_some_and(|rows| rows != 0);
        let config = DatabaseConfig {
            max_read_result_rows: configured,
            ..DatabaseConfig::default()
        };
        let path = directory.0.join(format!("project-{configured:?}"));
        let mut engine = if persistent {
            let mut options = hawdb::HawDBEmbeddedOpenOptions::new(&path);
            options.config = config;
            HawDBEmbedded::open_with_options(options).unwrap()
        } else {
            HawDBEmbedded::open_in_memory_with_config(config)
        };
        engine.query_admitted("CREATE NODE TABLE RowLimit").unwrap();
        engine
            .query_admitted("CREATE PROPERTY ON NODE TABLE RowLimit(score) TYPE INT")
            .unwrap();
        for start in (0..=cap).step_by(512) {
            let values = (start..(start + 512).min(cap + 1))
                .map(|score| {
                    Value::Map(BTreeMap::from([(
                        "score".into(),
                        Value::Int(i64::try_from(score).unwrap()),
                    )]))
                })
                .collect();
            engine
                .query_with_params_admitted(
                    "UNWIND $scores AS row CREATE (:RowLimit {score: row.score})",
                    &BTreeMap::from([("scores".into(), Value::List(values))]),
                )
                .unwrap();
        }
        let epoch = engine.database().commit_epoch().unwrap();
        let query = "MATCH (n:RowLimit) WHERE n.score < $count RETURN n.score AS score";
        for count in [cap - 1, cap, cap + 1] {
            let params =
                BTreeMap::from([("count".into(), Value::Int(i64::try_from(count).unwrap()))]);
            let owned = engine.query_with_params_admitted(query, &params);
            if !bounded || count <= cap {
                let output = owned.unwrap();
                assert_eq!(output.rows.len(), count);
                for (index, row) in output.rows.iter().enumerate() {
                    assert_eq!(row.get("score"), Some(&Value::Int(index as i64)));
                }
            } else {
                assert!(matches!(owned.unwrap_err(),
                    EmbeddedQueryError::Database(HawDBError::ReadBudgetExceeded(cause))
                    if cause.resource == ReadBudgetResource::Rows && cause.limit == cap));
            }
            let options = RetainedQueryOptions {
                max_result_rows: Some(cap + 1000),
                ..RetainedQueryOptions::default()
            };
            let mut cursor = engine
                .database()
                .query_with_params_retained(query, &params, options)
                .unwrap();
            let mut retained_rows = 0;
            loop {
                match cursor.next_batch() {
                    Ok(Some(batch)) => {
                        let RetainedColumnValues::Int64(values) = batch.column(0).unwrap() else {
                            panic!("integer fixture")
                        };
                        for index in batch.selected_rows() {
                            assert_eq!(values[*index as usize], retained_rows as i64);
                            retained_rows += 1;
                        }
                        assert!(!bounded || retained_rows <= cap);
                    }
                    Ok(None) => {
                        assert!(
                            !bounded || count <= cap,
                            "over-cap result must not appear complete"
                        );
                        assert_eq!(retained_rows, count);
                        assert_eq!(cursor.status(), RetainedQueryStatus::Completed);
                        break;
                    }
                    Err(error) => {
                        assert!(bounded);
                        assert_eq!(count, cap + 1);
                        assert!(
                            matches!(error, RetainedQueryError::ResultBudget { rows, .. } if rows > cap)
                        );
                        assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
                        assert_eq!(cursor.next_batch().unwrap_err(), error);
                        assert_eq!(cursor.profile().source_pinned_capacity_bytes, 0);
                        break;
                    }
                }
            }
            cursor.close();
            drop(cursor);
            let arrow = engine
                .database()
                .query_with_params_retained(query, &params, options)
                .unwrap();
            match consume_limit_arrow(arrow) {
                Ok(rows) => {
                    assert!(!bounded || count <= cap);
                    assert_eq!(rows, count);
                }
                Err((rows, message)) => {
                    assert!(bounded);
                    assert_eq!(count, cap + 1);
                    assert!(rows <= cap);
                    assert!(
                        message.starts_with(&format!(
                            "retained query ResultBudget {{ rows: {},",
                            cap + 1
                        )),
                        "unexpected Arrow refusal: {message}"
                    );
                }
            }
            assert_eq!(engine.database().commit_epoch().unwrap(), epoch);
            let governor = engine.runtime_governor();
            let retained = governor.retained_result_snapshot();
            assert_eq!(retained.retained_bytes, 0);
            assert_eq!(retained.buffer_owners, 0);
            assert_eq!(retained.view_handles, 0);
            let runtime = governor.snapshot();
            assert_eq!(runtime.admissions, runtime.completions);
            assert_eq!(runtime.admitted_memory_bytes, 0);
        }
        let mut strict = engine
            .database()
            .query_with_params_retained(
                query,
                &BTreeMap::from([("count".into(), Value::Int(2))]),
                RetainedQueryOptions {
                    batch_rows: std::num::NonZeroUsize::new(1).unwrap(),
                    max_result_rows: Some(1),
                    ..RetainedQueryOptions::default()
                },
            )
            .unwrap();
        assert_eq!(
            strict.next_batch().unwrap().unwrap().selected_rows().len(),
            1
        );
        assert!(matches!(
            strict.next_batch().unwrap_err(),
            RetainedQueryError::ResultBudget { rows: 2, .. }
        ));
        drop(strict);
        let output = engine
            .query_admitted("MATCH (n:RowLimit) RETURN count(n) AS total")
            .unwrap();
        assert_eq!(
            output.rows[0].get("total"),
            Some(&Value::Int((cap + 1) as i64))
        );
        // A predicate exercises aggregate execution independently of the
        // unfiltered label-count fast path above.
        let aggregate = engine
            .query_admitted("MATCH (n:RowLimit) WHERE n.score >= 0 RETURN count(n) AS total")
            .unwrap();
        assert_eq!(aggregate.rows.len(), 1);
        assert_eq!(
            aggregate.rows[0].get("total"),
            Some(&Value::Int((cap + 1) as i64))
        );
        let limited =
            format!("MATCH (n:RowLimit) WHERE n.score >= 0 RETURN n.score AS score LIMIT {cap}");
        assert_eq!(engine.query_admitted(&limited).unwrap().rows.len(), cap);
        let cursor = engine
            .database()
            .query_with_params_retained(&limited, &BTreeMap::new(), RetainedQueryOptions::default())
            .unwrap();
        assert_eq!(consume_limit_arrow(cursor).unwrap(), cap);
        engine
            .query_admitted("CREATE (:RowLimit {score: -1})")
            .unwrap();
        assert!(engine.database().commit_epoch().unwrap() > epoch);
        let output = engine
            .query_admitted("MATCH (n:RowLimit) RETURN count(n) AS total")
            .unwrap();
        assert_eq!(
            output.rows[0].get("total"),
            Some(&Value::Int((cap + 2) as i64))
        );
    }
}

#[test]
fn configured_sql_row_limits_refuse_work_instead_of_returning_partial_aggregates() {
    use hawdb::{Database, DatabaseConfig, HawDBError};

    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(2),
        ..DatabaseConfig::default()
    });
    database
        .query_sql("CREATE TABLE row_limit (id BIGINT PRIMARY KEY)")
        .unwrap();
    for id in 0..3 {
        database
            .query_sql(&format!("INSERT INTO row_limit (id) VALUES ({id})"))
            .unwrap();
    }
    let epoch = database.commit_epoch().unwrap();
    let selected = database.query_sql("SELECT id FROM row_limit").unwrap_err();
    assert!(
        matches!(&selected, HawDBError::Execution(message)
        if message == "relational SQL exceeds max_intermediate_rows 2"),
        "unexpected refusal: {selected:?}"
    );
    // SQL also has an intermediate-work cap derived from the database limit.
    // An aggregate can use a complete fast path or refuse that bounded work;
    // a successful aggregate must never silently count just the first two rows.
    match database.query_sql("SELECT COUNT(*) AS total FROM row_limit") {
        Ok(output) => assert_eq!(output.rows[0].get("total"), Some(&Value::Int(3))),
        Err(error) => assert!(matches!(error, HawDBError::Execution(message)
            if message == "relational SQL exceeds max_intermediate_rows 2")),
    }
    let selected = database
        .query_sql("SELECT id FROM row_limit WHERE id = 2")
        .unwrap();
    assert_eq!(selected.rows.len(), 1);
    assert_eq!(selected.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(database.commit_epoch().unwrap(), epoch);
}

struct IdentityDirectory(std::path::PathBuf);

impl IdentityDirectory {
    fn new() -> Self {
        static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-public-identity-output-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for IdentityDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn public_analytics_identity_output_uses_query_budget_above_retained_state_cap() {
    use hawdb::{Database, DatabaseConfig, QueryStreamOptions, StorageResidencyMode};
    use std::num::NonZeroUsize;

    let directory = IdentityDirectory::new();
    let path = directory.0.join("identity-output");
    {
        let mut database = Database::open(&path).unwrap();
        database
            .query_with_params(
                "CREATE (:Oversized {id: $id})",
                &BTreeMap::from([("id".into(), Value::String("large-id".repeat(128 * 1024)))]),
            )
            .unwrap();
        database
            .query("MATCH (a:Oversized), (b:Oversized) CREATE (a)-[:LINK]->(b)")
            .unwrap();
        database
            .query("CALL project_graph('identity', ['Oversized'], ['LINK'])")
            .unwrap();
        database.checkpoint().unwrap();
    }
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let database = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                storage_residency_mode: residency,
                execution_memory: ExecutionMemoryConfig {
                    query_memory_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
                    blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        let epoch = database.commit_epoch().unwrap();
        let mut snapshot = database.begin_read_transaction().unwrap();
        let query = "CALL page_rank('identity', maxIterations := 1) RETURN node, node_id, node_label, pagerank_score";
        let mut rows = Vec::new();
        let report = snapshot
            .query_streaming(
                query,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(2 * 1024 * 1024),
                },
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        let Value::String(identity) = &rows[0]["node_id"] else {
            panic!("missing materialized identity")
        };
        assert_eq!(identity.len(), 1024 * 1024);
        assert!(identity
            .as_bytes()
            .chunks_exact(8)
            .all(|part| part == b"large-id"));
        assert_eq!(rows[0]["node_label"], Value::String("Oversized".into()));
        assert!(
            report.execution_profile.blocking_operator_memory_reports[0].peak_tracked_bytes <= 4096
        );
        let mut emitted = 0;
        let error = snapshot
            .query_streaming(
                query,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(64 * 1024),
                },
                |_| {
                    emitted += 1;
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("max_read_result_payload_bytes 65536"),
            "{error}"
        );
        assert_eq!(emitted, 0);
        assert_eq!(snapshot.commit_epoch(), epoch);
        assert_eq!(database.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn public_execution_request_materializes_a_profiled_result() {
    let plan = PhysicalPlan::EmptyExec;
    let parameters = BTreeMap::new();
    let memory = ExecutionMemoryConfig::default();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let mut external = NoExternalReadOperator;

    let output = execute_with_request(
        ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(1), None),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap();

    assert!(output.rows.is_empty());
    assert_eq!(output.profile.max_rows, Some(1));
}

#[test]
fn public_consumer_does_not_receive_rows_before_limit_validation() {
    let plan = PhysicalPlan::SeqNodeScan {
        variable: "node".to_string(),
        label: "Item".to_string(),
    };
    let parameters = BTreeMap::new();
    let memory = ExecutionMemoryConfig::default();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("rank".to_string(), Value::Int(1))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("rank".to_string(), Value::Int(2))]),
        )
        .unwrap();
    let mut external = NoExternalReadOperator;
    let mut delivered = 0;

    let error = execute_with_request_consumer(
        ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(1), None),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |_| {
            delivered += 1;
            Ok(())
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("exceeding max_read_result_rows"));
    assert_eq!(delivered, 0);
}

fn projected_fixture(
    count: usize,
    parameters: &BTreeMap<String, Value>,
) -> (PhysicalPlan, Catalog, GraphStore) {
    let logical = hawdb::planner::plan_pipeline_query(
        "MATCH (n:Item) RETURN n.rank AS rank, $payload AS payload ORDER BY rank",
        parameters,
    )
    .unwrap();
    let plan = hawdb::optimizer::CascadesOptimizer::default().optimize(&logical);
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..count).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([("rank".to_string(), Value::Int(rank as i64))]),
            )
            .unwrap();
    }
    (plan, catalog, store)
}

#[test]
fn public_request_differential_covers_parameters_limits_and_accounting() {
    // Deterministic independent row oracle, varying batching and payload shape.
    for count in [0usize, 1, 7] {
        for width in [0, 7, 64] {
            let payload = Value::String("x".repeat(width));
            let parameters = BTreeMap::from([("payload".into(), payload.clone())]);
            let (plan, mut catalog, mut store) = projected_fixture(count, &parameters);
            let expected: Vec<_> = (0..count)
                .map(|rank| {
                    BTreeMap::from([
                        ("rank".into(), Value::Int(rank as i64)),
                        ("payload".into(), payload.clone()),
                    ])
                })
                .collect();
            for batch in [1, 3] {
                let memory = ExecutionMemoryConfig {
                    batch_rows: std::num::NonZeroUsize::new(batch).unwrap(),
                    ..ExecutionMemoryConfig::default()
                };
                for max_rows in [None, Some(count), Some(count.saturating_sub(1))] {
                    let request = ExecutionRequest::new(&plan, &parameters, &memory)
                        .with_output_limits(max_rows, None);
                    let mut external = NoExternalReadOperator;
                    let materialized = execute_with_request(
                        request,
                        ExecutionResources::new(&mut catalog, &mut store, &mut external),
                    );
                    let mut delivered = Vec::new();
                    let streamed = execute_with_request_consumer(
                        request,
                        ExecutionResources::new(&mut catalog, &mut store, &mut external),
                        &mut |row| {
                            delivered.push(row);
                            Ok(())
                        },
                    );
                    if max_rows.is_some_and(|limit| count > limit) {
                        let message = materialized.unwrap_err().to_string();
                        assert!(message.contains("max_read_result_rows"));
                        assert_eq!(streamed.unwrap_err().to_string(), message);
                        assert!(delivered.is_empty());
                    } else {
                        let materialized = materialized.unwrap();
                        let streamed = streamed.unwrap();
                        assert_eq!(materialized.rows.into_rows(), expected);
                        assert_eq!(delivered, expected);
                        assert_eq!(materialized.profile.max_rows, max_rows);
                        assert_eq!(streamed.profile.max_rows, max_rows);
                        let retained = materialized.profile.pipeline_memory_report;
                        let released = streamed.profile.pipeline_memory_report;
                        assert_eq!(retained.output_rows, count);
                        assert_eq!(released.output_rows, count);
                        assert_eq!(retained.output_payload_bytes, released.output_payload_bytes);
                        assert_eq!(released.query_memory_completion_bytes, 0);
                        assert_eq!(
                            retained.query_memory_budget_bytes,
                            memory.query_memory_bytes.get()
                        );
                        assert!(
                            retained.query_memory_peak_bytes <= memory.query_memory_bytes.get()
                        );
                        assert!(
                            released.query_memory_peak_bytes <= memory.query_memory_bytes.get()
                        );
                        if count > 0 {
                            assert!(retained.query_memory_completion_bytes > 0);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn late_payload_failure_withholds_all_rows_and_resources_remain_reusable() {
    let parameters = BTreeMap::from([("payload".into(), Value::String("payload".repeat(8)))]);
    let (plan, mut catalog, mut store) = projected_fixture(3, &parameters);
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    let request = ExecutionRequest::new(&plan, &parameters, &memory);
    let baseline = execute_with_request(
        request,
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap();
    let payload_bytes = baseline.profile.pipeline_memory_report.output_payload_bytes;
    for limit in [0, payload_bytes - 1, payload_bytes] {
        let mut delivered = Vec::new();
        let result = execute_with_request_consumer(
            request.with_output_limits(None, Some(limit)),
            ExecutionResources::new(&mut catalog, &mut store, &mut external),
            &mut |row| {
                delivered.push(row);
                Ok(())
            },
        );
        if limit < payload_bytes {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("max_payload_bytes"));
            assert!(
                delivered.is_empty(),
                "late payload rejection leaked a prefix"
            );
        } else {
            assert_eq!(
                result
                    .unwrap()
                    .profile
                    .pipeline_memory_report
                    .query_memory_completion_bytes,
                0
            );
            assert_eq!(delivered, baseline.rows.clone().into_rows());
        }
    }
}

#[test]
fn public_consumer_error_and_cancellation_stop_delivery_and_allow_retry() {
    use hawdb::{HawDBError, RuntimeCancellationToken, RuntimeTaskContext};
    let parameters = BTreeMap::from([("payload".into(), Value::String("value".into()))]);
    let (plan, mut catalog, mut store) = projected_fixture(4, &parameters);
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    for cancel in [false, true] {
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let request =
            ExecutionRequest::new(&plan, &parameters, &memory).with_task_context(&context);
        let mut delivered = Vec::new();
        let error = execute_with_request_consumer(
            request,
            ExecutionResources::new(&mut catalog, &mut store, &mut external),
            &mut |row| {
                delivered.push(row);
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(HawDBError::Execution("consumer rejected row".into()))
                }
            },
        )
        .unwrap_err();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["rank"], Value::Int(0));
        assert!(error.to_string().contains(if cancel {
            "cancel"
        } else {
            "consumer rejected row"
        }));
        let retry = execute_with_request(
            ExecutionRequest::new(&plan, &parameters, &memory),
            ExecutionResources::new(&mut catalog, &mut store, &mut external),
        )
        .unwrap();
        assert_eq!(retry.rows.len(), 4);
    }
}

#[test]
fn stopped_contexts_and_denied_memory_never_call_public_consumers() {
    use hawdb::{RuntimeCancellationToken, RuntimeTaskContext};
    let parameters = BTreeMap::from([("payload".into(), Value::String("value".into()))]);
    let (plan, mut catalog, mut store) = projected_fixture(3, &parameters);
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let contexts = [
        RuntimeTaskContext::without_deadline(token),
        RuntimeTaskContext::new(
            RuntimeCancellationToken::new(),
            Some(std::time::Instant::now()),
        ),
    ];
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    for context in &contexts {
        let request = ExecutionRequest::new(&plan, &parameters, &memory).with_task_context(context);
        let error = execute_with_request_consumer(
            request,
            ExecutionResources::new(&mut catalog, &mut store, &mut external),
            &mut |_| panic!("stopped request cannot deliver rows"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stopped"));
    }
    let denied = ExecutionMemoryConfig {
        query_memory_bytes: std::num::NonZeroUsize::MIN,
        ..memory
    };
    let error = execute_with_request_consumer(
        ExecutionRequest::new(&plan, &parameters, &denied),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |_| panic!("failed memory admission cannot deliver rows"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("memory"));
}

#[derive(Default)]
struct HostVectorRead {
    calls: usize,
    observed: Option<(Vec<f32>, bool, usize)>,
}

impl hawdb::executor::ExternalReadOperator for HostVectorRead {
    fn execute_vector_seed(
        &mut self,
        request: hawdb::executor::VectorSeedExecutionRequest<'_>,
    ) -> hawdb::Result<hawdb::executor::VectorSeedExecutionOutput> {
        use hawdb::executor::{
            VectorCompressionMode, VectorExecutionBackend, VectorExecutionReport,
            VectorScoreSource, VectorSeedExecutionOutput, VectorSeedExecutionRow,
        };
        request.resources.checkpoint()?;
        self.calls += 1;
        self.observed = Some((
            request.embedding.to_vec(),
            request.resources.task_context.is_some(),
            request.resources.max_parallelism.get(),
        ));
        Ok(VectorSeedExecutionOutput {
            rows: vec![VectorSeedExecutionRow {
                id: "hit".into(),
                external_id: None,
                score: 0.75,
            }],
            report: VectorExecutionReport {
                backend: VectorExecutionBackend::ScalarFlat,
                compression_mode: VectorCompressionMode::Disabled,
                candidate_source: hawdb::planner::VectorCandidateSource::Scalar,
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                candidate_score_source: VectorScoreSource::RawVector,
                final_score_source: VectorScoreSource::RawVector,
                generated_candidate_count: 1,
                descriptor_pruned_count: 0,
                scalar_filtered_count: 0,
                residual_filtered_count: 0,
                candidate_scan_rounds: 1,
                reranked_candidate_count: 1,
                returned_count: 1,
                raw_vector_bytes_read: 0,
                candidate_scan_metrics: None,
                index_covered_document_count: None,
                index_candidate_document_count: None,
                index_coverage_complete: None,
                fallback_reason_codes: Vec::new(),
            },
        })
    }
}

#[test]
fn host_can_implement_external_reads_using_only_the_hawdb_facade() {
    let parameters = BTreeMap::from([(
        "embedding".into(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);
    use hawdb::planner::{
        VectorCandidateSource, VectorExecutionResourceProfile, VectorPhysicalPlan,
    };
    let plan = PhysicalPlan::VectorSeedScan {
        embedding_parameter: "embedding".into(),
        output_external_id: false,
        metadata_filters: BTreeMap::new(),
        resource_profile: VectorExecutionResourceProfile {
            priority: 200,
            max_parallelism: 4,
            max_working_memory_bytes: Some(2048),
        },
        vector_plan: VectorPhysicalPlan::TopK {
            limit: 3,
            input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                source: VectorCandidateSource::Scalar,
                embedding_dimension: 2,
                candidate_limit: 3,
                input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
            }),
        },
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let memory = ExecutionMemoryConfig::default();
    let context = hawdb::RuntimeTaskContext::default();
    let request = ExecutionRequest::new(&plan, &parameters, &memory).with_task_context(&context);
    let mut external = HostVectorRead::default();
    let materialized = execute_with_request(
        request,
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap();
    assert_eq!(external.observed, Some((vec![1.0, 0.0], true, 1)));
    assert_eq!(materialized.rows.len(), 1);
    assert_eq!(
        materialized.rows[0].get("id"),
        Some(&Value::String("hit".into()))
    );
    assert_eq!(materialized.rows[0].get("score"), Some(&Value::Float(0.75)));
    assert_eq!(materialized.profile.vector_execution_reports.len(), 1);
    assert_eq!(
        materialized.profile.vector_execution_reports[0].returned_count,
        1
    );
    let mut delivered = Vec::new();
    let streamed = execute_with_request_consumer(
        request,
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |row| {
            delivered.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(delivered, materialized.rows.into_rows());
    assert_eq!(
        streamed.profile.vector_execution_reports,
        materialized.profile.vector_execution_reports
    );
    assert_eq!(external.calls, 2);
    assert_eq!(
        streamed
            .profile
            .pipeline_memory_report
            .query_memory_completion_bytes,
        0
    );
}

#[test]
fn session_result_row_settings_are_isolated_and_inherited_by_transactions() {
    use hawdb::{Database, DatabaseConfig, RetainedQueryError, RetainedQueryOptions};

    for configured in [Some(0), None, Some(2)] {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_read_result_rows: configured,
            ..DatabaseConfig::default()
        });
        db.query("CREATE NODE TABLE SessionLimit").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE SessionLimit(score) TYPE INT")
            .unwrap();
        for score in 0..3 {
            db.query(&format!("CREATE (:SessionLimit {{score: {score}}})"))
                .unwrap();
        }
        let epoch = db.commit_epoch().unwrap();
        let query = "MATCH (n:SessionLimit) WHERE n.score >= 0 RETURN n.score AS score";
        {
            let mut session = db.session();
            assert_eq!(session.config().max_result_rows, 0);
            session.set_max_result_rows(1);
            assert!(session.query(query).is_err());
            let mutation_error = session.query("MATCH (n:SessionLimit) WHERE n.score >= 0 SET n.score = 42 RETURN n.score AS score").unwrap_err();
            assert!(
                mutation_error
                    .to_string()
                    .contains("max_mutation_result_rows 1"),
                "unexpected mutation refusal: {mutation_error}"
            );
            assert_eq!(
                session
                    .query("MATCH (n:SessionLimit) WHERE n.score >= 0 RETURN count(n) AS total")
                    .unwrap()
                    .rows[0]
                    .get("total"),
                Some(&Value::Int(3))
            );
            assert!(session
                .query(&format!("{query} LIMIT 0"))
                .unwrap()
                .rows
                .is_empty());
            let mut cursor = session
                .query_with_params_retained(
                    query,
                    &BTreeMap::new(),
                    RetainedQueryOptions::default(),
                )
                .unwrap();
            session.set_max_result_rows(0);
            assert!(matches!(
                cursor.next_batch(),
                Err(RetainedQueryError::ResultBudget { rows: 3, .. })
            ));
            assert!(cursor.next_batch().is_err());
            drop(cursor);
            session.set_max_result_rows(1);
            assert!(consume_limit_arrow(
                session
                    .query_with_params_retained(
                        query,
                        &BTreeMap::new(),
                        RetainedQueryOptions::default()
                    )
                    .unwrap()
            )
            .is_err());
            session.query("BEGIN TRANSACTION").unwrap();
            session.set_max_result_rows(0);
            assert!(
                session.query(query).is_err(),
                "BEGIN captures the session policy"
            );
            session.query("ROLLBACK").unwrap();
            session.set_max_result_rows(1);
            session.query("BEGIN TRANSACTION").unwrap();
            session.query("CREATE (:SessionLimit {score: 3})").unwrap();
            session.query("CREATE (:SessionLimit {score: 4})").unwrap();
            session.set_max_result_rows(0);
            let commit_error = session.query("COMMIT").unwrap_err();
            assert!(
                commit_error
                    .to_string()
                    .contains("max_mutation_result_rows 1"),
                "unexpected commit refusal: {commit_error}"
            );
            assert_eq!(
                session
                    .query("MATCH (n:SessionLimit) RETURN count(n) AS total")
                    .unwrap()
                    .rows[0]
                    .get("total"),
                Some(&Value::Int(3))
            );
            match session.query(query) {
                Ok(output) => {
                    assert_ne!(configured, Some(2));
                    assert_eq!(output.rows.len(), 3);
                }
                Err(_) => assert_eq!(configured, Some(2)),
            }
        }
        assert_eq!(db.config().max_read_result_rows, configured);
        assert_eq!(db.session().config().max_result_rows, 0);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn zero_host_session_and_request_limits_preserve_their_public_contracts() {
    let mut database = hawdb::Database::new();
    assert_eq!(database.config().max_read_result_rows, None);
    assert_eq!(database.config().read_result_row_limit(), None);
    database.query("CREATE NODE TABLE ZeroLimit").unwrap();
    database.query("CREATE (:ZeroLimit)").unwrap();
    let mut session = database.session();
    assert_eq!(session.config().max_result_rows, 0);
    assert_eq!(
        session
            .query("MATCH (n:ZeroLimit) RETURN id(n) AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(session
        .query("MATCH (n:ZeroLimit) RETURN id(n) AS id LIMIT 0")
        .unwrap()
        .rows
        .is_empty());

    let parameters = BTreeMap::from([("payload".into(), Value::Int(1))]);
    let (plan, mut catalog, mut store) = projected_fixture(1, &parameters);
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    let mut delivered = 0;
    let error = execute_with_request_consumer(
        ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(0), None),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |_| {
            delivered += 1;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(matches!(error, hawdb::HawDBError::ReadBudgetExceeded(cause)
        if cause.resource == hawdb::ReadBudgetResource::Rows && cause.limit == 0));
    assert_eq!(delivered, 0);
}

#[test]
fn owned_default_and_explicit_row_limits_cross_the_former_boundary() {
    use hawdb::{
        DatabaseConfig, EmbeddedQueryError, HawDBEmbedded, HawDBError, ReadBudgetResource,
    };

    let cap = 100_000;
    let directory = IdentityDirectory::new();
    for (persistent, bounded) in [(false, false), (true, false), (false, true), (true, true)] {
        let config = DatabaseConfig {
            max_read_result_rows: Some(if bounded { cap } else { 0 }),
            ..DatabaseConfig::default()
        };
        let path = directory.0.join(format!("owned-{bounded}"));
        let mut engine = if persistent {
            let mut options = hawdb::HawDBEmbeddedOpenOptions::new(&path);
            options.config = config;
            HawDBEmbedded::open_with_options(options).unwrap()
        } else {
            HawDBEmbedded::open_in_memory_with_config(config)
        };
        engine.query_admitted("CREATE NODE TABLE RowLimit").unwrap();
        engine
            .query_admitted("CREATE PROPERTY ON NODE TABLE RowLimit(score) TYPE INT")
            .unwrap();
        for start in (0..=cap).step_by(512) {
            let values = (start..(start + 512).min(cap + 1))
                .map(|score| {
                    Value::Map(BTreeMap::from([("score".into(), Value::Int(score as i64))]))
                })
                .collect();
            engine
                .query_with_params_admitted(
                    "UNWIND $scores AS row CREATE (:RowLimit {score: row.score})",
                    &BTreeMap::from([("scores".into(), Value::List(values))]),
                )
                .unwrap();
        }
        let epoch = engine.database().commit_epoch().unwrap();
        for count in [cap - 1, cap, cap + 1] {
            let result = engine.query_with_params_admitted(
                "MATCH (n:RowLimit) WHERE n.score < $count RETURN n.score AS score",
                &BTreeMap::from([("count".into(), Value::Int(count as i64))]),
            );
            if bounded && count > cap {
                assert!(
                    matches!(result.unwrap_err(), EmbeddedQueryError::Database(HawDBError::ReadBudgetExceeded(cause))
                    if cause.resource == ReadBudgetResource::Rows && cause.limit == cap)
                );
            } else {
                let output = result.unwrap();
                assert_eq!(output.rows.len(), count);
                for (index, row) in output.rows.iter().enumerate() {
                    assert_eq!(row.get("score"), Some(&Value::Int(index as i64)));
                }
            }
            assert_eq!(engine.database().commit_epoch().unwrap(), epoch);
        }
        let total = engine
            .query_admitted("MATCH (n:RowLimit) WHERE n.score >= 0 RETURN count(n) AS total")
            .unwrap();
        assert_eq!(
            total.rows[0].get("total"),
            Some(&Value::Int((cap + 1) as i64))
        );
    }
}

#[test]
fn sql_session_row_limits_preserve_aggregates_and_rollback_failed_returning() {
    use hawdb::Database;

    let mut db = Database::new();
    db.query_sql("CREATE TABLE session_rows (id BIGINT PRIMARY KEY)")
        .unwrap();
    for id in 0..3 {
        db.query_sql(&format!("INSERT INTO session_rows (id) VALUES ({id})"))
            .unwrap();
    }
    let epoch = db.commit_epoch().unwrap();
    {
        let mut session = db.sql_session();
        assert_eq!(session.config().max_result_rows, 0);
        session.set_max_result_rows(1);
        assert!(session.query("SELECT id FROM session_rows", &[]).is_err());
        assert_eq!(
            session
                .query("SELECT COUNT(*) AS total FROM session_rows", &[])
                .unwrap()
                .output
                .rows[0]
                .get("total"),
            Some(&Value::Int(3))
        );
        assert!(session
            .query("SELECT id FROM session_rows LIMIT 0", &[])
            .unwrap()
            .output
            .rows
            .is_empty());
        assert_eq!(
            session
                .query(
                    "SELECT id FROM session_rows WHERE id = $1",
                    &[Value::Int(2)]
                )
                .unwrap()
                .output
                .rows[0]
                .get("id"),
            Some(&Value::Int(2))
        );
        let failed = session.transaction(|transaction| {
            transaction.query("INSERT INTO session_rows (id) VALUES (3)", &[])?;
            assert!(transaction
                .query("SELECT id FROM session_rows", &[])
                .is_err());
            Ok(())
        });
        assert!(
            failed.is_err(),
            "caught result refusal still rolls back staged writes"
        );
        assert!(session
            .query(
                "INSERT INTO session_rows (id) VALUES (4), (5) RETURNING id",
                &[]
            )
            .is_err());
        assert_eq!(
            session
                .query("SELECT COUNT(*) AS total FROM session_rows", &[])
                .unwrap()
                .output
                .rows[0]
                .get("total"),
            Some(&Value::Int(3))
        );
        session.set_max_result_rows(0);
        assert_eq!(
            session
                .query("SELECT id FROM session_rows", &[])
                .unwrap()
                .output
                .rows
                .len(),
            3
        );
        session
            .transaction(|transaction| {
                assert_eq!(
                    transaction
                        .query("SELECT id FROM session_rows", &[])?
                        .output
                        .rows
                        .len(),
                    3
                );
                Ok(())
            })
            .unwrap();
    }
    {
        let mut session = db.session();
        session.set_max_result_rows(1);
        assert!(session
            .query_sql("INSERT INTO session_rows (id) VALUES (6), (7) RETURNING id")
            .is_err());
        assert_eq!(
            session
                .query_sql("SELECT COUNT(*) AS total FROM session_rows")
                .unwrap()
                .rows[0]
                .get("total"),
            Some(&Value::Int(3))
        );
    }
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(db.sql_session().config().max_result_rows, 0);
    assert_eq!(
        db.sql_session()
            .query("SELECT id FROM session_rows", &[])
            .unwrap()
            .output
            .rows
            .len(),
        3
    );
}

#[test]
fn unlimited_sql_results_cross_the_former_default_boundary_without_hidden_scan_caps() {
    use hawdb::{Database, DatabaseConfig};

    let directory = IdentityDirectory::new();
    for persistent in [false, true] {
        let mut db = if persistent {
            Database::open(directory.0.join("sql-unlimited")).unwrap()
        } else {
            Database::new_with_config(DatabaseConfig {
                max_read_result_rows: None,
                ..DatabaseConfig::default()
            })
        };
        db.query_sql("CREATE TABLE unlimited_rows (id BIGINT PRIMARY KEY)")
            .unwrap();
        // File output cardinality comes from a real cross join, independently
        // of finite storage overlay/checkpoint-object budgets for source rows.
        let source_rows = if persistent { 1_001 } else { 100_001 };
        for start in (0..source_rows).step_by(512) {
            let values = (start..(start + 512).min(source_rows))
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            db.query_sql(&format!("INSERT INTO unlimited_rows (id) VALUES {values}"))
                .unwrap();
        }
        if persistent {
            db.query_sql("CREATE TABLE buckets (bucket BIGINT PRIMARY KEY)")
                .unwrap();
            let values = (0..100)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            db.query_sql(&format!("INSERT INTO buckets (bucket) VALUES {values}"))
                .unwrap();
        }
        let epoch = db.commit_epoch().unwrap();
        let mut session = db.sql_session();
        let selected = if persistent {
            "SELECT id, bucket FROM unlimited_rows CROSS JOIN buckets ORDER BY id, bucket LIMIT 100001"
        } else {
            "SELECT id FROM unlimited_rows ORDER BY id"
        };
        let output = session.query(selected, &[]).unwrap().output;
        assert_eq!(output.rows.len(), 100_001);
        for (index, row) in output.rows.iter().enumerate() {
            let id = if persistent { index / 100 } else { index };
            assert_eq!(row.get("id"), Some(&Value::Int(id as i64)));
            if persistent {
                assert_eq!(row.get("bucket"), Some(&Value::Int((index % 100) as i64)));
            }
        }
        session.set_max_result_rows(1);
        let counted = if persistent {
            "SELECT COUNT(*) AS total FROM unlimited_rows CROSS JOIN buckets"
        } else {
            "SELECT COUNT(*) AS total FROM unlimited_rows"
        };
        assert_eq!(
            session.query(counted, &[]).unwrap().output.rows[0].get("total"),
            Some(&Value::Int(if persistent { 100_100 } else { 100_001 }))
        );
        assert!(session.query("SELECT id FROM unlimited_rows", &[]).is_err());
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn read_only_sql_sessions_inherit_limits_without_altering_database_configuration() {
    use hawdb::{Database, DatabaseConfig};

    let directory = IdentityDirectory::new();
    let path = directory.0.join("session-read-only");
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql("CREATE TABLE session_rows (id BIGINT PRIMARY KEY)")
            .unwrap();
        db.query_sql("INSERT INTO session_rows (id) VALUES (0), (1)")
            .unwrap();
    }
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    {
        let mut session = db.sql_session();
        session.set_max_result_rows(1);
        assert!(session.query("SELECT id FROM session_rows", &[]).is_err());
        assert_eq!(
            session
                .query("SELECT COUNT(*) AS total FROM session_rows", &[])
                .unwrap()
                .output
                .rows[0]
                .get("total"),
            Some(&Value::Int(2))
        );
        session.set_max_result_rows(0);
        assert_eq!(
            session
                .query("SELECT id FROM session_rows", &[])
                .unwrap()
                .output
                .rows
                .len(),
            2
        );
        session.set_max_result_rows(u64::MAX);
        assert_eq!(
            session
                .query("SELECT id FROM session_rows", &[])
                .unwrap()
                .output
                .rows
                .len(),
            2
        );
    }
    assert_eq!(db.config().max_read_result_rows, None);
    assert!(db.config().read_only);
}

fn host_ceiling_graph_fixture() -> hawdb::Database {
    let mut database = hawdb::Database::new_with_config(hawdb::DatabaseConfig {
        max_read_result_rows: Some(2),
        ..hawdb::DatabaseConfig::default()
    });
    database.query("CREATE NODE TABLE HostLimit").unwrap();
    database
        .query("CREATE PROPERTY ON NODE TABLE HostLimit(score) TYPE INT")
        .unwrap();
    for _ in 0..3 {
        database.query("CREATE (:HostLimit {score: 0})").unwrap();
    }
    database
}

fn host_ceiling_sql_fixture() -> hawdb::Database {
    let mut database = hawdb::Database::new_with_config(hawdb::DatabaseConfig {
        max_read_result_rows: Some(2),
        ..hawdb::DatabaseConfig::default()
    });
    database
        .query_sql("CREATE TABLE host_limit (id BIGINT PRIMARY KEY)")
        .unwrap();
    for id in 0..3 {
        database
            .query_sql(&format!("INSERT INTO host_limit (id) VALUES ({id})"))
            .unwrap();
    }
    database
}

fn assert_host_row_ceiling(error: hawdb::HawDBError) {
    assert!(
        matches!(&error, hawdb::HawDBError::ReadBudgetExceeded(cause)
            if cause.resource == hawdb::ReadBudgetResource::Rows && cause.limit == 2)
            || matches!(&error, hawdb::HawDBError::Execution(message)
                if message.contains("max_mutation_result_rows 2")),
        "unexpected host-ceiling refusal: {error}"
    );
}

#[test]
fn session_host_ceiling_bounds_cypher_mutations_in_both_transaction_modes() {
    for max_result_rows in [0, 4, u64::MAX] {
        for explicit in [false, true] {
            let mut database = host_ceiling_graph_fixture();
            let epoch = database.commit_epoch().unwrap();
            {
                let mut session = database.session();
                session.set_max_result_rows(max_result_rows);
                if explicit {
                    session.query("BEGIN TRANSACTION").unwrap();
                }
                assert_host_row_ceiling(
                    session
                        .query("MATCH (n:HostLimit) SET n.score = 1 RETURN n.score AS score")
                        .unwrap_err(),
                );
                assert_eq!(
                    session
                        .query("MATCH (n:HostLimit) WHERE n.score = 1 RETURN count(n) AS total")
                        .unwrap()
                        .rows[0]
                        .get("total"),
                    Some(&Value::Int(0))
                );
                if explicit {
                    session.query("ROLLBACK").unwrap();
                }
            }
            assert_eq!(database.commit_epoch().unwrap(), epoch);
        }
    }
}

#[test]
fn session_host_ceiling_bounds_commit_results_before_publication() {
    for max_result_rows in [0, 4, u64::MAX] {
        let mut database = host_ceiling_graph_fixture();
        let epoch = database.commit_epoch().unwrap();
        {
            let mut session = database.session();
            session.set_max_result_rows(max_result_rows);
            session.query("BEGIN TRANSACTION").unwrap();
            for _ in 0..3 {
                session.query("CREATE (:HostLimit {score: 9})").unwrap();
            }
            assert_host_row_ceiling(session.query("COMMIT").unwrap_err());
            assert_eq!(
                session
                    .query("MATCH (n:HostLimit) WHERE n.score = 9 RETURN count(n) AS total")
                    .unwrap()
                    .rows[0]
                    .get("total"),
                Some(&Value::Int(0))
            );
        }
        assert_eq!(database.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn session_host_ceiling_bounds_sql_returning_and_rolls_back_caught_errors() {
    const RETURNING: &str = "INSERT INTO host_limit (id) VALUES (3), (4), (5) RETURNING id";
    for max_result_rows in [0, 4, u64::MAX] {
        for native_sql in [false, true] {
            let mut database = host_ceiling_sql_fixture();
            let epoch = database.commit_epoch().unwrap();
            let error = if native_sql {
                let mut session = database.sql_session();
                session.set_max_result_rows(max_result_rows);
                session.query(RETURNING, &[]).unwrap_err()
            } else {
                let mut session = database.session();
                session.set_max_result_rows(max_result_rows);
                session.query_sql(RETURNING).unwrap_err()
            };
            assert_host_row_ceiling(error);
            assert_eq!(database.commit_epoch().unwrap(), epoch);
            for id in 3..6 {
                assert!(database
                    .query_sql_with_params(
                        "SELECT id FROM host_limit WHERE id = $1",
                        &[Value::Int(id)]
                    )
                    .unwrap()
                    .rows
                    .is_empty());
            }
        }
        let mut database = host_ceiling_sql_fixture();
        let epoch = database.commit_epoch().unwrap();
        {
            let mut session = database.sql_session();
            session.set_max_result_rows(max_result_rows);
            let failed = session.transaction(|transaction| {
                transaction.query("INSERT INTO host_limit (id) VALUES (6)", &[])?;
                assert_host_row_ceiling(transaction.query(RETURNING, &[]).unwrap_err());
                Ok(())
            });
            assert!(
                failed.is_err(),
                "caught RETURNING refusal must abort the transaction"
            );
        }
        assert_eq!(database.commit_epoch().unwrap(), epoch);
        for id in 3..7 {
            assert!(database
                .query_sql_with_params("SELECT id FROM host_limit WHERE id = $1", &[Value::Int(id)])
                .unwrap()
                .rows
                .is_empty());
        }
    }
}

#[test]
fn runtime_view_reports_effective_host_row_ceiling() {
    for configured in [None, Some(0), Some(2)] {
        let mut database = hawdb::Database::new_with_config(hawdb::DatabaseConfig {
            max_read_result_rows: configured,
            ..hawdb::DatabaseConfig::default()
        });
        let expected = configured
            .filter(|rows| *rows != 0)
            .map_or(Value::Null, |rows| Value::Int(rows as i64));
        let sql = "SELECT max_read_result_rows FROM system.runtime_status";
        assert_eq!(
            database.query_sql(sql).unwrap().rows[0]["max_read_result_rows"],
            expected
        );
        assert_eq!(
            database.session().query_sql(sql).unwrap().rows[0]["max_read_result_rows"],
            expected
        );
        assert_eq!(
            database
                .begin_read_transaction()
                .unwrap()
                .query_sql(sql)
                .unwrap()
                .rows[0]["max_read_result_rows"],
            expected
        );
        assert_eq!(
            database.sql_session().query(sql, &[]).unwrap().output.rows[0]["max_read_result_rows"],
            expected
        );
    }
}

#[test]
fn autocommit_sql_preserves_session_variables_and_validates_before_commit() {
    for ceiling in [0, 4] {
        let mut database = hawdb::Database::new();
        database
            .query_sql("CREATE TABLE session_dml (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        // These RowPage statements have empty results by AST shape, even
        // though they mutate data. Strict zero budgets must allow the native
        // commit while result-bearing statements below still fail atomically.
        for sql in [
            "INSERT INTO session_dml (id, body) VALUES (99, 'empty result')",
            "UPDATE session_dml SET body = 'changed' WHERE id = 99",
            "DELETE FROM session_dml WHERE id = 99",
        ] {
            let epoch = database.commit_epoch().unwrap();
            let output = database
                .query_sql_with_params_options(
                    sql,
                    &[],
                    hawdb::QueryStreamOptions {
                        max_rows: Some(0),
                        max_payload_bytes: Some(0),
                    },
                )
                .unwrap();
            assert!(output.rows.is_empty());
            assert!(database.commit_epoch().unwrap() > epoch);
        }
        {
            let mut session = database.session();
            session
                .query("SET system.work_priority = 'background'")
                .unwrap();
            session
                .query("SET system.work_class = 'analytics'")
                .unwrap();
            session
                .query("SET system.estimated_operations = 64")
                .unwrap();
            session.set_max_result_rows(ceiling);
            let variables = session.system_variables().clone();
            let work = session.query_work_request();
            let inserted = session
                .query_sql_with_params(
                    "INSERT INTO session_dml (id, body) VALUES (1, $1), (2, $1) RETURNING id, body",
                    &[Value::String("original".into())],
                )
                .unwrap();
            assert_eq!(inserted.rows.len(), 2);
            session
                .query_sql("UPDATE session_dml SET body = 'updated' WHERE id = 1")
                .unwrap();
            session
                .query_sql("DELETE FROM session_dml WHERE id = 2")
                .unwrap();
            assert_eq!(
                session
                    .query_sql("SELECT body FROM session_dml WHERE id = 1")
                    .unwrap()
                    .rows[0]["body"],
                Value::String("updated".into())
            );
            assert_eq!(session.system_variables(), &variables);
            assert_eq!(session.query_work_request(), work);
            session.set_max_result_rows(1);
            let failure = session
                .query_sql(
                    "INSERT INTO session_dml (id, body) VALUES (3, 'a'), (4, 'b') RETURNING id",
                )
                .unwrap_err();
            assert!(
                matches!(failure, hawdb::HawDBError::ReadBudgetExceeded(cause) if cause.resource == hawdb::ReadBudgetResource::Rows && cause.limit == 1)
            );
            assert_eq!(session.system_variables(), &variables);
        }
        let epoch = database.commit_epoch().unwrap();
        let failure = database
            .query_sql_bounded(
                "INSERT INTO session_dml (id, body) VALUES (5, 'c') RETURNING id",
                Some(0),
            )
            .unwrap_err();
        assert!(
            matches!(failure, hawdb::HawDBError::ReadBudgetExceeded(cause) if cause.resource == hawdb::ReadBudgetResource::Rows && cause.limit == 0)
        );
        let failure = database
            .query_sql_with_params_options(
                "INSERT INTO session_dml (id, body) VALUES (6, $1) RETURNING body",
                &[Value::String("x".repeat(4096))],
                hawdb::QueryStreamOptions {
                    max_rows: None,
                    max_payload_bytes: Some(128),
                },
            )
            .unwrap_err();
        assert!(
            matches!(failure, hawdb::HawDBError::ReadBudgetExceeded(cause) if cause.resource == hawdb::ReadBudgetResource::PayloadBytes && cause.limit == 128)
        );
        assert_eq!(database.commit_epoch().unwrap(), epoch);
        assert_eq!(
            database
                .query_sql("SELECT COUNT(*) AS total FROM session_dml")
                .unwrap()
                .rows[0]["total"],
            Value::Int(1)
        );
    }
}

#[test]
fn empty_sql_results_preserve_custom_mutation_limits() {
    let mut config = hawdb::DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(1).unwrap();
    let mut database = hawdb::Database::new_with_config(config);
    database
        .query_sql("CREATE TABLE bounded_writes (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    for id in [1, 2] {
        database
            .query_sql_with_params(
                "INSERT INTO bounded_writes (id, body) VALUES ($1, 'original')",
                &[Value::Int(id)],
            )
            .unwrap();
    }
    for sql in [
        "INSERT INTO bounded_writes (id, body) VALUES (3, 'new'), (4, 'new')",
        "UPDATE bounded_writes SET body = 'changed' WHERE body = 'original'",
        "DELETE FROM bounded_writes WHERE body = 'original'",
    ] {
        let epoch = database.commit_epoch().unwrap();
        let error = database.query_sql(sql).unwrap_err();
        assert!(error.to_string().contains("max_affected_rows 1"), "{error}");
        assert_eq!(database.commit_epoch().unwrap(), epoch, "{sql}");
        let output = database
            .query_sql("SELECT id, body FROM bounded_writes ORDER BY id")
            .unwrap();
        assert_eq!(output.rows.len(), 2, "{sql}");
        for (row, id) in output.rows.iter().zip([1, 2]) {
            assert_eq!(row["id"], Value::Int(id));
            assert_eq!(row["body"], Value::String("original".into()));
        }
    }
}

fn native_sql_returning_payload_control(route: &str, mutation: &str) {
    for (ceiling, admitted) in [0, 1, 4096]
        .into_iter()
        .flat_map(|ceiling| [false, true].map(|admitted| (ceiling, admitted)))
    {
        let allowed = ceiling == 4096 && mutation == "insert";
        let directory = IdentityDirectory::new();
        let path = directory.0.join("returning-payload");
        let mut database = hawdb::Database::open(&path).unwrap();
        database
            .query_sql("CREATE TABLE payload_writes (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        database
            .query_sql("INSERT INTO payload_writes (id, body) VALUES (1, 'original'), (2, 'other')")
            .unwrap();
        let epoch = database.commit_epoch().unwrap();
        drop(database);
        let mut database = hawdb::Database::open_with_config(
            &path,
            hawdb::DatabaseConfig {
                max_read_result_payload_bytes: Some(if admitted { 8192 } else { ceiling }),
                ..Default::default()
            },
        )
        .unwrap();
        let context = if admitted {
            hawdb::RuntimeTaskContext::default().with_memory_reservation(
                hawdb::RuntimeMemoryReservation::new(
                    256 * 1024 * 1024,
                    u64::try_from(ceiling).unwrap(),
                ),
            )
        } else {
            hawdb::RuntimeTaskContext::default()
        };
        let sql = match mutation {
            "insert" => {
                "INSERT INTO payload_writes (id, body) VALUES (3, 'new') RETURNING id, body"
            }
            "update" => {
                "UPDATE payload_writes SET body = 'changed' WHERE id = 1 RETURNING id, body"
            }
            "delete" => "DELETE FROM payload_writes WHERE id = 1 RETURNING id, body",
            _ => unreachable!(),
        };
        let result = match route {
            "session" => database
                .sql_session()
                .with_task_context(&context)
                .query(sql, &[])
                .map(|_| ()),
            "direct" => {
                let mut transaction = database.begin_transaction_with_context(&context).unwrap();
                let result = transaction.query_sql(sql).map(|_| ());
                // A rejected statement must not remain staged and publish on
                // a later explicit commit of the otherwise empty transaction.
                transaction.commit().unwrap();
                result
            }
            "callback" => database
                .sql_session()
                .with_task_context(&context)
                .transaction(|transaction| {
                    if !allowed {
                        transaction.query(
                            "INSERT INTO payload_writes (id, body) VALUES (9, 'prior')",
                            &[],
                        )?;
                    }
                    let result = transaction.query(sql, &[]);
                    if allowed {
                        result?;
                    }
                    // A caught refusal must still roll back the preceding write.
                    Ok(())
                }),
            _ => unreachable!(),
        };
        let observed_epoch = database.commit_epoch().unwrap();
        drop(database);
        // Reopen through an ordinary, unrestricted reader so the result cap
        // under test cannot mask incomplete stored-row verification.
        let mut reopened = hawdb::Database::open(&path).unwrap();
        let rows = reopened
            .query_sql("SELECT id, body FROM payload_writes ORDER BY id")
            .unwrap();
        let stored = rows
            .rows
            .iter()
            .map(|row| (row["id"].clone(), row["body"].clone()))
            .collect::<Vec<_>>();
        let mut expected = vec![
            (Value::Int(1), Value::String("original".into())),
            (Value::Int(2), Value::String("other".into())),
        ];
        if allowed {
            result.unwrap();
            match mutation {
                "insert" => expected.push((Value::Int(3), Value::String("new".into()))),
                "update" => expected[0].1 = Value::String("changed".into()),
                "delete" => {
                    expected.remove(0);
                }
                _ => unreachable!(),
            }
            assert!(observed_epoch > epoch, "{route}/{mutation}/{ceiling}");
        } else {
            if mutation == "insert" {
                assert!(
                matches!(result, Err(hawdb::HawDBError::ReadBudgetExceeded(ref cause))
                    if cause.resource == hawdb::ReadBudgetResource::PayloadBytes && cause.limit == ceiling),
                "{route}/{mutation}/{ceiling}: result={result:?}, epochs={epoch}->{observed_epoch}, stored={stored:?}"
            );
            } else {
                // UPDATE/DELETE RETURNING are outside the current SQL
                // fragment. Preserve their fail-closed semantic refusal;
                // do not add new SQL capability to repair a result budget.
                assert!(
                    matches!(result, Err(hawdb::HawDBError::Semantic(ref message))
                    if message.contains("unsupported PostgreSQL")),
                    "{route}/{mutation}/{ceiling}: {result:?}"
                );
            }
            assert_eq!(observed_epoch, epoch, "{route}/{mutation}/{ceiling}");
        }
        assert_eq!(stored, expected, "{route}/{mutation}/{ceiling}");
        assert_eq!(reopened.commit_epoch().unwrap(), observed_epoch);
    }
}

macro_rules! native_sql_returning_payload_regression {
    ($name:ident, $route:literal, $mutation:literal) => {
        #[test]
        fn $name() {
            native_sql_returning_payload_control($route, $mutation);
        }
    };
}

native_sql_returning_payload_regression!(
    native_sql_returning_payload_insert_session,
    "session",
    "insert"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_update_session,
    "session",
    "update"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_delete_session,
    "session",
    "delete"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_insert_direct,
    "direct",
    "insert"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_update_direct,
    "direct",
    "update"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_delete_direct,
    "direct",
    "delete"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_insert_callback,
    "callback",
    "insert"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_update_callback,
    "callback",
    "update"
);
native_sql_returning_payload_regression!(
    native_sql_returning_payload_delete_callback,
    "callback",
    "delete"
);

#[test]
fn unlimited_rows_still_refuse_wide_json_payload_before_delivery() {
    let payload_limit = 4096;
    let mut database = hawdb::Database::new_with_config(hawdb::DatabaseConfig {
        max_read_result_payload_bytes: Some(payload_limit),
        execution_memory: ExecutionMemoryConfig {
            query_memory_bytes: std::num::NonZeroUsize::new(256 * 1024).unwrap(),
            batch_rows: std::num::NonZeroUsize::new(1).unwrap(),
            batch_payload_bytes: std::num::NonZeroUsize::new(64 * 1024).unwrap(),
            blocking_operator_bytes: std::num::NonZeroUsize::new(64 * 1024).unwrap(),
            ..ExecutionMemoryConfig::default()
        },
        ..hawdb::DatabaseConfig::default()
    });
    assert_eq!(database.config().read_result_row_limit(), None);
    database.query("CREATE NODE TABLE WideLimit").unwrap();
    database
        .query_with_params(
            "UNWIND $ids AS id CREATE (:WideLimit)",
            &BTreeMap::from([(
                "ids".into(),
                Value::List((0..1000).map(Value::Int).collect()),
            )]),
        )
        .unwrap();
    let mut snapshot = database.begin_read_transaction().unwrap();
    let epoch = snapshot.commit_epoch();
    // Full output would exceed the 256 KiB tracked query budget by orders
    // of magnitude. A typed payload refusal must win before accumulating it;
    // this tests the engine boundary, not a whole-process RSS bound.
    let fields = (0..20)
        .map(|index| format!("$wide AS field{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("MATCH (n:WideLimit) RETURN {fields}");
    let parameters = BTreeMap::from([("wide".into(), Value::String("\"\\\n界".repeat(128)))]);
    let mut delivered = 0;
    let failure = snapshot
        .query_with_params_streaming(
            &query,
            &parameters,
            hawdb::QueryStreamOptions::default(),
            |_| {
                delivered += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(
        matches!(failure, hawdb::HawDBError::ReadBudgetExceeded(cause) if cause.resource == hawdb::ReadBudgetResource::PayloadBytes && cause.limit == payload_limit)
    );
    assert_eq!(delivered, 0);
    assert_eq!(database.commit_epoch().unwrap(), epoch);
    assert_eq!(
        snapshot
            .query("MATCH (n:WideLimit) RETURN count(n) AS total")
            .unwrap()
            .rows[0]["total"],
        Value::Int(1000)
    );
}
