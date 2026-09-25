use hawdb::executor::{
    execute_with_request, execute_with_request_consumer, ExecutionMemoryConfig, ExecutionRequest,
    ExecutionResources, NoExternalReadOperator,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::schema::Catalog;
use hawdb::store::GraphStore;
use hawdb::value::Value;
use std::collections::BTreeMap;

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
