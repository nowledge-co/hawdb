use super::*;
use crate::external::tests::empty_report;
use crate::VectorSeedExecutionRow;
use skein_core::RuntimeCancellationToken;
use skein_plan::{VectorCandidateSource, VectorExecutionResourceProfile, VectorPhysicalPlan};

fn nz(bytes: usize) -> NonZeroUsize {
    NonZeroUsize::new(bytes).unwrap()
}

fn plan(dimension: usize, limit: usize) -> VectorPhysicalPlan {
    VectorPhysicalPlan::TopK {
        limit,
        input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
            source: VectorCandidateSource::Scalar,
            embedding_dimension: dimension,
            candidate_limit: limit,
            input: Box::new(VectorPhysicalPlan::Filter { fields: vec![] }),
        }),
    }
}

fn parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Int(1), Value::Float(-0.0)]),
    )])
}

fn output(count: usize) -> VectorSeedExecutionOutput {
    let mut report = empty_report();
    report.generated_candidate_count = count + 2;
    report.reranked_candidate_count = count + 1;
    report.returned_count = count;
    VectorSeedExecutionOutput {
        rows: (0..count)
            .map(|index| VectorSeedExecutionRow {
                id: format!("id-{index}"),
                external_id: (index % 2 == 0).then(|| format!("external-{index}")),
                score: index as f64 * 0.25,
            })
            .collect(),
        report,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Request {
    embedding: Vec<u32>,
    filters: BTreeMap<String, String>,
    plan: VectorPhysicalPlan,
    priority: u8,
    parallelism: usize,
    working_bytes: usize,
    rows: usize,
    result_bytes: usize,
    has_context: bool,
    reserved_bytes: usize,
}

struct Host<'a> {
    calls: Vec<Request>,
    response: Option<Result<VectorSeedExecutionOutput>>,
    ledger: &'a QueryMemoryLedger,
    cancel: Option<RuntimeCancellationToken>,
}

impl ExternalReadOperator for Host<'_> {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        self.calls.push(Request {
            embedding: request
                .embedding
                .iter()
                .map(|value| value.to_bits())
                .collect(),
            filters: request.metadata_filters.clone(),
            plan: request.vector_plan.clone(),
            priority: request.resources.priority,
            parallelism: request.resources.max_parallelism.get(),
            working_bytes: request.resources.max_working_memory_bytes.get(),
            rows: request.resources.result.max_rows,
            result_bytes: request.resources.result.max_memory_bytes.get(),
            has_context: request.resources.task_context.is_some(),
            reserved_bytes: self.ledger.snapshot().used_bytes,
        });
        if let Some(token) = &self.cancel {
            token.cancel();
        }
        self.response.take().expect("one host call")
    }
}

struct Case {
    plan: VectorPhysicalPlan,
    parameters: BTreeMap<String, Value>,
    profile: VectorExecutionResourceProfile,
    memory: ExecutionMemoryConfig,
    admitted_parallelism: Option<usize>,
    output_limit: Option<usize>,
    external_ids: bool,
    response: Result<VectorSeedExecutionOutput>,
    cancel_before: bool,
    cancel_after: bool,
    consumer_stop: bool,
    consumer_error: bool,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            plan: plan(2, 3),
            parameters: parameters(),
            profile: VectorExecutionResourceProfile {
                priority: 173,
                max_parallelism: 4,
                max_working_memory_bytes: Some(1024),
            },
            memory: ExecutionMemoryConfig {
                query_memory_bytes: nz(64 * 1024),
                blocking_operator_bytes: nz(4096),
                batch_rows: nz(2),
                ..ExecutionMemoryConfig::default()
            },
            admitted_parallelism: Some(2),
            output_limit: None,
            external_ids: true,
            response: Ok(output(3)),
            cancel_before: false,
            cancel_after: false,
            consumer_stop: false,
            consumer_error: false,
        }
    }
}

#[derive(Debug)]
struct Outcome {
    result: Result<BatchControl>,
    calls: Vec<Request>,
    batches: Vec<Vec<BTreeMap<String, Value>>>,
    emit_reservations: Vec<usize>,
    reports: Vec<crate::VectorExecutionReport>,
    ledger: crate::QueryMemoryLedgerSnapshot,
}

fn run(case: Case) -> Outcome {
    let ledger = QueryMemoryLedger::new(case.memory.query_memory_bytes);
    let observer = QueryExecutionObserver::default();
    let token = RuntimeCancellationToken::new();
    let task = case.admitted_parallelism.map(|parallelism| {
        RuntimeTaskContext::without_deadline(token.clone())
            .with_admitted_parallelism(nz(parallelism))
    });
    if case.cancel_before {
        token.cancel();
    }
    let mut host = Host {
        calls: vec![],
        response: Some(case.response),
        ledger: &ledger,
        cancel: case.cancel_after.then_some(token),
    };
    let filters = BTreeMap::from([("space_id".to_string(), "space-1".to_string())]);
    let mut batches = vec![];
    let mut emit_reservations = vec![];
    let result = VectorSeedScanSpec {
        embedding_parameter: "embedding",
        output_external_id: &case.external_ids,
        metadata_filters: &filters,
        resource_profile: &case.profile,
        vector_plan: &case.plan,
    }
    .stream(
        VectorSeedContext {
            parameters: &case.parameters,
            external: &BatchExternalReadAdapter::new(&mut host),
            memory: &case.memory,
            memory_ledger: &ledger,
            task_context: task.as_ref(),
            observer: &observer,
        },
        ExecutionLimit {
            output_rows: case.output_limit,
        },
        &mut |batch| {
            emit_reservations.push(ledger.snapshot().used_bytes);
            batches.push(
                batch
                    .into_iter()
                    .map(|binding| {
                        assert!(binding.nodes.is_empty());
                        assert!(binding.relationships.is_empty());
                        binding.values
                    })
                    .collect(),
            );
            if case.consumer_error {
                Err(SkeinError::Execution("consumer failure".to_string()))
            } else if case.consumer_stop {
                Ok(BatchControl::Stop)
            } else {
                Ok(BatchControl::Continue)
            }
        },
    );
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0, "leases must drop on every exit");
    assert!(snapshot.classes.iter().all(|class| class.used_bytes == 0));
    Outcome {
        result,
        calls: host.calls,
        batches,
        emit_reservations,
        reports: observer.into_reports().vector_execution,
        ledger: snapshot,
    }
}

fn error(outcome: &Outcome, text: &str, host_calls: usize, reports: usize) {
    assert!(
        outcome
            .result
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains(text),
        "{outcome:?}"
    );
    assert_eq!(outcome.calls.len(), host_calls, "{outcome:?}");
    assert_eq!(outcome.reports.len(), reports, "{outcome:?}");
    assert!(outcome.batches.is_empty(), "{outcome:?}");
}

#[test]
fn embedding_conversion_preserves_bits_errors_and_validation_order() {
    let cases = [
        (
            Value::List(vec![Value::Int(16_777_217), Value::Float(-0.0)]),
            2,
            Ok(vec![0x4b80_0000, 0x8000_0000]),
        ),
        (
            Value::List(vec![Value::Int(i64::MIN), Value::Int(i64::MAX)]),
            2,
            Ok(vec![0xdf00_0000, 0x5f00_0000]),
        ),
        (
            Value::List(vec![Value::Float(f32::MAX as f64)]),
            1,
            Ok(vec![0x7f7f_ffff]),
        ),
        (Value::List(vec![]), 0, Ok(vec![])),
        (Value::Null, 0, Err("must be a numeric list")),
        (
            Value::List(vec![Value::Null]),
            9,
            Err("must contain finite numbers"),
        ),
        (
            Value::List(vec![Value::String("1".into())]),
            1,
            Err("must contain finite numbers"),
        ),
        (
            Value::List(vec![Value::Float(f64::NAN)]),
            1,
            Err("must contain finite numbers"),
        ),
        (
            Value::List(vec![Value::Float(f64::INFINITY)]),
            1,
            Err("must contain finite numbers"),
        ),
        (
            Value::List(vec![Value::Float(f64::NEG_INFINITY)]),
            1,
            Err("must contain finite numbers"),
        ),
        (
            Value::List(vec![Value::Float(f64::MAX)]),
            9,
            Err("exceeds f32 range"),
        ),
        (
            Value::List(vec![Value::Float(-f64::MAX)]),
            1,
            Err("exceeds f32 range"),
        ),
        (
            Value::List(vec![Value::Int(1)]),
            2,
            Err("dimension changed after planning"),
        ),
    ];
    for (value, dimension, expected) in cases {
        let actual = vector_embedding_parameter(
            &BTreeMap::from([("embedding".to_string(), value)]),
            "embedding",
            &plan(dimension, 1),
        );
        match expected {
            Ok(bits) => assert_eq!(
                actual
                    .unwrap()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                bits
            ),
            Err(message) => {
                let error = actual.unwrap_err();
                assert!(matches!(error, SkeinError::Semantic(_)));
                assert_eq!(
                    error.to_string(),
                    SkeinError::Semantic(format!("vector search parameter '$embedding' {message}"))
                        .to_string()
                );
            }
        }
    }
    assert!(
        vector_embedding_parameter(&BTreeMap::new(), "absent", &plan(2, 1))
            .unwrap_err()
            .to_string()
            .contains("'$absent' must be a numeric list")
    );
}

#[test]
fn vector_seed_preserves_request_binding_and_reservation_lifetime() {
    let result = run(Case::default());
    assert_eq!(result.result.as_ref().unwrap(), &BatchControl::Continue);
    assert_eq!(
        result.calls,
        vec![Request {
            embedding: vec![1f32.to_bits(), (-0f32).to_bits()],
            filters: BTreeMap::from([("space_id".to_string(), "space-1".to_string())]),
            plan: plan(2, 3),
            priority: 173,
            parallelism: 2,
            working_bytes: 1024,
            rows: 3,
            result_bytes: 4096,
            has_context: true,
            reserved_bytes: 5120,
        }]
    );
    assert_eq!(
        result.batches.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(
        result.batches[0][0]["external_id"],
        Value::String("external-0".into())
    );
    assert!(!result.batches[0][1].contains_key("external_id"));
    assert_eq!(result.batches[1][0]["id"], Value::String("id-2".into()));
    assert_eq!(result.batches[1][0]["score"], Value::Float(0.5));
    assert_eq!(result.emit_reservations, vec![5120, 5120]);
    assert_eq!(result.reports, vec![output(3).report]);
    assert!(result.ledger.peak_bytes > 5120);
}

#[test]
fn preflight_zero_limit_and_cancellation_keep_error_precedence() {
    error(
        &run(Case {
            plan: VectorPhysicalPlan::Filter { fields: vec![] },
            parameters: BTreeMap::new(),
            output_limit: Some(0),
            ..Case::default()
        }),
        "missing TopK",
        0,
        0,
    );
    let empty = run(Case {
        plan: plan(2, 0),
        parameters: BTreeMap::new(),
        cancel_before: true,
        ..Case::default()
    });
    assert_eq!(empty.result.unwrap(), BatchControl::Continue);
    assert!(empty.calls.is_empty() && empty.batches.is_empty() && empty.reports.is_empty());
    assert_eq!(empty.ledger.peak_bytes, 0);
    error(
        &run(Case {
            parameters: BTreeMap::new(),
            cancel_before: true,
            ..Case::default()
        }),
        "must be a numeric list",
        0,
        0,
    );
    error(
        &run(Case {
            cancel_before: true,
            ..Case::default()
        }),
        "external read task stopped: cancelled",
        0,
        0,
    );
    error(
        &run(Case {
            cancel_after: true,
            response: Ok(output(4)),
            ..Case::default()
        }),
        "external read task stopped: cancelled",
        1,
        0,
    );
    error(
        &run(Case {
            cancel_after: true,
            response: Err(SkeinError::Execution("host failure".into())),
            ..Case::default()
        }),
        "host failure",
        1,
        0,
    );
}

#[test]
fn result_and_binding_limits_fail_closed_without_partial_emission() {
    error(
        &run(Case {
            response: Ok(output(4)),
            ..Case::default()
        }),
        "exceeding result row budget 3",
        1,
        0,
    );
    let mut oversized = output(1);
    oversized.rows[0].id = "x".repeat(8192);
    error(
        &run(Case {
            response: Ok(oversized),
            ..Case::default()
        }),
        "exceeding result memory budget 4096",
        1,
        0,
    );
    // The wire result fits while its binding representation does not.
    let mut binding = Case {
        response: Ok(output(1)),
        ..Case::default()
    };
    binding.memory.blocking_operator_bytes = nz(128);
    error(&run(binding), "exceeding blocking_operator_bytes 128", 1, 1);
    let mut root = Case::default();
    root.memory.query_memory_bytes = nz(5119);
    root.cancel_before = true;
    error(&run(root), "exceeding query_memory_bytes 5119", 0, 0);
    let mut overlapping = Case::default();
    overlapping.memory.query_memory_bytes = nz(5120);
    error(&run(overlapping), "exceeding query_memory_bytes 5120", 1, 1);
}

#[test]
fn consumer_stop_and_error_release_external_reservations() {
    let stopped = run(Case {
        consumer_stop: true,
        ..Case::default()
    });
    assert_eq!(stopped.result.unwrap(), BatchControl::Stop);
    assert_eq!(stopped.batches.len(), 1);
    assert_eq!(stopped.emit_reservations, vec![5120]);
    assert_eq!(stopped.reports.len(), 1);
    let failed = run(Case {
        consumer_error: true,
        ..Case::default()
    });
    assert!(failed
        .result
        .unwrap_err()
        .to_string()
        .contains("consumer failure"));
    assert_eq!(failed.batches.len(), 1);
    assert_eq!(failed.emit_reservations, vec![5120]);
    assert_eq!(failed.reports.len(), 1);
}

mod differential;
