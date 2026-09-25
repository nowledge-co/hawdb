use super::*;

#[derive(Clone, Copy, Debug)]
enum Endpoint {
    RowLimit,
    RowLimitAndContext,
    RowLimitProfile,
    RowLimitProfileAndExternal,
    OutputLimitsProfileAndExternal,
    OutputLimitsProfileAndExternalAndMemory,
    RowLimitProfileAndExternalAndMemory,
    RowLimitProfileAndExternalAndContext,
    OutputLimitsProfileAndExternalAndContext,
    OutputLimitsProfileAndExternalAndContextAndMemory,
    RowConsumerProfile,
    RowConsumerProfileAndExternal,
    RowConsumerProfileAndExternalAndMemory,
    RowConsumerProfileAndExternalAndContext,
    RowConsumerProfileAndExternalAndContextAndMemory,
}

const ENDPOINTS: [Endpoint; 15] = [
    Endpoint::RowLimit,
    Endpoint::RowLimitAndContext,
    Endpoint::RowLimitProfile,
    Endpoint::RowLimitProfileAndExternal,
    Endpoint::OutputLimitsProfileAndExternal,
    Endpoint::OutputLimitsProfileAndExternalAndMemory,
    Endpoint::RowLimitProfileAndExternalAndMemory,
    Endpoint::RowLimitProfileAndExternalAndContext,
    Endpoint::OutputLimitsProfileAndExternalAndContext,
    Endpoint::OutputLimitsProfileAndExternalAndContextAndMemory,
    Endpoint::RowConsumerProfile,
    Endpoint::RowConsumerProfileAndExternal,
    Endpoint::RowConsumerProfileAndExternalAndMemory,
    Endpoint::RowConsumerProfileAndExternalAndContext,
    Endpoint::RowConsumerProfileAndExternalAndContextAndMemory,
];

impl Endpoint {
    fn consumer(self) -> bool {
        matches!(
            self,
            Self::RowConsumerProfile
                | Self::RowConsumerProfileAndExternal
                | Self::RowConsumerProfileAndExternalAndMemory
                | Self::RowConsumerProfileAndExternalAndContext
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
    fn parameters(self) -> bool {
        matches!(
            self,
            Self::RowLimitProfileAndExternal
                | Self::OutputLimitsProfileAndExternal
                | Self::OutputLimitsProfileAndExternalAndMemory
                | Self::RowLimitProfileAndExternalAndMemory
                | Self::RowLimitProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContextAndMemory
                | Self::RowConsumerProfile
                | Self::RowConsumerProfileAndExternal
                | Self::RowConsumerProfileAndExternalAndMemory
                | Self::RowConsumerProfileAndExternalAndContext
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
    fn payload(self) -> bool {
        matches!(
            self,
            Self::OutputLimitsProfileAndExternal
                | Self::OutputLimitsProfileAndExternalAndMemory
                | Self::OutputLimitsProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContextAndMemory
                | Self::RowConsumerProfile
                | Self::RowConsumerProfileAndExternal
                | Self::RowConsumerProfileAndExternalAndMemory
                | Self::RowConsumerProfileAndExternalAndContext
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
    fn context(self) -> bool {
        matches!(
            self,
            Self::RowLimitAndContext
                | Self::RowLimitProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContextAndMemory
                | Self::RowConsumerProfileAndExternalAndContext
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
    fn memory(self) -> bool {
        matches!(
            self,
            Self::OutputLimitsProfileAndExternalAndMemory
                | Self::RowLimitProfileAndExternalAndMemory
                | Self::OutputLimitsProfileAndExternalAndContextAndMemory
                | Self::RowConsumerProfileAndExternalAndMemory
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
    fn profile(self) -> bool {
        matches!(
            self,
            Self::RowLimitProfile
                | Self::RowLimitProfileAndExternal
                | Self::OutputLimitsProfileAndExternal
                | Self::OutputLimitsProfileAndExternalAndMemory
                | Self::RowLimitProfileAndExternalAndMemory
                | Self::RowLimitProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContext
                | Self::OutputLimitsProfileAndExternalAndContextAndMemory
                | Self::RowConsumerProfile
                | Self::RowConsumerProfileAndExternal
                | Self::RowConsumerProfileAndExternalAndMemory
                | Self::RowConsumerProfileAndExternalAndContext
                | Self::RowConsumerProfileAndExternalAndContextAndMemory
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct Case {
    rows: Option<usize>,
    payload: Option<usize>,
    cancelled: bool,
    small_memory: bool,
    consumer_error: bool,
    external: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    rows: Vec<Row>,
    profile: Option<ReadExecutionProfile>,
    error: Option<String>,
    external: Option<ExternalEvidence>,
}

#[derive(Debug, PartialEq, Eq)]
struct ExternalEvidence {
    embedding: Vec<u32>,
    filters: BTreeMap<String, String>,
    context: bool,
    parallelism: usize,
    working_bytes: usize,
    rows: usize,
    result_bytes: usize,
}

#[derive(Default)]
struct ExternalProbe(Option<ExternalEvidence>);

impl ExternalReadOperator for ExternalProbe {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        self.0 = Some(ExternalEvidence {
            embedding: request
                .embedding
                .iter()
                .map(|value| value.to_bits())
                .collect(),
            filters: request.metadata_filters.clone(),
            context: request.resources.task_context.is_some(),
            parallelism: request.resources.max_parallelism.get(),
            working_bytes: request.resources.max_working_memory_bytes.get(),
            rows: request.resources.result.max_rows,
            result_bytes: request.resources.result.max_memory_bytes.get(),
        });
        Err(HawDBError::Execution("host external read rejected".into()))
    }
}

fn normalized(mut profile: ReadExecutionProfile) -> ReadExecutionProfile {
    // OS-wide samples vary between calls; all deterministic engine accounting
    // and operator evidence must remain identical.
    let report = &mut profile.pipeline_memory_report;
    report.start_resident_bytes = None;
    report.start_peak_resident_bytes = None;
    report.steady_resident_bytes = None;
    report.peak_resident_bytes = None;
    report.steady_resident_growth_bytes = None;
    report.lifetime_peak_resident_growth_bytes = None;
    report.total_page_faults = None;
    report.minor_page_faults = None;
    report.major_page_faults = None;
    profile
}

fn run(endpoint: Endpoint, case: Case, legacy: bool) -> Outcome {
    let parameters = if case.external {
        BTreeMap::from([(
            "embedding".into(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )])
    } else if endpoint.parameters() {
        BTreeMap::from([("payload".into(), Value::String("parameter value".into()))])
    } else {
        BTreeMap::new()
    };
    let query = if case.external {
        "MATCH (n:Item) RETURN n.rank AS rank"
    } else if endpoint.parameters() {
        "MATCH (n:Item) RETURN n.rank AS rank, $payload AS payload ORDER BY rank"
    } else {
        "MATCH (n:Item) RETURN n.rank AS rank ORDER BY rank"
    };
    let logical = hawdb_plan_cypher::plan_pipeline_query(query, &parameters).unwrap();
    let plan = if case.external {
        use hawdb_plan_cypher::{
            VectorCandidateSource, VectorExecutionResourceProfile, VectorPhysicalPlan,
        };
        PhysicalPlan::VectorSeedScan {
            embedding_parameter: "embedding".into(),
            output_external_id: false,
            metadata_filters: BTreeMap::from([("space".into(), "selected".into())]),
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
        }
    } else {
        hawdb_optimizer::CascadesOptimizer::default().optimize(&logical)
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in [2, 0, 1] {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([("rank".into(), Value::Int(rank))]),
            )
            .unwrap();
    }
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: if case.small_memory {
            NonZeroUsize::new(64).unwrap()
        } else {
            ExecutionMemoryConfig::default().query_memory_bytes
        },
        ..ExecutionMemoryConfig::default()
    };
    let token = hawdb_core::RuntimeCancellationToken::new();
    if case.cancelled {
        token.cancel();
    }
    let context = RuntimeTaskContext::without_deadline(token);
    let mut external = ExternalProbe::default();
    let mut delivered = Vec::new();
    let mut consumer = |row| {
        delivered.push(row);
        if case.consumer_error {
            Err(HawDBError::Execution("consumer failure".into()))
        } else {
            Ok(())
        }
    };
    let result: Result<(Vec<Row>, Option<ReadExecutionProfile>)> = if legacy {
        match endpoint {
            Endpoint::RowLimit => {
                execute_with_row_limit(&plan, &mut catalog, &mut store, case.rows)
                    .map(|output| (output, None))
            }
            Endpoint::RowLimitAndContext => execute_with_row_limit_and_context(
                &plan,
                &mut catalog,
                &mut store,
                case.rows,
                &context,
            )
            .map(|output| (output, None)),
            Endpoint::RowLimitProfile => {
                execute_with_row_limit_profile(&plan, &mut catalog, &mut store, case.rows)
                    .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::RowLimitProfileAndExternal => execute_with_row_limit_profile_and_external(
                &plan,
                &mut catalog,
                &mut store,
                &parameters,
                &mut external,
                case.rows,
            )
            .map(|output| (output.rows.into_rows(), Some(output.profile))),
            Endpoint::OutputLimitsProfileAndExternal => {
                execute_with_output_limits_profile_and_external(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::OutputLimitsProfileAndExternalAndMemory => {
                execute_with_output_limits_profile_and_external_and_memory(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &memory,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::RowLimitProfileAndExternalAndMemory => {
                execute_with_row_limit_profile_and_external_and_memory(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    &memory,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::RowLimitProfileAndExternalAndContext => {
                execute_with_row_limit_profile_and_external_and_context(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    &context,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::OutputLimitsProfileAndExternalAndContext => {
                execute_with_output_limits_profile_and_external_and_context(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &context,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::OutputLimitsProfileAndExternalAndContextAndMemory => {
                execute_with_output_limits_profile_and_external_and_context_and_memory(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &context,
                    &memory,
                )
                .map(|output| (output.rows.into_rows(), Some(output.profile)))
            }
            Endpoint::RowConsumerProfile => execute_with_row_consumer_profile(
                &plan,
                &mut catalog,
                &mut store,
                &parameters,
                case.rows,
                case.payload,
                &mut consumer,
            )
            .map(|output| (Vec::new(), Some(output.profile))),
            Endpoint::RowConsumerProfileAndExternal => {
                execute_with_row_consumer_profile_and_external(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &mut consumer,
                )
                .map(|output| (Vec::new(), Some(output.profile)))
            }
            Endpoint::RowConsumerProfileAndExternalAndMemory => {
                execute_with_row_consumer_profile_and_external_and_memory(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &mut consumer,
                    &memory,
                )
                .map(|output| (Vec::new(), Some(output.profile)))
            }
            Endpoint::RowConsumerProfileAndExternalAndContext => {
                execute_with_row_consumer_profile_and_external_and_context(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &mut consumer,
                    &context,
                )
                .map(|output| (Vec::new(), Some(output.profile)))
            }
            Endpoint::RowConsumerProfileAndExternalAndContextAndMemory => {
                execute_with_row_consumer_profile_and_external_and_context_and_memory(
                    &plan,
                    &mut catalog,
                    &mut store,
                    &parameters,
                    &mut external,
                    case.rows,
                    case.payload,
                    &mut consumer,
                    &context,
                    &memory,
                )
                .map(|output| (Vec::new(), Some(output.profile)))
            }
        }
    } else {
        let request = ExecutionRequest::new(&plan, &parameters, &memory)
            .with_output_limits(case.rows, case.payload)
            .with_optional_task_context(endpoint.context().then_some(&context));
        let resources = ExecutionResources::new(&mut catalog, &mut store, &mut external);
        if endpoint.consumer() {
            execute_with_request_consumer(request, resources, &mut consumer)
                .map(|output| (Vec::new(), Some(output.profile)))
        } else {
            execute_with_request(request, resources).map(|output| {
                (
                    output.rows.into_rows(),
                    endpoint.profile().then_some(output.profile),
                )
            })
        }
    };
    match result {
        Ok((rows, profile)) => Outcome {
            rows: if endpoint.consumer() { delivered } else { rows },
            profile: profile.map(normalized),
            external: external.0,
            error: None,
        },
        Err(error) => Outcome {
            rows: delivered,
            profile: None,
            error: Some(error.to_string()),
            external: external.0,
        },
    }
}

#[test]
fn every_legacy_entrypoint_matches_request_contract_on_supported_inputs() {
    let mut cases = 0;
    for endpoint in ENDPOINTS {
        for rows in [None, Some(0), Some(2), Some(4)] {
            for payload in [None, Some(1), Some(4096)] {
                for cancelled in [false, true] {
                    for small_memory in [false, true] {
                        for consumer_error in [false, true] {
                            if (!endpoint.payload() && payload.is_some())
                                || (!endpoint.context() && cancelled)
                                || (!endpoint.memory() && small_memory)
                                || (!endpoint.consumer() && consumer_error)
                                // Legacy unbounded callbacks release each row;
                                // the request API deliberately validates first.
                                || (endpoint.consumer() && rows.is_none() && payload.is_none())
                            {
                                continue;
                            }
                            let case = Case {
                                rows,
                                payload,
                                cancelled,
                                small_memory,
                                consumer_error,
                                external: false,
                            };
                            let old = run(endpoint, case, true);
                            let new = run(endpoint, case, false);
                            assert_eq!(old, new, "{endpoint:?}, {case:?}");
                            if old.error.is_none() {
                                assert_eq!(old.rows.len(), 3);
                                for (rank, row) in old.rows.iter().enumerate() {
                                    assert_eq!(row.get("rank"), Some(&Value::Int(rank as i64)));
                                }
                            }
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    assert!(cases > 200);
    println!("Public request compatibility: {cases} cases across 15 legacy entrypoints");
}

#[test]
fn external_wrappers_forward_the_same_host_resource_contract() {
    for endpoint in [
        Endpoint::RowLimitProfileAndExternal,
        Endpoint::OutputLimitsProfileAndExternal,
        Endpoint::OutputLimitsProfileAndExternalAndMemory,
        Endpoint::RowLimitProfileAndExternalAndMemory,
        Endpoint::RowLimitProfileAndExternalAndContext,
        Endpoint::OutputLimitsProfileAndExternalAndContext,
        Endpoint::OutputLimitsProfileAndExternalAndContextAndMemory,
        Endpoint::RowConsumerProfileAndExternal,
        Endpoint::RowConsumerProfileAndExternalAndMemory,
        Endpoint::RowConsumerProfileAndExternalAndContext,
        Endpoint::RowConsumerProfileAndExternalAndContextAndMemory,
    ] {
        for cancelled in [false, true] {
            if cancelled && !endpoint.context() {
                continue;
            }
            let case = Case {
                rows: Some(2),
                payload: None,
                cancelled,
                small_memory: false,
                consumer_error: false,
                external: true,
            };
            let old = run(endpoint, case, true);
            let new = run(endpoint, case, false);
            assert_eq!(old, new, "{endpoint:?}, {case:?}");
            assert!(old.rows.is_empty());
            if cancelled {
                assert!(old.external.is_none());
            } else {
                let observed = old.external.unwrap();
                assert_eq!(observed.embedding, vec![1.0f32.to_bits(), 0.0f32.to_bits()]);
                assert_eq!(
                    observed.filters.get("space").map(String::as_str),
                    Some("selected")
                );
                assert_eq!(observed.context, endpoint.context());
                assert!(old.error.unwrap().contains("host external read rejected"));
            }
        }
    }
}

#[test]
fn unbounded_legacy_delivery_remains_incremental_but_requests_validate_first() {
    let parameters = BTreeMap::new();
    let logical = hawdb_plan_cypher::plan_pipeline_query(
        "MATCH (n:Item) RETURN 1 / n.rank AS value",
        &parameters,
    )
    .unwrap();
    let plan = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in [1, 0] {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([("rank".into(), Value::Int(rank))]),
            )
            .unwrap();
    }
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::MIN,
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let mut legacy_prefix = Vec::new();
    let old = execute_with_row_consumer_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &parameters,
        &mut external,
        None,
        None,
        &mut |row| {
            legacy_prefix.push(row);
            Ok(())
        },
        &memory,
    )
    .unwrap_err();
    let mut request_prefix = Vec::new();
    let new = execute_with_request_consumer(
        ExecutionRequest::new(&plan, &parameters, &memory),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |row| {
            request_prefix.push(row);
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(old.to_string(), new.to_string());
    assert!(old.to_string().contains("division by zero"));
    assert_eq!(legacy_prefix.len(), 1);
    assert!(request_prefix.is_empty());
}
