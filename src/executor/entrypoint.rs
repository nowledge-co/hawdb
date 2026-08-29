//! Root execution orchestration and result-accounting lifecycle.

use super::*;
use skein_executor::{QueryMemoryAccount, QueryMemoryLease};

#[derive(Clone, Copy)]
pub(super) struct ExecutionRequest<'a> {
    plan: &'a PhysicalPlan,
    parameters: &'a BTreeMap<String, Value>,
    output_limits: OutputLimits,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

impl<'a> ExecutionRequest<'a> {
    pub(super) fn new(
        plan: &'a PhysicalPlan,
        parameters: &'a BTreeMap<String, Value>,
        memory: &'a ExecutionMemoryConfig,
    ) -> Self {
        Self {
            plan,
            parameters,
            output_limits: OutputLimits::default(),
            memory,
            task_context: None,
        }
    }

    pub(super) fn with_output_limits(
        mut self,
        max_rows: Option<usize>,
        max_payload_bytes: Option<usize>,
    ) -> Self {
        self.output_limits = OutputLimits {
            max_rows,
            max_payload_bytes,
        };
        self
    }

    pub(super) fn with_optional_task_context(
        mut self,
        task_context: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        self.task_context = task_context;
        self
    }

    pub(super) fn with_task_context(mut self, task_context: &'a RuntimeTaskContext) -> Self {
        self.task_context = Some(task_context);
        self
    }
}

pub(super) struct ExecutionResources<'a> {
    catalog: &'a mut Catalog,
    store: &'a mut GraphStore,
    external: &'a mut dyn ExternalReadOperator,
}

impl<'a> ExecutionResources<'a> {
    pub(super) fn new(
        catalog: &'a mut Catalog,
        store: &'a mut GraphStore,
        external: &'a mut dyn ExternalReadOperator,
    ) -> Self {
        Self {
            catalog,
            store,
            external,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ConsumerMemoryMode {
    /// The consumer keeps emitted rows alive through the final memory snapshot.
    Retained,
    /// Each row can be uncharged as soon as the consumer call returns.
    ReleasedAfterCall,
}

#[derive(Clone, Copy, Default)]
struct OutputLimits {
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
}

#[derive(Clone, Copy, Default)]
struct OutputMetrics {
    rows: usize,
    payload_bytes: usize,
}

struct QueryOutputAccumulator<'a> {
    consumer: &'a mut dyn FnMut(Row) -> Result<()>,
    memory_mode: ConsumerMemoryMode,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    result_account: QueryMemoryAccount,
    retained_result_lease: QueryMemoryLease,
    metrics: OutputMetrics,
}

impl<'a> QueryOutputAccumulator<'a> {
    fn new(
        limits: OutputLimits,
        memory: &ExecutionMemoryConfig,
        memory_ledger: &QueryMemoryLedger,
        memory_mode: ConsumerMemoryMode,
        consumer: &'a mut dyn FnMut(Row) -> Result<()>,
    ) -> Result<Self> {
        let result_account = memory_ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "query result",
            memory.query_memory_bytes,
        );
        let retained_result_lease = result_account.reserve(0)?;
        Ok(Self {
            consumer,
            memory_mode,
            max_rows: limits.max_rows,
            max_payload_bytes: limits.max_payload_bytes,
            result_account,
            retained_result_lease,
            metrics: OutputMetrics::default(),
        })
    }

    fn emit(&mut self, binding: Binding) -> Result<()> {
        if let Some(max_rows) = self.max_rows
            && self.metrics.rows >= max_rows
        {
            return Err(SkeinError::Execution(format!(
                "read query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
        }

        let row = binding.values;
        let row_memory_bytes = map_memory_bytes(&row);
        let transient_result_lease = match self.memory_mode {
            ConsumerMemoryMode::Retained => {
                self.retained_result_lease.grow(row_memory_bytes)?;
                None
            }
            ConsumerMemoryMode::ReleasedAfterCall => {
                Some(self.result_account.reserve(row_memory_bytes)?)
            }
        };
        let row_payload_bytes = map_payload_bytes(&row);
        let next_payload_bytes = self.metrics.payload_bytes.saturating_add(row_payload_bytes);
        if let Some(max_payload_bytes) = self.max_payload_bytes
            && next_payload_bytes > max_payload_bytes
        {
            return Err(SkeinError::Execution(format!(
                "read query payload would exceed max_payload_bytes {max_payload_bytes} (max_read_result_payload_bytes {max_payload_bytes}; next total {next_payload_bytes})"
            )));
        }

        (self.consumer)(row)?;
        drop(transient_result_lease);
        self.metrics.rows = self.metrics.rows.saturating_add(1);
        self.metrics.payload_bytes = next_payload_bytes;
        Ok(())
    }

    fn metrics(&self) -> OutputMetrics {
        self.metrics
    }
}

struct ExecutionProfileBuilder {
    profile: ReadExecutionProfile,
    process_memory_start: Option<skein_qos::ProcessMemorySnapshot>,
}

impl ExecutionProfileBuilder {
    fn start(
        plan: &PhysicalPlan,
        max_rows: Option<usize>,
        process_memory_start: Option<skein_qos::ProcessMemorySnapshot>,
    ) -> Result<Self> {
        Ok(Self {
            profile: read_execution_profile(plan, max_rows)?,
            process_memory_start,
        })
    }

    fn finish(
        mut self,
        observer: QueryExecutionObserver,
        memory_ledger: &QueryMemoryLedger,
        output: OutputMetrics,
    ) -> ReadExecutionProfile {
        let QueryExecutionReports {
            scan_pruning,
            vector_execution,
            graph_expansion,
            blocking_memory,
            mut pipeline_memory,
        } = observer.into_reports();
        self.profile.scan_pruning_reports = scan_pruning;
        self.profile.vector_execution_reports = vector_execution;
        self.profile.graph_expansion_reports = graph_expansion;
        self.profile.blocking_operator_memory_reports = blocking_memory;

        pipeline_memory.output_rows = output.rows;
        pipeline_memory.output_payload_bytes = output.payload_bytes;
        let query_memory = memory_ledger.snapshot();
        pipeline_memory.query_memory_budget_bytes = query_memory.budget_bytes;
        pipeline_memory.query_memory_peak_bytes = query_memory.peak_bytes;
        pipeline_memory.query_memory_completion_bytes = query_memory.used_bytes;
        pipeline_memory.query_memory_account_count = query_memory.account_count;
        self.record_process_memory(&mut pipeline_memory);
        self.profile.pipeline_memory_report = pipeline_memory;
        self.profile
    }

    fn record_process_memory(&self, report: &mut skein_executor::PipelineMemoryReport) {
        let Ok(process_memory_end) = skein_qos::ProcessMemorySnapshot::capture() else {
            return;
        };
        report.steady_resident_bytes = Some(process_memory_end.resident_bytes);
        report.peak_resident_bytes = Some(process_memory_end.peak_resident_bytes);
        let Some(process_memory_start) = self.process_memory_start else {
            return;
        };
        let process_memory =
            skein_qos::ProcessMemoryProfile::between(process_memory_start, process_memory_end);
        report.start_resident_bytes = Some(process_memory.start_resident_bytes);
        report.start_peak_resident_bytes = Some(process_memory.start_peak_resident_bytes);
        report.steady_resident_growth_bytes = Some(process_memory.steady_resident_growth_bytes);
        report.lifetime_peak_resident_growth_bytes =
            Some(process_memory.lifetime_peak_resident_growth_bytes);
        report.total_page_faults = process_memory.total_page_faults;
        report.minor_page_faults = process_memory.minor_page_faults;
        report.major_page_faults = process_memory.major_page_faults;
    }
}

pub(super) fn execute_profiled_rows(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_>,
) -> Result<ProfiledQueryRows> {
    let mut rows = QueryRowsBuilder::new();
    let streamed = execute_profiled_consumer(
        request,
        resources,
        ConsumerMemoryMode::Retained,
        &mut |row| rows.push_named_row(row),
    )?;
    Ok(ProfiledQueryRows {
        rows: rows.finish(),
        profile: streamed.profile,
    })
}

pub(super) fn execute_profiled_consumer(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_>,
    output_memory: ConsumerMemoryMode,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    let ExecutionResources {
        catalog,
        store,
        external,
    } = resources;
    store.ensure_usable()?;

    let memory_ledger = QueryMemoryLedger::new(request.memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new(
        request.output_limits,
        request.memory,
        &memory_ledger,
        output_memory,
        consumer,
    )?;
    let process_memory_start = skein_qos::ProcessMemorySnapshot::capture().ok();
    let execution_limit = ExecutionLimit::from_user_max_rows(request.output_limits.max_rows)?;
    let profile = ExecutionProfileBuilder::start(
        request.plan,
        request.output_limits.max_rows,
        process_memory_start,
    )?;
    let prepared_plan = PreparedPhysicalPlan::prepare(request.plan, store, request.memory);
    debug_assert_eq!(
        prepared_plan.storage_capability(),
        if store.is_out_of_core() {
            PreparedStorageCapability::OutOfCore
        } else {
            PreparedStorageCapability::InMemory
        }
    );
    debug_assert_eq!(
        prepared_plan.required_memory(),
        estimated_execution_memory(request.plan, request.memory)
    );
    let batch_plan = prepared_plan.batch();
    let fully_streamed = batch_plan.is_some();
    let observer = QueryExecutionObserver::default();
    let mut context = ExecutionContext {
        parameters: request.parameters,
        external,
        memory: request.memory,
        memory_ledger: &memory_ledger,
        task_context: request.task_context,
        observer: &observer,
    };

    if let Some(batch_plan) = batch_plan {
        let external = BatchExternalReadAdapter::new(&mut *context.external);
        let batch_context = BatchReadContext {
            catalog,
            store,
            parameters: context.parameters,
            external: &external,
            memory: request.memory,
            memory_ledger: &memory_ledger,
            task_context: request.task_context,
            observer: context.observer,
        };
        execute_prepared_binding_batches(
            batch_plan,
            batch_context,
            execution_limit,
            &mut |batch| {
                for binding in batch {
                    output.emit(binding)?;
                }
                Ok(BatchControl::Continue)
            },
        )?;
    } else {
        let bindings = execute_bindings_with_limit(
            request.plan,
            catalog,
            store,
            &mut context,
            execution_limit,
        )?;
        for binding in bindings {
            output.emit(binding)?;
        }
    }

    let profile = profile.finish(observer, &memory_ledger, output.metrics());
    Ok(ProfiledQueryStream {
        fully_streamed,
        profile,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(value: &str) -> Binding {
        Binding::values(BTreeMap::from([(
            "value".to_string(),
            Value::String(value.to_string()),
        )]))
    }

    #[test]
    fn released_consumer_memory_is_not_retained_between_rows() {
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let plan = PhysicalPlan::EmptyExec;
        let parameters = BTreeMap::new();
        let request = ExecutionRequest::new(&plan, &parameters, &memory);
        let mut rows = 0usize;
        let mut consumer = |_| {
            rows = rows.saturating_add(1);
            Ok(())
        };
        let mut output = QueryOutputAccumulator::new(
            request.output_limits,
            request.memory,
            &ledger,
            ConsumerMemoryMode::ReleasedAfterCall,
            &mut consumer,
        )
        .unwrap();

        output.emit(binding("first")).unwrap();
        output.emit(binding("second")).unwrap();

        assert_eq!(output.metrics().rows, 2);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        drop(output);
        assert_eq!(rows, 2);
    }

    #[test]
    fn retained_consumer_memory_lives_until_accumulator_drop() {
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let plan = PhysicalPlan::EmptyExec;
        let parameters = BTreeMap::new();
        let request = ExecutionRequest::new(&plan, &parameters, &memory);
        let mut consumer = |_| Ok(());
        let mut output = QueryOutputAccumulator::new(
            request.output_limits,
            request.memory,
            &ledger,
            ConsumerMemoryMode::Retained,
            &mut consumer,
        )
        .unwrap();

        output.emit(binding("retained")).unwrap();
        assert!(ledger.snapshot().used_bytes > 0);

        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn output_limits_stop_before_calling_the_consumer() {
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let plan = PhysicalPlan::EmptyExec;
        let parameters = BTreeMap::new();
        let request =
            ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(1), None);
        let mut rows = 0usize;
        let mut consumer = |_| {
            rows = rows.saturating_add(1);
            Ok(())
        };
        let mut output = QueryOutputAccumulator::new(
            request.output_limits,
            request.memory,
            &ledger,
            ConsumerMemoryMode::ReleasedAfterCall,
            &mut consumer,
        )
        .unwrap();

        output.emit(binding("first")).unwrap();
        let error = output.emit(binding("second")).unwrap_err();

        assert!(error.to_string().contains("max_read_result_rows 1"));
        assert_eq!(output.metrics().rows, 1);
        drop(output);
        assert_eq!(rows, 1);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn payload_limit_failure_releases_transient_result_memory() {
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let plan = PhysicalPlan::EmptyExec;
        let parameters = BTreeMap::new();
        let request =
            ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(None, Some(0));
        let mut rows = 0usize;
        let mut consumer = |_| {
            rows = rows.saturating_add(1);
            Ok(())
        };
        let mut output = QueryOutputAccumulator::new(
            request.output_limits,
            request.memory,
            &ledger,
            ConsumerMemoryMode::ReleasedAfterCall,
            &mut consumer,
        )
        .unwrap();

        let error = output.emit(binding("too-large")).unwrap_err();

        assert!(error
            .to_string()
            .contains("max_read_result_payload_bytes 0"));
        assert_eq!(output.metrics().rows, 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        drop(output);
        assert_eq!(rows, 0);
    }
}
