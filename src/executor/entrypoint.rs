//! Root execution orchestration and result-accounting lifecycle.

use super::*;
use skein_executor::observer::ExecutionProfileBuilder;
pub(super) use skein_executor::result_delivery::ConsumerMemoryMode;
use skein_executor::result_delivery::{OutputLimits, QueryOutputAccumulator};

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

fn record_process_memory(
    report: &mut skein_executor::PipelineMemoryReport,
    process_memory_start: Option<skein_qos::ProcessMemorySnapshot>,
) {
    let Ok(process_memory_end) = skein_qos::ProcessMemorySnapshot::capture() else {
        return;
    };
    report.steady_resident_bytes = Some(process_memory_end.resident_bytes);
    report.peak_resident_bytes = Some(process_memory_end.peak_resident_bytes);
    let Some(process_memory_start) = process_memory_start else {
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

    let memory_ledger = QueryMemoryLedger::new(enforced_query_memory_budget(
        request.memory,
        request.task_context,
    )?);
    let result_memory_budget = enforced_result_memory_budget(request.memory, request.task_context)?;
    let mut output = QueryOutputAccumulator::new(
        request.output_limits,
        result_memory_budget,
        &memory_ledger,
        output_memory,
        consumer,
    )?;
    let process_memory_start = skein_qos::ProcessMemorySnapshot::capture().ok();
    let execution_limit = ExecutionLimit::from_user_max_rows(request.output_limits.max_rows)?;
    let profile = ExecutionProfileBuilder::start(request.plan, request.output_limits.max_rows)?;
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
    let observer = QueryExecutionObserver::new(request.plan);
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
        observer.record_operator_start(request.plan);
        let bindings = execute_bindings_with_limit(
            request.plan,
            catalog,
            store,
            &mut context,
            execution_limit,
        )?;
        observer.record_operator_output(request.plan, bindings.len());
        for binding in bindings {
            output.emit(binding)?;
        }
    }

    output.finish_delivery(request.task_context)?;
    let profile = profile.finish(observer, &memory_ledger, output.metrics(), |report| {
        record_process_memory(report, process_memory_start);
    });
    Ok(ProfiledQueryStream {
        fully_streamed,
        profile,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };

    #[test]
    fn concurrent_ledger_capacity_is_bounded_by_governor_reservations() {
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(4 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
        );
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            resources,
            IoConcurrencyBudget::new(2, 1),
        );
        let first = governor
            .try_admit(RuntimeWorkRequest::foreground_query(64, 8))
            .unwrap();
        let second = governor
            .try_admit(RuntimeWorkRequest::foreground_query(96, 16))
            .unwrap();
        let first_context = first.bind_task_context(RuntimeTaskContext::default());
        let second_context = second.bind_task_context(RuntimeTaskContext::default());
        let memory = ExecutionMemoryConfig::default();
        let ledgers = [&first_context, &second_context].map(|context| {
            QueryMemoryLedger::new(enforced_query_memory_budget(&memory, Some(context)).unwrap())
        });

        let aggregate_ledger_capacity = ledgers
            .iter()
            .map(|ledger| u64::try_from(ledger.snapshot().budget_bytes).unwrap())
            .sum::<u64>();
        assert_eq!(aggregate_ledger_capacity, 160);
        assert!(aggregate_ledger_capacity <= governor.snapshot().admitted_memory_bytes);
        assert_eq!(
            enforced_result_memory_budget(&memory, Some(&first_context))
                .unwrap()
                .get(),
            8
        );
        assert_eq!(
            enforced_result_memory_budget(&memory, Some(&second_context))
                .unwrap()
                .get(),
            16
        );
    }
}
