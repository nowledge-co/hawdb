// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Root execution orchestration and result-accounting lifecycle.

use super::*;
use hawdb_executor::execution_request::ExecutionRequest;
use hawdb_executor::observer::ExecutionProfileBuilder;
pub(super) use hawdb_executor::result_delivery::ConsumerMemoryMode;
use hawdb_executor::result_delivery::QueryOutputAccumulator;

/// Host-owned mutable resources required to execute one physical plan.
///
/// The resources are borrowed for the duration of the call. The executor does
/// not retain them or create a process-global execution context.
pub struct ExecutionResources<'a, S: ExecutionStore> {
    access: ExecutionAccess<'a, S>,
    external: &'a mut dyn ExternalReadOperator,
}

enum ExecutionAccess<'a, S> {
    Mutable {
        catalog: &'a mut Catalog,
        store: &'a mut S,
    },
    ReadOnly {
        catalog: &'a Catalog,
        store: &'a S,
    },
}

impl<S> ExecutionAccess<'_, S> {
    fn read(&self) -> (&Catalog, &S) {
        match self {
            Self::Mutable { catalog, store } => (catalog, store),
            Self::ReadOnly { catalog, store } => (catalog, store),
        }
    }
}

impl<'a, S: ExecutionStore> ExecutionResources<'a, S> {
    pub fn new(
        catalog: &'a mut Catalog,
        store: &'a mut S,
        external: &'a mut dyn ExternalReadOperator,
    ) -> Self {
        Self {
            access: ExecutionAccess::Mutable { catalog, store },
            external,
        }
    }

    pub(crate) fn read_only(
        catalog: &'a Catalog,
        store: &'a S,
        external: &'a mut dyn ExternalReadOperator,
    ) -> Self {
        Self {
            access: ExecutionAccess::ReadOnly { catalog, store },
            external,
        }
    }
}

fn record_process_memory(
    report: &mut hawdb_executor::PipelineMemoryReport,
    process_memory_start: Option<hawdb_qos::ProcessMemorySnapshot>,
) {
    let Ok(process_memory_end) = hawdb_qos::ProcessMemorySnapshot::capture() else {
        return;
    };
    report.steady_resident_bytes = Some(process_memory_end.resident_bytes);
    report.peak_resident_bytes = Some(process_memory_end.peak_resident_bytes);
    let Some(process_memory_start) = process_memory_start else {
        return;
    };
    let process_memory =
        hawdb_qos::ProcessMemoryProfile::between(process_memory_start, process_memory_end);
    report.start_resident_bytes = Some(process_memory.start_resident_bytes);
    report.start_peak_resident_bytes = Some(process_memory.start_peak_resident_bytes);
    report.steady_resident_growth_bytes = Some(process_memory.steady_resident_growth_bytes);
    report.lifetime_peak_resident_growth_bytes =
        Some(process_memory.lifetime_peak_resident_growth_bytes);
    report.total_page_faults = process_memory.total_page_faults;
    report.minor_page_faults = process_memory.minor_page_faults;
    report.major_page_faults = process_memory.major_page_faults;
}

pub(super) fn execute_profiled_rows<S: ExecutionStore>(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_, S>,
) -> Result<ProfiledQueryRows> {
    let (streamed, rows) = execute_profiled_output(request, resources, OutputTarget::Owned)?;
    Ok(ProfiledQueryRows {
        rows: rows.expect("owned execution creates owned rows"),
        profile: streamed.profile,
    })
}

pub(super) fn execute_profiled_consumer<S: ExecutionStore>(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_, S>,
    output_memory: ConsumerMemoryMode,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    execute_profiled_output(
        request,
        resources,
        OutputTarget::Consumer(output_memory, consumer),
    )
    .map(|(streamed, _)| streamed)
}

pub(super) fn execute_profiled_borrowed_consumer<'consumer, S: ExecutionStore>(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_, S>,
    delivery: StreamDelivery,
    consumer: &'consumer mut hawdb_executor::result_delivery::BorrowedRowConsumer<'consumer>,
) -> Result<ProfiledQueryStream> {
    execute_profiled_output(
        request,
        resources,
        OutputTarget::Borrowed(delivery, consumer),
    )
    .map(|(streamed, _)| streamed)
}

enum OutputTarget<'a> {
    Owned,
    Consumer(ConsumerMemoryMode, &'a mut dyn FnMut(Row) -> Result<()>),
    Borrowed(
        StreamDelivery,
        &'a mut hawdb_executor::result_delivery::BorrowedRowConsumer<'a>,
    ),
}

fn execute_profiled_output<S: ExecutionStore>(
    request: ExecutionRequest<'_>,
    resources: ExecutionResources<'_, S>,
    target: OutputTarget<'_>,
) -> Result<(ProfiledQueryStream, Option<QueryRows>)> {
    let ExecutionResources {
        mut access,
        external,
    } = resources;
    let (catalog, store) = access.read();
    store.ensure_usable()?;
    let plan = request.plan();
    let parameters = request.parameters();
    let output_limits = request.output_limits();
    let memory = request.memory();
    let task_context = request.task_context();

    let memory_ledger = QueryMemoryLedger::for_query(memory, task_context)?
        .with_file_descriptors(store.file_descriptor_context());
    let (result_memory_budget, budget_option) =
        enforced_result_memory_budget_with_option(memory, task_context)?;
    let mut output = match target {
        OutputTarget::Consumer(output_memory, consumer) => {
            QueryOutputAccumulator::new_with_budget_option(
                output_limits,
                result_memory_budget,
                &memory_ledger,
                budget_option,
                output_memory,
                consumer,
            )?
        }
        OutputTarget::Borrowed(delivery, consumer) => {
            QueryOutputAccumulator::new_borrowed_with_budget_option(
                output_limits,
                result_memory_budget,
                &memory_ledger,
                budget_option,
                delivery,
                consumer,
            )?
        }
        OutputTarget::Owned => QueryOutputAccumulator::new_owned_with_budget_option(
            output_limits,
            result_memory_budget,
            &memory_ledger,
            budget_option,
        )?,
    };
    if let Some(columns) = request.output_columns() {
        output.bind_output_schema(columns.iter().map(String::as_str))?;
    }
    let process_memory_start = hawdb_qos::ProcessMemorySnapshot::capture().ok();
    let execution_limit = ExecutionLimit::from_user_max_rows(output_limits.max_rows)?;
    let profile = ExecutionProfileBuilder::start(plan, output_limits.max_rows)?;
    let prepared_plan = PreparedPhysicalPlan::prepare(plan, store, memory);
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
        estimated_execution_memory(plan, memory)
    );
    let batch_plan = prepared_plan.batch();
    let fully_streamed = batch_plan.is_some();
    let observer = QueryExecutionObserver::new(plan);
    let mut context = ExecutionContext {
        parameters,
        external,
        memory,
        memory_ledger: &memory_ledger,
        task_context,
        observer: &observer,
        host_scorer: request.host_scorer(),
    };

    if let Some(batch_plan) = batch_plan {
        let external = BatchExternalReadAdapter::new(&mut *context.external);
        let batch_context = BatchReadContext {
            catalog,
            store,
            parameters: context.parameters,
            external: &external,
            memory,
            memory_ledger: &memory_ledger,
            task_context,
            observer: context.observer,
            host_scorer: context.host_scorer,
        };
        let native = hawdb_executor::numeric::try_stream_native_projection(
            batch_plan.plan(),
            batch_context.numeric_context(),
            execution_limit,
            &mut |row| {
                output.emit_borrowed(row)?;
                Ok(BatchControl::Continue)
            },
        );
        if let Some(result) = native {
            result?;
        } else {
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
        }
    } else {
        let ExecutionAccess::Mutable { catalog, store } = &mut access else {
            return Err(crate::HawDBError::Execution(
                "materialized execution requires a private query workspace".into(),
            ));
        };
        observer.record_operator_start(plan);
        let bindings =
            execute_bindings_with_limit(plan, catalog, *store, &mut context, execution_limit)?;
        observer.record_operator_output(plan, bindings.len());
        for binding in bindings {
            output.emit(binding)?;
        }
    }

    output.finish_delivery(task_context)?;
    let profile = profile.finish(observer, &memory_ledger, output.metrics(), |report| {
        record_process_memory(report, process_memory_start);
    });
    let rows = output.take_owned_rows();
    Ok((
        ProfiledQueryStream {
            fully_streamed,
            profile,
        },
        rows,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_qos::{
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
        let ledgers = [&first_context, &second_context]
            .map(|context| QueryMemoryLedger::for_query(&memory, Some(context)).unwrap());

        let aggregate_ledger_capacity = ledgers
            .iter()
            .map(|ledger| u64::try_from(ledger.snapshot().budget_bytes).unwrap())
            .sum::<u64>();
        assert_eq!(aggregate_ledger_capacity, 160);
        assert!(aggregate_ledger_capacity <= governor.snapshot().admitted_memory_bytes);
        let account = ledgers[0].account(
            QueryMemoryClass::BlockingState,
            "runtime grant probe",
            memory.query_memory_bytes,
        );
        let error = account.reserve(65).unwrap_err();
        let HawDBError::ResourceBudgetExceeded(error) = error else {
            panic!("expected typed runtime grant rejection");
        };
        assert_eq!(error.scope, hawdb_core::ResourceBudgetScope::Query);
        assert_eq!(error.budget, 64);
        assert_eq!(
            error.budget_option,
            "RuntimeTaskContext.memory_reservation.memory_bytes"
        );
        assert_eq!(ledgers[0].snapshot().used_bytes, 0);
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
