//! Internal numeric scan eligibility, execution, and batch preparation.
//!
//! The embedded facade supplies storage views and query-owned resources. These
//! prepared-kernel contracts are not a second host integration API.

mod lending;

#[cfg(test)]
mod differential;

use crate::binding::Binding;
use crate::columnar::{
    filter_float64_values, filter_int64_values, select_float64_values_view,
    select_int64_values_view, BindingSchema, ColumnVector, ColumnarBatch, NumericLiteral,
    NumericPredicate, Selection, SlotDescriptor, SlotId, SlotType, Validity, ValidityBuilder,
    ValidityView,
};
use crate::expression::insert_projected_value;
use crate::morsel::{
    MorselAdmission, MorselAdmissionRequest, MorselOutput, MorselStreamControl,
    MorselStreamResources, PipelineId, SharedPoolMorselScheduler,
};
use crate::observer::ExecutionObserver;
use crate::observer::QueryExecutionObserver;
use crate::pipeline::{runtime_checkpoint, BatchControl, BindingBatch};
use crate::store::{GraphExecutionRead, ScanControl};
use crate::SharedExecutorPool;
use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
use lending::{
    admitted_numeric_batch_rows, LendingBatchCursor, NumericNodeBatch, NumericNodeBatchCursor,
    OwnedNumericBatchBuffer,
};
use skein_core::{Catalog, Result, RuntimeTaskContext, SkeinError, Value};
use skein_plan::{PhysicalPlan, PlanChildren, Predicate, Projection, ProjectionExpression};
use skein_storage::{NodeRecord, ScanPruningReport};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Shared ceiling for root admission and numeric morsel execution.
pub const MAX_MORSEL_PARALLELISM: usize = 16;
const DEFAULT_MORSEL_CPU_SHARE_DIVISOR: usize = 4;
const DEFAULT_MORSEL_MIN_PARALLELISM: usize = 4;

/// Derives the CPU budget for a morsel-capable query from host capacity.
pub fn default_morsel_cpu_ceiling(effective_cpu_slots: usize) -> usize {
    let effective_cpu_slots = effective_cpu_slots.max(1);
    effective_cpu_slots
        .div_ceil(DEFAULT_MORSEL_CPU_SHARE_DIVISOR)
        .max(DEFAULT_MORSEL_MIN_PARALLELISM)
        .min(effective_cpu_slots)
        .min(MAX_MORSEL_PARALLELISM)
}

#[derive(Clone, Copy)]
pub struct NumericExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn GraphExecutionRead,
    pub memory: &'a ExecutionMemoryConfig,
    pub memory_ledger: &'a QueryMemoryLedger,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a QueryExecutionObserver,
}

const DEFAULT_BATCHES_PER_MORSEL: usize = 16;
const DEFAULT_MIN_MORSELS_PER_WORKER: usize = 4;
const PREDICATE_VALUE_SLOT: SlotId = SlotId(0);
const NODE_ID_SLOT: SlotId = SlotId(1);

#[derive(Debug, Clone, Copy)]
pub struct NumericFragment<'a> {
    pub label: &'a str,
    pub property: &'a str,
    pub property_type: skein_core::PropertyType,
    pub predicate: NumericPredicate,
    pub expected: NumericLiteral,
    pub fused_operators: Option<FusedNumericOperators<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct FusedNumericOperators<'a> {
    scan: &'a PhysicalPlan,
    filter: &'a PhysicalPlan,
}

#[derive(Debug, Clone, Copy)]
pub struct LendingNumericScan {
    pub batch_rows: usize,
    pub needs_node_ids: bool,
}

struct NumericBatchEmitter<'plan, 'task, 'observer, 'emit> {
    fragment: NumericFragment<'plan>,
    items: &'plan [Projection],
    emitted: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'task RuntimeTaskContext>,
    observer: &'observer QueryExecutionObserver,
    emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    selected_rows: Vec<u32>,
}

pub fn supports_parallel_morsel_execution(plan: &PhysicalPlan, catalog: &Catalog) -> bool {
    match plan {
        PhysicalPlan::ProjectExec { items, input } => {
            NumericFragment::try_prepare(items, input, catalog)
                .is_some_and(|fragment| fragment.supports_lending_projection(items))
        }
        PhysicalPlan::NodeProjectionScanExec {
            variable,
            label,
            access,
            predicate: Some(predicate),
            items,
            ..
        } if access.is_label_scan() => {
            NumericFragment::try_prepare_parts(items, variable, label, predicate, catalog)
                .is_some_and(|fragment| fragment.supports_lending_projection(items))
        }
        _ => match plan.children() {
            PlanChildren::None => false,
            PlanChildren::Unary(input) => supports_parallel_morsel_execution(input, catalog),
            PlanChildren::Binary(left, right) => {
                supports_parallel_morsel_execution(left, catalog)
                    || supports_parallel_morsel_execution(right, catalog)
            }
        },
    }
}

pub fn default_morsel_parallelism(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn crate::store::GraphExecutionRead,
    memory: &ExecutionMemoryConfig,
) -> usize {
    match plan {
        PhysicalPlan::ProjectExec { items, input } => {
            if let Some(fragment) = NumericFragment::try_prepare(items, input, catalog) {
                if !fragment.supports_lending_projection(items) {
                    return 1;
                }
                return fragment.default_parallelism(items, catalog, store, memory);
            }
            default_morsel_parallelism(input, catalog, store, memory)
        }
        PhysicalPlan::NodeProjectionScanExec {
            variable,
            label,
            access,
            predicate: Some(predicate),
            items,
            ..
        } if access.is_label_scan() => {
            NumericFragment::try_prepare_parts(items, variable, label, predicate, catalog)
                .filter(|fragment| fragment.supports_lending_projection(items))
                .map_or(1, |fragment| {
                    fragment.default_parallelism(items, catalog, store, memory)
                })
        }
        _ => match plan.children() {
            PlanChildren::None => 1,
            PlanChildren::Unary(input) => default_morsel_parallelism(input, catalog, store, memory),
            PlanChildren::Binary(left, right) => {
                default_morsel_parallelism(left, catalog, store, memory)
                    .max(default_morsel_parallelism(right, catalog, store, memory))
            }
        },
    }
}

pub fn try_stream_columnar_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    let fragment = NumericFragment::try_prepare(items, input, context.catalog)?;
    Some(fragment.stream(items, context, execution_limit, emit))
}

pub fn try_stream_columnar_node_projection_batches(
    variable: &str,
    label: &str,
    predicate: Option<&Predicate>,
    items: &[Projection],
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    if context.store.is_out_of_core() {
        return None;
    }
    let fragment =
        NumericFragment::try_prepare_parts(items, variable, label, predicate?, context.catalog)?;
    Some(fragment.stream(items, context, execution_limit, emit))
}

impl<'a> NumericFragment<'a> {
    fn try_prepare(
        items: &[Projection],
        input: &'a PhysicalPlan,
        catalog: &Catalog,
    ) -> Option<Self> {
        let filter = input;
        let PhysicalPlan::FilterExec { predicate, input } = filter else {
            return None;
        };
        let scan = input.as_ref();
        let PhysicalPlan::SeqNodeScan {
            variable: scan_variable,
            label,
        } = scan
        else {
            return None;
        };
        let mut fragment =
            Self::try_prepare_parts(items, scan_variable, label, predicate, catalog)?;
        fragment.fused_operators = Some(FusedNumericOperators { scan, filter });
        Some(fragment)
    }

    fn try_prepare_parts(
        items: &[Projection],
        scan_variable: &str,
        label: &'a str,
        predicate: &'a Predicate,
        catalog: &Catalog,
    ) -> Option<Self> {
        let (variable, property, numeric_predicate, value) = match predicate {
            Predicate::PropertyEq {
                variable,
                property,
                value,
            } => (variable, property, NumericPredicate::Eq, value),
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            } => (variable, property, NumericPredicate::Compare(*op), value),
            _ => return None,
        };
        if variable != scan_variable || label.is_empty() || label.contains('|') {
            return None;
        }
        if !items.iter().all(|item| {
            matches!(
                &item.expression,
                ProjectionExpression::Id { variable }
                    | ProjectionExpression::Property { variable, .. }
                    if variable == scan_variable
            ) || matches!(&item.expression, ProjectionExpression::Literal(_))
        }) {
            return None;
        }
        let expected = NumericLiteral::from_value(value)?;
        let table_id = catalog.table_id(skein_core::TableKind::Node, label)?;
        let descriptor_id = catalog.property_descriptor_id(table_id, property)?;
        let descriptor = catalog.property_descriptor(descriptor_id)?;
        if descriptor.state != skein_core::SchemaObjectState::Public
            || !matches!(
                descriptor.value_type,
                skein_core::PropertyType::Int | skein_core::PropertyType::Float
            )
        {
            return None;
        }
        Some(Self {
            label,
            property,
            property_type: descriptor.value_type,
            predicate: numeric_predicate,
            expected,
            fused_operators: None,
        })
    }

    fn select_int64_values(
        self,
        values: &[i64],
        validity: ValidityView<'_>,
        selected_rows: &mut Vec<u32>,
    ) -> Result<()> {
        select_int64_values_view(
            values,
            validity,
            self.predicate,
            self.expected,
            selected_rows,
        )
    }

    fn select_float64_values(
        self,
        values: &[f64],
        validity: ValidityView<'_>,
        selected_rows: &mut Vec<u32>,
    ) -> Result<()> {
        select_float64_values_view(
            values,
            validity,
            self.predicate,
            self.expected,
            selected_rows,
        )
    }

    fn filter_int64_values(
        self,
        values: &[i64],
        validity: &Validity,
        input: &Selection,
    ) -> Result<Selection> {
        filter_int64_values(values, validity, input, self.predicate, self.expected)
    }

    fn filter_float64_values(
        self,
        values: &[f64],
        validity: &Validity,
        input: &Selection,
    ) -> Result<Selection> {
        filter_float64_values(values, validity, input, self.predicate, self.expected)
    }

    fn filter_batch(self, batch: ColumnarBatch) -> Result<ColumnarBatch> {
        batch.filter_numeric(PREDICATE_VALUE_SLOT, self.predicate, self.expected)
    }

    fn start_fused_operators(self, observer: &QueryExecutionObserver) {
        if let Some(operators) = self.fused_operators {
            observer.record_operator_start(operators.scan);
            observer.record_operator_start(operators.filter);
        }
    }

    fn record_fused_cardinality(
        self,
        observer: &QueryExecutionObserver,
        scan_rows: usize,
        filter_rows: usize,
    ) {
        if let Some(operators) = self.fused_operators {
            observer.record_operator_output(operators.scan, scan_rows);
            observer.record_operator_output(operators.filter, filter_rows);
        }
    }

    fn supports_lending_projection(self, items: &[Projection]) -> bool {
        items.iter().all(|item| match &item.expression {
            ProjectionExpression::Id { .. } | ProjectionExpression::Literal(_) => true,
            ProjectionExpression::Property { property, .. } => property == self.property,
            _ => false,
        })
    }

    fn default_parallelism(
        self,
        items: &[Projection],
        catalog: &Catalog,
        store: &dyn crate::store::GraphExecutionRead,
        memory: &ExecutionMemoryConfig,
    ) -> usize {
        if store.is_out_of_core() {
            return 1;
        }
        let Some(label_id) = catalog.label_id(self.label) else {
            return 1;
        };
        let needs_node_ids = items
            .iter()
            .any(|item| matches!(item.expression, ProjectionExpression::Id { .. }));
        let batch_rows = if self.supports_lending_projection(items) {
            admitted_numeric_batch_rows(
                memory.batch_rows.get(),
                memory.batch_payload_bytes.get(),
                needs_node_ids,
                true,
            )
            .unwrap_or(1)
        } else {
            memory.batch_rows.get()
        };
        let morsel_rows = batch_rows.saturating_mul(DEFAULT_BATCHES_PER_MORSEL);
        let morsel_count = store
            .node_count_for_label(Some(label_id))
            .div_ceil(morsel_rows.max(1));
        default_morsel_worker_count(morsel_count, MAX_MORSEL_PARALLELISM)
    }

    fn stream(
        self,
        items: &[Projection],
        context: NumericExecutionContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        self.start_fused_operators(context.observer);
        let Some(label_id) = context.catalog.label_id(self.label) else {
            return Ok(BatchControl::Continue);
        };
        let supports_typed_projection = self.supports_lending_projection(items);
        let use_lending = !context.store.is_out_of_core() && supports_typed_projection;
        let use_owned_typed = context.store.is_out_of_core() && supports_typed_projection;
        let needs_node_ids = items
            .iter()
            .any(|item| matches!(item.expression, ProjectionExpression::Id { .. }));
        let target_rows = if use_lending || use_owned_typed {
            let admitted = admitted_numeric_batch_rows(
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes.get(),
                needs_node_ids,
                true,
            )
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "numeric lending scan scratch requires more than batch_payload_bytes {}",
                    context.memory.batch_payload_bytes
                ))
            })?;
            NonZeroUsize::new(admitted).expect("admitted batch rows are non-zero")
        } else {
            context.memory.batch_rows
        };
        let lending_scan = LendingNumericScan {
            batch_rows: target_rows.get(),
            needs_node_ids,
        };
        let candidate_count = context.store.node_count_for_label(Some(label_id));
        let morsel_rows =
            NonZeroUsize::new(target_rows.get().saturating_mul(DEFAULT_BATCHES_PER_MORSEL))
                .expect("morsel row target is non-zero");
        let columnar_schema = use_lending
            .then(|| numeric_columnar_schema(self, lending_scan.needs_node_ids))
            .transpose()?;
        let typed_morsel_memory = columnar_schema.as_ref().map(|schema| {
            numeric_morsel_memory(
                morsel_rows.get(),
                target_rows.get(),
                lending_scan.needs_node_ids,
                schema,
            )
        });
        let typed_batch_fits = typed_morsel_memory.is_some_and(|memory| {
            memory.output_reservation_bytes.get() <= context.memory.batch_payload_bytes.get()
        });
        let admitted_parallelism = context
            .task_context
            .map(|task_context| task_context.admitted_parallelism().get())
            .unwrap_or(1);
        let pool = if context.store.is_out_of_core() {
            None
        } else {
            let pool = match context
                .task_context
                .and_then(RuntimeTaskContext::executor_thread_limit)
            {
                Some(worker_limit) => SharedExecutorPool::shared_bounded(worker_limit),
                None => SharedExecutorPool::shared_default(),
            }
            .map_err(|error| SkeinError::Execution(error.to_string()))?;
            Some(pool)
        };
        let pool_parallelism = pool
            .as_ref()
            .map_or(1, SharedExecutorPool::worker_count)
            .min(MAX_MORSEL_PARALLELISM);
        let morsel_count = candidate_count.div_ceil(morsel_rows.get());
        let parallel_eligible = use_lending && typed_batch_fits && pool.is_some();
        let requested_parallelism = NonZeroUsize::new(if parallel_eligible {
            default_morsel_worker_count(morsel_count, pool_parallelism.min(admitted_parallelism))
                .max(1)
        } else {
            1
        })
        .expect("morsel parallelism is non-zero");
        let input_reference_bytes = morsel_rows
            .get()
            .saturating_mul(std::mem::size_of::<&NodeRecord>());
        let bytes_per_worker = if parallel_eligible {
            typed_morsel_memory
                .expect("parallel typed scan has a memory estimate")
                .worker_live_bytes
        } else {
            NonZeroUsize::new(
                context
                    .memory
                    .batch_payload_bytes
                    .get()
                    .saturating_add(input_reference_bytes),
            )
            .expect("batch payload budget is non-zero")
        };
        let admission = MorselAdmission::try_new(MorselAdmissionRequest {
            pipeline_id: PipelineId(0),
            input_rows: candidate_count,
            target_rows: morsel_rows,
            requested_parallelism,
            bytes_per_worker,
            memory_budget_bytes: context.memory.query_memory_bytes,
        })?;
        let parallel = parallel_eligible
            && admission.max_workers() > 1
            && execution_limit
                .output_rows
                .is_none_or(|limit| limit > morsel_rows.get())
            && pool.is_some();
        context.observer.record_morsel_admission(
            admission.max_workers(),
            if parallel {
                admission.max_workers()
            } else {
                usize::from(admission.morsel_count() > 0)
            },
        );

        let (emitted, stopped) = if use_owned_typed {
            stream_owned_typed_numeric_nodes(
                self,
                items,
                label_id,
                lending_scan,
                context,
                execution_limit,
                emit,
            )?
        } else if context.store.is_out_of_core() {
            stream_owned_numeric_nodes(self, items, label_id, context, execution_limit, emit)?
        } else if parallel {
            stream_parallel_borrowed_numeric_nodes(
                self,
                items,
                label_id,
                morsel_rows,
                lending_scan,
                columnar_schema.expect("parallel typed scan has a columnar schema"),
                typed_morsel_memory.expect("parallel typed scan has a memory estimate"),
                admission.max_workers(),
                pool.expect("parallel morsel execution requires a shared pool"),
                context,
                execution_limit,
                emit,
            )?
        } else if use_lending {
            stream_lending_numeric_nodes(
                self,
                items,
                label_id,
                lending_scan,
                context,
                execution_limit,
                emit,
            )?
        } else {
            stream_borrowed_numeric_nodes(self, items, label_id, context, execution_limit, emit)?
        };
        context
            .observer
            .record_scan_pruning_report(ScanPruningReport {
                target_kind: skein_storage::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: skein_storage::ScanPruningStrategy::FullLabelScan,
                pruned: false,
                exact_empty: candidate_count == 0,
                candidate_count_before_pruning: candidate_count,
                pruned_candidate_count: 0,
                candidate_count_before_filter: candidate_count,
                output_count: emitted,
                filtered_out_count: candidate_count.saturating_sub(emitted),
            });
        Ok(if stopped {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    }
}

fn default_morsel_worker_count(morsel_count: usize, worker_ceiling: usize) -> usize {
    worker_ceiling
        .min(morsel_count / DEFAULT_MIN_MORSELS_PER_WORKER)
        .max(1)
}

fn numeric_columnar_schema(
    fragment: NumericFragment<'_>,
    needs_node_ids: bool,
) -> Result<Arc<BindingSchema>> {
    let logical_type = match fragment.property_type {
        skein_core::PropertyType::Int | skein_core::PropertyType::Float => {
            fragment.property_type.logical_type()
        }
        _ => unreachable!("numeric fragment eligibility checks the property type"),
    };
    let mut slots = vec![SlotDescriptor {
        id: PREDICATE_VALUE_SLOT,
        name: fragment.property.to_string(),
        slot_type: SlotType::logical(logical_type),
    }];
    if needs_node_ids {
        slots.push(SlotDescriptor {
            id: NODE_ID_SLOT,
            name: "__node_id".to_string(),
            slot_type: SlotType::NodeId,
        });
    }
    Ok(Arc::new(BindingSchema::try_new(slots)?))
}

struct PreparedNumericBatch {
    input_rows: usize,
    selected_rows: usize,
    output: BindingBatch,
}

#[derive(Debug)]
struct PreparedColumnarBatch {
    input_rows: usize,
    batch: ColumnarBatch,
}

#[derive(Debug)]
struct PreparedColumnarMorsel {
    batches: Vec<PreparedColumnarBatch>,
}

impl PreparedColumnarMorsel {
    fn resident_bytes(&self) -> usize {
        self.batches.iter().fold(0usize, |total, batch| {
            total.saturating_add(batch.batch.estimated_memory_bytes())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NumericMorselMemory {
    output_reservation_bytes: NonZeroUsize,
    worker_live_bytes: NonZeroUsize,
}

#[derive(Clone, Copy)]
struct NumericMorselPreparation<'plan, 'task> {
    fragment: NumericFragment<'plan>,
    scan: LendingNumericScan,
    output_budget_bytes: usize,
    columnar_schema: &'plan Arc<BindingSchema>,
    task_context: Option<&'task RuntimeTaskContext>,
}

#[allow(clippy::too_many_arguments)]
fn stream_parallel_borrowed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: skein_core::LabelId,
    morsel_rows: NonZeroUsize,
    lending_scan: LendingNumericScan,
    columnar_schema: Arc<BindingSchema>,
    morsel_memory: NumericMorselMemory,
    max_workers: usize,
    pool: SharedExecutorPool,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let requested_parallelism =
        NonZeroUsize::new(max_workers).expect("parallel execution has at least one worker");
    let bytes_per_worker = morsel_memory.worker_live_bytes;
    let scheduler = SharedPoolMorselScheduler::new(pool);
    let output_reservation_bytes = morsel_memory.output_reservation_bytes;
    let output_account_budget =
        NonZeroUsize::new(output_reservation_bytes.get().saturating_mul(max_workers))
            .expect("parallel morsel output budget is non-zero");
    let output_account = context.memory_ledger.account(
        crate::QueryMemoryClass::MorselOutput,
        "columnar morsel output",
        output_account_budget,
    );
    let wave_capacity = morsel_rows.get().saturating_mul(max_workers);
    let wave_bytes = wave_capacity.saturating_mul(std::mem::size_of::<&NodeRecord>());
    let wave_budget = NonZeroUsize::new(wave_bytes)
        .ok_or_else(|| SkeinError::Execution("parallel morsel wave has no capacity".to_string()))?;
    let wave_account = context.memory_ledger.account(
        crate::QueryMemoryClass::PipelineBatch,
        "columnar morsel input wave",
        wave_budget,
    );
    let _wave_lease = wave_account.reserve(wave_bytes)?;
    let mut nodes = context.store.scan_nodes_borrowed(Some(label_id));
    let mut wave = Vec::with_capacity(wave_capacity);
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let preparation = NumericMorselPreparation {
        fragment,
        scan: lending_scan,
        output_budget_bytes: output_reservation_bytes.get(),
        columnar_schema: &columnar_schema,
        task_context: context.task_context,
    };
    let mut stopped = false;
    loop {
        wave.clear();
        wave.extend(nodes.by_ref().take(wave.capacity()));
        if wave.is_empty() {
            break;
        }
        let admission = MorselAdmission::try_new(MorselAdmissionRequest {
            pipeline_id: PipelineId(0),
            input_rows: wave.len(),
            target_rows: morsel_rows,
            requested_parallelism,
            bytes_per_worker,
            memory_budget_bytes: context.memory.query_memory_bytes,
        })?;
        let stream_report = scheduler.execute_accounted_ordered(
            &admission,
            MorselStreamResources {
                task_context: context.task_context,
                output_account: &output_account,
                output_reservation_bytes,
            },
            |morsel| {
                let output = prepare_parallel_numeric_morsel(
                    preparation,
                    &wave[morsel.start_row..morsel.start_row + morsel.row_count],
                )?;
                let resident_bytes = output.resident_bytes();
                Ok(MorselOutput::new(output, resident_bytes))
            },
            |_morsel, output| {
                context.observer.record_morsels(1);
                for batch in output.batches {
                    stopped = batch_emitter.emit_columnar(batch)? == BatchControl::Stop;
                    if stopped || batch_emitter.limit_reached() {
                        stopped = true;
                        break;
                    }
                }
                Ok(if stopped {
                    MorselStreamControl::Stop
                } else {
                    MorselStreamControl::Continue
                })
            },
        )?;
        context.observer.record_morsel_buffering(
            stream_report.peak_buffered_outputs,
            stream_report.peak_buffered_output_bytes,
            stream_report.peak_reorder_entries,
        );
        if stopped || wave.len() < wave.capacity() {
            break;
        }
    }
    Ok((batch_emitter.emitted, stopped))
}

fn stream_lending_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: skein_core::LabelId,
    scan: LendingNumericScan,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut cursor = NumericNodeBatchCursor::new(
        context.store.scan_nodes_borrowed(Some(label_id)),
        fragment,
        scan.batch_rows,
        scan.needs_node_ids,
    );
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    while let Some(batch) = cursor.next_batch()? {
        stopped = batch_emitter.emit_typed(batch)? == BatchControl::Stop;
        if stopped || batch_emitter.limit_reached() {
            stopped = true;
            break;
        }
    }
    Ok((batch_emitter.emitted, stopped))
}

fn stream_borrowed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: skein_core::LabelId,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut nodes = Vec::with_capacity(context.memory.batch_rows.get());
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    for node in context.store.scan_nodes_borrowed(Some(label_id)) {
        nodes.push(node);
        if nodes.len() == context.memory.batch_rows.get() {
            stopped = batch_emitter.emit_nodes(&nodes)? == BatchControl::Stop;
            nodes.clear();
        }
        if stopped || batch_emitter.limit_reached() {
            stopped = true;
            break;
        }
    }
    if !stopped && !nodes.is_empty() {
        stopped = batch_emitter.emit_nodes(&nodes)? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

pub fn stream_owned_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: skein_core::LabelId,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut nodes = Vec::with_capacity(context.memory.batch_rows.get());
    let mut buffered_bytes = 0usize;
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    {
        let mut consume = |node: NodeRecord| {
            if stopped {
                return Ok(ScanControl::Stop);
            }
            let node_bytes = crate::binding::node_memory_bytes(&node);
            if !nodes.is_empty()
                && buffered_bytes.saturating_add(node_bytes)
                    > context.memory.batch_payload_bytes.get()
            {
                match batch_emitter.emit_owned(&mut nodes)? {
                    BatchControl::Continue => buffered_bytes = 0,
                    BatchControl::Stop => {
                        stopped = true;
                        return Ok(ScanControl::Stop);
                    }
                }
            }
            buffered_bytes = buffered_bytes.saturating_add(node_bytes);
            nodes.push(node);
            if nodes.len() == context.memory.batch_rows.get()
                || buffered_bytes >= context.memory.batch_payload_bytes.get()
            {
                match batch_emitter.emit_owned(&mut nodes)? {
                    BatchControl::Continue => buffered_bytes = 0,
                    BatchControl::Stop => {
                        stopped = true;
                        return Ok(ScanControl::Stop);
                    }
                }
            }
            if batch_emitter.limit_reached() {
                stopped = true;
                Ok(ScanControl::Stop)
            } else {
                Ok(ScanControl::Continue)
            }
        };
        context
            .store
            .visit_nodes_owned(Some(label_id), &mut consume)?;
    }
    if !stopped && !nodes.is_empty() {
        stopped = batch_emitter.emit_owned(&mut nodes)? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

#[allow(clippy::too_many_arguments)]
pub fn stream_owned_typed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: skein_core::LabelId,
    scan: LendingNumericScan,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut buffer = OwnedNumericBatchBuffer::new(fragment, scan.batch_rows, scan.needs_node_ids);
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    {
        let mut consume = |node: NodeRecord| {
            if stopped {
                return Ok(ScanControl::Stop);
            }
            buffer.push_owned(node)?;
            if buffer.is_full() {
                match batch_emitter.emit_typed(buffer.take_batch())? {
                    BatchControl::Continue => buffer.clear(),
                    BatchControl::Stop => {
                        stopped = true;
                        return Ok(ScanControl::Stop);
                    }
                }
            }
            if batch_emitter.limit_reached() {
                stopped = true;
                Ok(ScanControl::Stop)
            } else {
                Ok(ScanControl::Continue)
            }
        };
        context
            .store
            .visit_nodes_owned(Some(label_id), &mut consume)?;
    }
    if !stopped && !buffer.is_empty() {
        stopped = batch_emitter.emit_typed(buffer.take_batch())? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

impl<'plan, 'task, 'observer, 'emit> NumericBatchEmitter<'plan, 'task, 'observer, 'emit> {
    fn new(
        fragment: NumericFragment<'plan>,
        items: &'plan [Projection],
        execution_limit: ExecutionLimit,
        task_context: Option<&'task RuntimeTaskContext>,
        observer: &'observer QueryExecutionObserver,
        emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Self {
        Self {
            fragment,
            items,
            emitted: 0,
            execution_limit,
            task_context,
            observer,
            emit,
            selected_rows: Vec::new(),
        }
    }

    fn limit_reached(&self) -> bool {
        self.execution_limit.is_reached(self.emitted)
    }

    fn emit_owned(&mut self, nodes: &mut Vec<NodeRecord>) -> Result<BatchControl> {
        let control = self.emit_nodes(nodes)?;
        nodes.clear();
        Ok(control)
    }

    fn emit_typed(&mut self, input: NumericNodeBatch<'_>) -> Result<BatchControl> {
        runtime_checkpoint(self.task_context)?;
        self.observer.record_morsels(1);
        let prepared =
            prepare_typed_batch(self.fragment, self.items, input, &mut self.selected_rows)?;
        self.emit_prepared(prepared)
    }

    fn emit_nodes<N: Borrow<NodeRecord>>(&mut self, input: &[N]) -> Result<BatchControl> {
        self.observer.record_morsels(1);
        self.emit_nodes_without_morsel(input)
    }

    fn emit_nodes_without_morsel<N: Borrow<NodeRecord>>(
        &mut self,
        input: &[N],
    ) -> Result<BatchControl> {
        let prepared = prepare_numeric_batch(self.fragment, self.items, input, self.task_context)?;
        self.emit_prepared(prepared)
    }

    fn emit_prepared(&mut self, mut prepared: PreparedNumericBatch) -> Result<BatchControl> {
        self.fragment.record_fused_cardinality(
            self.observer,
            prepared.input_rows,
            prepared.selected_rows,
        );
        self.observer
            .record_columnar_batch(prepared.input_rows, prepared.selected_rows);
        let remaining = self
            .execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(self.emitted);
        if prepared.output.len() > remaining {
            prepared.output.truncate(remaining);
        }
        self.emit_output(prepared.output)
    }

    fn emit_columnar(&mut self, prepared: PreparedColumnarBatch) -> Result<BatchControl> {
        self.fragment.record_fused_cardinality(
            self.observer,
            prepared.input_rows,
            prepared.batch.selected_count(),
        );
        self.observer
            .record_columnar_batch(prepared.input_rows, prepared.batch.selected_count());
        let remaining = self
            .execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(self.emitted);
        let batch = prepared.batch.limit(0, remaining);
        let property = batch
            .column(PREDICATE_VALUE_SLOT)
            .expect("prepared columnar batch retains its predicate column");
        let node_ids = batch.column(NODE_ID_SLOT);
        let mut output = Vec::with_capacity(batch.selected_count());
        for row in batch.selection().iter() {
            let mut values = BTreeMap::new();
            for item in self.items {
                let value = match &item.expression {
                    ProjectionExpression::Id { .. } => node_ids
                        .expect("typed scan retains requested node ids")
                        .value(row)
                        .expect("node id columns are non-null"),
                    ProjectionExpression::Property { .. } => {
                        property.value(row).unwrap_or(Value::Null)
                    }
                    ProjectionExpression::Literal(value) => value.clone(),
                    _ => unreachable!("typed projection eligibility checks expressions"),
                };
                insert_projected_value(&mut values, &item.name, value);
            }
            output.push(Binding::values(values));
        }
        self.emit_output(output)
    }

    fn emit_output(&mut self, output: BindingBatch) -> Result<BatchControl> {
        self.emitted = self.emitted.saturating_add(output.len());
        runtime_checkpoint(self.task_context)?;
        if !output.is_empty() && (self.emit)(output)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if self.limit_reached() {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    }
}

fn prepare_typed_batch(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: NumericNodeBatch<'_>,
    selected_rows: &mut Vec<u32>,
) -> Result<PreparedNumericBatch> {
    match input.values {
        lending::NumericBatchValues::Int(values) => {
            fragment.select_int64_values(values, input.validity, selected_rows)?
        }
        lending::NumericBatchValues::Float(values) => {
            fragment.select_float64_values(values, input.validity, selected_rows)?
        }
    }
    let selected_count = selected_rows.len();
    let mut output = Vec::with_capacity(selected_count);
    for row in selected_rows.iter().copied().map(|row| row as usize) {
        let mut values = BTreeMap::new();
        for item in items {
            let value = match &item.expression {
                ProjectionExpression::Id { .. } => Value::Int(
                    input
                        .node_ids
                        .expect("typed scan retains requested node ids")[row]
                        as i64,
                ),
                ProjectionExpression::Property { .. } => input.values.value(row),
                ProjectionExpression::Literal(value) => value.clone(),
                _ => unreachable!("typed projection eligibility checks expressions"),
            };
            insert_projected_value(&mut values, &item.name, value);
        }
        output.push(Binding::values(values));
    }
    Ok(PreparedNumericBatch {
        input_rows: input.input_rows,
        selected_rows: selected_count,
        output,
    })
}

fn prepare_numeric_batch<N: Borrow<NodeRecord>>(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: &[N],
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedNumericBatch> {
    runtime_checkpoint(task_context)?;
    let mut validity = ValidityBuilder::with_capacity(input.len());
    let selection = match fragment.property_type {
        skein_core::PropertyType::Int => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                let node = node.borrow();
                match node.properties.get(fragment.property) {
                    Some(Value::Int(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            fragment.filter_int64_values(
                &values,
                &validity.finish(),
                &Selection::all(input.len()),
            )?
        }
        skein_core::PropertyType::Float => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                let node = node.borrow();
                match node.properties.get(fragment.property) {
                    Some(Value::Float(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0.0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            fragment.filter_float64_values(
                &values,
                &validity.finish(),
                &Selection::all(input.len()),
            )?
        }
        _ => unreachable!("numeric fragment eligibility checks the property type"),
    };
    runtime_checkpoint(task_context)?;
    let selected_rows = selection.selected_count();
    let mut output = Vec::with_capacity(selected_rows);
    for row in selection.iter() {
        let node = input[row].borrow();
        let mut values = BTreeMap::new();
        for item in items {
            let value = match &item.expression {
                ProjectionExpression::Id { .. } => Value::Int(node.id.0 as i64),
                ProjectionExpression::Property { property, .. } => node
                    .properties
                    .get(property)
                    .cloned()
                    .unwrap_or(Value::Null),
                ProjectionExpression::Literal(value) => value.clone(),
                _ => unreachable!("columnar projection eligibility checks expressions"),
            };
            insert_projected_value(&mut values, &item.name, value);
        }
        output.push(Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        });
    }
    Ok(PreparedNumericBatch {
        input_rows: input.len(),
        selected_rows,
        output,
    })
}

fn prepare_owned_columnar_batch(
    fragment: NumericFragment<'_>,
    input: &[&NodeRecord],
    needs_node_ids: bool,
    schema: Arc<BindingSchema>,
) -> Result<PreparedColumnarBatch> {
    let mut validity = ValidityBuilder::with_capacity(input.len());
    let mut node_ids = needs_node_ids.then(|| Vec::with_capacity(input.len()));
    let property = match fragment.property_type {
        skein_core::PropertyType::Int => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                if let Some(node_ids) = &mut node_ids {
                    node_ids.push(node.id.0);
                }
                match node.properties.get(fragment.property) {
                    Some(Value::Int(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            Arc::new(ColumnVector::int64(values, validity.finish())?)
        }
        skein_core::PropertyType::Float => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                if let Some(node_ids) = &mut node_ids {
                    node_ids.push(node.id.0);
                }
                match node.properties.get(fragment.property) {
                    Some(Value::Float(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0.0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            Arc::new(ColumnVector::float64(values, validity.finish())?)
        }
        _ => unreachable!("numeric fragment eligibility checks the property type"),
    };
    let mut columns = vec![property];
    if let Some(node_ids) = node_ids {
        columns.push(Arc::new(ColumnVector::node_ids(node_ids)));
    }
    let batch = fragment.filter_batch(ColumnarBatch::try_new(schema, columns)?)?;
    Ok(PreparedColumnarBatch {
        input_rows: input.len(),
        batch,
    })
}

fn prepare_parallel_numeric_morsel(
    preparation: NumericMorselPreparation<'_, '_>,
    input: &[&NodeRecord],
) -> Result<PreparedColumnarMorsel> {
    prepare_lending_numeric_morsel(
        preparation.fragment,
        input,
        preparation.scan,
        preparation.output_budget_bytes,
        preparation.columnar_schema,
        preparation.task_context,
    )
}

fn prepare_lending_numeric_morsel(
    fragment: NumericFragment<'_>,
    input: &[&NodeRecord],
    scan: LendingNumericScan,
    output_budget_bytes: usize,
    schema: &Arc<BindingSchema>,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedColumnarMorsel> {
    if estimated_numeric_columnar_morsel_bytes(
        input.len(),
        scan.batch_rows,
        scan.needs_node_ids,
        schema,
    ) > output_budget_bytes
    {
        return Err(SkeinError::Execution(format!(
            "columnar morsel cannot fit one typed batch within its {output_budget_bytes}-byte reservation"
        )));
    }
    let max_morsel_rows = scan.batch_rows.saturating_mul(DEFAULT_BATCHES_PER_MORSEL);
    if input.len() > max_morsel_rows {
        return Err(SkeinError::Execution(format!(
            "columnar morsel has {} rows, exceeding its {max_morsel_rows}-row typed batch window",
            input.len(),
        )));
    }
    if input.is_empty() {
        return Err(SkeinError::Execution(
            "columnar morsel input is empty".to_string(),
        ));
    }
    let mut batches = Vec::with_capacity(input.len().div_ceil(scan.batch_rows));
    let mut output_bytes = 0usize;
    for rows in input.chunks(scan.batch_rows) {
        runtime_checkpoint(task_context)?;
        let batch =
            prepare_owned_columnar_batch(fragment, rows, scan.needs_node_ids, Arc::clone(schema))?;
        output_bytes = output_bytes.saturating_add(batch.batch.estimated_memory_bytes());
        if output_bytes > output_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "columnar morsel retained {output_bytes} bytes after admission reserved {output_budget_bytes}"
            )));
        }
        batches.push(batch);
    }
    Ok(PreparedColumnarMorsel { batches })
}

fn estimated_numeric_columnar_morsel_bytes(
    rows: usize,
    batch_rows: usize,
    needs_node_ids: bool,
    schema: &BindingSchema,
) -> usize {
    let schema_bytes = schema.slots().iter().fold(
        schema.len() * std::mem::size_of::<SlotDescriptor>(),
        |total, slot| total.saturating_add(slot.name.len()),
    );
    let mut remaining = rows;
    let mut total = 0usize;
    while remaining > 0 {
        let rows = remaining.min(batch_rows);
        let validity_bytes = rows
            .div_ceil(u64::BITS as usize)
            .saturating_mul(std::mem::size_of::<u64>());
        let selection_bytes = rows.saturating_mul(std::mem::size_of::<u32>());
        let column_bytes = rows
            .saturating_mul(std::mem::size_of::<f64>())
            .saturating_add(
                usize::from(needs_node_ids)
                    .saturating_mul(rows)
                    .saturating_mul(std::mem::size_of::<u64>()),
            );
        total = total
            .saturating_add(schema_bytes)
            .saturating_add(validity_bytes)
            .saturating_add(selection_bytes)
            .saturating_add(column_bytes);
        remaining -= rows;
    }
    total
}

fn numeric_morsel_memory(
    rows: usize,
    batch_rows: usize,
    needs_node_ids: bool,
    schema: &BindingSchema,
) -> NumericMorselMemory {
    let output_reservation_bytes = NonZeroUsize::new(estimated_numeric_columnar_morsel_bytes(
        rows,
        batch_rows,
        needs_node_ids,
        schema,
    ))
    .expect("non-empty typed morsel has a non-zero output reservation");
    let input_reference_bytes = rows.saturating_mul(std::mem::size_of::<&NodeRecord>());
    let worker_live_bytes = NonZeroUsize::new(
        output_reservation_bytes
            .get()
            .saturating_add(input_reference_bytes),
    )
    .expect("typed morsel live memory is non-zero");
    NumericMorselMemory {
        output_reservation_bytes,
        worker_live_bytes,
    }
}

fn schema_value_mismatch(fragment: NumericFragment<'_>, value: &Value) -> SkeinError {
    SkeinError::Execution(format!(
        "columnar scan found value {value:?} that violates {:?} schema for {}.{}",
        fragment.property_type, fragment.label, fragment.property
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        default_morsel_cpu_ceiling, default_morsel_worker_count, numeric_columnar_schema,
        numeric_morsel_memory, prepare_lending_numeric_morsel, LendingNumericScan, NumericFragment,
        NumericPredicate, DEFAULT_BATCHES_PER_MORSEL, DEFAULT_MIN_MORSELS_PER_WORKER,
    };
    use crate::morsel::{MorselAdmission, MorselAdmissionRequest, PipelineId};
    use crate::{ExecutionMemoryConfig, NumericLiteral};
    use skein_core::PropertyType;
    use skein_core::Value;
    use skein_plan::ComparisonOp;
    use skein_plan::{Projection, ProjectionExpression};
    use skein_storage::{NodeId, NodeRecord};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;

    #[test]
    fn default_morsel_cpu_ceiling_scales_with_effective_cpu_capacity() {
        assert_eq!(default_morsel_cpu_ceiling(1), 1);
        assert_eq!(default_morsel_cpu_ceiling(2), 2);
        assert_eq!(default_morsel_cpu_ceiling(4), 4);
        assert_eq!(default_morsel_cpu_ceiling(8), 4);
        assert_eq!(default_morsel_cpu_ceiling(16), 4);
        assert_eq!(default_morsel_cpu_ceiling(17), 5);
        assert_eq!(default_morsel_cpu_ceiling(32), 8);
        assert_eq!(default_morsel_cpu_ceiling(64), 16);
        assert_eq!(default_morsel_cpu_ceiling(128), 16);
    }

    #[test]
    fn default_worker_count_requires_enough_work_per_worker() {
        assert_eq!(default_morsel_worker_count(0, 4), 1);
        assert_eq!(default_morsel_worker_count(4, 4), 1);
        assert_eq!(default_morsel_worker_count(8, 4), 2);
        assert_eq!(default_morsel_worker_count(15, 4), 3);
        assert_eq!(default_morsel_worker_count(16, 4), 4);
        assert_eq!(default_morsel_worker_count(32, 16), 8);
        assert_eq!(default_morsel_worker_count(64, 16), 16);
        assert_eq!(default_morsel_worker_count(128, 16), 16);
        assert_eq!(default_morsel_worker_count(64, 2), 2);
    }

    #[test]
    fn typed_morsel_memory_admits_default_parallelism_without_phantom_batch_payloads() {
        let memory = ExecutionMemoryConfig::default();
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: PropertyType::Int,
            predicate: NumericPredicate::Compare(ComparisonOp::Gte),
            expected: NumericLiteral::Int(0),
            fused_operators: None,
        };
        let schema = numeric_columnar_schema(fragment, true).unwrap();
        let morsel_rows = memory
            .batch_rows
            .get()
            .saturating_mul(DEFAULT_BATCHES_PER_MORSEL);
        let morsel_memory =
            numeric_morsel_memory(morsel_rows, memory.batch_rows.get(), true, &schema);

        assert!(
            morsel_memory
                .output_reservation_bytes
                .get()
                .saturating_mul(100)
                < memory.batch_payload_bytes.get()
        );
        assert_eq!(
            morsel_memory.worker_live_bytes.get(),
            morsel_memory
                .output_reservation_bytes
                .get()
                .saturating_add(morsel_rows * std::mem::size_of::<&NodeRecord>())
        );

        let morsel_count =
            super::MAX_MORSEL_PARALLELISM.saturating_mul(DEFAULT_MIN_MORSELS_PER_WORKER);
        let requested_parallelism =
            default_morsel_worker_count(morsel_count, super::MAX_MORSEL_PARALLELISM);
        let admission = MorselAdmission::try_new(MorselAdmissionRequest {
            pipeline_id: PipelineId(0),
            input_rows: morsel_rows.saturating_mul(morsel_count),
            target_rows: NonZeroUsize::new(morsel_rows).unwrap(),
            requested_parallelism: NonZeroUsize::new(requested_parallelism).unwrap(),
            bytes_per_worker: morsel_memory.worker_live_bytes,
            memory_budget_bytes: memory.query_memory_bytes,
        })
        .unwrap();

        assert_eq!(admission.max_workers(), super::MAX_MORSEL_PARALLELISM);
    }

    #[test]
    fn parallel_morsels_require_a_fully_typed_projection() {
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: PropertyType::Int,
            predicate: NumericPredicate::Compare(ComparisonOp::Gte),
            expected: NumericLiteral::Int(0),
            fused_operators: None,
        };
        let typed = vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        }];
        let wide = vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "name".to_string(),
            },
            name: "name".to_string(),
        }];

        assert!(fragment.supports_lending_projection(&typed));
        assert!(!fragment.supports_lending_projection(&wide));
    }

    #[test]
    fn parallel_lending_morsel_retains_columnar_batches() {
        let nodes = (0..8)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("score".to_string(), Value::Int(id as i64))]),
            })
            .collect::<Vec<_>>();
        let rows = nodes.iter().collect::<Vec<_>>();
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: PropertyType::Int,
            predicate: NumericPredicate::Compare(ComparisonOp::Gte),
            expected: NumericLiteral::Int(4),
            fused_operators: None,
        };
        let schema = numeric_columnar_schema(fragment, true).unwrap();

        let prepared = prepare_lending_numeric_morsel(
            fragment,
            &rows,
            LendingNumericScan {
                batch_rows: 4,
                needs_node_ids: true,
            },
            4096,
            &schema,
            None,
        )
        .unwrap();

        assert_eq!(prepared.batches.len(), 2);
        assert_eq!(prepared.batches[0].input_rows, 4);
        assert_eq!(prepared.batches[0].batch.selected_count(), 0);
        assert_eq!(prepared.batches[1].input_rows, 4);
        assert_eq!(prepared.batches[1].batch.selected_count(), 4);
    }

    #[test]
    fn columnar_morsel_rejects_before_decoding_when_reservation_cannot_fit() {
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([(
                "score".to_string(),
                Value::String("invalid".to_string()),
            )]),
        };
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: PropertyType::Int,
            predicate: NumericPredicate::Compare(ComparisonOp::Gte),
            expected: NumericLiteral::Int(0),
            fused_operators: None,
        };
        let schema = numeric_columnar_schema(fragment, true).unwrap();

        let error = prepare_lending_numeric_morsel(
            fragment,
            &[&node],
            LendingNumericScan {
                batch_rows: 1,
                needs_node_ids: true,
            },
            1,
            &schema,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("cannot fit one typed batch"));
    }
}
