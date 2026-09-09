//! Streaming batch orchestration and pipeline dispatch.

use super::*;
use skein_executor::observer::ExecutionObserver;
use skein_storage::{ScanPruningStrategy, ScanPruningTargetKind};
use std::cell::Cell;

fn charge_graph_algorithm_memory(
    algorithm: &'static str,
    phase: &'static str,
    tracker: &mut OperatorMemoryTracker,
    bytes: usize,
) -> Result<()> {
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "GraphAlgorithm {algorithm} {phase} requires {} tracked bytes, exceeding blocking_operator_bytes {}",
            tracker.used_bytes.saturating_add(bytes),
            tracker.budget_bytes,
        )));
    }
    tracker.try_charge(bytes)?;
    Ok(())
}

fn estimated_vec_memory_bytes<T>(item_count: usize) -> usize {
    item_count
        .saturating_mul(std::mem::size_of::<T>())
        .saturating_mul(2)
}

fn graph_algorithm_memory_report(
    tracker: &OperatorMemoryTracker,
    input_rows: usize,
    memory: &ExecutionMemoryConfig,
) -> skein_executor::BlockingOperatorMemoryReport {
    skein_executor::BlockingOperatorMemoryReport {
        operator: "GraphAlgorithm".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows,
        max_spill_bytes: memory.max_spill_bytes.get(),
        max_spill_runs: memory.max_spill_runs.get(),
        spilled_bytes: 0,
        spill_run_count: 0,
        spilled_rows: 0,
    }
}

fn stream_node_column_lookup_batches(
    spec: NodeColumnLookupSpec<'_>,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut output = Vec::with_capacity(context.memory.batch_rows.get());
    let mut emitted = 0usize;
    execute_prepared_binding_batches(
        BatchPlanRef::descendant(input),
        context,
        ExecutionLimit::unlimited(),
        &mut |batch| {
            let remaining = execution_limit
                .output_rows
                .unwrap_or(usize::MAX)
                .saturating_sub(emitted);
            if remaining == 0 {
                return Ok(BatchControl::Stop);
            }
            let bindings = execute_node_column_lookup(
                spec,
                batch,
                context,
                ExecutionLimit {
                    output_rows: Some(remaining),
                },
            )?;
            for binding in bindings {
                output.push(binding);
                emitted = emitted.saturating_add(1);
                if output.len() == context.memory.batch_rows.get()
                    && emit(std::mem::replace(
                        &mut output,
                        Vec::with_capacity(context.memory.batch_rows.get()),
                    ))? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
                if execution_limit.is_reached(emitted) {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        },
    )?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[derive(Clone, Copy)]
struct OptionalDegreeSpec<'a> {
    source_variable: &'a str,
    rel_type: &'a str,
    rel_properties: &'a BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_label: &'a str,
    target_properties: &'a BTreeMap<String, Value>,
    alias: &'a str,
    input: &'a PhysicalPlan,
}

impl OptionalDegreeSpec<'_> {
    fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } = self;
        let rel_type_id = if rel_type.is_empty() {
            None
        } else {
            context.catalog.rel_type_id(rel_type)
        };
        let target_label_ids = label_ids_for_pattern(context.catalog, target_label);
        let adjacency_account = context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "OptionalDegreeExec adjacency",
            context.memory.blocking_operator_bytes,
        );
        let mut emitted = 0usize;
        execute_prepared_binding_batches(
            BatchPlanRef::descendant(input),
            context,
            execution_limit,
            &mut |batch| {
                let mut output = Vec::with_capacity(batch.len());
                for mut binding in batch {
                    let degree = if !rel_type.is_empty() && rel_type_id.is_none() {
                        0
                    } else {
                        let source = binding.nodes.get(source_variable).ok_or_else(|| {
                            SkeinError::Execution(format!(
                                "missing variable '{source_variable}' during optional degree"
                            ))
                        })?;
                        let mut degree = 0usize;
                        skein_executor::traversal::visit_one_hop_relationships_with_budget(
                            context.store,
                            skein_executor::traversal::OneHopRelationshipSpec {
                                source: source.id,
                                rel_type_id,
                                target_label_ids: target_label_ids.as_deref(),
                                rel_properties,
                                relationship_scan_filter: None,
                                direction,
                            },
                            skein_executor::store::AdjacencyReadMemory {
                                budget_bytes: context.memory.blocking_operator_bytes.get(),
                                account: Some(&adjacency_account),
                            },
                            context.observer,
                            &mut |_, target| {
                                if node_properties_match(&target, target_properties) {
                                    degree = degree.saturating_add(1);
                                }
                                Ok(skein_executor::store::ScanControl::Continue)
                            },
                        )?;
                        degree
                    };
                    binding
                        .values
                        .insert(alias.to_string(), Value::Int(degree as i64));
                    output.push(binding);
                }
                emitted = emitted.saturating_add(output.len());
                if !output.is_empty() && emit(output)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                Ok(if execution_limit.is_reached(emitted) {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            },
        )
    }
}

#[derive(Clone, Copy)]
pub(super) struct BatchReadContext<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a dyn skein_executor::store::GraphExecutionRead,
    pub(super) parameters: &'a BTreeMap<String, Value>,
    pub(super) external: &'a dyn BatchExternalRead,
    pub(super) memory: &'a ExecutionMemoryConfig,
    pub(super) memory_ledger: &'a QueryMemoryLedger,
    pub(super) task_context: Option<&'a RuntimeTaskContext>,
    pub(super) observer: &'a QueryExecutionObserver,
}

#[derive(Clone, Copy)]
pub(super) struct BatchPlanRef<'a>(&'a PhysicalPlan);

impl<'a> BatchPlanRef<'a> {
    pub(super) fn try_new(plan: &'a PhysicalPlan) -> Option<Self> {
        let locally_supported = match plan.class() {
            PhysicalPlanClass::Access
            | PhysicalPlanClass::Traversal
            | PhysicalPlanClass::Relational => true,
            PhysicalPlanClass::Procedure => !matches!(plan, PhysicalPlan::ProjectGraph { .. }),
            PhysicalPlanClass::Schema | PhysicalPlanClass::Mutation => false,
        };
        if !locally_supported {
            return None;
        }
        match plan.children() {
            PlanChildren::None => {}
            PlanChildren::Unary(input) => {
                Self::try_new(input)?;
            }
            PlanChildren::Binary(left, right) => {
                Self::try_new(left)?;
                Self::try_new(right)?;
            }
        }
        Some(Self(plan))
    }

    fn descendant(plan: &'a PhysicalPlan) -> Self {
        debug_assert!(Self::try_new(plan).is_some());
        Self(plan)
    }

    pub(super) fn plan(self) -> &'a PhysicalPlan {
        self.0
    }
}

pub(super) fn collect_batch_pipeline(
    plan: BatchPlanRef<'_>,
    catalog: &Catalog,
    store: &dyn skein_executor::store::GraphExecutionRead,
    execution_context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    let memory = execution_context.memory;
    let task_context = execution_context.task_context;
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        execution_context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "materialized batch pipeline",
            memory.blocking_operator_bytes,
        ),
    );
    let external = BatchExternalReadAdapter::new(&mut *execution_context.external);
    let context = BatchReadContext {
        catalog,
        store,
        parameters: execution_context.parameters,
        external: &external,
        memory,
        memory_ledger: execution_context.memory_ledger,
        task_context,
        observer: execution_context.observer,
    };
    execute_prepared_binding_batches(plan, context, execution_limit, &mut |batch| {
        for binding in batch {
            push_bounded_operator_binding(
                "MaterializedBatchPipeline",
                &mut output,
                binding,
                &mut tracker,
            )?;
            if execution_limit.is_reached(output.len()) {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    Ok(output)
}

pub(super) fn execute_binding_batches(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let prepared = PreparedPhysicalPlan::prepare(plan, context.store, context.memory);
    let plan = prepared.batch().ok_or_else(|| {
        SkeinError::Execution(format!(
            "physical operator '{}' does not support batch execution",
            prepared.plan().kind().as_str()
        ))
    })?;
    execute_prepared_binding_batches(plan, context, execution_limit, emit)
}

pub(super) fn execute_prepared_binding_batches(
    plan: BatchPlanRef<'_>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    if execution_limit.output_rows == Some(0) {
        return Ok(BatchControl::Continue);
    }
    let operator = plan.plan();
    context.observer.record_operator_start(operator);
    let pipeline_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        format!("{} pipeline", plan.plan().kind().as_str()),
        context.memory.batch_payload_bytes,
    );
    let mut measured_emit = |batch: BindingBatch| {
        runtime_checkpoint(context.task_context)?;
        context
            .observer
            .record_operator_output(operator, batch.len());
        let control = emit_byte_bounded_batches(
            batch,
            context.memory.batch_payload_bytes.get(),
            &pipeline_account,
            context.observer,
            emit,
        )?;
        runtime_checkpoint(context.task_context)?;
        Ok(control)
    };
    execute_binding_batches_inner(plan, context, execution_limit, &mut measured_emit)
}

fn emit_byte_bounded_batches(
    batch: BindingBatch,
    max_payload_bytes: usize,
    memory_account: &skein_executor::QueryMemoryAccount,
    observer: &QueryExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut batch_bytes = 0usize;
    let mut requires_split = false;
    for binding in &batch {
        let binding_bytes = binding_memory_bytes(binding);
        if binding_bytes > max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {max_payload_bytes}"
            )));
        }
        batch_bytes = batch_bytes.saturating_add(binding_bytes);
        requires_split |= batch_bytes > max_payload_bytes;
    }
    if !requires_split {
        if !batch.is_empty() {
            let _batch_lease = memory_account.reserve(batch_bytes)?;
            observer.record_pipeline_batch(&batch);
            return emit(batch);
        }
        return Ok(BatchControl::Continue);
    }

    let mut bounded = Vec::with_capacity(batch.len());
    let mut bounded_bytes = 0usize;
    for binding in batch {
        let binding_bytes = binding_memory_bytes(&binding);
        if binding_bytes > max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {max_payload_bytes}"
            )));
        }
        if !bounded.is_empty() && bounded_bytes.saturating_add(binding_bytes) > max_payload_bytes {
            let _batch_lease = memory_account.reserve(bounded_bytes)?;
            observer.record_pipeline_batch(&bounded);
            if emit(std::mem::take(&mut bounded))? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            bounded_bytes = 0;
        }
        bounded_bytes = bounded_bytes.saturating_add(binding_bytes);
        bounded.push(binding);
    }
    if !bounded.is_empty() {
        let _batch_lease = memory_account.reserve(bounded_bytes)?;
        observer.record_pipeline_batch(&bounded);
        if emit(bounded)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    Ok(BatchControl::Continue)
}

fn execute_binding_batches_inner(
    plan: BatchPlanRef<'_>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let plan = plan.plan();
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    match plan {
        PhysicalPlan::EmptyExec => Ok(BatchControl::Continue),
        PhysicalPlan::SeqNodeScan { variable, label } => {
            stream_node_scan_batches(variable, label, None, context, execution_limit, emit)
        }
        PhysicalPlan::NodeProjectionScanExec {
            variable,
            label,
            access,
            required_properties,
            predicate,
            items,
        } => {
            if !items.is_empty()
                && access.is_label_scan()
                && let Some(result) = try_stream_columnar_node_projection_batches(
                    variable,
                    label,
                    predicate.as_ref(),
                    items,
                    context,
                    execution_limit,
                    emit,
                )
            {
                return result;
            }
            stream_node_projection_scan_batches(
                NodeProjectionScanSpec {
                    variable,
                    label,
                    access,
                    required_properties,
                    predicate: predicate.as_ref(),
                    items,
                },
                context,
                execution_limit,
                emit,
            )
        }
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => {
            stream_source_segment_scan_batches(variable, predicate, context, execution_limit, emit)
        }
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            std::slice::from_ref(value),
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            values,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeUnionSeek {
            variable,
            label,
            branches,
        } => stream_index_node_union_seek_batches(
            variable,
            label,
            branches,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
                store.visit_nodes_by_composite_property_owned(label_id, predicates, consumer)
            })
        }
        PhysicalPlan::IndexNodeCompositeRangeSeek {
            variable,
            label,
            seek,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
                store.visit_nodes_by_composite_range_owned(label_id, seek, consumer)
            })
        }
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
                store.visit_nodes_by_property_range_owned(
                    label_id,
                    property,
                    lower.as_ref(),
                    upper.as_ref(),
                    consumer,
                )
            })
        }
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
                store.visit_nodes_by_full_text_property_owned(label_id, property, query, consumer)
            })
        }
        PhysicalPlan::ShortestPathExec {
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
            ..
        } => {
            let source_visibility_filter = source_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let target_visibility_filter = target_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let bindings = execute_shortest_path(
                catalog,
                store,
                ShortestPathExecInput {
                    source_label,
                    source_id,
                    source_visibility_filter: source_visibility_filter.as_ref(),
                    path_node_visibility_filter: source_visibility_filter.as_ref(),
                    rel_type,
                    direction: *direction,
                    target_label,
                    target_id,
                    target_visibility_filter: target_visibility_filter.as_ref(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    returns,
                },
                execution_limit,
                TraversalExecutionContext {
                    memory: context.memory,
                    memory_ledger: context.memory_ledger,
                    task_context: context.task_context,
                    observer: context.observer,
                },
            )?;
            bindings.emit_batches(memory.batch_rows.get(), emit)
        }
        PhysicalPlan::ThreadRepairStatsExec {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => {
            let bindings = thread_repair_stats_rows(
                catalog,
                store,
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
                memory.blocking_operator_bytes,
                context.memory_ledger,
                context.observer,
                context.task_context,
            )?;
            bindings.emit_batches(memory.batch_rows.get(), emit)
        }
        PhysicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } => {
            let Some(definition) = store.projected_graph_definition(graph_name) else {
                return Err(SkeinError::Execution(format!(
                    "projected graph '{graph_name}' does not exist"
                )));
            };
            let node_visibility_filter = node_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let layout = match algorithm {
                GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
                GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
            };
            let budget = ProjectionMemoryBudget::new(memory.blocking_operator_bytes);
            let graph = if let Some(filter) = node_visibility_filter.as_ref() {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |node| node_matches_property_filter(node, filter),
                    layout,
                    budget,
                )
            } else {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |_| true,
                    layout,
                    budget,
                )
            }?;
            runtime_checkpoint(context.task_context)?;
            let mut tracker = OperatorMemoryTracker::with_account(
                memory.blocking_operator_bytes,
                context.memory_ledger.account(
                    QueryMemoryClass::BlockingState,
                    "GraphAlgorithm",
                    memory.blocking_operator_bytes,
                ),
            );
            let projection_bytes = graph.memory_estimate().estimated_bytes;
            charge_graph_algorithm_memory(
                match algorithm {
                    GraphAlgorithmKind::PageRank => "PageRank",
                    GraphAlgorithmKind::Louvain => "Louvain",
                },
                "projection",
                &mut tracker,
                projection_bytes,
            )?;
            let input_rows = graph.node_count();
            let output_limit = execution_limit.output_rows.unwrap_or(usize::MAX);
            let execution_result: Result<Vec<Binding>> = (|| {
                let mut bindings = Vec::new();
                match algorithm {
                    GraphAlgorithmKind::PageRank => {
                        let options = PageRankOptions {
                            iterations: options
                                .max_iterations
                                .unwrap_or_else(|| PageRankOptions::default().iterations),
                            damping: options
                                .damping
                                .unwrap_or_else(|| PageRankOptions::default().damping),
                        };
                        let estimate = graph.page_rank_memory_estimate();
                        charge_graph_algorithm_memory(
                            "PageRank",
                            "scratch and result state",
                            &mut tracker,
                            estimate.algorithm_peak_bytes,
                        )?;
                        let scores = graph.page_rank_with_context(options, context.task_context)?;
                        tracker.release(estimate.algorithm_peak_bytes);
                        let result_bytes = estimated_vec_memory_bytes::<
                            crate::analytics::PageRankScore,
                        >(scores.len());
                        charge_graph_algorithm_memory(
                            "PageRank",
                            "materialized result",
                            &mut tracker,
                            result_bytes,
                        )?;
                        for score in scores.into_iter().take(output_limit) {
                            push_bounded_operator_binding(
                                "GraphAlgorithm",
                                &mut bindings,
                                Binding {
                                    values: BTreeMap::from([
                                        ("node".to_string(), Value::Int(score.node.0 as i64)),
                                        (score_column.clone(), Value::Float(score.score)),
                                    ]),
                                    nodes: BTreeMap::new(),
                                    relationships: BTreeMap::new(),
                                },
                                &mut tracker,
                            )?;
                        }
                        tracker.release(result_bytes);
                    }
                    GraphAlgorithmKind::Louvain => {
                        let options = LouvainOptions {
                            max_iterations: options
                                .max_iterations
                                .unwrap_or_else(|| LouvainOptions::default().max_iterations),
                            max_levels: options
                                .max_levels
                                .unwrap_or_else(|| LouvainOptions::default().max_levels),
                        };
                        let estimate = graph.louvain_memory_estimate(options);
                        charge_graph_algorithm_memory(
                            "Louvain",
                            "scratch and result state",
                            &mut tracker,
                            estimate.algorithm_peak_bytes,
                        )?;
                        let assignments = graph.hierarchical_louvain_communities_with_context(
                            options,
                            context.task_context,
                        )?;
                        tracker.release(estimate.algorithm_peak_bytes);
                        let result_bytes = estimated_vec_memory_bytes::<
                            crate::analytics::HierarchicalCommunityAssignment,
                        >(assignments.len());
                        charge_graph_algorithm_memory(
                            "Louvain",
                            "materialized result",
                            &mut tracker,
                            result_bytes,
                        )?;
                        for assignment in assignments.into_iter().take(output_limit) {
                            push_bounded_operator_binding(
                                "GraphAlgorithm",
                                &mut bindings,
                                Binding {
                                    values: BTreeMap::from([
                                        ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                                        ("level".to_string(), Value::Int(assignment.level as i64)),
                                        (
                                            "louvain_id".to_string(),
                                            Value::Int(assignment.community.0 as i64),
                                        ),
                                    ]),
                                    nodes: BTreeMap::new(),
                                    relationships: BTreeMap::new(),
                                },
                                &mut tracker,
                            )?;
                        }
                        tracker.release(result_bytes);
                    }
                }
                Ok(bindings)
            })();
            context
                .observer
                .record_blocking_memory_report(graph_algorithm_memory_report(
                    &tracker, input_rows, memory,
                ));
            let bindings = execution_result?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::VectorSeedScan {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            resource_profile,
            vector_plan,
        } => {
            let max_rows = vector_plan_top_k(vector_plan)
                .ok_or_else(|| {
                    SkeinError::Execution("vector seed physical plan is missing TopK".to_string())
                })?
                .min(execution_limit.output_rows.unwrap_or(usize::MAX));
            if max_rows == 0 {
                return emit_owned_binding_batches(Vec::new(), memory.batch_rows.get(), emit);
            }
            let embedding =
                vector_embedding_parameter(context.parameters, embedding_parameter, vector_plan)?;
            let external_memory = external_read_memory_budget(*resource_profile, memory);
            let admitted_parallelism = context
                .task_context
                .map_or(1, |task_context| task_context.admitted_parallelism().get());
            let resources = ExternalReadResourceContract {
                priority: resource_profile.priority,
                max_parallelism: NonZeroUsize::new(
                    resource_profile
                        .max_parallelism
                        .max(1)
                        .min(admitted_parallelism),
                )
                .expect("resolved external read parallelism is non-zero"),
                max_working_memory_bytes: external_memory.max_working_bytes,
                result: ExternalReadResultBudget {
                    max_rows,
                    max_memory_bytes: external_memory.max_result_bytes,
                },
                task_context: context.task_context,
            };
            let external_account = context.memory_ledger.account(
                QueryMemoryClass::ExternalRead,
                "VectorSeedScan external read",
                NonZeroUsize::new(resources.reserved_memory_bytes())
                    .expect("external read reservation is non-zero"),
            );
            let _external_lease = external_account.reserve(resources.reserved_memory_bytes())?;
            resources.checkpoint()?;
            let output = context
                .external
                .execute_vector_seed(VectorSeedExecutionRequest {
                    embedding: &embedding,
                    metadata_filters,
                    vector_plan,
                    resources,
                })?;
            resources.checkpoint()?;
            output.validate_result_budget(resources.result)?;
            context.observer.record_vector_execution(output.report);
            let mut bindings = collect_bounded_operator_bindings_with_account(
                "VectorSeedScan",
                output.rows.into_iter().map(|row| {
                    let mut values = BTreeMap::from([
                        ("id".to_string(), Value::String(row.id)),
                        ("score".to_string(), Value::Float(row.score)),
                    ]);
                    if *output_external_id && let Some(external_id) = row.external_id {
                        values.insert("external_id".to_string(), Value::String(external_id));
                    }
                    Binding {
                        values,
                        nodes: BTreeMap::new(),
                        relationships: BTreeMap::new(),
                    }
                }),
                memory.blocking_operator_bytes,
                context.memory_ledger.account(
                    QueryMemoryClass::BlockingState,
                    "VectorSeedScan",
                    memory.blocking_operator_bytes,
                ),
            )?;
            bindings.truncate(execution_limit.output_rows.unwrap_or(usize::MAX));
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => stream_node_column_lookup_batches(
            NodeColumnLookupSpec {
                variable,
                label,
                property,
                column,
                optional: *optional,
            },
            input,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::OptionalDegreeExec {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => OptionalDegreeSpec {
            source_variable,
            rel_type,
            rel_properties,
            direction: *direction,
            target_label,
            target_properties,
            alias,
            input,
        }
        .stream(context, execution_limit, emit),
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            output,
            ..
        } => {
            let label_ids = label_ids_for_pattern(catalog, label);
            let count_account = context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "OptionalRelationshipCountSumExec",
                context.memory.blocking_operator_bytes,
            );
            let mut total = 0usize;
            let mut nodes_since_checkpoint = 0usize;
            store.visit_nodes_owned(None, &mut |node| {
                nodes_since_checkpoint += 1;
                if nodes_since_checkpoint == context.memory.batch_rows.get() {
                    nodes_since_checkpoint = 0;
                    runtime_checkpoint(context.task_context)?;
                }
                if !node_matches_label_pattern(&node, label_ids.as_deref())
                    || !node_properties_match(&node, properties)
                {
                    return Ok(ScanControl::Continue);
                }
                for leg in legs {
                    let count = relationship_count_sum_leg(
                        catalog,
                        store,
                        node.id,
                        leg,
                        skein_executor::store::AdjacencyReadMemory {
                            budget_bytes: context.memory.blocking_operator_bytes.get(),
                            account: Some(&count_account),
                        },
                        context.observer,
                        context.task_context,
                    )?;
                    total = total.saturating_add(count);
                }
                Ok(ScanControl::Continue)
            })?;
            emit(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(total as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::NodeCountExec { label, output } => {
            let label_id = (!label.is_empty())
                .then(|| catalog.label_id(label))
                .flatten();
            let count = if label.is_empty() {
                store.node_count_for_label(None)
            } else if let Some(label_id) = label_id {
                store.node_count_for_label(Some(label_id))
            } else {
                0
            };
            context
                .observer
                .record_scan_pruning_report(ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Node,
                    label_id,
                    rel_type_id: None,
                    strategy: ScanPruningStrategy::ExactCount,
                    pruned: true,
                    exact_empty: count == 0,
                    candidate_count_before_pruning: count,
                    pruned_candidate_count: count,
                    candidate_count_before_filter: 0,
                    output_count: 1,
                    filtered_out_count: 0,
                });
            let count = i64::try_from(count).map_err(|_| {
                SkeinError::Execution(format!(
                    "node count for label '{label}' exceeds the supported i64 result range"
                ))
            })?;
            emit(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(count))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::RelationshipCountExec { rel_type, output } => {
            let rel_type_id = (!rel_type.is_empty())
                .then(|| catalog.rel_type_id(rel_type))
                .flatten();
            let count = if rel_type.is_empty() {
                store.relationship_count_for_type(None)
            } else if let Some(rel_type_id) = rel_type_id {
                store.relationship_count_for_type(Some(rel_type_id))
            } else {
                0
            };
            context
                .observer
                .record_scan_pruning_report(ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Relationship,
                    label_id: None,
                    rel_type_id,
                    strategy: ScanPruningStrategy::ExactCount,
                    pruned: true,
                    exact_empty: count == 0,
                    candidate_count_before_pruning: count,
                    pruned_candidate_count: count,
                    candidate_count_before_filter: 0,
                    output_count: 1,
                    filtered_out_count: 0,
                });
            let count = i64::try_from(count).map_err(|_| {
                SkeinError::Execution(format!(
                    "relationship count for type '{rel_type}' exceeds the supported i64 result range"
                ))
            })?;
            emit(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(count))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => stream_adjacency_expand_batches(
            plan,
            input,
            context,
            execution_limit,
            AdjacencyExpandFilters::default(),
            emit,
        ),
        PhysicalPlan::AdjacencyExistsExec { input, .. } => {
            stream_adjacency_exists_batches(plan, input, context, execution_limit, emit)
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            stream_cartesian_product_batches(left, right, context, execution_limit, emit)
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            if let PhysicalPlan::SeqNodeScan { variable, label } = input.as_ref()
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_node_scan_batches(
                    variable,
                    label,
                    Some((predicate, &filter)),
                    context,
                    execution_limit,
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                rel_variable: Some(rel_variable),
                input: expand_input,
                ..
            } = input.as_ref()
                && let Some(filter) =
                    exact_relationship_scan_filter_from_predicate(predicate, rel_variable)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: Some(&filter),
                        target_scan_filter: None,
                    },
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                target_variable,
                input: expand_input,
                ..
            } = input.as_ref()
                && predicate_references_only_variable(predicate, target_variable)
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: None,
                        target_scan_filter: Some(&filter),
                    },
                    emit,
                );
            }
            let predicate_account = context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "FilterExec relationship predicate",
                context.memory.blocking_operator_bytes,
            );
            let emitted = Cell::new(0usize);
            execute_prepared_binding_batches(
                BatchPlanRef::descendant(input),
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    let remaining = execution_limit
                        .output_rows
                        .unwrap_or(usize::MAX)
                        .saturating_sub(emitted.get());
                    if remaining == 0 {
                        return Ok(BatchControl::Stop);
                    }
                    let mut filtered = TransformBatchBuilder::new(
                        "FilterExec",
                        context.memory.batch_rows.get(),
                        context.memory.batch_payload_bytes,
                        context.memory_ledger,
                    )?;
                    let mut emit_filtered = |output: BindingBatch| {
                        emitted.set(emitted.get().saturating_add(output.len()));
                        emit(output)
                    };
                    for binding in batch {
                        if evaluate_predicate_observed(
                            predicate,
                            catalog,
                            store,
                            &binding,
                            context.observer,
                            skein_executor::store::AdjacencyReadMemory {
                                budget_bytes: context.memory.blocking_operator_bytes.get(),
                                account: Some(&predicate_account),
                            },
                        )? {
                            filtered.reserve_before_allocation()?;
                            filtered.push(binding);
                            if filtered.is_full()
                                && filtered.emit(&mut emit_filtered)? == BatchControl::Stop
                            {
                                return Ok(BatchControl::Stop);
                            }
                            if execution_limit
                                .is_reached(emitted.get().saturating_add(filtered.len()))
                            {
                                break;
                            }
                        }
                    }
                    if !filtered.is_empty()
                        && filtered.emit(&mut emit_filtered)? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    Ok(if execution_limit.is_reached(emitted.get()) {
                        BatchControl::Stop
                    } else {
                        BatchControl::Continue
                    })
                },
            )
        }
        PhysicalPlan::ProjectExec { items, input } => {
            if let Some(result) =
                try_stream_columnar_projection_batches(items, input, context, execution_limit, emit)
            {
                return result;
            }
            let emitted = Cell::new(0usize);
            execute_prepared_binding_batches(
                BatchPlanRef::descendant(input),
                context,
                execution_limit,
                &mut |batch| {
                    let mut projected = TransformBatchBuilder::new(
                        "ProjectExec",
                        context.memory.batch_rows.get(),
                        context.memory.batch_payload_bytes,
                        context.memory_ledger,
                    )?;
                    let mut emit_projected = |output: BindingBatch| {
                        emitted.set(emitted.get().saturating_add(output.len()));
                        emit(output)
                    };
                    for binding in batch {
                        projected.reserve_before_allocation()?;
                        let mut values = BTreeMap::new();
                        for item in items {
                            let value = project_value(item, catalog, &binding)?;
                            insert_projected_value(&mut values, &item.name, value);
                        }
                        projected.push(Binding {
                            values,
                            nodes: binding.nodes,
                            relationships: binding.relationships,
                        });
                        if projected.is_full()
                            && projected.emit(&mut emit_projected)? == BatchControl::Stop
                        {
                            return Ok(BatchControl::Stop);
                        }
                    }
                    if !projected.is_empty()
                        && projected.emit(&mut emit_projected)? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    Ok(if execution_limit.is_reached(emitted.get()) {
                        BatchControl::Stop
                    } else {
                        BatchControl::Continue
                    })
                },
            )
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let skipped = Cell::new(0usize);
            let emitted = Cell::new(0usize);
            let output_cap = match (limit, execution_limit.output_rows) {
                (Some(limit), Some(parent)) => (*limit).min(parent),
                (Some(limit), None) => *limit,
                (None, Some(parent)) => parent,
                (None, None) => usize::MAX,
            };
            execute_prepared_binding_batches(
                BatchPlanRef::descendant(input),
                context,
                ExecutionLimit {
                    output_rows: Some(offset.saturating_add(output_cap)),
                },
                &mut |batch| {
                    let mut output = TransformBatchBuilder::new(
                        "LimitExec",
                        context.memory.batch_rows.get(),
                        context.memory.batch_payload_bytes,
                        context.memory_ledger,
                    )?;
                    let mut emit_output = |batch: BindingBatch| {
                        emitted.set(emitted.get().saturating_add(batch.len()));
                        emit(batch)
                    };
                    for binding in batch {
                        if skipped.get() < *offset {
                            skipped.set(skipped.get().saturating_add(1));
                            continue;
                        }
                        if emitted.get() == output_cap {
                            break;
                        }
                        output.reserve_before_allocation()?;
                        output.push(binding);
                        if output.is_full() && output.emit(&mut emit_output)? == BatchControl::Stop
                        {
                            return Ok(BatchControl::Stop);
                        }
                    }
                    if !output.is_empty() && output.emit(&mut emit_output)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    Ok(if emitted.get() == output_cap {
                        BatchControl::Stop
                    } else {
                        BatchControl::Continue
                    })
                },
            )
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => stream_top_n_batches(
            input,
            items,
            *offset,
            *limit,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::SortExec { items, input } => {
            stream_sort_batches(input, items, context, execution_limit, emit)
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => stream_aggregate_batches(input, group_keys, items, context, execution_limit, emit),
        PhysicalPlan::DistinctExec { input } => {
            stream_distinct_batches(input, context, execution_limit, emit)
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => {
            unreachable!("BatchPlanRef rejected this physical operator")
        }
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn optional_relationship_count_checks_cancellation_without_matching_nodes() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(&mut catalog, "Other", BTreeMap::new())
            .unwrap();
        let plan = PhysicalPlan::OptionalRelationshipCountSumExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            properties: BTreeMap::new(),
            legs: vec![RelationshipCountLeg {
                rel_type: "HAS_MEMORY".to_string(),
                direction: RelationshipDirection::Outgoing,
                distinct: false,
                filter: None,
            }],
            output: "count".to_string(),
        };
        let memory = ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(1).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let parameters = BTreeMap::new();
        let mut external_operator = NoExternalReadOperator;
        let external = BatchExternalReadAdapter::new(&mut external_operator);
        let observer = QueryExecutionObserver::default();
        let cancellation = skein_core::RuntimeCancellationToken::new();
        let task_context = RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let context = BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &memory_ledger,
            task_context: Some(&task_context),
            observer: &observer,
        };

        // Bypass the pipeline-entry checkpoint to isolate the operator's scan loop.
        let error = execute_binding_batches_inner(
            BatchPlanRef::descendant(&plan),
            context,
            ExecutionLimit::unlimited(),
            &mut |_| Ok(BatchControl::Continue),
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));
    }
}

#[cfg(test)]
mod byte_bounded_batch_tests {
    use super::*;

    #[test]
    fn within_budget_batch_keeps_its_allocation() {
        let batch = vec![Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(1))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
        let allocation = batch.as_ptr();
        let observer = QueryExecutionObserver::default();
        let memory = ExecutionMemoryConfig::default();
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let memory_account = memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "test batch",
            memory.query_memory_bytes,
        );
        let mut emitted_allocation = None;

        let control = emit_byte_bounded_batches(
            batch,
            usize::MAX,
            &memory_account,
            &observer,
            &mut |emitted| {
                emitted_allocation = Some(emitted.as_ptr());
                Ok(BatchControl::Continue)
            },
        )
        .unwrap();

        assert_eq!(control, BatchControl::Continue);
        assert_eq!(emitted_allocation, Some(allocation));
        assert_eq!(observer.into_reports().pipeline_memory.intermediate_rows, 1);
    }
}
