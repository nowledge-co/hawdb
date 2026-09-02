//! Node, index, source-segment, and adjacency scan execution.

use super::*;
use skein_executor::observer::ExecutionObserver;
use std::cell::Cell;

pub(super) fn stream_node_scan_batches(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "NodeScanExec",
        context.memory.blocking_operator_bytes,
    );
    let batch_memory_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "NodeScanExec output",
        context.memory.batch_payload_bytes,
    );
    let mut predicate = |binding: &Binding| match filter {
        Some((predicate, _)) => evaluate_predicate_observed(
            predicate,
            context.catalog,
            context.store,
            binding,
            context.observer,
            skein_executor::store::AdjacencyReadMemory {
                budget_bytes: context.memory.blocking_operator_bytes.get(),
                account: Some(&memory_account),
            },
        ),
        None => Ok(true),
    };
    skein_executor::scan::stream_node_scan_batches(
        NodeScanSpec {
            variable,
            label,
            property_filter: filter.map(|(_, filter)| filter),
        },
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_memory_account,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        &mut predicate,
        context.observer,
        emit,
    )
}

pub(super) fn stream_node_projection_scan_batches(
    spec: NodeProjectionScanSpec<'_>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "NodeProjectionScanExec",
        context.memory.blocking_operator_bytes,
    );
    let batch_memory_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "NodeProjectionScanExec output",
        context.memory.batch_payload_bytes,
    );
    skein_executor::scan::stream_node_projection_scan_batches(
        spec,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_memory_account,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        context.observer,
        emit,
    )
}

pub(super) fn stream_index_node_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    values: &[Value],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "IndexNodeSeekExec",
        context.memory.blocking_operator_bytes,
    );
    let batch_memory_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "IndexNodeSeekExec output",
        context.memory.batch_payload_bytes,
    );
    skein_executor::scan::stream_index_node_seek_batches(
        variable,
        label,
        property,
        values,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_memory_account,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        context.observer,
        emit,
    )
}

pub(super) fn stream_index_node_union_seek_batches(
    variable: &str,
    label: &str,
    branches: &[skein_plan::ExactPropertySeekBranch],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "IndexNodeUnionSeekExec",
        context.memory.blocking_operator_bytes,
    );
    let batch_memory_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "IndexNodeUnionSeekExec output",
        context.memory.batch_payload_bytes,
    );
    skein_executor::scan::stream_index_node_union_seek_batches(
        variable,
        label,
        branches,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_memory_account,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        context.observer,
        emit,
    )
}

pub(super) fn stream_visited_node_batches(
    variable: &str,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    visit: impl FnOnce(&mut dyn FnMut(NodeRecord) -> GraphScanControl) -> Result<GraphScanControl>,
) -> Result<BatchControl> {
    let mut batch = AccountedBindingBatch::with_ledger(
        "IndexNodeScanExec",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        context.memory_ledger,
    );
    let mut emitted = 0usize;
    let mut callback_error = None;
    let mut consumer = |node| {
        if let Err(error) = runtime_checkpoint(context.task_context) {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        match batch.push(single_node_binding(variable, node), emit) {
            Ok(BatchControl::Continue) => {}
            Ok(BatchControl::Stop) => return GraphScanControl::Stop,
            Err(error) => {
                callback_error = Some(error);
                return GraphScanControl::Stop;
            }
        }
        emitted = emitted.saturating_add(1);
        if batch.is_full() {
            match batch.emit(emit) {
                Ok(BatchControl::Continue) => {}
                Ok(BatchControl::Stop) => return GraphScanControl::Stop,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if execution_limit.is_reached(emitted) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    };
    let control = visit(&mut consumer)?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    if !batch.is_empty() && batch.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == GraphScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub(super) fn stream_source_segment_scan_batches(
    variable: &str,
    predicate: &Predicate,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext {
        catalog,
        store,
        memory,
        task_context,
        observer,
        ..
    } = context;
    runtime_checkpoint(task_context)?;
    let Some(storage_predicate) = source_storage_scan_predicate(predicate, variable) else {
        return stream_node_scan_batches(variable, "Source", None, context, execution_limit, emit);
    };
    let io_depth = NonZeroUsize::new(SOURCE_SEGMENT_SCAN_IO_DEPTH)
        .expect("source segment scan I/O depth is non-zero");
    let max_coalesced_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES)
        .expect("source segment scan coalesced range limit is non-zero");
    let max_wave_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES)
        .expect("source segment scan wave byte limit is non-zero");
    // The sidecar decoder owns a bounded candidate wave. Reserve its complete
    // local limit before issuing I/O so admission and the live ledger describe
    // the same peak even though decoding is storage-owned.
    let scratch_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "SourceSegmentScan candidates",
        memory.blocking_operator_bytes,
    );
    let _scratch = scratch_account.reserve(memory.blocking_operator_bytes.get())?;
    let source_label_id = catalog.label_id("Source");
    let source_count = source_label_id
        .map(|label_id| store.node_count_for_label(Some(label_id)))
        .unwrap_or_default();
    let mut output = AccountedBindingBatch::with_ledger(
        "SourceSegmentScan",
        memory.batch_rows.get(),
        memory.batch_payload_bytes,
        context.memory_ledger,
    );
    let emitted = Cell::new(0usize);
    let mut emit_output = |batch: BindingBatch| {
        emitted.set(emitted.get().saturating_add(batch.len()));
        emit(batch)
    };
    let visit = store.visit_published_source_scan_candidates_bounded(
        &storage_predicate,
        SourceScanCandidateLimits::bounded(
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            memory.blocking_operator_bytes,
        ),
        task_context,
        &mut |row| {
            runtime_checkpoint(task_context)?;
            if execution_limit.is_reached(emitted.get().saturating_add(output.len())) {
                return Ok(GraphScanControl::Stop);
            }
            let Some(node) = store.node_owned(NodeId(row.node_id))? else {
                return Err(SkeinError::StorageIntegrity(
                    "SourceSegmentScan sidecar candidate is absent from the canonical graph"
                        .to_string(),
                ));
            };
            if source_label_id.is_none_or(|label_id| !node.labels.contains(&label_id))
                || node.properties != row.properties
            {
                return Err(SkeinError::StorageIntegrity(
                    "SourceSegmentScan sidecar candidate disagrees with the canonical graph"
                        .to_string(),
                ));
            }
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), node)]),
                relationships: BTreeMap::new(),
            };
            if output.push(binding, &mut emit_output)? == BatchControl::Stop {
                return Ok(GraphScanControl::Stop);
            }
            if output.is_full() && output.emit(&mut emit_output)? == BatchControl::Stop {
                return Ok(GraphScanControl::Stop);
            }
            Ok(
                if execution_limit.is_reached(emitted.get().saturating_add(output.len())) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                },
            )
        },
    );
    runtime_checkpoint(task_context)?;
    let SourceScanCandidateVisit::Rows {
        skipped_segment_count,
        candidate_count,
        ..
    } = visit?
    else {
        return stream_node_scan_batches(variable, "Source", None, context, execution_limit, emit);
    };
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: source_label_id,
        rel_type_id: None,
        strategy: source_scan_pruning_strategy(&storage_predicate),
        pruned: skipped_segment_count > 0 || candidate_count < source_count,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: source_count,
        pruned_candidate_count: source_count.saturating_sub(candidate_count),
        candidate_count_before_filter: candidate_count,
        output_count: emitted.get().saturating_add(output.len()),
        filtered_out_count: 0,
    });
    if output.emit(&mut emit_output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if execution_limit.is_reached(emitted.get()) {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub(super) fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    let memory_budget = context.memory.blocking_operator_bytes;
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "NodeColumnLookupExec",
        memory_budget,
    );
    let batch_memory_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "NodeColumnLookupExec output",
        context.memory.batch_payload_bytes,
    );
    skein_executor::scan::execute_node_column_lookup(
        spec,
        input,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_memory_account,
            batch_rows: 1,
            task_context: None,
        },
        context.observer,
    )
}

pub(super) fn stream_filtered_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    predicate: &Predicate,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext { catalog, store, .. } = context;
    let predicate_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "AdjacencyExpandExec residual predicate",
        context.memory.blocking_operator_bytes,
    );
    let mut emitted = 0usize;
    stream_adjacency_expand_batches(
        plan,
        input,
        context,
        execution_limit,
        filters,
        &mut |batch| {
            let remaining = execution_limit
                .output_rows
                .unwrap_or(usize::MAX)
                .saturating_sub(emitted);
            if remaining == 0 {
                return Ok(BatchControl::Stop);
            }
            let mut filtered = Vec::with_capacity(batch.len().min(remaining));
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
                    filtered.push(binding);
                    if filtered.len() == remaining {
                        break;
                    }
                }
            }
            emitted = emitted.saturating_add(filtered.len());
            if !filtered.is_empty() && emit(filtered)? == BatchControl::Stop {
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

pub(super) fn stream_adjacency_exists_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let PhysicalPlan::AdjacencyExistsExec {
        source_variable,
        rel_type,
        direction,
        target_variable,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency exists plan".to_string(),
        ));
    };
    let rel_type_id = context.catalog.rel_type_id(rel_type);
    let emitted = Cell::new(0usize);
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        let remaining = execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(emitted.get());
        if remaining == 0 {
            return Ok(BatchControl::Stop);
        }
        let mut filtered = TransformBatchBuilder::new(
            "AdjacencyExistsExec",
            context.memory.batch_rows.get(),
            context.memory.batch_payload_bytes,
            context.memory_ledger,
        )?;
        let mut emit_filtered = |output: BindingBatch| {
            emitted.set(emitted.get().saturating_add(output.len()));
            emit(output)
        };
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            let exists = match (
                rel_type_id,
                binding.nodes.get(source_variable),
                binding.nodes.get(target_variable),
            ) {
                (Some(rel_type_id), Some(source), Some(target)) => {
                    skein_executor::scan::adjacency_exists(
                        context.store,
                        source.id,
                        target.id,
                        rel_type_id,
                        *direction,
                        context.task_context,
                    )?
                }
                _ => false,
            };
            if exists {
                filtered.reserve_before_allocation()?;
                filtered.push(binding);
                if filtered.is_full() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                if execution_limit.is_reached(emitted.get().saturating_add(filtered.len())) {
                    break;
                }
            }
        }
        if !filtered.is_empty() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if execution_limit.is_reached(emitted.get()) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    })
}

pub(super) fn stream_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        graph_budget,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency expand plan".to_string(),
        ));
    };
    let adjacency_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "AdjacencyExpandExec",
        memory.blocking_operator_bytes,
    );
    let mut graph_expansion = GraphExpansionExecutionState::with_memory_account(
        *graph_budget,
        0,
        context.observer.current_vector_rerank_count(),
        &adjacency_account,
    )?;
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            record_graph_expansion_state(
                context.observer,
                &graph_expansion,
                rel_type,
                *min_hops,
                *max_hops,
                0,
            );
            return Ok(BatchControl::Continue);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let batch_rows = memory.batch_rows.get();
    let batch_payload_bytes = memory.batch_payload_bytes.get();
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "AdjacencyExpandExec output",
        memory.batch_payload_bytes,
    );
    let mut output_lease = output_account.reserve(0)?;
    let mut output = Vec::with_capacity(batch_rows);
    let mut output_bytes = 0usize;
    let control = execute_binding_batches(
        input,
        context,
        ExecutionLimit::unlimited(),
        &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                graph_expansion.record_seed();
                let expand_control = stream_expand_binding(
                    &binding,
                    AdjacencyExpandSpec {
                        source_variable,
                        rel_variable: rel_variable.as_deref(),
                        rel_properties,
                        direction: *direction,
                        target_variable,
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        optional: *optional,
                    },
                    rel_type_id,
                    target_label_ids.as_deref(),
                    &filters,
                    store,
                    skein_executor::store::AdjacencyReadMemory {
                        budget_bytes: memory.blocking_operator_bytes.get(),
                        account: Some(&adjacency_account),
                    },
                    context.task_context,
                    context.observer,
                    &mut |candidate| {
                        runtime_checkpoint(context.task_context)?;
                        let candidate_bytes = binding_memory_bytes(&candidate.binding);
                        if candidate_bytes > batch_payload_bytes {
                            return Err(SkeinError::Execution(format!(
                                "intermediate row uses {candidate_bytes} bytes, exceeding batch_payload_bytes {batch_payload_bytes}"
                            )));
                        }
                        if !output.is_empty()
                            && (output.len() == batch_rows
                                || output_bytes.saturating_add(candidate_bytes)
                                    > batch_payload_bytes)
                        {
                            let emitted =
                                std::mem::replace(&mut output, Vec::with_capacity(batch_rows));
                            output_lease.reset();
                            if emit(emitted)? == BatchControl::Stop {
                                return Ok(skein_executor::store::ScanControl::Stop);
                            }
                            output_bytes = 0;
                        }
                        if !graph_expansion.try_admit(
                            &candidate.binding,
                            candidate.target_id,
                            candidate.hop,
                        )? {
                            return Ok(skein_executor::store::ScanControl::Stop);
                        }
                        output_lease.grow(candidate_bytes)?;
                        output_bytes = output_bytes.saturating_add(candidate_bytes);
                        output.push(candidate.binding);
                        if execution_limit.is_reached(graph_expansion.returned_count()) {
                            Ok(skein_executor::store::ScanControl::Stop)
                        } else {
                            Ok(skein_executor::store::ScanControl::Continue)
                        }
                    },
                )?;
                if expand_control == skein_executor::store::ScanControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        },
    )?;
    graph_expansion.set_reranked_seed_count(context.observer.current_vector_rerank_count());
    if !output.is_empty() {
        output_lease.reset();
    }
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        record_graph_expansion_state(
            context.observer,
            &graph_expansion,
            rel_type,
            *min_hops,
            *max_hops,
            graph_expansion.returned_count(),
        );
        return Ok(BatchControl::Stop);
    }
    record_graph_expansion_state(
        context.observer,
        &graph_expansion,
        rel_type,
        *min_hops,
        *max_hops,
        graph_expansion.returned_count(),
    );
    Ok(control)
}

fn record_graph_expansion_state(
    observer: &QueryExecutionObserver,
    state: &GraphExpansionExecutionState,
    rel_type: &str,
    min_hops: usize,
    max_hops: usize,
    returned_count: usize,
) {
    if let Some(report) = state.report(rel_type, min_hops, max_hops, returned_count) {
        observer.record_graph_expansion(report);
    }
}
