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

//! Node, index, source-segment, and adjacency scan execution.

use super::*;
use crate::observer::ExecutionObserver;
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
            crate::store::AdjacencyReadMemory {
                budget_bytes: context.memory.blocking_operator_bytes.get(),
                account: Some(&memory_account),
            },
            context.task_context,
        ),
        None => Ok(true),
    };
    crate::scan::stream_node_scan_batches(
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
    crate::scan::stream_node_projection_scan_batches(
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
    crate::scan::stream_index_node_seek_batches(
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
    branches: &[hawdb_plan_cypher::ExactPropertySeekBranch],
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
    crate::scan::stream_index_node_union_seek_batches(
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

pub(super) fn stream_node_access_batches(
    variable: &str,
    label: &str,
    access: &hawdb_plan_cypher::NodeProjectionAccess,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let memory_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "IndexNodeScanExec",
        context.memory.blocking_operator_bytes,
    );
    let batch_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "IndexNodeScanExec output",
        context.memory.batch_payload_bytes,
    );
    crate::scan::stream_node_access_batches(
        variable,
        label,
        access,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            memory_account: &memory_account,
            batch_memory_budget: context.memory.batch_payload_bytes,
            batch_memory_account: &batch_account,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        emit,
    )
}

pub fn stream_visited_node_batches(
    variable: &str,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    visit: impl FnOnce(&mut dyn FnMut(NodeRecord) -> Result<ScanControl>) -> Result<ScanControl>,
) -> Result<BatchControl> {
    let mut batch = AccountedBindingBatch::with_ledger(
        "IndexNodeScanExec",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        context.memory_ledger,
    );
    let mut emitted = 0usize;
    let mut consumer = |node| {
        runtime_checkpoint(context.task_context)?;
        if batch.push(single_node_binding(variable, node), emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        if execution_limit.is_reached(emitted) {
            Ok(ScanControl::Stop)
        } else {
            Ok(ScanControl::Continue)
        }
    };
    let control = visit(&mut consumer)?;
    if !batch.is_empty() && batch.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == ScanControl::Stop {
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
    let node_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "SourceSegmentScan node ownership",
        memory.blocking_operator_bytes,
    );
    let mut allocate_slots =
        |bytes| crate::store::admit_graph_read(&node_account, task_context, bytes);
    let mut allocations = hawdb_storage::read_view::AdmittedVec::new(
        &hawdb_storage::read_view::GraphReadAdmission::new(&mut allocate_slots),
    )?;
    let emitted = Cell::new(0usize);
    let mut emit_output = |batch: BindingBatch| {
        emitted.set(emitted.get().saturating_add(batch.len()));
        emit(batch)
    };
    let visit = store.visit_source_scan_candidates(
        &storage_predicate,
        SourceScanReadLimits {
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            max_live_candidate_bytes: memory.blocking_operator_bytes,
        },
        task_context,
        &mut |row| {
            runtime_checkpoint(task_context)?;
            if execution_limit.is_reached(emitted.get().saturating_add(output.len())) {
                return Ok(ScanControl::Stop);
            }
            let input = store.node_with_allocation(NodeId(row.node_id), None, &mut |bytes| {
                let bytes = bytes.saturating_add(1024 + variable.len());
                if !output.is_empty()
                    && (!node_account.can_reserve(bytes) || output.would_exceed_payload(bytes))
                {
                    let control = output.emit(&mut emit_output)?;
                    allocations.clear();
                    if control == BatchControl::Stop {
                        return Ok(None);
                    }
                }
                crate::store::admit_graph_read(&node_account, task_context, bytes).map(Some)
            })?;
            if matches!(input, hawdb_storage::read_view::AdmittedNodeRead::Stopped) {
                return Ok(ScanControl::Stop);
            }
            let hawdb_storage::read_view::AdmittedNodeRead::Node(node) = input else {
                return Err(HawDBError::StorageIntegrity(
                    "SourceSegmentScan sidecar candidate is absent from the canonical graph"
                        .to_string(),
                ));
            };
            let (node, allocation) = node.into_parts();
            if source_label_id.is_none_or(|label_id| !node.labels.contains(&label_id))
                || node.properties != row.properties
            {
                return Err(HawDBError::StorageIntegrity(
                    "SourceSegmentScan sidecar candidate disagrees with the canonical graph"
                        .to_string(),
                ));
            }
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), node)]),
                relationships: BTreeMap::new(),
            };
            allocations.try_push(allocation)?;
            if output.push(binding, &mut emit_output)? == BatchControl::Stop {
                return Ok(ScanControl::Stop);
            }
            if output.is_full() {
                let control = output.emit(&mut emit_output)?;
                allocations.clear();
                if control == BatchControl::Stop {
                    return Ok(ScanControl::Stop);
                }
            }
            Ok(
                if execution_limit.is_reached(emitted.get().saturating_add(output.len())) {
                    ScanControl::Stop
                } else {
                    ScanControl::Continue
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
        target_kind: hawdb_storage::scan::ScanPruningTargetKind::Node,
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
    crate::scan::execute_node_column_lookup(
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
            task_context: context.task_context,
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
                    crate::store::AdjacencyReadMemory {
                        budget_bytes: context.memory.blocking_operator_bytes.get(),
                        account: Some(&predicate_account),
                    },
                    context.task_context,
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
        return Err(HawDBError::Execution(
            "expected adjacency exists plan".to_string(),
        ));
    };
    let rel_type_id = context.catalog.rel_type_id(rel_type);
    let adjacency_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "AdjacencyExistsExec adjacency",
        context.memory.blocking_operator_bytes,
    );
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
                    crate::scan::adjacency_exists_with_memory(
                        context.store,
                        source.id,
                        target.id,
                        rel_type_id,
                        *direction,
                        crate::store::AdjacencyReadMemory {
                            budget_bytes: context.memory.blocking_operator_bytes.get(),
                            account: Some(&adjacency_account),
                        },
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
        source_label,
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
        return Err(HawDBError::Execution(
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
        catalog.rel_type_id(rel_type)
    };
    let unknown_relationship_type = !rel_type.is_empty() && rel_type_id.is_none();
    if unknown_relationship_type && !optional && *min_hops != 0 {
        record_graph_expansion_state(
            context.observer,
            &graph_expansion,
            rel_type,
            *min_hops,
            *max_hops,
            0,
        );
        return Ok(BatchControl::Continue);
    }
    let source_label_ids = label_ids_for_pattern(catalog, source_label);
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "AdjacencyExpandExec output",
        memory.batch_payload_bytes,
    );
    let mut output_lease = output_account.reserve(0)?;
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    let control =
        execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                graph_expansion.record_seed();
                let spec = AdjacencyExpandSpec {
                    source_variable,
                    rel_variable: rel_variable.as_deref(),
                    rel_properties,
                    direction: *direction,
                    target_variable,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    optional: *optional,
                };
                let mut visit_candidate = ExpandBatchConsumer {
                    context,
                    execution_limit,
                    graph_expansion: &mut graph_expansion,
                    output: &mut output,
                    output_bytes: &mut output_bytes,
                    output_lease: &mut output_lease,
                    emit,
                };
                let source_label_mismatch =
                    binding.nodes.get(source_variable).is_some_and(|node| {
                        !node_matches_label_pattern(node, source_label_ids.as_deref())
                    });
                let expand_control = if source_label_mismatch {
                    crate::scan::stream_unmatched_expand_binding(
                        &binding,
                        &spec,
                        &mut visit_candidate,
                    )?
                } else if unknown_relationship_type {
                    crate::scan::stream_zero_hop_expand_binding(
                        &binding,
                        &spec,
                        crate::scan::ZeroHopExpandContext {
                            store,
                            target_label_ids: target_label_ids.as_deref(),
                            filters: &filters,
                            memory: crate::store::AdjacencyReadMemory {
                                budget_bytes: memory.blocking_operator_bytes.get(),
                                account: Some(&adjacency_account),
                            },
                            task_context: context.task_context,
                        },
                        &mut visit_candidate,
                    )?
                } else {
                    stream_expand_binding_admitted(
                        &binding,
                        spec,
                        rel_type_id,
                        target_label_ids.as_deref(),
                        &filters,
                        store,
                        crate::store::AdjacencyReadMemory {
                            budget_bytes: memory.blocking_operator_bytes.get(),
                            account: Some(&adjacency_account),
                        },
                        context.task_context,
                        context.observer,
                        &mut visit_candidate,
                    )?
                };
                if expand_control == crate::store::ScanControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        })?;
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

struct ExpandBatchConsumer<'a, 'context> {
    context: BatchReadContext<'context>,
    execution_limit: ExecutionLimit,
    graph_expansion: &'a mut GraphExpansionExecutionState,
    output: &'a mut Vec<Binding>,
    output_bytes: &'a mut usize,
    output_lease: &'a mut crate::QueryMemoryLease,
    emit: &'a mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
}

impl crate::scan::ExpandedBindingConsumer for ExpandBatchConsumer<'_, '_> {
    fn push(
        &mut self,
        preview: crate::scan::ExpandedBindingPreview<'_>,
        create: impl FnOnce() -> crate::scan::ExpandedBinding,
    ) -> Result<ScanControl> {
        runtime_checkpoint(self.context.task_context)?;
        let scoring = self.context.observer.seed_graph_scoring_input().is_some();
        let reduction = if scoring {
            crate::scoring::seed_hop_payload_reduction(
                preview.input,
                preview.hop,
                preview.target_id.is_some(),
            )?
        } else {
            0
        };
        let candidate_bytes = preview.memory_bytes.saturating_sub(reduction);
        let payload_bytes = preview.payload_bytes.saturating_sub(reduction);
        crate::scan::ensure_expanded_binding_fits(
            candidate_bytes,
            self.context.memory.blocking_operator_bytes.get(),
        )?;
        let batch_payload_bytes = self.context.memory.batch_payload_bytes.get();
        if candidate_bytes > batch_payload_bytes {
            return Err(HawDBError::Execution(format!(
                "intermediate row uses {candidate_bytes} bytes, exceeding batch_payload_bytes {batch_payload_bytes}"
            )));
        }
        if !self.output.is_empty()
            && (self.output.len() == self.context.memory.batch_rows.get()
                || self.output_bytes.saturating_add(candidate_bytes) > batch_payload_bytes)
        {
            let emitted = std::mem::take(self.output);
            self.output_lease.reset();
            *self.output_bytes = 0;
            if (self.emit)(emitted)? == BatchControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        // A downstream callback may cancel while accepting the previous batch.
        // Check again before admitting or constructing the next output row.
        runtime_checkpoint(self.context.task_context)?;
        if !self
            .graph_expansion
            .try_admit_payload(payload_bytes, preview.target_id, preview.hop)?
        {
            return Ok(ScanControl::Stop);
        }
        // The lazy constructor cannot run until its complete retained output
        // allocation is charged. This same lease then owns the buffered row.
        self.output_lease.grow(candidate_bytes)?;
        let mut candidate = create();
        if scoring {
            crate::scoring::advance_seed_hop(
                &mut candidate.binding,
                candidate.hop,
                candidate.target_id.is_some(),
            )?;
        }
        debug_assert_eq!(binding_memory_bytes(&candidate.binding), candidate_bytes);
        debug_assert_eq!(
            crate::binding::binding_payload_bytes(&candidate.binding),
            payload_bytes
        );
        *self.output_bytes = self.output_bytes.saturating_add(candidate_bytes);
        crate::pipeline::reserve_binding_slot(self.output);
        self.output.push(candidate.binding);
        Ok(
            if self
                .execution_limit
                .is_reached(self.graph_expansion.returned_count())
            {
                ScanControl::Stop
            } else {
                ScanControl::Continue
            },
        )
    }
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

#[cfg(test)]
mod expand_output_tests {
    use super::*;
    use crate::scan::{ExpandedBinding, ExpandedBindingConsumer, ExpandedBindingPreview};
    use std::num::NonZeroUsize;

    #[test]
    fn expand_output_exact_root_flushes_before_copy_and_respects_stop() {
        for (stop, cancel) in [(false, false), (true, false), (false, true)] {
            let row = Binding::scalar("v", Value::String("x".repeat(4097)));
            let bytes = binding_memory_bytes(&row);
            let limit = NonZeroUsize::new(bytes).unwrap();
            let memory = ExecutionMemoryConfig {
                query_memory_bytes: limit,
                batch_payload_bytes: limit,
                batch_rows: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            };
            let ledger = QueryMemoryLedger::new(limit);
            let account = ledger.account(
                QueryMemoryClass::PipelineBatch,
                "exact expand output",
                limit,
            );
            let mut lease = account.reserve(0).unwrap();
            let catalog = Catalog::default();
            let store = hawdb_storage::store::GraphStore::default();
            let parameters = BTreeMap::new();
            let observer = QueryExecutionObserver::default();
            let mut external = crate::external::NoExternalReadOperator;
            let external = BatchExternalReadAdapter::new(&mut external);
            let token = hawdb_core::RuntimeCancellationToken::new();
            let task = RuntimeTaskContext::new(token.clone(), None);
            let context = BatchReadContext {
                catalog: &catalog,
                store: &store,
                parameters: &parameters,
                external: &external,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: Some(&task),
                observer: &observer,
                host_scorer: None,
            };
            let copies = Cell::new(0);
            let flushes = Cell::new(0);
            let mut emit = |rows: BindingBatch| {
                assert_eq!(rows, vec![row.clone()]);
                assert_eq!(ledger.snapshot().used_bytes, 0);
                flushes.set(flushes.get() + 1);
                if cancel {
                    token.cancel();
                }
                Ok(if stop {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            };
            let mut output = Vec::new();
            let mut output_bytes = 0;
            let mut graph_expansion = GraphExpansionExecutionState::new(None, 0, 0);
            {
                let mut sink = ExpandBatchConsumer {
                    context,
                    execution_limit: ExecutionLimit::unlimited(),
                    graph_expansion: &mut graph_expansion,
                    output: &mut output,
                    output_bytes: &mut output_bytes,
                    output_lease: &mut lease,
                    emit: &mut emit,
                };
                let mut push = || {
                    sink.push(
                        ExpandedBindingPreview {
                            input: &row,
                            memory_bytes: bytes,
                            payload_bytes: crate::binding::binding_payload_bytes(&row),
                            target_id: None,
                            hop: 0,
                        },
                        || {
                            // The complete output lease must already exist when
                            // ownership is constructed, including at the exact cap.
                            assert_eq!(ledger.snapshot().used_bytes, bytes);
                            copies.set(copies.get() + 1);
                            ExpandedBinding {
                                binding: row.clone(),
                                target_id: None,
                                hop: 0,
                            }
                        },
                    )
                };
                assert_eq!(push().unwrap(), ScanControl::Continue);
                if cancel {
                    let error =
                        push().expect_err("flush cancellation must precede the next constructor");
                    assert!(error.to_string().contains("cancelled"), "{error}");
                } else {
                    assert_eq!(
                        push().unwrap(),
                        if stop {
                            ScanControl::Stop
                        } else {
                            ScanControl::Continue
                        }
                    );
                }
            }
            assert_eq!(flushes.get(), 1);
            assert_eq!(copies.get(), if stop || cancel { 1 } else { 2 });
            assert_eq!(output.len(), usize::from(!stop && !cancel));
            assert_eq!(output_bytes, if stop || cancel { 0 } else { bytes });
            assert_eq!(
                ledger.snapshot().used_bytes,
                if stop || cancel { 0 } else { bytes }
            );
            drop(output);
            drop(lease);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}
