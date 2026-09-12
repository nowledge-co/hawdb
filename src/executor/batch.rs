//! Streaming batch orchestration and pipeline dispatch.

use super::*;
use skein_executor::observer::ExecutionObserver;
use skein_executor::pipeline::BatchExecutionContext;
use skein_storage::{ScanPruningStrategy, ScanPruningTargetKind};

mod dispatch;
mod graph_algorithm;
mod procedures;
mod scans;
mod transforms;

use dispatch::{unsupported_batch_operator, BatchDispatch, BatchExecution, BatchSupport};
use graph_algorithm::*;
use procedures::*;
use scans::*;
use transforms::*;

#[cfg(test)]
mod dispatch_tests;

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

impl<'a> BatchReadContext<'a> {
    pub(super) fn kernel_context(self) -> BatchExecutionContext<'a> {
        BatchExecutionContext {
            catalog: self.catalog,
            memory: self.memory,
            memory_ledger: self.memory_ledger,
            task_context: self.task_context,
            observer: self.observer,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct BatchPlanRef<'a>(&'a PhysicalPlan);

impl<'a> BatchPlanRef<'a> {
    pub(super) fn try_new(plan: &'a PhysicalPlan) -> Option<Self> {
        if !dispatch_batch_operator(plan, BatchSupport) {
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
    let plan = prepared
        .batch()
        .ok_or_else(|| unsupported_batch_operator(prepared.plan()))?;
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
    dispatch_batch_operator(
        plan.plan(),
        BatchExecution {
            context,
            execution_limit,
            emit,
        },
    )
}

fn dispatch_batch_operator<D: BatchDispatch>(plan: &PhysicalPlan, dispatch: D) -> D::Output {
    match plan {
        PhysicalPlan::EmptyExec => dispatch.supported(stream_empty_batches),
        PhysicalPlan::SeqNodeScan { variable, label } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_node_scan_batches(variable, label, None, context, execution_limit, emit)
            })
        }
        PhysicalPlan::NodeProjectionScanExec {
            variable,
            label,
            access,
            required_properties,
            predicate,
            items,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_node_projection_batches(
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
        }),
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_source_segment_scan_batches(variable, predicate, context, execution_limit, emit)
        }),
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_index_node_seek_batches(
                variable,
                label,
                property,
                std::slice::from_ref(value),
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_index_node_seek_batches(
                variable,
                label,
                property,
                values,
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::IndexNodeUnionSeek {
            variable,
            label,
            branches,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_index_node_union_seek_batches(
                variable,
                label,
                branches,
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_composite_node_seek_batches(
                variable,
                label,
                predicates,
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::IndexNodeCompositeRangeSeek {
            variable,
            label,
            seek,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_composite_node_range_seek_batches(
                variable,
                label,
                seek,
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => dispatch.supported(|context, execution_limit, emit| {
            NodeRangeSeekSpec {
                variable,
                label,
                property,
                lower,
                upper,
            }
            .stream(context, execution_limit, emit)
        }),
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_node_text_seek_batches(
                variable,
                label,
                property,
                query,
                context,
                execution_limit,
                emit,
            )
        }),
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
        } => dispatch.supported(|context, execution_limit, emit| {
            ShortestPathSpec {
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
            }
            .stream(context, execution_limit, emit)
        }),
        PhysicalPlan::ThreadRepairStatsExec {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => dispatch.supported(|context, _execution_limit, emit| {
            ThreadRepairStatsSpec {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            }
            .stream(context, _execution_limit, emit)
        }),
        PhysicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } => dispatch.supported(|context, execution_limit, emit| {
            GraphAlgorithmSpec {
                algorithm,
                graph_name,
                options,
                score_column,
                node_visibility_predicate,
            }
            .stream(
                GraphAlgorithmContext {
                    catalog: context.catalog,
                    store: context.store,
                    memory: context.memory,
                    memory_ledger: context.memory_ledger,
                    task_context: context.task_context,
                    observer: context.observer,
                },
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::VectorSeedScan {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            resource_profile,
            vector_plan,
        } => dispatch.supported(|context, execution_limit, emit| {
            VectorSeedScanSpec {
                embedding_parameter,
                output_external_id,
                metadata_filters,
                resource_profile,
                vector_plan,
            }
            .stream(
                VectorSeedContext {
                    parameters: context.parameters,
                    external: context.external,
                    memory: context.memory,
                    memory_ledger: context.memory_ledger,
                    task_context: context.task_context,
                    observer: context.observer,
                },
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_node_column_lookup_batches(
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
            )
        }),
        PhysicalPlan::OptionalDegreeExec {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => dispatch.supported(|context, execution_limit, emit| {
            OptionalDegreeSpec {
                source_variable,
                rel_type,
                rel_properties,
                direction: *direction,
                target_label,
                target_properties,
                alias,
                input,
            }
            .stream(context, execution_limit, emit)
        }),
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            output,
            ..
        } => dispatch.supported(|context, _execution_limit, emit| {
            stream_optional_relationship_count_sum_batches(
                label,
                properties,
                legs,
                output,
                context,
                _execution_limit,
                emit,
            )
        }),
        PhysicalPlan::NodeCountExec { label, output } => {
            dispatch.supported(|context, _execution_limit, emit| {
                stream_node_count_batches(label, output, context, _execution_limit, emit)
            })
        }
        PhysicalPlan::RelationshipCountExec { rel_type, output } => {
            dispatch.supported(|context, _execution_limit, emit| {
                stream_relationship_count_batches(rel_type, output, context, _execution_limit, emit)
            })
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_adjacency_expand_batches(
                    plan,
                    input,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters::default(),
                    emit,
                )
            })
        }
        PhysicalPlan::AdjacencyExistsExec { input, .. } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_adjacency_exists_batches(plan, input, context, execution_limit, emit)
            })
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_cartesian_product_batches(left, right, context, execution_limit, emit)
            })
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_filter_batches(predicate, input, context, execution_limit, emit)
            })
        }
        PhysicalPlan::ProjectExec { items, input } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_projection_batches(items, input, context, execution_limit, emit)
            })
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_limit_batches(*offset, *limit, input, context, execution_limit, emit)
        }),
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_top_n_batches(
                input,
                items,
                *offset,
                *limit,
                context,
                execution_limit,
                emit,
            )
        }),
        PhysicalPlan::SortExec { items, input } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_sort_batches(input, items, context, execution_limit, emit)
            })
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => dispatch.supported(|context, execution_limit, emit| {
            stream_aggregate_batches(input, group_keys, items, context, execution_limit, emit)
        }),
        PhysicalPlan::DistinctExec { input } => {
            dispatch.supported(|context, execution_limit, emit| {
                stream_distinct_batches(input, context, execution_limit, emit)
            })
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
        | PhysicalPlan::CreateRelationship { .. } => dispatch.unsupported(plan),
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
