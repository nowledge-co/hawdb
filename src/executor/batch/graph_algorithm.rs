//! Projected graph execution and its memory-accounting boundary.

use super::*;

pub(super) struct GraphAlgorithmSpec<'a> {
    pub(super) algorithm: &'a GraphAlgorithmKind,
    pub(super) graph_name: &'a str,
    pub(super) options: &'a crate::planner::GraphAlgorithmOptions,
    pub(super) score_column: &'a str,
    pub(super) node_visibility_predicate: &'a Option<Predicate>,
}

impl GraphAlgorithmSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } = self;
        let Some(definition) = context.store.projected_graph_definition(graph_name) else {
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
        let budget = ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes);
        let graph = if let Some(filter) = node_visibility_filter.as_ref() {
            try_projected_graph_with_node_filter(
                context.catalog,
                context.store,
                &definition.node_labels,
                &definition.rel_types,
                |node| node_matches_property_filter(node, filter),
                layout,
                budget,
            )
        } else {
            try_projected_graph_with_node_filter(
                context.catalog,
                context.store,
                &definition.node_labels,
                &definition.rel_types,
                |_| true,
                layout,
                budget,
            )
        }?;
        runtime_checkpoint(context.task_context)?;
        let mut tracker = OperatorMemoryTracker::with_account(
            context.memory.blocking_operator_bytes,
            context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "GraphAlgorithm",
                context.memory.blocking_operator_bytes,
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
                    let result_bytes =
                        estimated_vec_memory_bytes::<crate::analytics::PageRankScore>(scores.len());
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
                                    (score_column.to_owned(), Value::Float(score.score)),
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
                &tracker,
                input_rows,
                context.memory,
            ));
        let bindings = execution_result?;
        emit_owned_binding_batches(bindings, context.memory.batch_rows.get(), emit)
    }
}

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
