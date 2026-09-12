//! Internal graph algorithm execution over storage-neutral reads.

use crate::binding::Binding;
use crate::expression::property_filter_from_predicate;
use crate::kernel::{push_bounded_operator_binding, OperatorMemoryTracker};
use crate::observer::QueryExecutionObserver;
use crate::pipeline::{emit_owned_binding_batches, runtime_checkpoint, BatchControl, BindingBatch};
use crate::predicate::node_matches_property_filter;
use crate::store::{GraphExecutionRead, ScanControl};
use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryClass, QueryMemoryLedger};
use skein_analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectedGraphExecution, ProjectionLayout,
    ProjectionMemoryBudget,
};
use skein_core::{Catalog, Result, RuntimeTaskContext, SkeinError, Value};
use skein_plan::{GraphAlgorithmKind, Predicate};
use skein_storage::{NodeRecord, RelRecord};
use std::collections::BTreeMap;

/// Borrows the existing query/store seams without owning admission or catalog mutation.
#[derive(Clone, Copy)]
pub struct GraphAlgorithmContext<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn GraphExecutionRead,
    pub memory: &'a ExecutionMemoryConfig,
    pub memory_ledger: &'a QueryMemoryLedger,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a QueryExecutionObserver,
}

pub struct GraphAlgorithmSpec<'a> {
    pub algorithm: &'a GraphAlgorithmKind,
    pub graph_name: &'a str,
    pub options: &'a skein_plan::GraphAlgorithmOptions,
    pub score_column: &'a str,
    pub node_visibility_predicate: &'a Option<Predicate>,
}

impl GraphAlgorithmSpec<'_> {
    pub fn stream(
        self,
        context: GraphAlgorithmContext<'_>,
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
                        estimated_vec_memory_bytes::<skein_analytics::PageRankScore>(scores.len());
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
                        skein_analytics::HierarchicalCommunityAssignment,
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
) -> crate::BlockingOperatorMemoryReport {
    crate::BlockingOperatorMemoryReport {
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

pub fn try_projected_graph_with_node_filter(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    node_labels: &[String],
    rel_types: &[String],
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> Result<ProjectedGraph> {
    let source = GraphExecutionProjectionSource(store);
    if node_labels.is_empty() && rel_types.is_empty() {
        return ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &source,
            None,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &[],
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::try_from_store_without_edges_with_node_filter_and_layout(
                &source,
                include_node,
                layout,
                budget,
            )
            .map_err(|error| SkeinError::Execution(error.to_string()));
        }
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &label_ids,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    ProjectedGraph::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
        &source,
        &label_ids,
        &rel_type_ids,
        include_node,
        layout,
        budget,
    )
    .map_err(|error| SkeinError::Execution(error.to_string()))
}

struct GraphExecutionProjectionSource<'a>(&'a dyn GraphExecutionRead);

impl skein_analytics::ProjectionSource for GraphExecutionProjectionSource<'_> {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        self.0
            .visit_nodes_owned(None, &mut |node| {
                Ok(match visitor(node) {
                    skein_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    skein_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => skein_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => skein_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        self.0
            .visit_relationships_owned(None, &mut |relationship| {
                Ok(match visitor(relationship) {
                    skein_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    skein_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => skein_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => skein_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
