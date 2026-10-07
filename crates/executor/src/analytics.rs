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

//! Internal graph algorithm execution over storage-neutral reads.

use crate::binding::{node_memory_bytes, Binding};
use crate::expression::property_filter_from_predicate;
use crate::kernel::{push_bounded_operator_binding, OperatorMemoryTracker};
use crate::observer::QueryExecutionObserver;
use crate::pipeline::{emit_owned_binding_batches, runtime_checkpoint, BatchControl, BindingBatch};
use crate::predicate::node_matches_property_filter;
use crate::store::{GraphExecutionRead, ScanControl};
use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryClass, QueryMemoryLedger};
use hawdb_analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectedGraphExecution, ProjectionLayout,
    ProjectionMemoryBudget,
};
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan_cypher::{GraphAlgorithmKind, Predicate};
use hawdb_storage::{NodeId, NodeRecord, RelRecord};
use std::collections::BTreeMap;

mod streaming;

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

#[derive(Clone, Copy)]
pub struct GraphAlgorithmSpec<'a> {
    pub algorithm: &'a GraphAlgorithmKind,
    pub graph_name: &'a str,
    pub options: &'a hawdb_plan_cypher::GraphAlgorithmOptions,
    pub score_column: &'a str,
    pub return_node_identity: bool,
    pub node_visibility_predicate: &'a Option<Predicate>,
}

pub struct ProjectedGraphFilters<'a> {
    pub node_labels: &'a [String],
    pub rel_types: &'a [String],
    pub relationship_predicates:
        &'a BTreeMap<String, hawdb_storage::projection::ProjectedRelationshipPredicate>,
}

impl GraphAlgorithmSpec<'_> {
    fn page_rank_options(self) -> Result<PageRankOptions> {
        let defaults = PageRankOptions::default();
        let options = PageRankOptions {
            iterations: self.options.max_iterations.unwrap_or(defaults.iterations),
            damping: self.options.damping.unwrap_or(defaults.damping),
            tolerance: self.options.tolerance.unwrap_or(defaults.tolerance),
            normalize_initial: self
                .options
                .normalize_initial
                .unwrap_or(defaults.normalize_initial),
        };
        if !options.damping.is_finite() || !(0.0..1.0).contains(&options.damping) {
            return Err(HawDBError::Semantic(
                "PageRank damping must be finite and in [0, 1)".into(),
            ));
        }
        if !options.tolerance.is_finite() || options.tolerance < 0.0 {
            return Err(HawDBError::Semantic(
                "PageRank tolerance must be finite and non-negative".into(),
            ));
        }
        Ok(options)
    }

    fn louvain_options(self) -> Result<LouvainOptions> {
        let defaults = LouvainOptions::default();
        let options = LouvainOptions {
            max_iterations: self
                .options
                .max_iterations
                .unwrap_or(defaults.max_iterations),
            max_levels: self.options.max_levels.unwrap_or(defaults.max_levels),
            resolution: self.options.resolution.unwrap_or(defaults.resolution),
        };
        if !options.resolution.is_finite() || options.resolution <= 0.0 {
            return Err(HawDBError::Semantic(
                "Louvain resolution must be finite and greater than 0".into(),
            ));
        }
        Ok(options)
    }

    fn validate_options(self) -> Result<()> {
        match self.algorithm {
            GraphAlgorithmKind::PageRank => {
                self.page_rank_options()?;
            }
            GraphAlgorithmKind::Louvain => {
                self.louvain_options()?;
            }
        }
        Ok(())
    }

    pub fn stream(
        self,
        context: GraphAlgorithmContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            algorithm,
            graph_name,
            options: _,
            score_column,
            return_node_identity,
            node_visibility_predicate,
        } = self;
        let Some(definition) = context.store.projected_graph_definition(graph_name) else {
            return Err(HawDBError::Execution(format!(
                "projected graph '{graph_name}' does not exist"
            )));
        };
        self.validate_options()?;
        let node_visibility_filter = node_visibility_predicate
            .as_ref()
            .map(property_filter_from_predicate)
            .transpose()?;
        let layout = match algorithm {
            GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
            GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
        };
        let budget = ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes);
        let source = GraphExecutionProjectionSource(context.store, context.task_context);
        let admitted = if let Some(filter) = node_visibility_filter.as_ref() {
            try_projected_graph_with_filters_admitted(
                context.catalog,
                &source,
                ProjectedGraphFilters {
                    node_labels: &definition.node_labels,
                    rel_types: &definition.rel_types,
                    relationship_predicates: &definition.relationship_predicates,
                },
                |node| node_matches_property_filter(node, filter),
                layout,
                budget,
            )
        } else {
            try_projected_graph_with_filters_admitted(
                context.catalog,
                &source,
                ProjectedGraphFilters {
                    node_labels: &definition.node_labels,
                    rel_types: &definition.rel_types,
                    relationship_predicates: &definition.relationship_predicates,
                },
                |_| true,
                layout,
                budget,
            )
        };
        let graph = match admitted {
            Ok(graph) => {
                let estimate = match algorithm {
                    GraphAlgorithmKind::PageRank => graph.page_rank_memory_estimate(),
                    GraphAlgorithmKind::Louvain => {
                        graph.louvain_memory_estimate(self.louvain_options()?)
                    }
                };
                if estimate.total_peak_bytes > context.memory.blocking_operator_bytes.get() {
                    drop(graph);
                    return self.stream_external(context, execution_limit, emit);
                }
                graph
            }
            Err(error) if error.storage_error.is_none() => {
                return self.stream_external(context, execution_limit, emit);
            }
            Err(error) => {
                // ProjectionSource transports scan errors as strings. Preserve
                // the runtime's typed cancellation/deadline outcome when the
                // checkpoint inside that scan stopped the projection build.
                runtime_checkpoint(context.task_context)?;
                return Err(HawDBError::Execution(error.to_string()));
            }
        };
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
                    let options = self.page_rank_options()?;
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
                        estimated_vec_memory_bytes::<hawdb_analytics::PageRankScore>(scores.len());
                    charge_graph_algorithm_memory(
                        "PageRank",
                        "materialized result",
                        &mut tracker,
                        result_bytes,
                    )?;
                    for score in scores.into_iter().take(output_limit) {
                        let mut values = BTreeMap::from([
                            ("node".to_string(), Value::Int(score.node.0 as i64)),
                            (score_column.to_owned(), Value::Float(score.score)),
                        ]);
                        let hydration_bytes = if return_node_identity {
                            append_node_identity(
                                &mut values,
                                context.catalog,
                                context.store,
                                score.node,
                                &definition.node_labels,
                                "PageRank",
                                &mut tracker,
                            )?
                        } else {
                            0
                        };
                        let push_result = push_bounded_operator_binding(
                            "GraphAlgorithm",
                            &mut bindings,
                            Binding {
                                values,
                                nodes: BTreeMap::new(),
                                relationships: BTreeMap::new(),
                            },
                            &mut tracker,
                        );
                        tracker.release(hydration_bytes);
                        push_result?;
                    }
                    tracker.release(result_bytes);
                }
                GraphAlgorithmKind::Louvain => {
                    let options = self.louvain_options()?;
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
                        hawdb_analytics::HierarchicalCommunityAssignment,
                    >(assignments.len());
                    charge_graph_algorithm_memory(
                        "Louvain",
                        "materialized result",
                        &mut tracker,
                        result_bytes,
                    )?;
                    for assignment in assignments.into_iter().take(output_limit) {
                        let mut values = BTreeMap::from([
                            ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                            ("level".to_string(), Value::Int(assignment.level as i64)),
                            (
                                "louvain_id".to_string(),
                                Value::Int(assignment.community.0 as i64),
                            ),
                        ]);
                        let hydration_bytes = if return_node_identity {
                            append_node_identity(
                                &mut values,
                                context.catalog,
                                context.store,
                                assignment.node,
                                &definition.node_labels,
                                "Louvain",
                                &mut tracker,
                            )?
                        } else {
                            0
                        };
                        let push_result = push_bounded_operator_binding(
                            "GraphAlgorithm",
                            &mut bindings,
                            Binding {
                                values,
                                nodes: BTreeMap::new(),
                                relationships: BTreeMap::new(),
                            },
                            &mut tracker,
                        );
                        tracker.release(hydration_bytes);
                        push_result?;
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

fn append_node_identity(
    values: &mut BTreeMap<String, Value>,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    node_id: NodeId,
    preferred_labels: &[String],
    algorithm: &'static str,
    tracker: &mut OperatorMemoryTracker,
) -> Result<usize> {
    let node = store.node_owned(node_id)?.ok_or_else(|| {
        HawDBError::Execution(format!(
            "graph algorithm result references missing node {}",
            node_id.0
        ))
    })?;
    let hydration_bytes = node_memory_bytes(&node);
    charge_graph_algorithm_memory(
        algorithm,
        "node identity hydration",
        tracker,
        hydration_bytes,
    )?;
    let external_id = node.properties.get("id").cloned().unwrap_or(Value::Null);
    let label = preferred_labels
        .iter()
        .find(|label| {
            catalog
                .label_id(label)
                .is_some_and(|label_id| node.labels.contains(&label_id))
        })
        .cloned()
        .or_else(|| {
            node.labels
                .iter()
                .find_map(|label_id| catalog.label_name(*label_id).map(str::to_string))
        })
        .map(Value::String)
        .unwrap_or(Value::Null);
    values.insert("node_id".to_string(), external_id);
    values.insert("node_label".to_string(), label);
    Ok(hydration_bytes)
}

fn charge_graph_algorithm_memory(
    algorithm: &'static str,
    phase: &'static str,
    tracker: &mut OperatorMemoryTracker,
    bytes: usize,
) -> Result<()> {
    if tracker.would_exceed(bytes) {
        return Err(HawDBError::Execution(format!(
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
        candidate_rows: 0,
        replay_rows: 0,
        repartitions: 0,
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
    try_projected_graph_with_filters(
        catalog,
        store,
        ProjectedGraphFilters {
            node_labels,
            rel_types,
            relationship_predicates: &BTreeMap::new(),
        },
        include_node,
        layout,
        budget,
    )
}

pub fn try_projected_graph_with_filters(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    filters: ProjectedGraphFilters<'_>,
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> Result<ProjectedGraph> {
    try_projected_graph_with_filters_admitted(
        catalog,
        &GraphExecutionProjectionSource(store, None),
        filters,
        include_node,
        layout,
        budget,
    )
    .map_err(|error| HawDBError::Execution(error.to_string()))
}

fn try_projected_graph_with_filters_admitted(
    catalog: &Catalog,
    source: &GraphExecutionProjectionSource<'_>,
    filters: ProjectedGraphFilters<'_>,
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> std::result::Result<ProjectedGraph, hawdb_analytics::ProjectionMemoryAdmissionError> {
    let ProjectedGraphFilters {
        node_labels,
        rel_types,
        relationship_predicates,
    } = filters;
    if node_labels.is_empty() && rel_types.is_empty() && relationship_predicates.is_empty() {
        return ProjectedGraph::try_from_store_with_node_filter_and_layout(
            source,
            None,
            include_node,
            layout,
            budget,
        );
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            source,
            &[],
            include_node,
            layout,
            budget,
        );
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    let relationship_filters = relationship_predicates
        .iter()
        .filter_map(|(rel_type, predicate)| {
            catalog
                .rel_type_id(rel_type)
                .map(|rel_type_id| (rel_type_id, predicate))
        })
        .collect::<BTreeMap<_, _>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::try_from_store_without_edges_with_node_filter_and_layout(
                source,
                include_node,
                layout,
                budget,
            );
        }
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            source,
            &label_ids,
            include_node,
            layout,
            budget,
        );
    }
    ProjectedGraph::try_from_store_labels_and_rel_types_with_filters_and_layout(
        source,
        &label_ids,
        &rel_type_ids,
        include_node,
        |relationship| {
            relationship_filters
                .get(&relationship.rel_type)
                .is_none_or(|predicate| predicate.matches(&relationship.properties))
        },
        layout,
        budget,
    )
}

pub fn bind_projected_relationship_predicates(
    predicates: &BTreeMap<String, Predicate>,
) -> Result<BTreeMap<String, hawdb_storage::projection::ProjectedRelationshipPredicate>> {
    predicates
        .iter()
        .map(|(rel_type, predicate)| {
            bind_projected_relationship_predicate(predicate)
                .map(|predicate| (rel_type.clone(), predicate))
        })
        .collect()
}

fn bind_projected_relationship_predicate(
    predicate: &Predicate,
) -> Result<hawdb_storage::projection::ProjectedRelationshipPredicate> {
    use hawdb_plan_cypher::ComparisonOp;
    use hawdb_storage::projection::ProjectedRelationshipPredicate;

    match predicate {
        Predicate::And(predicates) if !predicates.is_empty() => predicates
            .iter()
            .map(bind_projected_relationship_predicate)
            .collect::<Result<Vec<_>>>()
            .map(ProjectedRelationshipPredicate::And),
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } if variable == "r" => Ok(ProjectedRelationshipPredicate::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyCompare {
            variable,
            property,
            op: ComparisonOp::Gte,
            value,
        } if variable == "r" => Ok(ProjectedRelationshipPredicate::Gte {
            property: property.clone(),
            value: value.clone(),
        }),
        _ => Err(HawDBError::Semantic(
            "projected relationship predicates support only literal r.property comparisons joined by AND"
                .to_string(),
        )),
    }
}

struct GraphExecutionProjectionSource<'a>(
    &'a dyn GraphExecutionRead,
    Option<&'a RuntimeTaskContext>,
);

impl hawdb_analytics::ProjectionSource for GraphExecutionProjectionSource<'_> {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> hawdb_analytics::ProjectionScanControl,
    ) -> std::result::Result<hawdb_analytics::ProjectionScanControl, String> {
        let mut ordinal = 0usize;
        self.0
            .visit_nodes_owned(None, &mut |node| {
                if ordinal.is_multiple_of(1024) {
                    runtime_checkpoint(self.1)?;
                }
                ordinal = ordinal.saturating_add(1);
                Ok(match visitor(node) {
                    hawdb_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    hawdb_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => hawdb_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => hawdb_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> hawdb_analytics::ProjectionScanControl,
    ) -> std::result::Result<hawdb_analytics::ProjectionScanControl, String> {
        let mut ordinal = 0usize;
        self.0
            .visit_relationships_owned(None, &mut |relationship| {
                if ordinal.is_multiple_of(1024) {
                    runtime_checkpoint(self.1)?;
                }
                ordinal = ordinal.saturating_add(1);
                Ok(match visitor(relationship) {
                    hawdb_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    hawdb_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => hawdb_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => hawdb_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
