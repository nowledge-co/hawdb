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

use super::*;
use crate::binding::{node_memory_bytes, relationship_memory_bytes};
use hawdb_analytics::{AnalyticsEdgeSource, StreamingGraph};
use hawdb_storage::{adjacency::AdjacencyDirection, NodeId};
use std::cell::Cell;
use std::num::NonZeroUsize;

// Includes original IDs, member vectors, overlapping contraction state, dense
// neighbor membership, algorithm scratch, and Vec capacity growth. Results are
// accounted separately, including all requested hierarchy levels.
const LOUVAIN_NODE_STATE_BYTES: usize = 576;
const PAGE_RANK_NODE_STATE_BYTES: usize = 192;

struct EdgeSource<'a> {
    context: GraphAlgorithmContext<'a>,
    rel_types: &'a [String],
    record_budget: usize,
    record_peak: Cell<usize>,
    record_account: crate::QueryMemoryAccount,
}

impl AnalyticsEdgeSource for EdgeSource<'_> {
    fn visit_neighbors(
        &self,
        node: NodeId,
        undirected: bool,
        visitor: &mut dyn FnMut(NodeId) -> Result<()>,
    ) -> Result<()> {
        runtime_checkpoint(self.context.task_context)?;
        let mut ordinal = 0usize;
        let mut scan = |direction, rel_type| {
            let control = self.context.store.visit_adjacent_relationships_owned(
                node,
                Some(rel_type),
                direction,
                &mut |relationship| {
                    if ordinal.is_multiple_of(1024) {
                        runtime_checkpoint(self.context.task_context)?;
                    }
                    ordinal = ordinal.saturating_add(1);
                    let bytes = relationship_memory_bytes(&relationship);
                    if bytes > self.record_budget {
                        return Err(HawDBError::Execution(format!(
                            "GraphAlgorithm streaming adjacency record requires {bytes} bytes, exceeding remaining blocking_operator_bytes {}",
                            self.record_budget,
                        )));
                    }
                    let _record = self.record_account.reserve(bytes)?;
                    self.record_peak.set(self.record_peak.get().max(bytes));
                    visitor(match direction {
                        AdjacencyDirection::Outgoing => relationship.target,
                        AdjacencyDirection::Incoming => relationship.source,
                    })?;
                    Ok(ScanControl::Continue)
                },
            )?;
            if control == ScanControl::Stop {
                return Err(HawDBError::Execution(
                    "streaming analytics adjacency scan is incomplete".into(),
                ));
            }
            Ok(())
        };
        for kind in self.context.catalog.rel_types() {
            if !self.rel_types.is_empty() && !self.rel_types.iter().any(|name| name == &kind.name) {
                continue;
            }
            scan(AdjacencyDirection::Outgoing, kind.id)?;
            if undirected {
                scan(AdjacencyDirection::Incoming, kind.id)?;
            }
        }
        runtime_checkpoint(self.context.task_context)
    }
}

impl GraphAlgorithmSpec<'_> {
    pub(super) fn stream_external(
        self,
        context: GraphAlgorithmContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let definition = context
            .store
            .projected_graph_definition(self.graph_name)
            .ok_or_else(|| {
                HawDBError::Execution(format!(
                    "projected graph '{}' does not exist",
                    self.graph_name
                ))
            })?;
        let visibility = self
            .node_visibility_predicate
            .as_ref()
            .map(property_filter_from_predicate)
            .transpose()?;
        let mut tracker = OperatorMemoryTracker::with_account(
            context.memory.blocking_operator_bytes,
            context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "GraphAlgorithm",
                context.memory.blocking_operator_bytes,
            ),
        );
        let mut node_count = 0;
        let result = (|| {
            runtime_checkpoint(context.task_context)?;
            let levels = match self.algorithm {
                GraphAlgorithmKind::PageRank => 1,
                GraphAlgorithmKind::Louvain => self
                    .options
                    .max_levels
                    .unwrap_or(LouvainOptions::default().max_levels)
                    .max(1),
            };
            let result_per_node = match self.algorithm {
                GraphAlgorithmKind::PageRank => {
                    std::mem::size_of::<hawdb_analytics::PageRankScore>() * 2
                }
                GraphAlgorithmKind::Louvain => {
                    std::mem::size_of::<hawdb_analytics::HierarchicalCommunityAssignment>()
                        .saturating_mul(levels)
                        .saturating_mul(2)
                }
            };
            let state_per_node = match self.algorithm {
                GraphAlgorithmKind::PageRank => PAGE_RANK_NODE_STATE_BYTES,
                GraphAlgorithmKind::Louvain => LOUVAIN_NODE_STATE_BYTES,
            };
            let per_node = state_per_node.saturating_add(result_per_node);
            let mut nodes = Vec::new();
            let mut ordinal = 0usize;
            let control = context.store.visit_nodes_owned(None, &mut |node| {
                if ordinal.is_multiple_of(1024) {
                    runtime_checkpoint(context.task_context)?;
                }
                ordinal = ordinal.saturating_add(1);
                if !(definition.node_labels.is_empty()
                    || node.labels.iter().any(|label| {
                        context.catalog.label_name(*label).is_some_and(|name| {
                            definition
                                .node_labels
                                .iter()
                                .any(|selected| selected == name)
                        })
                    }))
                    || visibility
                        .as_ref()
                        .is_some_and(|filter| !node_matches_property_filter(&node, filter))
                {
                    return Ok(ScanControl::Continue);
                }
                let transient = node_memory_bytes(&node);
                charge_graph_algorithm_memory(
                    "streaming",
                    "node scan",
                    &mut tracker,
                    per_node.saturating_add(transient),
                )?;
                nodes.push(node.id);
                tracker.release(transient);
                Ok(ScanControl::Continue)
            })?;
            if control == ScanControl::Stop {
                return Err(HawDBError::Execution(
                    "streaming analytics node scan is incomplete".into(),
                ));
            }
            nodes.sort_unstable();
            node_count = nodes.len();
            let source = EdgeSource {
                context,
                rel_types: &definition.rel_types,
                record_budget: tracker.budget_bytes.saturating_sub(tracker.used_bytes),
                record_peak: Cell::new(0),
                record_account: context.memory_ledger.account(
                    QueryMemoryClass::BlockingState,
                    "GraphAlgorithm adjacency record",
                    NonZeroUsize::new(tracker.budget_bytes.saturating_sub(tracker.used_bytes))
                        .unwrap_or(NonZeroUsize::MIN),
                ),
            };
            let graph = StreamingGraph::new(
                &source,
                nodes,
                matches!(self.algorithm, GraphAlgorithmKind::Louvain),
            )?;
            let result_rows: Result<_> = match self.algorithm {
                GraphAlgorithmKind::PageRank => {
                    let rows = graph
                        .page_rank(
                            PageRankOptions {
                                iterations: self
                                    .options
                                    .max_iterations
                                    .unwrap_or(PageRankOptions::default().iterations),
                                damping: self
                                    .options
                                    .damping
                                    .unwrap_or(PageRankOptions::default().damping),
                            },
                            context.task_context,
                        )
                        .map(AlgorithmRows::PageRank);
                    drop(graph);
                    rows
                }
                GraphAlgorithmKind::Louvain => graph
                    .hierarchical_louvain(
                        LouvainOptions {
                            max_iterations: self
                                .options
                                .max_iterations
                                .unwrap_or(LouvainOptions::default().max_iterations),
                            max_levels: levels,
                        },
                        context.task_context,
                    )
                    .map(AlgorithmRows::Louvain),
            };
            tracker.peak_bytes = tracker
                .peak_bytes
                .max(tracker.used_bytes.saturating_add(source.record_peak.get()));
            let rows = result_rows?;
            // All graph computation has completed before any result is emitted.
            // Drop graph state, retaining only the admitted scalar result vector.
            tracker.reset();
            let result_bytes = rows.memory_bytes();
            charge_graph_algorithm_memory("streaming", "result", &mut tracker, result_bytes)?;
            let mut batch = Vec::new();
            let mut batch_bytes = 0usize;
            for (ordinal, row) in rows
                .bindings(self.score_column)
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .enumerate()
            {
                if ordinal.is_multiple_of(1024) {
                    runtime_checkpoint(context.task_context)?;
                }
                let bytes = crate::binding::binding_memory_bytes(&row);
                if bytes > context.memory.batch_payload_bytes.get() {
                    return Err(HawDBError::Execution(
                        "GraphAlgorithm result row exceeds batch_payload_bytes".into(),
                    ));
                }
                if !batch.is_empty()
                    && (batch.len() == context.memory.batch_rows.get()
                        || batch_bytes.saturating_add(bytes)
                            > context.memory.batch_payload_bytes.get()
                        || tracker.would_exceed(bytes))
                {
                    tracker.release(batch_bytes);
                    batch_bytes = 0;
                    if emit(std::mem::take(&mut batch))? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                }
                charge_graph_algorithm_memory("streaming", "output batch", &mut tracker, bytes)?;
                batch_bytes = batch_bytes.saturating_add(bytes);
                batch.push(row);
            }
            if !batch.is_empty() {
                tracker.release(batch_bytes);
                return emit(batch);
            }
            Ok(BatchControl::Continue)
        })();
        let mut report = graph_algorithm_memory_report(&tracker, node_count, context.memory);
        report.operator = "GraphAlgorithmStreaming".into();
        context.observer.record_blocking_memory_report(report);
        result
    }
}

enum AlgorithmRows {
    PageRank(Vec<hawdb_analytics::PageRankScore>),
    Louvain(Vec<hawdb_analytics::HierarchicalCommunityAssignment>),
}

impl AlgorithmRows {
    fn memory_bytes(&self) -> usize {
        match self {
            Self::PageRank(rows) => {
                estimated_vec_memory_bytes::<hawdb_analytics::PageRankScore>(rows.len())
            }
            Self::Louvain(rows) => estimated_vec_memory_bytes::<
                hawdb_analytics::HierarchicalCommunityAssignment,
            >(rows.len()),
        }
    }

    fn bindings(self, score_column: &str) -> Box<dyn Iterator<Item = Binding> + '_> {
        match self {
            Self::PageRank(rows) => Box::new(rows.into_iter().map(|row| Binding {
                values: BTreeMap::from([
                    ("node".into(), Value::Int(row.node.0 as i64)),
                    (score_column.into(), Value::Float(row.score)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            })),
            Self::Louvain(rows) => Box::new(rows.into_iter().map(|row| Binding {
                values: BTreeMap::from([
                    ("node".into(), Value::Int(row.node.0 as i64)),
                    ("level".into(), Value::Int(row.level as i64)),
                    ("louvain_id".into(), Value::Int(row.community.0 as i64)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            })),
        }
    }
}
