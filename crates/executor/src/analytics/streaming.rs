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
use crate::binding::value_memory_bytes;
use hawdb_analytics::{AnalyticsEdgeSource, StreamingGraph};
use hawdb_storage::{adjacency::AdjacencyDirection, NodeId};

// Includes original IDs, member vectors, overlapping contraction state, dense
// neighbor membership, algorithm scratch, and Vec capacity growth. Results are
// accounted separately, including all requested hierarchy levels.
const LOUVAIN_NODE_STATE_BYTES: usize = 576;
const PAGE_RANK_NODE_STATE_BYTES: usize = 192;

struct EdgeSource<'a> {
    context: GraphAlgorithmContext<'a>,
    rel_types: &'a [String],
    relationship_predicates:
        &'a BTreeMap<String, hawdb_storage::projection::ProjectedRelationshipPredicate>,
    record_budget: usize,
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
            let mut admit = |bytes| {
                if bytes > self.record_budget {
                    return Err(HawDBError::Execution(format!("GraphAlgorithm streaming adjacency record requires {bytes} bytes, exceeding query_memory_bytes {}", self.record_budget)));
                }
                crate::store::admit_graph_read(
                    &self.record_account,
                    self.context.task_context,
                    bytes,
                )
                .map(Some)
            };
            let control = self
                .context
                .store
                .visit_ordered_adjacent_relationships_with_allocation(
                    node,
                    Some(rel_type),
                    direction,
                    crate::store::AdjacencyReadMemory {
                        budget_bytes: self.record_budget,
                        account: Some(&self.record_account),
                    },
                    &mut admit,
                    &mut |input| {
                        let (relationship, _allocation) = input.into_parts();
                        if ordinal.is_multiple_of(1024) {
                            runtime_checkpoint(self.context.task_context)?;
                        }
                        ordinal = ordinal.saturating_add(1);
                        if self
                            .context
                            .catalog
                            .rel_type_name(relationship.rel_type)
                            .and_then(|name| self.relationship_predicates.get(name))
                            .is_some_and(|predicate| !predicate.matches(&relationship.properties))
                        {
                            return Ok(ScanControl::Continue);
                        }
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
        self.validate_options()?;
        let visibility = self
            .node_visibility_predicate
            .as_ref()
            .map(property_filter_from_predicate)
            .transpose()?;
        let state_account = context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "GraphAlgorithm",
            context.memory.blocking_operator_bytes,
        );
        let mut tracker = OperatorMemoryTracker::with_account(
            context.memory.blocking_operator_bytes,
            state_account.clone(),
        );
        let source_account = context
            .memory_ledger
            .source_account(
                "GraphAlgorithm streaming source",
                context.memory.query_memory_bytes,
                context.memory.blocking_operator_bytes,
            )
            .with_retained_state(state_account);
        let mut node_count = 0;
        let result = (|| {
            runtime_checkpoint(context.task_context)?;
            let levels = match self.algorithm {
                GraphAlgorithmKind::PageRank => 1,
                GraphAlgorithmKind::Louvain => {
                    if self.louvain_options()?.hierarchy {
                        self.louvain_options()?.max_levels.max(1)
                    } else {
                        1
                    }
                }
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
            let mut consume = |node: NodeRecord| {
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
                charge_graph_algorithm_memory("streaming", "node scan", &mut tracker, per_node)?;
                nodes.push(node.id);
                Ok(ScanControl::Continue)
            };
            let mut admit = |bytes| {
                crate::store::admit_graph_read(&source_account, context.task_context, bytes)
                    .map(Some)
            };
            let control = if visibility.is_none() {
                context.store.visit_projected_nodes_with_allocation(
                    None,
                    &BTreeSet::new(),
                    &mut admit,
                    &mut |input| {
                        let (node, _allocation) = input.into_parts();
                        consume(NodeRecord {
                            id: node.id,
                            labels: node.labels,
                            properties: node.properties,
                        })
                    },
                )?
            } else {
                context
                    .store
                    .visit_nodes_with_allocation(None, &mut admit, &mut |input| {
                        let (node, _allocation) = input.into_parts();
                        consume(node)
                    })?
            };
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
                relationship_predicates: &definition.relationship_predicates,
                record_budget: context.memory.query_memory_bytes.get(),
                record_account: source_account.clone(),
            };
            let graph = StreamingGraph::new(
                &source,
                nodes,
                matches!(self.algorithm, GraphAlgorithmKind::Louvain),
            )?;
            let result_rows: Result<_> = match self.algorithm {
                GraphAlgorithmKind::PageRank => {
                    let rows = graph
                        .page_rank_procedure(self.page_rank_options()?, context.task_context)
                        .map(AlgorithmRows::PageRank);
                    drop(graph);
                    rows
                }
                GraphAlgorithmKind::Louvain => graph
                    .louvain_procedure(self.louvain_options()?, context.task_context)
                    .map(AlgorithmRows::Louvain),
            };
            let rows = result_rows?;
            // All graph computation has completed before any result is emitted.
            // Drop graph state, retaining only the admitted scalar result vector.
            tracker.reset();
            let result_bytes = rows.memory_bytes();
            charge_graph_algorithm_memory("streaming", "result", &mut tracker, result_bytes)?;
            // Hydrate once per original node before emitting, so a late lookup
            // failure cannot expose a prefix of identity-bearing algorithm rows.
            // Canonical output identities use the query allowance independently
            // of the scalar algorithm result vector's retained-state cap.
            let mut identity_tracker = OperatorMemoryTracker::with_account(
                context.memory.query_memory_bytes,
                context.memory_ledger.account(
                    QueryMemoryClass::ResultMaterialization,
                    "GraphAlgorithm streaming identity staging",
                    context.memory.query_memory_bytes,
                ),
            );
            let mut identities = BTreeMap::new();
            if self.return_node_identity {
                identity_tracker.try_charge(1024)?;
                for (ordinal, node) in rows
                    .node_ids()
                    .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                    .enumerate()
                {
                    if ordinal.is_multiple_of(1024) {
                        runtime_checkpoint(context.task_context)?;
                    }
                    if identities.contains_key(&node) {
                        continue;
                    }
                    let mut values = BTreeMap::new();
                    let hydration = append_node_identity(
                        &mut values,
                        context,
                        node,
                        &definition.node_labels,
                        &source_account,
                    )?;
                    let id = values
                        .remove("node_id")
                        .expect("identity helper fills node_id");
                    let label = values
                        .remove("node_label")
                        .expect("identity helper fills node_label");
                    let bytes = std::mem::size_of::<(NodeId, (Value, Value))>()
                        .saturating_mul(3)
                        .saturating_add(value_memory_bytes(&id))
                        .saturating_add(value_memory_bytes(&label));
                    identity_tracker.try_charge(bytes)?;
                    identities.insert(node, (id, label));
                    drop(hydration);
                }
            }
            let mut batch = Vec::new();
            let mut batch_bytes = 0usize;
            let mut output_tracker = OperatorMemoryTracker::with_account(
                context.memory.query_memory_bytes,
                context.memory_ledger.account(
                    QueryMemoryClass::ResultMaterialization,
                    "GraphAlgorithm streaming output",
                    context.memory.query_memory_bytes,
                ),
            );
            for (ordinal, mut row) in rows
                .bindings(self.score_column)
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .enumerate()
            {
                if ordinal.is_multiple_of(1024) {
                    runtime_checkpoint(context.task_context)?;
                }
                let identity = if self.return_node_identity {
                    let Some(Value::Int(node)) = row.values.get("node") else {
                        return Err(HawDBError::StorageIntegrity(
                            "graph algorithm returned an invalid node identity".into(),
                        ));
                    };
                    Some(&identities[&NodeId(*node as u64)])
                } else {
                    None
                };
                let bytes = crate::binding::binding_memory_bytes_with_values(
                    &row,
                    row.values
                        .iter()
                        .map(|(name, value)| (name.as_str(), value))
                        .chain(
                            identity
                                .into_iter()
                                .flat_map(|(id, label)| [("node_id", id), ("node_label", label)]),
                        ),
                );
                if bytes > context.memory.batch_payload_bytes.get() {
                    return Err(HawDBError::Execution(
                        "GraphAlgorithm result row exceeds batch_payload_bytes".into(),
                    ));
                }
                if !batch.is_empty()
                    && (batch.len() == context.memory.batch_rows.get()
                        || batch_bytes.saturating_add(bytes)
                            > context.memory.batch_payload_bytes.get()
                        || output_tracker.would_exceed(bytes))
                {
                    let control = emit(std::mem::take(&mut batch))?;
                    output_tracker.reset();
                    batch_bytes = 0;
                    if control == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    runtime_checkpoint(context.task_context)?;
                }
                output_tracker.try_charge(bytes)?;
                if let Some((id, label)) = identity {
                    row.values.insert("node_id".into(), id.clone());
                    row.values.insert("node_label".into(), label.clone());
                }
                batch_bytes = batch_bytes.saturating_add(bytes);
                crate::pipeline::reserve_binding_slot(&mut batch);
                batch.push(row);
            }
            if !batch.is_empty() {
                let control = emit(batch)?;
                runtime_checkpoint(context.task_context)?;
                return Ok(control);
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
    fn node_ids(&self) -> Box<dyn Iterator<Item = NodeId> + '_> {
        match self {
            Self::PageRank(rows) => Box::new(rows.iter().map(|row| row.node)),
            Self::Louvain(rows) => Box::new(rows.iter().map(|row| row.node)),
        }
    }

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
