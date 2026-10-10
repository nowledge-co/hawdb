// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Canonical graph relevance producer. No projection or host-supplied score
//! participates in identity, relevance, or the private scoring annotations.

use super::*;
use crate::graph_seed::GraphSeedScorer;
use crate::store::admit_graph_read;
use hawdb_storage::read_view::{AdmittedVec, GraphReadAdmission, GraphReadAllocation};
use std::cmp::Ordering;

pub(super) struct GraphSeedScanSpec<'a> {
    pub query_parameter: &'a str,
    pub label: &'a str,
    pub variable: &'a str,
    pub score_column: &'a str,
    pub top_k: usize,
    pub node_visibility_predicate: Option<&'a Predicate>,
}

struct Candidate {
    node: NodeRecord,
    score: f64,
    allocation: Box<dyn GraphReadAllocation>,
}

fn candidate_order(left: &Candidate, right: &Candidate) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then(left.node.id.cmp(&right.node.id))
}

impl GraphSeedScanSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        runtime_checkpoint(context.task_context)?;
        let Some(Value::String(query)) = context.parameters.get(self.query_parameter) else {
            return Err(HawDBError::Semantic(
                "graph seed query must be a string parameter".into(),
            ));
        };
        let Some(label_id) = context.catalog.label_id(self.label) else {
            return Ok(BatchControl::Continue);
        };
        if self.top_k == 0 || execution_limit.output_rows == Some(0) {
            return Ok(BatchControl::Continue);
        }
        let account = context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "GraphSeedScan",
            context.memory.blocking_operator_bytes,
        );
        let source_account = context
            .kernel_context()
            .source_account("GraphSeedScan source")
            .with_retained_state(account.clone());
        let scorer = GraphSeedScorer::new(query, &source_account, context.task_context)?;
        if scorer.is_empty() {
            return Ok(BatchControl::Continue);
        }
        let mut allocate_slots = |bytes| admit_graph_read(&account, context.task_context, bytes);
        let mut candidates =
            AdmittedVec::<Candidate>::new(&GraphReadAdmission::new(&mut allocate_slots))?;
        let mut visited = 0usize;
        context.store.visit_nodes_with_allocation(
            Some(label_id),
            &mut |bytes| {
                runtime_checkpoint(context.task_context)?;
                // Maps, aliases, private annotations, and overlapping output row
                // capacity remain covered through the downstream callback.
                let bytes = bytes
                    .checked_add(4096)
                    .and_then(|bytes| bytes.checked_add(self.variable.len()))
                    .and_then(|bytes| bytes.checked_add(self.score_column.len()))
                    .ok_or_else(|| {
                        HawDBError::Execution("graph seed binding size overflow".into())
                    })?;
                admit_graph_read(&source_account, context.task_context, bytes).map(Some)
            },
            &mut |input| {
                runtime_checkpoint(context.task_context)?;
                visited = visited.saturating_add(1);
                let (node, allocation) = input.into_parts();
                let mut binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(self.variable.to_string(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = self.node_visibility_predicate
                    && !evaluate_predicate_observed(
                        predicate,
                        context.catalog,
                        context.store,
                        &binding,
                        context.observer,
                        crate::store::AdjacencyReadMemory {
                            budget_bytes: context.memory.query_memory_bytes.get(),
                            account: Some(&source_account),
                        },
                        context.task_context,
                    )?
                {
                    return Ok(ScanControl::Continue);
                }
                let node = binding
                    .nodes
                    .remove(self.variable)
                    .expect("canonical producer owns its node");
                let score = scorer.score(&node)?.score;
                if score <= 0.0 {
                    return Ok(ScanControl::Continue);
                }
                let mut candidate = Candidate {
                    node,
                    score,
                    allocation,
                };
                if candidates.as_slice().len() < self.top_k {
                    candidate.allocation.retain_state()?;
                    candidates.try_push(candidate)?;
                } else {
                    let (index, worst) = candidates
                        .as_slice()
                        .iter()
                        .enumerate()
                        .max_by(|(_, left), (_, right)| candidate_order(left, right))
                        .expect("nonzero topK is full");
                    if candidate_order(&candidate, worst).is_lt() {
                        // The displaced node is no longer retained. Drop it
                        // before transferring the replacement so a full K-row
                        // state is never spuriously charged as K + 1.
                        drop(candidates.swap_remove(index));
                        candidate.allocation.retain_state()?;
                        candidates.try_push(candidate)?;
                    }
                }
                Ok(ScanControl::Continue)
            },
        )?;
        runtime_checkpoint(context.task_context)?;
        context
            .observer
            .record_scan_pruning_report(ScanPruningReport {
                target_kind: ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::FullLabelScan,
                pruned: false,
                exact_empty: visited == 0,
                candidate_count_before_pruning: visited,
                pruned_candidate_count: 0,
                candidate_count_before_filter: visited,
                output_count: candidates.as_slice().len(),
                filtered_out_count: visited.saturating_sub(candidates.as_slice().len()),
            });
        let (mut candidates, _candidate_slots) = candidates.into_parts();
        candidates.sort_unstable_by(candidate_order);
        let mut allocate_permits = |bytes| admit_graph_read(&account, context.task_context, bytes);
        let mut permits = AdmittedVec::<Box<dyn GraphReadAllocation>>::new(
            &GraphReadAdmission::new(&mut allocate_permits),
        )?;
        let mut output = AccountedBindingBatch::with_ledger(
            "GraphSeedScan",
            context.memory.batch_rows.get(),
            context.memory.batch_payload_bytes,
            context.memory_ledger,
        );
        for candidate in candidates
            .into_iter()
            .take(execution_limit.output_rows.unwrap_or(usize::MAX))
        {
            runtime_checkpoint(context.task_context)?;
            let mut values =
                BTreeMap::from([(self.score_column.to_string(), Value::Float(candidate.score))]);
            if context.observer.seed_graph_scoring_input().is_some() {
                crate::scoring::annotate_seed(&mut values, candidate.score);
            }
            let binding = Binding {
                values,
                nodes: BTreeMap::from([(self.variable.to_string(), candidate.node)]),
                relationships: BTreeMap::new(),
            };
            let bytes = binding_memory_bytes(&binding);
            output.check_row_size(bytes)?;
            if output.would_exceed_payload(bytes) && !output.is_empty() {
                let control = output.emit(emit)?;
                permits.clear();
                if control == BatchControl::Stop {
                    return Ok(control);
                }
            }
            permits.try_push(candidate.allocation)?;
            if output.push(binding, emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            if output.is_full() {
                let control = output.emit(emit)?;
                permits.clear();
                if control == BatchControl::Stop {
                    return Ok(control);
                }
            }
        }
        runtime_checkpoint(context.task_context)?;
        output.emit(emit)
    }
}
