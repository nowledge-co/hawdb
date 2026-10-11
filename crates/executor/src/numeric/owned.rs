// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Selected owned numeric inputs. Source permits follow buffered rows, and
//! preflight can flush/stop before the next selected payload is decoded.

use super::{
    lending::{numeric_batch_scratch_bytes, OwnedNumericBatchBuffer},
    LendingNumericScan, NumericBatchEmitter, NumericExecutionContext, NumericFragment,
};
use crate::pipeline::{runtime_checkpoint, BatchControl, BindingBatch};
use crate::store::{admit_graph_read, ScanControl};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryClass};
use hawdb_core::{LabelId, Result};
use hawdb_plan_cypher::{Projection, ProjectionExpression};
use hawdb_storage::read_view::{
    AdmittedProjectedNode, AdmittedVec, GraphReadAdmission, GraphReadAllocation,
};
use hawdb_storage::NodeRecord;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::BTreeSet;

struct AdmittedNumericNode {
    node: NodeRecord,
    allocation: Box<dyn GraphReadAllocation>,
}

impl From<AdmittedProjectedNode> for AdmittedNumericNode {
    fn from(input: AdmittedProjectedNode) -> Self {
        let (node, allocation) = input.into_parts();
        Self {
            node: NodeRecord {
                id: node.id,
                labels: node.labels,
                properties: node.properties,
            },
            allocation,
        }
    }
}

impl Borrow<NodeRecord> for AdmittedNumericNode {
    fn borrow(&self) -> &NodeRecord {
        &self.node
    }
}

fn selected_properties(fragment: NumericFragment<'_>, items: &[Projection]) -> BTreeSet<String> {
    std::iter::once(fragment.property)
        .chain(items.iter().filter_map(|item| match &item.expression {
            ProjectionExpression::Property { property, .. } => Some(property.as_str()),
            _ => None,
        }))
        .map(str::to_owned)
        .collect()
}

struct OwnedNumericRows<'plan, 'context, 'emit> {
    nodes: AdmittedVec<AdmittedNumericNode>,
    buffered_bytes: usize,
    batch_rows: usize,
    stopped: bool,
    selection_account: QueryMemoryAccount,
    emitter: NumericBatchEmitter<'plan, 'context, 'emit>,
}

impl OwnedNumericRows<'_, '_, '_> {
    fn flush(&mut self) -> Result<()> {
        if self.nodes.as_slice().is_empty() {
            return Ok(());
        }
        let _selection = self.selection_account.reserve(numeric_batch_scratch_bytes(
            self.nodes.as_slice().len(),
            false,
            true,
        ))?;
        let result = self.emitter.emit_nodes(self.nodes.as_slice());
        self.nodes.clear();
        self.buffered_bytes = 0;
        self.stopped = result? == BatchControl::Stop || self.emitter.limit_reached();
        Ok(())
    }
}

pub fn stream_owned_numeric_nodes<'plan>(
    fragment: NumericFragment<'plan>,
    items: &'plan [Projection],
    label_id: LabelId,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let properties = selected_properties(fragment, items);
    let input_account = context.memory_ledger.source_account(
        "numeric owned input",
        context.memory.query_memory_bytes,
        context.memory.blocking_operator_bytes,
    );
    let vector_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "numeric owned row storage",
        context.memory.batch_payload_bytes,
    );
    let nodes = {
        let mut allocate = |bytes| admit_graph_read(&vector_account, context.task_context, bytes);
        let admission = GraphReadAdmission::new(&mut allocate);
        AdmittedVec::new(&admission)?
    };
    let state = RefCell::new(OwnedNumericRows {
        nodes,
        buffered_bytes: 0,
        batch_rows: context.memory.batch_rows.get(),
        stopped: false,
        selection_account: context.memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "numeric owned selection",
            context.memory.batch_payload_bytes,
        ),
        emitter: NumericBatchEmitter::new(
            fragment,
            items,
            execution_limit,
            (context.memory, context.memory_ledger),
            context.task_context,
            context.observer,
            emit,
        ),
    });
    context.store.visit_projected_nodes_with_allocation(
        Some(label_id),
        &properties,
        &mut |bytes| {
            runtime_checkpoint(context.task_context)?;
            let mut state = state.borrow_mut();
            if !state.stopped
                && state.buffered_bytes.saturating_add(bytes)
                    > context.memory.batch_payload_bytes.get()
            {
                state.flush()?;
            }
            if state.stopped {
                return Ok(None);
            }
            admit_graph_read(&input_account, context.task_context, bytes).map(Some)
        },
        &mut |input| {
            let mut state = state.borrow_mut();
            let node = AdmittedNumericNode::from(input);
            let bytes = node.allocation.bytes();
            state.nodes.try_push(node)?;
            state.buffered_bytes = state.buffered_bytes.saturating_add(bytes);
            if state.nodes.as_slice().len() == state.batch_rows {
                state.flush()?;
            }
            Ok(if state.stopped {
                ScanControl::Stop
            } else {
                ScanControl::Continue
            })
        },
    )?;
    let mut state = state.into_inner();
    if !state.stopped {
        state.flush()?;
    }
    Ok((state.emitter.emitted, state.stopped))
}

pub fn stream_owned_typed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: LabelId,
    scan: LendingNumericScan,
    context: NumericExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let properties = selected_properties(fragment, items);
    let scratch_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "numeric owned typed scratch",
        context.memory.batch_payload_bytes,
    );
    // Cover scalar columns, optional IDs, validity words and retained selection
    // capacity before the typed buffer or its selection vector allocates.
    let _scratch = scratch_account.reserve(numeric_batch_scratch_bytes(
        scan.batch_rows,
        scan.needs_node_ids,
        true,
    ))?;
    let input_account = context.memory_ledger.source_account(
        "numeric owned typed input",
        context.memory.query_memory_bytes,
        context.memory.blocking_operator_bytes,
    );
    let mut buffer = OwnedNumericBatchBuffer::new(fragment, scan.batch_rows, scan.needs_node_ids);
    let mut emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        (context.memory, context.memory_ledger),
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    context.store.visit_projected_nodes_with_allocation(
        Some(label_id),
        &properties,
        &mut |bytes| admit_graph_read(&input_account, context.task_context, bytes).map(Some),
        &mut |input| {
            let node = AdmittedNumericNode::from(input);
            // The remaining allocation field lives through scalar extraction,
            // batch consumption and any error/Stop exit from this callback.
            buffer.push_owned(node.node)?;
            if buffer.is_full() {
                stopped = emitter.emit_typed(buffer.take_batch())? == BatchControl::Stop;
                buffer.clear();
            }
            stopped |= emitter.limit_reached();
            Ok(if stopped {
                ScanControl::Stop
            } else {
                ScanControl::Continue
            })
        },
    )?;
    if !stopped && !buffer.is_empty() {
        stopped = emitter.emit_typed(buffer.take_batch())? == BatchControl::Stop;
    }
    Ok((emitter.emitted, stopped))
}
