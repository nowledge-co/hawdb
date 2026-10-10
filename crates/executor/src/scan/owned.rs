// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Full-node scan ownership. Source permits stay with buffered bindings,
//! including the consumer callback; preflight may flush before owning a row.

use super::*;
use crate::store::admit_graph_read;
use crate::QueryMemoryClass;
use hawdb_storage::read_view::{
    AdmittedVec, ControlledGraphReadAllocator, GraphReadAdmission, GraphReadAllocation,
};
use std::cell::RefCell;

// Cover the outer binding map and row slot in addition to storage's node
// allocation. The source reservation survives the output batch's callback.
fn binding_overhead(variable: &str) -> usize {
    1024usize
        .saturating_add(std::mem::size_of::<Binding>().saturating_mul(2))
        .saturating_add(variable.len())
}

fn allocation_vector(
    context: NodeScanContext<'_>,
) -> Result<AdmittedVec<Box<dyn GraphReadAllocation>>> {
    let account = context.memory_account.sibling(
        QueryMemoryClass::BlockingState,
        "full node scan permit storage",
        context.memory_budget,
    );
    let mut allocate = |bytes| admit_graph_read(&account, context.task_context, bytes);
    AdmittedVec::new(&GraphReadAdmission::new(&mut allocate))
}

struct ScanVisit {
    control: ScanControl,
    fallback_label: Option<Option<LabelId>>,
}

fn visit_full_nodes(
    spec: NodeScanSpec<'_>,
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
    admit: &mut ControlledGraphReadAllocator<'_>,
    consume: &mut dyn FnMut(NodeRecord, Box<dyn GraphReadAllocation>) -> Result<ScanControl>,
) -> Result<ScanVisit> {
    let exact_label = exact_scan_label_id(context.catalog, spec.label);
    if !context.store.is_out_of_core()
        && let Some(label_id) = exact_label
        && context
            .store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= context.memory_budget.get()
    {
        let scan = context.pruned_scan(label_id, spec.property_filter)?;
        observer.record_scan_pruning_report(scan.report.clone());
        for node in scan.nodes {
            runtime_checkpoint(context.task_context)?;
            let Some(allocation) = admit(hawdb_core::ids::node_allocation_bytes(&node))? else {
                return Ok(ScanVisit {
                    control: ScanControl::Stop,
                    fallback_label: None,
                });
            };
            if consume(node.into_owned(), allocation)? == ScanControl::Stop {
                return Ok(ScanVisit {
                    control: ScanControl::Stop,
                    fallback_label: None,
                });
            }
        }
        return Ok(ScanVisit {
            control: ScanControl::Continue,
            fallback_label: None,
        });
    }
    let label_id = exact_label.flatten();
    let control = context
        .store
        .visit_nodes_with_allocation(label_id, admit, &mut |input| {
            let (node, allocation) = input.into_parts();
            consume(node, allocation)
        })?;
    Ok(ScanVisit {
        control,
        fallback_label: Some(label_id),
    })
}

fn matches_scan(spec: NodeScanSpec<'_>, context: NodeScanContext<'_>, node: &NodeRecord) -> bool {
    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    node_matches_label_pattern(node, label_ids.as_deref())
        && spec
            .property_filter
            .is_none_or(|filter| node_matches_property_filter(node, filter))
}

fn record_fallback(
    visit: &ScanVisit,
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
    emitted: usize,
) {
    if let Some(label_id) = visit.fallback_label {
        let candidate_count = context.store.node_count_for_label(label_id);
        observer.record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Node,
            label_id,
            rel_type_id: None,
            strategy: ScanPruningStrategy::FullLabelScan,
            pruned: false,
            exact_empty: candidate_count == 0,
            candidate_count_before_pruning: candidate_count,
            pruned_candidate_count: 0,
            candidate_count_before_filter: candidate_count,
            output_count: emitted,
            filtered_out_count: candidate_count.saturating_sub(emitted),
        });
    }
}

struct OwnedNodeBatch<'plan, 'context, 'consumer> {
    spec: NodeScanSpec<'plan>,
    context: NodeScanContext<'context>,
    predicate: &'consumer mut dyn FnMut(&Binding) -> Result<bool>,
    emit: &'consumer mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    batch: AccountedBindingBatch,
    allocations: AdmittedVec<Box<dyn GraphReadAllocation>>,
    emitted: usize,
    stopped: bool,
}

impl OwnedNodeBatch<'_, '_, '_> {
    fn flush(&mut self) -> Result<()> {
        let result = self.batch.emit(self.emit);
        // The consumer has finished using this batch's nodes. Capacity of the
        // permit vector remains owned and admitted after clearing its entries.
        self.allocations.clear();
        self.stopped |= result? == BatchControl::Stop;
        Ok(())
    }

    fn admit(&mut self, node_bytes: usize) -> Result<Option<Box<dyn GraphReadAllocation>>> {
        runtime_checkpoint(self.context.task_context)?;
        let bytes = node_bytes.saturating_add(binding_overhead(self.spec.variable));
        if !self.stopped
            && (!self.context.memory_account.can_reserve(bytes)
                || self.batch.would_exceed_payload(bytes))
        {
            self.flush()?;
        }
        if self.stopped {
            return Ok(None);
        }
        admit_graph_read(
            self.context.memory_account,
            self.context.task_context,
            bytes,
        )
        .map(Some)
    }

    fn push(
        &mut self,
        node: NodeRecord,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(self.context.task_context)?;
        if !matches_scan(self.spec, self.context, &node) {
            return Ok(ScanControl::Continue);
        }
        let binding = node_binding(self.spec.variable, node);
        if !(self.predicate)(&binding)? {
            return Ok(ScanControl::Continue);
        }
        self.batch.check_row_size(binding_memory_bytes(&binding))?;
        self.allocations.try_push(allocation)?;
        if self.batch.push(binding, self.emit)? == BatchControl::Stop {
            self.stopped = true;
            return Ok(ScanControl::Stop);
        }
        self.emitted = self.emitted.saturating_add(1);
        if self.batch.is_full() {
            self.flush()?;
        }
        self.stopped |= self.context.execution_limit.is_reached(self.emitted);
        Ok(if self.stopped {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    }
}

pub(super) fn stream(
    spec: NodeScanSpec<'_>,
    context: NodeScanContext<'_>,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let state = RefCell::new(OwnedNodeBatch {
        spec,
        context,
        predicate,
        emit,
        batch: context.output_batch("NodeScanExec"),
        allocations: allocation_vector(context)?,
        emitted: 0,
        stopped: false,
    });
    let visit = visit_full_nodes(
        spec,
        context,
        observer,
        &mut |bytes| state.borrow_mut().admit(bytes),
        &mut |node, allocation| state.borrow_mut().push(node, allocation),
    )?;
    let mut state = state.into_inner();
    record_fallback(&visit, context, observer, state.emitted);
    state.flush()?;
    Ok(if state.stopped || visit.control == ScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub(super) fn materialize(
    spec: NodeScanSpec<'_>,
    context: NodeScanContext<'_>,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    observer: &dyn ExecutionObserver,
) -> Result<Vec<Binding>> {
    let mut output = Vec::new();
    let mut allocations = allocation_vector(context)?;
    // The source permits account for the actual retained nodes, binding maps
    // and row slots. The logical operator limit still bounds the returned set.
    let mut tracker = OperatorMemoryTracker::new(context.memory_budget);
    let visit = visit_full_nodes(
        spec,
        context,
        observer,
        &mut |bytes| {
            admit_graph_read(
                context.memory_account,
                context.task_context,
                bytes.saturating_add(binding_overhead(spec.variable)),
            )
            .map(Some)
        },
        &mut |node, allocation| {
            runtime_checkpoint(context.task_context)?;
            if !matches_scan(spec, context, &node) {
                return Ok(ScanControl::Continue);
            }
            let binding = node_binding(spec.variable, node);
            if !predicate(&binding)? {
                return Ok(ScanControl::Continue);
            }
            allocations.try_push(allocation)?;
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)?;
            Ok(if context.execution_limit.is_reached(output.len()) {
                ScanControl::Stop
            } else {
                ScanControl::Continue
            })
        },
    )?;
    record_fallback(&visit, context, observer, output.len());
    Ok(output)
}
