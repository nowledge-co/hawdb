// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Necessary positive internal-ID conjuncts only. Keep residual predicates in
//! their original owner; OR/NOT, other variables and large lists use the scan.
use super::*;
use hawdb_storage::read_view::{
    AdmittedNodeRead, ControlledGraphReadAllocator, GraphReadAllocation,
};

const MAX_IDS: usize = 1024;

pub(crate) fn predicate_values<'a>(
    predicate: &'a Predicate,
    variable: &str,
) -> Option<&'a [Value]> {
    fn find<'a>(p: &'a Predicate, v: &str, depth: usize) -> Option<&'a [Value]> {
        if depth > 32 {
            return None;
        }
        match p {
            Predicate::And(parts) => parts.iter().find_map(|p| find(p, v, depth + 1)),
            Predicate::IdEq { variable, value } if variable == v => {
                Some(std::slice::from_ref(value))
            }
            Predicate::IdIn { variable, values } if variable == v => Some(values),
            _ => None,
        }
    }
    find(predicate, variable, 0).filter(|values| eligible(values))
}
pub(super) fn filter_values(filter: &PropertyFilter) -> Option<&[Value]> {
    fn find(p: &PropertyFilter, depth: usize) -> Option<&[Value]> {
        if depth > 32 {
            return None;
        }
        match p {
            PropertyFilter::And(parts) => parts.iter().find_map(|p| find(p, depth + 1)),
            PropertyFilter::IdEq { value } => Some(std::slice::from_ref(value)),
            PropertyFilter::IdIn { values } => Some(values),
            _ => None,
        }
    }
    find(filter, 0).filter(|values| eligible(values))
}
fn eligible(values: &[Value]) -> bool {
    values.len() <= MAX_IDS
        && values
            .iter()
            .all(|v| matches!(v, Value::Int(id) if *id >= 0))
}

/// Retain an admitted sorted/deduplicated key directory, preserving storage's
/// ascending-ID order and set semantics without owning any unrelated records.
pub(crate) struct Candidates {
    pub(crate) ids: Vec<NodeId>,
    _allocation: Box<dyn GraphReadAllocation>,
}
impl Candidates {
    pub(crate) fn new(
        values: &[Value],
        account: &QueryMemoryAccount,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        runtime_checkpoint(task)?;
        let allocation = crate::store::admit_graph_read(
            account,
            task,
            values.len().saturating_mul(std::mem::size_of::<NodeId>()),
        )?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(values.len()).map_err(|e| {
            HawDBError::Execution(format!("internal-ID key allocation failed: {e}"))
        })?;
        if ids.capacity() > values.len() {
            return Err(HawDBError::Execution(
                "internal-ID key allocation exceeds admission".into(),
            ));
        }
        for value in values {
            runtime_checkpoint(task)?;
            let Value::Int(id) = value else {
                return Err(HawDBError::Execution(
                    "invalid internal-ID candidate".into(),
                ));
            };
            ids.push(NodeId(*id as u64));
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Self {
            ids,
            _allocation: allocation,
        })
    }
    pub(crate) fn visit(
        &self,
        store: &dyn GraphExecutionRead,
        labels: Option<&[LabelId]>,
        task: Option<&RuntimeTaskContext>,
        admit: &mut ControlledGraphReadAllocator<'_>,
        consume: &mut dyn FnMut(NodeRecord, Box<dyn GraphReadAllocation>) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for id in &self.ids {
            runtime_checkpoint(task)?;
            match store.node_with_allocation(*id, labels, admit)? {
                AdmittedNodeRead::Missing => {}
                AdmittedNodeRead::Stopped => return Ok(ScanControl::Stop),
                AdmittedNodeRead::Node(input) => {
                    let (node, allocation) = input.into_parts();
                    if consume(node, allocation)? == ScanControl::Stop {
                        return Ok(ScanControl::Stop);
                    }
                }
            }
        }
        Ok(ScanControl::Continue)
    }
}
pub(crate) fn report(
    store: &dyn GraphExecutionRead,
    label: Option<LabelId>,
    values: usize,
    visited: usize,
    emitted: usize,
    control: ScanControl,
) -> ScanPruningReport {
    let total = store.node_count_for_label(label);
    ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: label,
        rel_type_id: None,
        strategy: if values == 1 {
            ScanPruningStrategy::IdEq
        } else {
            ScanPruningStrategy::IdIn
        },
        pruned: true,
        exact_empty: visited == 0 && control == ScanControl::Continue,
        candidate_count_before_pruning: total,
        pruned_candidate_count: total.saturating_sub(visited),
        candidate_count_before_filter: visited,
        output_count: emitted,
        filtered_out_count: visited.saturating_sub(emitted),
    }
}
