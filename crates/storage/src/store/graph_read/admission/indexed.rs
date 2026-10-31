// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Merge resident scalar postings without retaining a result-sized ID set.

use super::*;
use crate::read_view::{AdmittedVec, GraphReadAdmission};
use std::cmp::{Ordering, Reverse};
use std::collections::{btree_set, BinaryHeap};

struct PostingCursor<'a> {
    head: NodeId,
    remaining: btree_set::Iter<'a, NodeId>,
}

impl PartialEq for PostingCursor<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head
    }
}
impl Eq for PostingCursor<'_> {}
impl PartialOrd for PostingCursor<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PostingCursor<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.head.cmp(&other.head)
    }
}

fn work(admission: &ProjectionAdmission<'_>) -> Result<()> {
    admission.work.check_work_limit()?;
    admission
        .checkpoint()
        .and_then(|()| admission.charge_work(1))
        .map_err(canonical_segment_error)
}

fn advance(
    postings: &mut btree_set::Iter<'_, NodeId>,
    admission: &ProjectionAdmission<'_>,
) -> Result<Option<NodeId>> {
    // Charge attempts, including end-of-list, before touching the iterator.
    work(admission)?;
    Ok(postings.next().copied())
}

impl GraphStore {
    /// Optional admitted resident-index access. The caller must establish a
    /// complete scalar index in the same catalog snapshot. None acquires no
    /// ownership or consumer output and requests the ordinary scan fallback.
    #[doc(hidden)]
    pub fn visit_indexed_projected_nodes_by_property_admitted(
        &self,
        label: LabelId,
        property: &str,
        values: &[Value],
        properties: &BTreeSet<String>,
        admit: &mut GraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<Option<GraphScanControl>> {
        // A sparse canonical overlay cannot prove complete resident postings.
        if self.canonical_base.is_some() {
            return Ok(None);
        }
        // Empty lookups still propagate a prior refusal on this task.
        admit.work().check_work_limit()?;
        for value in values {
            admit.checkpoint()?;
            if !matches!(
                value,
                Value::Null
                    | Value::Bool(_)
                    | Value::Int(_)
                    | Value::Float(_)
                    | Value::String(_)
                    | Value::Binary(_)
                    | Value::Uuid(_)
                    | Value::Date(_)
                    | Value::LocalTime(_)
                    | Value::LocalDateTime(_)
                    | Value::DateTime(_)
                    | Value::Duration(_)
            ) {
                return Ok(None);
            }
        }
        let admission = ProjectionAdmission::new(admit);
        let result =
            self.visit_indexed_projection(label, property, values, &admission, &mut |id| {
                let Some(node) = self.nodes.get(&id) else {
                    return Ok(GraphScanControl::Continue);
                };
                // Recheck visible membership before owning selected properties.
                if !node.labels.contains(&label) || self.node_tombstones.contains(&id) {
                    return Ok(GraphScanControl::Continue);
                }
                let node = admission
                    .live_selection(node, Some(properties))
                    .map_err(canonical_segment_error)?;
                Ok(admission.consume(node, consumer))
            });
        let control = admission.finish(result)?;
        admission.work.check_work_limit()?;
        Ok(Some(control))
    }

    fn resident_postings<'a>(
        &'a self,
        label: LabelId,
        property: &str,
        value: &Value,
        admission: &ProjectionAdmission<'_>,
    ) -> Result<Option<&'a BTreeSet<NodeId>>> {
        work(admission)?;
        let bytes = std::mem::size_of::<(LabelId, String, Value)>()
            .saturating_add(property.len())
            .saturating_add(hawdb_core::ids::projected_value_allocation_bytes(value));
        let _key = admission.scratch(bytes).map_err(canonical_segment_error)?;
        Ok(self
            .property_index
            .get(&(label, property.to_owned(), value.clone()))
            .map(|postings| &**postings))
    }

    fn visit_indexed_projection(
        &self,
        label: LabelId,
        property: &str,
        values: &[Value],
        admission: &ProjectionAdmission<'_>,
        consumer: &mut impl FnMut(NodeId) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        admission.checkpoint().map_err(canonical_segment_error)?;
        if let [value] = values {
            if let Some(postings) = self.resident_postings(label, property, value, admission)? {
                let mut postings = postings.iter();
                while let Some(id) = advance(&mut postings, admission)? {
                    if consumer(id)? == GraphScanControl::Stop {
                        return Ok(GraphScanControl::Stop);
                    }
                }
            }
            return Ok(GraphScanControl::Continue);
        }
        let mut allocate = |bytes| admission.scratch(bytes).map_err(canonical_segment_error);
        let mut cursors = AdmittedVec::new(&GraphReadAdmission::new(&mut allocate))?;
        for value in values {
            if let Some(postings) = self.resident_postings(label, property, value, admission)? {
                let mut remaining = postings.iter();
                if let Some(head) = advance(&mut remaining, admission)? {
                    cursors.try_push(Reverse(PostingCursor { head, remaining }))?;
                }
            }
        }
        let (cursors, _memory) = cursors.into_parts();
        // Heap construction reuses the admitted Vec; space is O(query values).
        let mut cursors = BinaryHeap::from(cursors);
        let mut previous = None;
        while let Some(Reverse(mut cursor)) = cursors.pop() {
            admission.checkpoint().map_err(canonical_segment_error)?;
            if previous != Some(cursor.head) {
                previous = Some(cursor.head);
                if consumer(cursor.head)? == GraphScanControl::Stop {
                    return Ok(GraphScanControl::Stop);
                }
            }
            if let Some(head) = advance(&mut cursor.remaining, admission)? {
                cursor.head = head;
                cursors.push(Reverse(cursor));
            }
        }
        Ok(GraphScanControl::Continue)
    }
}
