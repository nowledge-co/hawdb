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

//! Shared selected-node admission. Permits follow owned records through filtering
//! and consumption; caller refusal remains distinct from storage corruption.

use super::*;
use crate::read_view::{
    AdmittedKeySet, AdmittedNodeRecord, AdmittedProjectedNode, ControlledGraphReadAllocator,
    GraphReadAllocation, GraphReadAllocator, GraphReadWork,
};
use std::cell::{Cell, RefCell};
use std::ops::Deref;

pub(super) struct AdmittedProjection {
    pub(super) node: ProjectedNodeRecord,
    _allocation: Box<dyn GraphReadAllocation>,
}

impl Deref for AdmittedProjection {
    type Target = ProjectedNodeRecord;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

pub(super) struct ProjectionAdmission<'a> {
    admission: ProjectionAllocationPolicy<'a>,
    error: RefCell<Option<HawDBError>>,
    stopped: Cell<bool>,
    work: GraphReadWork<'a>,
}

enum ProjectionAllocationPolicy<'a> {
    Required(RefCell<GraphReadAllocator<'a>>),
    Controlled(RefCell<ControlledGraphReadAllocator<'a>>),
}

pub(super) struct UnaccountedReadAllocation(pub(super) usize);
impl GraphReadAllocation for UnaccountedReadAllocation {
    fn bytes(&self) -> usize {
        self.0
    }
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.0 = self.0.saturating_add(bytes);
        Ok(())
    }
}

impl<'a> ProjectionAdmission<'a> {
    pub(super) fn new(admit: &'a mut GraphReadAllocator<'_>) -> Self {
        let work = admit.work();
        Self {
            admission: ProjectionAllocationPolicy::Required(RefCell::new(admit.reborrow())),
            error: RefCell::new(None),
            stopped: Cell::new(false),
            work,
        }
    }

    fn new_controlled(admit: &'a mut ControlledGraphReadAllocator<'_>) -> Self {
        let work = admit.work();
        Self {
            admission: ProjectionAllocationPolicy::Controlled(RefCell::new(admit.reborrow())),
            error: RefCell::new(None),
            stopped: Cell::new(false),
            work,
        }
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<Box<dyn GraphReadAllocation>, CanonicalSegmentError> {
        let result = match &self.admission {
            ProjectionAllocationPolicy::Required(admission) => {
                admission.borrow_mut().reserve(bytes).map(Some)
            }
            ProjectionAllocationPolicy::Controlled(admit) => admit.borrow_mut().reserve(bytes),
        };
        match result {
            Ok(Some(allocation)) if allocation.bytes() >= bytes => Ok(allocation),
            Ok(Some(_)) => Err(self.refusal(HawDBError::Execution(
                "graph read admission returned an insufficient allocation permit".into(),
            ))),
            Ok(None) => {
                self.stopped.set(true);
                Err(CanonicalSegmentError::Source(
                    "graph read caller stopped".into(),
                ))
            }
            Err(error) => Err(self.refusal(error)),
        }
    }

    pub(super) fn has_work_limit(&self) -> bool {
        self.work.has_work_limit()
    }

    pub(super) fn scratch(
        &self,
        bytes: usize,
    ) -> std::result::Result<Box<dyn GraphReadAllocation>, CanonicalSegmentError> {
        self.checkpoint()?;
        self.reserve(bytes)
    }

    pub(super) fn checkpoint(&self) -> std::result::Result<(), CanonicalSegmentError> {
        let result = match &self.admission {
            ProjectionAllocationPolicy::Required(admission) => admission.borrow().checkpoint(),
            ProjectionAllocationPolicy::Controlled(admission) => admission.borrow().checkpoint(),
        };
        result.map_err(|error| self.refusal(error))
    }

    fn charge_work(&self, units: usize) -> std::result::Result<(), CanonicalSegmentError> {
        self.work
            .charge_work(units)
            .map_err(|error| self.refusal(error))
    }

    fn refusal(&self, error: HawDBError) -> CanonicalSegmentError {
        *self.error.borrow_mut() = Some(error);
        CanonicalSegmentError::Source("graph read admission refused".into())
    }

    pub(super) fn key_set<T: Ord>(&self) -> Result<AdmittedKeySet<T>> {
        self.reserve(0)
            .map(AdmittedKeySet::with_allocation)
            .map_err(canonical_segment_error)
    }

    pub(super) fn insert_key<T: Ord>(
        &self,
        keys: &mut AdmittedKeySet<T>,
        key: T,
    ) -> std::result::Result<bool, CanonicalSegmentError> {
        keys.try_insert(key).map_err(|error| self.refusal(error))
    }

    pub(super) fn live_selection(
        &self,
        node: &NodeRecord,
        properties: Option<&BTreeSet<String>>,
    ) -> std::result::Result<AdmittedProjection, CanonicalSegmentError> {
        let bytes = properties.map_or_else(
            || hawdb_core::ids::node_allocation_bytes(node),
            |properties| projected_node_allocation_bytes(node, properties),
        );
        let allocation = self.reserve(bytes)?;
        Ok(AdmittedProjection {
            node: match properties {
                Some(properties) => project_node_record_ref(node, properties),
                None => ProjectedNodeRecord {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                },
            },
            _allocation: allocation,
        })
    }

    pub(super) fn canonical(
        &self,
        reader: &CanonicalSegmentReader,
        id: NodeId,
        properties: &BTreeSet<String>,
    ) -> std::result::Result<Option<AdmittedProjection>, CanonicalSegmentError> {
        reader.with_node_input(id, |input| {
            let (node, allocation) =
                input.decode_selected(properties, |bytes| self.reserve(bytes))?;
            Ok(AdmittedProjection {
                node,
                _allocation: allocation,
            })
        })
    }

    pub(super) fn canonical_selection(
        &self,
        reader: &CanonicalSegmentReader,
        id: NodeId,
        properties: Option<&BTreeSet<String>>,
    ) -> std::result::Result<Option<AdmittedProjection>, CanonicalSegmentError> {
        if let Some(properties) = properties {
            return self.canonical(reader, id, properties);
        }
        let Some(bytes) = reader.node_allocation_bytes(id, None)? else {
            return Ok(None);
        };
        let allocation = self.reserve(bytes)?;
        Ok(reader.get_node(id)?.map(|node| AdmittedProjection {
            node: ProjectedNodeRecord {
                id: node.id,
                labels: node.labels,
                properties: node.properties,
            },
            _allocation: allocation,
        }))
    }

    pub(super) fn input(
        &self,
        input: crate::canonical::CanonicalNodeInput<'_>,
        properties: &BTreeSet<String>,
    ) -> std::result::Result<AdmittedProjection, CanonicalSegmentError> {
        self.input_selection(input, Some(properties))
    }

    pub(super) fn input_selection(
        &self,
        input: crate::canonical::CanonicalNodeInput<'_>,
        properties: Option<&BTreeSet<String>>,
    ) -> std::result::Result<AdmittedProjection, CanonicalSegmentError> {
        let (node, allocation) = input.decode_selection(properties, |bytes| self.reserve(bytes))?;
        Ok(AdmittedProjection {
            node,
            _allocation: allocation,
        })
    }

    pub(super) fn visit_live_selection(
        &self,
        node: &NodeRecord,
        properties: Option<&BTreeSet<String>>,
        consumer: &mut impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> GraphScanControl {
        match self.live_selection(node, properties) {
            Ok(node) => consumer(node),
            Err(_) => GraphScanControl::Stop,
        }
    }

    pub(super) fn consume(
        &self,
        admitted: AdmittedProjection,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> GraphScanControl {
        // The remaining allocation field is dropped after the consumer returns,
        // including its error or Stop exit. A filtered input drops both fields.
        match consumer(admitted.node) {
            Ok(control) => control,
            Err(error) => {
                *self.error.borrow_mut() = Some(error);
                GraphScanControl::Stop
            }
        }
    }

    fn consume_allocated(
        &self,
        admitted: AdmittedProjection,
        consumer: &mut dyn FnMut(AdmittedProjectedNode) -> Result<GraphScanControl>,
    ) -> GraphScanControl {
        match consumer(AdmittedProjectedNode::new(
            admitted.node,
            admitted._allocation,
        )) {
            Ok(control) => control,
            Err(error) => {
                *self.error.borrow_mut() = Some(error);
                GraphScanControl::Stop
            }
        }
    }

    pub(super) fn finish(&self, result: Result<GraphScanControl>) -> Result<GraphScanControl> {
        match self.error.borrow_mut().take() {
            Some(error) => Err(error),
            None if self.stopped.get() => Ok(GraphScanControl::Stop),
            None => result,
        }
    }
}

impl GraphStore {
    /// Full-node ownership uses the same visibility and pre-decode admission
    /// boundary as selected scans. None is explicit all-properties selection.
    pub fn visit_nodes_with_allocation(
        &self,
        label_id: Option<LabelId>,
        admit: &mut ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(AdmittedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new_controlled(admit);
        let result = self.visit_selected_nodes_with_admission(label_id, None, &admission, |node| {
            admission.consume_allocated(node, &mut |input| {
                consumer(AdmittedNodeRecord::from_full_projection(input))
            })
        });
        admission.finish(result)
    }

    /// Indexed full records share selected-read admission without copying an
    /// intermediate full node or discarding the source's allocation lease.
    pub fn visit_nodes_by_access_with_allocation(
        &self,
        label_id: LabelId,
        access: &hawdb_core::access::NodeProjectionAccess,
        admit: &mut ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(AdmittedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new_controlled(admit);
        let result = self.visit_projected_nodes_by_access_with_admission(
            label_id,
            access,
            None,
            &admission,
            |node| {
                admission.consume_allocated(node, &mut |input| {
                    consumer(AdmittedNodeRecord::from_full_projection(input))
                })
            },
        );
        admission.finish(result)
    }

    /// Transfer each selected record's admitted allocation to its consumer.
    /// Returning None from admission stops before value cloning/decoding.
    pub fn visit_projected_nodes_with_allocation(
        &self,
        label_id: Option<LabelId>,
        properties: &BTreeSet<String>,
        admit: &mut ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(AdmittedProjectedNode) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new_controlled(admit);
        let result =
            self.visit_projected_nodes_with_admission(label_id, properties, &admission, |node| {
                admission.consume_allocated(node, consumer)
            });
        admission.finish(result)
    }

    pub fn visit_projected_nodes_admitted(
        &self,
        label_id: Option<LabelId>,
        properties: &BTreeSet<String>,
        admit: &mut GraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new(admit);
        let result =
            self.visit_projected_nodes_with_admission(label_id, properties, &admission, |node| {
                admission.consume(node, consumer)
            });
        admission.finish(result)
    }

    pub fn visit_projected_nodes_by_access_admitted(
        &self,
        label_id: LabelId,
        access: &hawdb_core::access::NodeProjectionAccess,
        properties: &BTreeSet<String>,
        admit: &mut GraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new(admit);
        let result = self.visit_projected_nodes_by_access_with_admission(
            label_id,
            access,
            Some(properties),
            &admission,
            |node| admission.consume(node, consumer),
        );
        admission.finish(result)
    }

    pub fn visit_projected_nodes_by_property_admitted(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        properties: &BTreeSet<String>,
        admit: &mut GraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new(admit);
        let result = self.visit_projected_nodes_by_property_with_admission(
            label_id,
            property,
            values,
            Some(properties),
            &admission,
            |node| admission.consume(node, consumer),
        );
        admission.finish(result)
    }

    pub(super) fn visit_projected_nodes_with_admission(
        &self,
        label_id: Option<LabelId>,
        properties: &BTreeSet<String>,
        admission: &ProjectionAdmission<'_>,
        consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        self.visit_selected_nodes_with_admission(label_id, Some(properties), admission, consumer)
    }

    pub(super) fn visit_selected_nodes_with_admission(
        &self,
        label_id: Option<LabelId>,
        properties: Option<&BTreeSet<String>>,
        admission: &ProjectionAdmission<'_>,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            for node in self.nodes.values() {
                admission.charge_work(1).map_err(canonical_segment_error)?;
                if self.node_matches_label(node, label_id)
                    && admission.visit_live_selection(node, properties, &mut consumer)
                        == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };
        let mut delta = self.nodes.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let (_, control) = reader
            .scan_node_inputs_control(|base| {
                admission.charge_work(1)?;
                while delta.peek().is_some_and(|(id, _)| **id <= base.id()) {
                    let (id, node) = delta.next().expect("peeked delta node exists");
                    admission.charge_work(1)?;
                    if !self.node_tombstones.contains(id)
                        && self.node_matches_label(node, label_id)
                        && admission.visit_live_selection(node, properties, &mut consumer)
                            == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                if self.node_tombstones.contains(&base.id()) || self.nodes.contains_key(&base.id())
                {
                    return Ok(CanonicalScanControl::Continue);
                }
                if let Some(label) = label_id
                    && !base.has_label(label)?
                {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = admission.input_selection(base, properties)?;
                if label_id.is_none_or(|label| node.labels.contains(&label))
                    && consumer(node) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(canonical_segment_error)?;
        if control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for (id, node) in delta {
            admission.charge_work(1).map_err(canonical_segment_error)?;
            if !self.node_tombstones.contains(id)
                && self.node_matches_label(node, label_id)
                && admission.visit_live_selection(node, properties, &mut consumer)
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub(super) fn visit_projected_property_fallback_with_admission(
        &self,
        label: LabelId,
        property: &str,
        values: &[Value],
        properties: Option<&BTreeSet<String>>,
        admission: &ProjectionAdmission<'_>,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        if let Some(reader) = &self.canonical_base {
            let predicate_properties = BTreeSet::from([property.to_owned()]);
            let mut seen = admission.key_set()?;
            for value in values {
                let mut graph_control = GraphScanControl::Continue;
                let (_, control) = reader
                    .scan_node_inputs_by_property_control(label, property, value, |input| {
                        admission.charge_work(1)?;
                        if self.node_tombstones.contains(&input.id())
                            || self.nodes.contains_key(&input.id())
                            || seen.contains(&input.id())
                            || !input.has_label(label)?
                        {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        // Check the access predicate before hydrating unrelated
                        // output columns, retaining its own admitted allocation.
                        let predicate = admission.input(input, &predicate_properties)?;
                        if predicate.properties.get(property) != Some(value) {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        admission.insert_key(&mut seen, input.id())?;
                        let node = admission.input_selection(input, properties)?;
                        if consumer(node) == GraphScanControl::Stop {
                            graph_control = GraphScanControl::Stop;
                            return Ok(CanonicalScanControl::Stop);
                        }
                        Ok(CanonicalScanControl::Continue)
                    })
                    .map_err(canonical_segment_error)?;
                if control == CanonicalScanControl::Stop {
                    return Ok(graph_control);
                }
            }
        }
        for node in self.nodes.values() {
            admission.charge_work(1).map_err(canonical_segment_error)?;
            if node.labels.contains(&label)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                && admission.visit_live_selection(node, properties, &mut consumer)
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub(super) fn visit_projected_nodes_filtered_with_admission(
        &self,
        label_id: LabelId,
        properties: Option<&BTreeSet<String>>,
        admission: &ProjectionAdmission<'_>,
        mut matches: impl FnMut(&BTreeMap<String, Value>) -> bool,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        // Resident predicates borrow storage values before selected hydration.
        if self.canonical_base.is_none() {
            for node in self.nodes.values() {
                admission.charge_work(1).map_err(canonical_segment_error)?;
                if node.labels.contains(&label_id)
                    && matches(&node.properties)
                    && admission.visit_live_selection(node, properties, &mut consumer)
                        == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        }
        self.visit_selected_nodes_with_admission(Some(label_id), properties, admission, |node| {
            if matches(&node.properties) {
                consumer(node)
            } else {
                GraphScanControl::Continue
            }
        })
    }
}

#[cfg(test)]
mod work_tests {
    use super::*;

    #[test]
    fn ordered_scratch_checkpoint_preserves_cancellation_without_payload_admission() {
        for bounded in [false, true] {
            let task = if bounded {
                hawdb_core::RuntimeTaskContext::default().with_work_limit(100)
            } else {
                hawdb_core::RuntimeTaskContext::default()
            };
            task.cancellation().cancel();
            let calls = Cell::new(0);
            let mut allocate = |bytes| -> Result<Box<dyn GraphReadAllocation>> {
                calls.set(calls.get() + 1);
                Ok(Box::new(UnaccountedReadAllocation(bytes)))
            };
            let mut allocator = GraphReadAllocator::new(&mut allocate, Some(&task));
            let admission = ProjectionAdmission::new(&mut allocator);
            assert!(admission.scratch(64).is_err());
            assert!(
                matches!(admission.finish(Ok(GraphScanControl::Continue)), Err(HawDBError::Execution(message)) if message.contains("runtime task stopped"))
            );
            assert_eq!(calls.get(), 0);
            assert_eq!(task.work_units(), 0);
        }
    }

    #[test]
    fn projection_work_admission_never_reserves_payload_and_preserves_refusal() {
        for bounded in [false, true] {
            let calls = Cell::new(0);
            let task = if bounded {
                hawdb_core::RuntimeTaskContext::default().with_work_limit(100)
            } else {
                hawdb_core::RuntimeTaskContext::default()
            };
            let mut allocate = |bytes| -> Result<Box<dyn GraphReadAllocation>> {
                assert_eq!(bytes, 0);
                calls.set(calls.get() + 1);
                panic!("work accounting must not request payload")
            };
            let mut policy = GraphReadAllocator::new(&mut allocate, Some(&task));
            let admission = ProjectionAdmission::new(&mut policy);
            assert_eq!(admission.has_work_limit(), bounded);
            for _ in 0..100 {
                admission.charge_work(1).unwrap();
            }
            assert_eq!(calls.get(), 0);
            assert_eq!(task.work_units(), if bounded { 100 } else { 0 });
            if bounded {
                assert!(admission.charge_work(1).is_err());
                assert!(matches!(
                    admission.finish(Ok(GraphScanControl::Continue)),
                    Err(HawDBError::ResourceBudgetExceeded(ref error))
                        if error.resource == hawdb_core::ResourceBudgetKind::WorkUnits
                ));
            }
        }
    }
}
