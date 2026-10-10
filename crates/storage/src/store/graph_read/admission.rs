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
    GraphReadAdmission, GraphReadAllocation,
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
}

enum ProjectionAllocationPolicy<'a> {
    Required(GraphReadAdmission<'a>),
    Controlled(RefCell<&'a mut ControlledGraphReadAllocator<'a>>),
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
    pub(super) fn new(
        admit: &'a mut dyn FnMut(usize) -> Result<Box<dyn GraphReadAllocation>>,
    ) -> Self {
        Self {
            admission: ProjectionAllocationPolicy::Required(GraphReadAdmission::new(admit)),
            error: RefCell::new(None),
            stopped: Cell::new(false),
        }
    }

    fn new_controlled(admit: &'a mut ControlledGraphReadAllocator<'a>) -> Self {
        Self {
            admission: ProjectionAllocationPolicy::Controlled(RefCell::new(admit)),
            error: RefCell::new(None),
            stopped: Cell::new(false),
        }
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<Box<dyn GraphReadAllocation>, CanonicalSegmentError> {
        let result = match &self.admission {
            ProjectionAllocationPolicy::Required(admission) => admission.reserve(bytes).map(Some),
            ProjectionAllocationPolicy::Controlled(admit) => (admit.borrow_mut())(bytes),
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

    pub(super) fn live(
        &self,
        node: &NodeRecord,
        properties: &BTreeSet<String>,
    ) -> std::result::Result<AdmittedProjection, CanonicalSegmentError> {
        self.live_selection(node, Some(properties))
    }

    fn live_selection(
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
        let Some(bytes) = reader.projected_node_allocation_bytes(id, properties)? else {
            return Ok(None);
        };
        let allocation = self.reserve(bytes)?;
        Ok(reader
            .get_projected_node(id, properties)?
            .map(|node| AdmittedProjection {
                node,
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

    fn input_selection(
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

    pub(super) fn visit_live(
        &self,
        node: &NodeRecord,
        properties: &BTreeSet<String>,
        consumer: &mut impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> GraphScanControl {
        self.visit_live_selection(node, Some(properties), consumer)
    }

    fn visit_live_selection(
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
        admit: &mut dyn FnMut(usize) -> Result<Box<dyn GraphReadAllocation>>,
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
        access: &hawdb_plan_cypher::NodeProjectionAccess,
        properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(usize) -> Result<Box<dyn GraphReadAllocation>>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new(admit);
        let result = self.visit_projected_nodes_by_access_with_admission(
            label_id,
            access,
            properties,
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
        admit: &mut dyn FnMut(usize) -> Result<Box<dyn GraphReadAllocation>>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let admission = ProjectionAdmission::new(admit);
        let result = self.visit_projected_nodes_by_property_with_admission(
            label_id,
            property,
            values,
            properties,
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

    fn visit_selected_nodes_with_admission(
        &self,
        label_id: Option<LabelId>,
        properties: Option<&BTreeSet<String>>,
        admission: &ProjectionAdmission<'_>,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            for node in self.nodes.values() {
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
                while delta.peek().is_some_and(|(id, _)| **id <= base.id()) {
                    let (id, node) = delta.next().expect("peeked delta node exists");
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
        properties: &BTreeSet<String>,
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
                        let node = admission.input(input, properties)?;
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
            if node.labels.contains(&label)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                && admission.visit_live(node, properties, &mut consumer) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub(super) fn visit_projected_nodes_filtered_with_admission(
        &self,
        label_id: LabelId,
        properties: &BTreeSet<String>,
        admission: &ProjectionAdmission<'_>,
        mut matches: impl FnMut(&BTreeMap<String, Value>) -> bool,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        // Resident predicates borrow storage values before selected hydration.
        if self.canonical_base.is_none() {
            for node in self.nodes.values() {
                if node.labels.contains(&label_id)
                    && matches(&node.properties)
                    && admission.visit_live(node, properties, &mut consumer)
                        == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        }
        self.visit_projected_nodes_with_admission(Some(label_id), properties, admission, |node| {
            if matches(&node.properties) {
                consumer(node)
            } else {
                GraphScanControl::Continue
            }
        })
    }
}
