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

use crate::cache::ManifestGeneration;

/// Ownership of a query's admitted graph-read allocation. Implementations
/// release the reservation on Drop; storage retains it through consumption.
/// Standalone storage callers may use their own explicit allocation policy.
pub trait GraphReadAllocation {
    fn bytes(&self) -> usize;

    /// Reserve additional bytes atomically before allocation. Refusal leaves
    /// the existing reservation intact.
    fn grow(&mut self, bytes: usize) -> hawdb_core::Result<()>;

    /// Move an admitted read into retained operator state before insertion.
    /// Query allocators may atomically transfer to a separately bounded state
    /// account. Standalone allocators retain their existing explicit policy.
    fn retain_state(&mut self) -> hawdb_core::Result<()> {
        Ok(())
    }
}

/// A borrowed read policy. Work accounting never invokes payload admission:
/// payload callbacks can add fixed overhead, flush a batch, or request Stop.
pub struct GraphReadAllocator<'a, T = Box<dyn GraphReadAllocation>> {
    admit: &'a mut dyn FnMut(usize) -> hawdb_core::Result<T>,
    work: GraphReadWork<'a>,
    control: Option<&'a hawdb_core::RuntimeTaskContext>,
}

impl<'a, T> GraphReadAllocator<'a, T> {
    pub fn new(
        admit: &'a mut dyn FnMut(usize) -> hawdb_core::Result<T>,
        task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            admit,
            work: GraphReadWork(task_context.filter(|task| task.has_work_limit())),
            control: task_context,
        }
    }

    pub fn reserve(&mut self, bytes: usize) -> hawdb_core::Result<T> {
        (self.admit)(bytes)
    }

    pub(crate) fn work(&self) -> GraphReadWork<'a> {
        self.work
    }

    pub(crate) fn checkpoint(&self) -> hawdb_core::Result<()> {
        self.control.map_or(Ok(()), |task| {
            task.checkpoint().map_err(|reason| {
                hawdb_core::HawDBError::Execution(format!("runtime task stopped: {reason}"))
            })
        })
    }

    pub(crate) fn reborrow(&mut self) -> GraphReadAllocator<'_, T> {
        GraphReadAllocator {
            admit: self.admit,
            work: self.work,
            control: self.control,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct GraphReadWork<'a>(Option<&'a hawdb_core::RuntimeTaskContext>);

impl GraphReadWork<'_> {
    pub(crate) fn has_work_limit(self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn check_work_limit(self) -> hawdb_core::Result<()> {
        self.0.map_or(Ok(()), |task| task.check_work_limit())
    }

    pub(crate) fn charge_work(self, units: usize) -> hawdb_core::Result<()> {
        self.0.map_or(Ok(()), |task| task.charge_work(units))
    }
}

/// None requests ordinary scan Stop before selected values are owned.
pub type ControlledGraphReadAllocator<'a> =
    GraphReadAllocator<'a, Option<Box<dyn GraphReadAllocation>>>;

/// A point read can be absent from the requested visible label set, or stop
/// normally before owning its payload. A successful read transfers its permit.
pub enum AdmittedNodeRead {
    Missing,
    Stopped,
    Node(AdmittedNodeRecord),
}

/// Relationship point reads transfer the permit acquired before ownership.
pub enum AdmittedRelationshipRead {
    Missing,
    Stopped,
    Relationship(AdmittedRelationshipRecord),
}

pub struct AdmittedRelationshipRecord {
    relationship: crate::RelRecord,
    allocation: Box<dyn GraphReadAllocation>,
}

impl AdmittedRelationshipRecord {
    pub(crate) fn new(
        relationship: crate::RelRecord,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> Self {
        Self {
            relationship,
            allocation,
        }
    }

    pub fn clone_admitted(
        relationship: &crate::RelRecord,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> hawdb_core::Result<Self> {
        if allocation.bytes() < hawdb_core::ids::relationship_allocation_bytes(relationship) {
            return Err(hawdb_core::HawDBError::Execution(
                "relationship admission returned an insufficient allocation permit".into(),
            ));
        }
        Ok(Self {
            relationship: relationship.clone(),
            allocation,
        })
    }

    /// Borrow the admitted record while its source allocation remains owned.
    pub fn relationship(&self) -> &crate::RelRecord {
        &self.relationship
    }

    /// Drops the properties `keep` rejects. The record only shrinks, so its
    /// allocation still covers it.
    pub fn retain_properties(&mut self, mut keep: impl FnMut(&str) -> bool) {
        self.relationship
            .properties
            .retain(|name, _| keep(name.as_str()));
    }

    pub fn into_parts(self) -> (crate::RelRecord, Box<dyn GraphReadAllocation>) {
        (self.relationship, self.allocation)
    }
}

/// Selected record and the reservation acquired before its values were owned.
/// Buffered consumers retain both until the record is consumed or dropped.
pub struct AdmittedProjectedNode {
    node: crate::ProjectedNodeRecord,
    allocation: Box<dyn GraphReadAllocation>,
}

impl AdmittedProjectedNode {
    pub(crate) fn new(
        node: crate::ProjectedNodeRecord,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> Self {
        Self { node, allocation }
    }

    /// Custom readers admit the selected allocation before copying any values.
    pub fn clone_admitted(
        node: &crate::NodeRecord,
        properties: &std::collections::BTreeSet<String>,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> hawdb_core::Result<Self> {
        if allocation.bytes() < hawdb_core::ids::projected_node_allocation_bytes(node, properties) {
            return Err(hawdb_core::HawDBError::Execution(
                "projected node admission returned an insufficient allocation permit".into(),
            ));
        }
        Ok(Self {
            node: hawdb_core::ids::project_node_record_ref(node, properties),
            allocation,
        })
    }

    pub fn into_parts(self) -> (crate::ProjectedNodeRecord, Box<dyn GraphReadAllocation>) {
        (self.node, self.allocation)
    }
}

/// Full node and the reservation acquired before any owned values were copied.
pub struct AdmittedNodeRecord {
    node: crate::NodeRecord,
    allocation: Box<dyn GraphReadAllocation>,
}

impl AdmittedNodeRecord {
    pub(crate) fn new(node: crate::NodeRecord, allocation: Box<dyn GraphReadAllocation>) -> Self {
        Self { node, allocation }
    }

    /// Custom resident readers can construct the admitted record directly
    /// from a borrowed source. Reject a short permit before cloning any Value.
    pub fn clone_admitted(
        node: &crate::NodeRecord,
        allocation: Box<dyn GraphReadAllocation>,
    ) -> hawdb_core::Result<Self> {
        if allocation.bytes() < hawdb_core::ids::node_allocation_bytes(node) {
            return Err(hawdb_core::HawDBError::Execution(
                "full node admission returned an insufficient allocation permit".into(),
            ));
        }
        Ok(Self {
            node: node.clone(),
            allocation,
        })
    }

    pub(crate) fn from_full_projection(input: AdmittedProjectedNode) -> Self {
        let (node, allocation) = input.into_parts();
        Self {
            node: crate::NodeRecord {
                id: node.id,
                labels: node.labels,
                properties: node.properties,
            },
            allocation,
        }
    }

    pub fn into_parts(self) -> (crate::NodeRecord, Box<dyn GraphReadAllocation>) {
        (self.node, self.allocation)
    }
}

/// Shared allocation policy for concurrently live graph-read collections.
pub struct GraphReadAdmission<'a> {
    admit: std::cell::RefCell<
        &'a mut dyn FnMut(usize) -> hawdb_core::Result<Box<dyn GraphReadAllocation>>,
    >,
}

impl<'a> GraphReadAdmission<'a> {
    pub fn new(
        admit: &'a mut dyn FnMut(usize) -> hawdb_core::Result<Box<dyn GraphReadAllocation>>,
    ) -> Self {
        Self {
            admit: std::cell::RefCell::new(admit),
        }
    }

    pub fn reserve(&self, bytes: usize) -> hawdb_core::Result<Box<dyn GraphReadAllocation>> {
        let allocation = (self.admit.borrow_mut())(bytes)?;
        if allocation.bytes() < bytes {
            return Err(hawdb_core::HawDBError::Execution(
                "graph read admission returned an insufficient allocation permit".into(),
            ));
        }
        Ok(allocation)
    }
}

/// A graph-read key set whose permit grows before a new key is inserted.
/// Each set owns a distinct permit, including simultaneously live AND/OR
/// candidates. Duplicate keys neither allocate tree nodes nor grow the permit.
pub struct AdmittedKeySet<T: Ord> {
    keys: std::collections::BTreeSet<T>,
    allocation: Box<dyn GraphReadAllocation>,
}

impl<T: Ord> AdmittedKeySet<T> {
    pub fn new(admission: &GraphReadAdmission<'_>) -> hawdb_core::Result<Self> {
        Ok(Self::with_allocation(admission.reserve(0)?))
    }

    pub(crate) fn with_allocation(allocation: Box<dyn GraphReadAllocation>) -> Self {
        Self {
            keys: std::collections::BTreeSet::new(),
            allocation,
        }
    }

    pub fn try_insert(&mut self, key: T) -> hawdb_core::Result<bool> {
        // Grow also carries the caller's task checkpoint. Repeated lookup
        // values must stay cancellable even when no new tree key is needed.
        self.allocation.grow(0)?;
        if self.keys.contains(&key) {
            return Ok(false);
        }
        self.allocation.retain_state()?;
        // Pinned std B-trees keep up to eleven keys plus node links. Cover
        // the first sparse node and conservative per-key tree overhead. These
        // read sets contain NodeId or borrowed Value keys, never owned Values.
        let key_bytes = std::mem::size_of::<T>();
        let bytes = self
            .keys
            .len()
            .saturating_add(1)
            .saturating_mul(64usize.saturating_add(key_bytes.saturating_mul(2)))
            .saturating_add(128usize.saturating_add(key_bytes.saturating_mul(11)));
        let additional = bytes.saturating_sub(self.allocation.bytes());
        self.allocation.grow(additional)?;
        if self.allocation.bytes() < bytes {
            return Err(hawdb_core::HawDBError::Execution(
                "graph read admission did not grow its allocation permit".into(),
            ));
        }
        Ok(self.keys.insert(key))
    }

    pub fn try_extend(&mut self, keys: impl IntoIterator<Item = T>) -> hawdb_core::Result<()> {
        for key in keys {
            self.try_insert(key)?;
        }
        Ok(())
    }
}

impl<T: Ord> std::ops::Deref for AdmittedKeySet<T> {
    type Target = std::collections::BTreeSet<T>;
    fn deref(&self) -> &Self::Target {
        &self.keys
    }
}

/// Capacity is admitted before a graph-read vector grows. Its permit can move
/// with the vector into a consumer iterator.
pub struct AdmittedVec<T> {
    values: Vec<T>,
    allocation: Box<dyn GraphReadAllocation>,
}

impl<T> AdmittedVec<T> {
    pub fn new(admission: &GraphReadAdmission<'_>) -> hawdb_core::Result<Self> {
        Ok(Self {
            values: Vec::new(),
            allocation: admission.reserve(0)?,
        })
    }

    pub fn try_push(&mut self, value: T) -> hawdb_core::Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().saturating_mul(2).max(4);
            let bytes = capacity.saturating_mul(std::mem::size_of::<T>());
            self.allocation
                .grow(bytes.saturating_sub(self.allocation.bytes()))?;
            if self.allocation.bytes() < bytes {
                return Err(hawdb_core::HawDBError::Execution(
                    "graph read admission did not grow its allocation permit".into(),
                ));
            }
            self.values.reserve_exact(capacity - self.values.len());
        }
        self.values.push(value);
        Ok(())
    }

    pub fn into_parts(self) -> (Vec<T>, Box<dyn GraphReadAllocation>) {
        (self.values, self.allocation)
    }

    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    /// Mutate admitted elements without changing vector capacity or ownership.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }

    pub fn clear(&mut self) {
        // The vector still owns its capacity; retain that allocation permit.
        self.values.clear();
    }

    /// Remove a consumed or displaced element, retaining admitted capacity.
    pub fn swap_remove(&mut self, index: usize) -> T {
        self.values.swap_remove(index)
    }
}

/// The immutable identity of one published database read.
///
/// The logical commit epoch controls visibility. The physical generation only
/// identifies the checkpoint base beneath that logical snapshot; later commits
/// may be represented by an immutable delta without changing the base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedReadView {
    visible_commit_epoch: u64,
    checkpoint_commit_epoch: Option<u64>,
    physical_generation: Option<ManifestGeneration>,
}

impl PublishedReadView {
    #[doc(hidden)]
    pub fn new(
        visible_commit_epoch: u64,
        checkpoint_commit_epoch: Option<u64>,
        physical_generation: Option<ManifestGeneration>,
    ) -> Self {
        debug_assert_eq!(
            checkpoint_commit_epoch.is_some(),
            physical_generation.is_some(),
            "checkpoint epoch and physical generation must be published together"
        );
        debug_assert!(
            checkpoint_commit_epoch.is_none_or(|epoch| epoch <= visible_commit_epoch),
            "checkpoint commit epoch must not exceed the visible commit epoch"
        );
        Self {
            visible_commit_epoch,
            checkpoint_commit_epoch,
            physical_generation,
        }
    }

    pub const fn visible_commit_epoch(self) -> u64 {
        self.visible_commit_epoch
    }

    pub const fn checkpoint_commit_epoch(self) -> Option<u64> {
        self.checkpoint_commit_epoch
    }

    pub const fn physical_generation(self) -> Option<ManifestGeneration> {
        self.physical_generation
    }

    pub const fn physical_base_is_current(self) -> bool {
        matches!(
            self.checkpoint_commit_epoch,
            Some(checkpoint_commit_epoch) if checkpoint_commit_epoch == self.visible_commit_epoch
        )
    }

    pub const fn has_delta_after_physical_generation(self) -> bool {
        matches!(
            self.checkpoint_commit_epoch,
            Some(checkpoint_commit_epoch) if checkpoint_commit_epoch < self.visible_commit_epoch
        )
    }
}
