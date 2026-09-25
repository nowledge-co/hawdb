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

//! Internal logical-lock metadata for the embedded transaction coordinator.
//!
//! The caller owns synchronization, conflict admission, waiting, and wakeups.
//! This module does not provide a standalone transaction manager.

use crate::relational::RelationalKey;
use hawdb_core::{HawDBError, Result};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem::size_of;
use std::ops::Bound;

#[cfg(test)]
mod differential;

pub const DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE: usize = 64;
pub(crate) const DEFAULT_MAX_LOCK_TABLE_ENTRIES: usize = 65_536;
pub(crate) const DEFAULT_MAX_LOCK_TABLE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    Shared,
    Exclusive,
    /// Compatible only with other optimistic commit holders. All writes must
    /// still be validated and published by one serialized commit sequencer.
    /// This is not a shared read lock and grants no permission to mutate data
    /// concurrently. The embedded coordinator uses it on the whole database.
    OptimisticCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockNamespace {
    RelationalIndex { table: String, columns: Vec<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphAllocationKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphAdjacencyDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockTarget {
    Database,
    RelationalTable {
        table: String,
    },
    RelationalRange {
        namespace: LockNamespace,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    },
    GraphAllocation {
        kind: GraphAllocationKind,
    },
    GraphLabel {
        label: String,
    },
    GraphRelationshipType {
        rel_type: String,
    },
    GraphNode {
        node_id: u64,
    },
    GraphRelationship {
        relationship_id: u64,
    },
    GraphNodeDeleteGuard {
        node_id: u64,
    },
    GraphAdjacency {
        node_id: u64,
        rel_type: Option<u32>,
        direction: GraphAdjacencyDirection,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRequest {
    pub mode: LockMode,
    pub target: LockTarget,
}

impl LockRequest {
    pub fn database(mode: LockMode) -> Self {
        Self {
            mode,
            target: LockTarget::Database,
        }
    }

    pub fn relational_point(
        mode: LockMode,
        table: impl Into<String>,
        columns: Vec<String>,
        key: RelationalKey,
    ) -> Self {
        Self::relational_range(
            mode,
            table,
            columns,
            Bound::Included(key.clone()),
            Bound::Included(key),
        )
    }

    pub fn relational_table(mode: LockMode, table: impl Into<String>) -> Self {
        Self {
            mode,
            target: LockTarget::RelationalTable {
                table: table.into(),
            },
        }
    }

    pub fn relational_range(
        mode: LockMode,
        table: impl Into<String>,
        columns: Vec<String>,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    ) -> Self {
        Self {
            mode,
            target: LockTarget::RelationalRange {
                namespace: LockNamespace::RelationalIndex {
                    table: table.into(),
                    columns,
                },
                lower,
                upper,
            },
        }
    }

    pub fn graph_allocation(kind: GraphAllocationKind) -> Self {
        Self {
            mode: LockMode::Exclusive,
            target: LockTarget::GraphAllocation { kind },
        }
    }

    pub fn graph_node(mode: LockMode, node_id: u64) -> Self {
        Self {
            mode,
            target: LockTarget::GraphNode { node_id },
        }
    }

    pub fn graph_label(mode: LockMode, label: impl Into<String>) -> Self {
        Self {
            mode,
            target: LockTarget::GraphLabel {
                label: label.into(),
            },
        }
    }

    pub fn graph_relationship_type(mode: LockMode, rel_type: impl Into<String>) -> Self {
        Self {
            mode,
            target: LockTarget::GraphRelationshipType {
                rel_type: rel_type.into(),
            },
        }
    }

    pub fn graph_relationship(mode: LockMode, relationship_id: u64) -> Self {
        Self {
            mode,
            target: LockTarget::GraphRelationship { relationship_id },
        }
    }

    pub fn graph_node_delete_guard(mode: LockMode, node_id: u64) -> Self {
        Self {
            mode,
            target: LockTarget::GraphNodeDeleteGuard { node_id },
        }
    }

    pub fn graph_adjacency(
        mode: LockMode,
        node_id: u64,
        rel_type: Option<u32>,
        direction: GraphAdjacencyDirection,
    ) -> Self {
        Self {
            mode,
            target: LockTarget::GraphAdjacency {
                node_id,
                rel_type,
                direction,
            },
        }
    }

    pub fn acquisition_cmp(&self, other: &Self) -> Ordering {
        lock_target_cmp(&self.target, &other.target).then(other.mode.cmp(&self.mode))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldLock {
    transaction_id: u64,
    request: LockRequest,
}

#[derive(Debug, Clone, Copy)]
struct LockTableLimits {
    max_entries: usize,
    max_bytes: usize,
    escalation_entries_per_table: usize,
}

impl Default for LockTableLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_LOCK_TABLE_ENTRIES,
            max_bytes: DEFAULT_MAX_LOCK_TABLE_BYTES,
            escalation_entries_per_table: DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
        }
    }
}

#[derive(Debug, Default)]
pub struct LockTable {
    locks: Vec<HeldLock>,
    estimated_bytes: usize,
    limits: LockTableLimits,
}

impl LockTable {
    pub fn covers_all(&self, transaction_id: u64, requests: &[LockRequest]) -> bool {
        requests.iter().all(|request| {
            self.locks.iter().any(|held| {
                held.transaction_id == transaction_id
                    && mode_covers(held.request.mode, request.mode)
                    && target_covers(&held.request.target, &request.target)
            })
        })
    }

    pub fn blockers(&self, transaction_id: u64, request: &LockRequest) -> BTreeSet<u64> {
        self.locks
            .iter()
            .filter(|held| {
                held.transaction_id != transaction_id && requests_conflict(&held.request, request)
            })
            .map(|held| held.transaction_id)
            .collect()
    }

    pub fn normalized_request(&self, transaction_id: u64, request: LockRequest) -> LockRequest {
        match &request.target {
            LockTarget::RelationalRange { namespace, .. } => {
                let table = relational_namespace_table(namespace);
                let narrow_locks = self
                    .locks
                    .iter()
                    .filter(|held| {
                        held.transaction_id == transaction_id
                            && matches!(
                                &held.request.target,
                                LockTarget::RelationalRange { namespace, .. }
                                    if relational_namespace_table(namespace) == table
                            )
                    })
                    .collect::<Vec<_>>();
                if narrow_locks.len().saturating_add(1) <= self.limits.escalation_entries_per_table
                {
                    return request;
                }
                let mode = covering_mode(request.mode, &narrow_locks);
                LockRequest::relational_table(mode, table.to_string())
            }
            LockTarget::GraphAdjacency {
                node_id,
                rel_type: Some(_),
                direction,
            } => {
                let narrow_locks = self
                    .locks
                    .iter()
                    .filter(|held| {
                        held.transaction_id == transaction_id
                            && matches!(
                                held.request.target,
                                LockTarget::GraphAdjacency {
                                    node_id: held_node_id,
                                    rel_type: Some(_),
                                    direction: held_direction,
                                } if held_node_id == *node_id && held_direction == *direction
                            )
                    })
                    .collect::<Vec<_>>();
                if narrow_locks.len().saturating_add(1) <= self.limits.escalation_entries_per_table
                {
                    return request;
                }
                LockRequest::graph_adjacency(
                    covering_mode(request.mode, &narrow_locks),
                    *node_id,
                    None,
                    *direction,
                )
            }
            _ => request,
        }
    }

    pub fn grant(&mut self, transaction_id: u64, request: LockRequest) -> Result<()> {
        if self.locks.iter().any(|held| {
            held.transaction_id == transaction_id
                && mode_covers(held.request.mode, request.mode)
                && target_covers(&held.request.target, &request.target)
        }) {
            return Ok(());
        }
        let removed_bytes = self
            .locks
            .iter()
            .filter(|held| {
                held.transaction_id == transaction_id
                    && mode_covers(request.mode, held.request.mode)
                    && target_covers(&request.target, &held.request.target)
            })
            .map(held_lock_estimated_bytes)
            .sum::<usize>();
        let removed_entries = self
            .locks
            .iter()
            .filter(|held| {
                held.transaction_id == transaction_id
                    && mode_covers(request.mode, held.request.mode)
                    && target_covers(&request.target, &held.request.target)
            })
            .count();
        let added_bytes = lock_request_estimated_bytes(&request);
        let next_entries = self
            .locks
            .len()
            .saturating_sub(removed_entries)
            .saturating_add(1);
        let next_bytes = self
            .estimated_bytes
            .saturating_sub(removed_bytes)
            .saturating_add(added_bytes);
        if next_entries > self.limits.max_entries || next_bytes > self.limits.max_bytes {
            return Err(HawDBError::Execution(format!(
                "lock table resource budget exceeded: required_entries={next_entries} max_entries={} required_bytes={next_bytes} max_bytes={}; retry after competing transactions finish",
                self.limits.max_entries, self.limits.max_bytes
            )));
        }
        self.locks.retain(|held| {
            held.transaction_id != transaction_id
                || !mode_covers(request.mode, held.request.mode)
                || !target_covers(&request.target, &held.request.target)
        });
        self.locks.push(HeldLock {
            transaction_id,
            request,
        });
        self.estimated_bytes = next_bytes;
        Ok(())
    }

    pub fn release_transaction(&mut self, transaction_id: u64) {
        self.locks
            .retain(|held| held.transaction_id != transaction_id);
        self.estimated_bytes = self.locks.iter().map(held_lock_estimated_bytes).sum();
    }

    pub fn savepoint(&self, transaction_id: u64) -> Vec<LockRequest> {
        self.locks
            .iter()
            .filter(|held| held.transaction_id == transaction_id)
            .map(|held| held.request.clone())
            .collect()
    }

    pub fn restore_transaction(&mut self, transaction_id: u64, requests: Vec<LockRequest>) {
        self.release_transaction(transaction_id);
        self.locks
            .extend(requests.into_iter().map(|request| HeldLock {
                transaction_id,
                request,
            }));
        self.estimated_bytes = self.locks.iter().map(held_lock_estimated_bytes).sum();
    }

    #[cfg(test)]
    fn lock_count(&self) -> usize {
        self.locks.len()
    }

    #[cfg(test)]
    fn with_limits(limits: LockTableLimits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }
}

fn held_lock_estimated_bytes(held: &HeldLock) -> usize {
    lock_request_estimated_bytes(&held.request)
}

fn covering_mode(requested: LockMode, held: &[&HeldLock]) -> LockMode {
    if held.iter().any(|held| held.request.mode != requested) {
        // Shared and optimistic-commit modes are incomparable: each excludes
        // holders the other admits. Their only common covering mode is X.
        LockMode::Exclusive
    } else {
        requested
    }
}

fn lock_request_estimated_bytes(request: &LockRequest) -> usize {
    size_of::<HeldLock>().saturating_add(match &request.target {
        LockTarget::Database => 0,
        LockTarget::RelationalTable { table } => table.len(),
        LockTarget::RelationalRange {
            namespace,
            lower,
            upper,
        } => lock_namespace_estimated_bytes(namespace)
            .saturating_add(bound_estimated_bytes(lower))
            .saturating_add(bound_estimated_bytes(upper)),
        LockTarget::GraphAllocation { .. }
        | LockTarget::GraphNode { .. }
        | LockTarget::GraphRelationship { .. }
        | LockTarget::GraphNodeDeleteGuard { .. }
        | LockTarget::GraphAdjacency { .. } => 0,
        LockTarget::GraphLabel { label } => label.len(),
        LockTarget::GraphRelationshipType { rel_type } => rel_type.len(),
    })
}

fn lock_namespace_estimated_bytes(namespace: &LockNamespace) -> usize {
    match namespace {
        LockNamespace::RelationalIndex { table, columns } => table.len().saturating_add(
            columns
                .iter()
                .map(|column| size_of::<String>().saturating_add(column.len()))
                .sum(),
        ),
    }
}

fn bound_estimated_bytes(bound: &Bound<RelationalKey>) -> usize {
    let (Bound::Included(key) | Bound::Excluded(key)) = bound else {
        return 0;
    };
    size_of::<RelationalKey>().saturating_add(
        key.0
            .iter()
            .map(|value| {
                size_of::<crate::relational::RelationalValue>()
                    .saturating_add(value.estimated_payload_bytes())
            })
            .sum(),
    )
}

/// Bounded FIFO metadata for requests waiting on logical locks. One request
/// per transaction may wait at a time; callers serialize all operations with
/// the granted lock table. Compatible requests may pass older waiters.
#[derive(Debug, Default)]
pub struct LockWaitQueue {
    requests: VecDeque<HeldLock>,
    estimated_bytes: usize,
    limits: LockTableLimits,
}

impl LockWaitQueue {
    pub fn enqueue(&mut self, transaction_id: u64, request: &LockRequest) -> Result<()> {
        if let Some(existing) = self
            .requests
            .iter()
            .find(|r| r.transaction_id == transaction_id)
        {
            if existing.request == *request {
                return Ok(());
            }
            return Err(HawDBError::Execution(
                "transaction already has a different pending lock request".into(),
            ));
        }
        let next_entries = self.requests.len().saturating_add(1);
        let next_bytes = self
            .estimated_bytes
            .saturating_add(lock_request_estimated_bytes(request));
        if next_entries > self.limits.max_entries || next_bytes > self.limits.max_bytes {
            return Err(HawDBError::Execution(format!(
                "lock wait queue resource budget exceeded: required_entries={next_entries} max_entries={} required_bytes={next_bytes} max_bytes={}; retry after competing transactions finish",
                self.limits.max_entries, self.limits.max_bytes,
            )));
        }
        self.requests.push_back(HeldLock {
            transaction_id,
            request: request.clone(),
        });
        self.estimated_bytes = next_bytes;
        Ok(())
    }

    pub fn remove(&mut self, transaction_id: u64) {
        if let Some(index) = self
            .requests
            .iter()
            .position(|r| r.transaction_id == transaction_id)
        {
            let removed = self
                .requests
                .remove(index)
                .expect("located pending request");
            self.estimated_bytes -= held_lock_estimated_bytes(&removed);
        }
    }

    /// A not-yet-enqueued requester is younger than every current waiter.
    pub fn blockers(&self, transaction_id: u64, request: &LockRequest) -> BTreeSet<u64> {
        self.requests
            .iter()
            .take_while(|r| r.transaction_id != transaction_id)
            .filter(|r| requests_conflict(&r.request, request))
            .map(|r| r.transaction_id)
            .collect()
    }

    /// Derive dependencies from current ownership and queue order, rather than
    /// retaining stale edges after grant, timeout, rollback or cancellation.
    /// The BFS stores only visited waiters and parents, not a quadratic graph.
    pub fn check_deadlock(&self, transaction_id: u64, locks: &LockTable) -> Result<()> {
        let requests = self
            .requests
            .iter()
            .map(|pending| (pending.transaction_id, &pending.request))
            .collect::<BTreeMap<_, _>>();
        let mut parents = BTreeMap::from([(transaction_id, None)]);
        let mut pending = VecDeque::from([transaction_id]);
        while let Some(waiter) = pending.pop_front() {
            let Some(request) = requests.get(&waiter) else {
                continue; // An active owner that is not waiting cannot close a cycle.
            };
            let mut blockers = locks.blockers(waiter, request);
            blockers.extend(self.blockers(waiter, request));
            for blocker in blockers {
                if blocker == transaction_id {
                    let mut path = vec![waiter];
                    let mut cursor = waiter;
                    while let Some(parent) = parents[&cursor] {
                        path.push(parent);
                        cursor = parent;
                    }
                    path.reverse();
                    path.push(transaction_id);
                    return Err(deadlock_error(transaction_id, &path));
                }
                if requests.contains_key(&blocker)
                    && let std::collections::btree_map::Entry::Vacant(entry) =
                        parents.entry(blocker)
                {
                    entry.insert(Some(waiter));
                    pending.push_back(blocker);
                }
            }
        }
        Ok(())
    }
}

fn requests_conflict(left: &LockRequest, right: &LockRequest) -> bool {
    modes_conflict(left.mode, right.mode) && targets_overlap(&left.target, &right.target)
}

#[derive(Debug, Default)]
pub struct WaitForGraph {
    edges: BTreeMap<u64, BTreeSet<u64>>,
}

impl WaitForGraph {
    pub fn register(&mut self, waiter: u64, owners: &BTreeSet<u64>) -> Result<()> {
        if owners.is_empty() {
            self.clear_waiter(waiter);
            return Ok(());
        }
        self.edges.insert(waiter, owners.clone());
        if let Some(cycle) = self.cycle_from(waiter) {
            self.edges.remove(&waiter);
            return Err(deadlock_error(waiter, &cycle));
        }
        Ok(())
    }

    pub fn clear_waiter(&mut self, transaction_id: u64) {
        self.edges.remove(&transaction_id);
    }

    pub fn remove_transaction(&mut self, transaction_id: u64) {
        self.edges.remove(&transaction_id);
        self.edges.retain(|_, owners| {
            owners.remove(&transaction_id);
            !owners.is_empty()
        });
    }

    fn cycle_from(&self, waiter: u64) -> Option<Vec<u64>> {
        let mut path = vec![waiter];
        let mut visiting = BTreeSet::from([waiter]);
        self.path_to(waiter, waiter, &mut path, &mut visiting)
    }

    fn path_to(
        &self,
        current: u64,
        target: u64,
        path: &mut Vec<u64>,
        visiting: &mut BTreeSet<u64>,
    ) -> Option<Vec<u64>> {
        for owner in self.edges.get(&current).into_iter().flatten().copied() {
            path.push(owner);
            if owner == target {
                return Some(path.clone());
            }
            if visiting.insert(owner) {
                if let Some(cycle) = self.path_to(owner, target, path, visiting) {
                    return Some(cycle);
                }
                visiting.remove(&owner);
            }
            path.pop();
        }
        None
    }
}

fn modes_conflict(left: LockMode, right: LockMode) -> bool {
    !matches!(
        (left, right),
        (LockMode::Shared, LockMode::Shared)
            | (LockMode::OptimisticCommit, LockMode::OptimisticCommit)
    )
}

fn mode_covers(held: LockMode, requested: LockMode) -> bool {
    held == LockMode::Exclusive || held == requested
}

fn target_covers(held: &LockTarget, requested: &LockTarget) -> bool {
    match (held, requested) {
        (LockTarget::Database, _) => true,
        (_, LockTarget::Database) => false,
        (
            LockTarget::RelationalTable { table: held },
            LockTarget::RelationalTable { table: requested },
        ) => held == requested,
        (LockTarget::RelationalTable { table }, LockTarget::RelationalRange { namespace, .. }) => {
            relational_namespace_table(namespace) == table
        }
        (LockTarget::RelationalRange { .. }, LockTarget::RelationalTable { .. }) => false,
        (
            LockTarget::RelationalRange {
                namespace: held_namespace,
                lower: held_lower,
                upper: held_upper,
            },
            LockTarget::RelationalRange {
                namespace: requested_namespace,
                lower: requested_lower,
                upper: requested_upper,
            },
        ) => {
            held_namespace == requested_namespace
                && lower_bound_covers(held_lower, requested_lower)
                && upper_bound_covers(held_upper, requested_upper)
        }
        (
            LockTarget::GraphAllocation { kind: held },
            LockTarget::GraphAllocation { kind: requested },
        ) => held == requested,
        (LockTarget::GraphLabel { label: held }, LockTarget::GraphLabel { label: requested }) => {
            held == requested
        }
        (
            LockTarget::GraphRelationshipType { rel_type: held },
            LockTarget::GraphRelationshipType {
                rel_type: requested,
            },
        ) => held == requested,
        (LockTarget::GraphNode { node_id: held }, LockTarget::GraphNode { node_id: requested })
        | (
            LockTarget::GraphNodeDeleteGuard { node_id: held },
            LockTarget::GraphNodeDeleteGuard { node_id: requested },
        ) => held == requested,
        (
            LockTarget::GraphRelationship {
                relationship_id: held,
            },
            LockTarget::GraphRelationship {
                relationship_id: requested,
            },
        ) => held == requested,
        (
            LockTarget::GraphAdjacency {
                node_id: held_node,
                rel_type: held_type,
                direction: held_direction,
            },
            LockTarget::GraphAdjacency {
                node_id: requested_node,
                rel_type: requested_type,
                direction: requested_direction,
            },
        ) => {
            held_node == requested_node
                && held_direction == requested_direction
                && (held_type.is_none() || held_type == requested_type)
        }
        _ => false,
    }
}

fn lower_bound_covers(held: &Bound<RelationalKey>, requested: &Bound<RelationalKey>) -> bool {
    match (held, requested) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(held), Bound::Included(requested)) => held <= requested,
        (Bound::Included(held), Bound::Excluded(requested)) => held <= requested,
        (Bound::Excluded(held), Bound::Included(requested)) => held < requested,
        (Bound::Excluded(held), Bound::Excluded(requested)) => held <= requested,
    }
}

fn upper_bound_covers(held: &Bound<RelationalKey>, requested: &Bound<RelationalKey>) -> bool {
    match (held, requested) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(held), Bound::Included(requested)) => held >= requested,
        (Bound::Included(held), Bound::Excluded(requested)) => held >= requested,
        (Bound::Excluded(held), Bound::Included(requested)) => held > requested,
        (Bound::Excluded(held), Bound::Excluded(requested)) => held >= requested,
    }
}

fn targets_overlap(left: &LockTarget, right: &LockTarget) -> bool {
    match (left, right) {
        (LockTarget::Database, _) | (_, LockTarget::Database) => true,
        (
            LockTarget::RelationalTable { table: left },
            LockTarget::RelationalTable { table: right },
        ) => left == right,
        (LockTarget::RelationalTable { table }, LockTarget::RelationalRange { namespace, .. })
        | (LockTarget::RelationalRange { namespace, .. }, LockTarget::RelationalTable { table }) => {
            relational_namespace_table(namespace) == table
        }
        (
            LockTarget::RelationalRange {
                namespace: left_namespace,
                lower: left_lower,
                upper: left_upper,
            },
            LockTarget::RelationalRange {
                namespace: right_namespace,
                lower: right_lower,
                upper: right_upper,
            },
        ) => {
            left_namespace == right_namespace
                && !upper_is_before_lower(left_upper, right_lower)
                && !upper_is_before_lower(right_upper, left_lower)
        }
        (
            LockTarget::GraphAllocation { kind: left },
            LockTarget::GraphAllocation { kind: right },
        ) => left == right,
        (LockTarget::GraphLabel { label: left }, LockTarget::GraphLabel { label: right }) => {
            left == right
        }
        (
            LockTarget::GraphRelationshipType { rel_type: left },
            LockTarget::GraphRelationshipType { rel_type: right },
        ) => left == right,
        (LockTarget::GraphNode { node_id: left }, LockTarget::GraphNode { node_id: right })
        | (
            LockTarget::GraphNodeDeleteGuard { node_id: left },
            LockTarget::GraphNodeDeleteGuard { node_id: right },
        ) => left == right,
        (
            LockTarget::GraphRelationship {
                relationship_id: left,
            },
            LockTarget::GraphRelationship {
                relationship_id: right,
            },
        ) => left == right,
        (
            LockTarget::GraphAdjacency {
                node_id: left_node,
                rel_type: left_type,
                direction: left_direction,
            },
            LockTarget::GraphAdjacency {
                node_id: right_node,
                rel_type: right_type,
                direction: right_direction,
            },
        ) => {
            left_node == right_node
                && left_direction == right_direction
                && (left_type.is_none() || right_type.is_none() || left_type == right_type)
        }
        _ => false,
    }
}

fn relational_namespace_table(namespace: &LockNamespace) -> &str {
    match namespace {
        LockNamespace::RelationalIndex { table, .. } => table,
    }
}

fn lock_target_cmp(left: &LockTarget, right: &LockTarget) -> Ordering {
    let left_rank = lock_target_rank(left);
    let right_rank = lock_target_rank(right);
    let rank_order = left_rank.cmp(&right_rank);
    if rank_order != Ordering::Equal {
        return rank_order;
    }
    match (left, right) {
        (LockTarget::Database, LockTarget::Database) => Ordering::Equal,
        (LockTarget::Database, _) => Ordering::Less,
        (_, LockTarget::Database) => Ordering::Greater,
        (
            LockTarget::RelationalTable { table: left },
            LockTarget::RelationalTable { table: right },
        ) => left.cmp(right),
        (LockTarget::RelationalTable { .. }, LockTarget::RelationalRange { .. }) => Ordering::Less,
        (LockTarget::RelationalRange { .. }, LockTarget::RelationalTable { .. }) => {
            Ordering::Greater
        }
        (
            LockTarget::RelationalRange {
                namespace: left_namespace,
                lower: left_lower,
                upper: left_upper,
            },
            LockTarget::RelationalRange {
                namespace: right_namespace,
                lower: right_lower,
                upper: right_upper,
            },
        ) => left_namespace
            .cmp(right_namespace)
            .then_with(|| bound_cmp(left_lower, right_lower))
            .then_with(|| bound_cmp(left_upper, right_upper)),
        (
            LockTarget::GraphAllocation { kind: left },
            LockTarget::GraphAllocation { kind: right },
        ) => left.cmp(right),
        (LockTarget::GraphLabel { label: left }, LockTarget::GraphLabel { label: right }) => {
            left.cmp(right)
        }
        (
            LockTarget::GraphRelationshipType { rel_type: left },
            LockTarget::GraphRelationshipType { rel_type: right },
        ) => left.cmp(right),
        (LockTarget::GraphNode { node_id: left }, LockTarget::GraphNode { node_id: right })
        | (
            LockTarget::GraphRelationship {
                relationship_id: left,
            },
            LockTarget::GraphRelationship {
                relationship_id: right,
            },
        )
        | (
            LockTarget::GraphNodeDeleteGuard { node_id: left },
            LockTarget::GraphNodeDeleteGuard { node_id: right },
        ) => left.cmp(right),
        (
            LockTarget::GraphAdjacency {
                node_id: left_node,
                rel_type: left_type,
                direction: left_direction,
            },
            LockTarget::GraphAdjacency {
                node_id: right_node,
                rel_type: right_type,
                direction: right_direction,
            },
        ) => left_node
            .cmp(right_node)
            .then(left_direction.cmp(right_direction))
            .then(left_type.cmp(right_type)),
        _ => Ordering::Equal,
    }
}

fn lock_target_rank(target: &LockTarget) -> u8 {
    match target {
        LockTarget::Database => 0,
        LockTarget::RelationalTable { .. } => 1,
        LockTarget::RelationalRange { .. } => 2,
        LockTarget::GraphAllocation { .. } => 3,
        LockTarget::GraphLabel { .. } => 4,
        LockTarget::GraphRelationshipType { .. } => 5,
        LockTarget::GraphNode { .. } => 6,
        LockTarget::GraphNodeDeleteGuard { .. } => 7,
        LockTarget::GraphRelationship { .. } => 8,
        LockTarget::GraphAdjacency { .. } => 9,
    }
}

fn bound_cmp(left: &Bound<RelationalKey>, right: &Bound<RelationalKey>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => left.cmp(right),
        (Bound::Included(left), Bound::Excluded(right)) => left.cmp(right).then(Ordering::Less),
        (Bound::Excluded(left), Bound::Included(right)) => left.cmp(right).then(Ordering::Greater),
    }
}

fn upper_is_before_lower(upper: &Bound<RelationalKey>, lower: &Bound<RelationalKey>) -> bool {
    match (upper, lower) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(upper), Bound::Included(lower)) => upper < lower,
        (Bound::Included(upper), Bound::Excluded(lower))
        | (Bound::Excluded(upper), Bound::Included(lower))
        | (Bound::Excluded(upper), Bound::Excluded(lower)) => upper <= lower,
    }
}

fn deadlock_error(victim: u64, cycle: &[u64]) -> HawDBError {
    let cycle = cycle
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" -> ");
    HawDBError::Execution(format!(
        "deadlock detected; transaction {victim} selected as victim; wait cycle: {cycle}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::RelationalValue;

    fn key(value: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(value)])
    }

    fn range(
        mode: LockMode,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    ) -> LockRequest {
        LockRequest::relational_range(mode, "messages", vec!["id".to_string()], lower, upper)
    }

    #[test]
    fn wait_queue_preserves_priority_and_allows_compatible_progress() {
        let mut queue = LockWaitQueue::default();
        let older = LockRequest::graph_node(LockMode::Exclusive, 1);
        queue.enqueue(2, &older).unwrap();
        let compatible = LockRequest::graph_node(LockMode::Exclusive, 2);
        assert!(queue.blockers(3, &compatible).is_empty());
        assert_eq!(queue.blockers(3, &older), BTreeSet::from([2]));
        queue.enqueue(3, &older).unwrap();
        queue.enqueue(2, &older).unwrap(); // Wakeups must not move the old ticket.
        assert!(queue.blockers(2, &older).is_empty());
        assert_eq!(queue.blockers(3, &older), BTreeSet::from([2]));
        queue.remove(2);
        assert!(queue.blockers(3, &older).is_empty());
    }

    #[test]
    fn wait_queue_admission_is_bounded_atomic_and_refundable() {
        let request = LockRequest::relational_table(LockMode::Exclusive, "messages");
        let bytes = lock_request_estimated_bytes(&request);
        let mut queue = LockWaitQueue {
            limits: LockTableLimits {
                max_entries: 1,
                max_bytes: bytes,
                ..LockTableLimits::default()
            },
            ..LockWaitQueue::default()
        };
        queue.enqueue(1, &request).unwrap();
        queue.enqueue(1, &request).unwrap();
        assert_eq!(queue.estimated_bytes, bytes);
        assert!(queue
            .enqueue(2, &request)
            .unwrap_err()
            .to_string()
            .contains("resource budget exceeded"));
        assert!(queue
            .enqueue(1, &LockRequest::database(LockMode::Shared))
            .is_err());
        assert_eq!(queue.requests.len(), 1);
        queue.remove(1);
        assert_eq!(queue.estimated_bytes, 0);
        queue.limits.max_bytes = bytes - 1;
        assert!(queue.enqueue(2, &request).is_err());
        assert!(queue.requests.is_empty());
        assert_eq!(queue.estimated_bytes, 0);
    }

    #[test]
    fn wait_queue_detects_queue_owner_cycles_without_stale_dependencies() {
        let mut locks = LockTable::default();
        locks
            .grant(1, LockRequest::graph_node(LockMode::Exclusive, 1))
            .unwrap();
        let mut queue = LockWaitQueue::default();
        queue
            .enqueue(2, &LockRequest::database(LockMode::Exclusive))
            .unwrap();
        queue.check_deadlock(2, &locks).unwrap();
        queue
            .enqueue(1, &LockRequest::graph_node(LockMode::Exclusive, 2))
            .unwrap();
        let error = queue.check_deadlock(1, &locks).unwrap_err();
        assert!(error
            .to_string()
            .contains("transaction 1 selected as victim"));
        assert!(error.to_string().contains("1 -> 2 -> 1"));
        queue.remove(2);
        queue.check_deadlock(1, &locks).unwrap();
        assert!(queue
            .blockers(1, &LockRequest::graph_node(LockMode::Exclusive, 2))
            .is_empty());
    }

    #[test]
    fn wait_queue_matches_independent_dependency_graph() {
        // Literal compatibility matrix and two-bit target sets form an oracle
        // independent of the production overlap helpers and BFS traversal.
        let conflict = [[false, true, true], [true, true, true], [true, true, false]];
        let modes = [
            LockMode::Shared,
            LockMode::Exclusive,
            LockMode::OptimisticCommit,
        ];
        let request = |mode: usize, mask: usize| match mask {
            1 => LockRequest::graph_node(modes[mode], 1),
            2 => LockRequest::graph_node(modes[mode], 2),
            _ => LockRequest::database(modes[mode]),
        };
        let mut seed = 0x6241_c03a_f723_1875_u64;
        let mut pick = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as usize % n
        };
        for _ in 0..4096 {
            let held = std::array::from_fn::<_, 3, _>(|_| (pick(3), pick(4)));
            let wanted = std::array::from_fn::<_, 3, _>(|_| (pick(3), pick(3) + 1));
            let mut order = [0, 1, 2];
            order.swap(0, pick(3));
            order.swap(1, pick(3));
            let mut locks = LockTable::default();
            let mut queue = LockWaitQueue::default();
            for (id, &(mode, mask)) in held.iter().enumerate() {
                if mask != 0 {
                    locks.grant(id as u64, request(mode, mask)).unwrap();
                }
            }
            for id in order {
                queue
                    .enqueue(id as u64, &request(wanted[id].0, wanted[id].1))
                    .unwrap();
            }
            let mut edges = [[false; 3]; 3];
            for id in 0..3 {
                let (mode, mask) = wanted[id];
                let mut older = BTreeSet::new();
                for other in order.into_iter().take_while(|&other| other != id) {
                    if conflict[mode][wanted[other].0] && mask & wanted[other].1 != 0 {
                        older.insert(other as u64);
                        edges[id][other] = true;
                    }
                }
                assert_eq!(queue.blockers(id as u64, &request(mode, mask)), older);
                for other in 0..3 {
                    edges[id][other] |=
                        other != id && conflict[mode][held[other].0] && mask & held[other].1 != 0;
                }
            }
            for middle in 0..3 {
                for from in 0..3 {
                    for to in 0..3 {
                        edges[from][to] |= edges[from][middle] && edges[middle][to];
                    }
                }
            }
            for (id, reachable) in edges.iter().enumerate() {
                assert_eq!(
                    queue.check_deadlock(id as u64, &locks).is_err(),
                    reachable[id]
                );
            }
        }
    }

    #[test]
    fn optimistic_commit_escalation_covers_incomparable_modes() {
        let mut table = LockTable::with_limits(LockTableLimits {
            escalation_entries_per_table: 1,
            ..LockTableLimits::default()
        });
        table
            .grant(
                1,
                LockRequest::graph_adjacency(
                    LockMode::OptimisticCommit,
                    7,
                    Some(1),
                    GraphAdjacencyDirection::Outgoing,
                ),
            )
            .unwrap();
        let saved = table.savepoint(1);
        let request = table.normalized_request(
            1,
            LockRequest::graph_adjacency(
                LockMode::Shared,
                7,
                Some(2),
                GraphAdjacencyDirection::Outgoing,
            ),
        );
        assert_eq!(
            request,
            LockRequest::graph_adjacency(
                LockMode::Exclusive,
                7,
                None,
                GraphAdjacencyDirection::Outgoing,
            )
        );
        table.grant(1, request).unwrap();
        assert_eq!(table.lock_count(), 1);
        table.restore_transaction(1, saved);
        assert!(!table.covers_all(
            1,
            &[LockRequest::graph_adjacency(
                LockMode::Shared,
                7,
                Some(1),
                GraphAdjacencyDirection::Outgoing,
            )]
        ));
        assert!(table.covers_all(
            1,
            &[LockRequest::graph_adjacency(
                LockMode::OptimisticCommit,
                7,
                Some(1),
                GraphAdjacencyDirection::Outgoing,
            )]
        ));
    }

    #[test]
    fn optimistic_commit_modes_preserve_regular_exclusion_and_coverage() {
        let modes = [
            LockMode::Shared,
            LockMode::Exclusive,
            LockMode::OptimisticCommit,
        ];
        let conflicts = [[false, true, true], [true, true, true], [true, true, false]];
        let covers = [
            [true, false, false],
            [true, true, true],
            [false, false, true],
        ];
        for (i, held) in modes.into_iter().enumerate() {
            for (j, requested) in modes.into_iter().enumerate() {
                let mut table = LockTable::default();
                table.grant(1, LockRequest::database(held)).unwrap();
                assert_eq!(
                    !table
                        .blockers(2, &LockRequest::database(requested))
                        .is_empty(),
                    conflicts[i][j],
                );
                assert_eq!(
                    table.covers_all(1, &[LockRequest::database(requested)]),
                    covers[i][j]
                );
                // Database admission must also exclude narrow regular locks.
                assert_eq!(
                    !table
                        .blockers(2, &LockRequest::graph_node(requested, 9))
                        .is_empty(),
                    conflicts[i][j],
                );
            }
        }
        let mut table = LockTable::default();
        table
            .grant(1, LockRequest::database(LockMode::OptimisticCommit))
            .unwrap();
        table
            .grant(2, LockRequest::database(LockMode::OptimisticCommit))
            .unwrap();
        table.release_transaction(1);
        assert_eq!(
            table.blockers(3, &LockRequest::database(LockMode::Shared)),
            BTreeSet::from([2])
        );
        table.release_transaction(2);
        assert!(table
            .blockers(3, &LockRequest::database(LockMode::Exclusive))
            .is_empty());
    }

    #[test]
    fn point_locks_conflict_only_on_the_same_key() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                LockRequest::relational_point(
                    LockMode::Exclusive,
                    "messages",
                    vec!["id".to_string()],
                    key(7),
                ),
            )
            .unwrap();

        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["id".to_string()],
                    key(7),
                )
            ),
            BTreeSet::from([1])
        );
        assert!(table
            .blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Exclusive,
                    "messages",
                    vec!["id".to_string()],
                    key(8),
                )
            )
            .is_empty());
    }

    #[test]
    fn graph_entity_and_adjacency_locks_conflict_only_on_logical_identity() {
        let mut table = LockTable::default();
        table
            .grant(1, LockRequest::graph_node(LockMode::Exclusive, 7))
            .unwrap();
        table
            .grant(
                1,
                LockRequest::graph_adjacency(
                    LockMode::Exclusive,
                    7,
                    None,
                    GraphAdjacencyDirection::Outgoing,
                ),
            )
            .unwrap();

        assert_eq!(
            table.blockers(2, &LockRequest::graph_node(LockMode::Exclusive, 7)),
            BTreeSet::from([1])
        );
        assert!(table
            .blockers(2, &LockRequest::graph_node(LockMode::Exclusive, 8))
            .is_empty());
        assert_eq!(
            table.blockers(
                2,
                &LockRequest::graph_adjacency(
                    LockMode::Exclusive,
                    7,
                    Some(3),
                    GraphAdjacencyDirection::Outgoing,
                )
            ),
            BTreeSet::from([1])
        );
        assert!(table
            .blockers(
                2,
                &LockRequest::graph_adjacency(
                    LockMode::Exclusive,
                    7,
                    Some(3),
                    GraphAdjacencyDirection::Incoming,
                )
            )
            .is_empty());
    }

    #[test]
    fn statement_savepoint_restores_replaced_lock_and_budget() {
        let mut table = LockTable::default();
        let original = LockRequest::graph_node(LockMode::Shared, 7);
        table.grant(1, original.clone()).unwrap();
        let savepoint = table.savepoint(1);
        let original_bytes = table.estimated_bytes;

        table
            .grant(1, LockRequest::graph_node(LockMode::Exclusive, 7))
            .unwrap();
        table
            .grant(1, LockRequest::graph_node(LockMode::Exclusive, 8))
            .unwrap();
        table.restore_transaction(1, savepoint);

        assert_eq!(table.lock_count(), 1);
        assert_eq!(table.estimated_bytes, original_bytes);
        assert!(table.covers_all(1, &[original]));
        assert!(!table.covers_all(1, &[LockRequest::graph_node(LockMode::Exclusive, 7)]));
        assert!(!table.covers_all(1, &[LockRequest::graph_node(LockMode::Shared, 8)]));
    }

    #[test]
    fn half_open_ranges_have_no_boundary_conflict() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                range(
                    LockMode::Exclusive,
                    Bound::Included(key(10)),
                    Bound::Excluded(key(20)),
                ),
            )
            .unwrap();

        assert!(table
            .blockers(
                2,
                &range(
                    LockMode::Exclusive,
                    Bound::Included(key(20)),
                    Bound::Included(key(30)),
                )
            )
            .is_empty());
        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["id".to_string()],
                    key(19),
                )
            ),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn shared_ranges_are_compatible_and_database_exclusive_is_universal() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                range(LockMode::Shared, Bound::Unbounded, Bound::Unbounded),
            )
            .unwrap();
        assert!(table
            .blockers(
                2,
                &range(
                    LockMode::Shared,
                    Bound::Included(key(1)),
                    Bound::Included(key(1)),
                )
            )
            .is_empty());
        assert_eq!(
            table.blockers(2, &LockRequest::database(LockMode::Exclusive)),
            BTreeSet::from([1])
        );
        table.release_transaction(1);
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn relational_table_lock_conflicts_with_every_index_namespace_on_that_table() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                LockRequest::relational_table(LockMode::Exclusive, "messages"),
            )
            .unwrap();

        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["owner_id".to_string()],
                    key(7),
                )
            ),
            BTreeSet::from([1])
        );
        assert!(table
            .blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Exclusive,
                    "threads",
                    vec!["id".to_string()],
                    key(7),
                )
            )
            .is_empty());
    }

    #[test]
    fn relational_table_lock_covers_narrower_index_requests() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                LockRequest::relational_table(LockMode::Exclusive, "messages"),
            )
            .unwrap();

        assert!(table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Exclusive,
                "messages",
                vec!["id".to_string()],
                key(7),
            )]
        ));
        assert!(!table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Exclusive,
                "threads",
                vec!["id".to_string()],
                key(7),
            )]
        ));
    }

    #[test]
    fn a_held_range_covers_narrower_repeated_requests() {
        let mut table = LockTable::default();
        table
            .grant(
                1,
                range(
                    LockMode::Shared,
                    Bound::Included(key(10)),
                    Bound::Excluded(key(20)),
                ),
            )
            .unwrap();

        assert!(table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Shared,
                "messages",
                vec!["id".to_string()],
                key(15),
            )]
        ));
        assert!(!table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Shared,
                "messages",
                vec!["id".to_string()],
                key(20),
            )]
        ));
        assert!(!table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Exclusive,
                "messages",
                vec!["id".to_string()],
                key(15),
            )]
        ));
    }

    #[test]
    fn narrow_locks_escalate_before_the_next_entry_is_granted() {
        let mut table = LockTable::with_limits(LockTableLimits {
            escalation_entries_per_table: 1,
            ..LockTableLimits::default()
        });
        table
            .grant(
                1,
                LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["id".to_string()],
                    key(1),
                ),
            )
            .unwrap();
        let normalized = table.normalized_request(
            1,
            LockRequest::relational_point(
                LockMode::Exclusive,
                "messages",
                vec!["id".to_string()],
                key(2),
            ),
        );
        assert_eq!(
            normalized,
            LockRequest::relational_table(LockMode::Exclusive, "messages")
        );
        table.grant(1, normalized).unwrap();

        assert_eq!(table.lock_count(), 1);
        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["owner_id".to_string()],
                    key(9),
                )
            ),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn lock_table_hard_cap_rejects_without_growing_residency() {
        let probe = LockRequest::relational_point(
            LockMode::Exclusive,
            "messages",
            vec!["id".to_string()],
            key(1),
        );
        let mut table = LockTable::with_limits(LockTableLimits {
            max_entries: 1,
            max_bytes: lock_request_estimated_bytes(&probe),
            escalation_entries_per_table: usize::MAX,
        });
        table.grant(1, probe).unwrap();

        let error = table
            .grant(
                2,
                LockRequest::relational_point(
                    LockMode::Exclusive,
                    "threads",
                    vec!["id".to_string()],
                    key(2),
                ),
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("lock table resource budget exceeded"));
        assert_eq!(table.lock_count(), 1);
    }

    #[test]
    fn wait_for_graph_detects_a_cycle_with_multiple_blockers() {
        let mut graph = WaitForGraph::default();
        graph.register(1, &BTreeSet::from([2, 4])).unwrap();
        graph.register(2, &BTreeSet::from([3])).unwrap();

        let error = graph.register(3, &BTreeSet::from([1, 5])).unwrap_err();

        assert!(error.to_string().contains("deadlock detected"));
        assert!(error.to_string().contains("3 -> 1 -> 2 -> 3"));
    }

    #[test]
    fn removing_an_owner_cleans_all_wait_dependencies() {
        let mut graph = WaitForGraph::default();
        graph.register(1, &BTreeSet::from([2, 3])).unwrap();
        graph.remove_transaction(2);
        assert_eq!(graph.edges, BTreeMap::from([(1, BTreeSet::from([3]))]));
        graph.remove_transaction(3);
        assert!(graph.edges.is_empty());
    }
}
