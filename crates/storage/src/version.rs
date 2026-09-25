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

//! Copy-on-write version stamps for optimistic transaction validation.
//!
//! This index retains only the newest committed stamp for each mutable
//! identity. The data snapshot itself remains owned by the graph and
//! relational stores; keeping these concerns separate lets a transaction
//! validate its write set without retaining a second copy of user data.

mod history_budget;
use history_budget::{HistoryBudget, HistoryLease};

use crate::{
    adjacency::AdjacencyDirection,
    cow::{CowPageWeight, CowSegmentedMap},
    relational::RelationalKey,
    NodeId, RelId,
};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

// All storage snapshots register, including transaction workspaces, savepoints,
// and snapshots from which another transaction might later be created.
#[derive(Debug, Clone, Default)]
pub(crate) struct VersionSnapshotPins(Arc<Mutex<BTreeMap<u64, usize>>>);

#[derive(Debug)]
pub(crate) struct VersionSnapshotPin {
    epoch: u64,
    pins: VersionSnapshotPins,
}

impl VersionSnapshotPins {
    pub(crate) fn pin(&self, epoch: u64) -> VersionSnapshotPin {
        let mut pins = self.0.lock().expect("version snapshot pins lock poisoned");
        *pins.entry(epoch).or_default() += 1;
        VersionSnapshotPin {
            epoch,
            pins: self.clone(),
        }
    }

    pub(crate) fn oldest_epoch(&self) -> Option<u64> {
        self.0
            .lock()
            .expect("version snapshot pins lock poisoned")
            .first_key_value()
            .map(|(&epoch, _)| epoch)
    }
}

impl VersionSnapshotPin {
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for VersionSnapshotPin {
    fn drop(&mut self) {
        let mut pins = self
            .pins
            .0
            .lock()
            .expect("version snapshot pins lock poisoned");
        let count = pins
            .get_mut(&self.epoch)
            .expect("registered version snapshot pin");
        *count -= 1;
        if *count == 0 {
            pins.remove(&self.epoch);
        }
    }
}

/// Estimated retained key/write bytes, excluding allocator and B-tree overhead.
pub const DEFAULT_MAX_VERSION_WRITE_SET_BYTES: usize = crate::config::DEFAULT_MAX_WAL_RECORD_BYTES;

/// Current-index payload estimate; excludes historical COW roots.
pub const DEFAULT_MAX_VERSION_INDEX_BYTES: usize = 64 * 1024 * 1024;
/// Shared budget for retained/staged non-Database root payload estimates.
pub const DEFAULT_MAX_RETAINED_VERSION_HISTORY_BYTES: usize = 256 * 1024 * 1024;

pub const DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES: usize =
    crate::config::DEFAULT_MAX_WAL_BATCH_OPERATIONS;

/// A mutable identity whose latest committed version participates in optimistic
/// transaction validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum VersionKey {
    Database,
    Schema,
    GraphNode(NodeId),
    GraphRelationship(RelId),
    GraphAdjacency {
        node_id: NodeId,
        direction: AdjacencyDirection,
    },
    RelationalRow {
        table: String,
        primary_key: RelationalKey,
    },
    RelationalIndex {
        table: String,
        index: String,
        key: RelationalKey,
    },
    ForeignKey {
        table: String,
        key: RelationalKey,
    },
    AppendTable(String),
}

impl CowPageWeight for VersionKey {
    fn cow_page_bytes(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        match self {
            Self::Database | Self::Schema => base,
            Self::GraphNode(_) | Self::GraphRelationship(_) => base,
            Self::GraphAdjacency { .. } => base,
            Self::RelationalRow { table, primary_key } => base
                .saturating_add(table.len())
                .saturating_add(relational_key_bytes(primary_key)),
            Self::RelationalIndex { table, index, key } => base
                .saturating_add(table.len())
                .saturating_add(index.len())
                .saturating_add(relational_key_bytes(key)),
            Self::ForeignKey { table, key } => base
                .saturating_add(table.len())
                .saturating_add(relational_key_bytes(key)),
            Self::AppendTable(table) => base.saturating_add(table.len()),
        }
    }
}

fn relational_key_bytes(key: &RelationalKey) -> usize {
    std::mem::size_of::<RelationalKey>().saturating_add(
        key.0
            .iter()
            .map(|value| {
                std::mem::size_of_val(value).saturating_add(value.estimated_payload_bytes())
            })
            .fold(0usize, usize::saturating_add),
    )
}

/// The current visibility state of a versioned identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionDisposition {
    Live,
    Tombstone,
}

/// The newest committed version of one identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionStamp {
    pub commit_epoch: u64,
    pub disposition: VersionDisposition,
}

impl CowPageWeight for VersionStamp {
    fn cow_page_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// The change made to an identity while a transaction is being staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionWrite {
    pub disposition: VersionDisposition,
}

/// A bounded, deduplicated transaction write set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionWriteSet {
    writes: BTreeMap<VersionKey, VersionWrite>,
    max_entries: usize,
    max_bytes: usize,
    estimated_bytes: usize,
}

impl Default for VersionWriteSet {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES)
    }
}

impl VersionWriteSet {
    pub fn new(max_entries: usize) -> Self {
        Self::with_limits(max_entries, DEFAULT_MAX_VERSION_WRITE_SET_BYTES)
    }

    /// Limits retained key/write estimates, not total allocator or process memory.
    pub fn with_limits(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            writes: BTreeMap::new(),
            max_entries,
            max_bytes,
            estimated_bytes: 0,
        }
    }

    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub fn len(&self) -> usize {
        self.writes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub fn contains_key(&self, key: &VersionKey) -> bool {
        self.writes.contains_key(key)
    }

    pub fn record_live(&mut self, key: VersionKey) -> Result<(), VersionWriteSetError> {
        self.record(key, VersionDisposition::Live)
    }

    pub fn record_tombstone(&mut self, key: VersionKey) -> Result<(), VersionWriteSetError> {
        self.record(key, VersionDisposition::Tombstone)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VersionKey, VersionWrite)> {
        self.writes.iter().map(|(key, write)| (key, *write))
    }

    fn record(
        &mut self,
        key: VersionKey,
        disposition: VersionDisposition,
    ) -> Result<(), VersionWriteSetError> {
        // Preserve the resident key on replacement: equal logical keys may
        // carry different allocation capacities, which this estimate excludes.
        if let Some(write) = self.writes.get_mut(&key) {
            write.disposition = disposition;
            return Ok(());
        }
        if self.writes.len() >= self.max_entries {
            return Err(VersionWriteSetError::LimitExceeded {
                max_entries: self.max_entries,
            });
        }
        let next_bytes = key
            .cow_page_bytes()
            .checked_add(std::mem::size_of::<VersionWrite>())
            .and_then(|bytes| self.estimated_bytes.checked_add(bytes))
            .filter(|bytes| *bytes <= self.max_bytes)
            .ok_or(VersionWriteSetError::ByteLimitExceeded {
                max_bytes: self.max_bytes,
            })?;
        self.writes.insert(key, VersionWrite { disposition });
        self.estimated_bytes = next_bytes;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionWriteSetError {
    LimitExceeded { max_entries: usize },
    ByteLimitExceeded { max_bytes: usize },
}

impl Display for VersionWriteSetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded { max_entries } => write!(
                formatter,
                "transaction version write set exceeds its {max_entries}-entry limit"
            ),
            Self::ByteLimitExceeded { max_bytes } => write!(
                formatter,
                "transaction version write set exceeds its {max_bytes}-byte estimated payload limit"
            ),
        }
    }
}

impl std::error::Error for VersionWriteSetError {}

/// A conflict between a transaction's read epoch and a newer committed write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionConflict {
    pub key: VersionKey,
    pub committed_epoch: u64,
}

/// Mutable version state shared by a live database handle.
#[derive(Debug, Clone)]
pub struct VersionIndex {
    // Database publication occurs after WAL on legacy paths. Keep this fixed
    // identity inline so publishing it never detaches a shared COW page.
    database_barrier: Option<VersionStamp>,
    stamps: CowSegmentedMap<VersionKey, VersionStamp>,
    estimated_bytes: usize,
    max_estimated_bytes: usize,
    history_budget: HistoryBudget,
    // Drop after the map, so the last holder releases storage before its charge.
    history_lease: Option<Arc<HistoryLease>>,
}

impl Default for VersionIndex {
    fn default() -> Self {
        Self::with_byte_limit(DEFAULT_MAX_VERSION_INDEX_BYTES)
    }
}

impl VersionIndex {
    pub(crate) fn with_byte_limit(max_estimated_bytes: usize) -> Self {
        Self::with_history_limit(
            max_estimated_bytes,
            DEFAULT_MAX_RETAINED_VERSION_HISTORY_BYTES,
        )
    }

    pub(crate) fn with_history_limit(max_estimated_bytes: usize, max_history_bytes: usize) -> Self {
        // Reserve the legacy barrier even before it exists, so legacy commit
        // publication cannot encounter a new admission failure after WAL.
        let reserved = version_stamp_bytes(&VersionKey::Database);
        assert!(max_estimated_bytes >= reserved);
        Self {
            database_barrier: None,
            stamps: CowSegmentedMap::default(),
            estimated_bytes: reserved,
            max_estimated_bytes,
            history_budget: HistoryBudget::new(max_history_bytes),
            history_lease: None,
        }
    }

    /// Includes a permanent reservation for the Database barrier, not allocator
    /// overhead, spare capacity, or pages retained by other snapshot roots.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub(crate) fn read_baseline(&self, epoch: u64) -> Self {
        let mut baseline = Self {
            database_barrier: None,
            stamps: CowSegmentedMap::default(),
            estimated_bytes: version_stamp_bytes(&VersionKey::Database),
            max_estimated_bytes: self.max_estimated_bytes,
            history_budget: self.history_budget.clone(),
            history_lease: None,
        };
        if epoch > 0 {
            baseline.apply_database_barrier(epoch);
        }
        baseline
    }

    pub(crate) fn admits(&self, writes: &VersionWriteSet) -> bool {
        let mut bytes = self.estimated_bytes;
        for (key, _) in writes.iter() {
            if *key != VersionKey::Database && !self.stamps.contains_key(key) {
                let Some(next) = bytes.checked_add(version_stamp_bytes(key)) else {
                    return false;
                };
                bytes = next;
            }
            if bytes > self.max_estimated_bytes {
                return false;
            }
        }
        bytes <= self.max_estimated_bytes
    }

    #[cfg(test)]
    pub(crate) fn retained_history_bytes(&self) -> usize {
        self.history_budget.used()
    }

    /// Reserve the complete candidate root before any COW allocation or WAL.
    /// The existing root remains charged until its last snapshot is dropped.
    pub(crate) fn stage(&self, writes: &VersionWriteSet, epoch: u64) -> Option<Self> {
        if !self.admits(writes) {
            return None;
        }
        let mut bytes = self.estimated_bytes - version_stamp_bytes(&VersionKey::Database);
        let mut changes_map = false;
        for (key, _) in writes.iter() {
            if *key != VersionKey::Database {
                changes_map = true;
                if !self.stamps.contains_key(key) {
                    bytes = bytes.checked_add(version_stamp_bytes(key))?;
                }
            }
        }
        let lease = if bytes == 0 {
            None
        } else if changes_map || self.history_lease.is_none() {
            Some(self.history_budget.reserve(bytes)?)
        } else {
            self.history_lease.clone()
        };
        let mut staged = self.clone();
        staged.history_lease = lease;
        staged.apply_prepared(writes, epoch);
        Some(staged)
    }

    fn insert_stamp(&mut self, key: VersionKey, stamp: VersionStamp) {
        if key == VersionKey::Database {
            self.database_barrier = Some(stamp);
            return;
        }
        if !self.stamps.contains_key(&key) {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(version_stamp_bytes(&key));
        }
        self.stamps.insert(key, stamp);
    }

    fn recount_bytes(&mut self) {
        self.estimated_bytes = self.stamps.iter().fold(
            version_stamp_bytes(&VersionKey::Database),
            |bytes, (key, _)| bytes.saturating_add(version_stamp_bytes(key)),
        );
    }

    /// Creates a conservative baseline for all live identities present in a
    /// recovered checkpoint. WAL replay can then overwrite individual stamps.
    pub fn from_live_keys_at_epoch(
        keys: impl IntoIterator<Item = VersionKey>,
        commit_epoch: u64,
    ) -> Self {
        let mut stamps = keys
            .into_iter()
            .map(|key| {
                (
                    key,
                    VersionStamp {
                        commit_epoch,
                        disposition: VersionDisposition::Live,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut index = Self {
            database_barrier: stamps.remove(&VersionKey::Database),
            stamps: stamps.into(),
            ..Self::default()
        };
        index.recount_bytes();
        index
    }

    pub fn len(&self) -> usize {
        self.stamps.len() + usize::from(self.database_barrier.is_some())
    }

    pub fn is_empty(&self) -> bool {
        self.stamps.is_empty() && self.database_barrier.is_none()
    }

    pub fn stamp(&self, key: &VersionKey) -> Option<VersionStamp> {
        if *key == VersionKey::Database {
            self.database_barrier
        } else {
            self.stamps.get(key).copied()
        }
    }

    /// Returns the first write-write conflict under first-committer-wins.
    pub fn first_conflict(
        &self,
        writes: &VersionWriteSet,
        read_epoch: u64,
    ) -> Option<VersionConflict> {
        writes.iter().find_map(|(key, _)| {
            let stamp = self.stamp(key)?;
            (stamp.commit_epoch > read_epoch).then(|| VersionConflict {
                key: key.clone(),
                committed_epoch: stamp.commit_epoch,
            })
        })
    }

    /// Applies stamps to an unmanaged index without resource admission.
    /// Callers must supply a nonzero epoch. Production commits use `stage`
    /// before WAL, then install that prepared index after canonical application.
    /// This construction/test primitive rejects indexes holding history leases.
    pub fn apply(&mut self, writes: &VersionWriteSet, commit_epoch: u64) {
        assert!(
            self.history_lease.is_none(),
            "managed history requires a staged update"
        );
        self.apply_prepared(writes, commit_epoch);
    }

    fn apply_prepared(&mut self, writes: &VersionWriteSet, commit_epoch: u64) {
        debug_assert_ne!(commit_epoch, 0, "committed version stamps require an epoch");
        for (key, write) in writes.iter() {
            self.insert_stamp(
                key.clone(),
                VersionStamp {
                    commit_epoch,
                    disposition: write.disposition,
                },
            );
        }
    }

    /// Legacy direct mutations do not carry a proven complete write set.
    /// Publish a broad barrier at their already-committed epoch so older
    /// optimistic workspaces cannot overwrite graph or catalog changes.
    pub(crate) fn apply_database_barrier(&mut self, commit_epoch: u64) {
        debug_assert_ne!(commit_epoch, 0, "committed version stamps require an epoch");
        self.insert_stamp(
            VersionKey::Database,
            VersionStamp {
                commit_epoch,
                disposition: VersionDisposition::Live,
            },
        );
    }

    /// Removes tombstones only after every pinned reader is newer than the
    /// deletion that created them. Live stamps are retained indefinitely.
    pub fn prune_tombstones_before(&mut self, oldest_reader_epoch: u64) {
        if self.database_barrier.is_some_and(|stamp| {
            stamp.disposition == VersionDisposition::Tombstone
                && stamp.commit_epoch < oldest_reader_epoch
        }) {
            self.database_barrier = None;
        }
        self.prune_map(|stamp| {
            stamp.disposition != VersionDisposition::Tombstone
                || stamp.commit_epoch >= oldest_reader_epoch
        });
    }

    /// Removes conflict history strictly older than every usable snapshot.
    /// Absent stamps compare as zero; a removed stamp was already <= every
    /// protected read epoch. This never removes the canonical record itself.
    pub(crate) fn prune_before(&mut self, oldest_reader_epoch: u64) {
        if self
            .database_barrier
            .is_some_and(|stamp| stamp.commit_epoch < oldest_reader_epoch)
        {
            self.database_barrier = None;
        }
        self.prune_map(|stamp| stamp.commit_epoch >= oldest_reader_epoch);
    }

    fn prune_map(&mut self, keep: impl Fn(&VersionStamp) -> bool) {
        if self.stamps.iter().all(|(_, stamp)| keep(stamp)) {
            return;
        }
        if self.stamps.iter().all(|(_, stamp)| !keep(stamp)) {
            self.stamps = CowSegmentedMap::default();
            self.history_lease = None;
            self.recount_bytes();
            return;
        }
        if let Some(lease) = &self.history_lease
            && Arc::strong_count(lease) > 1
        {
            // retain can copy all input pages before discarding entries, so
            // reserve the old root's full payload, then refund the removed part.
            let bytes = self.estimated_bytes - version_stamp_bytes(&VersionKey::Database);
            let Some(replacement) = self.history_budget.reserve(bytes) else {
                return; // Retaining extra conflict history is safe under pressure.
            };
            self.history_lease = Some(replacement);
        }
        self.stamps.retain(|_, stamp| keep(stamp));
        self.recount_bytes();
        if let Some(lease) = self.history_lease.as_mut() {
            Arc::get_mut(lease)
                .expect("pruned root owns its history charge")
                .shrink(self.estimated_bytes - version_stamp_bytes(&VersionKey::Database));
        }
    }

    /// Tests sharing of non-Database COW pages, not equality of inline barriers.
    #[doc(hidden)]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        self.stamps.shares_storage_with(&other.stamps)
    }
}

fn version_stamp_bytes(key: &VersionKey) -> usize {
    key.cow_page_bytes()
        .saturating_add(std::mem::size_of::<VersionStamp>())
}

#[cfg(test)]
mod tests {
    use super::{
        VersionDisposition, VersionIndex, VersionKey, VersionWriteSet,
        DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES,
    };
    use crate::NodeId;

    #[test]
    fn retained_history_leases_share_refund_and_bound_partial_pruning() {
        let reserve = super::version_stamp_bytes(&VersionKey::Database);
        let key1 = VersionKey::GraphNode(NodeId(1));
        let key2 = VersionKey::GraphNode(NodeId(2));
        let weight = super::version_stamp_bytes(&key1);
        let mut index = VersionIndex::with_history_limit(reserve + 3 * weight, 3 * weight);
        let budget = index.history_budget.clone();
        let mut first = VersionWriteSet::default();
        first.record_live(key1.clone()).unwrap();
        index = index.stage(&first, 1).unwrap();
        let old = index.clone();
        assert_eq!(budget.used(), weight);
        let mut second = VersionWriteSet::default();
        second.record_live(key2.clone()).unwrap();
        let cancelled = index.stage(&second, 2).unwrap();
        assert_eq!(budget.used(), 3 * weight);
        drop(cancelled);
        assert_eq!(budget.used(), weight);
        index = index.stage(&second, 2).unwrap();
        let current_snapshot = index.clone();
        assert_eq!(budget.used(), 3 * weight);
        assert!(index.stage(&second, 3).is_none());
        index.apply_database_barrier(3);
        assert_eq!(budget.used(), 3 * weight);
        index.prune_before(2);
        assert!(index.stamp(&key1).is_some()); // copying shared pages cannot fit
        drop(old);
        index.prune_before(2);
        assert!(index.stamp(&key1).is_some()); // still needs a full input-root reserve
        drop(current_snapshot);
        index.prune_before(2);
        assert!(index.stamp(&key1).is_none());
        assert!(index.stamp(&key2).is_some());
        assert_eq!(budget.used(), weight);
        let read = index.read_baseline(3);
        assert_eq!(read.retained_history_bytes(), weight);
        let retained = index.clone();
        index.prune_before(4); // empty replacement requires no copying credit
        assert!(index.is_empty());
        assert_eq!(budget.used(), weight);
        drop(retained);
        assert_eq!(budget.used(), 0);
        drop(index);
        assert_eq!(read.retained_history_bytes(), 0);

        // With enough credit, shared partial pruning copies the full input,
        // then shrinks its new lease while the old snapshot remains unchanged.
        let mut index = VersionIndex::with_history_limit(reserve + 3 * weight, 4 * weight);
        index = index.stage(&first, 1).unwrap();
        index = index.stage(&second, 2).unwrap();
        let retained = index.clone();
        index.prune_before(2);
        assert!(index.stamp(&key1).is_none());
        assert!(index.stamp(&key2).is_some());
        assert!(retained.stamp(&key1).is_some());
        assert_eq!(index.retained_history_bytes(), 3 * weight);
        drop(retained);
        assert_eq!(index.retained_history_bytes(), weight);
    }

    #[test]
    fn retained_history_concurrent_reservations_admit_only_available_credit() {
        let key1 = VersionKey::GraphNode(NodeId(1));
        let key2 = VersionKey::GraphNode(NodeId(2));
        let weight = super::version_stamp_bytes(&key1);
        let reserve = super::version_stamp_bytes(&VersionKey::Database);
        let mut writes = VersionWriteSet::default();
        writes.record_live(key1).unwrap();
        let index = VersionIndex::with_history_limit(reserve + 3 * weight, 3 * weight)
            .stage(&writes, 1)
            .unwrap();
        let budget = index.history_budget.clone();
        writes = VersionWriteSet::default();
        writes.record_live(key2).unwrap();
        let barrier = std::sync::Barrier::new(8);
        let results = std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let candidate = index.stage(&writes, 2);
                        barrier.wait();
                        candidate
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);
        assert_eq!(budget.used(), 3 * weight);
        drop(results);
        assert_eq!(budget.used(), weight);
        drop(index);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn inline_database_barrier_preserves_shared_pages_and_snapshot_values() {
        let key = VersionKey::GraphNode(NodeId(1));
        let mut index = VersionIndex::from_live_keys_at_epoch(
            [VersionKey::Database, key.clone(), VersionKey::Database],
            1,
        );
        assert_eq!(index.len(), 2);
        assert_eq!(index.stamps.len(), 1);
        let old = index.clone();
        let bytes = index.estimated_bytes();
        index.apply_database_barrier(2);
        assert!(index.shares_storage_with(&old));
        assert_eq!(index.estimated_bytes(), bytes);
        assert_eq!(index.stamp(&VersionKey::Database).unwrap().commit_epoch, 2);
        assert_eq!(old.stamp(&VersionKey::Database).unwrap().commit_epoch, 1);
        let mut writes = VersionWriteSet::default();
        writes.record_tombstone(VersionKey::Database).unwrap();
        index.apply(&writes, 3);
        assert!(index.shares_storage_with(&old));
        index.prune_tombstones_before(3);
        assert!(index.stamp(&VersionKey::Database).is_some());
        index.prune_tombstones_before(4);
        assert!(index.stamp(&VersionKey::Database).is_none());
        assert!(index.shares_storage_with(&old));
        assert_eq!(index.estimated_bytes(), bytes);
        let read = old.read_baseline(1);
        assert_eq!(read.len(), 1);
        assert!(read.stamps.is_empty());
        writes.record_live(key).unwrap();
        index.apply(&writes, 4);
        assert!(!index.shares_storage_with(&old));
    }

    #[test]
    fn inline_database_barrier_matches_map_oracle_across_operation_sequences() {
        use super::VersionStamp;
        use crate::cow::CowPageWeight;
        let keys = [
            VersionKey::Database,
            VersionKey::Schema,
            VersionKey::GraphNode(NodeId(1)),
        ];
        for sequence in 0usize..8usize.pow(4) {
            let mut index = VersionIndex::default();
            let mut expected = std::collections::BTreeMap::new();
            let mut code = sequence;
            for epoch in 1..=4 {
                let snapshot = index.clone();
                let before = expected.clone();
                let op = code % 8;
                code /= 8;
                if op < 6 {
                    let key = keys[op / 2].clone();
                    let disposition = if op % 2 == 0 {
                        VersionDisposition::Live
                    } else {
                        VersionDisposition::Tombstone
                    };
                    let mut writes = VersionWriteSet::default();
                    writes.record(key.clone(), disposition).unwrap();
                    index.apply(&writes, epoch);
                    expected.insert(
                        key,
                        VersionStamp {
                            commit_epoch: epoch,
                            disposition,
                        },
                    );
                } else if op == 6 {
                    index.prune_tombstones_before(epoch);
                    expected.retain(|_, stamp: &mut VersionStamp| {
                        stamp.disposition != VersionDisposition::Tombstone
                            || stamp.commit_epoch >= epoch
                    });
                } else {
                    index.prune_before(epoch);
                    expected.retain(|_, stamp: &mut VersionStamp| stamp.commit_epoch >= epoch);
                }
                assert_eq!(index.len(), expected.len());
                assert_eq!(index.is_empty(), expected.is_empty());
                for key in &keys {
                    assert_eq!(index.stamp(key), expected.get(key).copied());
                    assert_eq!(snapshot.stamp(key), before.get(key).copied());
                }
                let resident: usize = expected
                    .keys()
                    .filter(|key| **key != VersionKey::Database)
                    .map(|key| key.cow_page_bytes() + std::mem::size_of::<VersionStamp>())
                    .sum();
                assert_eq!(
                    index.estimated_bytes(),
                    super::version_stamp_bytes(&VersionKey::Database) + resident
                );
            }
        }
    }

    #[test]
    fn version_history_budget_tracks_replacement_pruning_and_reserved_barrier() {
        use super::version_stamp_bytes;
        let reserve = version_stamp_bytes(&VersionKey::Database);
        let key = VersionKey::AppendTable("events".into());
        let limit = reserve + version_stamp_bytes(&key);
        let mut index = VersionIndex::with_byte_limit(limit);
        let mut writes = VersionWriteSet::default();
        writes.record_live(key.clone()).unwrap();
        assert!(index.admits(&writes));
        index.apply(&writes, 1);
        assert_eq!(index.estimated_bytes(), limit);
        let snapshot = index.clone();
        writes.record_tombstone(key.clone()).unwrap();
        assert!(index.admits(&writes));
        index.apply(&writes, 2);
        index.apply_database_barrier(3);
        assert_eq!(index.estimated_bytes(), limit);
        let mut growth = VersionWriteSet::default();
        growth.record_live(VersionKey::Schema).unwrap();
        assert!(!index.admits(&growth));
        index.prune_tombstones_before(2);
        assert_eq!(index.estimated_bytes(), limit);
        index.prune_tombstones_before(3);
        assert_eq!(index.estimated_bytes(), reserve);
        assert_eq!(snapshot.estimated_bytes(), limit);
        assert_eq!(
            snapshot.stamp(&key).unwrap().disposition,
            VersionDisposition::Live
        );
        assert!(index.admits(&growth));
        index.apply(&growth, 4);
        index.prune_before(5);
        assert_eq!(index.estimated_bytes(), reserve);
        assert!(index.is_empty());
    }

    #[test]
    fn history_pruning_preserves_boundary_and_new_conflicts_for_all_stamp_kinds() {
        let mut writes = VersionWriteSet::default();
        writes
            .record_live(VersionKey::GraphNode(NodeId(1)))
            .unwrap();
        writes
            .record_tombstone(VersionKey::GraphNode(NodeId(2)))
            .unwrap();
        writes.record_live(VersionKey::Database).unwrap();
        writes.record_live(VersionKey::Schema).unwrap();
        let mut index = VersionIndex::default();
        index.apply(&writes, 5);
        let snapshot = index.clone();
        index.prune_before(5);
        assert!(index.shares_storage_with(&snapshot));
        assert_eq!(index.len(), 4);
        index.prune_before(6);
        assert!(index.is_empty());
        assert_eq!(snapshot.len(), 4);
        assert!(index.first_conflict(&writes, 6).is_none());
        index.apply(&writes, 7);
        assert_eq!(index.first_conflict(&writes, 6).unwrap().committed_epoch, 7);
        index.prune_before(6);
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn non_reclaiming_prune_preserves_shared_pages() {
        let mut writes = VersionWriteSet::default();
        writes
            .record_tombstone(VersionKey::GraphNode(NodeId(1)))
            .unwrap();
        let mut index = VersionIndex::default();
        index.apply(&writes, 5);
        let snapshot = index.clone();
        index.prune_tombstones_before(5);
        assert!(index.shares_storage_with(&snapshot));
        index.prune_tombstones_before(6);
        assert!(!index.shares_storage_with(&snapshot));
        assert_eq!(index.len(), 0);
        assert_eq!(snapshot.len(), 1);
    }

    #[test]
    fn validation_accepts_disjoint_writes_and_rejects_newer_same_key() {
        let left = VersionKey::GraphNode(NodeId(1));
        let right = VersionKey::GraphNode(NodeId(2));
        let mut index = VersionIndex::default();
        let mut left_write = VersionWriteSet::default();
        left_write.record_live(left.clone()).unwrap();
        index.apply(&left_write, 4);

        let mut right_write = VersionWriteSet::default();
        right_write.record_live(right).unwrap();
        assert_eq!(index.first_conflict(&right_write, 3), None);

        let conflict = index.first_conflict(&left_write, 3).unwrap();
        assert_eq!(conflict.key, left);
        assert_eq!(conflict.committed_epoch, 4);
    }

    #[test]
    fn later_write_in_one_workspace_wins_over_earlier_tombstone() {
        let key = VersionKey::GraphNode(NodeId(7));
        let mut writes = VersionWriteSet::new(1);
        writes.record_tombstone(key.clone()).unwrap();
        writes.record_live(key.clone()).unwrap();

        let mut index = VersionIndex::default();
        index.apply(&writes, 2);
        assert_eq!(
            index.stamp(&key).unwrap().disposition,
            VersionDisposition::Live
        );
    }

    #[test]
    fn write_set_limit_applies_only_to_new_identities() {
        let mut writes = VersionWriteSet::new(1);
        let key = VersionKey::GraphNode(NodeId(1));
        writes.record_live(key.clone()).unwrap();
        writes.record_tombstone(key).unwrap();
        assert_eq!(writes.len(), 1);
        assert!(writes
            .record_live(VersionKey::GraphNode(NodeId(2)))
            .is_err());
    }

    #[test]
    fn write_set_byte_admission_preserves_replacements_and_rejects_atomically() {
        use super::{VersionWrite, VersionWriteSetError};
        use crate::{
            cow::CowPageWeight,
            relational::{RelationalKey, RelationalValue},
        };
        let key = VersionKey::RelationalRow {
            table: "records".into(),
            primary_key: RelationalKey(vec![RelationalValue::Text("large-key".repeat(100))]),
        };
        let bytes = key.cow_page_bytes() + std::mem::size_of::<VersionWrite>();
        let mut writes = VersionWriteSet::with_limits(10, bytes);
        writes.record_live(key.clone()).unwrap();
        assert_eq!(writes.estimated_bytes(), bytes);
        writes.record_tombstone(key.clone()).unwrap();
        assert_eq!(writes.estimated_bytes(), bytes);
        assert_eq!(
            writes.iter().next().unwrap().1.disposition,
            VersionDisposition::Tombstone
        );
        let before = writes.clone();
        assert_eq!(
            writes.record_live(VersionKey::GraphNode(NodeId(2))),
            Err(VersionWriteSetError::ByteLimitExceeded { max_bytes: bytes })
        );
        assert_eq!(writes, before);
        let mut too_small = VersionWriteSet::with_limits(10, bytes - 1);
        assert!(matches!(
            too_small.record_live(key),
            Err(VersionWriteSetError::ByteLimitExceeded { .. })
        ));
        assert!(too_small.is_empty());
        assert_eq!(too_small.estimated_bytes(), 0);
    }

    #[test]
    fn write_set_byte_accounting_matches_independent_sequences() {
        use crate::cow::CowPageWeight;
        let keys = [
            VersionKey::Database,
            VersionKey::GraphNode(NodeId(1)),
            VersionKey::AppendTable("events".into()),
        ];
        let weights = keys
            .each_ref()
            .map(|key| key.cow_page_bytes() + std::mem::size_of::<super::VersionWrite>());
        // Every four-operation sequence, both dispositions and several byte
        // boundaries. The oracle stores only key indices and recomputes sums.
        for budget in [0, weights[0], weights[0] + weights[1], weights.iter().sum()] {
            for sequence in 0usize..6usize.pow(4) {
                let mut writes = VersionWriteSet::with_limits(3, budget);
                let mut expected = std::collections::BTreeMap::new();
                let mut code = sequence;
                for _ in 0..4 {
                    let op = code % 6;
                    code /= 6;
                    let id = op / 2;
                    let disposition = if op % 2 == 0 {
                        VersionDisposition::Live
                    } else {
                        VersionDisposition::Tombstone
                    };
                    let old_bytes: usize = expected.keys().map(|id: &usize| weights[*id]).sum();
                    let admitted = expected.contains_key(&id) || old_bytes + weights[id] <= budget;
                    let result = if disposition == VersionDisposition::Live {
                        writes.record_live(keys[id].clone())
                    } else {
                        writes.record_tombstone(keys[id].clone())
                    };
                    assert_eq!(result.is_ok(), admitted);
                    if admitted {
                        expected.insert(id, disposition);
                    }
                    assert_eq!(
                        writes.estimated_bytes(),
                        expected.keys().map(|id| weights[*id]).sum::<usize>()
                    );
                    assert_eq!(writes.len(), expected.len());
                    for (id, disposition) in &expected {
                        assert_eq!(
                            writes
                                .iter()
                                .find(|(key, _)| **key == keys[*id])
                                .unwrap()
                                .1
                                .disposition,
                            *disposition
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn tombstones_wait_until_the_oldest_reader_has_advanced() {
        let key = VersionKey::GraphNode(NodeId(3));
        let mut writes = VersionWriteSet::default();
        writes.record_tombstone(key.clone()).unwrap();
        let mut index = VersionIndex::default();
        index.apply(&writes, 5);

        index.prune_tombstones_before(5);
        assert!(index.stamp(&key).is_some());
        index.prune_tombstones_before(6);
        assert!(index.stamp(&key).is_none());
    }

    #[test]
    fn snapshots_share_version_pages_until_a_write_detaches_them() {
        let mut writes = VersionWriteSet::default();
        writes
            .record_live(VersionKey::GraphNode(NodeId(1)))
            .unwrap();
        let mut index = VersionIndex::default();
        index.apply(&writes, 1);
        let snapshot = index.clone();
        assert!(index.shares_storage_with(&snapshot));

        let mut next = VersionWriteSet::default();
        next.record_live(VersionKey::GraphNode(NodeId(2))).unwrap();
        index.apply(&next, 2);
        assert!(!index.shares_storage_with(&snapshot));
        assert_eq!(snapshot.len(), 1);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn default_limit_tracks_the_wal_batch_admission_limit() {
        const { assert!(DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES > 0) };
    }
}
