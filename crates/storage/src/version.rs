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

use crate::{AdjacencyDirection, CowPageWeight, CowSegmentedMap, NodeId, RelId, RelationalKey};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

pub const DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES: usize = crate::DEFAULT_MAX_WAL_BATCH_OPERATIONS;

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
}

impl Default for VersionWriteSet {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES)
    }
}

impl VersionWriteSet {
    pub fn new(max_entries: usize) -> Self {
        Self {
            writes: BTreeMap::new(),
            max_entries,
        }
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
        if !self.writes.contains_key(&key) && self.writes.len() >= self.max_entries {
            return Err(VersionWriteSetError::LimitExceeded {
                max_entries: self.max_entries,
            });
        }
        self.writes.insert(key, VersionWrite { disposition });
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionWriteSetError {
    LimitExceeded { max_entries: usize },
}

impl Display for VersionWriteSetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded { max_entries } => write!(
                formatter,
                "transaction version write set exceeds its {max_entries}-entry limit"
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
#[derive(Debug, Clone, Default)]
pub struct VersionIndex {
    stamps: CowSegmentedMap<VersionKey, VersionStamp>,
}

impl VersionIndex {
    /// Creates a conservative baseline for all live identities present in a
    /// recovered checkpoint. WAL replay can then overwrite individual stamps.
    pub fn from_live_keys_at_epoch(
        keys: impl IntoIterator<Item = VersionKey>,
        commit_epoch: u64,
    ) -> Self {
        let stamps = keys
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
        Self {
            stamps: stamps.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.stamps.len()
    }

    pub fn stamp(&self, key: &VersionKey) -> Option<VersionStamp> {
        self.stamps.get(key).copied()
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

    /// Publishes every write at `commit_epoch` after its WAL record is durable.
    /// Callers must provide a nonzero epoch derived from the serialized commit
    /// path, before this method is reached.
    pub fn apply(&mut self, writes: &VersionWriteSet, commit_epoch: u64) {
        debug_assert_ne!(commit_epoch, 0, "committed version stamps require an epoch");
        for (key, write) in writes.iter() {
            self.stamps.insert(
                key.clone(),
                VersionStamp {
                    commit_epoch,
                    disposition: write.disposition,
                },
            );
        }
    }

    /// Removes tombstones only after every pinned reader is newer than the
    /// deletion that created them. Live stamps are retained indefinitely.
    pub fn prune_tombstones_before(&mut self, oldest_reader_epoch: u64) {
        self.stamps.retain(|_, stamp| {
            stamp.disposition != VersionDisposition::Tombstone
                || stamp.commit_epoch >= oldest_reader_epoch
        });
    }

    #[doc(hidden)]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        self.stamps.shares_storage_with(&other.stamps)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        VersionDisposition, VersionIndex, VersionKey, VersionWriteSet,
        DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES,
    };
    use crate::NodeId;

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
        assert!(DEFAULT_MAX_VERSION_WRITE_SET_ENTRIES > 0);
    }
}
