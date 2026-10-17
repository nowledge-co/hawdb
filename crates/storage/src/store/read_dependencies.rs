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

//! Pinned read dependencies and read-only invalidation summaries.
//!
//! Kernel inputs must come from complete query collectors. Validation here is
//! not publication authorization: a facade must retain its source/target fence
//! through durable activation and separately check source durability.

#[path = "read_dependencies/adjacency.rs"]
mod adjacency;

use super::*;
use crate::version::{
    VersionConflict, VersionKey, VersionSnapshotPin, VersionWriteSet, VersionWriteSetError,
};
use crate::wal::WalOp;
use std::fmt;

#[path = "read_dependencies/indexed_nodes.rs"]
mod indexed_nodes;
#[path = "read_dependencies/relational_indexes.rs"]
mod relational_indexes;
#[path = "read_dependencies/relationship_domains.rs"]
mod relationship_domains;
use indexed_nodes::{record_stamp, IndexedNodeReadInvalidations};

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceSyncFailure {
    Wal,
    Directory,
}

#[cfg(test)]
thread_local! {
    static SOURCE_SYNC_FAILURE: std::cell::Cell<Option<SourceSyncFailure>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) fn source_sync_failpoint(point: SourceSyncFailure) -> Result<()> {
    if SOURCE_SYNC_FAILURE.get() == Some(point) {
        SOURCE_SYNC_FAILURE.set(None);
        return Err(HawDBError::Storage(
            "injected source durability barrier failure".into(),
        ));
    }
    Ok(())
}

/// Opaque, bounded dependencies captured from the canonical source. Not Clone:
/// one registration owns its source-history pin and summary-tracking lease.
#[derive(Debug)]
pub struct GraphReadDependencies {
    source_epoch: u64,
    keys: VersionWriteSet,
    refused: bool,
    readers: Arc<AtomicUsize>,
    _pin: VersionSnapshotPin,
}

impl GraphReadDependencies {
    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    pub fn estimated_bytes(&self) -> usize {
        self.keys.estimated_bytes()
    }

    pub fn requires_unchanged_database_epoch(&self) -> bool {
        self.keys.contains_key(&VersionKey::Database)
    }

    /// Trusted query collectors may omit physical identities only after proving
    /// that their complete query source is restricted to this label domain.
    #[doc(hidden)]
    pub fn covers_node_domain(&self, label: &str) -> bool {
        !self.refused
            && (self.keys.contains_key(&VersionKey::Database)
                || self
                    .keys
                    .keys_from(&VersionKey::ReadNodeDomain(None))
                    .take_while(|key| matches!(key, VersionKey::ReadNodeDomain(_)))
                    .any(|key| {
                        matches!(key, VersionKey::ReadNodeDomain(name)
                        if name.as_deref().is_none_or(|name| !label.is_empty() && name == label))
                    }))
    }

    /// An empty type requires the all-relationship scope. This only reports
    /// protection already held; it does not register or qualify a query.
    #[doc(hidden)]
    pub fn covers_relationship_domain(&self, kind: &str) -> bool {
        !self.refused
            && (self.keys.contains_key(&VersionKey::Database)
                || self
                    .keys
                    .keys_from(&VersionKey::ReadRelationshipDomain(None))
                    .take_while(|key| matches!(key, VersionKey::ReadRelationshipDomain(_)))
                    .any(|key| {
                        matches!(key, VersionKey::ReadRelationshipDomain(name)
                        if name.as_deref().is_none_or(|name| !kind.is_empty() && name == kind))
                    }))
    }

    pub fn exact_node_property_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::ReadNodeProperty { .. }))
            .count()
    }

    pub fn exact_relational_row_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::RelationalRow { .. }))
            .count()
    }

    pub fn relational_index_prefix_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::ReadRelationalIndexPrefix { .. }))
            .count()
    }

    pub fn node_domain_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::ReadNodeDomain(_)))
            .count()
    }

    pub fn relational_table_domain_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::ReadRelationalTableDomain(_)))
            .count()
    }

    pub fn relationship_domain_count(&self) -> usize {
        self.keys
            .iter()
            .filter(|(key, _)| matches!(key, VersionKey::ReadRelationshipDomain(_)))
            .count()
    }

    /// Used by trusted query collectors, including absence/domain dependencies.
    /// An ignored budget error permanently invalidates this capture.
    pub fn record(&mut self, key: VersionKey) -> std::result::Result<(), VersionWriteSetError> {
        // Whole-epoch equality already implies every precise identity is
        // unchanged. In full-source scans this keeps retained metadata constant
        // rather than adding one redundant key per enumerated row. A refused
        // capture stays refused; it cannot regain validity through this path.
        if !self.refused && self.keys.contains_key(&VersionKey::Database) {
            return Ok(());
        }
        // Eager table/label scopes already protect their row/prefix/property
        // keys. Preserve sticky refusal and the shared barriers; this does not
        // subsume arbitrary physical node or adjacency reads.
        if !self.refused {
            let covering_scope = match &key {
                VersionKey::RelationalRow { table, .. }
                | VersionKey::ReadRelationalIndexPrefix { table, .. } => {
                    Some(VersionKey::ReadRelationalTableDomain(table.clone()))
                }
                VersionKey::ReadNodeProperty { label, .. } => {
                    Some(VersionKey::ReadNodeDomain(Some(label.clone())))
                }
                _ => None,
            };
            if covering_scope.is_some_and(|scope| self.keys.contains_key(&scope)) {
                return Ok(());
            }
        }
        let result = self.keys.record_live(key);
        self.refused |= result.is_err();
        result
    }
}

impl Drop for GraphReadDependencies {
    fn drop(&mut self) {
        let previous = self.readers.fetch_sub(1, AtomicOrdering::AcqRel);
        debug_assert!(previous > 0, "read dependency lease underflow");
    }
}

#[derive(Debug)]
pub enum GraphReadDependencyError {
    SourceChanged,
    CaptureRefused,
    Conflict(VersionConflict),
    Unavailable(HawDBError),
}

impl fmt::Display for GraphReadDependencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceChanged => {
                f.write_str("read dependency source instance changed or is not canonical")
            }
            Self::CaptureRefused => f.write_str("read dependency capture exceeded its budget"),
            Self::Conflict(conflict) => write!(
                f,
                "read dependency {:?} changed at epoch {}",
                conflict.key, conflict.committed_epoch
            ),
            Self::Unavailable(error) => write!(f, "read dependency source unavailable: {error}"),
        }
    }
}

impl std::error::Error for GraphReadDependencyError {}

impl GraphStore {
    /// A completed checkpoint or WAL barrier, not an inference from configured
    /// durability. Replayed WAL starts above the known checkpoint floor.
    #[doc(hidden)]
    pub fn known_source_durable_epoch(&self) -> Option<u64> {
        self.durable
            .as_ref()
            .map(|durable| durable.known_durable_commit_epoch().min(self.commit_epoch))
    }

    /// Explicit publication dependency barrier. It does not change the host's
    /// policy for future writes and must stay under the publication owner.
    #[doc(hidden)]
    pub fn synchronize_source_prefix(&mut self) -> Result<u64> {
        self.ensure_usable()?;
        if self.version_snapshot_pin.is_some() || self.wal_sync_group_active() {
            return Err(HawDBError::Execution(
                "source synchronization requires canonical ownership outside a WAL sync group"
                    .into(),
            ));
        }
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Execution("source synchronization requires persistent storage".into())
        })?;
        if durable.read_only {
            return Err(HawDBError::Execution(
                "source synchronization requires writable storage".into(),
            ));
        }
        match durable.synchronize_source_prefix() {
            Ok(epoch) => Ok(epoch),
            Err(error) => {
                self.post_wal_apply_poisoned
                    .store(true, AtomicOrdering::Release);
                Err(error)
            }
        }
    }

    /// Capture only from a live canonical owner. Register before that owner's
    /// borrow escapes, then capture/query its snapshot under the same owner.
    /// Starting from an older ordinary snapshot is forbidden: summaries were
    /// not necessarily enabled during that snapshot's lifetime.
    #[doc(hidden)]
    pub fn capture_read_dependencies(
        &self,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<GraphReadDependencies> {
        self.ensure_usable()?;
        if self.version_snapshot_pin.is_some() {
            return Err(HawDBError::Execution(
                "read dependencies must be captured from the canonical source".into(),
            ));
        }
        self.read_dependency_readers
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| {
                HawDBError::Execution("read dependency registration capacity exhausted".into())
            })?;
        Ok(GraphReadDependencies {
            source_epoch: self.commit_epoch,
            keys: VersionWriteSet::with_limits(max_entries, max_bytes),
            refused: false,
            readers: Arc::clone(&self.read_dependency_readers),
            _pin: self.version_snapshot_pins.pin(self.commit_epoch),
        })
    }

    /// Kernel-only validation under canonical ownership. The caller must retain
    /// that ownership through publication; this check is not a reusable permit.
    #[doc(hidden)]
    pub fn validate_read_dependencies(
        &self,
        captured: &GraphReadDependencies,
    ) -> std::result::Result<(), GraphReadDependencyError> {
        self.ensure_usable()
            .map_err(GraphReadDependencyError::Unavailable)?;
        if self.version_snapshot_pin.is_some()
            || !Arc::ptr_eq(&self.read_dependency_readers, &captured.readers)
            || self.commit_epoch < captured.source_epoch
        {
            return Err(GraphReadDependencyError::SourceChanged);
        }
        if captured.refused {
            return Err(GraphReadDependencyError::CaptureRefused);
        }
        if captured.keys.contains_key(&VersionKey::Database)
            && self.commit_epoch != captured.source_epoch
        {
            return Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::Database,
                committed_epoch: self.commit_epoch,
            }));
        }
        let has_read_summary = captured.keys.iter().any(|(key, _)| {
            matches!(
                key,
                VersionKey::ReadNodeDomain(_)
                    | VersionKey::ReadRelationshipDomain(_)
                    | VersionKey::ReadRelationalTableDomain(_)
                    | VersionKey::ReadNodeProperty { .. }
                    | VersionKey::ReadRelationalIndexPrefix { .. }
            )
        });
        let barrier_keys = [VersionKey::Database, VersionKey::Schema];
        let keys = captured
            .keys
            .iter()
            .map(|(key, _)| key)
            .chain(barrier_keys.iter())
            .chain(has_read_summary.then_some(&VersionKey::ReadBarrier));
        for key in keys {
            if let Some(stamp) = self.version_index.stamp(key)
                && stamp.commit_epoch > captured.source_epoch
            {
                return Err(GraphReadDependencyError::Conflict(VersionConflict {
                    key: key.clone(),
                    committed_epoch: stamp.commit_epoch,
                }));
            }
        }
        Ok(())
    }

    pub(super) fn tracks_read_dependencies(&self) -> bool {
        self.version_snapshot_pin.is_none()
            && self.read_dependency_readers.load(AtomicOrdering::Acquire) > 0
    }

    pub(super) fn extend_read_invalidation_stamps(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        if self.version_snapshot_pin.is_some()
            || self.read_dependency_readers.load(AtomicOrdering::Acquire) == 0
        {
            return Ok(());
        }
        let mut created = BTreeMap::new();
        let indexes = IndexedNodeReadInvalidations::new(catalog);
        self.collect_node_read_invalidations(&indexes, ops, stamps, &mut created)?;
        self.extend_relationship_read_invalidations(catalog, ops, stamps)
    }

    fn collect_node_read_invalidations(
        &self,
        indexes: &IndexedNodeReadInvalidations<'_>,
        ops: &[WalOp],
        stamps: &mut VersionWriteSet,
        created: &mut BTreeMap<NodeId, String>,
    ) -> Result<()> {
        for op in ops {
            match op {
                WalOp::CreateNode {
                    id,
                    label,
                    properties,
                } => {
                    created.insert(*id, label.clone());
                    record_stamp(stamps, VersionKey::ReadNodeDomain(None))?;
                    indexes.record_label(label, Some(properties), None, stamps)?;
                }
                WalOp::SetNodeProperty { id, .. } | WalOp::DeleteNode { id } => {
                    record_stamp(stamps, VersionKey::ReadNodeDomain(None))?;
                    let changed = match op {
                        WalOp::SetNodeProperty {
                            property, value, ..
                        } => Some((property.as_str(), value)),
                        _ => None,
                    };
                    if let Some(label) = created.get(id) {
                        // Create already stamped its initial keys. Each later
                        // indexed assignment adds its new key, including ABA.
                        indexes.record_label(label, None, changed, stamps)?;
                    } else if let Some(node) = self.nodes.get(id) {
                        indexes.record_node(&node.labels, &node.properties, changed, stamps)?;
                    } else if let Some(node) = indexes.projected_before(self, *id)? {
                        indexes.record_node(&node.labels, &node.properties, changed, stamps)?;
                    } else {
                        record_stamp(stamps, VersionKey::ReadBarrier)?;
                    }
                }
                WalOp::Batch(ops) => {
                    self.collect_node_read_invalidations(indexes, ops, stamps, created)?
                }
                // Other data families are not narrowed by this collector.
                // Their readers must use Database until separately qualified.
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cow::CowPageWeight;
    use crate::mutation::{GraphMutation, MutationLimits, PropertyFilter};
    use crate::version::{VersionIndex, VersionStamp};

    fn create(label: &str) -> GraphMutation {
        GraphMutation::CreateNode {
            label: label.into(),
            properties: BTreeMap::new(),
        }
    }

    fn update(id: u64, value: i64) -> GraphMutation {
        GraphMutation::SetNodeProperty {
            label: "Memory".into(),
            filter: Some(PropertyFilter::IdEq {
                value: Value::Int(id as i64),
            }),
            property: "value".into(),
            value: Value::Int(value),
        }
    }

    fn capture(store: &GraphStore, key: VersionKey) -> GraphReadDependencies {
        let mut captured = store.capture_read_dependencies(16, 4096).unwrap();
        captured.record(key).unwrap();
        captured
    }

    #[test]
    fn empty_domain_detects_insert_without_rejecting_another_label() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        store
            .commit_mutations(&mut catalog, vec![create("Other")])
            .unwrap();
        store.validate_read_dependencies(&captured).unwrap();
        store
            .commit_mutations(&mut catalog, vec![create("Memory")])
            .unwrap();
        assert!(matches!(
            store.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadNodeDomain(_),
                ..
            }))
        ));
        store.reclaim_version_history();
        assert!(store.validate_read_dependencies(&captured).is_err());
    }

    #[test]
    fn absent_point_and_present_point_track_exact_identity() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        let absent = capture(&store, VersionKey::GraphNode(NodeId(0)));
        store
            .commit_mutations(&mut catalog, vec![create("Memory"), create("Memory")])
            .unwrap();
        assert!(store.validate_read_dependencies(&absent).is_err());
        drop(absent);
        let point = capture(&store, VersionKey::GraphNode(NodeId(0)));
        let snapshot = store.snapshot_for_read();
        store
            .commit_mutations(&mut catalog, vec![update(1, 10)])
            .unwrap();
        store.validate_read_dependencies(&point).unwrap();
        assert!(!snapshot
            .nodes
            .get(&NodeId(1))
            .unwrap()
            .properties
            .contains_key("value"));
        store
            .commit_mutations(&mut catalog, vec![update(0, 20)])
            .unwrap();
        store.reclaim_version_history();
        assert!(matches!(
            store.validate_read_dependencies(&point),
            Err(GraphReadDependencyError::Conflict(_))
        ));
    }

    #[test]
    fn read_summaries_do_not_conflict_disjoint_stale_writers() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        store
            .commit_mutations(&mut catalog, vec![create("Memory"), create("Memory")])
            .unwrap();
        let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        let mut first = store.begin_mutation_transaction(&catalog);
        let mut second = store.begin_mutation_transaction(&catalog);
        first
            .stage_mutation_with_limits(update(0, 10), MutationLimits::default())
            .unwrap();
        second
            .stage_mutation_with_limits(update(1, 20), MutationLimits::default())
            .unwrap();
        for transaction in [first, second] {
            store
                .commit_mutation_transaction_and_relational(
                    &mut catalog,
                    transaction,
                    RelationalTransaction::default(),
                    MutationLimits::default(),
                )
                .unwrap();
        }
        assert_eq!(
            store.nodes.get(&NodeId(0)).unwrap().properties["value"],
            Value::Int(10)
        );
        assert_eq!(
            store.nodes.get(&NodeId(1)).unwrap().properties["value"],
            Value::Int(20)
        );
        assert!(store.validate_read_dependencies(&captured).is_err());
    }

    #[test]
    fn scopes_detect_predicate_changes_and_delete_not_only_insertions() {
        for deletion in [false, true] {
            let mut store = GraphStore::default();
            let mut catalog = Catalog::default();
            store
                .commit_mutations(&mut catalog, vec![create("Memory")])
                .unwrap();
            let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
            let mutation = if deletion {
                GraphMutation::DeleteNode {
                    label: "Memory".into(),
                    filter: None,
                    detach: false,
                }
            } else {
                update(0, 99)
            };
            store
                .commit_mutations(&mut catalog, vec![mutation])
                .unwrap();
            assert!(store.validate_read_dependencies(&captured).is_err());
        }
    }

    #[test]
    fn canonical_capture_rejects_snapshot_and_replacement_even_at_equal_epoch() {
        let store = GraphStore::default();
        let captured = capture(&store, VersionKey::GraphNode(NodeId(0)));
        let snapshot = store.snapshot_for_read();
        assert!(snapshot.capture_read_dependencies(16, 4096).is_err());
        assert!(matches!(
            snapshot.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::SourceChanged)
        ));
        let replacement = GraphStore::default();
        assert_eq!(replacement.commit_epoch(), captured.source_epoch());
        assert!(matches!(
            replacement.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::SourceChanged)
        ));
    }

    #[test]
    fn dependency_budget_refusal_is_sticky_and_registration_is_released() {
        let mut store = GraphStore::default();
        let mut captured = store.capture_read_dependencies(0, 0).unwrap();
        assert!(captured.record(VersionKey::GraphNode(NodeId(0))).is_err());
        assert!(matches!(
            store.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::CaptureRefused)
        ));
        assert_eq!(
            store.read_dependency_readers.load(AtomicOrdering::Acquire),
            1
        );
        assert_eq!(store.version_snapshot_pins.oldest_epoch(), Some(0));
        drop(captured);
        assert_eq!(
            store.read_dependency_readers.load(AtomicOrdering::Acquire),
            0
        );
        assert_eq!(store.version_snapshot_pins.oldest_epoch(), None);
        let mut catalog = Catalog::default();
        store
            .commit_mutations(&mut catalog, vec![create("Memory")])
            .unwrap();
        assert!(store
            .version_index
            .stamp(&VersionKey::ReadNodeDomain(None))
            .is_none());
    }

    #[test]
    fn late_capture_after_disabled_tracking_has_no_history_gap() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        store
            .commit_mutations(&mut catalog, vec![create("Memory")])
            .unwrap();
        let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        store.validate_read_dependencies(&captured).unwrap();
        drop(captured);
        store
            .commit_mutations(&mut catalog, vec![update(0, 10)])
            .unwrap();
        let later = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        store.validate_read_dependencies(&later).unwrap();
        store
            .commit_mutations(&mut catalog, vec![update(0, 20)])
            .unwrap();
        assert!(store.validate_read_dependencies(&later).is_err());
    }

    #[test]
    fn database_scope_subsumes_precise_history_without_resetting_refusal() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        let mut broad = store.capture_read_dependencies(1, 4096).unwrap();
        broad.record(VersionKey::Database).unwrap();
        let bytes = broad.estimated_bytes();
        for id in 0..1000 {
            broad.record(VersionKey::GraphNode(NodeId(id))).unwrap();
        }
        assert_eq!(broad.estimated_bytes(), bytes);
        store.validate_read_dependencies(&broad).unwrap();
        store
            .commit_mutations(&mut catalog, vec![create("Other")])
            .unwrap();
        assert!(matches!(
            store.validate_read_dependencies(&broad),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::Database,
                ..
            }))
        ));
        let mut refused = store.capture_read_dependencies(1, 4096).unwrap();
        refused.record(VersionKey::GraphNode(NodeId(0))).unwrap();
        assert!(refused.record(VersionKey::GraphNode(NodeId(1))).is_err());
        assert!(refused.record(VersionKey::Database).is_err());
        assert!(matches!(
            store.validate_read_dependencies(&refused),
            Err(GraphReadDependencyError::CaptureRefused)
        ));
    }

    #[test]
    fn opaque_and_legacy_paths_retain_conservative_invalidation() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        let broad = capture(&store, VersionKey::Database);
        store
            .commit_mutations(&mut catalog, vec![create("Other")])
            .unwrap();
        assert!(store.validate_read_dependencies(&broad).is_err());
        let scoped = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        store
            .create_node(&mut catalog, "Other", BTreeMap::new())
            .unwrap();
        assert!(matches!(
            store.validate_read_dependencies(&scoped),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::Database,
                ..
            }))
        ));
    }

    fn source_fixture(
        policy: crate::config::DurabilityPolicy,
    ) -> (std::path::PathBuf, Catalog, GraphStore) {
        static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb_source_barrier_{}_{}_{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, AtomicOrdering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability(&path, &mut catalog, policy).unwrap();
        assert_eq!(store.known_source_durable_epoch(), Some(0));
        store
            .commit_mutations(&mut catalog, vec![create("Memory")])
            .unwrap();
        (path, catalog, store)
    }

    #[test]
    fn explicit_source_barrier_covers_relaxed_prefix_without_changing_write_policy() {
        use crate::config::DurabilityPolicy;
        let (path, mut catalog, mut store) = source_fixture(DurabilityPolicy::SyncOnCheckpoint);
        assert_eq!(store.known_source_durable_epoch(), Some(0));
        let first = store.commit_epoch();
        assert_eq!(store.synchronize_source_prefix().unwrap(), first);
        store
            .commit_mutations(&mut catalog, vec![update(0, 2)])
            .unwrap();
        assert!(store.commit_epoch() > first);
        assert_eq!(store.known_source_durable_epoch(), Some(first));
        store.checkpoint(&catalog).unwrap();
        assert_eq!(
            store.known_source_durable_epoch(),
            Some(store.commit_epoch())
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn replay_and_future_sync_policy_do_not_certify_recovered_wal() {
        use crate::config::DurabilityPolicy;
        let (path, _, store) = source_fixture(DurabilityPolicy::SyncOnCheckpoint);
        let epoch = store.commit_epoch();
        drop(store);
        let mut catalog = Catalog::default();
        let mut reopened = GraphStore::open_with_durability(
            &path,
            &mut catalog,
            DurabilityPolicy::SyncOnEveryWrite,
        )
        .unwrap();
        assert_eq!(reopened.commit_epoch(), epoch);
        assert_eq!(reopened.known_source_durable_epoch(), Some(0));
        assert_eq!(reopened.synchronize_source_prefix().unwrap(), epoch);
        assert_eq!(reopened.known_source_durable_epoch(), Some(epoch));
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn source_sync_failures_do_not_advance_floor_and_poison_publication() {
        use crate::config::DurabilityPolicy;
        for failure in [SourceSyncFailure::Wal, SourceSyncFailure::Directory] {
            let (path, _, mut store) = source_fixture(DurabilityPolicy::SyncOnCheckpoint);
            let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
            let wal_path = store.durable.as_ref().unwrap().wal_path.clone();
            let wal = std::fs::read(&wal_path).unwrap();
            SOURCE_SYNC_FAILURE.set(Some(failure));
            assert!(store.synchronize_source_prefix().is_err());
            assert_eq!(store.known_source_durable_epoch(), Some(0));
            assert_eq!(std::fs::read(wal_path).unwrap(), wal);
            assert!(matches!(
                store.validate_read_dependencies(&captured),
                Err(GraphReadDependencyError::Unavailable(_))
            ));
            assert!(store.synchronize_source_prefix().is_err());
            drop(captured);
            drop(store);
            let mut catalog = Catalog::default();
            let recovered = GraphStore::open(&path, &mut catalog).unwrap();
            assert_eq!(recovered.node_count_for_label(None), 1);
            assert_eq!(recovered.known_source_durable_epoch(), Some(0));
            drop(recovered);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn first_sync_write_after_reopen_requires_namespace_barrier_for_existing_wal() {
        use crate::config::DurabilityPolicy;
        let (path, _, store) = source_fixture(DurabilityPolicy::SyncOnCheckpoint);
        let before = store.commit_epoch();
        drop(store);
        let mut catalog = Catalog::default();
        let mut reopened = GraphStore::open(&path, &mut catalog).unwrap();
        let captured = capture(&reopened, VersionKey::ReadNodeDomain(Some("Memory".into())));
        SOURCE_SYNC_FAILURE.set(Some(SourceSyncFailure::Directory));
        assert!(reopened
            .commit_mutations(&mut catalog, vec![update(0, 2)])
            .is_err());
        assert_eq!(SOURCE_SYNC_FAILURE.get(), None);
        assert_eq!(reopened.known_source_durable_epoch(), Some(0));
        assert!(matches!(
            reopened.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::Unavailable(_))
        ));
        drop(captured);
        drop(reopened);
        let mut catalog = Catalog::default();
        let mut recovered = GraphStore::open(&path, &mut catalog).unwrap();
        // The failed response retained a complete, uncertain WAL suffix. It is
        // not permission to roll back or discard a complete recovered record.
        assert_eq!(recovered.commit_epoch(), before + 1);
        assert_eq!(recovered.known_source_durable_epoch(), Some(0));
        assert_eq!(recovered.synchronize_source_prefix().unwrap(), before + 1);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn group_sync_advances_known_floor_only_after_shared_flush() {
        use crate::config::DurabilityPolicy;
        let (path, mut catalog, mut store) = source_fixture(DurabilityPolicy::SyncOnEveryWrite);
        let before = store.commit_epoch();
        assert_eq!(store.known_source_durable_epoch(), Some(before));
        assert!(store.begin_wal_sync_group().unwrap());
        store
            .commit_mutations(&mut catalog, vec![update(0, 2)])
            .unwrap();
        assert_eq!(store.known_source_durable_epoch(), Some(before));
        assert!(store.synchronize_source_prefix().is_err());
        store.ensure_usable().unwrap();
        assert!(store.finish_wal_sync_group().unwrap().fsync_performed);
        assert_eq!(
            store.known_source_durable_epoch(),
            Some(store.commit_epoch())
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn missing_wal_is_not_recreated_by_source_synchronization() {
        use crate::config::DurabilityPolicy;
        let (path, _, mut store) = source_fixture(DurabilityPolicy::SyncOnCheckpoint);
        let wal_path = store.durable.as_ref().unwrap().wal_path.clone();
        std::fs::remove_file(&wal_path).unwrap();
        assert!(store.synchronize_source_prefix().is_err());
        assert!(!wal_path.exists());
        assert_eq!(store.known_source_durable_epoch(), Some(0));
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_summary_admission_failure_precedes_wal_and_reopen() {
        let path = std::env::temp_dir().join(format!(
            "hawdb_read_dependency_admission_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Existing", BTreeMap::new())
            .unwrap();
        let reserve = VersionIndex::default().estimated_bytes();
        let weight =
            VersionKey::GraphNode(NodeId(0)).cow_page_bytes() + std::mem::size_of::<VersionStamp>();
        store.version_index = VersionIndex::with_byte_limit(reserve + weight);
        let captured = capture(&store, VersionKey::ReadNodeDomain(Some("Memory".into())));
        let wal_path = store.durable.as_ref().unwrap().wal_path.clone();
        let wal = std::fs::read(&wal_path).unwrap();
        let epoch = store.commit_epoch();
        let error = store
            .commit_mutations(&mut catalog, vec![create("Memory")])
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("version index estimated payload budget"),
            "{error}"
        );
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.node_count_for_label(None), 1);
        assert_eq!(catalog.label_id("Memory"), None);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
        store.validate_read_dependencies(&captured).unwrap();
        drop(store);
        let mut recovered_catalog = Catalog::default();
        let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), epoch);
        assert_eq!(recovered.node_count_for_label(None), 1);
        assert!(matches!(
            recovered.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::SourceChanged)
        ));
        drop(captured);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}
