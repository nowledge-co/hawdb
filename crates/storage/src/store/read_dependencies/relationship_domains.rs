// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Complete type scopes, independent of selected endpoint adjacency roots.

use super::*;

const MAX_WORK: usize = 65_536;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const MAX_DEPTH: usize = 32;

struct Collector<'a> {
    store: &'a GraphStore,
    catalog: &'a Catalog,
    created: BTreeMap<RelId, &'a str>,
    last_kind: Option<&'a str>,
    work: usize,
    bytes: usize,
}

impl GraphStore {
    pub(super) fn extend_relationship_read_invalidations(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        if stamps.contains_key(&VersionKey::Database) {
            return Ok(());
        }
        let mut collector = Collector {
            store: self,
            catalog,
            created: BTreeMap::new(),
            last_kind: None,
            work: 0,
            bytes: 0,
        };
        let result = collector.collect(ops, 0, stamps);
        self.poison_on_storage_error(&result);
        if !result? {
            record_stamp(stamps, VersionKey::ReadBarrier)?;
        }
        Ok(())
    }
}

impl<'a> Collector<'a> {
    fn visit(&mut self) -> bool {
        self.work += 1;
        self.work <= MAX_WORK
    }
    fn admit(&mut self, bytes: usize) -> bool {
        self.bytes = self.bytes.saturating_add(bytes);
        self.bytes <= MAX_METADATA_BYTES
    }
    fn stamp(&mut self, kind: &'a str, stamps: &mut VersionWriteSet) -> Result<bool> {
        if self.last_kind == Some(kind) {
            return Ok(true);
        }
        // Preflight before owning the name; no property values are copied.
        if !self.admit(
            2 * std::mem::size_of::<VersionKey>()
                + 2 * std::mem::size_of::<crate::version::VersionWrite>()
                + kind.len(),
        ) {
            return Ok(false);
        }
        record_stamp(stamps, VersionKey::ReadRelationshipDomain(None))?;
        record_stamp(
            stamps,
            VersionKey::ReadRelationshipDomain(Some(kind.into())),
        )?;
        self.last_kind = Some(kind);
        Ok(true)
    }
    fn existing_kind(&self, id: RelId) -> Result<Option<&'a str>> {
        if let Some(kind) = self.created.get(&id) {
            return Ok(Some(*kind));
        }
        if self.store.relationship_tombstones.contains(&id) {
            return Ok(None);
        }
        let kind = if let Some(rel) = self.store.relationships.get(&id) {
            Some(rel.rel_type)
        } else if let Some(reader) = &self.store.canonical_base {
            let result = reader
                .relationship_identity(id)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()));
            self.store.poison_on_storage_error(&result);
            result?.map(|(_, _, kind)| kind)
        } else {
            None
        };
        let Some(kind) = kind else { return Ok(None) };
        self.catalog.rel_type_name(kind).map(Some).ok_or_else(|| {
            HawDBError::StorageIntegrity("relationship summary type is absent from catalog".into())
        })
    }
    fn collect(
        &mut self,
        ops: &'a [WalOp],
        depth: usize,
        stamps: &mut VersionWriteSet,
    ) -> Result<bool> {
        if depth > MAX_DEPTH {
            return Ok(false);
        }
        for op in ops {
            if !self.visit() {
                return Ok(false);
            }
            match op {
                WalOp::CreateRelationship { id, rel_type, .. } => {
                    if !self.admit(
                        std::mem::size_of::<RelId>()
                            + std::mem::size_of::<&str>()
                            + 3 * std::mem::size_of::<usize>(),
                    ) || !self.stamp(rel_type, stamps)?
                    {
                        return Ok(false);
                    }
                    // Borrow from the same immutable WAL input, including
                    // nested creates followed by property changes/deletes.
                    self.created.insert(*id, rel_type);
                }
                WalOp::SetRelationshipProperty { id, .. } | WalOp::DeleteRelationship { id } => {
                    if !self.visit() {
                        return Ok(false);
                    }
                    let Some(kind) = self.existing_kind(*id)? else {
                        return Ok(false);
                    };
                    if !self.stamp(kind, stamps)? {
                        return Ok(false);
                    }
                }
                WalOp::Batch(ops) if !self.collect(ops, depth + 1, stamps)? => {
                    return Ok(false);
                }
                _ => {}
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::{GraphMutation, MutationLimits, PropertyFilter};

    fn update(source: u64, target: u64) -> GraphMutation {
        GraphMutation::SetRelationshipProperty {
            source_label: "Memory".into(),
            filter: Some(PropertyFilter::IdEq {
                value: Value::Int(source as i64),
            }),
            rel_type: "HAS_LABEL".into(),
            target_label: "Label".into(),
            target_filter: Some(PropertyFilter::IdEq {
                value: Value::Int(target as i64),
            }),
            rel_filter: None,
            property: "weight".into(),
            value: Value::Int(1),
        }
    }

    #[test]
    fn type_scope_invalidates_without_becoming_a_shared_writer_intent() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        let a = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let x = store
            .create_node(&mut catalog, "Label", BTreeMap::new())
            .unwrap();
        let y = store
            .create_node(&mut catalog, "Label", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, a, x, "HAS_LABEL", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, b, y, "HAS_LABEL", BTreeMap::new())
            .unwrap();
        let mut read = store.capture_read_dependencies(8, 4096).unwrap();
        read.record(VersionKey::ReadRelationshipDomain(Some("HAS_LABEL".into())))
            .unwrap();
        let mut absent = store.capture_read_dependencies(8, 4096).unwrap();
        absent
            .record(VersionKey::ReadRelationshipDomain(Some("OTHER".into())))
            .unwrap();
        let mut first = store.begin_mutation_transaction(&catalog);
        let mut second = store.begin_mutation_transaction(&catalog);
        first
            .stage_mutation_with_limits(update(a.0, x.0), MutationLimits::default())
            .unwrap();
        second
            .stage_mutation_with_limits(update(b.0, y.0), MutationLimits::default())
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
        assert!(matches!(
            store.validate_read_dependencies(&read),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadRelationshipDomain(_),
                ..
            }))
        ));
        store.validate_read_dependencies(&absent).unwrap();
    }

    #[test]
    fn nested_create_update_delete_uses_borrowed_names_without_property_ownership() {
        let store = GraphStore::default();
        let catalog = Catalog::default();
        let ops = vec![WalOp::Batch(vec![
            WalOp::CreateRelationship {
                id: RelId(4),
                source: NodeId(1),
                target: NodeId(2),
                rel_type: "HAS_LABEL".into(),
                properties: BTreeMap::from([(
                    "large".into(),
                    Value::String("x".repeat(1024 * 1024)),
                )]),
            },
            WalOp::SetRelationshipProperty {
                id: RelId(4),
                property: "weight".into(),
                value: Value::Int(1),
            },
            WalOp::DeleteRelationship { id: RelId(4) },
        ])];
        let mut stamps = VersionWriteSet::default();
        store
            .extend_relationship_read_invalidations(&catalog, &ops, &mut stamps)
            .unwrap();
        assert!(
            stamps.contains_key(&VersionKey::ReadRelationshipDomain(Some(
                "HAS_LABEL".into()
            )))
        );
        assert!(stamps.contains_key(&VersionKey::ReadRelationshipDomain(None)));
        assert!(!stamps.contains_key(&VersionKey::ReadBarrier));
        assert!(stamps.estimated_bytes() < 1024);
    }

    #[test]
    fn unknown_identity_and_unadmitted_name_use_the_shared_barrier() {
        for op in [
            WalOp::DeleteRelationship { id: RelId(99) },
            WalOp::CreateRelationship {
                id: RelId(4),
                source: NodeId(1),
                target: NodeId(2),
                rel_type: "x".repeat(MAX_METADATA_BYTES),
                properties: BTreeMap::new(),
            },
        ] {
            let store = GraphStore::default();
            let mut stamps = VersionWriteSet::default();
            store
                .extend_relationship_read_invalidations(&Catalog::default(), &[op], &mut stamps)
                .unwrap();
            assert!(stamps.contains_key(&VersionKey::ReadBarrier));
            assert_eq!(stamps.len(), 1);
        }
    }

    #[test]
    fn cold_identity_summary_does_not_materialize_relationship_properties() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-rel-scope-cold-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            crate::config::DurabilityPolicy::SyncOnEveryWrite,
            crate::store::WalReplayConfig {
                residency_mode: crate::config::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        let a = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Label", BTreeMap::new())
            .unwrap();
        let id = store
            .create_relationship(
                &mut catalog,
                a,
                b,
                "HAS_LABEL",
                BTreeMap::from([("body".into(), Value::String("x".repeat(1024 * 1024)))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert!(store.relationships.is_empty());
        let mut stamps = VersionWriteSet::default();
        store
            .extend_relationship_read_invalidations(
                &catalog,
                &[WalOp::SetRelationshipProperty {
                    id,
                    property: "weight".into(),
                    value: Value::Int(1),
                }],
                &mut stamps,
            )
            .unwrap();
        assert!(
            stamps.contains_key(&VersionKey::ReadRelationshipDomain(Some(
                "HAS_LABEL".into()
            )))
        );
        assert!(store.relationships.is_empty());
        assert!(stamps.estimated_bytes() < 1024);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn failed_wal_never_installs_a_relationship_domain_stamp() {
        use crate::store::{set_wal_append_failpoint, WalAppendFailure};
        for failure in [
            WalAppendFailure::PartialWrite,
            WalAppendFailure::Rollback,
            WalAppendFailure::Sync,
        ] {
            let path = std::env::temp_dir().join(format!(
                "hawdb-rel-scope-wal-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let a = store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            let b = store
                .create_node(&mut catalog, "Label", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, a, b, "HAS_LABEL", BTreeMap::new())
                .unwrap();
            let mut read = store.capture_read_dependencies(8, 4096).unwrap();
            let key = VersionKey::ReadRelationshipDomain(Some("HAS_LABEL".into()));
            read.record(key.clone()).unwrap();
            let previous = store.version_index.stamp(&key);
            let epoch = store.commit_epoch();
            set_wal_append_failpoint(failure);
            assert!(store
                .commit_mutations(&mut catalog, vec![update(a.0, b.0)])
                .is_err());
            assert_eq!(store.version_index.stamp(&key), previous);
            assert_eq!(store.commit_epoch(), epoch);
            drop(read);
            drop(store);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn covered_row_keys_do_not_undo_a_previous_capture_refusal() {
        let store = GraphStore::default();
        let mut read = store.capture_read_dependencies(1, 4096).unwrap();
        let table = VersionKey::ReadRelationalTableDomain("rows".into());
        read.record(table.clone()).unwrap();
        assert!(read.record(VersionKey::GraphNode(NodeId(0))).is_err());
        assert!(read
            .record(VersionKey::RelationalRow {
                table: "rows".into(),
                primary_key: RelationalKey(vec![])
            })
            .is_err());
        read.record(table).unwrap();
        assert!(matches!(
            store.validate_read_dependencies(&read),
            Err(GraphReadDependencyError::CaptureRefused)
        ));
    }
}
