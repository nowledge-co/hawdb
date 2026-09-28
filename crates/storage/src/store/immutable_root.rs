//! GraphStore integration for immutable-root publication and WAL handoff.

use super::{GraphStore, MANIFEST_FILE};
use crate::checkpoint_closure::build_sealed_root;
use crate::durable_manifest::DurableManifest;
use crate::error::{HawDBError, Result};
use crate::immutable_object::ImmutableObjectStore;
use crate::sealed_wal::{prepare_wal_rotation, PreparedWalRotation};
use crate::{branch_head, sealed_root::SealedRoot};
use std::fs;
use std::path::{Path, PathBuf};

/// A prepared immutable-root handoff. The successor WAL exists and is durable,
/// but the branch selector has not been switched yet.
#[derive(Debug)]
pub struct PreparedImmutableRootHandoff {
    pub root: SealedRoot,
    pub rotation: PreparedWalRotation,
    pub immutable_store_root: PathBuf,
}

impl GraphStore {
    /// Seals the current active WAL, publishes the validated manifest-bound
    /// checkpoint closure and root, and creates the next private WAL. The
    /// caller must keep the branch publication barrier until
    /// [`GraphStore::complete_immutable_root_handoff`] succeeds.
    ///
    /// This method intentionally publishes only artifacts enumerated by the
    /// validated durable manifest. Physical descendants still require their
    /// family readers to contribute closure inputs before #779 is complete.
    pub fn prepare_immutable_root_handoff(
        &mut self,
        immutable_store_root: impl AsRef<Path>,
    ) -> Result<PreparedImmutableRootHandoff> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("immutable root handoff requires durable storage".to_string())
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only storage cannot prepare an immutable root handoff".to_string(),
            ));
        }
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let immutable_store_root = immutable_store_root.as_ref().to_path_buf();
        let mut objects = ImmutableObjectStore::open(&immutable_store_root)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let next_generation = durable
            .wal_generation
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("WAL generation overflow".to_string()))?;
        let next_wal_path = durable
            .root_path
            .join(crate::artifact_files::wal_generation_file(next_generation));
        let rotation = prepare_wal_rotation(
            &durable.wal_path,
            &next_wal_path,
            durable.wal_generation,
            next_generation,
            durable.wal_replay_start_lsn,
            max_active_wal_bytes,
            &mut objects,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;

        let manifest = DurableManifest::load(&durable.root_path.join(MANIFEST_FILE))?;
        let closure = durable
            .checkpoint_closure_plan(manifest)?
            .publish(&mut objects)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let root = build_sealed_root(
            &closure,
            manifest.checkpoint_epoch,
            self.commit_epoch,
            manifest.wal_replay_start_lsn,
            vec![crate::sealed_root::SealedWalReference {
                start_lsn: rotation.sealed.start_lsn,
                end_lsn: rotation.sealed.end_lsn,
                object: rotation.sealed.object,
            }],
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let root_reference = root
            .object_reference()
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let encoded = root
            .encode()
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        objects
            .publish(root_reference, &encoded)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        Ok(PreparedImmutableRootHandoff {
            root,
            rotation,
            immutable_store_root,
        })
    }

    /// Switches the branch head to a previously prepared immutable root and
    /// successor WAL, then updates this GraphStore's durable WAL identity.
    pub fn complete_immutable_root_handoff(
        &mut self,
        prepared: PreparedImmutableRootHandoff,
        head_path: impl AsRef<Path>,
        project_id: [u8; 16],
        branch_id: [u8; 16],
        expected_head_generation: u64,
    ) -> Result<branch_head::BranchHead> {
        self.ensure_usable()?;
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("immutable root handoff requires durable storage".to_string())
        })?;
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let root_reference = prepared
            .root
            .object_reference()
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let mut objects = ImmutableObjectStore::open(&prepared.immutable_store_root)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let head = branch_head::publish_prepared_wal_rotation_with_root(
            head_path.as_ref(),
            branch_head::PreparedWalRootPublicationRequest {
                expected_current_generation: expected_head_generation,
                project_id,
                branch_id,
                logical_commit_epoch: self.commit_epoch,
                prepared: &prepared.rotation,
                max_active_wal_bytes,
            },
            &prepared.root,
            &mut objects,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        if head.sealed_root != root_reference {
            return Err(HawDBError::Storage(
                "published branch head selected an unexpected immutable root".to_string(),
            ));
        }
        durable.wal_append_file = None;
        durable.wal_path = prepared.rotation.next_wal_path;
        durable.wal_generation = prepared.rotation.next_generation;
        durable.wal_replay_start_lsn = prepared.rotation.next_start_lsn;
        durable.wal_bytes = fs::metadata(&durable.wal_path)?.len();
        durable.wal_commit_epoch = self.commit_epoch;
        Ok(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use crate::value::Value;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hawdb-{label}-{suffix}"));
        fs::create_dir_all(&path).expect("create test directory");
        path
    }

    #[test]
    fn graph_store_prepares_root_and_private_successor_wal() {
        let database = temp_dir("immutable-root");
        let objects = database.join("immutable");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&database, &mut catalog).expect("open database");
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(1))]),
            )
            .expect("create node");
        store.checkpoint(&catalog).expect("publish checkpoint");
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(2))]),
            )
            .expect("create post-checkpoint node");

        let prepared = store
            .prepare_immutable_root_handoff(&objects)
            .expect("prepare immutable root");
        prepared.root.validate().expect("valid root");
        assert!(!prepared.root.checkpoint_references.is_empty());
        assert!(prepared.rotation.next_generation > prepared.rotation.sealed.generation);
        assert!(prepared.rotation.next_wal_path.exists());
        assert!(objects.join("objects").exists());

        let _ = fs::remove_dir_all(database);
    }
}
