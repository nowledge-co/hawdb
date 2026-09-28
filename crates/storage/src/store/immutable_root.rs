//! GraphStore integration for immutable-root publication and WAL handoff.

use super::{GraphStore, MANIFEST_FILE};
use crate::checkpoint_closure::build_sealed_root;
use crate::durable_manifest::DurableManifest;
use crate::error::{HawDBError, Result};
use crate::immutable_object::ImmutableObjectStore;
use crate::sealed_wal::{
    prepare_wal_rotation, seal_wal_file, PreparedWalRotation, SealedWalPublication,
};
use crate::{branch_catalog, branch_head, sealed_root::SealedRoot};
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
    /// Creates an isolated child branch from the selected sealed parent head.
    /// The catalog/head primitives own the durable state machine; this method
    /// only derives the child request from the live parent selector and keeps
    /// the parent data files untouched.
    #[doc(hidden)]
    pub fn create_isolated_branch_from_parent_head(
        &self,
        catalog_path: impl AsRef<Path>,
        parent_head_path: impl AsRef<Path>,
        child_head_path: impl AsRef<Path>,
        child_wal_path: impl AsRef<Path>,
        request: branch_catalog::CreateRequest,
    ) -> Result<branch_catalog::BranchCreateResult> {
        let parent = branch_head::read_branch_head(parent_head_path.as_ref())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        if request.base_root_digest != *parent.sealed_root.sha256.as_bytes()
            || request.source_commit_epoch != parent.logical_commit_epoch
        {
            return Err(HawDBError::Storage(
                "child branch request does not match the selected parent head".to_string(),
            ));
        }
        let active_wal_generation = parent
            .active_wal
            .generation
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("child WAL generation overflow".to_string()))?;
        let max_active_wal_bytes = self
            .durable
            .as_ref()
            .and_then(|durable| durable.max_wal_bytes)
            .unwrap_or(u64::MAX);
        let child_request = branch_head::ChildBranchHeadRequest {
            project_id: parent.project_id,
            branch_id: *request.id.as_uuid().as_bytes(),
            sealed_root: parent.sealed_root,
            logical_commit_epoch: parent.logical_commit_epoch,
            active_wal_generation,
            replay_start_lsn: parent.active_wal.replay_start_lsn,
            head_path: child_head_path.as_ref().to_path_buf(),
            wal_path: child_wal_path.as_ref().to_path_buf(),
        };
        let expected_parent = branch_head::ChildBranchSourceExpectation {
            branch_id: parent.branch_id,
            physical_generation: parent.physical_generation,
            logical_commit_epoch: parent.logical_commit_epoch,
            sealed_root: parent.sealed_root,
        };
        branch_catalog::create_branch_from_parent(
            catalog_path.as_ref(),
            parent_head_path.as_ref(),
            child_request,
            expected_parent,
            max_active_wal_bytes,
            request,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))
    }

    /// Reconstructs a read/write GraphStore from immutable root objects for
    /// recovery qualification. The source directory is copied only as a
    /// container for non-checkpoint metadata; every root-bound checkpoint
    /// artifact and sealed WAL is read from the immutable object store.
    #[doc(hidden)]
    pub fn open_from_immutable_root(
        source: &GraphStore,
        root: &SealedRoot,
        immutable_store_root: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        catalog: &mut crate::schema::Catalog,
    ) -> Result<GraphStore> {
        root.validate()
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let source_durable = source.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("immutable root replay requires durable storage".to_string())
        })?;
        let destination = destination.as_ref();
        copy_recovery_container(&source_durable.root_path, destination)?;
        let objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let manifest = root
            .checkpoint_references
            .iter()
            .filter_map(|reference| objects.read(*reference).ok())
            .filter_map(|bytes| {
                std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| DurableManifest::decode(text).ok())
            })
            .next()
            .unwrap_or(DurableManifest::load(
                &source_durable.root_path.join(MANIFEST_FILE),
            )?);
        fs::write(
            destination.join(MANIFEST_FILE),
            manifest.encode().into_bytes(),
        )?;
        let plan = source_durable.checkpoint_closure_plan(manifest)?;
        for input in plan.inputs() {
            if !root.checkpoint_references.contains(&input.reference) {
                return Err(HawDBError::Storage(
                    "immutable root is missing a checkpoint closure artifact".to_string(),
                ));
            }
            let bytes = objects
                .read(input.reference)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            let name = input.path.file_name().ok_or_else(|| {
                HawDBError::Storage("checkpoint closure artifact has no file name".to_string())
            })?;
            fs::write(destination.join(name), bytes)?;
        }
        if root.sealed_wals.len() != 1 {
            return Err(HawDBError::Storage(
                "immutable root replay currently requires one sealed WAL interval".to_string(),
            ));
        }
        let wal = objects
            .read(root.sealed_wals[0].object)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        fs::write(
            destination.join(
                manifest
                    .wal_path(&source_durable.root_path)
                    .file_name()
                    .ok_or_else(|| {
                        HawDBError::Storage("manifest WAL path has no file name".to_string())
                    })?,
            ),
            wal,
        )?;
        GraphStore::open(destination, catalog)
    }

    /// Opens an immutable root selected by a durable branch-head selector.
    /// This is the recovery-facing bridge used while the public branch facade
    /// remains in its separate lifecycle issue.
    #[doc(hidden)]
    pub fn open_from_branch_head(
        source: &GraphStore,
        head_path: impl AsRef<Path>,
        immutable_store_root: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        catalog: &mut crate::schema::Catalog,
    ) -> Result<(GraphStore, branch_head::BranchHead)> {
        let head = branch_head::read_branch_head(head_path.as_ref())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let root_bytes = objects
            .read(head.sealed_root)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let root = SealedRoot::decode(&root_bytes)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let store = Self::open_from_immutable_root(
            source,
            &root,
            immutable_store_root,
            destination,
            catalog,
        )?;
        Ok((store, head))
    }

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

        let root = publish_sealed_root(durable, self.commit_epoch, rotation.sealed, &mut objects)?;
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

    /// Publishes an initial sealed snapshot with a private empty WAL.
    /// The ordinary database remains manifest-owned: changing its writer here
    /// would lose subsequent writes when ordinary open replays the manifest WAL.
    #[doc(hidden)]
    pub fn initialize_immutable_root_head(
        &mut self,
        immutable_store_root: impl AsRef<Path>,
        head_path: impl AsRef<Path>,
        project_id: [u8; 16],
        branch_id: [u8; 16],
    ) -> Result<branch_head::BranchHead> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "immutable root initialization requires durable storage".to_string(),
            )
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only storage cannot initialize a branch".to_string(),
            ));
        }
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let mut objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let sealed = seal_wal_file(
            &durable.wal_path,
            durable.wal_generation,
            durable.wal_replay_start_lsn,
            max_active_wal_bytes,
            &mut objects,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let root = publish_sealed_root(durable, self.commit_epoch, sealed, &mut objects)?;
        let root_reference = root
            .object_reference()
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let generation = durable.wal_generation.checked_add(1).ok_or_else(|| {
            HawDBError::Storage("initial branch WAL generation overflow".to_string())
        })?;
        let head_path = head_path.as_ref();
        let wal_path = head_path.with_file_name(format!("wal-{generation}.hawdb"));
        if wal_path.try_exists()? {
            // A crash may leave the private WAL before publishing the selector.
            // Only an exact empty successor can be reused; never adopt data.
            let header = crate::wal::frame::encode_binary_wal_header(generation, sealed.end_lsn);
            if fs::metadata(&wal_path)?.len() != header.len() as u64
                || fs::read(&wal_path)? != header
            {
                return Err(HawDBError::Storage(
                    "initial branch WAL is not the expected empty successor".to_string(),
                ));
            }
            return branch_head::create_initial_branch_head(
                branch_head::InitialBranchHeadRequest {
                    path: head_path,
                    project_id,
                    branch_id,
                    sealed_root: root_reference,
                    logical_commit_epoch: self.commit_epoch,
                    wal_path: &wal_path,
                    wal_generation: generation,
                    replay_start_lsn: sealed.end_lsn,
                    max_active_wal_bytes,
                },
            )
            .map_err(|error| HawDBError::Storage(error.to_string()));
        }
        branch_head::create_child_branch_head(
            head_path,
            branch_head::ChildBranchHeadRequest {
                project_id,
                branch_id,
                sealed_root: root_reference,
                logical_commit_epoch: self.commit_epoch,
                active_wal_generation: generation,
                replay_start_lsn: sealed.end_lsn,
                head_path: head_path.to_path_buf(),
                wal_path,
            },
            max_active_wal_bytes,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))
    }
}

fn publish_sealed_root(
    durable: &super::durable::DurableStore,
    commit_epoch: u64,
    sealed: SealedWalPublication,
    objects: &mut ImmutableObjectStore,
) -> Result<SealedRoot> {
    let manifest = DurableManifest::load(&durable.root_path.join(MANIFEST_FILE))?;
    let mut closure = durable
        .checkpoint_closure_plan(manifest)?
        .publish(objects)
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    let manifest_bytes = manifest.encode().into_bytes();
    let manifest_reference = crate::immutable_object::ObjectReference::for_bytes(
        crate::immutable_object::ObjectKind::CheckpointArtifact,
        1,
        &manifest_bytes,
    );
    objects
        .publish(manifest_reference, &manifest_bytes)
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    closure.references.push(manifest_reference);
    closure.references.sort_unstable();
    let root = build_sealed_root(
        &closure,
        manifest.checkpoint_epoch,
        commit_epoch,
        manifest.wal_replay_start_lsn,
        vec![crate::sealed_root::SealedWalReference {
            start_lsn: sealed.start_lsn,
            end_lsn: sealed.end_lsn,
            object: sealed.object,
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
    Ok(root)
}

fn copy_recovery_container(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        fs::remove_dir_all(destination)?;
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy().contains(".lock") {
            continue;
        }
        let destination_path = destination.join(name);
        if source_path.is_dir() {
            copy_recovery_container(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
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

        let replay_database = temp_dir("immutable-root-replay");
        let mut replay_catalog = Catalog::default();
        let replayed = GraphStore::open_from_immutable_root(
            &store,
            &prepared.root,
            &objects,
            &replay_database,
            &mut replay_catalog,
        )
        .expect("replay immutable root");
        assert_eq!(replayed.node_count_for_label(None), 2);
        drop(replayed);

        let head_path = database.join("branch.head");
        let seed_wal_path = database.join("seed-wal.99");
        let root_reference = prepared.root.object_reference().expect("root reference");
        crate::branch_head::create_child_branch_head(
            &head_path,
            crate::branch_head::ChildBranchHeadRequest {
                project_id: [1; 16],
                branch_id: [2; 16],
                sealed_root: root_reference,
                logical_commit_epoch: store.commit_epoch(),
                active_wal_generation: 99,
                replay_start_lsn: 1,
                head_path: head_path.clone(),
                wal_path: seed_wal_path,
            },
            u64::MAX,
        )
        .expect("seed branch selector");
        store
            .complete_immutable_root_handoff(prepared, &head_path, [1; 16], [2; 16], 1)
            .expect("complete immutable root handoff");

        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(3))]),
            )
            .expect("write unrelated branch state");
        drop(store);
        let mut reopened_catalog = Catalog::default();
        let mut reopened_source =
            GraphStore::open(&database, &mut reopened_catalog).expect("reopen source database");
        let replay_after_write_database = temp_dir("immutable-root-replay-after-write");
        let mut replay_after_write_catalog = Catalog::default();
        let (replay_after_write, reopened_head) = GraphStore::open_from_branch_head(
            &reopened_source,
            &head_path,
            &objects,
            &replay_after_write_database,
            &mut replay_after_write_catalog,
        )
        .expect("replay immutable root after selector reopen");
        assert_eq!(reopened_head.sealed_root, root_reference);
        assert_eq!(replay_after_write.node_count_for_label(None), 2);

        // A failed selector publication must leave the previously published
        // root authoritative even though the successor WAL and root were
        // prepared durably.
        reopened_source
            .create_node(
                &mut reopened_catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(4))]),
            )
            .expect("create post-reopen node");
        let next_prepared = reopened_source
            .prepare_immutable_root_handoff(&objects)
            .expect("prepare second immutable root");
        let previous_head_bytes = fs::read(&head_path).expect("read published head");
        let _failure = crate::durability::fail_durable_replace_for_destination(
            head_path.file_name().expect("head file name"),
        );
        let error = reopened_source
            .complete_immutable_root_handoff(
                next_prepared,
                &head_path,
                [1; 16],
                [2; 16],
                reopened_head.physical_generation,
            )
            .expect_err("injected selector publication failure");
        assert!(error.to_string().contains("uncertain"));
        assert_eq!(
            fs::read(&head_path).expect("read unchanged published head"),
            previous_head_bytes
        );

        let _ = fs::remove_dir_all(database);
        let _ = fs::remove_dir_all(replay_database);
        let _ = fs::remove_dir_all(replay_after_write_database);
    }

    #[test]
    fn graph_store_creates_isolated_child_from_selected_parent_head() {
        let database = temp_dir("isolated-child");
        let catalog_path = database.join("catalog");
        let parent_head_path = database.join("parent.head");
        let child_directory = database.join("child");
        let child_head_path = child_directory.join("branch.head");
        let child_wal_path = child_directory.join("wal.8");
        fs::create_dir_all(&child_directory).expect("create child directory");

        let project_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(1))
            .expect("project id");
        let parent_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(2))
            .expect("parent id");
        let child_id =
            crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(3)).expect("child id");
        let parent_root = crate::immutable_object::ObjectReference::for_bytes(
            crate::immutable_object::ObjectKind::SealedRoot,
            1,
            b"parent-root",
        );
        let parent_head = crate::branch_head::BranchHead {
            project_id: *project_id.as_uuid().as_bytes(),
            branch_id: *parent_id.as_uuid().as_bytes(),
            physical_generation: 4,
            sealed_root: parent_root,
            logical_commit_epoch: 7,
            active_wal: crate::branch_head::ActiveWalIdentity {
                generation: 7,
                replay_start_lsn: 20,
                byte_length: 1,
                sha256: hawdb_integrity::sha256(b"parent-wal"),
            },
        };
        fs::write(
            &parent_head_path,
            parent_head.encode().expect("encode parent head"),
        )
        .expect("write parent head");
        let catalog = crate::branch_catalog::Catalog {
            project_id,
            revision: 1,
            branches: vec![crate::branch_catalog::BranchRecord {
                id: parent_id,
                name: crate::branch_catalog::BranchName::new("parent").expect("parent name"),
                parent_id: None,
                source_commit_epoch: 7,
                base_root_digest: Some(*parent_root.sha256.as_bytes()),
                metadata_revision: 1,
                state: crate::branch_catalog::BranchState::Ready,
                owner: None,
                expires_at_unix_seconds: None,
                create_request_key: "parent-create".to_string(),
                request_fingerprint: [1; 32],
                create_outcome: crate::branch_catalog::CreateOutcome::Succeeded,
            }],
        };
        crate::branch_catalog::write_catalog(&catalog_path, &catalog).expect("write catalog");

        let request = crate::branch_catalog::CreateRequest {
            id: child_id,
            name: crate::branch_catalog::BranchName::new("child").expect("child name"),
            parent_id,
            source_commit_epoch: 7,
            base_root_digest: *parent_root.sha256.as_bytes(),
            owner: None,
            expires_at_unix_seconds: None,
            request_key: "child-create".to_string(),
            request_fingerprint: [2; 32],
        };
        let parent_bytes = fs::read(&parent_head_path).expect("read parent head");
        let result = GraphStore::default()
            .create_isolated_branch_from_parent_head(
                &catalog_path,
                &parent_head_path,
                &child_head_path,
                &child_wal_path,
                request,
            )
            .expect("create isolated child");

        assert_eq!(result.head.sealed_root, parent_root);
        assert_eq!(result.head.branch_id, *child_id.as_uuid().as_bytes());
        assert_ne!(
            result.head.active_wal.generation,
            parent_head.active_wal.generation
        );
        assert!(child_head_path.is_file());
        assert!(child_wal_path.is_file());
        assert_eq!(
            fs::read(&parent_head_path).expect("read parent head"),
            parent_bytes
        );
        let child_record = crate::branch_catalog::read_catalog(&catalog_path)
            .expect("read catalog")
            .branches
            .into_iter()
            .find(|branch| branch.id == child_id)
            .expect("child catalog record");
        assert_eq!(
            child_record.state,
            crate::branch_catalog::BranchState::Ready
        );
        assert_eq!(child_record.parent_id, Some(parent_id));

        let _ = fs::remove_dir_all(database);
    }
}
