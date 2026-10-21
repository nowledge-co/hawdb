use super::*;
use crate::branch_catalog::{BranchName, CreateRequest};
use crate::branch_project::ProjectSelector;
use crate::file_descriptors::ProjectFileDescriptors;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    files: ProjectFileDescriptors,
    child: BranchId,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-abandonment-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            files: ProjectFileDescriptors::acquire(&root, 32).unwrap(),
            child: BranchId::new(Uuid::from_u128(3)).unwrap(),
        }
    }

    fn initialize(&self) -> ProjectMetadata {
        let project = BranchId::new(Uuid::from_u128(1)).unwrap();
        let main = BranchId::new(Uuid::from_u128(2)).unwrap();
        let directory = self.directory();
        fs::create_dir_all(directory.join(self.child.as_uuid().to_string())).unwrap();
        fs::create_dir_all(directory.join("objects/objects/sealed-root")).unwrap();
        let mut catalog = Catalog::bootstrap(project, main).unwrap();
        catalog.branches[0].base_root_digest = Some([7; 32]);
        catalog
            .reserve_create(CreateRequest {
                id: self.child,
                name: BranchName::new("pending").unwrap(),
                parent_id: main,
                source_commit_epoch: 1,
                base_root_digest: [8; 32],
                owner: None,
                request_key: "pending-request".into(),
                request_fingerprint: [9; 32],
            })
            .unwrap();
        branch_catalog::write_catalog(&directory.join("catalog.hawdb"), &catalog).unwrap();
        for (path, bytes) in [
            (
                self.files.root().join("manifest.hawdb"),
                ProjectSelector::new(project, main).unwrap().encode(),
            ),
            (
                directory
                    .join(self.child.as_uuid().to_string())
                    .join("branch.head"),
                b"damaged head".to_vec(),
            ),
            (
                directory
                    .join("objects/objects/sealed-root")
                    .join("8".repeat(64)),
                b"damaged root".to_vec(),
            ),
        ] {
            fs::write(&path, bytes).unwrap();
            sync_file(&path).unwrap();
            durability::sync_directory_ancestors(path.parent().unwrap()).unwrap();
        }
        ProjectMetadata::from_files(self.files.clone()).unwrap()
    }

    fn directory(&self) -> PathBuf {
        self.files.root().join("branches")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.files.root()).unwrap();
    }
}

#[test]
fn reclamation_verifies_archives_without_holding_the_publication_barrier() {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let plan = metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .unwrap();
    metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap();
    let catalog_path = fixture.directory().join("catalog.hawdb");
    let snapshot = prepare_reclamation_with_verifier(&catalog_path, |directory, catalog| {
        let project = directory.to_path_buf();
        std::thread::spawn(move || {
            let _publisher = CatalogMetadataLease::acquire(&project).unwrap();
        })
        .join()
        .unwrap();
        reclamation_evidence(directory, catalog, Default::default())
    })
    .unwrap();
    let directory = fixture.directory();
    assert_eq!(
        std::thread::spawn(move || CatalogMetadataLease::acquire(&directory)
            .unwrap_err()
            .kind())
        .join()
        .unwrap(),
        io::ErrorKind::WouldBlock,
    );
    drop(snapshot);
}

#[test]
fn catalog_changes_during_quarantine_verification_refuse_the_sweep() {
    let fixture = Fixture::new();
    fixture.initialize();
    let catalog_path = fixture.directory().join("catalog.hawdb");
    let result = prepare_reclamation_with_verifier(&catalog_path, |directory, catalog| {
        let proofs = reclamation_evidence(directory, catalog, Default::default())?;
        let mut changed = catalog.clone();
        changed.revision += 1;
        branch_catalog::write_catalog(&catalog_path, &changed).unwrap();
        Ok(proofs)
    });
    assert!(matches!(result, Err(HawDBError::BranchBusy { .. })));
}

#[test]
fn quarantine_budget_refusal_is_a_resource_error_and_preserves_the_archive() {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let plan = metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .unwrap();
    let report = metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap();
    let before = fs::read(report.quarantine_directory.join("intent.json")).unwrap();
    let error = prepare_reclamation(
        &fixture.directory().join("catalog.hawdb"),
        BranchAbandonmentLimits {
            max_bytes: 1,
            ..Default::default()
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        HawDBError::BranchEvidenceBudgetExceeded {
            resource: "bytes",
            limit: 1,
            ..
        }
    ));
    assert_eq!(
        fs::read(report.quarantine_directory.join("intent.json")).unwrap(),
        before
    );
    prepare_reclamation(
        &fixture.directory().join("catalog.hawdb"),
        Default::default(),
    )
    .unwrap();
}

#[test]
fn abandonment_retries_each_publication_boundary_and_freezes_recovery() {
    for destination in ["intent.json", "file-0", "catalog.hawdb", "committed"] {
        let fixture = Fixture::new();
        let metadata = fixture.initialize();
        let plan = metadata
            .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
            .unwrap();
        let acknowledgement = plan.acknowledge_potential_data_loss();
        let failure = durability::fail_durable_replace_for_destination(destination);
        assert!(
            metadata
                .abandon_pending_branch(acknowledgement.clone())
                .is_err(),
            "{destination}"
        );
        drop(failure);
        let catalog =
            branch_catalog::read_catalog(&fixture.directory().join("catalog.hawdb")).unwrap();
        assert!(reclamation_evidence(&fixture.directory(), &catalog, Default::default()).is_err());
        let terminal = destination == "committed";
        assert_eq!(catalog.branches[1].state == BranchState::Deleted, terminal);
        if !terminal {
            let error = branch_catalog::recover_create_from_head_file(
                &fixture.directory().join("catalog.hawdb"),
                fixture.child,
                &fixture
                    .directory()
                    .join(fixture.child.as_uuid().to_string())
                    .join("branch.head"),
                1024,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("operator abandonment intent"),
                "{error}"
            );
            // An unrelated catalog transition requires a fresh CAS approval,
            // while the original evidence snapshot remains reusable.
            let mut advanced = catalog.clone();
            advanced.revision += 1;
            branch_catalog::write_catalog(&fixture.directory().join("catalog.hawdb"), &advanced)
                .unwrap();
            assert!(metadata.abandon_pending_branch(acknowledgement).is_err());
        }
        let refreshed = metadata
            .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
            .unwrap();
        let report = metadata
            .abandon_pending_branch(refreshed.acknowledge_potential_data_loss())
            .unwrap();
        assert_eq!(report.replayed, terminal);
        let catalog =
            branch_catalog::read_catalog(&fixture.directory().join("catalog.hawdb")).unwrap();
        reclamation_evidence(&fixture.directory(), &catalog, Default::default()).unwrap();
        // Metadata keeps its original directory authority, but no child handle.
        assert_eq!(fixture.files.metrics().open, 1);
        assert_eq!(fixture.files.metrics().transient_handles, 1);
        assert_eq!(fixture.files.metrics().reserved, 0);
        drop(metadata);
        assert_eq!(fixture.files.metrics().open, 0);
    }
}

#[cfg(unix)]
#[test]
fn abandonment_rejects_symlinked_sources_and_archive_directories() {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let source = fixture.directory().join("objects/escape");
    std::os::unix::fs::symlink(fixture.files.root().join("manifest.hawdb"), &source).unwrap();
    assert!(metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .is_err());
    std::fs::remove_file(source).unwrap();
    let plan = metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .unwrap();
    std::os::unix::fs::symlink(
        fixture.directory().join("objects"),
        fixture.directory().join("quarantine"),
    )
    .unwrap();
    assert!(metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .is_err());
    assert!(!fixture
        .directory()
        .join("objects")
        .join(fixture.child.as_uuid().to_string())
        .exists());
}

#[cfg(unix)]
#[test]
fn abandonment_power_loss_never_publishes_a_tombstone_without_durable_evidence() {
    use crate::power_loss::image::{CrashPlan, ImageLimits, PersistOperation};
    use crate::power_loss::{IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel};

    let mut schedules = 0;
    let mut torn_schedules = 0;
    for case in 0..9 {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let fixture = Fixture::new();
            let model = PowerLossModel::attach(&fixture.files, ImageLimits::default()).unwrap();
            let metadata = fixture.initialize();
            let plan = metadata
                .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
                .unwrap();
            let archive =
                PathBuf::from("branches/quarantine").join(fixture.child.as_uuid().to_string());
            let (event, path) = match case {
                0 => (IoEvent::Write, archive.join("intent.partial")),
                1 => (IoEvent::FileSync, archive.join("intent.partial")),
                2 => (IoEvent::Rename, archive.join("intent.json")),
                3 => (IoEvent::Write, archive.join("file-0.partial")),
                4 => (IoEvent::FileSync, archive.join("file-0.partial")),
                5 => (IoEvent::Rename, archive.join("file-0")),
                6 => (IoEvent::Rename, PathBuf::from("branches/catalog.hawdb")),
                7 => (IoEvent::Rename, archive.join("committed")),
                8 => (IoEvent::DirectorySync, archive.clone()),
                _ => unreachable!(),
            };
            model
                .observe(ObservationPoint {
                    event,
                    relative_path: path.clone(),
                    boundary,
                    skip_matches: 0,
                    include_descendants: false,
                    keep_last: case == 8,
                })
                .unwrap();
            metadata
                .abandon_pending_branch(plan.acknowledge_potential_data_loss())
                .unwrap();
            let acknowledged = model.capture().unwrap();
            let cut = model
                .take_observation()
                .unwrap()
                .expect("publication cut must execute");
            let complete = cut.persist_all_plan();
            let mut plans = vec![
                CrashPlan::default(),
                complete.clone(),
                CrashPlan {
                    persistence: complete.persistence.iter().rev().cloned().collect(),
                },
            ];
            for operation in &complete.persistence {
                plans.push(CrashPlan {
                    persistence: vec![operation.clone()],
                });
            }
            if matches!(event, IoEvent::Write | IoEvent::FileSync) {
                for write in cut.uncovered_writes(&path).unwrap() {
                    if write.length > 1 {
                        let mut persistence = complete.persistence.clone();
                        persistence.retain(|operation| {
                            *operation != PersistOperation::Whole(write.operation)
                        });
                        persistence.push(PersistOperation::TornWrite {
                            operation: write.operation,
                            bytes: 0..write.length / 2,
                        });
                        plans.push(CrashPlan { persistence });
                        torn_schedules += 1;
                    }
                }
            }
            for (snapshot, crash) in plans
                .into_iter()
                .map(|crash| (&cut, crash))
                .chain([(&acknowledged, CrashPlan::default())])
            {
                let image = fixture
                    .files
                    .root()
                    .with_extension(format!("image-{schedules}"));
                snapshot.crash(&crash).unwrap().materialize(&image).unwrap();
                let recovered = ProjectMetadata::open(&image, 32).unwrap();
                let catalog = recovered.catalog();
                let image_archive = image.join(&archive);
                let terminal = catalog.branches[1].state == BranchState::Deleted;
                if terminal {
                    let manifest = load_intent(&image_archive)
                        .unwrap()
                        .expect("tombstone requires durable intent");
                    manifest.matches(catalog, true).unwrap();
                    verify_archive(&image_archive, &manifest).unwrap();
                }
                // A published commit marker authorizes only a durable terminal
                // catalog. Without it the collector must retain the originals.
                if image_archive.join("committed").exists() {
                    assert!(terminal);
                    reclamation_evidence(&image.join("branches"), catalog, Default::default())
                        .unwrap();
                } else if image_archive.exists() {
                    assert!(reclamation_evidence(
                        &image.join("branches"),
                        catalog,
                        Default::default()
                    )
                    .is_err());
                }
                let refreshed = recovered
                    .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
                    .unwrap();
                recovered
                    .abandon_pending_branch(refreshed.acknowledge_potential_data_loss())
                    .unwrap();
                drop(recovered);
                std::fs::remove_dir_all(image).unwrap();
                schedules += 1;
            }
        }
    }
    assert!(
        torn_schedules >= 4,
        "torn writes must actually be exercised"
    );
    eprintln!("branch-abandonment-power-v1 schedules={schedules} torn={torn_schedules}");
}

#[test]
fn abandonment_preserves_descriptor_errors_and_rejects_completed_creates() {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let plan = metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .unwrap();
    let held: Vec<_> = (0..31)
        .map(|_| File::open(fixture.files.root().join("manifest.hawdb")).unwrap())
        .collect();
    let error = metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap_err();
    assert!(matches!(error, HawDBError::FileDescriptors(_)), "{error:?}");
    drop(held);
    assert!(!fixture.directory().join("quarantine").exists());
    let path = fixture.directory().join("catalog.hawdb");
    let mut catalog = branch_catalog::read_catalog(&path).unwrap();
    catalog
        .complete_create(fixture.child, plan.metadata_revision())
        .unwrap();
    branch_catalog::write_catalog(&path, &catalog).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .is_err());
    assert!(metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!fixture.directory().join("quarantine").exists());
}

#[test]
fn abandonment_revalidates_empty_directories_with_the_inspected_entry_budget() {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let child = fixture
        .directory()
        .join(fixture.child.as_uuid().to_string());
    for index in 0..1100 {
        fs::create_dir(child.join(format!("empty-{index}"))).unwrap();
    }
    let plan = metadata
        .inspect_pending_branch_abandonment(
            fixture.child.as_uuid(),
            BranchAbandonmentLimits {
                max_files: 1200,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(plan.evidence_files(), 3);
    metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap();
    let catalog = branch_catalog::read_catalog(&fixture.directory().join("catalog.hawdb")).unwrap();
    reclamation_evidence(&fixture.directory(), &catalog, Default::default()).unwrap();
}

#[cfg(unix)]
fn reject_dangling_archive_entry(destination: &str) {
    let fixture = Fixture::new();
    let metadata = fixture.initialize();
    let plan = metadata
        .inspect_pending_branch_abandonment(fixture.child.as_uuid(), Default::default())
        .unwrap();
    let acknowledgement = plan.acknowledge_potential_data_loss();
    let failure = durability::fail_durable_replace_for_destination(destination);
    assert!(metadata
        .abandon_pending_branch(acknowledgement.clone())
        .is_err());
    drop(failure);
    let path = archive_directory(&fixture.directory(), fixture.child).join(destination);
    std::os::unix::fs::symlink("absent-evidence", &path).unwrap();
    let catalog_path = fixture.directory().join("catalog.hawdb");
    let before = std::fs::read(&catalog_path).unwrap();
    assert!(metadata
        .abandon_pending_branch(acknowledgement.clone())
        .is_err());
    assert!(std::fs::symlink_metadata(&path)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
    std::fs::remove_file(&path).unwrap();
    metadata.abandon_pending_branch(acknowledgement).unwrap();
}

#[cfg(unix)]
#[test]
fn abandonment_rejects_dangling_evidence_copy() {
    reject_dangling_archive_entry("file-0");
}

#[cfg(unix)]
#[test]
fn abandonment_rejects_dangling_commit_marker() {
    reject_dangling_archive_entry("committed");
}

#[test]
fn abandonment_bounds_encoded_json_including_escape_expansion() {
    let fits = "\n".repeat(MAX_MANIFEST_BYTES as usize / 2 - 1);
    let encoded = bounded_json(&fits).unwrap();
    assert_eq!(encoded.len() as u64, MAX_MANIFEST_BYTES);
    drop(encoded);
    let oversized = "\n".repeat(MAX_MANIFEST_BYTES as usize / 2);
    assert!((oversized.len() as u64) < MAX_MANIFEST_BYTES);
    assert!(bounded_json(&oversized)
        .unwrap_err()
        .to_string()
        .contains("encoding limit"));
}
