use super::*;
use crate::branch_catalog::{bind_main_head_file, initialize_catalog_file};
use crate::ownership::DatabaseDirectoryLease;
use hawdb_core::error::FileDescriptorError;
use hawdb_core::Uuid;

struct Fixture {
    files: ProjectFileDescriptors,
}

impl Fixture {
    fn new(limit: usize) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-project-selector-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        Self {
            files: ProjectFileDescriptors::acquire(&root, limit).unwrap(),
        }
    }

    fn root(&self) -> &Path {
        self.files.root()
    }

    fn publish_metadata(&self, selector: ProjectSelector) {
        initialize_catalog_file(
            &catalog_path(self.root()),
            selector.project_id,
            selector.main_branch_id,
        )
        .unwrap();
        bind_main_head_file(
            &catalog_path(self.root()),
            selector.project_id,
            selector.main_branch_id,
            [7; 32],
            42,
        )
        .unwrap();
        // This fixture intentionally has no usable head or closure. Metadata
        // admission must not recover data or claim that the main runtime works.
        std::fs::write(self.root().join(MANIFEST_FILE), selector.encode()).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.root()).unwrap();
    }
}

fn identity(project: u128, main: u128) -> ProjectSelector {
    ProjectSelector::new(
        BranchId::new(Uuid::from_u128(project)).unwrap(),
        BranchId::new(Uuid::from_u128(main)).unwrap(),
    )
    .unwrap()
}

#[test]
fn selector_rejects_torn_noncanonical_and_unknown_encodings() {
    let selector = identity(1, 2);
    let bytes = selector.encode();
    assert_eq!(
        bytes,
        concat!(
            "HAWDB_BRANCH_PROJECT_V1\n",
            "project_id\t00000000-0000-0000-0000-000000000001\n",
            "main_branch_id\t00000000-0000-0000-0000-000000000002\n",
            "catalog_format\t2\n",
            "checksum\t4139119803\n",
        )
        .as_bytes(),
    );
    assert_eq!(ProjectSelector::decode(&bytes).unwrap(), selector);
    for cut in 0..bytes.len() {
        assert!(ProjectSelector::decode(&bytes[..cut]).is_err(), "cut {cut}");
    }
    let text = String::from_utf8(bytes).unwrap();
    for invalid in [
        text.replace(PROJECT_HEADER, "HAWDB_BRANCH_PROJECT_V2"),
        text.replace("catalog_format\t2", "catalog_format\t1"),
        text.replace("main_branch_id\t", "project_id\t"),
        text.replace(
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000000",
        ),
        text.replace(
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000001",
        ),
        format!("{text}project_id\t00000000-0000-0000-0000-000000000001\n"),
        text.replace('\n', "\r\n"),
    ] {
        assert!(ProjectSelector::decode(invalid.as_bytes()).is_err());
    }
    let mut oversized = selector.encode();
    oversized.resize(MAX_SELECTOR_BYTES + 1, b' ');
    assert!(ProjectSelector::decode(&oversized).is_err());
}

#[test]
fn manifest_probe_only_recognizes_the_exact_legacy_header() {
    let fixture = Fixture::new(4);
    assert_eq!(
        inspect_project_manifest(fixture.root()).unwrap(),
        ProjectManifest::Missing,
    );
    let path = fixture.root().join(MANIFEST_FILE);
    let mut legacy = LEGACY_HEADER.to_vec();
    legacy.resize(1024 * 1024, b'x');
    std::fs::write(&path, legacy).unwrap();
    assert_eq!(
        inspect_project_manifest(fixture.root()).unwrap(),
        ProjectManifest::Legacy,
    );
    for invalid in [
        &b"HAWDB_MANIFEST_V1"[..],
        &b"HAWDB_MANIFEST_V2\n"[..],
        &b"HAWDB_BRANCH_PROJECT_V1\n"[..],
        &b"unrecognized manifest\n"[..],
    ] {
        std::fs::write(&path, invalid).unwrap();
        assert!(inspect_project_manifest(fixture.root()).is_err());
    }
    assert_eq!(fixture.files.metrics().open, 0);
}

#[test]
fn metadata_opens_while_main_is_busy_without_reading_its_wal() {
    let fixture = Fixture::new(4);
    let selector = identity(11, 12);
    fixture.publish_metadata(selector);
    let main_directory = fixture
        .root()
        .join(BRANCH_DIRECTORY)
        .join(selector.main_branch_id.as_uuid().to_string());
    std::fs::create_dir(&main_directory).unwrap();
    let wal_path = main_directory.join("wal-1.hawdb");
    std::fs::write(
        &wal_path,
        b"corrupt data; must not be read on metadata open",
    )
    .unwrap();
    let writer = DatabaseDirectoryLease::acquire(&main_directory).unwrap();
    let before = fixture.files.metrics();
    assert_eq!(before.open, 1);
    let first = ProjectMetadata::open(fixture.root(), 4).unwrap();
    let second = ProjectMetadata::open(fixture.root(), 4).unwrap();
    assert_eq!(first.selector(), selector);
    assert_eq!(first.main().source_commit_epoch, 42);
    assert_eq!(second.main().id, selector.main_branch_id);
    assert_eq!(first.file_descriptors().metrics().open, before.open);
    assert_eq!(fixture.files.metrics().mutable_wals, 0);
    assert_eq!(fixture.files.metrics().admitted_runtimes, 0);
    assert!(!main_directory.join("head.hawdb").exists());
    assert_eq!(
        std::fs::read(&wal_path).unwrap(),
        b"corrupt data; must not be read on metadata open",
    );
    drop(writer);
    assert_eq!(fixture.files.metrics().open, 0);
    assert_eq!(first.refresh().unwrap().catalog().revision, 2);
}

#[test]
fn metadata_failure_never_falls_back_to_an_empty_database() {
    let fixture = Fixture::new(4);
    let selector = identity(21, 22);
    fixture.publish_metadata(selector);
    let manifest = std::fs::read(fixture.root().join(MANIFEST_FILE)).unwrap();
    let path = catalog_path(fixture.root());
    let complete_catalog = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(ProjectMetadata::open(fixture.root(), 4).is_err());
    assert!(!path.exists());
    std::fs::write(&path, &complete_catalog[..complete_catalog.len() / 2]).unwrap();
    assert!(ProjectMetadata::open(fixture.root(), 4).is_err());
    assert_eq!(
        std::fs::read(fixture.root().join(MANIFEST_FILE)).unwrap(),
        manifest,
    );
    assert!(!fixture.root().join("wal-1.hawdb").exists());
    assert_eq!(fixture.files.metrics().open, 0);

    let foreign = branch_catalog::Catalog::bootstrap(
        BranchId::new(Uuid::from_u128(31)).unwrap(),
        BranchId::new(Uuid::from_u128(32)).unwrap(),
    )
    .unwrap();
    std::fs::write(&path, foreign.encode().unwrap()).unwrap();
    assert!(ProjectMetadata::open(fixture.root(), 4).is_err());
    let unbound =
        branch_catalog::Catalog::bootstrap(selector.project_id, selector.main_branch_id).unwrap();
    std::fs::write(&path, unbound.encode().unwrap()).unwrap();
    assert!(ProjectMetadata::open(fixture.root(), 4).is_err());
    std::fs::write(&path, complete_catalog).unwrap();
    assert_eq!(
        ProjectMetadata::open(fixture.root(), 4).unwrap().selector(),
        selector
    );
}

#[test]
fn metadata_exhaustion_preserves_files_and_can_be_retried() {
    let fixture = Fixture::new(4);
    let selector = identity(41, 42);
    fixture.publish_metadata(selector);
    let manifest_path = fixture.root().join(MANIFEST_FILE);
    let original = std::fs::read(&manifest_path).unwrap();
    let handles = (0..4)
        .map(|_| File::open(&manifest_path).unwrap())
        .collect::<Vec<_>>();
    assert!(matches!(
        ProjectMetadata::open(fixture.root(), 4).unwrap_err(),
        HawDBError::FileDescriptors(FileDescriptorError::BudgetExceeded { limit: 4, .. }),
    ));
    assert_eq!(std::fs::read(&manifest_path).unwrap(), original);
    assert_eq!(fixture.files.metrics().open, 4);
    drop(handles);
    let metadata = ProjectMetadata::open(fixture.root(), 4).unwrap();
    assert_eq!(metadata.selector(), selector);
    assert_eq!(metadata.file_descriptors().metrics().open, 0);
    assert!(metadata.file_descriptors().metrics().high_water <= 4);
    assert!(matches!(
        ProjectMetadata::open(fixture.root(), 5).unwrap_err(),
        HawDBError::FileDescriptors(FileDescriptorError::ConfigurationConflict { .. }),
    ));
}

#[test]
fn bootstrap_lost_response_reuses_the_original_durable_identity() {
    let fixture = Fixture::new(4);
    let original = identity(51, 52);
    reserve_bootstrap_identity(&fixture.files, original).unwrap();
    let path = fixture.root().join(BOOTSTRAP_FILE);
    let acknowledged_bytes = std::fs::read(&path).unwrap();
    // The host did not observe the first return and proposes newly generated
    // UUIDs on restart. The synchronized reservation remains authoritative.
    assert_eq!(
        reserve_bootstrap_identity(&fixture.files, identity(61, 62)).unwrap(),
        original,
    );
    assert_eq!(std::fs::read(&path).unwrap(), acknowledged_bytes);
    assert!(!fixture.root().join(MANIFEST_FILE).exists());
    assert!(!catalog_path(fixture.root()).exists());
    assert_eq!(fixture.files.metrics().open, 0);
}

#[test]
fn bootstrap_uses_existing_development_catalog_identity_and_checks_conflicts() {
    let fixture = Fixture::new(4);
    let existing = identity(71, 72);
    initialize_catalog_file(
        &catalog_path(fixture.root()),
        existing.project_id,
        existing.main_branch_id,
    )
    .unwrap();
    assert_eq!(
        reserve_bootstrap_identity(&fixture.files, identity(81, 82)).unwrap(),
        existing,
    );
    let conflicting = branch_catalog::Catalog::bootstrap(
        BranchId::new(Uuid::from_u128(91)).unwrap(),
        BranchId::new(Uuid::from_u128(92)).unwrap(),
    )
    .unwrap();
    std::fs::write(catalog_path(fixture.root()), conflicting.encode().unwrap()).unwrap();
    assert!(reserve_bootstrap_identity(&fixture.files, existing).is_err());
    assert_eq!(
        ProjectSelector::decode_with_header(
            &std::fs::read(fixture.root().join(BOOTSTRAP_FILE)).unwrap(),
            BOOTSTRAP_HEADER,
        )
        .unwrap(),
        existing,
    );
}

#[test]
fn damaged_bootstrap_intent_is_retained_instead_of_replaced() {
    let fixture = Fixture::new(4);
    let original = identity(101, 102);
    reserve_bootstrap_identity(&fixture.files, original).unwrap();
    let path = fixture.root().join(BOOTSTRAP_FILE);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.truncate(bytes.len() / 2);
    std::fs::write(&path, &bytes).unwrap();
    assert!(reserve_bootstrap_identity(&fixture.files, identity(111, 112)).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert!(!catalog_path(fixture.root()).exists());
    assert_eq!(fixture.files.metrics().open, 0);
}

#[test]
#[cfg(not(windows))]
fn uncertain_intent_publication_keeps_identity_available_for_reopen() {
    let fixture = Fixture::new(4);
    let selector = identity(121, 122);
    let path = fixture.root().join(BOOTSTRAP_FILE);
    {
        let _failure = durability::fail_sync_directory_for(fixture.root());
        assert!(publish_bootstrap_intent(&path, selector).is_err());
    }
    assert_eq!(
        reserve_bootstrap_identity(&fixture.files, identity(131, 132)).unwrap(),
        selector,
    );
    assert_eq!(fixture.files.metrics().open, 0);
}

#[test]
fn busy_metadata_rejects_bootstrap_without_publishing_an_identity() {
    let fixture = Fixture::new(4);
    let directory = fixture.root().join(BRANCH_DIRECTORY);
    std::fs::create_dir(&directory).unwrap();
    let lease = CatalogMetadataLease::acquire(&directory).unwrap();
    assert!(reserve_bootstrap_identity(&fixture.files, identity(151, 152)).is_err());
    assert!(!fixture.root().join(BOOTSTRAP_FILE).exists());
    assert!(!fixture.root().join(MANIFEST_FILE).exists());
    assert!(!catalog_path(fixture.root()).exists());
    drop(lease);
    assert_eq!(
        reserve_bootstrap_identity(&fixture.files, identity(161, 162)).unwrap(),
        identity(161, 162),
    );
    assert_eq!(fixture.files.metrics().open, 0);
}

#[test]
fn legacy_store_rejects_project_selector_before_wal_or_reclamation() {
    let fixture = Fixture::new(256);
    fixture.publish_metadata(identity(141, 142));
    let wal = fixture.root().join("wal-1.hawdb");
    let old_checkpoint = fixture.root().join("checkpoint-1.hawdb");
    std::fs::write(&wal, b"preserve WAL evidence").unwrap();
    std::fs::write(&old_checkpoint, b"preserve old generation").unwrap();
    let mut schema = hawdb_core::schema::Catalog::default();
    assert!(crate::store::GraphStore::open(fixture.root(), &mut schema).is_err());
    assert_eq!(std::fs::read(&wal).unwrap(), b"preserve WAL evidence");
    assert_eq!(
        std::fs::read(&old_checkpoint).unwrap(),
        b"preserve old generation",
    );
    assert_eq!(fixture.files.metrics().open, 0);
}
