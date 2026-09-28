//! Typed branch metadata lifecycle for the embedded database facade.
//!
//! Durable head publication and branch opening remain storage-owned. This
//! module provides the stable identity/state surface without exposing catalog
//! paths or filesystem records to callers.

use super::Database;
use crate::error::HawDBError;
use hawdb_core::Uuid;
use hawdb_storage::branch_catalog as storage;
use hawdb_storage::branch_head;
use hawdb_storage::ownership::DatabaseDirectoryLease;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const BRANCH_DIRECTORY: &str = "branches";
const BRANCH_CATALOG_FILE: &str = "catalog.hawdb";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSelector {
    Id(Uuid),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateRequest {
    pub name: Option<String>,
    pub parent: BranchSelector,
    pub expected_source_commit_epoch: u64,
    pub owner: Option<String>,
    pub expires_at_unix_seconds: Option<i64>,
    pub idempotency_key: String,
}

/// A query-capable lease on one ready branch.
///
/// The lease is admitted before the sealed head is replayed. The replay is
/// opened in a private recovery directory, so callers cannot mutate catalog
/// files or storage paths through this facade.
#[derive(Debug)]
pub struct BranchHandle {
    database: Option<Database>,
    branch: BranchInfo,
    _directory_lease: DatabaseDirectoryLease,
    recovery_path: PathBuf,
}

impl BranchHandle {
    pub fn branch(&self) -> &BranchInfo {
        &self.branch
    }

    pub fn query(&mut self, cypher: &str) -> Result<super::QueryOutput, HawDBError> {
        self.database
            .as_mut()
            .expect("branch handle database remains present while open")
            .query(cypher)
    }
}

impl Drop for BranchHandle {
    fn drop(&mut self) {
        drop(self.database.take());
        let _ = fs::remove_dir_all(&self.recovery_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn initializes_and_lists_a_typed_root_branch() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hawdb-branch-api-{suffix}"));
        let database = Database::open(&path).expect("open database");
        let main = database
            .initialize_branch_catalog(Uuid::from_u128(1), Uuid::from_u128(2))
            .expect("initialize branch catalog");
        assert_eq!(main.id, Uuid::from_u128(2));
        assert_eq!(main.state, BranchLifecycleState::Ready);
        assert_eq!(database.list_branches().unwrap(), vec![main.clone()]);
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(main.id))
                .unwrap(),
            main
        );
        assert!(matches!(
            database.delete_branch(BranchSelector::Id(main.id)),
            Err(BranchLifecycleError::RootBranchImmutable)
        ));
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn seals_and_publishes_the_initial_main_head_explicitly() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hawdb-main-head-{suffix}"));
        let mut database = Database::open(&path).expect("open database");
        database.checkpoint().expect("create initial checkpoint");
        database
            .query("CREATE (:Memory {id: 'branch-root'})")
            .expect("create initial graph state");
        let main = database
            .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
            .expect("publish initial main head");
        assert_eq!(main.name, "main");
        assert!(path
            .join("branches")
            .join(main.id.to_string())
            .join("branch.head")
            .is_file());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn opens_a_created_branch_for_queries_and_holds_its_lease() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hawdb-branch-open-api-{suffix}"));
        let mut database = Database::open(&path).expect("open database");
        database.checkpoint().expect("checkpoint graph state");
        database
            .query("CREATE (:Memory {id: 'branch-open'})")
            .expect("create graph state");
        let main = database
            .initialize_main_branch(Uuid::from_u128(21), Uuid::from_u128(22))
            .expect("publish main head");
        let child = database
            .create_branch(BranchCreateRequest {
                name: Some("agent-query".to_string()),
                parent: BranchSelector::Id(main.id),
                expected_source_commit_epoch: main.source_commit_epoch,
                owner: Some("test".to_string()),
                expires_at_unix_seconds: None,
                idempotency_key: "open-query-test".to_string(),
            })
            .expect("create child branch");
        let mut handle = database
            .open_branch(BranchSelector::Name(child.name.clone()))
            .expect("open child branch");
        assert_eq!(handle.branch().id, child.id);
        assert!(matches!(
            database.open_branch(BranchSelector::Id(child.id)),
            Err(BranchLifecycleError::LeaseUnavailable(_))
        ));
        let output = handle
            .query("MATCH (n:Memory) RETURN n.id")
            .expect("query child branch");
        assert_eq!(output.rows.len(), 1);
        drop(handle);
        let expired = database
            .create_branch(BranchCreateRequest {
                name: Some("expired-query".to_string()),
                parent: BranchSelector::Id(main.id),
                expected_source_commit_epoch: main.source_commit_epoch,
                owner: None,
                expires_at_unix_seconds: Some(1),
                idempotency_key: "expired-open-test".to_string(),
            })
            .expect("create expired branch");
        let transitioned = database
            .expire_branch(BranchSelector::Id(expired.id), i64::MAX)
            .expect("expire child branch");
        assert_eq!(transitioned.state, BranchLifecycleState::Expired);
        assert!(matches!(
            database.open_branch(BranchSelector::Id(expired.id)),
            Err(BranchLifecycleError::Expired)
        ));
        let deleted = database
            .delete_branch(BranchSelector::Name(expired.name.clone()))
            .expect("delete expired branch");
        assert_eq!(deleted.state, BranchLifecycleState::Deleted);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn initialized_database() -> (PathBuf, Database, BranchInfo) {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hawdb-branch-retry-{suffix}"));
        let mut database = Database::open(&path).unwrap();
        database.checkpoint().unwrap();
        database.query("CREATE (:Memory {id: 'root'})").unwrap();
        let main = database
            .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
            .unwrap();
        (path, database, main)
    }

    fn create_request(main: &BranchInfo) -> BranchCreateRequest {
        BranchCreateRequest {
            name: Some("child".to_string()),
            parent: BranchSelector::Id(main.id),
            expected_source_commit_epoch: main.source_commit_epoch,
            owner: None,
            expires_at_unix_seconds: None,
            idempotency_key: "create-child".to_string(),
        }
    }

    #[test]
    fn main_initialization_preserves_writes_across_reopen() {
        let (path, mut database, main) = initialized_database();
        let head_path = database.branch_head_path(main.id).unwrap();
        let head = branch_head::read_branch_head(&head_path).unwrap();
        database
            .query("CREATE (:Memory {id: 'after-bootstrap'})")
            .unwrap();
        let before = database
            .query("MATCH (m:Memory) RETURN m.id ORDER BY m.id")
            .unwrap();
        drop(database);
        let mut database = Database::open(&path).unwrap();
        let after = database
            .query("MATCH (m:Memory) RETURN m.id ORDER BY m.id")
            .unwrap();
        assert_eq!(before.rows, after.rows);
        database.checkpoint().unwrap();
        assert_eq!(branch_head::read_branch_head(&head_path).unwrap(), head);
        database.create_branch(create_request(&main)).unwrap();
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn create_rejects_changed_expiry_with_the_same_key() {
        let (path, mut database, main) = initialized_database();
        let mut request = create_request(&main);
        database.create_branch(request.clone()).unwrap();
        request.expires_at_unix_seconds = Some(0);
        assert!(matches!(
            database.create_branch(request),
            Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::Conflict(_)
            ))
        ));
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn main_initialization_repairs_interrupted_catalog_binding() {
        let (path, mut database, main) = initialized_database();
        let catalog_path = database.branch_catalog_path().unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        catalog.branches[0].base_root_digest = None;
        catalog.branches[0].source_commit_epoch = 0;
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        let retried = database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .unwrap();
        assert_eq!(retried.source_commit_epoch, main.source_commit_epoch);
        database.create_branch(create_request(&main)).unwrap();
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn creates_custom_and_generated_branches_and_replays_without_parent_files() {
        let (path, mut database, main) = initialized_database();
        let custom = database.create_branch(create_request(&main)).unwrap();
        assert_eq!(
            database
                .describe_branch(BranchSelector::Name(custom.name.clone()))
                .unwrap(),
            custom
        );
        let mut request = create_request(&custom);
        request.name = None;
        request.idempotency_key = "generated-child".to_string();
        let child = database.create_branch(request.clone()).unwrap();
        assert_eq!(child.name, format!("agent/{}", child.id));
        let deleted = database
            .delete_branch(BranchSelector::Id(custom.id))
            .unwrap();
        assert_eq!(deleted.state, BranchLifecycleState::Deleted);
        std::fs::remove_file(database.branch_head_path(custom.id).unwrap()).unwrap();
        assert_eq!(database.create_branch(request).unwrap(), child);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn create_retry_completes_a_pending_durable_child() {
        let (path, mut database, main) = initialized_database();
        let request = create_request(&main);
        let child = database.create_branch(request.clone()).unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        branch.state = storage::BranchState::Creating;
        branch.create_outcome = storage::CreateOutcome::Pending;
        storage::write_catalog(&database.branch_catalog_path().unwrap(), &catalog).unwrap();
        assert_eq!(database.create_branch(request).unwrap(), child);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn create_retry_rejects_a_pending_child_with_corrupt_wal() {
        let (path, mut database, main) = initialized_database();
        let request = create_request(&main);
        let child = database.create_branch(request.clone()).unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        branch.state = storage::BranchState::Creating;
        branch.create_outcome = storage::CreateOutcome::Pending;
        let catalog_path = database.branch_catalog_path().unwrap();
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        let head =
            branch_head::read_branch_head(&database.branch_head_path(child.id).unwrap()).unwrap();
        std::fs::write(
            database
                .branch_wal_path(child.id, head.active_wal.generation)
                .unwrap(),
            b"corrupt",
        )
        .unwrap();
        assert!(database.create_branch(request).is_err());
        assert_eq!(storage::read_catalog(&catalog_path).unwrap(), catalog);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn readonly_branch_mutations_leave_catalog_unchanged() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let before = std::fs::read(&catalog_path).unwrap();
        drop(database);
        let mut database = Database::open_with_config(
            &path,
            super::super::DatabaseConfig {
                read_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(database.create_branch(create_request(&main)).is_err());
        assert!(database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .is_err());
        assert!(database
            .delete_branch(BranchSelector::Id(child.id))
            .is_err());
        assert_eq!(std::fs::read(catalog_path).unwrap(), before);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn initial_head_retry_reuses_only_an_empty_private_wal() {
        let (path, mut database, main) = initialized_database();
        let head_path = database.branch_head_path(main.id).unwrap();
        let head = branch_head::read_branch_head(&head_path).unwrap();
        let wal_path = database
            .branch_wal_path(main.id, head.active_wal.generation)
            .unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        catalog.branches[0].base_root_digest = None;
        catalog.branches[0].source_commit_epoch = 0;
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        std::fs::remove_file(&head_path).unwrap();
        assert_eq!(
            database
                .initialize_main_branch(Uuid::from_u128(11), main.id)
                .unwrap(),
            main
        );
        assert_eq!(branch_head::read_branch_head(&head_path).unwrap(), head);
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        std::fs::remove_file(&head_path).unwrap();
        std::fs::write(&wal_path, b"corrupt").unwrap();
        assert!(database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .is_err());
        assert!(!head_path.exists());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchLifecycleState {
    Creating,
    Ready,
    Expired,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchInfo {
    pub id: Uuid,
    pub name: String,
    pub parent_id: Option<Uuid>,
    pub source_commit_epoch: u64,
    pub state: BranchLifecycleState,
    pub owner: Option<String>,
    pub expires_at_unix_seconds: Option<i64>,
}

#[derive(Debug)]
pub enum BranchLifecycleError {
    InMemoryDatabase,
    Storage(String),
    CatalogIo(std::io::Error),
    Catalog(storage::CatalogError),
    Transition(storage::CatalogTransitionError),
    UnknownBranch,
    RootBranchImmutable,
    Expired,
    Deleting,
    Deleted,
    LeaseUnavailable(String),
}

impl Display for BranchLifecycleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InMemoryDatabase => {
                formatter.write_str("branch lifecycle requires a durable database")
            }
            Self::Storage(message) => {
                write!(formatter, "branch storage operation failed: {message}")
            }
            Self::CatalogIo(error) => write!(formatter, "branch catalog I/O failed: {error}"),
            Self::Catalog(error) => write!(formatter, "branch catalog is invalid: {error}"),
            Self::Transition(error) => {
                write!(formatter, "branch lifecycle transition failed: {error}")
            }
            Self::UnknownBranch => formatter.write_str("branch does not exist"),
            Self::RootBranchImmutable => formatter.write_str("the root branch cannot be deleted"),
            Self::Expired => formatter.write_str("branch has expired and cannot be opened"),
            Self::Deleting => formatter.write_str("branch is being deleted"),
            Self::Deleted => formatter.write_str("branch has been deleted"),
            Self::LeaseUnavailable(message) => {
                write!(formatter, "branch lease unavailable: {message}")
            }
        }
    }
}

impl std::error::Error for BranchLifecycleError {}

impl From<BranchLifecycleError> for HawDBError {
    fn from(error: BranchLifecycleError) -> Self {
        HawDBError::Storage(error.to_string())
    }
}

fn state(state: storage::BranchState) -> BranchLifecycleState {
    match state {
        storage::BranchState::Creating => BranchLifecycleState::Creating,
        storage::BranchState::Ready => BranchLifecycleState::Ready,
        storage::BranchState::Expired => BranchLifecycleState::Expired,
        storage::BranchState::Deleting => BranchLifecycleState::Deleting,
        storage::BranchState::Deleted => BranchLifecycleState::Deleted,
    }
}

fn info(branch: &storage::BranchRecord) -> BranchInfo {
    BranchInfo {
        id: branch.id.as_uuid(),
        name: branch.name.as_str().to_string(),
        parent_id: branch.parent_id.map(storage::BranchId::as_uuid),
        source_commit_epoch: branch.source_commit_epoch,
        state: state(branch.state),
        owner: branch.owner.clone(),
        expires_at_unix_seconds: branch.expires_at_unix_seconds,
    }
}

fn selector_matches(branch: &storage::BranchRecord, selector: &BranchSelector) -> bool {
    match selector {
        BranchSelector::Id(id) => branch.id.as_uuid() == *id,
        BranchSelector::Name(name) => branch.name.as_str() == name,
    }
}

fn request_fingerprint(request: &BranchCreateRequest) -> [u8; 32] {
    let parent = match &request.parent {
        BranchSelector::Id(id) => ("id", id.to_string()),
        BranchSelector::Name(name) => ("name", name.clone()),
    };
    // Preserve option tags and field boundaries in the durable request identity.
    let encoded = serde_json::to_vec(&(
        "hawdb-branch-create-v1",
        &request.name,
        parent,
        request.expected_source_commit_epoch,
        &request.owner,
        request.expires_at_unix_seconds,
        &request.idempotency_key,
    ))
    .expect("branch request contains only JSON-serializable scalar fields");
    *hawdb_integrity::sha256(&encoded).as_bytes()
}

impl Database {
    fn ensure_branch_writable(&self) -> Result<(), BranchLifecycleError> {
        if self.config.read_only {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState(
                    "read-only storage cannot change branch metadata",
                ),
            ));
        }
        Ok(())
    }

    fn branch_catalog_path(&self) -> Result<PathBuf, BranchLifecycleError> {
        let root = self
            .store
            .durable_root_path()
            .ok_or(BranchLifecycleError::InMemoryDatabase)?;
        Ok(root.join(BRANCH_DIRECTORY).join(BRANCH_CATALOG_FILE))
    }

    fn branch_directory(&self, id: Uuid) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self
            .branch_catalog_path()?
            .parent()
            .expect("branch catalog has a parent")
            .join(id.to_string()))
    }

    fn branch_head_path(&self, id: Uuid) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self.branch_directory(id)?.join("branch.head"))
    }

    fn branch_wal_path(&self, id: Uuid, generation: u64) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self
            .branch_directory(id)?
            .join(format!("wal-{generation}.hawdb")))
    }

    fn read_branch_catalog(&self) -> Result<storage::Catalog, BranchLifecycleError> {
        storage::read_catalog(&self.branch_catalog_path()?).map_err(BranchLifecycleError::CatalogIo)
    }

    /// Initializes the durable branch catalog with an immutable root branch.
    /// Initialization is explicit so opening an existing database never
    /// invents lineage metadata or a synthetic sealed root.
    pub fn initialize_branch_catalog(
        &self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let path = self.branch_catalog_path()?;
        if path.exists() {
            return self
                .read_branch_catalog()?
                .branches
                .into_iter()
                .find(|branch| branch.name.as_str() == "main")
                .map(|branch| info(&branch))
                .ok_or(BranchLifecycleError::UnknownBranch);
        }
        let project = storage::BranchId::new(project_id).map_err(BranchLifecycleError::Catalog)?;
        let main = storage::BranchId::new(main_branch_id).map_err(BranchLifecycleError::Catalog)?;
        let catalog =
            storage::Catalog::bootstrap(project, main).map_err(BranchLifecycleError::Catalog)?;
        let directory = path.parent().expect("branch catalog has a parent");
        std::fs::create_dir_all(directory).map_err(BranchLifecycleError::CatalogIo)?;
        storage::write_catalog(&path, &catalog).map_err(BranchLifecycleError::CatalogIo)?;
        Ok(info(&catalog.branches[0]))
    }

    /// Publishes the current durable state as the initial sealed `main` snapshot.
    /// Requires a checkpoint and a non-empty active WAL. The snapshot has its own
    /// private WAL; ordinary database writes and reopen remain manifest-owned.
    /// Later ordinary writes do not advance this sealed branch source.
    pub fn initialize_main_branch(
        &mut self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let main = self.initialize_branch_catalog(project_id, main_branch_id)?;
        let mut catalog = self.read_branch_catalog()?;
        if main.id != main_branch_id || catalog.project_id.as_uuid() != project_id {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::Conflict(
                    "initial main identity does not match the catalog",
                ),
            ));
        }
        let head_path = self.branch_head_path(main.id)?;
        let head = if head_path
            .try_exists()
            .map_err(BranchLifecycleError::CatalogIo)?
        {
            let head = branch_head::read_branch_head(&head_path)
                .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
            if head.project_id != *project_id.as_bytes()
                || head.branch_id != *main_branch_id.as_bytes()
            {
                return Err(BranchLifecycleError::Storage(
                    "existing main head identity does not match the catalog".to_string(),
                ));
            }
            head
        } else {
            if catalog
                .branches
                .iter()
                .any(|branch| branch.id.as_uuid() == main.id && branch.base_root_digest.is_some())
            {
                return Err(BranchLifecycleError::Storage(
                    "initialized main branch is missing its head".to_string(),
                ));
            }
            let branch_directory = head_path
                .parent()
                .expect("branch head has a parent directory");
            std::fs::create_dir_all(branch_directory).map_err(BranchLifecycleError::CatalogIo)?;
            let objects = self
                .branch_catalog_path()?
                .parent()
                .expect("branch catalog has a parent")
                .join("objects");
            self.store
                .initialize_immutable_root_head(
                    &objects,
                    &head_path,
                    *project_id.as_bytes(),
                    *main_branch_id.as_bytes(),
                )
                .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?
        };
        let catalog_path = self.branch_catalog_path()?;
        let objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
            catalog_path
                .parent()
                .expect("branch catalog has a parent")
                .join("objects"),
        )
        .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let root_bytes = objects
            .read(head.sealed_root)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let root = hawdb_storage::sealed_root::SealedRoot::decode(&root_bytes)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let wal = branch_head::active_wal_identity_from_file(
            &self.branch_wal_path(main.id, head.active_wal.generation)?,
            head.active_wal.generation,
            head.active_wal.replay_start_lsn,
            head.active_wal.byte_length,
        )
        .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        if root.commit_epoch != head.logical_commit_epoch || wal != head.active_wal {
            return Err(BranchLifecycleError::Storage(
                "initial main root/WAL does not match its head".to_string(),
            ));
        }
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == main_branch_id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        if let Some(digest) = branch.base_root_digest {
            if digest != *head.sealed_root.sha256.as_bytes()
                || branch.source_commit_epoch != head.logical_commit_epoch
            {
                return Err(BranchLifecycleError::Storage(
                    "main catalog binding does not match its sealed head".to_string(),
                ));
            }
            return Ok(info(branch));
        }
        branch.base_root_digest = Some(*head.sealed_root.sha256.as_bytes());
        branch.source_commit_epoch = head.logical_commit_epoch;
        branch.metadata_revision = branch.metadata_revision.checked_add(1).ok_or_else(|| {
            BranchLifecycleError::Storage("main metadata revision overflow".to_string())
        })?;
        catalog.revision = catalog.revision.checked_add(1).ok_or_else(|| {
            BranchLifecycleError::Storage("catalog revision overflow".to_string())
        })?;
        catalog.validate().map_err(BranchLifecycleError::Catalog)?;
        storage::write_catalog(&catalog_path, &catalog).map_err(BranchLifecycleError::CatalogIo)?;
        self.describe_branch(BranchSelector::Id(main_branch_id))
    }

    pub fn list_branches(&self) -> Result<Vec<BranchInfo>, BranchLifecycleError> {
        let path = self.branch_catalog_path()?;
        if !path.exists() {
            return Ok(Vec::new());
        }
        Ok(self
            .read_branch_catalog()?
            .branches
            .iter()
            .map(info)
            .collect())
    }

    pub fn describe_branch(
        &self,
        selector: BranchSelector,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let catalog = self.read_branch_catalog()?;
        let branch = match selector {
            BranchSelector::Id(id) => catalog
                .branches
                .iter()
                .find(|branch| branch.id.as_uuid() == id),
            BranchSelector::Name(name) => catalog
                .branches
                .iter()
                .find(|branch| branch.name.as_str() == name),
        }
        .ok_or(BranchLifecycleError::UnknownBranch)?;
        Ok(info(branch))
    }

    /// Marks a non-root branch as deleting and then deleted using the catalog
    /// CAS transitions. Physical head/object cleanup is performed by storage
    /// reclamation after leases and roots have been checked.
    pub fn delete_branch(
        &self,
        selector: BranchSelector,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let path = self.branch_catalog_path()?;
        let mut catalog = self.read_branch_catalog()?;
        let id = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &selector))
            .map(|branch| branch.id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .expect("branch selector resolved above");
        if branch.name.as_str() == "main" {
            return Err(BranchLifecycleError::RootBranchImmutable);
        }
        let revision = branch.metadata_revision;
        catalog
            .begin_delete(id, revision)
            .map_err(BranchLifecycleError::Transition)?;
        let deleting_revision = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .expect("deleting branch remains in catalog")
            .metadata_revision;
        catalog
            .finish_delete(id, deleting_revision)
            .map_err(BranchLifecycleError::Transition)?;
        storage::write_catalog(&path, &catalog).map_err(BranchLifecycleError::CatalogIo)?;
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        Ok(info(branch))
    }

    /// Marks a ready branch expired at the supplied wall-clock time. Expiry
    /// changes only catalog state; any admitted [`BranchHandle`] remains valid
    /// until its lease is released, and storage reclamation stays separate.
    pub fn expire_branch(
        &self,
        selector: BranchSelector,
        now_unix_seconds: i64,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let path = self.branch_catalog_path()?;
        let mut catalog = self.read_branch_catalog()?;
        let id = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &selector))
            .map(|branch| branch.id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        let revision = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .expect("branch remains after selector resolution")
            .metadata_revision;
        catalog
            .expire(id, revision, now_unix_seconds)
            .map_err(BranchLifecycleError::Transition)?;
        storage::write_catalog(&path, &catalog).map_err(BranchLifecycleError::CatalogIo)?;
        self.describe_branch(BranchSelector::Id(id.as_uuid()))
    }

    /// Creates an isolated child branch from a sealed parent head. The
    /// idempotency key is retained by the catalog, so retrying the same
    /// request returns the existing record without creating another head.
    pub fn create_branch(
        &mut self,
        request: BranchCreateRequest,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let catalog_path = self.branch_catalog_path()?;
        let catalog = self.read_branch_catalog()?;
        let fingerprint = request_fingerprint(&request);
        if let Some(existing) = catalog
            .branches
            .iter()
            .find(|branch| branch.create_request_key == request.idempotency_key)
        {
            if existing.request_fingerprint != fingerprint {
                return Err(BranchLifecycleError::Transition(
                    storage::CatalogTransitionError::Conflict(
                        "create request key has a different fingerprint",
                    ),
                ));
            }
            match existing.create_outcome {
                storage::CreateOutcome::Succeeded => return Ok(info(existing)),
                storage::CreateOutcome::Aborted => {
                    return Err(BranchLifecycleError::Transition(
                        storage::CatalogTransitionError::InvalidState(
                            "branch creation was aborted",
                        ),
                    ))
                }
                storage::CreateOutcome::Pending => {}
            }
        }
        let parent = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &request.parent))
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        if parent.state != storage::BranchState::Ready {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState("create parent is not ready"),
            ));
        }
        let parent_id = parent.id;
        let parent_head_path = self.branch_head_path(parent_id.as_uuid())?;
        let parent_head = branch_head::read_branch_head(&parent_head_path)
            .map_err(|error| BranchLifecycleError::CatalogIo(std::io::Error::other(error)))?;
        if parent_head.logical_commit_epoch != request.expected_source_commit_epoch {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::Conflict("create source revision is stale"),
            ));
        }
        let id_bytes = *hawdb_integrity::sha256(request.idempotency_key.as_bytes()).as_bytes();
        let id = Uuid::from_bytes(id_bytes[..16].try_into().expect("UUID width"));
        let child_id = storage::BranchId::new(id).map_err(BranchLifecycleError::Catalog)?;
        let name = match request.name {
            Some(name) => storage::BranchName::new(name).map_err(BranchLifecycleError::Catalog)?,
            None => storage::BranchName::generated_agent(child_id),
        };
        let child_head_path = self.branch_head_path(id)?;
        let generation = parent_head.active_wal.generation.checked_add(1).ok_or(
            BranchLifecycleError::Transition(storage::CatalogTransitionError::Overflow(
                "child WAL generation",
            )),
        )?;
        let child_wal_path = self.branch_wal_path(id, generation)?;
        let create_request = storage::CreateRequest {
            id: child_id,
            name,
            parent_id,
            source_commit_epoch: request.expected_source_commit_epoch,
            base_root_digest: *parent_head.sealed_root.sha256.as_bytes(),
            owner: request.owner,
            expires_at_unix_seconds: request.expires_at_unix_seconds,
            request_key: request.idempotency_key,
            request_fingerprint: fingerprint,
        };
        let mut candidate = catalog.clone();
        candidate
            .reserve_create(create_request.clone())
            .map_err(BranchLifecycleError::Transition)?;
        self.store
            .create_isolated_branch_from_parent_head(
                &catalog_path,
                &parent_head_path,
                &child_head_path,
                &child_wal_path,
                create_request,
            )
            .map_err(|error| BranchLifecycleError::CatalogIo(std::io::Error::other(error)))?;
        self.describe_branch(BranchSelector::Id(id))
    }

    /// Opens a ready branch for queries after admitting its directory lease.
    /// Expired and deleting branches are rejected before any recovery files
    /// are materialized.
    pub fn open_branch(
        &self,
        selector: BranchSelector,
    ) -> Result<BranchHandle, BranchLifecycleError> {
        let catalog = self.read_branch_catalog()?;
        let branch = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &selector))
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        let branch_info = info(branch);
        match branch.state {
            storage::BranchState::Ready => {}
            storage::BranchState::Expired => return Err(BranchLifecycleError::Expired),
            storage::BranchState::Deleting => return Err(BranchLifecycleError::Deleting),
            storage::BranchState::Deleted => return Err(BranchLifecycleError::Deleted),
            storage::BranchState::Creating => {
                return Err(BranchLifecycleError::Transition(
                    storage::CatalogTransitionError::InvalidState("open branch is not ready"),
                ));
            }
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?
            .as_secs() as i64;
        if branch
            .expires_at_unix_seconds
            .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(BranchLifecycleError::Expired);
        }
        let branch_directory = self.branch_directory(branch.id.as_uuid())?;
        let directory_lease = DatabaseDirectoryLease::acquire(&branch_directory)
            .map_err(|error| BranchLifecycleError::LeaseUnavailable(error.to_string()))?;
        let head_path = self.branch_head_path(branch.id.as_uuid())?;
        let objects = self
            .branch_catalog_path()?
            .parent()
            .expect("branch catalog has a parent")
            .join("objects");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?
            .as_nanos();
        let recovery_path =
            std::env::temp_dir().join(format!("hawdb-branch-open-{}-{nonce}", branch.id.as_uuid()));
        let mut graph_catalog = hawdb_storage::schema::Catalog::default();
        hawdb_storage::store::GraphStore::open_from_branch_head(
            &self.store,
            &head_path,
            &objects,
            &recovery_path,
            &mut graph_catalog,
        )
        .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let database = Database::open(&recovery_path)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        Ok(BranchHandle {
            database: Some(database),
            branch: branch_info,
            _directory_lease: directory_lease,
            recovery_path,
        })
    }
}
