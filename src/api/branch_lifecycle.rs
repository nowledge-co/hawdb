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
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

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
        BranchSelector::Id(id) => id.to_string(),
        BranchSelector::Name(name) => name.clone(),
    };
    let encoded = format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        request.name.as_deref().unwrap_or(""),
        parent,
        request.expected_source_commit_epoch,
        request.owner.as_deref().unwrap_or(""),
        request.expires_at_unix_seconds.unwrap_or_default(),
        request.idempotency_key,
    );
    *hawdb_integrity::sha256(encoded.as_bytes()).as_bytes()
}

impl Database {
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

    /// Seals the current durable store and publishes the first `main` head.
    /// This is explicit because it changes the WAL selector and requires a
    /// checkpoint closure; ordinary database open never invents a root.
    pub fn initialize_main_branch(
        &mut self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let main = self.initialize_branch_catalog(project_id, main_branch_id)?;
        let head_path = self.branch_head_path(main.id)?;
        if head_path.exists() {
            let head = branch_head::read_branch_head(&head_path)
                .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
            if head.project_id != *project_id.as_bytes()
                || head.branch_id != *main_branch_id.as_bytes()
            {
                return Err(BranchLifecycleError::Storage(
                    "existing main head identity does not match the catalog".to_string(),
                ));
            }
            return Ok(main);
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
        let prepared = self
            .store
            .prepare_immutable_root_handoff(&objects)
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let head = self
            .store
            .initialize_immutable_root_head(
                prepared,
                &head_path,
                *project_id.as_bytes(),
                *main_branch_id.as_bytes(),
            )
            .map_err(|error| BranchLifecycleError::Storage(error.to_string()))?;
        let catalog_path = self.branch_catalog_path()?;
        let mut catalog = self.read_branch_catalog()?;
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == main_branch_id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
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
        let revision = catalog.revision;
        catalog
            .begin_delete(id, revision)
            .map_err(BranchLifecycleError::Transition)?;
        let deleting_revision = catalog.revision;
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

    /// Creates an isolated child branch from a sealed parent head. The
    /// idempotency key is retained by the catalog, so retrying the same
    /// request returns the existing record without creating another head.
    pub fn create_branch(
        &self,
        request: BranchCreateRequest,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let catalog_path = self.branch_catalog_path()?;
        let catalog = self.read_branch_catalog()?;
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
        let fingerprint = request_fingerprint(&request);
        let id_bytes = *hawdb_integrity::sha256(request.idempotency_key.as_bytes()).as_bytes();
        let id = Uuid::from_bytes(id_bytes[..16].try_into().expect("UUID width"));
        let child_id = storage::BranchId::new(id).map_err(BranchLifecycleError::Catalog)?;
        let name = match request.name {
            Some(name) => storage::BranchName::new(name).map_err(BranchLifecycleError::Catalog)?,
            None => storage::BranchName::generated_agent(child_id),
        };
        let child_head_path = self.branch_head_path(id)?;
        let child_wal_path = self.branch_wal_path(id, parent_head.active_wal.generation + 1)?;
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
}
