//! Typed branch metadata lifecycle for the embedded database facade.
//!
//! Durable head publication and branch opening remain storage-owned. This
//! module provides the stable identity/state surface without exposing catalog
//! paths or filesystem records to callers.

use super::Database;
use crate::error::HawDBError;
use hawdb_core::Uuid;
use hawdb_storage::branch_catalog as storage;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

const BRANCH_DIRECTORY: &str = "branches";
const BRANCH_CATALOG_FILE: &str = "catalog.hawdb";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchSelector {
    Id(Uuid),
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

impl Database {
    fn branch_catalog_path(&self) -> Result<PathBuf, BranchLifecycleError> {
        let root = self
            .store
            .durable_root_path()
            .ok_or(BranchLifecycleError::InMemoryDatabase)?;
        Ok(root.join(BRANCH_DIRECTORY).join(BRANCH_CATALOG_FILE))
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
        let id = match selector {
            BranchSelector::Id(id) => {
                storage::BranchId::new(id).map_err(BranchLifecycleError::Catalog)?
            }
        };
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
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
}
