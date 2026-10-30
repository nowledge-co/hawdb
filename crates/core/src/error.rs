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

use crate::RuntimeCapability;
use std::fmt::{Display, Formatter};

mod resource;
pub use resource::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope};
mod transaction;
pub use transaction::{TransactionRetryError, TransactionRetryReason};
mod storage_format;
pub use storage_format::StorageFormatError;

/// Descriptor admission failures are distinct from corrupt storage or a busy
/// branch. Project quota counts belong to one engine resource domain; OS-limit
/// failures can additionally report the process allowance when it is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileDescriptorError {
    InvalidBudget {
        limit: usize,
    },
    ConfigurationConflict {
        configured: usize,
        requested: usize,
    },
    CapacityPolicyConflict {
        configured: bool,
        requested: bool,
    },
    BudgetExceeded {
        requested: usize,
        available: usize,
        limit: usize,
    },
    OsLimit {
        requested: usize,
        os_code: Option<i32>,
        soft: Option<u64>,
        hard: Option<u64>,
    },
}

impl Display for FileDescriptorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBudget { limit } => write!(formatter, "invalid file descriptor budget: {limit}"),
            Self::ConfigurationConflict { configured, requested } => write!(formatter,
                "project file descriptor budget conflict: configured {configured}, requested {requested}"),
            Self::CapacityPolicyConflict { configured, requested } => write!(formatter,
                "project strict_capacity_limit conflict: configured {configured}, requested {requested}"),
            Self::BudgetExceeded { requested, available, limit } => write!(formatter,
                "project file descriptor budget exceeded: requested {requested}, available {available}, limit {limit}"),
            Self::OsLimit { requested, os_code, soft, hard } => write!(formatter,
                "operating system file descriptor limit: requested {requested}, soft {soft:?}, hard {hard:?}, OS code {os_code:?}"),
        }
    }
}

impl std::error::Error for FileDescriptorError {}

/// The result resource whose selected read limit rejected delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadBudgetResource {
    Rows,
    PayloadBytes,
}

/// Retains budget identity independently of the execution diagnostic text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBudgetError {
    pub resource: ReadBudgetResource,
    pub limit: usize,
    pub message: String,
}

impl Display for ReadBudgetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ReadBudgetError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawDBError {
    Parse(String),
    Semantic(String),
    Storage(String),
    FileDescriptors(FileDescriptorError),
    StorageIntegrity(String),
    /// Recovery IO may have completed an effect; reopen to resume its audit.
    WalRecoveryIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
        os_code: Option<i32>,
        message: String,
    },
    UnsupportedStorageFormat(StorageFormatError),
    /// A registered Search target is retained by another native operation.
    ProjectionBusy,
    Execution(String),
    /// Box the diagnostic so successful Result values do not grow with its fields.
    ResourceBudgetExceeded(Box<ResourceBudgetError>),
    ReadBudgetExceeded(ReadBudgetError),
    GraphExpansionCandidateLimitExceeded {
        requested: usize,
        limit: usize,
    },
    GraphExpansionPayloadLimitExceeded {
        requested: usize,
        limit: usize,
    },
    TransactionConflict {
        read_epoch: u64,
        committed_epoch: u64,
        key: String,
    },
    /// A pessimistic conflict requires retrying the complete transaction.
    TransactionRetryRequired(Box<TransactionRetryError>),
    /// Requested read validation cannot be proved; never downgrade isolation.
    TransactionReadDependencyRefused {
        reason: String,
    },
    AppendSequenceExhausted {
        table: String,
        watermark: i64,
        requested: usize,
    },
    CapabilityUnavailable {
        capability: RuntimeCapability,
    },
    BranchCommandUnsupported {
        command: &'static str,
        context: &'static str,
    },
    BranchBusy {
        resource: &'static str,
    },
    BranchEvidenceBudgetExceeded {
        resource: &'static str,
        requested: u64,
        limit: u64,
    },
    /// An unleased pending create prevents a complete reclamation mark.
    /// This identifies retained evidence, not authorization to discard it.
    BranchReclamationBlocked {
        branch_id: crate::Uuid,
        catalog_revision: u64,
        metadata_revision: u64,
        cause: Box<HawDBError>,
    },
}

impl Display for HawDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HawDBError::Parse(message) => write!(f, "parse error: {message}"),
            HawDBError::Semantic(message) => write!(f, "semantic error: {message}"),
            HawDBError::Storage(message) => write!(f, "storage error: {message}"),
            HawDBError::FileDescriptors(error) => Display::fmt(error, f),
            HawDBError::WalRecoveryIo { operation, message, .. } => write!(f, "WAL recovery {operation} failed; retain evidence and retry open: {message}"),
            HawDBError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            HawDBError::UnsupportedStorageFormat(error) => Display::fmt(error, f),
            HawDBError::ProjectionBusy => f.write_str("search projection operation is busy"),
            HawDBError::Execution(message) => write!(f, "execution error: {message}"),
            HawDBError::ResourceBudgetExceeded(error) => write!(f, "execution error: {error}"),
            HawDBError::ReadBudgetExceeded(error) => write!(f, "execution error: {error}"),
            HawDBError::GraphExpansionCandidateLimitExceeded { requested, limit } => write!(
                f,
                "graph expansion candidate_limit exceeded: requested {requested} rows, limit {limit} rows; configure ExecutionMemoryConfig.graph_expansion_budget.candidate_limit"
            ),
            HawDBError::GraphExpansionPayloadLimitExceeded { requested, limit } => write!(
                f,
                "graph expansion payload_byte_limit exceeded: requested {requested} bytes, limit {limit} bytes; configure ExecutionMemoryConfig.graph_expansion_budget.payload_byte_limit"
            ),
            HawDBError::TransactionConflict {
                read_epoch,
                committed_epoch,
                key,
            } => write!(
                f,
                "transaction conflict on {key}: transaction read epoch {read_epoch}, conflicting commit epoch {committed_epoch}"
            ),
            HawDBError::TransactionRetryRequired(error) => Display::fmt(error, f),
            HawDBError::AppendSequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                f,
                "append commit sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            HawDBError::TransactionReadDependencyRefused { reason } => {
                write!(f, "transaction read dependency capture refused: {reason}")
            }
            HawDBError::CapabilityUnavailable { capability } => {
                write!(f, "capability unavailable: {}", capability.as_str())
            }
            HawDBError::BranchCommandUnsupported { command, context } => {
                write!(f, "{command} is unavailable in {context}")
            }
            HawDBError::BranchBusy { resource } => write!(f, "branch is busy: {resource}"),
            HawDBError::BranchEvidenceBudgetExceeded { resource, requested, limit } => write!(
                f,
                "branch quarantine evidence {resource} budget exceeded: requested {requested}, limit {limit}"
            ),
            HawDBError::BranchReclamationBlocked {
                branch_id,
                catalog_revision,
                metadata_revision,
                cause,
            } => write!(
                f,
                "pending branch {branch_id} blocks reclamation at catalog revision {catalog_revision}, branch revision {metadata_revision}: {cause}"
            ),
        }
    }
}

impl std::error::Error for HawDBError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileDescriptors(error) => Some(error),
            Self::ResourceBudgetExceeded(error) => Some(error.as_ref()),
            Self::ReadBudgetExceeded(error) => Some(error),
            Self::TransactionRetryRequired(error) => Some(error.as_ref()),
            Self::UnsupportedStorageFormat(error) => Some(error),
            Self::BranchReclamationBlocked { cause, .. } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl HawDBError {
    pub fn read_budget_exceeded(
        resource: ReadBudgetResource,
        limit: usize,
        message: impl Into<String>,
    ) -> Self {
        Self::ReadBudgetExceeded(ReadBudgetError {
            resource,
            limit,
            message: message.into(),
        })
    }

    /// Preserve typed storage refusals through nested error wrappers.
    pub fn from_storage_error(error: impl std::error::Error + 'static) -> Self {
        if let Some(error) = (&error as &dyn std::error::Error).downcast_ref::<Self>() {
            return error.clone();
        }
        if let Some(resource) = file_descriptor_error(&error) {
            Self::FileDescriptors(resource)
        } else if let Some(resource) = resource::find_resource_budget_error(&error) {
            Self::from(resource)
        } else if let Some(error) = storage_format::find_storage_format_error(&error) {
            Self::UnsupportedStorageFormat(error)
        } else {
            Self::Storage(error.to_string())
        }
    }

    /// `from_storage_error`, prefixing a generic `Storage` message with
    /// `context` and leaving a preserved typed cause (e.g. `FileDescriptors`)
    /// unprefixed and unchanged.
    pub fn from_storage_error_with_context(
        error: impl std::error::Error + 'static,
        context: &str,
    ) -> Self {
        match Self::from_storage_error(error) {
            Self::Storage(message) => Self::Storage(format!("{context}: {message}")),
            error => error,
        }
    }

    /// Retry the complete transaction, never only the failed statement.
    /// Lock timeouts and generic execution/storage failures are not classified here.
    pub const fn is_retryable_transaction_conflict(&self) -> bool {
        matches!(
            self,
            Self::TransactionConflict { .. } | Self::TransactionRetryRequired(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, HawDBError>;

impl From<ResourceBudgetError> for HawDBError {
    fn from(error: ResourceBudgetError) -> Self {
        Self::ResourceBudgetExceeded(Box::new(error))
    }
}

impl From<std::io::Error> for HawDBError {
    fn from(error: std::io::Error) -> Self {
        Self::from_storage_error(error)
    }
}

pub fn file_descriptor_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<FileDescriptorError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(resource) = error.downcast_ref::<FileDescriptorError>() {
            return Some(resource.clone());
        }
        if let Some(HawDBError::FileDescriptors(resource)) = error.downcast_ref::<HawDBError>() {
            return Some(resource.clone());
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            if let Some(code) = error.raw_os_error()
                && descriptor_os_limit(code)
            {
                return Some(FileDescriptorError::OsLimit {
                    requested: 1,
                    os_code: Some(code),
                    soft: None,
                    hard: None,
                });
            }
            // io::Error::source can skip its boxed concrete error. Inspect it
            // explicitly so typed capacity rejections survive each IO layer.
            if let Some(inner) = error.get_ref()
                && let Some(resource) = file_descriptor_error(inner)
            {
                return Some(resource);
            }
        }
        current = error.source();
    }
    None
}

fn descriptor_os_limit(code: i32) -> bool {
    #[cfg(unix)]
    {
        matches!(code, 23 | 24)
    }
    #[cfg(windows)]
    {
        code == 4
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = code;
        false
    }
}
