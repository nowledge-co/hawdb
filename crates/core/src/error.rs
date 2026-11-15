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
pub use storage_format::{
    StorageFormatError, APPEND_WAL_FORMAT, LOGICAL_DUMP_FORMAT,
    RELATIONAL_CHECKPOINT_LAYOUT_FORMAT, RELATIONAL_DURABLE_CODEC_FORMAT,
    RELATIONAL_ROW_PAGE_FORMAT, RELATIONAL_SCHEMA_FORMAT, SEARCH_PROJECTION_MANIFEST_FORMAT,
    STORAGE_MANIFEST_FORMAT,
};
mod relational;
pub use relational::{find_relational_error_kind, RelationalErrorKind, RelationalErrorSurface};

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

impl ReadBudgetResource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rows => "result_rows",
            Self::PayloadBytes => "result_payload_bytes",
        }
    }

    /// The `DatabaseConfig` read ceiling that bounds this resource. A
    /// per-request limit or an admitted runtime result budget can be smaller;
    /// the refusal's `limit` is the effective one.
    pub const fn budget_option(self) -> &'static str {
        match self {
            Self::Rows => "max_read_result_rows",
            Self::PayloadBytes => "max_read_result_payload_bytes",
        }
    }
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

/// Why a search projection rebuild was refused before it installed anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionRebuildRefusal {
    /// The target name already exists; the rebuild writes only to a new directory.
    TargetExists,
    /// The target's parent directory does not exist.
    TargetParentMissing,
    /// The target path has no final component.
    TargetInvalid,
    /// Another entry appeared at the target name before the no-replace install.
    TargetAppeared,
    /// The built generation differs from the pinned graph scan.
    VerificationMismatch,
}

impl SearchProjectionRebuildRefusal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TargetExists => "target_exists",
            Self::TargetParentMissing => "target_parent_missing",
            Self::TargetInvalid => "target_invalid",
            Self::TargetAppeared => "target_appeared",
            Self::VerificationMismatch => "verification_mismatch",
        }
    }
}

/// Why a metadata-only search projection epoch advance was refused before it
/// published anything. The projection keeps its previous epoch; the host can
/// fall back to an ordinary apply or catch-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionEpochAdvanceRefusal {
    /// A graph commit in the range changed something that maps to a search
    /// projection document, so the projection content may differ at the target.
    ProjectionChanged,
    /// The retained search change log does not cover the whole range: a
    /// commit in it could not be captured, or its entries were trimmed.
    ChangeLogGap,
    /// The range cannot be checked: the projection has no source epoch, or the
    /// target is beyond the graph's current commit epoch.
    RangeUnavailable,
    /// The resident projection holds changes that are not checkpointed, so its
    /// durable files do not describe the in-memory projection.
    UncheckpointedChanges,
}

impl SearchProjectionEpochAdvanceRefusal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectionChanged => "projection_changed",
            Self::ChangeLogGap => "change_log_gap",
            Self::RangeUnavailable => "range_unavailable",
            Self::UncheckpointedChanges => "uncheckpointed_changes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawDBError {
    Parse(String),
    Semantic(String),
    Storage(String),
    /// A relational constraint violation, such as a duplicate key or a NULL
    /// in a NOT NULL column. It displays as `Storage` or `Execution` would,
    /// by `surface`; `message` is the relational text after that prefix.
    Constraint {
        surface: RelationalErrorSurface,
        message: String,
    },
    /// A relational schema refusal, such as a value of the wrong type for its
    /// column. Displays like [`HawDBError::Constraint`].
    Schema {
        surface: RelationalErrorSurface,
        message: String,
    },
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
    /// A directory publication reached the point where its result may be
    /// visible at `destination`, then failed before success was durable (for
    /// example the parent-directory sync). The destination is not a failed
    /// attempt: never remove it on the strength of this error. Reconcile it
    /// instead (docs/LOGICAL_DUMP.md, "Unknown publication outcome").
    PublishOutcomeUnknown {
        destination: std::path::PathBuf,
        message: String,
    },
    /// A registered Search target is retained by another native operation.
    ProjectionBusy,
    /// A search projection's resident snapshot disagrees with the immutable
    /// projection its selector publishes, for example after a crash inside a
    /// resident checkpoint written by HawDB 0.31.0 or earlier (#1711). The
    /// projection is refused as a whole and is not retryable as-is: rebuild it
    /// from the authoritative graph. The check reads only the projection's own
    /// directory; the graph opens first and is not affected. `projection` is
    /// the projection directory when the refusing open knows it. Displays as
    /// a `storage error`.
    SearchProjectionMismatch {
        projection: Option<std::path::PathBuf>,
        message: String,
    },
    /// A search projection rebuild was refused and installed nothing. The
    /// graph, the current projection directory and the target name are
    /// unchanged; only this attempt is void. This is not a storage failure:
    /// fix the precondition named by `reason` (or retry a verification
    /// mismatch after investigating the host source) and rebuild again.
    SearchProjectionRebuildRefused {
        reason: SearchProjectionRebuildRefusal,
        target: std::path::PathBuf,
        message: String,
    },
    /// A metadata-only search projection epoch advance was refused and
    /// published nothing: the projection keeps `source_graph_commit_epoch`.
    /// This is not a storage failure. The graph and the projection are
    /// unchanged; apply or catch up the range normally instead.
    SearchProjectionEpochAdvanceRefused {
        reason: SearchProjectionEpochAdvanceRefusal,
        source_graph_commit_epoch: Option<u64>,
        target_graph_commit_epoch: u64,
        message: String,
    },
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
            HawDBError::Constraint { surface, message }
            | HawDBError::Schema { surface, message } => {
                write!(f, "{}: {message}", surface.prefix())
            }
            HawDBError::FileDescriptors(error) => Display::fmt(error, f),
            HawDBError::WalRecoveryIo { operation, message, .. } => write!(f, "WAL recovery {operation} failed; retain evidence and retry open: {message}"),
            HawDBError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            HawDBError::UnsupportedStorageFormat(error) => Display::fmt(error, f),
            HawDBError::PublishOutcomeUnknown { destination, message } => write!(
                f,
                "publication of {} has an unknown outcome; keep the destination and reconcile it before retrying: {message}",
                destination.display()
            ),
            HawDBError::ProjectionBusy => f.write_str("search projection operation is busy"),
            HawDBError::SearchProjectionMismatch { message, .. } => {
                write!(f, "storage error: {message}")
            }
            HawDBError::SearchProjectionRebuildRefused {
                reason, message, ..
            } => write!(
                f,
                "search projection rebuild refused ({}): {message}",
                reason.as_str()
            ),
            HawDBError::SearchProjectionEpochAdvanceRefused {
                reason, message, ..
            } => write!(
                f,
                "search projection epoch advance refused ({}): {message}",
                reason.as_str()
            ),
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

    /// A relational refusal of `kind`, reported by `surface`.
    pub fn relational(
        kind: RelationalErrorKind,
        surface: RelationalErrorSurface,
        message: impl Into<String>,
    ) -> Self {
        let message = message.into();
        match kind {
            RelationalErrorKind::Constraint => Self::Constraint { surface, message },
            RelationalErrorKind::Schema => Self::Schema { surface, message },
        }
    }

    /// The relational refusal kind, for `Constraint` and `Schema` only.
    pub const fn relational_error_kind(&self) -> Option<RelationalErrorKind> {
        match self {
            Self::Constraint { .. } => Some(RelationalErrorKind::Constraint),
            Self::Schema { .. } => Some(RelationalErrorKind::Schema),
            _ => None,
        }
    }

    /// The layer that reported a `Constraint` or `Schema` refusal.
    pub const fn relational_error_surface(&self) -> Option<RelationalErrorSurface> {
        match self {
            Self::Constraint { surface, .. } | Self::Schema { surface, .. } => Some(*surface),
            _ => None,
        }
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
        } else if let Some(kind) = find_relational_error_kind(&error) {
            Self::relational(kind, RelationalErrorSurface::Storage, error.to_string())
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
            Self::Constraint {
                surface: surface @ RelationalErrorSurface::Storage,
                message,
            } => Self::Constraint {
                surface,
                message: format!("{context}: {message}"),
            },
            Self::Schema {
                surface: surface @ RelationalErrorSurface::Storage,
                message,
            } => Self::Schema {
                surface,
                message: format!("{context}: {message}"),
            },
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
