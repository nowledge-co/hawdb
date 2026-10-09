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
            Self::BudgetExceeded { requested, available, limit } => write!(formatter,
                "project file descriptor budget exceeded: requested {requested}, available {available}, limit {limit}"),
            Self::OsLimit { requested, os_code, soft, hard } => write!(formatter,
                "operating system file descriptor limit: requested {requested}, soft {soft:?}, hard {hard:?}, OS code {os_code:?}"),
        }
    }
}

impl std::error::Error for FileDescriptorError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawDBError {
    Parse(String),
    Semantic(String),
    Storage(String),
    FileDescriptors(FileDescriptorError),
    StorageIntegrity(String),
    Execution(String),
    TransactionConflict {
        read_epoch: u64,
        committed_epoch: u64,
        key: String,
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
}

impl Display for HawDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HawDBError::Parse(message) => write!(f, "parse error: {message}"),
            HawDBError::Semantic(message) => write!(f, "semantic error: {message}"),
            HawDBError::Storage(message) => write!(f, "storage error: {message}"),
            HawDBError::FileDescriptors(error) => Display::fmt(error, f),
            HawDBError::StorageIntegrity(message) => {
                write!(f, "storage integrity error: {message}")
            }
            HawDBError::Execution(message) => write!(f, "execution error: {message}"),
            HawDBError::TransactionConflict {
                read_epoch,
                committed_epoch,
                key,
            } => write!(
                f,
                "transaction conflict on {key}: transaction read epoch {read_epoch}, conflicting commit epoch {committed_epoch}"
            ),
            HawDBError::AppendSequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                f,
                "append commit sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            HawDBError::CapabilityUnavailable { capability } => {
                write!(f, "capability unavailable: {}", capability.as_str())
            }
            HawDBError::BranchCommandUnsupported { command, context } => {
                write!(f, "{command} is unavailable in {context}")
            }
            HawDBError::BranchBusy { resource } => write!(f, "branch is busy: {resource}"),
        }
    }
}

impl std::error::Error for HawDBError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileDescriptors(error) => Some(error),
            _ => None,
        }
    }
}

impl HawDBError {
    /// Preserve a descriptor rejection through nested storage error wrappers.
    pub fn from_storage_error(error: impl std::error::Error + 'static) -> Self {
        if let Some(error) = (&error as &dyn std::error::Error).downcast_ref::<Self>() {
            return error.clone();
        }
        if let Some(resource) = file_descriptor_error(&error) {
            Self::FileDescriptors(resource)
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

    pub const fn is_retryable_transaction_conflict(&self) -> bool {
        matches!(self, Self::TransactionConflict { .. })
    }
}

pub type Result<T> = std::result::Result<T, HawDBError>;

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
