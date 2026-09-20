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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HawDBError {
    Parse(String),
    Semantic(String),
    Storage(String),
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
}

impl Display for HawDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HawDBError::Parse(message) => write!(f, "parse error: {message}"),
            HawDBError::Semantic(message) => write!(f, "semantic error: {message}"),
            HawDBError::Storage(message) => write!(f, "storage error: {message}"),
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
        }
    }
}

impl std::error::Error for HawDBError {}

impl HawDBError {
    pub const fn is_retryable_transaction_conflict(&self) -> bool {
        matches!(self, Self::TransactionConflict { .. })
    }
}

pub type Result<T> = std::result::Result<T, HawDBError>;

impl From<std::io::Error> for HawDBError {
    fn from(error: std::io::Error) -> Self {
        HawDBError::Storage(error.to_string())
    }
}
