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

use hawdb_core::RuntimeCancellationReason;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum ProjectionError {
    InvalidConfiguration(String),
    InvalidVector(String),
    DuplicateId(u64),
    NonMonotonicId { previous: u64, next: u64 },
    CorruptArtifact(String),
    ResourceBudgetExceeded { required: usize, available: usize },
    UnsupportedKernel(&'static str),
    Cancelled(RuntimeCancellationReason),
    Io(io::Error),
    Serialization(serde_json::Error),
}

impl Display for ProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => {
                write!(formatter, "invalid vector projection configuration: {reason}")
            }
            Self::InvalidVector(reason) => write!(formatter, "invalid vector: {reason}"),
            Self::DuplicateId(id) => write!(formatter, "duplicate vector projection id {id}"),
            Self::NonMonotonicId { previous, next } => write!(
                formatter,
                "vector projection ids must be strictly increasing: {next} followed {previous}"
            ),
            Self::CorruptArtifact(reason) => {
                write!(formatter, "corrupt vector projection artifact: {reason}")
            }
            Self::ResourceBudgetExceeded {
                required,
                available,
            } => write!(
                formatter,
                "vector projection resource budget exceeded: required {required} bytes, available {available} bytes"
            ),
            Self::UnsupportedKernel(kernel) => {
                write!(formatter, "vector projection kernel {kernel} is unsupported")
            }
            Self::Cancelled(reason) => write!(formatter, "vector projection {reason}"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Serialization(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialization(error) => Some(error),
            Self::Cancelled(reason) => Some(reason),
            _ => None,
        }
    }
}

impl From<io::Error> for ProjectionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ProjectionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

impl From<RuntimeCancellationReason> for ProjectionError {
    fn from(reason: RuntimeCancellationReason) -> Self {
        Self::Cancelled(reason)
    }
}

pub type Result<T> = std::result::Result<T, ProjectionError>;
