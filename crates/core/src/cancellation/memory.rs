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

use super::*;

/// Ownership of memory admitted before an allocation is attempted.
/// Keep this lease until the allocation is destroyed, including retained state.
pub trait RuntimeMemoryPermit: std::fmt::Debug + Send + Sync {
    fn bytes(&self) -> u64;
}

/// A shared allocation ledger belonging to an already admitted task.
/// Controller ownership does not keep execution slots alive after task closure.
pub trait RuntimeMemoryController: std::fmt::Debug + Send + Sync {
    fn reserve(
        &self,
        bytes: u64,
        ceiling: u64,
    ) -> Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMemoryError {
    Stopped(RuntimeCancellationReason),
    ReservationExceeded {
        requested_bytes: u64,
        available_bytes: u64,
    },
    Closed,
    Pressure,
}

impl Display for RuntimeMemoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped(reason) => Display::fmt(reason, formatter),
            Self::ReservationExceeded { requested_bytes, available_bytes } => write!(formatter,
                "runtime memory allocation requested {requested_bytes} bytes, exceeding the {available_bytes}-byte remaining reservation"),
            Self::Closed => formatter.write_str("runtime memory reservation is closed"),
            Self::Pressure => formatter.write_str("background memory pressure defers allocation"),
        }
    }
}

impl Error for RuntimeMemoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            _ => None,
        }
    }
}
