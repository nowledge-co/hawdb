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

#![deny(unsafe_code)]

//! Host-neutral protocol models for Nowledge retrieval and maintenance.
//!
//! Graph storage, transactions, and embedded runtime ownership remain with
//! the root HawDB facade.

mod graph;
mod maintenance;
mod read_snapshot;
mod retrieval;
mod storage_lifecycle;
mod workload;

#[doc(hidden)]
pub mod test_support;

/// Stable host-neutral contracts exposed to the root HawDB facade.
pub mod public {
    pub use crate::graph::*;
    pub use crate::maintenance::*;
    pub use crate::read_snapshot::*;
    pub use crate::retrieval::*;
    pub use crate::storage_lifecycle::*;
    pub use crate::workload::*;
}

pub use graph::*;
pub use maintenance::*;
pub use read_snapshot::*;
pub use retrieval::*;
pub use storage_lifecycle::*;
pub use workload::*;
