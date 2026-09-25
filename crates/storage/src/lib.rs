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

// Let the embedded graph kernel keep addressing this crate as `hawdb_storage`
// after moving in: its sources predate the move and are kept verbatim.
extern crate self as hawdb_storage;

pub use hawdb_core::error::{HawDBError, Result};
pub use hawdb_core::value::Value;

pub mod adjacency;
pub mod append_table;
#[doc(hidden)]
pub mod artifact_binding;
#[doc(hidden)]
pub mod artifact_files;
pub mod background;
pub mod backup;
pub mod cache;
pub mod canonical;
pub mod canonical_adjacency;
#[doc(hidden)]
pub mod checkpoint;
pub mod column_group;
pub mod config;
#[doc(hidden)]
pub mod consistency;
#[doc(hidden)]
pub mod cow;
pub mod derived_repair;
pub mod doctor;
pub mod durability;
#[doc(hidden)]
pub mod durable_manifest;
#[doc(hidden)]
pub mod graph_constraints;
pub mod graph_descriptor_page;
pub mod graph_descriptor_tree;
#[doc(hidden)]
pub mod graph_engine;
#[doc(hidden)]
pub mod graph_index;
#[doc(hidden)]
pub mod graph_index_metrics;
#[doc(hidden)]
pub mod graph_overlay;
pub use hawdb_core::ids;
pub use hawdb_core::ids::{NodeId, NodeRecord, ProjectedNodeRecord, RelId, RelRecord};
pub mod index_page;
#[doc(hidden)]
pub mod io;
pub mod mutation;
#[doc(hidden)]
pub mod ownership;
#[doc(hidden)]
pub mod predicate;
pub mod pressure;
pub mod projection;

// Compatibility shims so the graph kernel can move into this crate without
// edit churn: the kernel addresses these items through `crate::error`,
// `crate::schema`, `crate::value`, `crate::telemetry` and `crate::analytics`,
// which were root-level re-exports before the move.
#[doc(hidden)]
pub mod error {
    pub use hawdb_core::error::*;
}

#[doc(hidden)]
pub mod value {
    pub use hawdb_core::value::*;
}

#[doc(hidden)]
pub mod analytics {
    pub use hawdb_analytics::*;
}
pub mod projection_generation;
pub mod property_projection;
pub mod property_spill;
#[doc(hidden)]
pub mod read_view;
pub mod relational;
#[doc(hidden)]
pub mod relational_index_view;
#[doc(hidden)]
pub mod relational_row_workspace;
#[doc(hidden)]
pub mod residency;
pub mod scan;
pub mod schema;
pub mod snapshot;
#[doc(hidden)]
pub mod source_scan;
pub mod stable_identity;
pub mod statistics;
#[doc(hidden)]
pub mod statistics_refresh;
pub mod store;
pub mod telemetry;
#[doc(hidden)]
pub mod text;
#[doc(hidden)]
pub mod transaction_locks;
#[doc(hidden)]
pub mod version;
#[doc(hidden)]
pub mod wal;
pub mod wire;

#[cfg(test)]
mod file_lock_tests;
#[cfg(test)]
mod hex_test_support;
