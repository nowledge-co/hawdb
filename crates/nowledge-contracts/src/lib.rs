#![deny(unsafe_code)]

//! Host-neutral protocol models for Nowledge retrieval and maintenance.
//!
//! Graph storage, transactions, and embedded runtime ownership remain with
//! the root Skein facade.

mod graph;
mod maintenance;
mod read_snapshot;
mod retrieval;
mod storage_lifecycle;
mod workload;

#[doc(hidden)]
pub mod test_support;

/// Stable host-neutral contracts exposed to the root Skein facade.
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
