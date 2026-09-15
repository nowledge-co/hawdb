#![deny(unsafe_code)]

//! Host-neutral protocol models for Nowledge retrieval and maintenance.
//!
//! Graph storage, transactions, and embedded runtime ownership remain with
//! the root Skein facade.

mod graph;
mod retrieval;
mod storage_lifecycle;

#[doc(hidden)]
pub mod test_support;

/// Stable host-neutral contracts exposed to the root Skein facade.
pub mod public {
    pub use crate::graph::*;
    pub use crate::retrieval::*;
    pub use crate::storage_lifecycle::*;
}

pub use graph::*;
pub use retrieval::*;
pub use storage_lifecycle::*;
