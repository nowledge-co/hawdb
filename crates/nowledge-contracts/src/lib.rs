#![deny(unsafe_code)]

//! Host-neutral protocol models for Nowledge retrieval and maintenance.
//!
//! Graph storage, transactions, and embedded runtime ownership remain with
//! the root Skein facade.

mod graph;
mod retrieval;

pub use graph::*;
pub use retrieval::*;
