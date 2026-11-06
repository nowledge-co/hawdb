// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual SQLsmith and frozen-workload cost experiments over the embedded API.
//! This crate is private developer tooling, never a production dependency.

pub mod cardinality;
pub mod cli;
pub mod graph_statistics;
pub mod manifest;
pub mod runner;
