// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::ExternalReadResourceContract;
use hawdb_core::{Result, VectorRef};
use hawdb_storage::NodeId;

pub struct VectorIndexExecutionRequest<'a> {
    pub index: &'a str,
    pub embedding: VectorRef<'a>,
    pub candidate_limit: usize,
    pub top_k: usize,
    pub mode: hawdb_core::VectorIndexQueryMode,
    pub eligible: Option<&'a crate::vector_eligibility::VectorEligibleDomain<'a>>,
    pub resources: ExternalReadResourceContract<'a>,
}

#[derive(Debug, Clone, Copy)]
pub struct VectorIndexExecutionRow {
    pub node_id: NodeId,
    pub distance: f64,
}

/// The implementation keeps candidate payloads attached to their original
/// admission. Rows expose fixed-size identities without copying retained IDs.
pub trait VectorIndexExecutionOutput {
    fn row_count(&self) -> usize;
    fn row(&self, ordinal: usize) -> Result<VectorIndexExecutionRow>;
}
