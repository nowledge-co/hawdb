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

//! Storage-neutral projection contract shared by the graph kernel, the
//! analytics crate, and the embedded facade.
//!
//! These items live below both `hawdb-storage` and `hawdb-analytics` so the
//! kernel can materialize projected graphs without an inverted dependency on
//! the analytics crate.

use crate::ids::{NodeRecord, RelRecord};

/// Control flow a projection scan visitor returns to the scan driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionScanControl {
    Continue,
    Stop,
}

/// Storage-neutral source consumed while building an immutable projection.
pub trait ProjectionSource {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> ProjectionScanControl,
    ) -> Result<ProjectionScanControl, String>;

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> ProjectionScanControl,
    ) -> Result<ProjectionScanControl, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionLayout {
    Outgoing,
    Incoming,
    Bidirectional,
    Undirected,
}

impl ProjectionLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
            Self::Bidirectional => "bidirectional",
            Self::Undirected => "undirected",
        }
    }

    #[doc(hidden)]
    pub fn stores_outgoing(self) -> bool {
        matches!(
            self,
            Self::Outgoing | Self::Bidirectional | Self::Undirected
        )
    }

    #[doc(hidden)]
    pub fn stores_incoming(self) -> bool {
        matches!(self, Self::Incoming | Self::Bidirectional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryBudget {
    max_bytes: Option<std::num::NonZeroUsize>,
}

impl ProjectionMemoryBudget {
    pub const fn unlimited() -> Self {
        Self { max_bytes: None }
    }

    pub const fn new(max_bytes: std::num::NonZeroUsize) -> Self {
        Self {
            max_bytes: Some(max_bytes),
        }
    }

    pub fn max_bytes(self) -> Option<usize> {
        self.max_bytes.map(std::num::NonZeroUsize::get)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryEstimate {
    pub layout: ProjectionLayout,
    pub node_count: usize,
    pub relationship_count: usize,
    pub projected_edge_count: usize,
    pub estimated_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphAlgorithmMemoryEstimate {
    pub projection_bytes: usize,
    pub algorithm_peak_bytes: usize,
    pub result_bytes: usize,
    pub total_peak_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionMemoryAdmissionError {
    pub estimate: ProjectionMemoryEstimate,
    pub budget_bytes: usize,
    pub storage_error: Option<String>,
}

impl std::fmt::Display for ProjectionMemoryAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(error) = &self.storage_error {
            return write!(
                formatter,
                "analytics projection storage scan failed: {error}"
            );
        }
        write!(
            formatter,
            "analytics projection layout '{}' requires an estimated {} bytes for {} nodes and {} relationships, exceeding the {} byte budget",
            self.estimate.layout.as_str(),
            self.estimate.estimated_bytes,
            self.estimate.node_count,
            self.estimate.relationship_count,
            self.budget_bytes,
        )
    }
}

impl std::error::Error for ProjectionMemoryAdmissionError {}

#[doc(hidden)]
impl ProjectionMemoryAdmissionError {
    pub fn storage(error: impl std::fmt::Display) -> Self {
        Self {
            estimate: ProjectionMemoryEstimate {
                layout: ProjectionLayout::Bidirectional,
                node_count: 0,
                relationship_count: 0,
                projected_edge_count: 0,
                estimated_bytes: 0,
            },
            budget_bytes: 0,
            storage_error: Some(error.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankOptions {
    pub iterations: usize,
    pub damping: f64,
}

impl Default for PageRankOptions {
    fn default() -> Self {
        Self {
            iterations: 20,
            damping: 0.85,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankScore {
    pub node: crate::ids::NodeId,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LouvainOptions {
    pub max_iterations: usize,
    pub max_levels: usize,
}

impl Default for LouvainOptions {
    fn default() -> Self {
        Self {
            max_iterations: 20,
            max_levels: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommunityAssignment {
    pub node: crate::ids::NodeId,
    pub community: crate::ids::NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HierarchicalCommunityAssignment {
    pub level: usize,
    pub node: crate::ids::NodeId,
    pub community: crate::ids::NodeId,
}
