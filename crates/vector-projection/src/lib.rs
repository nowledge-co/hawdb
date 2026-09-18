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

//! Rebuildable RaBitQ candidate projections. Applications integrate through
//! the embedded `hawdb` facade, not through experimental index primitives.
//!
//! # Experimental APIs
//!
//! The HNSW, delta, and advisor exports are retained for standalone experiments
//! and benchmarks, not used by the embedded serving or maintenance path:
//!
//! - [`HnswIndex`] and [`HnswBuildConfig`] have no persistent generation binding
//!   or candidate-filter support.
//! - [`DeltaBuffer`], [`search_with_delta`], and [`DeltaMergedSearchOutput`] have
//!   no delete/tombstone or checkpoint/recovery contract.
//! - [`IndexAdvisor`], [`IndexPlanner`], [`AutoIndexPolicy`], [`WorkloadSample`],
//!   [`IndexRecommendation`], [`IndexAction`], and [`QueryPath`] express
//!   recommendations or caller-supplied availability, not serving readiness.
//!
//! Their retention decision, prerequisites, verification gates, and removal
//! criteria are tracked in the
//! [vector experiment roadmap](https://github.com/nowledge-co/hawdb/blob/main/docs/VECTOR_EXPERIMENT_ROADMAP.md)
//! for [issue #228](https://github.com/nowledge-co/hawdb/issues/228).
//! This is not approval to wire them into production or auto-apply advice.

mod advisor;
mod artifact;
mod build;
mod codec;
mod delta;
mod error;
mod hnsw;
mod kernel;
mod model;
mod rabitq;
mod scan;
mod transform;

pub use advisor::{
    AutoIndexPolicy, IndexAction, IndexAdvisor, IndexPlanner, IndexRecommendation, QueryPath,
    WorkloadSample,
};
pub use artifact::{FileProjection, ProjectionWriter};
pub use build::{source_digest, ProjectionBuilder};
pub use delta::{search_with_delta, DeltaBuffer, DeltaMergedSearchOutput};
pub use error::{ProjectionError, Result};
pub use hnsw::{HnswBuildConfig, HnswIndex};
pub use kernel::{KernelPreference, ScanKernel};
pub use model::{
    InMemoryProjection, ProjectionBuildAdmission, ProjectionBuildConfig, ProjectionBuildReport,
    ProjectionIdentity, ProjectionManifest, ProjectionMetric, RaBitQBitWidth, SegmentDescriptor,
    DEFAULT_BUILD_MEMORY_BYTES, DEFAULT_PROJECTION_BIT_WIDTH, DEFAULT_SEGMENT_ROWS,
    DEFAULT_TRANSFORM_SEED, PROJECTION_ALGORITHM, PROJECTION_BIT_WIDTH, PROJECTION_CALIBRATION,
    PROJECTION_FORMAT_VERSION, PROJECTION_PROTOCOL, PROJECTION_QUANTIZER, PROJECTION_TRANSFORM,
};
pub use scan::{
    CandidateSet, ProjectionHit, ProjectionSearchOptions, ProjectionSearchOutput,
    ProjectionSearchReport,
};
