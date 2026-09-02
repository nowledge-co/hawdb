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
