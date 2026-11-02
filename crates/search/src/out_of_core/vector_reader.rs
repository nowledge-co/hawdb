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

//! File-backed vector metadata follows the same shared segment lifetime.
use super::{
    open_rabitq_projection, vector_projection_error, SearchOutOfCoreManifestBody,
    SearchOutOfCoreSegmentManifest,
};
use crate::build_memory::{
    path::OwnedPath,
    read::{ReadAdmission, ReaderMemory},
};
use crate::{HawDBError, Result};
use hawdb_vector_projection::{
    FileProjection, ProjectionManifest, ProjectionSearchOptions, ProjectionSearchOutput,
};
use std::path::Path;

mod diskann;

/// Algorithm-neutral borrowed diagnostics for the selected candidate artifact.
pub(super) struct Manifest<'a> {
    pub(super) identity: &'a hawdb_vector_projection::ProjectionIdentity,
    pub(super) algorithm: &'a str,
    pub(super) format_version: u32,
    pub(super) dimension: usize,
    pub(super) document_count: usize,
    pub(super) source_digest: u64,
    pub(super) payload_bytes: u64,
    pub(super) payload_checksum: u32,
    pub(super) bit_width: u8,
    pub(super) transform_seed: u64,
    pub(super) segment_count: usize,
    pub(super) requested_segment_rows: usize,
    pub(super) admitted_segment_rows: usize,
    pub(super) configured_build_working_bytes: usize,
    pub(super) peak_build_working_bytes: usize,
}

impl<'a> From<&'a ProjectionManifest> for Manifest<'a> {
    fn from(value: &'a ProjectionManifest) -> Self {
        Self {
            identity: &value.identity,
            algorithm: &value.algorithm,
            format_version: value.format_version,
            dimension: value.dimension,
            document_count: value.document_count,
            source_digest: value.source_digest,
            payload_bytes: value.payload_bytes,
            payload_checksum: value.payload_checksum,
            bit_width: value.bit_width,
            transform_seed: value.transform_seed,
            segment_count: value.segments.len(),
            requested_segment_rows: value.requested_segment_rows,
            admitted_segment_rows: value.admitted_segment_rows,
            configured_build_working_bytes: value.configured_build_working_bytes,
            peak_build_working_bytes: value.peak_build_working_bytes,
        }
    }
}

#[derive(Debug)]
pub(super) enum Projection {
    Ordinary(FileProjection),
    Admitted(FileProjection<ReaderMemory>),
    DiskAnn(Box<diskann::GraphProjection<()>>),
    AdmittedDiskAnn(Box<diskann::GraphProjection<ReaderMemory>>),
}
impl Projection {
    pub(super) fn is_diskann(&self) -> bool {
        matches!(self, Self::DiskAnn(_) | Self::AdmittedDiskAnn(_))
    }

    /// Distance metric recorded in the artifact. RaBitQ projections are
    /// cosine-only (`VectorIndexDescriptor::new` refuses other metrics).
    pub(super) fn metric(&self) -> hawdb_core::VectorDistance {
        match self {
            Self::DiskAnn(value) => value.metric(),
            Self::AdmittedDiskAnn(value) => value.metric(),
            Self::Ordinary(_) | Self::Admitted(_) => hawdb_core::VectorDistance::Cosine,
        }
    }

    pub(super) fn backend(&self) -> Result<hawdb_core::VectorIndexBackend> {
        match self {
            Self::DiskAnn(value) => value.backend(),
            Self::AdmittedDiskAnn(value) => value.backend(),
            _ => {
                let metadata = self.manifest();
                Ok(hawdb_core::VectorIndexBackend::RaBitQ {
                    bit_width: metadata.bit_width,
                    segment_rows: u32::try_from(metadata.requested_segment_rows).map_err(|_| {
                        HawDBError::StorageIntegrity(
                            "RaBitQ segment rows exceed canonical bounds".into(),
                        )
                    })?,
                    transform_seed: metadata.transform_seed,
                })
            }
        }
    }

    pub(super) fn manifest(&self) -> Manifest<'_> {
        match self {
            Self::Ordinary(value) => value.manifest().into(),
            Self::Admitted(value) => value.manifest().into(),
            Self::DiskAnn(value) => value.manifest(),
            Self::AdmittedDiskAnn(value) => value.manifest(),
        }
    }
    pub(super) fn compatible(&self, other: &Self) -> bool {
        let left = self.manifest();
        let right = other.manifest();
        if left.algorithm != right.algorithm
            || left.bit_width != right.bit_width
            || left.transform_seed != right.transform_seed
        {
            return false;
        }
        match (self, other) {
            (Self::DiskAnn(left), Self::DiskAnn(right)) => left.parameters() == right.parameters(),
            (Self::DiskAnn(left), Self::AdmittedDiskAnn(right)) => {
                left.parameters() == right.parameters()
            }
            (Self::AdmittedDiskAnn(left), Self::DiskAnn(right)) => {
                left.parameters() == right.parameters()
            }
            (Self::AdmittedDiskAnn(left), Self::AdmittedDiskAnn(right)) => {
                left.parameters() == right.parameters()
            }
            _ => true,
        }
    }
    pub(super) fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> hawdb_vector_projection::Result<ProjectionSearchOutput> {
        match self {
            Self::Ordinary(value) => value.search(query, top_k, options),
            Self::Admitted(value) => value.search(query, top_k, options),
            Self::DiskAnn(value) => value.search(query, top_k, options),
            Self::AdmittedDiskAnn(value) => value.search(query, top_k, options),
        }
    }
}

pub(super) use crate::diskann_identity::embedding as embedding_identity_digest;

pub(super) fn open(
    root: &Path,
    active: &SearchOutOfCoreManifestBody,
    manifest: &SearchOutOfCoreSegmentManifest,
    max_working_bytes: usize,
    admission: Option<&ReadAdmission<'_>>,
    max_artifact_read_bytes: u64,
    captured_file: Option<hawdb_storage::file_io::File>,
) -> Result<Option<Projection>> {
    if manifest.diskann_source_digest.is_some() {
        return diskann::open(
            root,
            active,
            manifest,
            admission,
            max_artifact_read_bytes,
            captured_file,
        )
        .map(Some);
    }
    let Some(admission) = admission else {
        return open_rabitq_projection(root, active, manifest, max_working_bytes, captured_file)
            .map(|value| value.map(Projection::Ordinary));
    };
    if admission.is_synchronous()
        && manifest.rabitq_artifact_file.is_some()
        && captured_file.is_none()
    {
        return Err(HawDBError::Execution(
            "synchronous vector decoding requires a source-held file".into(),
        ));
    }
    let Some(name) = manifest.rabitq_artifact_file.as_deref() else {
        return Ok(None);
    };
    if manifest
        .rabitq_peak_build_working_bytes
        .expect("validated vector memory bound")
        > max_working_bytes
    {
        return Err(HawDBError::Execution(
            "search RaBitQ artifact exceeds serving admission".into(),
        ));
    }
    let length = manifest
        .rabitq_artifact_len
        .expect("validated vector length");
    let checksum = manifest
        .rabitq_artifact_checksum
        .expect("validated vector checksum");
    let path = OwnedPath::join(root, Path::new(name), admission.memory, admission.task)?;
    let file = match captured_file {
        Some(file) => {
            admission
                .visit_file_bound(file, length, checksum, max_artifact_read_bytes, |_| Ok(()))?
        }
        None => {
            admission.visit_bound(&path, length, checksum, max_artifact_read_bytes, |_| Ok(()))?
        }
    };
    // A logical File alone could be evicted and cold-validated inside vector
    // metadata()/read()/map(). Keep its admitted native lease until mapping and
    // metadata construction finish; the projection retains the map, not the FD.
    let file = admission.pin_bound_file(file, length, max_artifact_read_bytes)?;
    let projection = FileProjection::open_admitted_file(file, &path, admission)
        .map_err(vector_projection_error)?;
    let artifact = projection.manifest();
    if let Some(encoded) = &active.vector_index_binding {
        let _definition_memory = admission
            .memory
            .spool
            .reserve(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2)?;
        let definition = crate::vector_index::decode(encoded)?;
        crate::vector_index::validate_projection(&definition, artifact)?;
    }
    if artifact.identity.generation != manifest.generation
        || artifact.identity.source_epoch != manifest.source_graph_commit_epoch
        || artifact.identity.embedding_model.as_deref() != active.embedding_model.as_deref()
        || artifact.identity.embedding_version.as_deref() != active.embedding_version.as_deref()
        || Some(artifact.dimension) != active.embedding_dimension
        || Some(artifact.document_count) != manifest.rabitq_vector_document_count
        || Some(artifact.source_digest) != manifest.rabitq_source_digest
        || Some(artifact.payload_checksum) != manifest.rabitq_payload_checksum
    {
        return Err(HawDBError::Storage(
            "search out-of-core RaBitQ identity does not match its generation".into(),
        ));
    }
    Ok(Some(Projection::Admitted(projection)))
}
