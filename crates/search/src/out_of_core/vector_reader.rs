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

#[derive(Debug)]
pub(super) enum Projection {
    Ordinary(FileProjection),
    Admitted(FileProjection<ReaderMemory>),
}
impl Projection {
    pub(super) fn manifest(&self) -> &ProjectionManifest {
        match self {
            Self::Ordinary(value) => value.manifest(),
            Self::Admitted(value) => value.manifest(),
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
        }
    }
}

pub(super) fn open(
    root: &Path,
    active: &SearchOutOfCoreManifestBody,
    manifest: &SearchOutOfCoreSegmentManifest,
    max_working_bytes: usize,
    admission: Option<&ReadAdmission<'_>>,
    max_artifact_read_bytes: u64,
) -> Result<Option<Projection>> {
    let Some(admission) = admission else {
        return open_rabitq_projection(root, active, manifest, max_working_bytes)
            .map(|value| value.map(Projection::Ordinary));
    };
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
    let file =
        admission.visit_bound(&path, length, checksum, max_artifact_read_bytes, |_| Ok(()))?;
    // A logical File alone could be evicted and cold-validated inside vector
    // metadata()/read()/map(). Keep its admitted native lease until mapping and
    // metadata construction finish; the projection retains the map, not the FD.
    let file = admission.pin_bound_file(file, length, max_artifact_read_bytes)?;
    let projection = FileProjection::open_admitted_file(file, &path, admission)
        .map_err(vector_projection_error)?;
    let artifact = projection.manifest();
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
