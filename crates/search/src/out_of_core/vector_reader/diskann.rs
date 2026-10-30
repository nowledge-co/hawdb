// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Source-held graph mappings share the ordinary Consumer file/lease closure.

use super::*;
use hawdb_storage::file_io::File;
use hawdb_vector_projection::diskann::{
    search_diskann, DiskAnnArtifactIdentity, DiskAnnRead, DiskAnnSearchOptions, FileDiskAnn,
};
use hawdb_vector_projection::{
    ProjectionError, ProjectionHit, ProjectionIdentity, ProjectionSearchReport,
};

#[derive(Debug)]
pub(in crate::out_of_core) struct GraphProjection<R> {
    graph: FileDiskAnn<R>,
    identity: ProjectionIdentity,
    payload_bytes: u64,
    payload_checksum: u32,
    peak_build_bytes: usize,
    _memory: Option<ReaderMemory>,
}

pub(super) type Parameters = (
    hawdb_core::VectorDistance,
    usize,
    usize,
    u64,
    Option<(usize, usize, u64)>,
);

impl<R> GraphProjection<R> {
    pub(super) fn backend(&self) -> Result<hawdb_core::VectorIndexBackend> {
        let metadata = self.graph.metadata();
        let integer = |value| {
            u32::try_from(value)
                .map_err(|_| invalid("graph settings exceed canonical integer bounds"))
        };
        let milli = |value: f64| -> Result<u32> {
            let scaled = (value * 1000.0).round();
            if !scaled.is_finite()
                || scaled < 0.0
                || scaled > f64::from(u32::MAX)
                || scaled / 1000.0 != value
            {
                return Err(invalid(
                    "graph settings are not canonical integer-scaled values",
                ));
            }
            Ok(scaled as u32)
        };
        Ok(hawdb_core::VectorIndexBackend::DiskAnn {
            max_degree: integer(metadata.max_degree)?,
            build_beam: integer(metadata.build_beam)?,
            alpha_milli: milli(metadata.alpha)?,
            lsg: metadata
                .lsg
                .map(|value| -> Result<_> {
                    Ok(hawdb_core::VectorIndexLsg {
                        sample_size: integer(value.sample_size)?,
                        neighbors: integer(value.neighbors)?,
                        exponent_milli: milli(value.exponent)?,
                    })
                })
                .transpose()?,
        })
    }

    pub(super) fn parameters(&self) -> Parameters {
        let metadata = self.graph.metadata();
        (
            metadata.metric,
            metadata.max_degree,
            metadata.build_beam,
            metadata.alpha.to_bits(),
            metadata
                .lsg
                .map(|value| (value.sample_size, value.neighbors, value.exponent.to_bits())),
        )
    }

    pub(super) fn manifest(&self) -> Manifest<'_> {
        let metadata = self.graph.metadata();
        Manifest {
            identity: &self.identity,
            algorithm: "diskann",
            format_version: 2,
            dimension: metadata.dimension,
            document_count: metadata.node_count,
            source_digest: u64::from_le_bytes(
                metadata.identity.source_digest[..8]
                    .try_into()
                    .expect("digest prefix"),
            ),
            payload_bytes: self.payload_bytes,
            payload_checksum: self.payload_checksum,
            bit_width: 0,
            transform_seed: 0,
            segment_count: 1,
            requested_segment_rows: metadata.node_count,
            admitted_segment_rows: metadata.node_count,
            configured_build_working_bytes: self.peak_build_bytes,
            peak_build_working_bytes: self.peak_build_bytes,
        }
    }

    pub(super) fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> hawdb_vector_projection::Result<ProjectionSearchOutput> {
        // Validate explicit ISA requests, but report the shared Float64 distance
        // kernel actually used by graph traversal rather than a RaBitQ scanner.
        hawdb_vector_projection::select_scan_kernel(options.kernel)?;
        let kernel =
            match hawdb_core::vector::distance_kernel_name(self.graph.metric(), query.len()) {
                "avx2" => hawdb_vector_projection::ScanKernel::Avx2,
                "neon" => hawdb_vector_projection::ScanKernel::Neon,
                _ => hawdb_vector_projection::ScanKernel::Scalar,
            };
        let conversion_bytes = top_k
            .checked_mul(size_of::<ProjectionHit>())
            .ok_or_else(|| {
                ProjectionError::InvalidConfiguration("DiskANN result size overflow".into())
            })?;
        let max_working_bytes = options
            .max_working_bytes
            .checked_sub(conversion_bytes)
            .ok_or(ProjectionError::ResourceBudgetExceeded {
                required: conversion_bytes,
                available: options.max_working_bytes,
            })?;
        let output = search_diskann(
            &self.graph,
            query,
            top_k,
            options.candidates,
            DiskAnnSearchOptions {
                beam_width: top_k.max(crate::vector_execution::DISKANN_NAVIGATION_BEAM),
                max_working_bytes,
                task: options.task_context,
                ..Default::default()
            },
        )?;
        let report = output.report;
        let payload_bytes_read = (report.visited_nodes as u64)
            .saturating_mul(8)
            .saturating_add((report.id_lookup_steps as u64).saturating_mul(8))
            .saturating_add((report.neighbor_reads as u64).saturating_mul(4))
            .saturating_add((report.expanded_nodes as u64).saturating_mul(4))
            .saturating_add(
                (report.distance_comparisons as u64).saturating_mul(
                    (self.graph.dimension() as u64)
                        .saturating_mul(8)
                        .saturating_add(4),
                ),
            );
        let candidate_count = output.hits.len();
        Ok(ProjectionSearchOutput {
            hits: output
                .hits
                .into_iter()
                .map(|hit| ProjectionHit {
                    id: hit.id,
                    // Candidate priorities are Float32; logarithmic ordering
                    // stays finite even when a valid L2 distance exceeds f32.
                    // Final distances are independently reranked as Float64.
                    score: -(hit.distance.ln_1p() as f32),
                })
                .collect(),
            report: ProjectionSearchReport {
                kernel,
                worker_count: 1,
                segment_count: 1,
                scanned_segment_count: usize::from(report.visited_nodes != 0),
                document_count: self.graph.len(),
                scored_document_count: report.distance_comparisons,
                filtered_document_count: report.bridge_nodes,
                scanned_block_count: report.expanded_nodes,
                skipped_block_count: 0,
                adaptive_exact_scan_count: usize::from(report.adaptive_exact_scan),
                payload_bytes_read,
                admitted_working_bytes: options.max_working_bytes,
                candidate_count,
                candidate_set_exact: options.candidates.map(|value| value.exact),
            },
        })
    }
}

pub(super) fn open(
    root: &Path,
    active: &SearchOutOfCoreManifestBody,
    manifest: &SearchOutOfCoreSegmentManifest,
    admission: Option<&ReadAdmission<'_>>,
    max_read_bytes: u64,
    captured_file: Option<File>,
) -> Result<Projection> {
    let expected = DiskAnnArtifactIdentity {
        binding_digest: super::embedding_identity_digest(
            active.embedding_model.as_deref(),
            active.embedding_version.as_deref(),
            active.embedding_dimension,
            admission.map(|value| value.task),
        )?,
        source_digest: manifest
            .diskann_source_digest
            .ok_or_else(|| invalid("missing graph source digest"))?,
        source_epoch: manifest.source_graph_commit_epoch.unwrap_or(0),
        generation: manifest.generation,
    };
    let name = manifest
        .rabitq_artifact_file
        .as_deref()
        .ok_or_else(|| invalid("missing graph file"))?;
    let length = manifest
        .rabitq_artifact_len
        .ok_or_else(|| invalid("missing graph length"))?;
    let checksum = manifest
        .rabitq_artifact_checksum
        .ok_or_else(|| invalid("missing graph checksum"))?;
    let identity_bytes = size_of::<GraphProjection<ReaderMemory>>()
        .checked_add(active.embedding_model.as_ref().map_or(0, String::len))
        .and_then(|bytes| {
            bytes.checked_add(active.embedding_version.as_ref().map_or(0, String::len))
        })
        .ok_or_else(|| invalid("graph identity capacity overflow"))?;
    let identity_memory = admission
        .map(|value| value.retained(identity_bytes))
        .transpose()?;
    let identity = ProjectionIdentity {
        generation: manifest.generation,
        source_epoch: manifest.source_graph_commit_epoch,
        embedding_model: active.embedding_model.clone(),
        embedding_version: active.embedding_version.clone(),
    };
    let peak_build_bytes = manifest
        .rabitq_peak_build_working_bytes
        .ok_or_else(|| invalid("missing graph build envelope"))?;
    let projection = if let Some(admission) = admission {
        if admission.is_synchronous() && captured_file.is_none() {
            return Err(invalid(
                "synchronous graph mapping requires a source-held file",
            ));
        }
        let path = OwnedPath::join(root, Path::new(name), admission.memory, admission.task)?;
        let file = match captured_file {
            Some(file) => {
                admission.visit_file_bound(file, length, checksum, max_read_bytes, |_| Ok(()))?
            }
            None => admission.visit_bound(&path, length, checksum, max_read_bytes, |_| Ok(()))?,
        };
        let file = admission.pin_bound_file(file, length, max_read_bytes)?;
        let graph = FileDiskAnn::open_admitted_file(file, expected, admission)
            .map_err(vector_projection_error)?;
        validate(active, manifest, &graph, Some(admission))?;
        Projection::AdmittedDiskAnn(Box::new(GraphProjection {
            graph,
            identity,
            payload_bytes: length.saturating_sub(156),
            payload_checksum: manifest.rabitq_payload_checksum.unwrap_or(0),
            peak_build_bytes,
            _memory: identity_memory,
        }))
    } else {
        if captured_file.is_none() {
            super::super::verify_rabitq_artifact(root, manifest)?;
        }
        let graph = FileDiskAnn::open_file(
            match captured_file {
                Some(file) => file,
                None => File::open(root.join(name))?,
            },
            expected,
        )
        .map_err(vector_projection_error)?;
        validate(active, manifest, &graph, None)?;
        Projection::DiskAnn(Box::new(GraphProjection {
            graph,
            identity,
            payload_bytes: length.saturating_sub(156),
            payload_checksum: manifest.rabitq_payload_checksum.unwrap_or(0),
            peak_build_bytes,
            _memory: identity_memory,
        }))
    };
    Ok(projection)
}

fn validate<R>(
    active: &SearchOutOfCoreManifestBody,
    segment: &SearchOutOfCoreSegmentManifest,
    graph: &FileDiskAnn<R>,
    admission: Option<&ReadAdmission<'_>>,
) -> Result<()> {
    let metadata = graph.metadata();
    if Some(metadata.dimension) != active.embedding_dimension
        || Some(metadata.node_count) != segment.rabitq_vector_document_count
        || segment.rabitq_source_digest
            != Some(u64::from_le_bytes(
                metadata.identity.source_digest[..8]
                    .try_into()
                    .expect("digest prefix"),
            ))
    {
        return Err(invalid(
            "graph shape or source digest differs from its manifest",
        ));
    }
    if let Some(encoded) = &active.vector_index_binding {
        let _scratch = admission
            .map(|value| {
                value.scratch(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2)
            })
            .transpose()?;
        let definition = crate::vector_index::decode(encoded)?;
        let expected = crate::vector_index::diskann_build_config(definition.backend(), usize::MAX)
            .ok_or_else(|| invalid("graph artifact has a different bound backend"))?;
        if metadata.dimension != definition.dimension().get() as usize
            || metadata.metric != definition.distance()
            || metadata.max_degree != expected.max_degree
            || metadata.build_beam != expected.build_beam
            || metadata.alpha != expected.alpha
            || metadata.lsg != expected.lsg
        {
            return Err(invalid("graph settings differ from the bound definition"));
        }
    }
    Ok(())
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::StorageIntegrity(format!("search DiskANN: {message}"))
}
