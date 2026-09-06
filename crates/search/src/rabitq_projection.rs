use super::SearchDocument;
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_vector_projection::{
    FileProjection, InMemoryProjection, KernelPreference, ProjectionBuildConfig,
    ProjectionBuildReport, ProjectionBuilder, ProjectionError, ProjectionIdentity,
    ProjectionManifest, ProjectionSearchOptions, ProjectionSearchReport, ProjectionWriter,
    RaBitQBitWidth, DEFAULT_BUILD_MEMORY_BYTES, DEFAULT_SEGMENT_ROWS, DEFAULT_TRANSFORM_SEED,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;

const DEFAULT_RABITQ_SEARCH_MEMORY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaBitQCandidateProjectionBuildOptions {
    pub bit_width: RaBitQBitWidth,
    pub segment_rows: usize,
    pub max_working_bytes: usize,
    pub transform_seed: u64,
}

impl Default for RaBitQCandidateProjectionBuildOptions {
    fn default() -> Self {
        Self {
            bit_width: RaBitQBitWidth::default(),
            segment_rows: DEFAULT_SEGMENT_ROWS,
            max_working_bytes: DEFAULT_BUILD_MEMORY_BYTES,
            transform_seed: DEFAULT_TRANSFORM_SEED,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RaBitQCandidateScanOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: KernelPreference,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl RaBitQCandidateScanOptions<'_> {
    pub fn sequential() -> Self {
        Self {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_RABITQ_SEARCH_MEMORY_BYTES,
            kernel: KernelPreference::Auto,
            task_context: None,
        }
    }
}

impl Default for RaBitQCandidateScanOptions<'_> {
    fn default() -> Self {
        Self::sequential()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RaBitQCandidate {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RaBitQCandidateOutput {
    pub candidates: Vec<RaBitQCandidate>,
    pub report: ProjectionSearchReport,
}

#[derive(Debug)]
pub struct RaBitQCandidateProjection {
    storage: RaBitQCandidateProjectionStorage,
    ordinal_to_document_id: Vec<String>,
    document_to_ordinal: BTreeMap<String, u64>,
    build_report: ProjectionBuildReport,
}

#[derive(Debug)]
enum RaBitQCandidateProjectionStorage {
    InMemory(InMemoryProjection),
    File(FileProjection),
}

#[derive(Debug)]
pub(crate) enum RaBitQCandidateProjectionLoadError {
    Corrupt(SkeinError),
    NotApplicable(SkeinError),
}

impl RaBitQCandidateProjectionLoadError {
    pub(crate) const fn should_quarantine(&self) -> bool {
        matches!(self, Self::Corrupt(_))
    }

    fn into_skein_error(self) -> SkeinError {
        match self {
            Self::Corrupt(error) | Self::NotApplicable(error) => error,
        }
    }
}

struct RaBitQDocumentIdMap {
    dimension: usize,
    ordinal_to_document_id: Vec<String>,
    document_to_ordinal: BTreeMap<String, u64>,
}

impl RaBitQCandidateProjection {
    pub fn build_from_documents(
        documents: &BTreeMap<String, SearchDocument>,
        identity: ProjectionIdentity,
        options: RaBitQCandidateProjectionBuildOptions,
    ) -> Result<Option<Self>> {
        let Some(RaBitQDocumentIdMap {
            dimension,
            ordinal_to_document_id,
            document_to_ordinal,
        }) = validate_and_map_documents(documents)?
        else {
            return Ok(None);
        };
        let config = build_config(dimension, identity, options);
        let mut builder = ProjectionBuilder::new(config).map_err(projection_error)?;
        for (ordinal, document_id) in ordinal_to_document_id.iter().enumerate() {
            let embedding = documents[document_id]
                .embedding
                .as_deref()
                .expect("mapped RaBitQ document has an embedding");
            builder
                .push(ordinal as u64, embedding)
                .map_err(projection_error)?;
        }
        let projection = builder.finish().map_err(projection_error)?;
        let build_report = projection.build_report().clone();
        Ok(Some(Self {
            storage: RaBitQCandidateProjectionStorage::InMemory(projection),
            ordinal_to_document_id,
            document_to_ordinal,
            build_report,
        }))
    }

    pub fn write_from_documents(
        artifact_path: impl AsRef<Path>,
        documents: &BTreeMap<String, SearchDocument>,
        identity: ProjectionIdentity,
        options: RaBitQCandidateProjectionBuildOptions,
    ) -> Result<Option<Self>> {
        let Some(RaBitQDocumentIdMap {
            dimension,
            ordinal_to_document_id,
            document_to_ordinal,
        }) = validate_and_map_documents(documents)?
        else {
            return Ok(None);
        };
        let config = build_config(dimension, identity, options);
        let mut writer =
            ProjectionWriter::create(artifact_path, config).map_err(projection_error)?;
        for (ordinal, document_id) in ordinal_to_document_id.iter().enumerate() {
            let embedding = documents[document_id]
                .embedding
                .as_deref()
                .expect("mapped RaBitQ document has an embedding");
            writer
                .push(ordinal as u64, embedding)
                .map_err(projection_error)?;
        }
        let projection = writer.finish().map_err(projection_error)?;
        let build_report = projection.build_report();
        Ok(Some(Self {
            storage: RaBitQCandidateProjectionStorage::File(projection),
            ordinal_to_document_id,
            document_to_ordinal,
            build_report,
        }))
    }

    pub fn load_from_path(
        artifact_path: impl AsRef<Path>,
        documents: &BTreeMap<String, SearchDocument>,
        expected_identity: &ProjectionIdentity,
    ) -> Result<Self> {
        Self::load_from_path_classified(artifact_path, documents, expected_identity)
            .map_err(RaBitQCandidateProjectionLoadError::into_skein_error)
    }

    pub(crate) fn load_from_path_classified(
        artifact_path: impl AsRef<Path>,
        documents: &BTreeMap<String, SearchDocument>,
        expected_identity: &ProjectionIdentity,
    ) -> std::result::Result<Self, RaBitQCandidateProjectionLoadError> {
        let projection = FileProjection::open(artifact_path).map_err(classify_load_error)?;
        let Some(RaBitQDocumentIdMap {
            dimension,
            ordinal_to_document_id,
            document_to_ordinal,
        }) = validate_and_map_documents(documents)
            .map_err(RaBitQCandidateProjectionLoadError::NotApplicable)?
        else {
            return Err(RaBitQCandidateProjectionLoadError::NotApplicable(
                SkeinError::Storage(
                    "Skein RaBitQ projection exists without vector documents".to_string(),
                ),
            ));
        };
        let manifest = projection.manifest();
        if manifest.dimension != dimension {
            return Err(RaBitQCandidateProjectionLoadError::NotApplicable(
                SkeinError::Storage(format!(
                    "Skein RaBitQ projection dimension {} does not match search dimension {dimension}",
                    manifest.dimension
                )),
            ));
        }
        if &manifest.identity != expected_identity {
            return Err(RaBitQCandidateProjectionLoadError::NotApplicable(
                SkeinError::Storage(format!(
                    "Skein RaBitQ projection identity {:?} does not match expected {:?}",
                    manifest.identity, expected_identity
                )),
            ));
        }
        if manifest.document_count != ordinal_to_document_id.len() {
            return Err(RaBitQCandidateProjectionLoadError::NotApplicable(
                SkeinError::Storage(
                    "Skein RaBitQ projection vector count does not match search documents"
                        .to_string(),
                ),
            ));
        }
        // The canonical search snapshot persists IDs and embeddings. Rebuild
        // its rank-to-ID mapping, then bind it to the artifact's ordered vectors.
        let expected_digest =
            skein_vector_projection::source_digest(ordinal_to_document_id.iter().enumerate().map(
                |(ordinal, document_id)| {
                    (
                        ordinal as u64,
                        documents[document_id]
                            .embedding
                            .as_deref()
                            .expect("mapped RaBitQ document has an embedding"),
                    )
                },
            ));
        if manifest.source_digest != expected_digest {
            return Err(RaBitQCandidateProjectionLoadError::NotApplicable(
                SkeinError::Storage(
                    "Skein RaBitQ projection source digest does not match search documents"
                        .to_string(),
                ),
            ));
        }
        let build_report = projection.build_report();
        Ok(Self {
            storage: RaBitQCandidateProjectionStorage::File(projection),
            ordinal_to_document_id,
            document_to_ordinal,
            build_report,
        })
    }

    pub fn search(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&str]>,
    ) -> Result<RaBitQCandidateOutput> {
        self.search_with_options(
            query_embedding,
            limit,
            allowlist,
            RaBitQCandidateScanOptions::default(),
        )
    }

    pub fn search_with_options(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&str]>,
        options: RaBitQCandidateScanOptions<'_>,
    ) -> Result<RaBitQCandidateOutput> {
        let allowed_ordinals = allowlist.map(|allowed| {
            let mut ids = allowed
                .iter()
                .filter_map(|id| self.document_to_ordinal.get(*id).copied())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            ids
        });
        self.search_with_ordinal_allowlist(query_embedding, limit, allowed_ordinals, options)
    }

    pub(super) fn search_candidates_for_documents_with_options(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&[&SearchDocument]>,
        options: RaBitQCandidateScanOptions<'_>,
    ) -> Result<RaBitQCandidateOutput> {
        let allowed_ordinals = allowlist.map(|allowed| {
            let mut ids = allowed
                .iter()
                .filter_map(|document| self.document_to_ordinal.get(&document.id).copied())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            ids
        });
        self.search_with_ordinal_allowlist(query_embedding, limit, allowed_ordinals, options)
    }

    fn search_with_ordinal_allowlist(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowed_ordinals: Option<Vec<u64>>,
        options: RaBitQCandidateScanOptions<'_>,
    ) -> Result<RaBitQCandidateOutput> {
        let allowlist_bytes = allowed_ordinals.as_ref().map_or(0, |ids| {
            ids.len().saturating_mul(std::mem::size_of::<u64>())
        });
        let scan_working_bytes = options
            .max_working_bytes
            .checked_sub(allowlist_bytes)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "Skein RaBitQ projection allowlist requires {allowlist_bytes} bytes but the search budget is {} bytes",
                    options.max_working_bytes
                ))
            })?;
        let mut scan_options = ProjectionSearchOptions::new()
            .with_max_parallelism(options.max_parallelism)
            .with_max_working_bytes(scan_working_bytes)
            .with_kernel(options.kernel);
        if let Some(allowed) = &allowed_ordinals {
            scan_options = scan_options.with_allowed_ids(allowed);
        }
        if let Some(context) = options.task_context {
            scan_options = scan_options.with_task_context(context);
        }
        let mut output = match &self.storage {
            RaBitQCandidateProjectionStorage::InMemory(projection) => {
                projection.search(query_embedding, limit, scan_options)
            }
            RaBitQCandidateProjectionStorage::File(projection) => {
                projection.search(query_embedding, limit, scan_options)
            }
        }
        .map_err(projection_error)?;
        output.report.admitted_working_bytes = output
            .report
            .admitted_working_bytes
            .saturating_add(allowlist_bytes);
        let candidates = output
            .hits
            .into_iter()
            .map(|hit| {
                let id = usize::try_from(hit.id)
                    .ok()
                    .and_then(|ordinal| self.ordinal_to_document_id.get(ordinal))
                    .cloned()
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "Skein RaBitQ projection returned unknown vector ordinal {}",
                            hit.id
                        ))
                    })?;
                Ok(RaBitQCandidate {
                    id,
                    score: f64::from(hit.score),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RaBitQCandidateOutput {
            candidates,
            report: output.report,
        })
    }

    pub fn manifest(&self) -> &ProjectionManifest {
        match &self.storage {
            RaBitQCandidateProjectionStorage::InMemory(projection) => projection.manifest(),
            RaBitQCandidateProjectionStorage::File(projection) => projection.manifest(),
        }
    }

    pub fn build_report(&self) -> &ProjectionBuildReport {
        &self.build_report
    }

    pub fn is_file_backed(&self) -> bool {
        matches!(self.storage, RaBitQCandidateProjectionStorage::File(_))
    }

    pub fn contains_document_id(&self, document_id: &str) -> bool {
        self.document_to_ordinal.contains_key(document_id)
    }
}

fn validate_and_map_documents(
    documents: &BTreeMap<String, SearchDocument>,
) -> Result<Option<RaBitQDocumentIdMap>> {
    let mut dimension = None;
    let mut ordinal_to_document_id = Vec::new();
    let mut document_to_ordinal = BTreeMap::new();
    // Match the out-of-core generation writer: ascending document IDs, with
    // vectorless documents omitted. Ordinals belong only to this generation.
    for (document_id, document) in documents {
        if document_id != &document.id {
            return Err(SkeinError::Storage(format!(
                "Skein RaBitQ projection document key {document_id:?} does not match id {:?}",
                document.id
            )));
        }
        let Some(embedding) = document.embedding.as_deref() else {
            continue;
        };
        if embedding.is_empty() || !embedding.iter().all(|value| value.is_finite()) {
            return Err(SkeinError::Storage(format!(
                "Skein RaBitQ projection rejected empty or non-finite embedding for {}",
                document.id
            )));
        }
        match dimension {
            Some(existing) if existing != embedding.len() => {
                return Err(SkeinError::Storage(format!(
                    "Skein RaBitQ projection dimension mismatch: expected {existing}, got {}",
                    embedding.len()
                )));
            }
            Some(_) => {}
            None => dimension = Some(embedding.len()),
        }
        let ordinal = u64::try_from(ordinal_to_document_id.len()).map_err(|_| {
            SkeinError::Storage("Skein RaBitQ projection vector ordinal overflow".to_string())
        })?;
        ordinal_to_document_id.push(document.id.clone());
        document_to_ordinal.insert(document.id.clone(), ordinal);
    }
    Ok(dimension.map(|dimension| RaBitQDocumentIdMap {
        dimension,
        ordinal_to_document_id,
        document_to_ordinal,
    }))
}

fn build_config(
    dimension: usize,
    identity: ProjectionIdentity,
    options: RaBitQCandidateProjectionBuildOptions,
) -> ProjectionBuildConfig {
    ProjectionBuildConfig::new(dimension, identity)
        .with_bit_width(options.bit_width)
        .with_segment_rows(options.segment_rows)
        .with_max_working_bytes(options.max_working_bytes)
        .with_transform_seed(options.transform_seed)
}

fn classify_load_error(error: ProjectionError) -> RaBitQCandidateProjectionLoadError {
    let corrupt = matches!(
        &error,
        ProjectionError::CorruptArtifact(_) | ProjectionError::Serialization(_)
    ) || matches!(
        &error,
        ProjectionError::Io(error)
            if matches!(error.kind(), std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof)
    );
    let error = projection_error(error);
    if corrupt {
        RaBitQCandidateProjectionLoadError::Corrupt(error)
    } else {
        RaBitQCandidateProjectionLoadError::NotApplicable(error)
    }
}

fn projection_error(error: skein_vector_projection::ProjectionError) -> SkeinError {
    SkeinError::Storage(format!("Skein RaBitQ projection: {error}"))
}

#[cfg(test)]
mod ordinal_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rabitq_projection_round_trips_string_ids_and_raw_identity() {
        let documents = sample_documents();
        let root = unique_test_dir("roundtrip");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.skein");
        let identity = ProjectionIdentity {
            generation: 1,
            source_epoch: Some(9),
            embedding_model: Some("test-model".to_string()),
            embedding_version: Some("v1".to_string()),
        };
        let written = RaBitQCandidateProjection::write_from_documents(
            &artifact,
            &documents,
            identity.clone(),
            RaBitQCandidateProjectionBuildOptions {
                bit_width: RaBitQBitWidth::One,
                ..RaBitQCandidateProjectionBuildOptions::default()
            },
        )
        .unwrap()
        .unwrap();
        assert!(written.is_file_backed());
        assert_eq!(written.manifest().bit_width, 1);

        let loaded =
            RaBitQCandidateProjection::load_from_path(&artifact, &documents, &identity).unwrap();
        let output = loaded
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(output.candidates[0].id, "memory:a");
        drop(loaded);
        drop(written);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_allowlist_is_charged_to_the_search_budget() {
        let projection = RaBitQCandidateProjection::build_from_documents(
            &sample_documents(),
            ProjectionIdentity::new(1),
            RaBitQCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap();
        let allowed = ["memory:a", "memory:b"];
        let result = projection.search_with_options(
            &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            1,
            Some(&allowed),
            RaBitQCandidateScanOptions {
                max_working_bytes: std::mem::size_of::<u64>(),
                ..RaBitQCandidateScanOptions::default()
            },
        );

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("allowlist requires 16 bytes"));
    }

    #[test]
    fn unfiltered_candidate_scan_does_not_materialize_an_allowlist() {
        let projection = RaBitQCandidateProjection::build_from_documents(
            &sample_documents(),
            ProjectionIdentity::new(1),
            RaBitQCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap();
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let baseline = projection.search(&query, 1, None).unwrap();
        let exact_budget = baseline.report.admitted_working_bytes;

        projection
            .search_with_options(
                &query,
                1,
                None,
                RaBitQCandidateScanOptions {
                    max_working_bytes: exact_budget,
                    ..RaBitQCandidateScanOptions::default()
                },
            )
            .unwrap();
        let all_documents = ["memory:a", "memory:b"];
        let filtered = projection.search_with_options(
            &query,
            1,
            Some(&all_documents),
            RaBitQCandidateScanOptions {
                max_working_bytes: exact_budget,
                ..RaBitQCandidateScanOptions::default()
            },
        );
        assert!(filtered
            .unwrap_err()
            .to_string()
            .contains("resource budget exceeded"));
    }

    fn sample_documents() -> BTreeMap<String, SearchDocument> {
        [
            ("memory:a", vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ("memory:b", vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ]
        .into_iter()
        .map(|(id, embedding)| {
            (
                id.to_string(),
                SearchDocument {
                    id: id.to_string(),
                    title: id.to_string(),
                    content: String::new(),
                    embedding: Some(embedding),
                    metadata: BTreeMap::new(),
                },
            )
        })
        .collect()
    }

    pub(super) fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_rabitq_candidate_projection_{name}_{}_{nanos}",
            std::process::id()
        ))
    }
}
