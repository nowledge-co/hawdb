use super::super::RelationalOverflowReferenceSet;
use super::super::{decode_overflow_envelope, RelationalHydrationBudget, RelationalOverflowRef};
use super::{
    durability, manifest, reader, relational_overflow_descriptor_file,
    relational_overflow_extent_file, relational_overflow_manifest_generation_file,
    RelationalOverflowArtifactMetadata, RelationalOverflowExactGenerationRequest,
    RelationalOverflowExactPublicationReport, RelationalOverflowExtentDescriptor,
    RelationalOverflowExtentInput, RelationalOverflowPublicationConfig,
    RelationalOverflowPublicationError, RelationalOverflowPublicationPhase,
    RelationalOverflowPublicationReport, RelationalOverflowRootManifest,
    RelationalOverflowRootReader, CANDIDATE_PUBLICATION_TRACE, COMPLETE_PUBLICATION_TRACE,
    RELATIONAL_OVERFLOW_MANIFEST_FILE, RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE,
};
use crate::relational::RelationalError;
use crate::{durable_replace_file, sync_directory};
use skein_integrity::{integrity_digest, IntegrityHasher};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct RelationalOverflowPublisher {
    config: RelationalOverflowPublicationConfig,
}

impl RelationalOverflowPublisher {
    pub const fn new(config: RelationalOverflowPublicationConfig) -> Self {
        Self { config }
    }

    pub fn publish(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        extents: Vec<RelationalOverflowExtentInput>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        self.publish_inner(
            directory,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            extents,
            None,
        )
    }

    /// Persists one immutable generation without changing the independent
    /// latest selector. The caller must bind the returned artifacts into a
    /// stronger publish-last authority, such as the canonical checkpoint
    /// manifest, before the generation can serve production reads.
    pub fn persist_generation(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        base: Option<&RelationalOverflowRootReader>,
        expected_previous_generation: Option<u64>,
        extents: Vec<RelationalOverflowExtentInput>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        self.persist_generation_inner(
            GenerationPublication {
                directory,
                generation,
                source_commit_epoch,
                base,
                expected_previous_generation,
                retain_unmentioned_base: false,
                select_latest: false,
                stop_after: None,
            },
            extents,
        )
    }

    /// Persists a bounded delta while conservatively retaining every extent
    /// reachable from the pinned base generation.
    ///
    /// Metadata-only row serving cannot prove the exact overflow closure
    /// without scanning the whole row root. This mode keeps that checkpoint
    /// operation access-proportional: newly observed extents are merged into
    /// the base descriptor stream, while exact garbage collection remains a
    /// separate full-scan maintenance operation.
    pub fn persist_generation_retaining_base(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        base: &RelationalOverflowRootReader,
        expected_previous_generation: u64,
        extents: Vec<RelationalOverflowExtentInput>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        self.persist_generation_inner(
            GenerationPublication {
                directory,
                generation,
                source_commit_epoch,
                base: Some(base),
                expected_previous_generation: Some(expected_previous_generation),
                retain_unmentioned_base: true,
                select_latest: false,
                stop_after: None,
            },
            extents,
        )
    }

    /// Persists exactly the overflow references found by a bounded full-row
    /// closure scan. Unmentioned base extents are intentionally omitted.
    ///
    /// `resolve_new` is consulted only for references absent from the pinned
    /// base. It must be repeatable because admission is completed before any
    /// candidate artifact is created, then the same sorted source is replayed
    /// to write the candidate.
    pub fn persist_generation_exact_references(
        &self,
        request: RelationalOverflowExactGenerationRequest<'_>,
        mut resolve_new: impl FnMut(
            &RelationalOverflowRef,
        ) -> Result<
            Option<std::sync::Arc<[u8]>>,
            RelationalOverflowPublicationError,
        >,
    ) -> Result<RelationalOverflowExactPublicationReport, RelationalOverflowPublicationError> {
        let RelationalOverflowExactGenerationRequest {
            directory,
            generation,
            source_commit_epoch,
            base,
            expected_previous_generation,
            references,
            task,
        } = request;
        validate_publication_identity(
            generation,
            source_commit_epoch,
            references.report().unique_references == 0,
        )?;
        validate_publication_config(self.config)?;
        fs::create_dir_all(directory).map_err(durability("create overflow directory"))?;
        let _lock = acquire_publication_lock(directory)?;
        let paths = PublicationPaths::new(directory, generation);
        paths.remove_temps()?;
        paths.require_fresh_generation()?;
        validate_base_identity(
            generation,
            source_commit_epoch,
            Some(base),
            Some(expected_previous_generation),
            false,
        )?;
        let preflight =
            preflight_exact_references(base, references, &mut resolve_new, self.config, task)?;
        task.checkpoint()
            .map_err(RelationalOverflowPublicationError::Stopped)?;

        let result = (|| {
            maybe_stop(None, RelationalOverflowPublicationPhase::CandidateStarted)?;
            let artifacts = write_exact_artifacts(ExactArtifactWrite {
                extent_path: &paths.extent_tmp,
                descriptor_path: &paths.descriptor_tmp,
                base,
                references,
                resolve_new: &mut resolve_new,
                generation,
                config: self.config,
                task,
            })?;
            task.checkpoint()
                .map_err(RelationalOverflowPublicationError::Stopped)?;
            self.finish_candidate_publication(PublicationCommit {
                paths: &paths,
                generation,
                source_commit_epoch,
                expected_previous_generation: Some(expected_previous_generation),
                artifacts,
                select_latest: false,
                stop_after: None,
            })
        })();
        // Success has durably renamed every candidate; only failure leaves temporary files.
        if result.is_err() {
            let _ = paths.remove_temps();
        }
        result.map(|publication| RelationalOverflowExactPublicationReport {
            publication,
            copied_base_extent_count: preflight.copied_base_extent_count,
            introduced_extent_count: preflight.introduced_extent_count,
        })
    }

    pub(super) fn publish_inner(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        extents: Vec<RelationalOverflowExtentInput>,
        stop_after: Option<RelationalOverflowPublicationPhase>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        let base = RelationalOverflowRootReader::open_latest(directory, self.config)?;
        self.persist_generation_inner(
            GenerationPublication {
                directory,
                generation,
                source_commit_epoch,
                base: base.as_ref(),
                expected_previous_generation,
                retain_unmentioned_base: false,
                select_latest: true,
                stop_after,
            },
            extents,
        )
    }

    fn persist_generation_inner(
        &self,
        publication: GenerationPublication<'_>,
        extents: Vec<RelationalOverflowExtentInput>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        let GenerationPublication {
            directory,
            generation,
            source_commit_epoch,
            base,
            expected_previous_generation,
            retain_unmentioned_base,
            select_latest,
            stop_after,
        } = publication;
        validate_publication_identity(generation, source_commit_epoch, extents.is_empty())?;
        validate_publication_config(self.config)?;
        let extents = preflight_inputs(extents, self.config)?;
        fs::create_dir_all(directory).map_err(durability("create overflow directory"))?;
        let _lock = acquire_publication_lock(directory)?;
        let paths = PublicationPaths::new(directory, generation);
        paths.remove_temps()?;
        paths.require_fresh_generation()?;

        validate_base_identity(
            generation,
            source_commit_epoch,
            base,
            expected_previous_generation,
            select_latest,
        )?;
        preflight_root_capacity(base, &extents, retain_unmentioned_base, self.config)?;

        let result = self.build_and_publish(PublicationBuild {
            paths: &paths,
            base,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            extents: &extents,
            retain_unmentioned_base,
            select_latest,
            stop_after,
        });
        // Success has durably renamed every candidate; only failure leaves temporary files.
        if result.is_err() {
            let _ = paths.remove_temps();
        }
        result
    }

    fn build_and_publish(
        &self,
        build: PublicationBuild<'_>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        maybe_stop(
            build.stop_after,
            RelationalOverflowPublicationPhase::CandidateStarted,
        )?;
        let artifacts = write_artifacts(
            &build.paths.extent_tmp,
            &build.paths.descriptor_tmp,
            build.base,
            build.extents,
            build.retain_unmentioned_base,
            build.generation,
            self.config,
        )?;
        self.finish_candidate_publication(PublicationCommit {
            paths: build.paths,
            generation: build.generation,
            source_commit_epoch: build.source_commit_epoch,
            expected_previous_generation: build.expected_previous_generation,
            artifacts,
            select_latest: build.select_latest,
            stop_after: build.stop_after,
        })
    }

    fn finish_candidate_publication(
        &self,
        build: PublicationCommit<'_>,
    ) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
        let PublicationCommit {
            paths,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            artifacts,
            select_latest,
            stop_after,
        } = build;
        let manifest = RelationalOverflowRootManifest {
            generation,
            source_commit_epoch,
            previous_generation: expected_previous_generation,
            extent_count: artifacts.extent_count,
            new_extent_count: artifacts.new_extent_count,
            extent_artifact: artifacts.extent_artifact,
            descriptor_artifact: artifacts.descriptor_artifact,
            root_set_digest: artifacts.root_set_digest,
        };
        let encoded_manifest = manifest::encode_manifest(&manifest, self.config)?;
        write_synced(&paths.generation_manifest_tmp, &encoded_manifest)?;

        durable_publish_immutable(&paths.extent_tmp, &paths.extent)?;
        maybe_stop(
            stop_after,
            RelationalOverflowPublicationPhase::CandidateExtentsDurable,
        )?;
        durable_publish_immutable(&paths.descriptor_tmp, &paths.descriptor)?;
        maybe_stop(
            stop_after,
            RelationalOverflowPublicationPhase::CandidateRootDurable,
        )?;
        durable_publish_immutable(&paths.generation_manifest_tmp, &paths.generation_manifest)?;
        maybe_stop(
            stop_after,
            RelationalOverflowPublicationPhase::CandidateManifestDurable,
        )?;

        if select_latest {
            let actual_previous =
                manifest::read_manifest_if_exists(&paths.latest_manifest, self.config)?
                    .map(|manifest| manifest.generation);
            if actual_previous != expected_previous_generation {
                return Err(RelationalOverflowPublicationError::StaleGeneration {
                    expected_previous: expected_previous_generation,
                    actual_previous,
                });
            }
        }
        maybe_stop(
            stop_after,
            RelationalOverflowPublicationPhase::BaseRevalidated,
        )?;
        if select_latest {
            write_synced(&paths.latest_manifest_tmp, &encoded_manifest)?;
            durable_replace_file(&paths.latest_manifest_tmp, &paths.latest_manifest)
                .map_err(durability("publish latest overflow manifest"))?;
        }

        let manifest_digest = integrity_digest(&encoded_manifest);

        Ok(RelationalOverflowPublicationReport {
            generation,
            source_commit_epoch,
            extent_count: manifest.extent_count,
            new_extent_count: manifest.new_extent_count,
            reused_extent_count: artifacts.reused_extent_count,
            extent_artifact_bytes: manifest.extent_artifact.encoded_len,
            descriptor_artifact_bytes: manifest.descriptor_artifact.encoded_len,
            manifest_bytes: encoded_manifest.len() as u64,
            generation_artifacts: super::RelationalOverflowGenerationArtifacts {
                generation: manifest.generation,
                source_commit_epoch: manifest.source_commit_epoch,
                root_set_digest: manifest.root_set_digest,
                manifest_artifact: RelationalOverflowArtifactMetadata {
                    encoded_len: encoded_manifest.len() as u64,
                    encoded_crc32c: manifest_digest.crc32c.get(),
                    encoded_sha256: manifest_digest.sha256,
                },
            },
            events: if select_latest {
                COMPLETE_PUBLICATION_TRACE
            } else {
                CANDIDATE_PUBLICATION_TRACE
            },
        })
    }
}

struct PublicationCommit<'a> {
    paths: &'a PublicationPaths,
    generation: u64,
    source_commit_epoch: u64,
    expected_previous_generation: Option<u64>,
    artifacts: WrittenArtifacts,
    select_latest: bool,
    stop_after: Option<RelationalOverflowPublicationPhase>,
}

struct GenerationPublication<'a> {
    directory: &'a Path,
    generation: u64,
    source_commit_epoch: u64,
    base: Option<&'a RelationalOverflowRootReader>,
    expected_previous_generation: Option<u64>,
    retain_unmentioned_base: bool,
    select_latest: bool,
    stop_after: Option<RelationalOverflowPublicationPhase>,
}

struct PublicationBuild<'a> {
    paths: &'a PublicationPaths,
    base: Option<&'a RelationalOverflowRootReader>,
    generation: u64,
    source_commit_epoch: u64,
    expected_previous_generation: Option<u64>,
    extents: &'a [RelationalOverflowExtentInput],
    retain_unmentioned_base: bool,
    select_latest: bool,
    stop_after: Option<RelationalOverflowPublicationPhase>,
}

struct WrittenArtifacts {
    extent_artifact: RelationalOverflowArtifactMetadata,
    descriptor_artifact: RelationalOverflowArtifactMetadata,
    root_set_digest: skein_integrity::Sha256Digest,
    extent_count: u64,
    new_extent_count: u64,
    reused_extent_count: u64,
}

fn validate_base_identity(
    generation: u64,
    source_commit_epoch: u64,
    base: Option<&RelationalOverflowRootReader>,
    expected_previous_generation: Option<u64>,
    select_latest: bool,
) -> Result<(), RelationalOverflowPublicationError> {
    let base_generation = base.map(|reader| reader.manifest().generation);
    if (select_latest || base.is_some()) && base_generation != expected_previous_generation {
        return Err(RelationalOverflowPublicationError::StaleGeneration {
            expected_previous: expected_previous_generation,
            actual_previous: base_generation,
        });
    }
    if let Some(previous) = expected_previous_generation
        && generation <= previous
    {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "new overflow generation {generation} must exceed previous generation {previous}"
        )));
    }
    if let Some(base) = base {
        if generation <= base.manifest().generation {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "new overflow generation {generation} must exceed published generation {}",
                base.manifest().generation
            )));
        }
        if source_commit_epoch < base.manifest().source_commit_epoch {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow source epoch {source_commit_epoch} precedes published epoch {}",
                base.manifest().source_commit_epoch
            )));
        }
    }
    Ok(())
}

fn preflight_exact_references(
    base: &RelationalOverflowRootReader,
    references: &RelationalOverflowReferenceSet,
    resolve_new: &mut impl FnMut(
        &RelationalOverflowRef,
    ) -> Result<
        Option<std::sync::Arc<[u8]>>,
        RelationalOverflowPublicationError,
    >,
    config: RelationalOverflowPublicationConfig,
    task: &skein_core::RuntimeTaskContext,
) -> Result<ExactReferencePreflight, RelationalOverflowPublicationError> {
    let extent_count = references.report().unique_references;
    if extent_count > config.max_extents.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "exact overflow root contains {extent_count} extents, exceeding limit {}",
            config.max_extents
        )));
    }
    let descriptor_bytes = extent_count
        .checked_mul(reader::DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "exact overflow descriptor byte count overflow".to_string(),
            )
        })?;
    if descriptor_bytes > config.max_descriptor_bytes.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "exact overflow root requires {descriptor_bytes} descriptor bytes, exceeding limit {}",
            config.max_descriptor_bytes
        )));
    }

    let mut base_file = File::open(base.descriptor_path())
        .map_err(durability("open base overflow descriptor artifact"))?;
    let mut base_ordinal = 0u64;
    let mut base_descriptor = if base.manifest().extent_count == 0 {
        None
    } else {
        Some(base.read_descriptor_from(&mut base_file, 0)?)
    };
    let mut new_extent_bytes = 0u64;
    let mut copied_base_extent_count = 0u64;
    let mut introduced_extent_count = 0u64;
    references.visit(&mut |reference| {
        task.checkpoint()
            .map_err(RelationalOverflowPublicationError::Stopped)?;
        while base_descriptor
            .as_ref()
            .is_some_and(|descriptor| descriptor.reference.digest < reference.digest)
        {
            advance_base_descriptor(
                base,
                &mut base_file,
                &mut base_ordinal,
                &mut base_descriptor,
            )?;
        }
        if let Some(existing) = base_descriptor
            .filter(|descriptor| descriptor.reference.digest == reference.digest)
        {
            if existing.reference != reference {
                return Err(RelationalOverflowPublicationError::Corrupt(format!(
                    "base overflow descriptor {} metadata differs from its content identity",
                    reference.digest
                )));
            }
            copied_base_extent_count = copied_base_extent_count.checked_add(1).ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "copied base overflow extent count overflow".to_string(),
                )
            })?;
            new_extent_bytes = new_extent_bytes
                .checked_add(existing.envelope_bytes)
                .ok_or_else(|| {
                    RelationalOverflowPublicationError::Admission(
                        "exact overflow extent byte count overflow".to_string(),
                    )
                })?;
        } else {
            let encoded = resolve_new(&reference)?.ok_or(
                RelationalOverflowPublicationError::MissingExtent(reference.digest),
            )?;
            validate_encoded_extent(&reference, &encoded, config)?;
            introduced_extent_count = introduced_extent_count.checked_add(1).ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "introduced overflow extent count overflow".to_string(),
                )
            })?;
            new_extent_bytes = new_extent_bytes
                .checked_add(encoded.len() as u64)
                .ok_or_else(|| {
                    RelationalOverflowPublicationError::Admission(
                        "exact overflow extent byte count overflow".to_string(),
                    )
                })?;
        }
        if new_extent_bytes > config.max_new_extent_bytes.get() {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "exact overflow root needs {new_extent_bytes} rewritten extent bytes, exceeding limit {}",
                config.max_new_extent_bytes
            )));
        }
        Ok(true)
    })?;
    Ok(ExactReferencePreflight {
        copied_base_extent_count,
        introduced_extent_count,
    })
}

type NewExtentResolver<'a> = dyn FnMut(
        &RelationalOverflowRef,
    ) -> Result<Option<std::sync::Arc<[u8]>>, RelationalOverflowPublicationError>
    + 'a;

struct ExactArtifactWrite<'a> {
    extent_path: &'a Path,
    descriptor_path: &'a Path,
    base: &'a RelationalOverflowRootReader,
    references: &'a RelationalOverflowReferenceSet,
    resolve_new: &'a mut NewExtentResolver<'a>,
    generation: u64,
    config: RelationalOverflowPublicationConfig,
    task: &'a skein_core::RuntimeTaskContext,
}

fn write_exact_artifacts(
    request: ExactArtifactWrite<'_>,
) -> Result<WrittenArtifacts, RelationalOverflowPublicationError> {
    let ExactArtifactWrite {
        extent_path,
        descriptor_path,
        base,
        references,
        resolve_new,
        generation,
        config,
        task,
    } = request;
    let mut writer = ArtifactWriter::new(extent_path, descriptor_path, generation, config)?;
    let mut base_file = File::open(base.descriptor_path())
        .map_err(durability("open base overflow descriptor artifact"))?;
    let mut base_ordinal = 0u64;
    let mut base_descriptor = if base.manifest().extent_count == 0 {
        None
    } else {
        Some(base.read_descriptor_from(&mut base_file, 0)?)
    };
    references.visit(&mut |reference| {
        task.checkpoint()
            .map_err(RelationalOverflowPublicationError::Stopped)?;
        while base_descriptor
            .as_ref()
            .is_some_and(|descriptor| descriptor.reference.digest < reference.digest)
        {
            advance_base_descriptor(
                base,
                &mut base_file,
                &mut base_ordinal,
                &mut base_descriptor,
            )?;
        }
        if let Some(existing) =
            base_descriptor.filter(|descriptor| descriptor.reference.digest == reference.digest)
        {
            if existing.reference != reference {
                return Err(RelationalOverflowPublicationError::Corrupt(format!(
                    "base overflow descriptor {} changed after exact preflight",
                    reference.digest
                )));
            }
            let encoded = base.read_encoded_extent(&existing)?;
            writer.emit_input(&RelationalOverflowExtentInput::Write { reference, encoded })?;
        } else {
            let encoded = resolve_new(&reference)?.ok_or(
                RelationalOverflowPublicationError::MissingExtent(reference.digest),
            )?;
            writer.emit_input(&RelationalOverflowExtentInput::Write { reference, encoded })?;
        }
        Ok(true)
    })?;
    writer.finish()
}

struct ExactReferencePreflight {
    copied_base_extent_count: u64,
    introduced_extent_count: u64,
}

fn write_artifacts(
    extent_path: &Path,
    descriptor_path: &Path,
    base: Option<&RelationalOverflowRootReader>,
    inputs: &[RelationalOverflowExtentInput],
    retain_unmentioned_base: bool,
    generation: u64,
    config: RelationalOverflowPublicationConfig,
) -> Result<WrittenArtifacts, RelationalOverflowPublicationError> {
    let mut writer = ArtifactWriter::new(extent_path, descriptor_path, generation, config)?;
    let mut base_file = base
        .map(|reader| {
            File::open(reader.descriptor_path())
                .map_err(durability("open base overflow descriptor artifact"))
        })
        .transpose()?;
    let mut base_ordinal = 0u64;
    let mut base_descriptor = base
        .filter(|reader| reader.manifest().extent_count > 0)
        .map(|reader| {
            reader
                .read_descriptor_from(base_file.as_mut().expect("base descriptor file is open"), 0)
        })
        .transpose()?;
    let mut input_ordinal = 0usize;
    while input_ordinal < inputs.len() || base_descriptor.is_some() {
        match (inputs.get(input_ordinal), base_descriptor) {
            (Some(input), Some(existing)) => {
                match input.reference().digest.cmp(&existing.reference.digest) {
                    std::cmp::Ordering::Less => {
                        writer.emit_input(input)?;
                        input_ordinal += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        validate_matching_input(input, &existing, config)?;
                        writer.emit_reused(existing)?;
                        input_ordinal += 1;
                        advance_base_descriptor(
                            base.expect("base descriptor exists only with a base reader"),
                            base_file.as_mut().expect("base descriptor file is open"),
                            &mut base_ordinal,
                            &mut base_descriptor,
                        )?;
                    }
                    std::cmp::Ordering::Greater => {
                        if retain_unmentioned_base {
                            writer.emit_reused(existing)?;
                        }
                        advance_base_descriptor(
                            base.expect("base descriptor exists only with a base reader"),
                            base_file.as_mut().expect("base descriptor file is open"),
                            &mut base_ordinal,
                            &mut base_descriptor,
                        )?;
                    }
                }
            }
            (Some(input), None) => {
                writer.emit_input(input)?;
                input_ordinal += 1;
            }
            (None, Some(existing)) => {
                if !retain_unmentioned_base {
                    break;
                }
                writer.emit_reused(existing)?;
                advance_base_descriptor(
                    base.expect("base descriptor exists only with a base reader"),
                    base_file.as_mut().expect("base descriptor file is open"),
                    &mut base_ordinal,
                    &mut base_descriptor,
                )?;
            }
            (None, None) => break,
        }
    }
    writer.finish()
}

struct ArtifactWriter {
    extent_file: File,
    descriptor_file: File,
    extent_hasher: IntegrityHasher,
    descriptor_hasher: IntegrityHasher,
    root_hasher: IntegrityHasher,
    generation: u64,
    config: RelationalOverflowPublicationConfig,
    extent_bytes: u64,
    extent_count: u64,
    new_extent_count: u64,
    reused_extent_count: u64,
}

impl ArtifactWriter {
    fn new(
        extent_path: &Path,
        descriptor_path: &Path,
        generation: u64,
        config: RelationalOverflowPublicationConfig,
    ) -> Result<Self, RelationalOverflowPublicationError> {
        Ok(Self {
            extent_file: File::create(extent_path)
                .map_err(durability("create overflow extent candidate"))?,
            descriptor_file: File::create(descriptor_path)
                .map_err(durability("create overflow descriptor candidate"))?,
            extent_hasher: IntegrityHasher::new(),
            descriptor_hasher: IntegrityHasher::new(),
            root_hasher: IntegrityHasher::new(),
            generation,
            config,
            extent_bytes: 0,
            extent_count: 0,
            new_extent_count: 0,
            reused_extent_count: 0,
        })
    }

    fn emit_input(
        &mut self,
        input: &RelationalOverflowExtentInput,
    ) -> Result<(), RelationalOverflowPublicationError> {
        match input {
            RelationalOverflowExtentInput::Reuse(reference) => Err(
                RelationalOverflowPublicationError::MissingExtent(reference.digest),
            ),
            RelationalOverflowExtentInput::Write { reference, encoded } => {
                validate_encoded_extent(reference, encoded, self.config)?;
                let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
                    RelationalOverflowPublicationError::Admission(
                        "overflow envelope length does not fit u64".to_string(),
                    )
                })?;
                let next_extent_bytes =
                    self.extent_bytes.checked_add(encoded_len).ok_or_else(|| {
                        RelationalOverflowPublicationError::Admission(
                            "overflow extent artifact length overflow".to_string(),
                        )
                    })?;
                if next_extent_bytes > self.config.max_new_extent_bytes.get() {
                    return Err(RelationalOverflowPublicationError::Admission(format!(
                        "overflow extent artifact requires {next_extent_bytes} bytes, exceeding limit {}",
                        self.config.max_new_extent_bytes
                    )));
                }
                self.extent_file
                    .write_all(encoded)
                    .map_err(durability("write overflow extent candidate"))?;
                self.extent_hasher.update(encoded);
                let digest = integrity_digest(encoded);
                let descriptor = RelationalOverflowExtentDescriptor {
                    reference: *reference,
                    physical_generation: self.generation,
                    physical_offset: self.extent_bytes,
                    envelope_bytes: encoded_len,
                    envelope_crc32c: digest.crc32c.get(),
                };
                self.extent_bytes = next_extent_bytes;
                self.new_extent_count = self.new_extent_count.checked_add(1).ok_or_else(|| {
                    RelationalOverflowPublicationError::Admission(
                        "new overflow extent count overflow".to_string(),
                    )
                })?;
                self.emit_descriptor(descriptor)
            }
        }
    }

    fn emit_reused(
        &mut self,
        descriptor: RelationalOverflowExtentDescriptor,
    ) -> Result<(), RelationalOverflowPublicationError> {
        self.reused_extent_count = self.reused_extent_count.checked_add(1).ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "reused overflow extent count overflow".to_string(),
            )
        })?;
        self.emit_descriptor(descriptor)
    }

    fn emit_descriptor(
        &mut self,
        descriptor: RelationalOverflowExtentDescriptor,
    ) -> Result<(), RelationalOverflowPublicationError> {
        let encoded_descriptor =
            reader::encode_descriptor(descriptor, self.generation, self.extent_count, self.config)?;
        self.descriptor_file
            .write_all(&encoded_descriptor)
            .map_err(durability("write overflow descriptor candidate"))?;
        self.descriptor_hasher.update(&encoded_descriptor);
        self.root_hasher.update(&encoded_descriptor[..88]);
        self.extent_count = self.extent_count.checked_add(1).ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow extent count overflow".to_string(),
            )
        })?;
        Ok(())
    }

    fn finish(self) -> Result<WrittenArtifacts, RelationalOverflowPublicationError> {
        self.extent_file
            .sync_all()
            .map_err(durability("sync overflow extent candidate"))?;
        self.descriptor_file
            .sync_all()
            .map_err(durability("sync overflow descriptor candidate"))?;
        let descriptor_bytes = self
            .extent_count
            .checked_mul(reader::DESCRIPTOR_BYTES as u64)
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow descriptor artifact length overflow".to_string(),
                )
            })?;
        let extent_digest = self.extent_hasher.finish();
        let descriptor_digest = self.descriptor_hasher.finish();
        Ok(WrittenArtifacts {
            extent_artifact: RelationalOverflowArtifactMetadata {
                encoded_len: self.extent_bytes,
                encoded_crc32c: extent_digest.crc32c.get(),
                encoded_sha256: extent_digest.sha256,
            },
            descriptor_artifact: RelationalOverflowArtifactMetadata {
                encoded_len: descriptor_bytes,
                encoded_crc32c: descriptor_digest.crc32c.get(),
                encoded_sha256: descriptor_digest.sha256,
            },
            root_set_digest: self.root_hasher.finish().sha256,
            extent_count: self.extent_count,
            new_extent_count: self.new_extent_count,
            reused_extent_count: self.reused_extent_count,
        })
    }
}

fn validate_matching_input(
    input: &RelationalOverflowExtentInput,
    existing: &RelationalOverflowExtentDescriptor,
    config: RelationalOverflowPublicationConfig,
) -> Result<(), RelationalOverflowPublicationError> {
    if existing.reference != *input.reference() {
        return Err(RelationalOverflowPublicationError::Corrupt(format!(
            "base overflow descriptor {} metadata differs from its content identity",
            input.reference().digest
        )));
    }
    if let RelationalOverflowExtentInput::Write { reference, encoded } = input {
        validate_encoded_extent(reference, encoded, config)?;
    }
    Ok(())
}

fn advance_base_descriptor(
    base: &RelationalOverflowRootReader,
    file: &mut File,
    ordinal: &mut u64,
    descriptor: &mut Option<RelationalOverflowExtentDescriptor>,
) -> Result<(), RelationalOverflowPublicationError> {
    *ordinal = ordinal.checked_add(1).ok_or_else(|| {
        RelationalOverflowPublicationError::Corrupt(
            "base overflow descriptor ordinal overflow".to_string(),
        )
    })?;
    *descriptor = if *ordinal < base.manifest().extent_count {
        Some(base.read_descriptor_from(file, *ordinal)?)
    } else {
        None
    };
    Ok(())
}

fn validate_encoded_extent(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
    config: RelationalOverflowPublicationConfig,
) -> Result<(), RelationalOverflowPublicationError> {
    if reference.compressed_bytes > config.max_value_bytes.get() as u64
        || reference.uncompressed_bytes > config.max_value_bytes.get() as u64
    {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow extent {} exceeds value limit {}",
            reference.digest, config.max_value_bytes
        )));
    }
    let mut budget = RelationalHydrationBudget {
        max_rows: 1,
        max_compressed_bytes: config.max_value_bytes.get(),
        max_decompressed_bytes: config.max_value_bytes.get(),
        max_memory_bytes: config.max_value_bytes.get(),
        ..RelationalHydrationBudget::default()
    };
    decode_overflow_envelope(reference, encoded, &mut budget, None)
        .map(|_| ())
        .map_err(|error| match error {
            RelationalError::Admission(message) => {
                RelationalOverflowPublicationError::Admission(message)
            }
            error => RelationalOverflowPublicationError::Corrupt(error.to_string()),
        })
}

fn preflight_inputs(
    mut inputs: Vec<RelationalOverflowExtentInput>,
    config: RelationalOverflowPublicationConfig,
) -> Result<Vec<RelationalOverflowExtentInput>, RelationalOverflowPublicationError> {
    let input_count = u64::try_from(inputs.len()).map_err(|_| {
        RelationalOverflowPublicationError::Admission(
            "overflow extent count does not fit u64".to_string(),
        )
    })?;
    if input_count > config.max_extents.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow publication contains {input_count} extents, exceeding limit {}",
            config.max_extents
        )));
    }
    let descriptor_bytes = input_count
        .checked_mul(reader::DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow descriptor byte count overflow".to_string(),
            )
        })?;
    if descriptor_bytes > config.max_descriptor_bytes.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow descriptor artifact requires {descriptor_bytes} bytes, exceeding limit {}",
            config.max_descriptor_bytes
        )));
    }
    inputs.sort_by_key(|input| input.reference().digest);
    if let Some(duplicate) = inputs
        .windows(2)
        .find(|pair| pair[0].reference().digest == pair[1].reference().digest)
    {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow publication contains duplicate digest {}",
            duplicate[0].reference().digest
        )));
    }
    Ok(inputs)
}

fn preflight_root_capacity(
    base: Option<&RelationalOverflowRootReader>,
    inputs: &[RelationalOverflowExtentInput],
    retain_unmentioned_base: bool,
    config: RelationalOverflowPublicationConfig,
) -> Result<(), RelationalOverflowPublicationError> {
    let mut base_file = base
        .map(|reader| {
            File::open(reader.descriptor_path())
                .map_err(durability("open base overflow descriptor artifact"))
        })
        .transpose()?;
    let mut base_ordinal = 0u64;
    let mut base_descriptor = base
        .filter(|reader| reader.manifest().extent_count > 0)
        .map(|reader| {
            reader
                .read_descriptor_from(base_file.as_mut().expect("base descriptor file is open"), 0)
        })
        .transpose()?;
    let mut new_extent_bytes = 0u64;
    let mut retained_root_extent_count = if retain_unmentioned_base {
        base.map_or(0, |reader| reader.manifest().extent_count)
    } else {
        0
    };
    for input in inputs {
        if let RelationalOverflowExtentInput::Write { reference, encoded } = input {
            validate_encoded_extent(reference, encoded, config)?;
        }
        while base_descriptor
            .as_ref()
            .is_some_and(|descriptor| descriptor.reference.digest < input.reference().digest)
        {
            advance_base_descriptor(
                base.expect("base descriptor exists only with a base reader"),
                base_file.as_mut().expect("base descriptor file is open"),
                &mut base_ordinal,
                &mut base_descriptor,
            )?;
        }
        if let Some(existing) = base_descriptor
            .filter(|descriptor| descriptor.reference.digest == input.reference().digest)
        {
            if existing.reference != *input.reference() {
                return Err(RelationalOverflowPublicationError::Corrupt(format!(
                    "base overflow descriptor {} metadata differs from its content identity",
                    input.reference().digest
                )));
            }
        } else {
            if retain_unmentioned_base {
                retained_root_extent_count =
                    retained_root_extent_count.checked_add(1).ok_or_else(|| {
                        RelationalOverflowPublicationError::Admission(
                            "overflow root extent count overflow".to_string(),
                        )
                    })?;
            }
            match input {
                RelationalOverflowExtentInput::Reuse(reference) => {
                    return Err(RelationalOverflowPublicationError::MissingExtent(
                        reference.digest,
                    ));
                }
                RelationalOverflowExtentInput::Write { encoded, .. } => {
                    let encoded_bytes = u64::try_from(encoded.len()).map_err(|_| {
                        RelationalOverflowPublicationError::Admission(
                            "overflow envelope length does not fit u64".to_string(),
                        )
                    })?;
                    new_extent_bytes =
                        new_extent_bytes.checked_add(encoded_bytes).ok_or_else(|| {
                            RelationalOverflowPublicationError::Admission(
                                "overflow extent artifact length overflow".to_string(),
                            )
                        })?;
                }
            }
        }
    }
    if new_extent_bytes > config.max_new_extent_bytes.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow extent artifact requires {new_extent_bytes} bytes, exceeding limit {}",
            config.max_new_extent_bytes
        )));
    }
    let root_extent_count = if retain_unmentioned_base {
        retained_root_extent_count
    } else {
        u64::try_from(inputs.len()).map_err(|_| {
            RelationalOverflowPublicationError::Admission(
                "overflow root extent count does not fit u64".to_string(),
            )
        })?
    };
    if root_extent_count > config.max_extents.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow root contains {root_extent_count} extents, exceeding limit {}",
            config.max_extents
        )));
    }
    let descriptor_bytes = root_extent_count
        .checked_mul(reader::DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow descriptor byte count overflow".to_string(),
            )
        })?;
    if descriptor_bytes > config.max_descriptor_bytes.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow root requires {descriptor_bytes} descriptor bytes, exceeding limit {}",
            config.max_descriptor_bytes
        )));
    }
    Ok(())
}

fn validate_publication_config(
    config: RelationalOverflowPublicationConfig,
) -> Result<(), RelationalOverflowPublicationError> {
    if config.max_manifest_bytes.get() < manifest::MANIFEST_HEADER_BYTES {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow manifest requires {} bytes, exceeding limit {}",
            manifest::MANIFEST_HEADER_BYTES,
            config.max_manifest_bytes
        )));
    }
    Ok(())
}

fn validate_publication_identity(
    generation: u64,
    source_commit_epoch: u64,
    root_is_empty: bool,
) -> Result<(), RelationalOverflowPublicationError> {
    if generation == 0 || (source_commit_epoch == 0 && !root_is_empty) {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow generation must be non-zero and epoch zero requires an empty root, got {generation}/{source_commit_epoch}"
        )));
    }
    Ok(())
}

fn acquire_publication_lock(directory: &Path) -> Result<File, RelationalOverflowPublicationError> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE))
        .map_err(durability("open overflow publication lock"))?;
    lock.lock()
        .map_err(durability("lock overflow publication"))?;
    Ok(lock)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), RelationalOverflowPublicationError> {
    let mut file = File::create(path).map_err(durability("create overflow candidate"))?;
    file.write_all(bytes)
        .map_err(durability("write overflow candidate"))?;
    file.sync_all()
        .map_err(durability("sync overflow candidate"))
}

fn durable_publish_immutable(
    source: &Path,
    destination: &Path,
) -> Result<(), RelationalOverflowPublicationError> {
    if destination.exists() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "immutable overflow artifact {} already exists",
            destination.display()
        )));
    }
    durable_replace_file(source, destination).map_err(durability("publish overflow artifact"))
}

fn maybe_stop(
    stop_after: Option<RelationalOverflowPublicationPhase>,
    phase: RelationalOverflowPublicationPhase,
) -> Result<(), RelationalOverflowPublicationError> {
    if stop_after == Some(phase) {
        return Err(RelationalOverflowPublicationError::Durability(format!(
            "injected stop after {phase:?}"
        )));
    }
    Ok(())
}

struct PublicationPaths {
    extent: PathBuf,
    extent_tmp: PathBuf,
    descriptor: PathBuf,
    descriptor_tmp: PathBuf,
    generation_manifest: PathBuf,
    generation_manifest_tmp: PathBuf,
    latest_manifest: PathBuf,
    latest_manifest_tmp: PathBuf,
}

impl PublicationPaths {
    fn new(directory: &Path, generation: u64) -> Self {
        let extent = directory.join(relational_overflow_extent_file(generation));
        let descriptor = directory.join(relational_overflow_descriptor_file(generation));
        let generation_manifest =
            directory.join(relational_overflow_manifest_generation_file(generation));
        let latest_manifest = directory.join(RELATIONAL_OVERFLOW_MANIFEST_FILE);
        Self {
            extent_tmp: extent.with_extension("skein.tmp"),
            descriptor_tmp: descriptor.with_extension("skein.tmp"),
            generation_manifest_tmp: generation_manifest.with_extension("skein.tmp"),
            latest_manifest_tmp: latest_manifest.with_extension("skein.tmp"),
            extent,
            descriptor,
            generation_manifest,
            latest_manifest,
        }
    }

    fn remove_temps(&self) -> Result<(), RelationalOverflowPublicationError> {
        for path in [
            &self.extent_tmp,
            &self.descriptor_tmp,
            &self.generation_manifest_tmp,
            &self.latest_manifest_tmp,
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(durability("remove stale overflow candidate")(error)),
            }
        }
        sync_directory(
            self.latest_manifest
                .parent()
                .expect("publication paths have a directory"),
        )
        .map_err(durability(
            "sync overflow directory after candidate cleanup",
        ))
    }

    fn require_fresh_generation(&self) -> Result<(), RelationalOverflowPublicationError> {
        for path in [&self.extent, &self.descriptor, &self.generation_manifest] {
            if path.exists() {
                return Err(RelationalOverflowPublicationError::Admission(format!(
                    "overflow generation artifact {} already exists",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn publication_lock_contract() {
        crate::file_lock_tests::assert_contract(
            RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
            "open overflow publication lock",
        );
    }

    #[test]
    #[ignore = "deterministic local publication lock campaign"]
    fn publication_lock_state_machine_campaign() {
        crate::file_lock_tests::assert_state_machine(
            RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
        );
    }
}
