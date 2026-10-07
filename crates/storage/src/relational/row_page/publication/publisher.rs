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

use super::compaction::RowPageRewriteControls;
use super::{
    durability, manifest, relational_row_page_artifact_file,
    relational_row_page_manifest_generation_file, relational_row_page_root_descriptor_file,
    relational_row_page_root_key_file, root, RelationalRowPageGenerationRequest,
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPagePublicationPhase, RelationalRowPagePublicationReport,
    RelationalRowPageRootDescriptor, RelationalRowPageRootManifest, RelationalRowPageRootReader,
    RelationalRowPageTableDelta, CANDIDATE_PUBLICATION_TRACE, COMPLETE_PUBLICATION_TRACE,
    RELATIONAL_ROW_PAGE_MANIFEST_FILE, RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE,
};
use crate::durability::{durable_replace_file, sync_directory};
use crate::file_io::{self as fs, File, OpenOptions};
use crate::relational::{
    RelationalOverflowPublicationError, RelationalOverflowRef, RelationalOverflowRootReader,
    RelationalValue,
};
use hawdb_integrity::{integrity_digest, Sha256Digest};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) mod checkpoint;

pub struct RelationalRowPagePublisher {
    config: RelationalRowPagePublicationConfig,
    work: Option<crate::background::CheckpointWorkContext>,
}

impl RelationalRowPagePublisher {
    pub const fn new(config: RelationalRowPagePublicationConfig) -> Self {
        Self { config, work: None }
    }

    #[doc(hidden)]
    pub fn with_work_context(mut self, work: &crate::background::CheckpointWorkContext) -> Self {
        self.work = Some(work.clone());
        self
    }

    pub fn publish(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        deltas: Vec<RelationalRowPageTableDelta>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        self.publish_inner(
            directory,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            deltas,
            PublicationControls {
                overflow_root: None,
                stop_after: None,
            },
        )
    }

    pub fn publish_with_overflow_root(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        deltas: Vec<RelationalRowPageTableDelta>,
        overflow_root: &RelationalOverflowRootReader,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        self.publish_inner(
            directory,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            deltas,
            PublicationControls {
                overflow_root: Some(overflow_root),
                stop_after: None,
            },
        )
    }

    /// Persists a complete immutable generation while leaving the independent
    /// latest selector untouched. The returned generation must be selected by
    /// the canonical checkpoint manifest before production readers may use it.
    pub fn persist_generation(
        &self,
        request: RelationalRowPageGenerationRequest<'_>,
        deltas: Vec<RelationalRowPageTableDelta>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        self.persist_generation_inner(
            GenerationPublication {
                directory: request.directory,
                generation: request.generation,
                source_commit_epoch: request.source_commit_epoch,
                base: request.base,
                expected_previous_generation: request.expected_previous_generation,
                overflow_root: request.overflow_root,
                select_latest: false,
                stop_after: None,
                rewrite: None,
            },
            deltas,
        )
    }

    /// Rewrites selected physical generations into a candidate without changing
    /// the canonical checkpoint or independent latest selector.
    pub fn persist_generation_compacting(
        &self,
        request: RelationalRowPageGenerationRequest<'_>,
        deltas: Vec<RelationalRowPageTableDelta>,
        config: super::RelationalRowPageRewriteConfig,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        self.persist_generation_inner(
            GenerationPublication {
                directory: request.directory,
                generation: request.generation,
                source_commit_epoch: request.source_commit_epoch,
                base: request.base,
                expected_previous_generation: request.expected_previous_generation,
                overflow_root: request.overflow_root,
                select_latest: false,
                stop_after: None,
                rewrite: Some(RowPageRewriteControls { config, task }),
            },
            deltas,
        )
    }

    pub(super) fn publish_inner(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        deltas: Vec<RelationalRowPageTableDelta>,
        controls: PublicationControls<'_>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        let base = RelationalRowPageRootReader::open_latest_with_work_context(
            directory,
            self.config,
            self.work.as_ref(),
        )?;
        self.persist_generation_inner(
            GenerationPublication {
                directory,
                generation,
                source_commit_epoch,
                base: base.as_ref(),
                expected_previous_generation,
                overflow_root: controls.overflow_root,
                select_latest: true,
                stop_after: controls.stop_after,
                rewrite: None,
            },
            deltas,
        )
    }

    fn persist_generation_inner(
        &self,
        publication: GenerationPublication<'_>,
        deltas: Vec<RelationalRowPageTableDelta>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        let GenerationPublication {
            directory,
            generation,
            source_commit_epoch,
            base,
            expected_previous_generation,
            overflow_root,
            select_latest,
            stop_after,
            rewrite,
        } = publication;
        if let Some(rewrite) = rewrite {
            rewrite.validate(base)?;
        }
        validate_publication_identity(generation, source_commit_epoch, deltas.is_empty())?;
        let overflow_binding = overflow_root.map(|reader| reader.manifest().binding());
        if overflow_binding.is_some_and(|binding| {
            binding.generation != generation || binding.source_commit_epoch != source_commit_epoch
        }) {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "overflow root generation/epoch does not match row-page generation/epoch {generation}/{source_commit_epoch}"
            )));
        }
        let mut deltas = preflight_deltas(
            deltas,
            generation,
            source_commit_epoch,
            overflow_root,
            self.config,
            self.work.as_ref(),
        )?;
        require_schema_source(base, &deltas, self.work.as_ref())?;
        checkpoint::io(self.work.as_ref(), || {
            fs::create_dir_all(directory).map_err(durability("create row-page directory"))
        })?;
        let _lock = checkpoint::lock(directory, self.work.as_ref())?;
        let paths = PublicationPaths::new(directory, generation);
        paths.require_fresh_generation(self.work.as_ref())?;

        let base_generation = base.map(|reader| reader.manifest.generation);
        if (select_latest || base.is_some()) && base_generation != expected_previous_generation {
            return Err(RelationalRowPagePublicationError::StaleGeneration {
                expected_previous: expected_previous_generation,
                actual_previous: base_generation,
            });
        }
        if let Some(previous) = expected_previous_generation
            && generation <= previous
        {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "new row-page generation {generation} must exceed previous generation {previous}"
            )));
        }
        if let Some(base) = base {
            if base.manifest.overflow_root.is_some() && overflow_binding.is_none() {
                return Err(RelationalRowPagePublicationError::Admission(
                    "a row root descended from overflow-bearing pages requires the next generation-bound overflow root"
                        .to_string(),
                ));
            }
            if generation <= base.manifest.generation {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "new row-page generation {generation} must exceed published generation {}",
                    base.manifest.generation
                )));
            }
            if source_commit_epoch < base.manifest.source_commit_epoch {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page source epoch {source_commit_epoch} precedes published epoch {}",
                    base.manifest.source_commit_epoch
                )));
            }
        }
        preflight_root_resources(base, &deltas, self.config, self.work.as_ref())?;

        // Cleanup drops before the publication lock and owns only exclusive creates.
        let mut temporary = OwnedTemporaryArtifacts::default();
        self.build_and_publish(PublicationBuild {
            temporary: &mut temporary,
            paths: &paths,
            base,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            overflow_root: overflow_binding,
            deltas: &mut deltas,
            select_latest,
            stop_after,
            rewrite,
        })
    }

    fn build_and_publish(
        &self,
        build: PublicationBuild<'_>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateStarted,
        )?;

        let page_file = checkpoint::create(
            build.temporary,
            &build.paths.page_tmp,
            "create row-page artifact",
            self.work.as_ref(),
        )?;
        let mut pages = root::PageArtifactWriter::new(page_file, self.config.page_limits)
            .with_work_context(self.work.as_ref());
        let mut dirty_page_count = 0u64;
        for delta in build.deltas.values_mut() {
            for page in &mut delta.dirty_pages {
                if let Some(rewrite) = build.rewrite {
                    rewrite.checkpoint()?;
                }
                pages.write(page)?;
                dirty_page_count += 1;
            }
        }
        let root = root::write_root_artifacts(
            root::RootBuildRequest {
                descriptor_file: checkpoint::create(
                    build.temporary,
                    &build.paths.descriptor_tmp,
                    "create row-page root descriptors",
                    self.work.as_ref(),
                )?,
                key_file: checkpoint::create(
                    build.temporary,
                    &build.paths.key_tmp,
                    "create row-page root keys",
                    self.work.as_ref(),
                )?,
                base: build.base,
                deltas: build.deltas,
                generation: build.generation,
                source_commit_epoch: build.source_commit_epoch,
                config: self.config,
            },
            &mut pages,
            build.rewrite,
        )?;
        let (page_artifact, written_pages) = pages.finish()?;
        if written_pages != dirty_page_count + root.relocated_page_count {
            return Err(RelationalRowPagePublicationError::Corrupt(
                "row-page rewrite lost its allocation accounting".to_string(),
            ));
        }
        if let Some(rewrite) = build.rewrite {
            rewrite.checkpoint()?;
        }
        let manifest = RelationalRowPageRootManifest {
            generation: build.generation,
            source_commit_epoch: build.source_commit_epoch,
            previous_generation: build.expected_previous_generation,
            page_bytes: self.config.page_limits.max_page_bytes.get() as u64,
            dirty_page_count,
            relocated_page_count: root.relocated_page_count,
            root_page_count: root.root_page_count,
            page_artifact,
            root_descriptor_artifact: root.descriptor_artifact,
            root_key_artifact: root.key_artifact,
            root_set_digest: match &self.work {
                Some(work) => manifest::root_set_digest_with_work_context(&root.tables, work)?,
                None => manifest::root_set_digest(&root.tables)?,
            },
            overflow_root: build.overflow_root,
            tables: root.tables,
            physical_generations: root.physical_generations,
        };
        let encoded_manifest = match &self.work {
            Some(work) => {
                manifest::encode_manifest_with_work_context(&manifest, self.config, work)?
            }
            None => manifest::encode_manifest(&manifest, self.config)?,
        };
        let manifest_digest = match &self.work {
            Some(work) => work
                .integrity(&encoded_manifest)
                .map_err(root::checkpoint::work_error)?,
            None => integrity_digest(&encoded_manifest),
        };
        checkpoint::write_synced(
            build.temporary,
            &build.paths.generation_manifest_tmp,
            &encoded_manifest,
            self.work.as_ref(),
        )?;

        checkpoint::publish(&build.paths.page_tmp, &build.paths.page, self.work.as_ref())?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidatePagesDurable,
        )?;
        checkpoint::publish(
            &build.paths.descriptor_tmp,
            &build.paths.descriptor,
            self.work.as_ref(),
        )?;
        checkpoint::publish(&build.paths.key_tmp, &build.paths.key, self.work.as_ref())?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateRootDurable,
        )?;
        checkpoint::publish(
            &build.paths.generation_manifest_tmp,
            &build.paths.generation_manifest,
            self.work.as_ref(),
        )?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateManifestDurable,
        )?;

        if build.select_latest {
            let actual_previous = manifest::read_manifest_if_exists_with_work_context(
                &build.paths.latest_manifest,
                self.config,
                self.work.as_ref(),
            )?
            .map(|manifest| manifest.generation);
            if actual_previous != build.expected_previous_generation {
                return Err(RelationalRowPagePublicationError::StaleGeneration {
                    expected_previous: build.expected_previous_generation,
                    actual_previous,
                });
            }
        }
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::BaseRevalidated,
        )?;
        if build.select_latest {
            checkpoint::write_synced(
                build.temporary,
                &build.paths.latest_manifest_tmp,
                &encoded_manifest,
                self.work.as_ref(),
            )?;
            checkpoint::select(
                &build.paths.latest_manifest_tmp,
                &build.paths.latest_manifest,
                self.work.as_ref(),
            )?;
        }

        Ok(RelationalRowPagePublicationReport {
            generation: build.generation,
            source_commit_epoch: build.source_commit_epoch,
            dirty_pages_written: dirty_page_count,
            relocated_pages_written: root.relocated_page_count,
            root_pages: manifest.root_page_count,
            reused_pages: root.reused_page_count,
            page_artifact_bytes: manifest.page_artifact.encoded_len,
            root_descriptor_bytes: manifest.root_descriptor_artifact.encoded_len,
            root_key_bytes: manifest.root_key_artifact.encoded_len,
            manifest_bytes: encoded_manifest.len() as u64,
            generation_artifacts: super::RelationalRowPageGenerationArtifacts {
                generation: manifest.generation,
                source_commit_epoch: manifest.source_commit_epoch,
                root_set_digest: manifest.root_set_digest,
                manifest_artifact: super::RelationalRowPageArtifactMetadata {
                    encoded_len: encoded_manifest.len() as u64,
                    encoded_crc32c: manifest_digest.crc32c.get(),
                    encoded_sha256: manifest_digest.sha256,
                },
            },
            events: if build.select_latest {
                COMPLETE_PUBLICATION_TRACE
            } else {
                CANDIDATE_PUBLICATION_TRACE
            },
        })
    }
}

fn require_schema_source(
    base: Option<&RelationalRowPageRootReader>,
    deltas: &BTreeMap<String, PreparedTableDelta>,
    work: Option<&crate::background::CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    for delta in deltas.values() {
        if let Some(work) = work {
            work.checkpoint().map_err(root::checkpoint::work_error)?;
        }
        let base_schema = base.and_then(|reader| {
            reader
                .manifest()
                .tables
                .binary_search_by(|table| table.table.cmp(&delta.table))
                .ok()
                .map(|index| &reader.manifest().tables[index].schema)
        });
        match (base_schema, delta.schema.as_ref()) {
            (None, None) => {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "new row-page table {} is missing its schema",
                    delta.table
                )));
            }
            (Some(base_schema), Some(schema)) => {
                let same = match work {
                    Some(work) => root::checkpoint::same_schema(base_schema, schema, work)?,
                    None => base_schema == schema,
                };
                if !same {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "table {} schema changed during incremental row-page publication",
                        delta.table
                    )));
                }
            }
            _ => {}
        }
    }
    if let Some(work) = work {
        work.checkpoint().map_err(root::checkpoint::work_error)?;
    }
    Ok(())
}

struct GenerationPublication<'a> {
    directory: &'a Path,
    generation: u64,
    source_commit_epoch: u64,
    base: Option<&'a RelationalRowPageRootReader>,
    expected_previous_generation: Option<u64>,
    overflow_root: Option<&'a RelationalOverflowRootReader>,
    select_latest: bool,
    stop_after: Option<RelationalRowPagePublicationPhase>,
    rewrite: Option<RowPageRewriteControls<'a>>,
}

pub(super) struct PublicationControls<'a> {
    pub overflow_root: Option<&'a RelationalOverflowRootReader>,
    pub stop_after: Option<RelationalRowPagePublicationPhase>,
}

struct PublicationBuild<'a> {
    temporary: &'a mut OwnedTemporaryArtifacts,
    paths: &'a PublicationPaths,
    base: Option<&'a RelationalRowPageRootReader>,
    generation: u64,
    source_commit_epoch: u64,
    expected_previous_generation: Option<u64>,
    overflow_root: Option<crate::relational::RelationalOverflowRootBinding>,
    deltas: &'a mut BTreeMap<String, PreparedTableDelta>,
    select_latest: bool,
    stop_after: Option<RelationalRowPagePublicationPhase>,
    rewrite: Option<RowPageRewriteControls<'a>>,
}

#[derive(Debug)]
pub(super) struct PreparedDirtyPage {
    pub page: super::ImmutableRelationalRowPage,
    pub descriptor: RelationalRowPageRootDescriptor,
}

#[derive(Debug)]
pub(super) struct PreparedTableDelta {
    pub table: String,
    pub schema: Option<crate::relational::RelationalTableSchema>,
    pub schema_digest: Sha256Digest,
    pub column_count: std::num::NonZeroU32,
    pub next_page_id: std::num::NonZeroU64,
    pub dirty_pages: Vec<PreparedDirtyPage>,
    pub deleted_page_ids: BTreeSet<super::RelationalRowPageId>,
}

fn preflight_deltas(
    deltas: Vec<RelationalRowPageTableDelta>,
    generation: u64,
    source_commit_epoch: u64,
    overflow_root: Option<&RelationalOverflowRootReader>,
    config: RelationalRowPagePublicationConfig,
    work: Option<&crate::background::CheckpointWorkContext>,
) -> Result<BTreeMap<String, PreparedTableDelta>, RelationalRowPagePublicationError> {
    let dirty_page_count = deltas.iter().try_fold(0usize, |count, delta| {
        checkpoint::cpu(work, || {
            count.checked_add(delta.dirty_pages.len()).ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "dirty page count overflow".to_string(),
                )
            })
        })
    })?;
    if dirty_page_count > config.max_dirty_pages.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication contains {dirty_page_count} dirty pages, exceeding limit {}",
            config.max_dirty_pages
        )));
    }
    let admitted_dirty_bytes = (dirty_page_count as u64)
        .checked_mul(config.page_limits.max_page_bytes.get() as u64)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "dirty page byte count overflow".to_string(),
            )
        })?;
    if admitted_dirty_bytes > config.max_dirty_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication reserves {admitted_dirty_bytes} dirty bytes, exceeding limit {}",
            config.max_dirty_bytes
        )));
    }
    if deltas.len() > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication contains {} table deltas, exceeding limit {}",
            deltas.len(),
            config.max_tables
        )));
    }

    let mut prepared = BTreeMap::new();
    let mut overflow_references = BTreeSet::<RelationalOverflowRef>::new();
    for delta in deltas {
        checkpoint::cpu(work, || {
            validate_table_name(&delta.table, config)?;
            if prepared.contains_key(&delta.table) {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "publication contains duplicate table delta {}",
                    delta.table
                )));
            }
            Ok(())
        })?;
        if let Some(schema) = &delta.schema {
            match work {
                Some(work) => crate::relational::codec::validate_relational_table_schema_codec_shape_with_work_context(
                    schema, config.page_limits.max_columns.get(), work,
                ),
                None => crate::relational::codec::validate_relational_table_schema_codec_shape(
                    schema, config.page_limits.max_columns.get(),
                ),
            }
            .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string()))?;
            let names_equal = match work {
                Some(work) => {
                    root::checkpoint::compare(schema.name.as_bytes(), delta.table.as_bytes(), work)?
                        .is_eq()
                }
                None => schema.name == delta.table,
            };
            if !names_equal {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page schema name {} differs from table {}",
                    schema.name, delta.table
                )));
            }
            if schema.columns.len() != delta.column_count.get() as usize {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page schema for table {} contains {} columns, expected {}",
                    delta.table,
                    schema.columns.len(),
                    delta.column_count
                )));
            }
            let digest = match work {
                Some(work) => {
                    crate::relational::index_shadow::relational_schema_digest_with_work_context(
                        schema, work,
                    )
                }
                None => crate::relational::index_shadow::relational_schema_digest(schema),
            }
            .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string()))?;
            if digest != delta.schema_digest {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page schema digest differs from table {}",
                    delta.table
                )));
            }
        }
        let deleted_count = delta.deleted_page_ids.len();
        let mut deleted_page_ids = BTreeSet::new();
        for page_id in delta.deleted_page_ids {
            checkpoint::cpu(work, || {
                deleted_page_ids.insert(page_id);
                Ok(())
            })?;
        }
        if deleted_page_ids.len() != deleted_count {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "table {} contains duplicate deleted page ids",
                delta.table
            )));
        }
        for page_id in &deleted_page_ids {
            let invalid = checkpoint::cpu(work, || Ok(page_id.get() >= delta.next_page_id.get()))?;
            if invalid {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {} deleted page id {} is not below next page id {}",
                    delta.table,
                    page_id.get(),
                    delta.next_page_id
                )));
            }
        }
        let mut seen_page_ids = BTreeSet::new();
        let mut dirty_pages = Vec::with_capacity(delta.dirty_pages.len());
        for page in delta.dirty_pages {
            checkpoint::cpu(work, || {
                if page.page_id.get() >= delta.next_page_id.get() {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "table {} page id {} is not below next page id {}",
                        delta.table,
                        page.page_id.get(),
                        delta.next_page_id
                    )));
                }
                if page.generation != generation || page.source_commit_epoch != source_commit_epoch
                {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                    "dirty page {} identifies generation/epoch {}/{}, expected {generation}/{source_commit_epoch}",
                    page.page_id.get(), page.generation, page.source_commit_epoch
                )));
                }
                if page.schema_digest != delta.schema_digest {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "dirty page {} schema digest differs from table {}",
                        page.page_id.get(),
                        delta.table
                    )));
                }
                if page.column_count != delta.column_count.get() as usize {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "dirty page {} contains {} columns, expected {} for table {}",
                        page.page_id.get(),
                        page.column_count,
                        delta.column_count,
                        delta.table
                    )));
                }
                if !seen_page_ids.insert(page.page_id) {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "table {} contains duplicate dirty page id {}",
                        delta.table,
                        page.page_id.get()
                    )));
                }
                if deleted_page_ids.contains(&page.page_id) {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "table {} both replaces and deletes page {}",
                        delta.table,
                        page.page_id.get()
                    )));
                }
                Ok(())
            })?;
            for entry in &page.rows {
                for value in entry.row.values() {
                    checkpoint::cpu(work, || {
                        if let RelationalValue::Overflow(reference) = value {
                            overflow_references.insert(*reference);
                        }
                        Ok(())
                    })?;
                }
            }
            dirty_pages.push(match work {
                Some(work) => root::prepare_dirty_page_with_work_context(
                    page,
                    config.page_limits,
                    Some(work),
                )?,
                None => root::prepare_dirty_page(page, config.page_limits)?,
            });
        }
        checkpoint::sort_dirty_pages(&mut dirty_pages, work)?;
        for pair in dirty_pages.windows(2) {
            let ordering = match work {
                Some(work) => root::checkpoint::compare(
                    &pair[0].descriptor.upper_bound,
                    &pair[1].descriptor.lower_bound,
                    work,
                )?,
                None => pair[0]
                    .descriptor
                    .upper_bound
                    .cmp(&pair[1].descriptor.lower_bound),
            };
            if !ordering.is_lt() {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {} dirty page bounds overlap or are unordered",
                    delta.table
                )));
            }
        }
        let table_name = clone_table_name(&delta.table, work)?;
        checkpoint::cpu(work, || {
            prepared.insert(
                table_name,
                PreparedTableDelta {
                    table: delta.table,
                    schema: delta.schema,
                    schema_digest: delta.schema_digest,
                    column_count: delta.column_count,
                    next_page_id: delta.next_page_id,
                    dirty_pages,
                    deleted_page_ids,
                },
            );
            Ok(())
        })?;
    }
    if !overflow_references.is_empty() {
        let overflow_root = overflow_root.ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row pages contain overflow references without a generation-bound overflow root"
                    .to_string(),
            )
        })?;
        for reference in overflow_references {
            let contains = match work {
                Some(work) => overflow_root.contains_with_work_context(&reference, work),
                None => overflow_root.contains(&reference),
            }
            .map_err(overflow_dependency_error)?;
            if !contains {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row page references missing overflow extent {}",
                    reference.digest
                )));
            }
        }
    }
    Ok(prepared)
}

fn overflow_dependency_error(
    error: RelationalOverflowPublicationError,
) -> RelationalRowPagePublicationError {
    match error {
        RelationalOverflowPublicationError::FileDescriptors(error) => {
            RelationalRowPagePublicationError::FileDescriptors(error)
        }
        RelationalOverflowPublicationError::Admission(message) => {
            RelationalRowPagePublicationError::Admission(message)
        }
        RelationalOverflowPublicationError::Corrupt(message) => {
            RelationalRowPagePublicationError::Corrupt(message)
        }
        RelationalOverflowPublicationError::Durability(message) => {
            RelationalRowPagePublicationError::Durability(message)
        }
        RelationalOverflowPublicationError::MissingExtent(digest) => {
            RelationalRowPagePublicationError::Admission(format!(
                "missing overflow extent {digest}"
            ))
        }
        RelationalOverflowPublicationError::Stopped(reason) => {
            RelationalRowPagePublicationError::Admission(format!(
                "overflow dependency validation stopped: {reason}"
            ))
        }
        RelationalOverflowPublicationError::StaleGeneration {
            expected_previous,
            actual_previous,
        } => RelationalRowPagePublicationError::Admission(format!(
            "overflow root changed: expected {expected_previous:?}, found {actual_previous:?}"
        )),
    }
}

fn validate_publication_identity(
    generation: u64,
    source_commit_epoch: u64,
    root_is_empty: bool,
) -> Result<(), RelationalRowPagePublicationError> {
    if generation == 0 || (source_commit_epoch == 0 && !root_is_empty) {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page generation must be non-zero and epoch zero requires an empty root, got {generation}/{source_commit_epoch}"
        )));
    }
    Ok(())
}

fn preflight_root_resources(
    base: Option<&RelationalRowPageRootReader>,
    deltas: &BTreeMap<String, PreparedTableDelta>,
    config: RelationalRowPagePublicationConfig,
    work: Option<&crate::background::CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    let base_page_count = base.map_or(0, |reader| reader.manifest.root_page_count);
    let dirty_page_count = deltas.values().try_fold(0u64, |count, delta| {
        checkpoint::cpu(work, || {
            count
                .checked_add(delta.dirty_pages.len() as u64)
                .ok_or_else(|| {
                    RelationalRowPagePublicationError::Admission(
                        "row-page root pre-admission count overflow".to_string(),
                    )
                })
        })
    })?;
    let root_page_upper_bound = base_page_count
        .checked_add(dirty_page_count)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page root pre-admission count overflow".to_string(),
            )
        })?;
    if root_page_upper_bound > config.max_root_pages.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {root_page_upper_bound} pages, exceeding limit {}",
            config.max_root_pages
        )));
    }

    let base_key_bytes = base.map_or(0, |reader| reader.manifest.root_key_artifact.encoded_len);
    let dirty_key_bytes = deltas.values().try_fold(0u64, |table_bytes, delta| {
        delta
            .dirty_pages
            .iter()
            .try_fold(table_bytes, |bytes, page| {
                checkpoint::cpu(work, || {
                    bytes
                        .checked_add(page.descriptor.lower_bound.len() as u64)
                        .and_then(|bytes| {
                            bytes.checked_add(page.descriptor.upper_bound.len() as u64)
                        })
                        .ok_or_else(|| {
                            RelationalRowPagePublicationError::Admission(
                                "row-page root key pre-admission overflow".to_string(),
                            )
                        })
                })
            })
    })?;
    let root_key_upper_bound = base_key_bytes.checked_add(dirty_key_bytes).ok_or_else(|| {
        RelationalRowPagePublicationError::Admission(
            "row-page root key pre-admission overflow".to_string(),
        )
    })?;
    if root_key_upper_bound > config.max_root_key_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {root_key_upper_bound} key bytes, exceeding limit {}",
            config.max_root_key_bytes
        )));
    }

    let mut table_names = BTreeSet::new();
    if let Some(base) = base {
        for table in &base.manifest.tables {
            let name = clone_table_name(&table.table, work)?;
            checkpoint::cpu(work, || {
                table_names.insert(name);
                Ok(())
            })?;
        }
    }
    for table in deltas.keys() {
        let name = clone_table_name(table, work)?;
        checkpoint::cpu(work, || {
            table_names.insert(name);
            Ok(())
        })?;
    }
    if table_names.len() > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {} tables, exceeding limit {}",
            table_names.len(),
            config.max_tables
        )));
    }
    let generation_count = base.map_or(0, |reader| reader.manifest.physical_generations.len());
    let mut manifest_upper_bound = generation_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(manifest::PHYSICAL_GENERATION_BYTES))
        .and_then(|bytes| bytes.checked_add(manifest::OCCUPANCY_TRAILER_BYTES))
        .and_then(|bytes| bytes.checked_add(manifest::MANIFEST_HEADER_BYTES))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page physical-generation inventory length overflow".to_string(),
            )
        })?;
    for table_name in table_names {
        let base_table = base.and_then(|reader| {
            reader
                .manifest
                .tables
                .binary_search_by(|table| table.table.cmp(&table_name))
                .ok()
                .map(|index| &reader.manifest.tables[index])
        });
        let delta = deltas.get(&table_name);
        let mut lower_len = base_table.map_or(0, |table| table.lower_bound.len());
        let mut upper_len = base_table.map_or(0, |table| table.upper_bound.len());
        if let Some(delta) = delta {
            for page in &delta.dirty_pages {
                checkpoint::cpu(work, || {
                    lower_len = lower_len.max(page.descriptor.lower_bound.len());
                    upper_len = upper_len.max(page.descriptor.upper_bound.len());
                    Ok(())
                })?;
            }
        }
        manifest_upper_bound = manifest_upper_bound
            .checked_add(68)
            .and_then(|bytes| bytes.checked_add(table_name.len()))
            .and_then(|bytes| bytes.checked_add(lower_len))
            .and_then(|bytes| bytes.checked_add(upper_len))
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page manifest pre-admission overflow".to_string(),
                )
            })?;
    }
    if manifest_upper_bound > config.max_manifest_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest may contain {manifest_upper_bound} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    if let Some(work) = work {
        work.checkpoint().map_err(root::checkpoint::work_error)?;
    }
    Ok(())
}

fn clone_table_name(
    name: &str,
    work: Option<&crate::background::CheckpointWorkContext>,
) -> Result<String, RelationalRowPagePublicationError> {
    match work {
        Some(work) => crate::relational::codec::clone_string_with_work_context(name, work)
            .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string())),
        None => Ok(name.to_owned()),
    }
}

fn validate_table_name(
    table: &str,
    config: RelationalRowPagePublicationConfig,
) -> Result<(), RelationalRowPagePublicationError> {
    if table.is_empty() || table.len() > config.max_table_name_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "table name contains {} bytes, outside admitted range 1..={}",
            table.len(),
            config.max_table_name_bytes
        )));
    }
    Ok(())
}

pub(crate) fn acquire_publication_lock(
    directory: &Path,
) -> Result<File, RelationalRowPagePublicationError> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE))
        .map_err(durability("open row-page publication lock"))?;
    lock.lock()
        .map_err(durability("lock row-page publication"))?;
    Ok(lock)
}

fn write_synced(
    temporary: &mut OwnedTemporaryArtifacts,
    path: &Path,
    bytes: &[u8],
) -> Result<(), RelationalRowPagePublicationError> {
    let mut file = temporary.create(path, "create row-page candidate")?;
    file.write_all(bytes)
        .map_err(durability("write row-page candidate"))?;
    file.sync_all()
        .map_err(durability("sync row-page candidate"))
}

fn durable_publish_immutable(
    source: &Path,
    destination: &Path,
) -> Result<(), RelationalRowPagePublicationError> {
    if destination.exists() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "immutable row-page artifact {} already exists",
            destination.display()
        )));
    }
    durable_replace_file(source, destination).map_err(durability("publish row-page artifact"))
}

fn maybe_stop(
    stop_after: Option<RelationalRowPagePublicationPhase>,
    phase: RelationalRowPagePublicationPhase,
) -> Result<(), RelationalRowPagePublicationError> {
    if stop_after == Some(phase) {
        return Err(RelationalRowPagePublicationError::Durability(format!(
            "injected stop after {phase:?}"
        )));
    }
    Ok(())
}

// Only an exclusive create transfers cleanup ownership. Renamed immutable files
// stay available for recovery; failed cleanup retains evidence. Global accounting
// and retrying cleanup debt are separate from this local ownership guard.
#[derive(Default)]
struct OwnedTemporaryArtifacts {
    paths: Vec<PathBuf>,
}

impl OwnedTemporaryArtifacts {
    fn create(
        &mut self,
        path: &Path,
        context: &'static str,
    ) -> Result<File, RelationalRowPagePublicationError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(durability(context))?;
        self.paths.push(path.to_path_buf());
        Ok(file)
    }
}

impl Drop for OwnedTemporaryArtifacts {
    fn drop(&mut self) {
        let mut removed = false;
        for path in &self.paths {
            if fs::remove_file(path).is_ok() {
                removed = true;
            }
        }
        if let Some(directory) = self
            .paths
            .first()
            .filter(|_| removed)
            .and_then(|path| path.parent())
        {
            let _ = sync_directory(directory);
        }
    }
}

struct PublicationPaths {
    page: PathBuf,
    page_tmp: PathBuf,
    descriptor: PathBuf,
    descriptor_tmp: PathBuf,
    key: PathBuf,
    key_tmp: PathBuf,
    generation_manifest: PathBuf,
    generation_manifest_tmp: PathBuf,
    latest_manifest: PathBuf,
    latest_manifest_tmp: PathBuf,
}

impl PublicationPaths {
    fn new(directory: &Path, generation: u64) -> Self {
        let page = directory.join(relational_row_page_artifact_file(generation));
        let descriptor = directory.join(relational_row_page_root_descriptor_file(generation));
        let key = directory.join(relational_row_page_root_key_file(generation));
        let generation_manifest =
            directory.join(relational_row_page_manifest_generation_file(generation));
        let latest_manifest = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        Self {
            page_tmp: page.with_extension("hawdb.tmp"),
            descriptor_tmp: descriptor.with_extension("hawdb.tmp"),
            key_tmp: key.with_extension("hawdb.tmp"),
            generation_manifest_tmp: generation_manifest.with_extension("hawdb.tmp"),
            latest_manifest_tmp: latest_manifest.with_extension("hawdb.tmp"),
            page,
            descriptor,
            key,
            generation_manifest,
            latest_manifest,
        }
    }

    fn require_fresh_generation(
        &self,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<(), RelationalRowPagePublicationError> {
        for path in [
            &self.page,
            &self.descriptor,
            &self.key,
            &self.generation_manifest,
        ] {
            checkpoint::io(work, || {
                if path.exists() {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "row-page generation artifact {} already exists",
                        path.display()
                    )));
                }
                Ok(())
            })?;
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
            RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
            "open row-page publication lock",
        );
    }

    #[test]
    #[ignore = "deterministic local publication lock campaign"]
    fn publication_lock_state_machine_campaign() {
        crate::file_lock_tests::assert_state_machine(
            RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
        );
    }
}
