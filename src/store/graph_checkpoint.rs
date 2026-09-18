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

//! Checkpoint preparation and publication, backup, residency and pressure reporting, and published-manifest accessors for [`GraphStore`].

use super::*;

struct ExactRelationalOverflowCheckpoint<'a> {
    references: &'a hawdb_storage::RelationalOverflowReferenceSet,
    scan: relational_row_pages::RelationalOverflowClosureScanReport,
    admitted_memory_bytes: u64,
    max_rewrite_bytes: NonZeroU64,
    task: &'a RuntimeTaskContext,
}

struct RelationalRowCompactionCheckpoint<'a> {
    config: RelationalRowPageCompactionConfig,
    admitted_memory_bytes: u64,
    task: &'a RuntimeTaskContext,
}

fn row_compaction_checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    task.checkpoint().map_err(|reason| {
        HawDBError::Execution(format!("relational row-page compaction stopped: {reason}"))
    })
}

fn row_compaction_publication_error(
    error: hawdb_storage::RelationalRowPagePublicationError,
) -> HawDBError {
    match error {
        hawdb_storage::RelationalRowPagePublicationError::Corrupt(_) => {
            HawDBError::StorageIntegrity(error.to_string())
        }
        _ => HawDBError::Storage(error.to_string()),
    }
}

fn exact_overflow_publication_error(
    error: hawdb_storage::RelationalOverflowPublicationError,
) -> HawDBError {
    let message = error.to_string();
    match error {
        hawdb_storage::RelationalOverflowPublicationError::Corrupt(_)
        | hawdb_storage::RelationalOverflowPublicationError::MissingExtent(_) => {
            HawDBError::StorageIntegrity(message)
        }
        hawdb_storage::RelationalOverflowPublicationError::Admission(_)
        | hawdb_storage::RelationalOverflowPublicationError::Durability(_)
        | hawdb_storage::RelationalOverflowPublicationError::StaleGeneration { .. } => {
            HawDBError::Storage(message)
        }
        hawdb_storage::RelationalOverflowPublicationError::Stopped(_) => {
            HawDBError::Execution(message)
        }
    }
}

fn push_property_projection_definition(
    definitions: &mut Vec<PersistentPropertyProjectionDefinition>,
    admission: &mut PersistentPropertyProjectionDefinitionAdmission,
    definition: PersistentPropertyProjectionDefinition,
) -> Result<()> {
    admission
        .admit(&definition)
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    definitions.push(definition);
    Ok(())
}

fn push_relationship_property_projection_definitions(
    definitions: &mut Vec<PersistentPropertyProjectionDefinition>,
    admission: &mut PersistentPropertyProjectionDefinitionAdmission,
    rel_type: RelTypeId,
    property: &str,
) -> Result<()> {
    for kind in [
        PersistentPropertyProjectionKind::RelationshipEquality,
        PersistentPropertyProjectionKind::RelationshipRange,
    ] {
        push_property_projection_definition(
            definitions,
            admission,
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(rel_type.0),
                property: property.to_string(),
                kind,
                complete: false,
            },
        )?;
    }
    Ok(())
}

impl GraphStore {
    pub(super) fn mount_append_generation_for_recovery(&mut self) -> Result<()> {
        let Some(durable) = self.durable.as_ref() else {
            self.append_generation_reader = None;
            self.append_state = AppendState::default();
            return Ok(());
        };
        let Some(binding) = durable.append_generation_artifacts else {
            self.append_generation_reader = None;
            self.append_state = AppendState::default();
            return Ok(());
        };
        let reader = AppendGenerationReader::open_bound(
            durable.root_path(),
            binding,
            self.append_publication_config,
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        self.append_state = AppendState::from_checkpoint_with_generated_order_watermarks(
            reader.manifest().schemas.clone(),
            reader.watermarks(),
            reader.generated_order_watermarks().clone(),
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        self.append_generation_reader = Some(reader);
        Ok(())
    }

    pub fn checkpoint(&mut self, catalog: &Catalog) -> Result<()> {
        self.checkpoint_with_reader_epoch(catalog, None)
    }

    pub(crate) fn compact_relational_row_pages(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        config: RelationalRowPageCompactionConfig,
        task: &RuntimeTaskContext,
    ) -> Result<RelationalRowPageCompactionReport> {
        let result = self.compact_relational_row_pages_inner(
            catalog,
            oldest_reader_commit_epoch,
            config,
            task,
        );
        if matches!(&result, Err(HawDBError::StorageIntegrity(_))) {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
        }
        result
    }

    fn compact_relational_row_pages_inner(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        config: RelationalRowPageCompactionConfig,
        task: &RuntimeTaskContext,
    ) -> Result<RelationalRowPageCompactionReport> {
        self.ensure_usable()?;
        row_compaction_checkpoint(task)?;
        config
            .rewrite
            .validate()
            .map_err(row_compaction_publication_error)?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "relational row-page compaction requires durable storage".to_string(),
            )
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only database cannot compact relational row pages".to_string(),
            ));
        }
        let materialized_sidecars = !self.canonical_base_out_of_core
            || !self.relational_state.canonical_row_metadata_only();
        let materialized_bytes = if materialized_sidecars {
            config
                .max_materialized_checkpoint_bytes
                .get()
                .checked_mul(2)
                .ok_or_else(|| {
                    HawDBError::Storage(
                        "row-page checkpoint materialization allowance overflow".to_string(),
                    )
                })?
        } else {
            0
        };
        let shadow_bytes = self.columnar_shadow_admission_bytes();
        let admitted_memory_bytes = config
            .admission_bytes()?
            .checked_add(materialized_bytes)
            .and_then(|bytes| bytes.checked_add(shadow_bytes))
            .ok_or_else(|| {
                HawDBError::Storage("row-page compaction admission byte count overflow".to_string())
            })?;
        let _permit = match &self.runtime_governor {
            Some(governor) => Some(
                governor
                    .try_admit(hawdb_storage::BackgroundWorkRequest {
                        cpu_slots: 1,
                        memory_bytes: admitted_memory_bytes,
                        io_slots: 1,
                    })
                    .map_err(|error| {
                        HawDBError::Storage(format!(
                            "row-page compaction admission denied: {error}"
                        ))
                    })?,
            ),
            None => None,
        };
        if materialized_sidecars {
            let graph_bytes = if self.canonical_base_out_of_core {
                0
            } else {
                self.estimated_logical_record_bytes()
            };
            let materialized_estimate = graph_bytes
                .checked_add(self.relational_state.estimated_materialized_row_bytes())
                .ok_or_else(|| {
                    HawDBError::Storage(
                        "row-page checkpoint materialization estimate overflow".to_string(),
                    )
                })?;
            if materialized_estimate > config.max_materialized_checkpoint_bytes.get() {
                return Err(HawDBError::Storage(
                    "row-page compaction exceeds its materialized checkpoint allowance".to_string(),
                ));
            }
        }
        row_compaction_checkpoint(task)?;
        let prepared = self
            .prepare_checkpoint_with_maintenance(
                catalog,
                DerivedArtifactBuildConfig::default(),
                None,
                Some(RelationalRowCompactionCheckpoint {
                    config,
                    admitted_memory_bytes,
                    task,
                }),
            )?
            .ok_or_else(|| {
                HawDBError::Storage(
                    "row-page compaction did not prepare a durable checkpoint".to_string(),
                )
            })?;
        if let Err(error) = row_compaction_checkpoint(task) {
            durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path)?;
            return Err(error);
        }
        let report = prepared
            .relational_row_compaction_report
            .clone()
            .expect("row-page compaction preparation includes its report");
        self.publish_prepared_checkpoint_with_shadow_admission(
            prepared,
            oldest_reader_commit_epoch,
            self.runtime_governor
                .as_ref()
                .map(|_| ColumnarShadowAdmission::pre_admitted(shadow_bytes)),
        )?;
        Ok(report)
    }

    pub(crate) fn compact_relational_overflow(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        config: RelationalOverflowCompactionConfig,
        task: &RuntimeTaskContext,
    ) -> Result<RelationalOverflowCompactionReport> {
        let result = self.compact_relational_overflow_inner(
            catalog,
            oldest_reader_commit_epoch,
            config,
            task,
        );
        if matches!(&result, Err(HawDBError::StorageIntegrity(_))) {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
        }
        result
    }

    fn compact_relational_overflow_inner(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        config: RelationalOverflowCompactionConfig,
        task: &RuntimeTaskContext,
    ) -> Result<RelationalOverflowCompactionReport> {
        self.ensure_usable()?;
        task.checkpoint().map_err(|reason| {
            HawDBError::Execution(format!("relational overflow compaction stopped: {reason}"))
        })?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "relational overflow compaction requires durable storage".to_string(),
            )
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only database cannot compact relational overflow".to_string(),
            ));
        }
        let admitted_memory_bytes = config.admission_bytes()?;
        let _permit = match &self.runtime_governor {
            Some(governor) => Some(
                governor
                    .try_admit(hawdb_storage::BackgroundWorkRequest {
                        cpu_slots: 1,
                        memory_bytes: admitted_memory_bytes,
                        io_slots: 1,
                    })
                    .map_err(|error| {
                        HawDBError::Storage(format!(
                            "relational overflow compaction admission denied: {error}"
                        ))
                    })?,
            ),
            None => None,
        };
        let generation = durable.checkpoint_epoch.saturating_add(1);
        let (references, scan) =
            self.collect_exact_relational_overflow_closure(generation, config, task)?;
        task.checkpoint().map_err(|reason| {
            HawDBError::Execution(format!("relational overflow compaction stopped: {reason}"))
        })?;
        let prepared = self
            .prepare_checkpoint_with_maintenance(
                catalog,
                DerivedArtifactBuildConfig::default(),
                Some(ExactRelationalOverflowCheckpoint {
                    references: &references,
                    scan,
                    admitted_memory_bytes,
                    max_rewrite_bytes: config.max_rewrite_bytes,
                    task,
                }),
                None,
            )?
            .ok_or_else(|| {
                HawDBError::Storage(
                    "relational overflow compaction did not prepare a durable checkpoint"
                        .to_string(),
                )
            })?;
        let report = prepared
            .relational_overflow_compaction_report
            .clone()
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(
                    "relational overflow compaction checkpoint lost its report".to_string(),
                )
            })?;
        self.publish_prepared_checkpoint(prepared, oldest_reader_commit_epoch)?;
        Ok(report)
    }

    pub fn backup_to(
        &mut self,
        catalog: &Catalog,
        destination: impl AsRef<Path>,
    ) -> Result<StorageBackupReport> {
        if self.durable.is_none() {
            return Err(HawDBError::Storage(
                "an in-memory database cannot create a durable backup".to_string(),
            ));
        }
        self.checkpoint(catalog)?;
        self.durable
            .as_ref()
            .expect("durable store must exist after checkpoint")
            .backup_to(destination.as_ref())
    }

    pub fn scrub_storage(&mut self) -> Result<StorageScrubReport> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("an in-memory database has no durable storage to scrub".to_string())
        })?;
        let result = durable.scrub_storage();
        if result.is_err() {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
        }
        result
    }

    pub fn checkpoint_with_reader_epoch(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        self.checkpoint_with_reader_epoch_and_build_config(
            catalog,
            oldest_reader_commit_epoch,
            DerivedArtifactBuildConfig::default(),
        )
    }

    pub(super) fn checkpoint_with_reader_epoch_and_build_config(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        build_config: DerivedArtifactBuildConfig,
    ) -> Result<()> {
        let Some(prepared) = self.prepare_checkpoint_with_build_config(catalog, build_config)?
        else {
            return Ok(());
        };
        self.publish_prepared_checkpoint(prepared, oldest_reader_commit_epoch)
    }

    pub(crate) fn checkpoint_source(&self) -> Self {
        let mut source = self.snapshot();
        source.durable = self.durable.clone();
        if let Some(durable) = &mut source.durable {
            durable.wal_append_file = None;
        }
        source
    }

    pub(crate) fn prepare_checkpoint(
        &self,
        catalog: &Catalog,
    ) -> Result<Option<PreparedCheckpoint>> {
        self.prepare_checkpoint_with_build_config(catalog, DerivedArtifactBuildConfig::default())
    }

    fn prepare_checkpoint_with_build_config(
        &self,
        catalog: &Catalog,
        build_config: DerivedArtifactBuildConfig,
    ) -> Result<Option<PreparedCheckpoint>> {
        self.prepare_checkpoint_with_maintenance(catalog, build_config, None, None)
    }

    fn prepare_checkpoint_with_maintenance(
        &self,
        catalog: &Catalog,
        build_config: DerivedArtifactBuildConfig,
        exact_overflow: Option<ExactRelationalOverflowCheckpoint<'_>>,
        row_compaction: Option<RelationalRowCompactionCheckpoint<'_>>,
    ) -> Result<Option<PreparedCheckpoint>> {
        let Some(durable) = self.durable.as_ref() else {
            return Ok(None);
        };
        let estimated_record_bytes = self.estimated_logical_record_bytes();
        let checkpoint_out_of_core = match self.residency_mode {
            StorageResidencyMode::Materialized => false,
            StorageResidencyMode::OutOfCore => true,
            StorageResidencyMode::Auto => {
                self.canonical_base_out_of_core
                    || estimated_record_bytes > self.auto_materialize_checkpoint_bytes
            }
        };
        let (projected_graph_artifacts, artifacts) = if checkpoint_out_of_core {
            (None, BTreeMap::new())
        } else {
            let encoded =
                encode_projected_graph_artifacts(catalog, self, self.next_projection_epoch());
            let (_, artifacts) = decode_projected_graph_artifacts(&encoded)?;
            (Some(encoded), artifacts)
        };
        let source_scan_projection = (!checkpoint_out_of_core).then(|| {
            source_scan::build(
                self.commit_epoch,
                catalog.label_id("Source"),
                self.nodes.values(),
            )
        });
        let merged_nodes = self.canonical_base.as_ref().map(|_| {
            self.node_records_owned().map(|record| {
                record.map_err(|error| CanonicalSegmentError::Source(error.to_string()))
            })
        });
        let property_projection_records = self.canonical_base.as_ref().map(|_| {
            let nodes = self.node_records_owned().map(|record| {
                record
                    .map(PersistentPropertyProjectionRecord::Node)
                    .map_err(|error| {
                        hawdb_storage::PersistentPropertyProjectionError::Source(error.to_string())
                    })
            });
            let relationships = self.relationship_records_owned().map(|record| {
                record
                    .map(PersistentPropertyProjectionRecord::Relationship)
                    .map_err(|error| {
                        hawdb_storage::PersistentPropertyProjectionError::Source(error.to_string())
                    })
            });
            nodes.chain(relationships)
        });
        let mut property_projection_definitions = Vec::new();
        let mut property_projection_definition_admission =
            PersistentPropertyProjectionDefinitionAdmission::new(build_config.property_projection);
        for index in catalog.property_indexes() {
            let kind = match index.kind {
                IndexKind::Equality => PersistentPropertyProjectionKind::Equality,
                IndexKind::Range => PersistentPropertyProjectionKind::Range,
                IndexKind::FullText => PersistentPropertyProjectionKind::FullText,
            };
            push_property_projection_definition(
                &mut property_projection_definitions,
                &mut property_projection_definition_admission,
                PersistentPropertyProjectionDefinition {
                    label_id: index.label_id,
                    property: index.property.clone(),
                    kind,
                    complete: false,
                },
            )?;
        }
        for index in catalog.composite_property_indexes() {
            let property = persistent_composite_property_identity(&index.properties)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            push_property_projection_definition(
                &mut property_projection_definitions,
                &mut property_projection_definition_admission,
                PersistentPropertyProjectionDefinition {
                    label_id: index.label_id,
                    property,
                    kind: PersistentPropertyProjectionKind::CompositeEquality,
                    complete: false,
                },
            )?;
        }
        let mut relationship_property_definitions = BTreeSet::new();
        for (rel_type, property, _) in self.relationship_property_index.keys() {
            if relationship_property_definitions.insert((*rel_type, property.clone())) {
                push_relationship_property_projection_definitions(
                    &mut property_projection_definitions,
                    &mut property_projection_definition_admission,
                    *rel_type,
                    property,
                )?;
            }
        }
        if let Some(projection) = &self.persistent_property_projection {
            for definition in &projection.manifest().definitions {
                if matches!(
                    definition.kind,
                    PersistentPropertyProjectionKind::RelationshipEquality
                        | PersistentPropertyProjectionKind::RelationshipRange
                ) && relationship_property_definitions.insert((
                    RelTypeId(definition.label_id.0),
                    definition.property.clone(),
                )) {
                    push_relationship_property_projection_definitions(
                        &mut property_projection_definitions,
                        &mut property_projection_definition_admission,
                        RelTypeId(definition.label_id.0),
                        &definition.property,
                    )?;
                }
            }
        }
        let merged_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| CanonicalSegmentError::Source(error.to_string()))
            })
        });
        let adjacency_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| {
                    hawdb_storage::CanonicalAdjacencyError::Source(error.to_string())
                })
            })
        });
        let commit_epoch = self.commit_epoch;
        let checkpoint_statistics = if checkpoint_out_of_core && !self.canonical_base_out_of_core {
            let mut statistics = graph_statistics_from_basic(self.basic_statistics(), false);
            statistics.index_samples = compute_index_statistics_samples(
                catalog,
                &self.property_index,
                &self.composite_property_index,
            );
            statistics
        } else {
            self.statistics(catalog)
        };
        let generation = durable.checkpoint_epoch.saturating_add(1);
        let staging_path = durable.prepare_checkpoint_staging(generation)?;
        let prepared = (|| {
            let append_rows = self
                .append_state
                .checkpoint_rows(self.append_publication_config.segment.max_rows)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            let append_report = AppendPublisher::publish_candidate_with_state(
                durable.root_path(),
                generation,
                commit_epoch,
                self.append_generation_reader.as_ref(),
                AppendPublicationState::new(
                    self.append_state.schemas(),
                    self.append_state.generated_order_watermarks(),
                ),
                &append_rows,
                self.append_publication_config,
            )
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
            let checkpoint_append_reader = AppendGenerationReader::open_bound(
                durable.root_path(),
                append_report.generation_artifacts,
                self.append_publication_config,
            )
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
            if let Some(encoded) = projected_graph_artifacts.as_deref() {
                durable.write_projected_graph_artifacts_to(
                    &staging_path.join(PROJECTED_GRAPHS_FILE),
                    encoded,
                )?;
            }
            let source_scan_publication = source_scan_projection
                .map(|mut projection| source_scan::write(&staging_path, &mut projection))
                .transpose()?;
            let (canonical_manifest_artifact, property_spill_manifest_artifact) =
                match (merged_nodes, merged_relationships) {
                    (Some(nodes), Some(relationships)) => durable.write_canonical_segments(
                        nodes,
                        relationships,
                        generation,
                        commit_epoch,
                    )?,
                    (None, None) => durable.write_canonical_segments(
                        self.nodes.values().map(|node| Ok(node.clone())),
                        self.relationships
                            .values()
                            .map(|relationship| Ok(relationship.clone())),
                        generation,
                        commit_epoch,
                    )?,
                    _ => unreachable!("canonical base iterators are created together"),
                };
            let canonical_adjacency_artifacts = match adjacency_relationships {
                Some(relationships) => durable.write_canonical_adjacency(
                    relationships,
                    generation,
                    commit_epoch,
                    build_config.adjacency,
                )?,
                None => durable.write_canonical_adjacency(
                    self.relationships.values().cloned().map(Ok),
                    generation,
                    commit_epoch,
                    build_config.adjacency,
                )?,
            };
            let property_projection_manifest_artifact = match property_projection_records {
                Some(records) => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    records,
                    generation,
                    commit_epoch,
                    build_config.property_projection,
                )?,
                None => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    self.nodes
                        .values()
                        .cloned()
                        .map(PersistentPropertyProjectionRecord::Node)
                        .map(Ok)
                        .chain(
                            self.relationships
                                .values()
                                .cloned()
                                .map(PersistentPropertyProjectionRecord::Relationship)
                                .map(Ok),
                        ),
                    generation,
                    commit_epoch,
                    build_config.property_projection,
                )?,
            };
            let relational_checkpoint_artifact = durable.write_relational_checkpoint(
                &self.relational_state,
                commit_epoch,
                generation,
            )?;
            let checkpoint_relational_state = relational_checkpoint_artifact
                .map(|_| {
                    let index_load = self.relational_checkpoint_index_load();
                    decode_relational_checkpoint_file_with_index_load(
                        &durable
                            .root_path()
                            .join(relational_checkpoint_generation_file(generation)),
                        RelationalDecodeLimits::checkpoint(),
                        index_load,
                    )
                    .map(|checkpoint| checkpoint.state)
                    .map_err(|error| HawDBError::Storage(error.to_string()))
                })
                .transpose()?;
            let mut overflow_publication_config = RelationalOverflowPublicationConfig::default();
            if let Some(exact) = exact_overflow.as_ref() {
                overflow_publication_config.max_new_extent_bytes = exact.max_rewrite_bytes;
            }
            let previous_overflow = durable
                .relational_overflow_generation_artifacts
                .map(|_| durable.open_bound_relational_overflow())
                .transpose()?;
            let previous_row = previous_overflow
                .as_ref()
                .map(|overflow| durable.open_bound_relational_row_pages(overflow))
                .transpose()?;
            let previous_allocated_pages = previous_row.as_ref().map_or(0, |reader| {
                reader
                    .manifest()
                    .physical_generations
                    .iter()
                    .map(|entry| entry.allocated_pages)
                    .sum()
            });
            let row_publication_config = row_compaction.as_ref().map_or_else(
                RelationalRowPagePublicationConfig::default,
                |row| {
                    hawdb_storage::relational::relational_row_page_compaction_publication_config(
                        row.config,
                    )
                },
            );
            let row_plan = self.plan_relational_row_page_checkpoint(
                previous_row,
                generation,
                commit_epoch,
                row_publication_config,
            )?;
            let max_materialized_overflow_bytes =
                usize::try_from(overflow_publication_config.max_new_extent_bytes.get())
                    .unwrap_or(usize::MAX);
            let metadata_only_rows = self.relational_state.canonical_row_metadata_only();
            if exact_overflow.is_some() && !metadata_only_rows {
                return Err(HawDBError::Storage(
                    "exact overflow compaction requires canonical metadata-only rows".to_string(),
                ));
            }
            let overflow_publisher = RelationalOverflowPublisher::new(overflow_publication_config);
            let mut copied_base_extent_count = 0u64;
            let mut introduced_extent_count = 0u64;
            let relational_overflow_report = if let Some(exact) = exact_overflow.as_ref() {
                let base = previous_overflow.as_ref().ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "exact overflow compaction requires a pinned overflow root".to_string(),
                    )
                })?;
                overflow_publisher
                    .persist_generation_exact_references(
                        hawdb_storage::RelationalOverflowExactGenerationRequest {
                            directory: durable.root_path(),
                            generation,
                            source_commit_epoch: commit_epoch,
                            base,
                            expected_previous_generation: base.manifest().generation,
                            references: exact.references,
                            task: exact.task,
                        },
                        |reference| Ok(self.relational_state.inline_overflow_envelope(reference)),
                    )
                    .map(|report| {
                        copied_base_extent_count = report.copied_base_extent_count;
                        introduced_extent_count = report.introduced_extent_count;
                        report.publication
                    })
                    .map_err(exact_overflow_publication_error)
            } else if metadata_only_rows {
                let base = previous_overflow.as_ref().ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "metadata-only relational checkpoint requires a pinned overflow root"
                            .to_string(),
                    )
                })?;
                let overflow_inputs = self
                    .relational_state
                    .overflow_delta_generation_inputs(&row_plan.deltas)
                    .map_err(|error| HawDBError::Storage(error.to_string()))?;
                overflow_publisher
                    .persist_generation_retaining_base(
                        durable.root_path(),
                        generation,
                        commit_epoch,
                        base,
                        base.manifest().generation,
                        overflow_inputs,
                    )
                    .map_err(|error| HawDBError::Storage(error.to_string()))
            } else {
                let overflow_inputs = self
                    .relational_state
                    .overflow_generation_inputs(
                        previous_overflow.is_some(),
                        max_materialized_overflow_bytes,
                    )
                    .map_err(|error| HawDBError::Storage(error.to_string()))?;
                overflow_publisher
                    .persist_generation(
                        durable.root_path(),
                        generation,
                        commit_epoch,
                        previous_overflow.as_ref(),
                        durable
                            .relational_overflow_generation_artifacts
                            .map(|binding| binding.generation),
                        overflow_inputs,
                    )
                    .map_err(|error| HawDBError::Storage(error.to_string()))
            }?;
            let relational_overflow_compaction_report =
                exact_overflow
                    .as_ref()
                    .map(|exact| RelationalOverflowCompactionReport {
                        source_commit_epoch: commit_epoch,
                        published_generation: generation,
                        tables_scanned: exact.scan.tables_scanned,
                        rows_scanned: exact.scan.rows_scanned,
                        pages_read: exact.scan.pages_read,
                        row_bytes_read: exact.scan.row_bytes_read,
                        hydrated_values: exact.scan.hydrated_values,
                        overlay_entries: exact.scan.overlay_entries,
                        overlay_bytes: exact.scan.overlay_bytes,
                        reference_occurrences: exact.scan.sort.reference_occurrences,
                        unique_references: exact.scan.sort.unique_references,
                        spill_run_count: exact.scan.sort.spill_run_count,
                        spill_bytes: exact.scan.sort.spill_bytes,
                        peak_sort_memory_bytes: exact.scan.sort.peak_memory_bytes,
                        previous_extent_count: previous_overflow
                            .as_ref()
                            .map_or(0, |base| base.manifest().extent_count),
                        published_extent_count: relational_overflow_report.extent_count,
                        reclaimable_base_extent_count: previous_overflow.as_ref().map_or(
                            0,
                            |base| {
                                base.manifest()
                                    .extent_count
                                    .saturating_sub(copied_base_extent_count)
                            },
                        ),
                        new_extent_count: relational_overflow_report.new_extent_count,
                        reused_extent_count: relational_overflow_report.reused_extent_count,
                        copied_base_extent_count,
                        introduced_extent_count,
                        admitted_memory_bytes: exact.admitted_memory_bytes,
                    });
            let relational_overflow_root =
                hawdb_storage::RelationalOverflowRootReader::open_generation(
                    durable.root_path(),
                    generation,
                    overflow_publication_config,
                )
                .map_err(|error| HawDBError::Storage(error.to_string()))?;

            let row_request = RelationalRowPageGenerationRequest {
                directory: durable.root_path(),
                generation,
                source_commit_epoch: commit_epoch,
                base: row_plan.base.as_deref(),
                expected_previous_generation: durable
                    .relational_row_generation_artifacts
                    .map(|binding| binding.generation),
                overflow_root: Some(&relational_overflow_root),
            };
            let row_publisher = RelationalRowPagePublisher::new(row_publication_config);
            let relational_row_report = match row_compaction.as_ref() {
                Some(compaction) => {
                    row_compaction_checkpoint(compaction.task)?;
                    let result = row_publisher.persist_generation_compacting(
                        row_request,
                        row_plan.deltas,
                        compaction.config.rewrite,
                        compaction.task,
                    );
                    row_compaction_checkpoint(compaction.task)?;
                    result.map_err(row_compaction_publication_error)?
                }
                None => row_publisher
                    .persist_generation(row_request, row_plan.deltas)
                    .map_err(|error| HawDBError::Storage(error.to_string()))?,
            };
            let relational_row_compaction_report = row_compaction
                .as_ref()
                .map(|compaction| {
                    let root = hawdb_storage::RelationalRowPageRootReader::open_generation(
                        durable.root_path(),
                        generation,
                        row_publication_config,
                    )
                    .map_err(row_compaction_publication_error)?;
                    Ok::<_, HawDBError>(RelationalRowPageCompactionReport {
                        source_commit_epoch: commit_epoch,
                        published_generation: generation,
                        root_pages: relational_row_report.root_pages,
                        dirty_pages_written: relational_row_report.dirty_pages_written,
                        relocated_pages_written: relational_row_report.relocated_pages_written,
                        reused_pages: relational_row_report.reused_pages,
                        previous_allocated_pages,
                        allocated_pages: root
                            .manifest()
                            .physical_generations
                            .iter()
                            .map(|entry| entry.allocated_pages)
                            .sum(),
                        admitted_memory_bytes: compaction.admitted_memory_bytes,
                    })
                })
                .transpose()?;
            let relational_index_candidate =
                self.prepare_relational_index_candidate(generation, commit_epoch);
            self.require_authoritative_relational_index_candidate(&relational_index_candidate)?;
            let relational_index = relational_index_candidate
                .as_ref()
                .and_then(|prepared| prepared.candidate.as_ref())
                .map(|report| report.generation_artifacts);
            let checkpoint_artifact = durable.write_checkpoint(
                CheckpointImage {
                    catalog,
                    commit_epoch,
                    next_node_id: self.next_node_id,
                    next_rel_id: self.next_rel_id,
                    search_projection_change_log_start_epoch: self
                        .search_projection_change_log_start_epoch,
                    search_projection_graph_changes: &self.search_projection_graph_changes,
                    statistics: &checkpoint_statistics,
                    projected_graphs: &self.projected_graphs,
                    initial_import_source_fingerprint: self
                        .initial_import_source_fingerprint
                        .as_deref(),
                    search_projection_database_identity: self.search_projection_database_identity,
                    relational_checkpoint: relational_checkpoint_artifact,
                },
                generation,
            )?;
            checkpoint_publish_failpoint(CheckpointPublishStage::CheckpointPersisted)?;
            durable.prepare_wal_generation(generation)?;
            checkpoint_publish_failpoint(CheckpointPublishStage::WalPrepared)?;
            if let Some(compaction) = row_compaction.as_ref() {
                row_compaction_checkpoint(compaction.task)?;
            }
            Ok(PreparedCheckpoint {
                source_commit_epoch: commit_epoch,
                source_checkpoint_epoch: durable.checkpoint_epoch,
                source_next_lsn: durable.next_lsn,
                generation,
                checkpoint_out_of_core,
                projected_graph_artifacts: artifacts,
                publish_projected_graph_artifacts: projected_graph_artifacts.is_some(),
                source_scan_publication,
                checkpoint_statistics,
                checkpoint_relational_state,
                checkpoint_append_reader,
                relational_index_candidate,
                relational_overflow_compaction_report,
                relational_row_compaction_report,
                manifest_artifacts: CheckpointManifestArtifacts {
                    checkpoint: checkpoint_artifact,
                    relational_checkpoint: relational_checkpoint_artifact,
                    canonical_manifest: canonical_manifest_artifact,
                    canonical_adjacency: canonical_adjacency_artifacts,
                    property_spill_manifest: property_spill_manifest_artifact,
                    property_projection_manifest: property_projection_manifest_artifact,
                    relational_row: relational_row_report.generation_artifacts,
                    relational_overflow: relational_overflow_report.generation_artifacts,
                    relational_index,
                    append: append_report.generation_artifacts,
                },
                staging_path: staging_path.clone(),
            })
        })();
        if prepared.is_err() {
            let _ = durable.discard_prepared_checkpoint(generation, &staging_path);
        }
        prepared.map(Some)
    }

    pub(crate) fn publish_prepared_checkpoint(
        &mut self,
        prepared: PreparedCheckpoint,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        self.publish_prepared_checkpoint_with_reclamation(
            prepared,
            oldest_reader_commit_epoch,
            None,
            None,
        )
    }

    /// Publication with an explicit pre-admitted shadow context: callers
    /// that already hold a governor permit (the nowledge_mem typed
    /// checkpoint) extend that single admission by
    /// [`GraphStore::columnar_shadow_admission_bytes`] and pass the token
    /// here; `None` lets the shadow acquire its own single non-nested
    /// admission.
    pub(crate) fn publish_prepared_checkpoint_with_shadow_admission(
        &mut self,
        prepared: PreparedCheckpoint,
        oldest_reader_commit_epoch: Option<u64>,
        shadow_admission: Option<ColumnarShadowAdmission>,
    ) -> Result<()> {
        self.publish_prepared_checkpoint_with_reclamation(
            prepared,
            oldest_reader_commit_epoch,
            None,
            shadow_admission,
        )
    }

    pub(crate) fn publish_prepared_checkpoint_with_reader_generations(
        &mut self,
        prepared: PreparedCheckpoint,
        oldest_reader_commit_epoch: Option<u64>,
        pinned_reader_generations: &BTreeSet<u64>,
        shadow_admission: Option<ColumnarShadowAdmission>,
    ) -> Result<()> {
        self.publish_prepared_checkpoint_with_reclamation(
            prepared,
            oldest_reader_commit_epoch,
            Some(pinned_reader_generations),
            shadow_admission,
        )
    }

    fn publish_prepared_checkpoint_with_reclamation(
        &mut self,
        prepared: PreparedCheckpoint,
        oldest_reader_commit_epoch: Option<u64>,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
        shadow_admission: Option<ColumnarShadowAdmission>,
    ) -> Result<()> {
        let generation = prepared.generation;
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("prepared checkpoint requires durable storage".to_string())
        })?;
        if self.commit_epoch != prepared.source_commit_epoch
            || durable.checkpoint_epoch != prepared.source_checkpoint_epoch
            || durable.next_lsn != prepared.source_next_lsn
        {
            durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path)?;
            return Err(HawDBError::Storage(format!(
                "checkpoint source changed before publication: prepared commit/checkpoint/lsn=({},{},{}), current=({},{},{}); retry checkpoint",
                prepared.source_commit_epoch,
                prepared.source_checkpoint_epoch,
                prepared.source_next_lsn,
                self.commit_epoch,
                durable.checkpoint_epoch,
                durable.next_lsn,
            )));
        }
        if let Err(error) = durable.publish_checkpoint_sidecars(
            &prepared.staging_path,
            prepared.publish_projected_graph_artifacts,
            prepared.source_scan_publication,
        ) {
            let _ =
                durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path);
            return Err(error);
        }
        durable.publish_checkpoint_manifest(
            prepared.generation,
            prepared.manifest_artifacts,
            prepared.source_commit_epoch,
            oldest_reader_commit_epoch,
            prepared.source_scan_publication,
        )?;
        let source_scan_manifest = prepared
            .source_scan_publication
            .map(|publication| {
                source_scan::load(
                    durable.root_path(),
                    prepared.source_commit_epoch,
                    publication.descriptor_checksum(),
                )
            })
            .transpose()?
            .flatten();
        self.projected_graph_artifacts = prepared.projected_graph_artifacts.into();
        self.source_scan_manifest = source_scan_manifest.into();
        self.checkpoint_statistics = prepared.checkpoint_statistics;
        if let Some(relational_state) = prepared.checkpoint_relational_state {
            self.relational_state = relational_state;
        }
        self.append_state = AppendState::from_checkpoint_with_generated_order_watermarks(
            prepared.checkpoint_append_reader.manifest().schemas.clone(),
            prepared.checkpoint_append_reader.watermarks(),
            prepared
                .checkpoint_append_reader
                .generated_order_watermarks()
                .clone(),
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
        self.append_generation_reader = Some(prepared.checkpoint_append_reader);
        if prepared.checkpoint_out_of_core {
            self.canonical_base = durable.canonical_segments.clone();
            self.canonical_adjacency = durable.canonical_adjacency.clone();
            self.persistent_property_projection = durable.persistent_property_projection.clone();
            self.canonical_base_out_of_core = true;
            self.nodes = CowSegmentedMap::default();
            self.relationships = CowSegmentedMap::default();
            self.node_tombstones = CowSegment::default();
            self.relationship_tombstones = CowSegment::default();
            self.outgoing = CowSegmentedMap::default();
            self.incoming = CowSegmentedMap::default();
            self.property_index = CowSegmentedMap::default();
            self.composite_property_index = CowSegmentedMap::default();
            self.full_text_property_index = CowSegmentedMap::default();
            self.relationship_property_index = CowSegmentedMap::default();
        }
        self.mount_relational_row_pages_for_recovery()?;
        self.install_prepared_relational_index_candidate(prepared.relational_index_candidate);
        self.validate_authoritative_relational_index_open()?;
        // Derived shadow double-write: published after the row-oriented
        // checkpoint so its `source_commit_epoch` is the epoch this
        // checkpoint made durable. The checkpoint's Result reflects
        // canonical publication only — a shadow failure is recorded in the
        // shadow report with dirty state preserved, and the next checkpoint
        // retries.
        self.record_columnar_shadow_checkpoint(prepared.source_commit_epoch, shadow_admission);
        // Generation reclamation is post-commit maintenance. It must run only
        // after every in-memory view has adopted the published generation, and
        // its failure must not change the checkpoint outcome.
        if let Some(durable) = self.durable.as_mut() {
            durable.reclaim_old_generations(generation, pinned_reader_generations);
        }
        Ok(())
    }

    /// Checkpoint entry that carries an explicit pre-admitted shadow
    /// context from a caller already holding a governor permit.
    pub fn checkpoint_with_shadow_admission(
        &mut self,
        catalog: &Catalog,
        shadow_admission: ColumnarShadowAdmission,
    ) -> Result<()> {
        let Some(prepared) = self
            .prepare_checkpoint_with_build_config(catalog, DerivedArtifactBuildConfig::default())?
        else {
            return Ok(());
        };
        self.publish_prepared_checkpoint_with_shadow_admission(
            prepared,
            None,
            Some(shadow_admission),
        )
    }

    pub fn rebuild_projected_graph_artifacts(&mut self, catalog: &Catalog) -> Result<()> {
        let projection_epoch = self.next_projection_epoch();
        let projected_graph_artifacts =
            encode_projected_graph_artifacts(catalog, self, projection_epoch);
        let (_, artifacts) = decode_projected_graph_artifacts(&projected_graph_artifacts)?;
        if let Some(durable) = &self.durable {
            durable.write_projected_graph_artifacts(&projected_graph_artifacts)?;
        }
        self.projected_graph_artifacts = artifacts.into();
        Ok(())
    }

    pub fn storage_reclamation_watermark(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StorageReclamationWatermark {
        match &self.durable {
            Some(durable) => {
                let safe_reclaim_commit_epoch =
                    if oldest_reader_commit_epoch == durable.oldest_reader_commit_epoch {
                        durable.safe_reclaim_commit_epoch
                    } else {
                        safe_reclaim_commit_epoch(
                            durable.checkpoint_commit_epoch,
                            oldest_reader_commit_epoch,
                        )
                    };
                StorageReclamationWatermark {
                    current_commit_epoch: self.commit_epoch,
                    checkpoint_epoch: Some(durable.checkpoint_epoch),
                    checkpoint_commit_epoch: Some(durable.checkpoint_commit_epoch),
                    oldest_reader_commit_epoch,
                    safe_reclaim_commit_epoch,
                    durable: true,
                }
            }
            None => StorageReclamationWatermark {
                current_commit_epoch: self.commit_epoch,
                checkpoint_epoch: None,
                checkpoint_commit_epoch: None,
                oldest_reader_commit_epoch,
                safe_reclaim_commit_epoch: safe_reclaim_commit_epoch(
                    self.commit_epoch,
                    oldest_reader_commit_epoch,
                ),
                durable: false,
            },
        }
    }

    pub fn storage_recovery_report(&self) -> StorageRecoveryReport {
        self.storage_recovery_report.clone()
    }

    pub fn segment_cache_snapshot(&self) -> Option<SegmentCacheSnapshot> {
        self.durable
            .as_ref()
            .map(|durable| durable.segment_cache.snapshot())
    }

    pub fn canonical_segment_manifest(&self) -> Option<&CanonicalSegmentManifest> {
        self.durable
            .as_ref()?
            .canonical_segments
            .as_ref()
            .map(CanonicalSegmentReader::manifest)
    }

    pub fn property_spill_manifest(&self) -> Option<&PropertySpillManifest> {
        self.durable
            .as_ref()?
            .canonical_segments
            .as_ref()?
            .property_spill_manifest()
    }

    pub fn persistent_property_projection_manifest(
        &self,
    ) -> Option<&PersistentPropertyProjectionManifest> {
        self.durable
            .as_ref()?
            .persistent_property_projection
            .as_ref()
            .map(PersistentPropertyProjectionReader::manifest)
    }

    pub fn storage_residency_report(&self) -> StorageResidencyReport {
        let manifest = self
            .canonical_base
            .as_ref()
            .map(CanonicalSegmentReader::manifest);
        let estimated_delta_resident_bytes = self.estimated_delta_resident_bytes();
        let cache = self.segment_cache_snapshot().unwrap_or_default();
        let (graph_manifest_open_budget_bytes, graph_manifest_encoded_bytes) =
            self.durable.as_ref().map_or((0, 0), |durable| {
                (
                    durable.graph_manifest_open_budget_bytes(),
                    durable.graph_manifest_encoded_bytes(),
                )
            });
        StorageResidencyReport {
            out_of_core: self.canonical_base_out_of_core,
            canonical_generation: manifest.map(|manifest| manifest.generation.0),
            canonical_artifact_bytes: manifest.map_or(0, |manifest| manifest.artifact_len),
            canonical_adjacency_artifact_bytes: self
                .canonical_adjacency
                .as_ref()
                .map_or(0, CanonicalAdjacencyReader::artifact_len),
            persistent_property_projection_artifact_bytes: self
                .persistent_property_projection
                .as_ref()
                .map_or(0, |reader| reader.manifest().artifact_len),
            canonical_node_count: manifest.map_or(0, |manifest| manifest.node_count),
            canonical_relationship_count: manifest
                .map_or(0, |manifest| manifest.relationship_count),
            delta_node_count: self.nodes.len(),
            delta_relationship_count: self.relationships.len(),
            node_tombstone_count: self.node_tombstones.len(),
            relationship_tombstone_count: self.relationship_tombstones.len(),
            estimated_delta_resident_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            delta_within_budget: self
                .max_out_of_core_delta_bytes
                .is_none_or(|limit| estimated_delta_resident_bytes <= limit),
            checkpoint_statistics_commit_epoch: self.checkpoint_statistics.computed_at_commit_epoch,
            checkpoint_statistics_complete: self.checkpoint_statistics.advanced_statistics_complete,
            checkpoint_statistics_stale: self
                .checkpoint_statistics
                .advanced_statistics_freshness(self.commit_epoch)
                != AdvancedStatisticsFreshness::Fresh,
            graph_manifest_open_budget_bytes,
            graph_manifest_encoded_bytes,
            segment_cache_capacity_bytes: cache.capacity_bytes,
            segment_cache_resident_bytes: cache.resident_bytes,
            segment_cache_pinned_bytes: cache.pinned_bytes,
            segment_cache_hit_count: cache.hit_count,
            segment_cache_miss_count: cache.miss_count,
            segment_cache_eviction_count: cache.eviction_count,
            segment_cache_admission_rejection_count: cache.admission_rejection_count,
            segment_cache_digest_mismatch_count: cache.digest_mismatch_count,
            graph_index_reads: self.graph_index_read_metrics.snapshot(),
            relational_rows: self
                .relational_row_pages
                .residency_report(self.commit_epoch, &self.relational_state),
            relational_indexes: self
                .relational_index_shadow
                .residency_report(self.commit_epoch),
        }
    }

    pub fn storage_pressure_snapshot(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StoragePressureSnapshot {
        let available_free_space_bytes = self
            .durable
            .as_ref()
            .and_then(|durable| available_storage_space(durable.root_path()));
        StorageDebtController.evaluate(
            self.storage_pressure_signals(oldest_reader_commit_epoch, available_free_space_bytes),
        )
    }

    pub(super) fn storage_pressure_signals(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
        available_free_space_bytes: Option<u64>,
    ) -> StoragePressureSignals {
        let cache = self.segment_cache_snapshot().unwrap_or_default();
        let checkpoint_commit_epoch = self
            .durable
            .as_ref()
            .map_or(self.commit_epoch, |durable| durable.checkpoint_commit_epoch);
        let (
            wal_bytes,
            wal_age_millis,
            max_wal_bytes,
            obsolete_generation_bytes,
            generation_reclamation_retry_required,
            generation_reclamation_pending_files,
            generation_reclamation_pending_bytes,
        ) = self
            .durable
            .as_ref()
            .map_or((0, 0, None, 0, false, 0, 0), |durable| {
                let reclamation = durable.generation_reclamation_debt();
                (
                    durable.wal_bytes,
                    (self.commit_epoch > durable.checkpoint_commit_epoch)
                        .then(|| durable.wal_age_millis())
                        .flatten()
                        .unwrap_or_default(),
                    durable.max_wal_bytes,
                    durable.obsolete_generation_bytes(oldest_reader_commit_epoch),
                    reclamation.retry_required,
                    reclamation.pending_file_count,
                    reclamation.pending_bytes,
                )
            });
        let has_checkpoint_debt =
            self.durable.is_some() && self.commit_epoch > checkpoint_commit_epoch;
        let estimated_checkpoint_temporary_bytes = if has_checkpoint_debt {
            self.estimated_logical_record_bytes()
                .saturating_add(self.relational_state.estimated_checkpoint_bytes())
                .saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER)
                .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES)
        } else {
            0
        };
        let property_projection_debt =
            self.persistent_property_projection
                .as_ref()
                .map_or(0, |reader| {
                    usize::try_from(
                        self.commit_epoch
                            .saturating_sub(reader.manifest().source_commit_epoch),
                    )
                    .unwrap_or(usize::MAX)
                });

        StoragePressureSignals {
            current_commit_epoch: self.commit_epoch,
            checkpoint_commit_epoch,
            wal_bytes,
            wal_age_millis,
            max_wal_bytes,
            delta_bytes: if self.canonical_base_out_of_core {
                self.estimated_delta_resident_bytes()
            } else {
                0
            },
            max_delta_bytes: if self.canonical_base_out_of_core {
                self.max_out_of_core_delta_bytes
            } else {
                None
            },
            adjacency_debt_entries: self.adjacency_consolidation_plan().estimated_entries,
            projection_debt_operations: property_projection_debt,
            generation_reclamation_retry_required,
            generation_reclamation_pending_files,
            generation_reclamation_pending_bytes,
            oldest_reader_commit_epoch,
            obsolete_generation_bytes,
            estimated_checkpoint_temporary_bytes,
            available_free_space_bytes,
            cache_capacity_bytes: cache.capacity_bytes,
            cache_resident_bytes: cache.resident_bytes,
            cache_pinned_bytes: cache.pinned_bytes,
            integrity_poisoned: self.storage_handle_poisoned(),
        }
    }

    pub(crate) fn checkpoint_estimated_operations(&self) -> usize {
        let statistics = self.basic_statistics();
        let graph_operations = statistics
            .node_count
            .saturating_add(statistics.relationship_count);
        usize::try_from(graph_operations)
            .unwrap_or(usize::MAX)
            .saturating_add(self.relational_state.total_row_count())
            .max(1)
    }

    pub(super) fn estimated_delta_resident_bytes(&self) -> u64 {
        let record_bytes = self
            .nodes
            .values()
            .fold(0u64, |bytes, node| {
                bytes.saturating_add(estimated_node_record_bytes(node))
            })
            .saturating_add(
                self.relationships
                    .values()
                    .fold(0u64, |bytes, relationship| {
                        bytes.saturating_add(estimated_relationship_record_bytes(relationship))
                    }),
            );
        let tombstone_bytes = (self
            .node_tombstones
            .len()
            .saturating_add(self.relationship_tombstones.len())
            as u64)
            .saturating_mul(32);
        let adjacency_bytes =
            self.outgoing
                .values()
                .chain(self.incoming.values())
                .fold(0u64, |bytes, posting| {
                    bytes
                        .saturating_add(48)
                        .saturating_add((posting.len() as u64).saturating_mul(24))
                });
        let node_index_bytes = self
            .property_index
            .values()
            .chain(self.composite_property_index.values())
            .chain(self.full_text_property_index.values())
            .fold(0u64, |bytes, posting| {
                bytes
                    .saturating_add(64)
                    .saturating_add((posting.len() as u64).saturating_mul(24))
            });
        let relationship_index_bytes =
            self.relationship_property_index
                .values()
                .fold(0u64, |bytes, posting| {
                    bytes
                        .saturating_add(64)
                        .saturating_add((posting.len() as u64).saturating_mul(24))
                });
        record_bytes
            .saturating_add(tombstone_bytes)
            .saturating_add(adjacency_bytes)
            .saturating_add(node_index_bytes)
            .saturating_add(relationship_index_bytes)
    }

    pub(super) fn estimated_logical_record_bytes(&self) -> u64 {
        let base_bytes = self
            .canonical_base
            .as_ref()
            .map_or(0, |reader| reader.manifest().artifact_len);
        self.nodes
            .values()
            .fold(base_bytes, |bytes, node| {
                bytes.saturating_add(estimated_node_record_bytes(node))
            })
            .saturating_add(self.relationships.values().fold(0, |bytes, relationship| {
                bytes.saturating_add(estimated_relationship_record_bytes(relationship))
            }))
    }
}
