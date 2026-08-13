//! Checkpoint preparation and publication, backup, residency and pressure reporting, and published-manifest accessors for [`GraphStore`].

use super::*;

impl GraphStore {
    pub fn checkpoint(&mut self, catalog: &Catalog) -> Result<()> {
        self.checkpoint_with_reader_epoch(catalog, None)
    }

    pub fn backup_to(
        &mut self,
        catalog: &Catalog,
        destination: impl AsRef<Path>,
    ) -> Result<StorageBackupReport> {
        if self.durable.is_none() {
            return Err(SkeinError::Storage(
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
            SkeinError::Storage("an in-memory database has no durable storage to scrub".to_string())
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
        let property_projection_nodes = self.canonical_base.as_ref().map(|_| {
            self.node_records_owned().map(|record| {
                record.map_err(|error| {
                    skein_storage::PersistentPropertyProjectionError::Source(error.to_string())
                })
            })
        });
        let property_projection_definitions = catalog
            .property_indexes()
            .filter_map(|index| {
                let kind = match index.kind {
                    IndexKind::Range => PersistentPropertyProjectionKind::Range,
                    IndexKind::FullText => PersistentPropertyProjectionKind::FullText,
                    IndexKind::Equality => return None,
                };
                Some(PersistentPropertyProjectionDefinition {
                    label_id: index.label_id,
                    property: index.property.clone(),
                    kind,
                    complete: false,
                })
            })
            .collect::<Vec<_>>();
        let merged_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| CanonicalSegmentError::Source(error.to_string()))
            })
        });
        let adjacency_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| {
                    skein_storage::CanonicalAdjacencyError::Source(error.to_string())
                })
            })
        });
        let commit_epoch = self.commit_epoch;
        let checkpoint_statistics = if checkpoint_out_of_core && !self.canonical_base_out_of_core {
            graph_statistics_from_basic(self.basic_statistics(), false)
        } else {
            self.statistics()
        };
        let generation = durable.checkpoint_epoch.saturating_add(1);
        let staging_path = durable.prepare_checkpoint_staging(generation)?;
        let prepared = (|| {
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
                    (Some(nodes), Some(relationships)) => {
                        durable.write_canonical_segments(nodes, relationships, generation)?
                    }
                    (None, None) => durable.write_canonical_segments(
                        self.nodes.values().map(|node| Ok(node.clone())),
                        self.relationships
                            .values()
                            .map(|relationship| Ok(relationship.clone())),
                        generation,
                    )?,
                    _ => unreachable!("canonical base iterators are created together"),
                };
            let canonical_adjacency_manifest_artifact = match adjacency_relationships {
                Some(relationships) => durable.write_canonical_adjacency(
                    relationships,
                    generation,
                    build_config.adjacency,
                )?,
                None => durable.write_canonical_adjacency(
                    self.relationships.values().cloned().map(Ok),
                    generation,
                    build_config.adjacency,
                )?,
            };
            let property_projection_manifest_artifact = match property_projection_nodes {
                Some(nodes) => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    nodes,
                    generation,
                    commit_epoch,
                    build_config.property_projection,
                )?,
                None => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    self.nodes.values().cloned().map(Ok),
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
                    decode_relational_checkpoint_file(
                        &durable
                            .root_path()
                            .join(relational_checkpoint_generation_file(generation)),
                        RelationalDecodeLimits::checkpoint(),
                    )
                    .map(|checkpoint| checkpoint.state)
                    .map_err(|error| SkeinError::Storage(error.to_string()))
                })
                .transpose()?;
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
                    relational_checkpoint: relational_checkpoint_artifact,
                },
                generation,
            )?;
            checkpoint_publish_failpoint(CheckpointPublishStage::CheckpointPersisted)?;
            durable.prepare_wal_generation(generation)?;
            checkpoint_publish_failpoint(CheckpointPublishStage::WalPrepared)?;
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
                manifest_artifacts: CheckpointManifestArtifacts {
                    checkpoint: checkpoint_artifact,
                    relational_checkpoint: relational_checkpoint_artifact,
                    canonical_manifest: canonical_manifest_artifact,
                    canonical_adjacency_manifest: canonical_adjacency_manifest_artifact,
                    property_spill_manifest: property_spill_manifest_artifact,
                    property_projection_manifest: property_projection_manifest_artifact,
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
        self.publish_prepared_checkpoint_with_shadow_admission(
            prepared,
            oldest_reader_commit_epoch,
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
        let durable = self.durable.as_mut().ok_or_else(|| {
            SkeinError::Storage("prepared checkpoint requires durable storage".to_string())
        })?;
        if self.commit_epoch != prepared.source_commit_epoch
            || durable.checkpoint_epoch != prepared.source_checkpoint_epoch
            || durable.next_lsn != prepared.source_next_lsn
        {
            durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path)?;
            return Err(SkeinError::Storage(format!(
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
        // Shadow double-write (spec §3.7): published after the row-oriented
        // checkpoint so its `source_commit_epoch` is the epoch this
        // checkpoint made durable. The checkpoint's Result reflects
        // canonical publication only — a shadow failure is recorded in the
        // shadow report with dirty state preserved, and the next checkpoint
        // retries.
        self.record_columnar_shadow_checkpoint(prepared.source_commit_epoch, shadow_admission);
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

    pub fn canonical_adjacency_manifest(&self) -> Option<&CanonicalAdjacencyManifest> {
        self.durable
            .as_ref()?
            .canonical_adjacency
            .as_ref()
            .map(CanonicalAdjacencyReader::manifest)
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
        StorageResidencyReport {
            out_of_core: self.canonical_base_out_of_core,
            canonical_generation: manifest.map(|manifest| manifest.generation.0),
            canonical_artifact_bytes: manifest.map_or(0, |manifest| manifest.artifact_len),
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
            checkpoint_statistics_stale: !self.checkpoint_statistics.advanced_statistics_complete
                || self.checkpoint_statistics.computed_at_commit_epoch < self.commit_epoch,
            segment_cache_capacity_bytes: cache.capacity_bytes,
            segment_cache_resident_bytes: cache.resident_bytes,
            segment_cache_pinned_bytes: cache.pinned_bytes,
            segment_cache_hit_count: cache.hit_count,
            segment_cache_miss_count: cache.miss_count,
            segment_cache_eviction_count: cache.eviction_count,
            segment_cache_admission_rejection_count: cache.admission_rejection_count,
            segment_cache_digest_mismatch_count: cache.digest_mismatch_count,
        }
    }

    pub fn storage_pressure_snapshot(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StoragePressureSnapshot {
        let cache = self.segment_cache_snapshot().unwrap_or_default();
        let checkpoint_commit_epoch = self
            .durable
            .as_ref()
            .map_or(self.commit_epoch, |durable| durable.checkpoint_commit_epoch);
        let (wal_bytes, wal_age_millis, max_wal_bytes, obsolete_generation_bytes) =
            self.durable.as_ref().map_or((0, 0, None, 0), |durable| {
                (
                    durable.wal_bytes,
                    (self.commit_epoch > durable.checkpoint_commit_epoch)
                        .then(|| durable.wal_age_millis())
                        .flatten()
                        .unwrap_or_default(),
                    durable.max_wal_bytes,
                    durable.obsolete_generation_bytes(oldest_reader_commit_epoch),
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

        StorageDebtController.evaluate(StoragePressureSignals {
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
            oldest_reader_commit_epoch,
            obsolete_generation_bytes,
            estimated_checkpoint_temporary_bytes,
            available_free_space_bytes: self
                .durable
                .as_ref()
                .and_then(|durable| available_storage_space(durable.root_path())),
            cache_capacity_bytes: cache.capacity_bytes,
            cache_resident_bytes: cache.resident_bytes,
            cache_pinned_bytes: cache.pinned_bytes,
            integrity_poisoned: self.storage_handle_poisoned(),
        })
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
