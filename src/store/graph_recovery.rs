//! Checkpoint mounting, WAL replay, derived-state loading, and snapshot import paths for [`GraphStore`].

use super::*;

impl GraphStore {
    pub fn import_graph_snapshot_rows(
        &mut self,
        catalog: &mut Catalog,
        nodes: Vec<GraphSnapshotNodeImport>,
        relationships: Vec<GraphSnapshotRelationshipImport>,
    ) -> Result<()> {
        if self.basic_statistics.node_count != 0 || self.basic_statistics.relationship_count != 0 {
            return Err(SkeinError::Storage(
                "Skein Lightning initial import requires an empty target graph".to_string(),
            ));
        }
        let mut node_ids = BTreeSet::new();
        for (id, label, _) in &nodes {
            if label.is_empty() {
                return Err(SkeinError::Storage(
                    "Skein Lightning initial import node label is empty".to_string(),
                ));
            }
            if !node_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import duplicate node id {}",
                    id.0
                )));
            }
        }
        let mut relationship_ids = BTreeSet::new();
        for (id, source, target, rel_type, _) in &relationships {
            if rel_type.is_empty() {
                return Err(SkeinError::Storage(
                    "Skein Lightning initial import relationship type is empty".to_string(),
                ));
            }
            if !relationship_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import duplicate relationship id {}",
                    id.0
                )));
            }
            if !node_ids.contains(source) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import relationship {} references missing source node {}",
                    id.0, source.0
                )));
            }
            if !node_ids.contains(target) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import relationship {} references missing target node {}",
                    id.0, target.0
                )));
            }
        }
        if nodes.is_empty() && relationships.is_empty() {
            return Ok(());
        }

        let mut working_catalog = catalog.clone();
        for (_, label, _) in &nodes {
            working_catalog.get_or_create_label(label);
        }
        for (_, _, _, rel_type, _) in &relationships {
            working_catalog.get_or_create_rel_type(rel_type);
        }
        let mut ops = Vec::with_capacity(nodes.len() + relationships.len());
        ops.extend(
            nodes
                .iter()
                .map(|(id, label, properties)| WalOp::CreateNode {
                    id: *id,
                    label: label.clone(),
                    properties: properties.clone(),
                }),
        );
        ops.extend(
            relationships
                .iter()
                .map(
                    |(id, source, target, rel_type, properties)| WalOp::CreateRelationship {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type.clone(),
                        properties: properties.clone(),
                    },
                ),
        );
        self.validate_constraints_for_ops(&working_catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(
            &working_catalog,
            self.commit_epoch + 1,
            &ops,
        );
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(())
    }

    pub(crate) fn import_skein_snapshot_rows_with_source_fingerprint(
        &mut self,
        catalog: &mut Catalog,
        import: SkeinSnapshotRowsImport,
    ) -> Result<()> {
        let SkeinSnapshotRowsImport {
            stable_id_mapping,
            source_fingerprint,
            nodes,
            relationships,
            relational_state,
            target_has_only_engine_bootstrap,
        } = import;
        if self.initial_import_source_fingerprint.is_some()
            || !self.nodes.is_empty()
            || !self.relationships.is_empty()
            || (!self.relational_state.is_empty() && !target_has_only_engine_bootstrap)
            || !catalog.is_empty()
        {
            return Err(SkeinError::Storage(
                "skein lightning initial import requires an empty target database".to_string(),
            ));
        }

        let mut node_ids = BTreeSet::new();
        for (id, label, _) in &nodes {
            if label.is_empty() {
                return Err(SkeinError::Storage(
                    "skein lightning initial import node label is empty".to_string(),
                ));
            }
            if !node_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import duplicate node id {}",
                    id.0
                )));
            }
        }
        let mut relationship_ids = BTreeSet::new();
        for (id, source, target, rel_type, _) in &relationships {
            if rel_type.is_empty() {
                return Err(SkeinError::Storage(
                    "skein lightning initial import relationship type is empty".to_string(),
                ));
            }
            if !relationship_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import duplicate relationship id {}",
                    id.0
                )));
            }
            if !node_ids.contains(source) || !node_ids.contains(target) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import relationship {} references a missing endpoint",
                    id.0
                )));
            }
        }

        let mut working_catalog = catalog.clone();
        for (_, label, _) in &nodes {
            working_catalog.get_or_create_label(label);
        }
        for (_, _, _, rel_type, _) in &relationships {
            working_catalog.get_or_create_rel_type(rel_type);
        }
        let target_commit_epoch = self
            .commit_epoch
            .checked_add(1)
            .ok_or_else(|| SkeinError::Storage("commit epoch overflow".to_string()))?;
        let relational_record =
            encode_relational_checkpoint(target_commit_epoch, &relational_state)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let mut ops = Vec::with_capacity(nodes.len() + relationships.len() + 2);
        ops.push(WalOp::MarkInitialImportSource { source_fingerprint });
        ops.push(WalOp::RelationalSnapshot {
            record: Arc::from(relational_record),
        });
        ops.extend(
            nodes
                .iter()
                .map(|(id, label, properties)| WalOp::CreateNode {
                    id: *id,
                    label: label.clone(),
                    properties: properties.clone(),
                }),
        );
        ops.extend(
            relationships
                .iter()
                .map(
                    |(id, source, target, rel_type, properties)| WalOp::CreateRelationship {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type.clone(),
                        properties: properties.clone(),
                    },
                ),
        );
        self.validate_constraints_for_ops(&working_catalog, &ops)?;

        // The mapping is durable before the WAL batch; recovery never observes imported
        // graph rows without the stable identities required to address them.
        self.replace_stable_id_mapping_for_epoch(stable_id_mapping, target_commit_epoch)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(
            &working_catalog,
            target_commit_epoch,
            &ops,
        );
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(())
    }

    pub(super) fn load_source_scan_manifest(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.source_scan_manifest = durable.load_source_scan_manifest(self.commit_epoch)?.into();
        Ok(())
    }

    pub(super) fn load_projected_graph_artifacts(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.projected_graph_artifacts = durable
            .load_projected_graph_artifacts()?
            .into_iter()
            .filter(|(name, artifact)| {
                artifact.commit_epoch == self.commit_epoch
                    && self
                        .projected_graphs
                        .get(name)
                        .is_some_and(|definition| definition == &artifact.definition)
            })
            .collect::<BTreeMap<_, _>>()
            .into();
        Ok(())
    }

    pub(super) fn load_stable_id_mapping(&mut self) -> Result<()> {
        let Some(durable) = &mut self.durable else {
            return Ok(());
        };
        durable.load_stable_id_mapping()?;
        self.stable_id_mapping = CowSegment::default();
        Ok(())
    }

    pub(super) fn load_checkpoint(
        &mut self,
        catalog: &mut Catalog,
        config: WalReplayConfig,
    ) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        if !durable.checkpoint_path.exists() {
            if durable.checkpoint_commit_epoch != 0 {
                return Err(SkeinError::Storage(format!(
                    "manifest checkpoint generation {} is missing",
                    durable.checkpoint_epoch
                )));
            }
            return Ok(());
        }
        let expected_generation = durable.checkpoint_epoch;
        let expected_commit_epoch = durable.checkpoint_commit_epoch;
        let durable_root_path = durable.root_path.clone();
        let text = durable.read_checkpoint_text(config)?;
        let (body, checksum) = split_checkpoint_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "checkpoint checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        let relational_checkpoint = relational_checkpoint_metadata(body)?;
        let canonical_relational_manifest = if config.residency_mode
            == StorageResidencyMode::OutOfCore
            && config
                .relational_index_mode
                .requires_authoritative_indexes()
        {
            let overflow = durable.open_bound_relational_overflow()?;
            Some(
                durable
                    .open_bound_relational_row_pages(&overflow)?
                    .manifest()
                    .clone(),
            )
        } else {
            None
        };
        if relational_checkpoint.is_none() && canonical_relational_manifest.is_none() {
            let overflow = durable.open_bound_relational_overflow()?;
            let rows = durable.open_bound_relational_row_pages(&overflow)?;
            if !rows.manifest().tables.is_empty() {
                return Err(SkeinError::Storage(
                    "checkpoint stores canonical metadata-only relational rows; reopen requires OutOfCore residency with Authoritative relational indexes"
                        .to_string(),
                ));
            }
        }
        let mut decoded = skein_storage::checkpoint::DecodedCheckpoint::default();
        skein_storage::checkpoint::parse_checkpoint(body, catalog, &mut decoded)?;
        self.next_node_id = decoded.next_node_id;
        self.next_rel_id = decoded.next_rel_id;
        if let Some(commit_epoch) = decoded.commit_epoch {
            self.commit_epoch = commit_epoch;
        }
        if let Some(start_epoch) = decoded.search_projection_change_log_start_epoch {
            self.search_projection_change_log_start_epoch = start_epoch;
        }
        self.search_projection_change_log_retained_bytes =
            decoded.search_projection_change_log_retained_bytes;
        self.search_projection_database_identity = decoded.search_projection_database_identity;
        self.initial_import_source_fingerprint = decoded.initial_import_source_fingerprint;
        self.search_projection_graph_changes = decoded.search_projection_graph_changes.into();
        self.basic_statistics = decoded.basic_statistics;
        self.checkpoint_statistics = decoded.checkpoint_statistics;
        for (name, definition) in decoded.projected_graphs {
            self.apply_project_graph_definition(name, definition);
        }
        if !decoded.nodes.is_empty() || !decoded.relationships.is_empty() {
            // Applying inline records rebuilds the basic counts. The decoded
            // totals must not be counted again during that reconstruction.
            self.basic_statistics = BasicGraphStatistics::default();
        }
        for node in decoded.nodes {
            self.apply_create_node_with_labels(catalog, node.id, node.labels, node.properties);
        }
        for relationship in decoded.relationships {
            self.apply_create_relationship(
                relationship.id,
                relationship.source,
                relationship.target,
                relationship.rel_type,
                relationship.properties,
            );
        }
        let loaded_search_projection_change_log_start_epoch =
            decoded.search_projection_change_log_start_epoch;
        let loaded_generation = decoded.generation;
        let loaded_commit_epoch = decoded.commit_epoch;
        let loaded_statistics_complete = decoded.statistics_complete;
        let saw_checkpoint_statistics = decoded.saw_checkpoint_statistics;
        let canonical_records = decoded.canonical_records;
        if saw_checkpoint_statistics {
            self.checkpoint_statistics.advanced_statistics_complete =
                loaded_statistics_complete.unwrap_or(true);
            retain_supported_property_statistics(&mut self.checkpoint_statistics, Some(catalog));
            retain_valid_index_statistics_samples(&mut self.checkpoint_statistics, catalog);
        }
        if loaded_generation != Some(expected_generation) {
            return Err(SkeinError::Storage(format!(
                "checkpoint generation {:?} does not match manifest generation {expected_generation}",
                loaded_generation
            )));
        }
        if loaded_commit_epoch != Some(expected_commit_epoch) {
            return Err(SkeinError::Storage(format!(
                "checkpoint commit epoch {:?} does not match manifest commit epoch {expected_commit_epoch}",
                loaded_commit_epoch
            )));
        }
        if let Some(metadata) = relational_checkpoint {
            let path =
                durable_root_path.join(relational_checkpoint_generation_file(expected_generation));
            let max_bytes = RelationalDecodeLimits::checkpoint().max_record_bytes;
            let file_len = fs::metadata(&path)?.len();
            if file_len > max_bytes as u64 {
                return Err(SkeinError::Storage(format!(
                    "relational checkpoint contains {file_len} bytes, exceeding max_record_bytes {max_bytes}"
                )));
            }
            verify_file_integrity(
                &path,
                metadata.encoded_len,
                metadata.encoded_checksum,
                metadata.encoded_sha256,
                "relational checkpoint",
            )?;
            if canonical_relational_manifest.is_none() {
                let index_load = if config
                    .relational_index_mode
                    .requires_authoritative_indexes()
                {
                    RelationalCheckpointIndexLoad::OmitMaterializedPostings
                } else {
                    RelationalCheckpointIndexLoad::MaterializedPostings
                };
                let checkpoint = decode_relational_checkpoint_file_with_index_load(
                    &path,
                    RelationalDecodeLimits::checkpoint(),
                    index_load,
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                if checkpoint.epoch != self.commit_epoch {
                    return Err(SkeinError::Storage(format!(
                        "relational checkpoint epoch {} does not match graph commit epoch {}",
                        checkpoint.epoch, self.commit_epoch
                    )));
                }
                self.relational_state = checkpoint.state;
            }
        }
        if let Some(row_root) = canonical_relational_manifest.as_ref() {
            if row_root.source_commit_epoch != self.commit_epoch {
                return Err(SkeinError::Storage(format!(
                    "relational row root epoch {} does not match graph commit epoch {}",
                    row_root.source_commit_epoch, self.commit_epoch
                )));
            }
            self.relational_state = RelationalState::from_canonical_row_root(row_root)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        }
        match loaded_search_projection_change_log_start_epoch {
            Some(start_epoch) => {
                validate_search_projection_checkpoint_changes(
                    start_epoch,
                    self.commit_epoch,
                    &self.search_projection_graph_changes,
                )?;
            }
            None => {
                return Err(SkeinError::Storage(
                    "checkpoint search projection changes are missing their start epoch"
                        .to_string(),
                ));
            }
        }
        if canonical_records {
            let reader = self
                .durable
                .as_ref()
                .and_then(|durable| durable.canonical_segments.clone())
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "checkpoint delegates records to missing canonical segments".to_string(),
                    )
                })?;
            let materialize = match config.residency_mode {
                StorageResidencyMode::Materialized => true,
                StorageResidencyMode::OutOfCore => false,
                StorageResidencyMode::Auto => {
                    reader.manifest().artifact_len <= config.auto_materialize_checkpoint_bytes
                }
            };
            if materialize {
                self.basic_statistics = BasicGraphStatistics::default();
                reader
                    .scan_nodes(|node| {
                        self.apply_create_node_with_labels(
                            catalog,
                            node.id,
                            node.labels,
                            node.properties,
                        );
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                reader
                    .scan_relationships(|relationship| {
                        self.apply_create_relationship(
                            relationship.id,
                            relationship.source,
                            relationship.target,
                            relationship.rel_type,
                            relationship.properties,
                        );
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            } else {
                self.basic_statistics.node_count = reader.manifest().node_count;
                self.basic_statistics.relationship_count = reader.manifest().relationship_count;
                self.canonical_base = Some(reader);
                self.canonical_adjacency = self
                    .durable
                    .as_ref()
                    .and_then(|durable| durable.canonical_adjacency.clone());
                self.persistent_property_projection = self
                    .durable
                    .as_ref()
                    .and_then(|durable| durable.persistent_property_projection.clone());
                self.canonical_base_out_of_core = true;
            }
        }
        if let Some(durable) = &mut self.durable {
            durable.relational_checkpoint_encoded_len =
                relational_checkpoint.map(|metadata| metadata.encoded_len);
            durable.relational_checkpoint_encoded_checksum =
                relational_checkpoint.map(|metadata| metadata.encoded_checksum);
            durable.relational_checkpoint_encoded_sha256 =
                relational_checkpoint.map(|metadata| metadata.encoded_sha256);
        }
        Ok(())
    }

    pub(super) fn replay_wal(
        &mut self,
        catalog: &mut Catalog,
        config: WalReplayConfig,
    ) -> Result<StorageRecoveryReport> {
        let Some(durable) = &self.durable else {
            return Ok(StorageRecoveryReport::default());
        };
        let wal_path = durable.wal_path.clone();
        let checkpoint_epoch = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let wal_replay_start_lsn = durable.wal_replay_start_lsn;
        let wal_generation = durable.wal_generation;
        let read_only = durable.read_only;
        let mut tail_repair = durable.wal_tail_repair.clone();
        let mut torn_tail_reason = tail_repair
            .as_ref()
            .map(|_| "resumed interrupted repair of an incomplete final WAL record".to_string());
        let checkpoint_present = durable.checkpoint_path.exists();
        let mut replayed_entries = 0_usize;
        let mut replayed_bytes = 0u64;
        let wal_present = wal_path.exists();
        if !wal_path.exists() {
            if checkpoint_epoch > 0 {
                return Err(SkeinError::Storage(format!(
                    "manifest WAL generation {wal_generation} is missing"
                )));
            }
            return Ok(StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: config.recovery_mode,
                max_wal_replay_entries: config.max_entries,
                max_wal_replay_bytes: config.max_bytes,
                max_wal_record_bytes: config.max_record_bytes,
                checkpoint_epoch: checkpoint_present.then_some(checkpoint_epoch),
                checkpoint_commit_epoch: checkpoint_present.then_some(checkpoint_commit_epoch),
                wal_present,
                wal_generation: Some(wal_generation),
                wal_replay_start_lsn: Some(wal_replay_start_lsn),
                next_lsn_after_replay: Some(wal_replay_start_lsn),
                replayed_wal_entries: replayed_entries,
                replayed_wal_bytes: replayed_bytes,
                torn_tail_ignored: false,
                torn_tail_repaired: false,
                discarded_wal_tail_bytes: 0,
                torn_tail_reason: None,
                recovered_commit_epoch: self.commit_epoch,
            });
        }
        let wal_len = fs::metadata(&wal_path)?.len();
        if config.max_bytes.is_some_and(|limit| wal_len > limit) {
            return Err(SkeinError::Storage(format!(
                "WAL replay byte limit exceeded: max_wal_replay_bytes={}",
                config.max_bytes.unwrap_or_default()
            )));
        }
        let mut cursor = match WalRecordCursor::open(&wal_path, config.max_record_bytes)? {
            WalOpenOutcome::Cursor(cursor) => cursor,
            WalOpenOutcome::MissingHeader => {
                return Err(SkeinError::Storage(format!(
                    "WAL generation {wal_generation} is missing its header"
                )));
            }
            WalOpenOutcome::HeaderTorn { reason } => {
                return Err(SkeinError::Storage(reason));
            }
            WalOpenOutcome::HeaderCorrupt { reason } => {
                return reject_corrupt_wal_record(
                    &wal_path,
                    wal_generation,
                    read_only,
                    config.max_quarantine_bytes,
                    0,
                    reason,
                );
            }
        };
        if cursor.generation() != wal_generation || cursor.start_lsn() != wal_replay_start_lsn {
            return Err(SkeinError::Storage(format!(
                "WAL header generation/start ({}, {}) does not match manifest ({wal_generation}, {wal_replay_start_lsn})",
                cursor.generation(),
                cursor.start_lsn()
            )));
        }
        let mut expected_lsn = wal_replay_start_lsn;
        let mut relational_recovery_source =
            RelationalRecoverySourceBuilder::new(wal_generation, wal_replay_start_lsn);
        loop {
            let (entry, record_start, record_encoded_len, payload_len, payload_sha256) =
                match cursor.next()? {
                    WalCursorEvent::Eof => break,
                    WalCursorEvent::TornTail { reason, .. }
                        if config.recovery_mode == RecoveryMode::AutoRepairTornTail
                            && !read_only =>
                    {
                        // Release the read handle before resizing the WAL on Windows.
                        drop(cursor);
                        let durable = self.durable.as_mut().expect("durable WAL replay");
                        let repair = super::doctor::automatic_wal_tail_repair_locked(
                            &durable.root_path,
                            config,
                        )?;
                        durable.wal_bytes = repair.retained_wal_len;
                        durable.wal_tail_repair = Some(repair.clone());
                        tail_repair = Some(repair);
                        torn_tail_reason = Some(reason);
                        break;
                    }
                    WalCursorEvent::TornTail { reason, .. } => {
                        return Err(SkeinError::Storage(format!(
                        "strict WAL recovery rejected torn tail: {reason}; use DatabaseDoctor to inspect and explicitly repair the incomplete final record"
                    )));
                    }
                    WalCursorEvent::Corrupt { offset, reason } => {
                        return reject_corrupt_wal_record(
                            &wal_path,
                            wal_generation,
                            read_only,
                            config.max_quarantine_bytes,
                            offset,
                            reason,
                        );
                    }
                    WalCursorEvent::Entry {
                        entry,
                        start_offset,
                        encoded_len,
                        payload_len,
                        payload_sha256,
                    } => (
                        entry,
                        start_offset,
                        encoded_len,
                        payload_len,
                        payload_sha256,
                    ),
                };
            if entry.lsn != expected_lsn {
                quarantine_corrupt_wal(
                    &wal_path,
                    wal_generation,
                    read_only,
                    config.max_quarantine_bytes,
                )?;
                return Err(SkeinError::Storage(format!(
                    "WAL LSN sequence mismatch at byte offset {record_start}: expected {expected_lsn}, got {}",
                    entry.lsn
                )));
            }
            if let Some(max_entries) = config.max_entries
                && replayed_entries >= max_entries
            {
                return Err(SkeinError::Storage(format!(
                    "WAL replay entry limit exceeded: max_wal_replay_entries={max_entries}"
                )));
            }
            if let WalOp::Batch(ops) = &entry.op
                && config
                    .max_batch_operations
                    .is_some_and(|limit| ops.len() > limit)
            {
                return Err(SkeinError::Storage(format!(
                    "WAL batch operation limit exceeded: max_wal_batch_operations={}",
                    config.max_batch_operations.unwrap_or_default()
                )));
            }
            replayed_entries += 1;
            replayed_bytes = replayed_bytes.saturating_add(record_encoded_len);
            relational_recovery_source
                .record(entry.lsn, payload_len, payload_sha256)
                .map_err(|reason| SkeinError::Storage(reason.to_string()))?;
            expected_lsn = expected_lsn
                .checked_add(1)
                .ok_or_else(|| SkeinError::Storage("WAL LSN overflow during replay".to_string()))?;
            match entry.op {
                WalOp::Batch(ops) => {
                    self.ensure_out_of_core_delta_replay_admission(&ops)?;
                    let commit_epoch = self.commit_epoch + 1;
                    let relational_primary_key_changes =
                        self.relational_primary_key_changes_from_wal_ops(&ops)?;
                    self.record_search_projection_changes_for_ops(
                        catalog,
                        commit_epoch,
                        &ops,
                        relational_primary_key_changes,
                    );
                    for op in ops {
                        self.apply_wal_op(catalog, op)?;
                    }
                    self.commit_epoch += 1;
                    self.advance_relational_row_recovery_epoch(self.commit_epoch);
                }
                op => {
                    self.ensure_out_of_core_delta_replay_admission(std::slice::from_ref(&op))?;
                    let commit_epoch = self.commit_epoch + 1;
                    let relational_primary_key_changes = self
                        .relational_primary_key_changes_from_wal_ops(std::slice::from_ref(&op))?;
                    self.record_search_projection_changes_for_ops(
                        catalog,
                        commit_epoch,
                        std::slice::from_ref(&op),
                        relational_primary_key_changes,
                    );
                    self.apply_wal_op(catalog, op)?;
                    self.commit_epoch += 1;
                    self.advance_relational_row_recovery_epoch(self.commit_epoch);
                }
            }
        }
        let relational_recovery_source = if replayed_entries == 0 {
            None
        } else {
            Some(
                relational_recovery_source
                    .finish()
                    .map_err(|reason| SkeinError::Storage(reason.to_string()))?,
            )
        };
        self.finish_relational_row_page_recovery(relational_recovery_source);
        self.finish_relational_index_recovery(relational_recovery_source);
        if let Some(durable) = &mut self.durable {
            durable.next_lsn = expected_lsn;
            durable.wal_commit_epoch = self.commit_epoch;
        }
        Ok(StorageRecoveryReport {
            open_timings: Default::default(),
            durable: true,
            recovery_mode: config.recovery_mode,
            max_wal_replay_entries: config.max_entries,
            max_wal_replay_bytes: config.max_bytes,
            max_wal_record_bytes: config.max_record_bytes,
            checkpoint_epoch: checkpoint_present.then_some(checkpoint_epoch),
            checkpoint_commit_epoch: checkpoint_present.then_some(checkpoint_commit_epoch),
            wal_present,
            wal_generation: Some(wal_generation),
            wal_replay_start_lsn: Some(wal_replay_start_lsn),
            next_lsn_after_replay: Some(expected_lsn),
            replayed_wal_entries: replayed_entries,
            replayed_wal_bytes: replayed_bytes,
            torn_tail_ignored: false,
            torn_tail_repaired: tail_repair.is_some(),
            discarded_wal_tail_bytes: tail_repair
                .as_ref()
                .map_or(0, |repair| repair.discarded_wal_tail_bytes),
            torn_tail_reason,
            recovered_commit_epoch: self.commit_epoch,
        })
    }
}
