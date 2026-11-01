// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Database maintenance implementation behind the embedded facade.

use super::*;

impl Database {
    pub fn automatic_checkpoint_report(&self) -> Result<Option<AutomaticCheckpointReport>> {
        self.runtime.automatic_checkpoint_report()
    }

    /// Completes a caller-triggered checkpoint of a writable persistent database.
    ///
    /// This synchronous call does not require automatic checkpoint pressure or
    /// background-maintenance admission. It remains available when background
    /// maintenance is disabled, including with `default-features = false`.
    /// Success follows checkpoint publication and its durability barrier; merely
    /// scheduling background work is not success. Read-only handles return an
    /// error. An in-memory database has no persistent checkpoint to publish.
    ///
    /// The call coordinates with the automatic owner and preserves active read
    /// snapshots. Hosts should run this blocking operation on a suitable thread
    /// and provide their own admission boundary for caller-triggered work.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.checkpoint_internal(None)
    }

    pub fn append_transaction(&mut self, transaction: AppendTransaction) -> Result<u64> {
        self.append_transaction_with_result(transaction)
            .map(|result| result.commit_epoch)
    }

    pub fn append_transaction_with_result(
        &mut self,
        transaction: AppendTransaction,
    ) -> Result<AppendCommitResult> {
        self.ensure_writable()?;
        let summary = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime.store.commit_kernel_write_batch(
                &mut branch_runtime.catalog,
                KernelWriteBatch {
                    append: transaction,
                    ..KernelWriteBatch::default()
                },
                self.config.mutation_limits,
            )
        }?;
        Ok(AppendCommitResult {
            commit_epoch: self.runtime.get()?.store.commit_epoch(),
            mutations: summary.append_mutation_outcomes,
        })
    }

    pub fn commit_kernel_write_batch(
        &mut self,
        batch: KernelWriteBatch,
    ) -> Result<MutationSummary> {
        self.ensure_writable()?;
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime.store.commit_kernel_write_batch(
                &mut branch_runtime.catalog,
                batch,
                self.config.mutation_limits,
            )
        }
    }

    pub fn read_append_partition(
        &self,
        table: &str,
        partition: &hawdb_storage::relational::RelationalKey,
        after: Option<&hawdb_storage::relational::RelationalKey>,
        max_rows: usize,
    ) -> Result<AppendSegmentReadOutput> {
        let max_rows = self
            .config
            .read_result_row_limit()
            .map_or(max_rows, |configured| configured.min(max_rows));
        let max_payload_bytes = self
            .config
            .max_read_result_payload_bytes
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
        self.read_append_partition_bounded(table, partition, after, max_rows, max_payload_bytes)
    }

    pub fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &hawdb_storage::relational::RelationalKey,
        after: Option<&hawdb_storage::relational::RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput> {
        let max_rows = self
            .config
            .read_result_row_limit()
            .map_or(max_rows, |configured| configured.min(max_rows));
        let max_payload_bytes = self
            .config
            .max_read_result_payload_bytes
            .map_or(max_payload_bytes, |configured| {
                configured.min(max_payload_bytes)
            });
        self.runtime.get()?.store.read_append_partition_bounded(
            table,
            partition,
            after,
            max_rows,
            max_payload_bytes,
        )
    }

    /// Returns an owned schema snapshot, valid across background checkpoint
    /// adoption and after this database handle is closed.
    pub fn append_table_schema(&self, table: &str) -> Result<Option<AppendTableSchema>> {
        Ok(self
            .runtime
            .get()?
            .store
            .append_table_schema(table)
            .cloned())
    }

    pub fn append_storage_residency_report(&self) -> Result<crate::AppendStorageResidencyReport> {
        Ok(self.runtime.get()?.store.append_storage_residency_report())
    }

    /// Rewrites sparse physical row-page generations through the ordinary
    /// manifest-last checkpoint boundary, retaining older pinned readers.
    pub fn compact_relational_row_pages(
        &mut self,
        config: crate::store::RelationalRowPageCompactionConfig,
    ) -> Result<crate::store::RelationalRowPageCompactionReport> {
        self.compact_relational_row_pages_context(
            config,
            &hawdb_core::RuntimeTaskContext::default(),
        )
    }

    pub fn compact_relational_row_pages_context(
        &mut self,
        config: crate::store::RelationalRowPageCompactionConfig,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<crate::store::RelationalRowPageCompactionReport> {
        self.ensure_writable()?;
        let _suspension = self.runtime.suspend_automatic_checkpoint()?;
        let oldest_reader_epoch = self
            .runtime
            .get()?
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime.store.compact_relational_row_pages(
                &branch_runtime.catalog,
                oldest_reader_epoch,
                config,
                task,
            )
        }
    }

    /// Runs an explicitly admitted full relational-row closure scan and
    /// publishes an exact overflow root through the ordinary manifest-last
    /// checkpoint boundary. Large overflow payloads are not hydrated.
    pub fn compact_relational_overflow(
        &mut self,
        config: crate::store::RelationalOverflowCompactionConfig,
    ) -> Result<crate::store::RelationalOverflowCompactionReport> {
        self.compact_relational_overflow_context(config, &hawdb_core::RuntimeTaskContext::default())
    }

    pub fn compact_relational_overflow_context(
        &mut self,
        config: crate::store::RelationalOverflowCompactionConfig,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<crate::store::RelationalOverflowCompactionReport> {
        self.ensure_writable()?;
        let _suspension = self.runtime.suspend_automatic_checkpoint()?;
        let oldest_reader_epoch = self
            .runtime
            .get()?
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime.store.compact_relational_overflow(
                &branch_runtime.catalog,
                oldest_reader_epoch,
                config,
                task,
            )
        }
    }

    /// Checkpoint entry carrying an explicit pre-admitted columnar-shadow
    /// context, for callers that already hold a governor permit and
    /// extended it by [`Database::columnar_shadow_admission_bytes`]. The
    /// shadow build then draws only against the passed token — it never
    /// touches the governor, so nested admission cannot deadlock a
    /// constrained configuration.
    pub(crate) fn checkpoint_with_shadow_admission(
        &mut self,
        shadow_admission: crate::store::ColumnarShadowAdmission,
    ) -> Result<()> {
        self.checkpoint_internal(Some(shadow_admission))
    }

    pub(super) fn checkpoint_internal(
        &mut self,
        shadow_admission: Option<crate::store::ColumnarShadowAdmission>,
    ) -> Result<()> {
        let _descriptor_operation = self.internal_descriptor_operation()?;
        self.ensure_writable()?;
        let started = hawdb_core::time::Instant::now();
        let durable = self.runtime.get()?.store.storage_recovery_report().durable;
        let prepared = self.checkpoint_source()?.prepare()?;
        let result = match prepared {
            Some(prepared) => {
                let (oldest_reader_epoch, pinned_reader_generations) = {
                    let runtime = self.runtime.get()?;
                    let pins = runtime
                        .reader_pins
                        .lock()
                        .expect("database reader pins lock should not be poisoned");
                    (pins.oldest_epoch(), pins.pinned_physical_generations())
                };
                self.runtime
                    .get_mut()?
                    .store
                    .publish_prepared_checkpoint_with_reader_generations(
                        prepared.checkpoint,
                        oldest_reader_epoch,
                        &pinned_reader_generations,
                        shadow_admission,
                    )
            }
            None => {
                self.runtime.get_mut()?.store.reclaim_version_history();
                Ok(())
            }
        };
        if durable && let Some(telemetry) = &self.telemetry {
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::Checkpoint,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: 1,
                byte_count: 0,
                fsync_micros: 0,
                generation: self.storage_reclamation_watermark()?.checkpoint_epoch,
            });
        }
        result
    }

    pub(super) fn complete_required_relational_row_checkpoint(
        &mut self,
        context: &str,
    ) -> Result<()> {
        if !self.relational_row_schema_checkpoint_required()? {
            return Ok(());
        }
        if self.runtime.get()?.store.wal_sync_group_active() {
            return Ok(());
        }
        let checkpoint = self.checkpoint_internal(None);
        let result = match checkpoint {
            Ok(()) if !self.relational_row_schema_checkpoint_required()? => Ok(()),
            Ok(()) => Err(HawDBError::StorageIntegrity(format!(
                "canonical relational row schema checkpoint remained required after {context}"
            ))),
            Err(error) => Err(HawDBError::StorageIntegrity(format!(
                "canonical relational row schema checkpoint failed after {context}; the durable WAL remains authoritative and writable reopen will retry: {error}"
            ))),
        };
        self.runtime.get()?.store.poison_on_storage_error(&result);
        result
    }

    pub(super) fn relational_row_schema_checkpoint_required(&self) -> Result<bool> {
        Ok({
            self.runtime
                .get()?
                .store
                .relational_row_schema_checkpoint_required()
        })
    }

    pub(crate) fn checkpoint_source(&mut self) -> Result<DatabaseCheckpointSource> {
        self.ensure_writable()?;
        let suspension = self.runtime.suspend_automatic_checkpoint()?;
        Ok(DatabaseCheckpointSource {
            catalog: self.runtime.get()?.catalog.clone(),
            store: self.runtime.get()?.store.checkpoint_source(),
            suspension,
        })
    }

    pub(crate) fn publish_prepared_checkpoint(
        &mut self,
        prepared: PreparedDatabaseCheckpoint,
    ) -> Result<()> {
        let (oldest_reader_epoch, pinned_reader_generations) = {
            let runtime = self.runtime.get()?;
            let pins = runtime
                .reader_pins
                .lock()
                .expect("database reader pins lock should not be poisoned");
            (pins.oldest_epoch(), pins.pinned_physical_generations())
        };
        self.runtime
            .get_mut()?
            .store
            .publish_prepared_checkpoint_with_reader_generations(
                prepared.checkpoint,
                oldest_reader_epoch,
                &pinned_reader_generations,
                None,
            )
    }

    pub fn backup_to(&mut self, destination: impl AsRef<Path>) -> Result<StorageBackupReport> {
        self.ensure_writable()?;
        let _suspension = self.runtime.suspend_automatic_checkpoint()?;
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .backup_to(&branch_runtime.catalog, destination)
        }
    }

    pub fn scrub_storage(&mut self) -> Result<StorageScrubReport> {
        self.runtime.get_mut()?.store.scrub_storage()
    }

    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<StorageRestoreReport> {
        restore_storage_backup(backup, destination)
    }

    /// Shadow write-amplification evidence of the most recent checkpoint,
    /// `None` while `graph_columnar_shadow_checkpoint` is off or before the first
    /// derived shadow checkpoint.
    pub fn columnar_shadow_checkpoint_report(
        &self,
    ) -> Result<Option<crate::store::ColumnarShadowCheckpointReport>> {
        Ok({
            self.runtime
                .get()?
                .store
                .columnar_shadow_checkpoint_report()
        })
    }

    /// Publication evidence for the derived relational index-page store.
    /// `None` while publication is disabled or before its first checkpoint
    /// attempt. The returned report is an owned snapshot; later background
    /// publication does not change a previously returned report.
    pub fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Result<Option<crate::store::RelationalIndexShadowCheckpointReport>> {
        Ok({
            self.runtime
                .get()?
                .store
                .relational_index_shadow_checkpoint_report()
                .cloned()
        })
    }

    pub fn prepare_hawdb_lightning_bootstrap_export(
        &mut self,
    ) -> Result<HawDBLightningBootstrapExport> {
        let snapshot = self.export_canonical_graph_snapshot_with_persisted_stable_ids()?;
        let relational_state = self.hawdb_lightning_relational_state()?;
        let relational_stream = HawDBLightningRelationalStream::from_state(
            self.runtime.get()?.store.commit_epoch(),
            &relational_state,
        )?;
        let manifest = snapshot.hawdb_lightning_bootstrap_manifest(&relational_stream);
        let graph_stream = snapshot.hawdb_lightning_graph_stream();
        Ok(HawDBLightningBootstrapExport {
            snapshot,
            manifest,
            graph_stream,
            relational_stream,
        })
    }

    pub fn hawdb_lightning_initial_import_readiness(
        &self,
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
    ) -> Result<HawDBLightningInitialImportReadiness> {
        Ok({
            hawdb_lightning_initial_import_readiness(
                manifest,
                self.runtime.get()?.store.commit_epoch(),
                projection_freshness,
            )
        })
    }

    pub fn hawdb_lightning_initial_import_cutover_catch_up_report(
        &self,
        session: &HawDBLightningInitialImportSessionReport,
        live_projection_freshness: Option<&SearchProjectionFreshness>,
    ) -> Result<HawDBLightningInitialImportCutoverCatchUpReport> {
        Ok({
            hawdb_lightning_initial_import_cutover_catch_up_report(
                session,
                self.runtime.get()?.store.commit_epoch(),
                live_projection_freshness,
            )
        })
    }

    pub fn hawdb_lightning_initial_import_session_bundle_readiness(
        &self,
        source_bundle: &HawDBLightningInitialImportSourceBundleReadiness,
        session: &HawDBLightningInitialImportSessionReport,
        catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
    ) -> HawDBLightningInitialImportSessionBundleReadiness {
        hawdb_lightning_initial_import_session_bundle_readiness(source_bundle, session, catch_up)
    }

    pub fn hawdb_lightning_initial_import_decode_durable_state(
        &self,
        manifest: &HawDBLightningBootstrapManifest,
        raw: &str,
    ) -> Result<HawDBLightningInitialImportDurableStateCodecReport> {
        hawdb_lightning_initial_import_decode_durable_state(manifest, raw)
    }

    pub fn hawdb_lightning_initial_import_startup_readiness(
        &self,
        inputs: HawDBLightningInitialImportReadinessInputs<'_>,
        durable_state: Option<&HawDBLightningInitialImportDurableState>,
    ) -> Result<HawDBLightningInitialImportStartupReadinessReport> {
        Ok({
            hawdb_lightning_initial_import_startup_readiness(
                inputs,
                self.runtime.get()?.store.commit_epoch(),
                durable_state,
            )
        })
    }

    pub fn hawdb_lightning_initial_import_recovery_readiness(
        &self,
        inputs: HawDBLightningInitialImportReadinessInputs<'_>,
        durable_state_payload: Option<&str>,
    ) -> Result<HawDBLightningInitialImportRecoveryReadinessReport> {
        Ok({
            hawdb_lightning_initial_import_recovery_readiness(
                inputs,
                self.runtime.get()?.store.commit_epoch(),
                durable_state_payload,
            )
        })
    }

    pub fn hawdb_lightning_initial_import_plan(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    ) -> Result<HawDBLightningInitialImportPlan> {
        Ok({
            hawdb_lightning_initial_import_plan(
                encoded_graph_stream,
                encoded_relational_stream,
                manifest,
                self.runtime.get()?.store.commit_epoch(),
                projection_freshness,
                checkpoint,
            )
        })
    }

    pub fn hawdb_lightning_initial_import_plan_with_document_identities(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
        document_identities: &[HawDBLightningInitialImportDocumentIdentity],
    ) -> Result<HawDBLightningInitialImportPlan> {
        Ok({
            hawdb_lightning_initial_import_plan_with_document_identities(
                encoded_graph_stream,
                encoded_relational_stream,
                manifest,
                self.runtime.get()?.store.commit_epoch(),
                projection_freshness,
                checkpoint,
                Some(document_identities),
            )
        })
    }

    pub fn hawdb_lightning_initial_import_apply(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
    ) -> Result<HawDBLightningInitialImportApplyReport> {
        self.hawdb_lightning_initial_import_apply_internal(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            projection_freshness,
            checkpoint,
            None,
        )
    }

    pub fn hawdb_lightning_initial_import_apply_with_document_identities(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
        document_identities: &[HawDBLightningInitialImportDocumentIdentity],
    ) -> Result<HawDBLightningInitialImportApplyReport> {
        self.hawdb_lightning_initial_import_apply_internal(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            projection_freshness,
            checkpoint,
            Some(document_identities),
        )
    }

    pub(super) fn hawdb_lightning_initial_import_apply_internal(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &HawDBLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&HawDBLightningInitialImportCheckpoint>,
        document_identities: Option<&[HawDBLightningInitialImportDocumentIdentity]>,
    ) -> Result<HawDBLightningInitialImportApplyReport> {
        self.ensure_writable()?;
        let mut blocker_codes = BTreeSet::new();
        let plan = hawdb_lightning_initial_import_plan_with_document_identities(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.runtime.get()?.store.commit_epoch(),
            projection_freshness,
            checkpoint,
            document_identities,
        );
        if !plan.ready_for_database_import {
            blocker_codes.insert("hawdb_lightning_database_streams_not_import_ready".to_string());
        }
        let source_fingerprint = hawdb_lightning_initial_import_source_fingerprint_key(manifest);
        if let Some(imported_source_fingerprint) = self
            .runtime
            .get()?
            .store
            .initial_import_source_fingerprint()
        {
            if imported_source_fingerprint == source_fingerprint && plan.ready_for_database_import {
                let (relational_table_count, relational_row_count) =
                    relational_state_counts(self.runtime.get()?.store.relational_state());
                return Ok(HawDBLightningInitialImportApplyReport {
                    applied: false,
                    ready_for_cutover: plan.ready_for_cutover,
                    database_commit_epoch: self.runtime.get()?.store.commit_epoch(),
                    node_count: self.runtime.get()?.store.basic_statistics().node_count as usize,
                    relationship_count: self
                        .runtime
                        .get()?
                        .store
                        .basic_statistics()
                        .relationship_count as usize,
                    relational_table_count,
                    relational_row_count,
                    plan,
                    blocker_codes: Vec::new(),
                });
            }
            if imported_source_fingerprint != source_fingerprint {
                blocker_codes.insert(
                    "hawdb_lightning_initial_import_source_fingerprint_mismatch".to_string(),
                );
            }
        }
        let statistics = self.runtime.get()?.store.basic_statistics();
        let target_has_only_engine_bootstrap = self.has_only_engine_system_schema_bootstrap()?;
        let relational_target_empty = self.runtime.get()?.store.relational_state().is_empty()
            || target_has_only_engine_bootstrap;
        let target_empty = statistics.node_count == 0
            && statistics.relationship_count == 0
            && relational_target_empty
            && self.runtime.get_mut()?.catalog.is_empty();
        if !target_empty {
            blocker_codes.insert("hawdb_lightning_initial_import_target_not_empty".to_string());
        }
        let snapshot = if blocker_codes.is_empty() {
            Some(parse_hawdb_lightning_graph_stream_export(
                encoded_graph_stream,
                Some(manifest),
            )?)
        } else {
            None
        };
        let relational_state = if blocker_codes.is_empty() {
            Some(
                hawdb_storage::relational::decode_relational_checkpoint(
                    encoded_relational_stream,
                    hawdb_storage::relational::RelationalDecodeLimits::checkpoint(),
                )
                .map_err(|error| HawDBError::Storage(error.to_string()))?
                .state,
            )
        } else {
            None
        };
        if let Some(snapshot) = &snapshot {
            for node in &snapshot.nodes {
                if node.labels.len() != 1 {
                    blocker_codes.insert(
                        "hawdb_lightning_initial_import_multi_label_node_unsupported".to_string(),
                    );
                    break;
                }
            }
        }
        if !blocker_codes.is_empty() {
            let (relational_table_count, relational_row_count) =
                relational_state_counts(self.runtime.get()?.store.relational_state());
            return Ok(HawDBLightningInitialImportApplyReport {
                applied: false,
                ready_for_cutover: false,
                database_commit_epoch: self.runtime.get()?.store.commit_epoch(),
                node_count: self.runtime.get()?.store.basic_statistics().node_count as usize,
                relationship_count: self
                    .runtime
                    .get()?
                    .store
                    .basic_statistics()
                    .relationship_count as usize,
                relational_table_count,
                relational_row_count,
                plan,
                blocker_codes: blocker_codes.into_iter().collect(),
            });
        }
        let snapshot = snapshot.expect("snapshot should be available without import blockers");
        let relational_state =
            relational_state.expect("relational state should be available without import blockers");
        Self::validate_hawdb_lightning_system_schema(&relational_state)?;
        let stable_id_mapping = StoreStableIdMapping {
            node_stable_ids: snapshot
                .nodes
                .iter()
                .filter_map(|node| {
                    node.stable_id
                        .as_ref()
                        .map(|stable_id| (NodeId(node.node_id), stable_id.clone()))
                })
                .collect(),
            relationship_stable_ids: snapshot
                .relationships
                .iter()
                .filter_map(|relationship| {
                    relationship
                        .stable_id
                        .as_ref()
                        .map(|stable_id| (RelId(relationship.relationship_id), stable_id.clone()))
                })
                .collect(),
        };
        let node_rows = snapshot
            .nodes
            .iter()
            .map(|node| {
                Ok((
                    NodeId(node.node_id),
                    single_import_label(node)?,
                    node.properties.clone(),
                ))
            })
            .collect::<Result<Vec<GraphSnapshotNodeImport>>>()?;
        let relationship_rows = snapshot
            .relationships
            .iter()
            .map(|relationship| {
                Ok((
                    RelId(relationship.relationship_id),
                    NodeId(relationship.source_node_id),
                    NodeId(relationship.target_node_id),
                    relationship.rel_type.clone(),
                    relationship.properties.clone(),
                ))
            })
            .collect::<Result<Vec<GraphSnapshotRelationshipImport>>>()?;
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .import_hawdb_snapshot_rows_with_source_fingerprint(
                    &mut branch_runtime.catalog,
                    HawDBSnapshotRowsImport {
                        stable_id_mapping,
                        source_fingerprint,
                        nodes: node_rows,
                        relationships: relationship_rows,
                        relational_state,
                        target_has_only_engine_bootstrap,
                    },
                )
        }?;
        self.complete_required_relational_row_checkpoint("HawDB Lightning initial import")?;
        let updated_plan = hawdb_lightning_initial_import_plan_with_document_identities(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.runtime.get()?.store.commit_epoch(),
            projection_freshness,
            checkpoint,
            document_identities,
        );
        let (relational_table_count, relational_row_count) =
            relational_state_counts(self.runtime.get()?.store.relational_state());
        Ok(HawDBLightningInitialImportApplyReport {
            applied: true,
            ready_for_cutover: updated_plan.ready_for_cutover,
            database_commit_epoch: self.runtime.get()?.store.commit_epoch(),
            node_count: self.runtime.get()?.store.basic_statistics().node_count as usize,
            relationship_count: self
                .runtime
                .get()?
                .store
                .basic_statistics()
                .relationship_count as usize,
            relational_table_count,
            relational_row_count,
            plan: updated_plan,
            blocker_codes: Vec::new(),
        })
    }

    pub fn hawdb_lightning_bootstrap_export_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let estimated_operations =
                self.hawdb_lightning_bootstrap_export_estimated_operations()?;
            if estimated_operations == 0 {
                return Ok(None);
            }
            Some(BackgroundWorkPlan::background(
                WorkClass::Import,
                estimated_operations,
                hint,
            ))
        })
    }

    pub fn prepare_background_hawdb_lightning_bootstrap_export(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<HawDBLightningBootstrapExport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self.hawdb_lightning_bootstrap_export_estimated_operations()?;
        if estimated_operations == 0 {
            return self.prepare_hawdb_lightning_bootstrap_export();
        }
        let request = WorkRequest::background(WorkClass::Import, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.prepare_hawdb_lightning_bootstrap_export(),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background HawDB Lightning bootstrap export deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background HawDB Lightning bootstrap export rejected: {reason}"
            ))),
        }
    }

    pub fn prepare_scheduled_background_hawdb_lightning_bootstrap_export(
        &mut self,
    ) -> Result<HawDBLightningBootstrapExport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self.hawdb_lightning_bootstrap_export_estimated_operations()?;
        if estimated_operations == 0 {
            return self.prepare_hawdb_lightning_bootstrap_export();
        }
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Import,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background HawDB Lightning bootstrap export deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background HawDB Lightning bootstrap export rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.prepare_hawdb_lightning_bootstrap_export();
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub(super) fn hawdb_lightning_bootstrap_export_estimated_operations(&self) -> Result<usize> {
        Ok({
            let runtime = self.runtime.get()?;
            let statistics = runtime.store.basic_statistics();
            let graph_total = statistics
                .node_count
                .saturating_add(statistics.relationship_count);
            let relational_total = runtime
                .store
                .relational_state()
                .table_schemas()
                .map(|schema| {
                    1usize.saturating_add(runtime.store.relational_state().row_count(&schema.name))
                })
                .fold(0usize, usize::saturating_add)
                .saturating_add(runtime.store.relational_state().overflow_segment_count());
            usize::try_from(graph_total)
                .unwrap_or(usize::MAX)
                .saturating_add(relational_total)
        })
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> Result<GraphStatistics> {
        Ok({
            self.runtime
                .get()?
                .store
                .statistics(&self.runtime.get()?.catalog)
        })
    }

    pub fn refresh_optimizer_statistics_external(
        &mut self,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
    ) -> Result<crate::store::OptimizerStatisticsRefreshReport> {
        self.ensure_writable()?;
        let previous_statistics = self.runtime.get()?.store.checkpoint_statistics_snapshot();
        let previous_dirty_state = self
            .runtime
            .get()?
            .store
            .advanced_statistics_dirty_snapshot();
        let mut report = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .refresh_optimizer_statistics_external(&branch_runtime.catalog, options)
        }?;
        if let Err(error) = self.checkpoint() {
            self.runtime
                .get_mut()?
                .store
                .restore_checkpoint_statistics(previous_statistics, previous_dirty_state);
            return Err(error);
        }
        report.checkpoint_persisted = true;
        *self.runtime.get()?.plan_cache.borrow_mut() = PlanCache::with_byte_limits(
            self.config.max_plan_cache_entries,
            Some(self.config.max_physical_plan_cache_bytes),
            Some(self.config.max_physical_plan_cache_entry_bytes),
        );
        self.runtime
            .get()?
            .optimizer_planning_cache
            .borrow_mut()
            .invalidate();
        Ok(report)
    }

    pub(super) fn optimizer_statistics_refresh_work(
        &self,
    ) -> Result<Option<OptimizerStatisticsRefreshWork>> {
        Ok({
            if self.config.read_only {
                return Ok(None);
            }
            self.runtime
                .get()?
                .store
                .optimizer_statistics_refresh_work(&self.runtime.get()?.catalog)
        })
    }

    pub fn optimizer_statistics_refresh_background_work_plan(
        &self,
        mut hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let Some(work) = self.optimizer_statistics_refresh_work()? else {
                return Ok(None);
            };
            hint.recent_delta_operations = hint
                .recent_delta_operations
                .max(work.recent_delta_operations);
            hint.source_graph_commit_lag = hint.source_graph_commit_lag.max(work.source_commit_lag);
            Some(BackgroundWorkPlan::background(
                WorkClass::Projection,
                work.estimated_operations,
                hint,
            ))
        })
    }

    pub fn refresh_background_optimizer_statistics(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
        hint: BackgroundWorkHint,
    ) -> Result<Option<crate::store::OptimizerStatisticsRefreshReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.optimizer_statistics_refresh_background_work_plan(hint)? else {
            return Ok(None);
        };
        match policy.admit(state, &plan.request) {
            QosAdmission::Admit => self
                .refresh_optimizer_statistics_external(options)
                .map(Some),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background optimizer statistics refresh deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background optimizer statistics refresh rejected: {reason}"
            ))),
        }
    }

    pub fn refresh_scheduled_background_optimizer_statistics(
        &mut self,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
        hint: BackgroundWorkHint,
    ) -> Result<Option<crate::store::OptimizerStatisticsRefreshReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.optimizer_statistics_refresh_background_work_plan(hint)? else {
            return Ok(None);
        };
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(plan.request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background optimizer statistics refresh deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background optimizer statistics refresh rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self
            .refresh_optimizer_statistics_external(options)
            .map(Some);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    #[cfg(test)]
    pub(crate) fn basic_statistics(&self) -> Result<crate::schema::BasicGraphStatistics> {
        Ok(self.runtime.get()?.store.basic_statistics())
    }

    pub fn basic_statistics_consistency_report(&self) -> Result<BasicStatisticsConsistencyReport> {
        Ok({
            self.runtime
                .get()?
                .store
                .basic_statistics_consistency_report()
        })
    }

    pub fn adjacency_consistency_report(&self) -> Result<AdjacencyConsistencyReport> {
        Ok(self.runtime.get()?.store.adjacency_consistency_report())
    }

    pub fn adjacency_consolidation_plan(&self) -> Result<AdjacencyConsolidationPlan> {
        Ok(self.runtime.get()?.store.adjacency_consolidation_plan())
    }

    pub fn storage_checkpoint_background_work_plan(
        &self,
        mut hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            if self.config.read_only {
                return Ok(None);
            }
            let pressure = self.storage_pressure_snapshot()?;
            if !pressure.recommends_checkpoint() {
                return Ok(None);
            }
            hint.recent_delta_operations = hint
                .recent_delta_operations
                .max(self.runtime.get()?.store.checkpoint_estimated_operations());
            hint.source_graph_commit_lag = hint.source_graph_commit_lag.max(
                pressure
                    .current_commit_epoch
                    .saturating_sub(pressure.checkpoint_commit_epoch),
            );
            hint.staleness_millis = hint.staleness_millis.max(pressure.wal_age_millis);
            Some(BackgroundWorkPlan::background(
                WorkClass::Mutation,
                self.runtime.get()?.store.checkpoint_estimated_operations(),
                hint,
            ))
        })
    }

    /// Attempts pressure-triggered checkpoint work under the supplied background
    /// policy. For an explicit App-triggered checkpoint, use [`Self::checkpoint`].
    pub fn checkpoint_background(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        hint: BackgroundWorkHint,
    ) -> Result<()> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.storage_checkpoint_background_work_plan(hint)? else {
            return Ok(());
        };
        match policy.admit(state, &plan.request) {
            QosAdmission::Admit => self.checkpoint(),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background storage checkpoint deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background storage checkpoint rejected: {reason}"
            ))),
        }
    }

    /// Attempts pressure-triggered checkpoint work under the local background
    /// scheduler. For an explicit App-triggered checkpoint, use [`Self::checkpoint`].
    pub fn checkpoint_scheduled_background(&mut self, hint: BackgroundWorkHint) -> Result<()> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.storage_checkpoint_background_work_plan(hint)? else {
            return Ok(());
        };
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(plan.request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background storage checkpoint deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background storage checkpoint rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };
        let result = self.checkpoint();
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn adjacency_consolidation_background_work_plan(
        &self,
        max_estimated_entries: usize,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let estimated_entries = self
                .runtime
                .get()?
                .store
                .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
            (estimated_entries > 0).then(|| {
                BackgroundWorkPlan::background(WorkClass::Mutation, estimated_entries, hint)
            })
        })
    }

    pub fn consolidate_bounded_adjacency_deltas(
        &mut self,
        max_estimated_entries: usize,
    ) -> Result<AdjacencyConsolidationReport> {
        Ok({
            self.runtime
                .get_mut()?
                .store
                .consolidate_bounded_adjacency_deltas(max_estimated_entries)
        })
    }

    pub fn consolidate_bounded_background_adjacency_deltas(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_entries: usize,
    ) -> Result<AdjacencyConsolidationReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_entries = self
            .runtime
            .get()?
            .store
            .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
        if estimated_entries == 0 {
            return self.consolidate_bounded_adjacency_deltas(max_estimated_entries);
        }
        match policy.admit(
            state,
            &WorkRequest::background(WorkClass::Mutation, estimated_entries),
        ) {
            QosAdmission::Admit => {
                Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries)?)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background adjacency consolidation deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background adjacency consolidation rejected: {reason}"
            ))),
        }
    }

    pub fn consolidate_bounded_scheduled_background_adjacency_deltas(
        &mut self,
        max_estimated_entries: usize,
    ) -> Result<AdjacencyConsolidationReport> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_entries = self
            .runtime
            .get()?
            .store
            .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
        if estimated_entries == 0 {
            return self.consolidate_bounded_adjacency_deltas(max_estimated_entries);
        }
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_entries,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background adjacency consolidation deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background adjacency consolidation rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries)?);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn degree_statistics_consistency_report(
        &self,
    ) -> Result<DegreeStatisticsConsistencyReport> {
        Ok({
            self.runtime
                .get()?
                .store
                .degree_statistics_consistency_report()
        })
    }

    pub fn distinct_value_statistics_consistency_report(
        &self,
    ) -> Result<DistinctValueStatisticsConsistencyReport> {
        Ok({
            self.runtime
                .get()?
                .store
                .distinct_value_statistics_consistency_report(&self.runtime.get()?.catalog)
        })
    }

    pub fn property_index_consistency_report(&self) -> Result<PropertyIndexConsistencyReport> {
        Ok({
            self.runtime
                .get()?
                .store
                .property_index_consistency_report(&self.runtime.get()?.catalog)
        })
    }

    #[cfg(test)]
    pub(crate) fn property_indexes(&self) -> Result<Vec<IndexDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .property_indexes()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn composite_property_indexes(&self) -> Result<Vec<CompositeIndexDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .composite_property_indexes()
                .cloned()
                .collect()
        })
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        Ok({
            property_index_projection_rebuild_output({
                let mut branch_runtime_access = self.runtime.get_mut()?;
                let branch_runtime = &mut *branch_runtime_access;
                branch_runtime
                    .store
                    .rebuild_bounded_property_index_projections(
                        &branch_runtime.catalog,
                        max_estimated_operations,
                    )
            })
        })
    }

    pub fn property_index_projection_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let estimated_operations = self
                .runtime
                .get()?
                .store
                .property_index_projection_estimated_operations(&self.runtime.get()?.catalog);
            if estimated_operations == 0 {
                return Ok(None);
            }
            Some(BackgroundWorkPlan::background(
                WorkClass::Projection,
                estimated_operations,
                hint,
            ))
        })
    }

    pub fn rebuild_bounded_background_property_index_projections(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .bounded_property_index_projection_estimated_operations(
                    &branch_runtime.catalog,
                    max_estimated_operations,
                )
        };
        if estimated_operations == 0 {
            return self.rebuild_bounded_property_index_projections(max_estimated_operations);
        }
        let request = WorkRequest::background(WorkClass::Projection, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => {
                Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations)?)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background property index projection rebuild deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background property index projection rebuild rejected: {reason}"
            ))),
        }
    }

    pub fn rebuild_bounded_scheduled_background_property_index_projections(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .bounded_property_index_projection_estimated_operations(
                    &branch_runtime.catalog,
                    max_estimated_operations,
                )
        };
        if estimated_operations == 0 {
            return self.rebuild_bounded_property_index_projections(max_estimated_operations);
        }
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Projection,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background property index projection rebuild deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background property index projection rebuild rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations)?);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    #[cfg(test)]
    pub(crate) fn unique_constraints(&self) -> Result<Vec<ConstraintDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .unique_constraints()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn node_property_exists_constraints(&self) -> Result<Vec<ConstraintDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .node_property_exists_constraints()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn relationship_property_exists_constraints(
        &self,
    ) -> Result<Vec<ConstraintDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .relationship_property_exists_constraints()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn relationship_unique_constraints(&self) -> Result<Vec<ConstraintDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .relationship_unique_constraints()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn table_descriptors(&self) -> Result<Vec<TableDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .table_descriptors()
                .cloned()
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn property_descriptors(&self) -> Result<Vec<PropertyDescriptor>> {
        Ok({
            self.runtime
                .get()?
                .catalog
                .property_descriptors()
                .cloned()
                .collect()
        })
    }

    pub fn plan_schema_maintenance(&self) -> Result<QueryOutput> {
        Ok({
            let rows: Vec<Row> = self
                .runtime
                .get()?
                .store
                .plan_schema_maintenance(&self.runtime.get()?.catalog)
                .into_iter()
                .map(|item| {
                    BTreeMap::from([
                        ("object_type".to_string(), Value::String(item.object_type)),
                        ("object".to_string(), Value::String(item.object)),
                        (
                            "from_state".to_string(),
                            schema_state_value(item.from_state),
                        ),
                        (
                            "to_state".to_string(),
                            item.to_state.map(schema_state_value).unwrap_or(Value::Null),
                        ),
                        ("action".to_string(), Value::String(item.action)),
                        (
                            "estimated_operations".to_string(),
                            Value::Int(
                                i64::try_from(item.estimated_operations).unwrap_or(i64::MAX),
                            ),
                        ),
                    ])
                })
                .collect();
            QueryOutput { rows: rows.into() }
        })
    }

    pub fn schema_maintenance_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        Ok({
            let estimated_operations = self.schema_maintenance_estimated_operations()?;
            if estimated_operations == 0 {
                return Ok(None);
            }
            Some(BackgroundWorkPlan::background(
                WorkClass::Mutation,
                estimated_operations,
                hint,
            ))
        })
    }

    pub fn run_schema_maintenance(&mut self) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .run_schema_maintenance(&mut branch_runtime.catalog)
        }?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime.store.run_bounded_schema_maintenance(
                &mut branch_runtime.catalog,
                max_estimated_operations,
            )
        }?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_schema_maintenance(),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_bounded_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations)?;
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_bounded_schema_maintenance(max_estimated_operations),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_planned_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<QueryOutput> {
        self.run_background_schema_maintenance(
            policy,
            state,
            self.schema_maintenance_estimated_operations()?,
        )
    }

    pub fn run_scheduled_background_schema_maintenance(
        &mut self,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_schema_maintenance();
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_bounded_scheduled_background_schema_maintenance(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations)?;
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_bounded_schema_maintenance(max_estimated_operations);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_planned_scheduled_background_schema_maintenance(&mut self) -> Result<QueryOutput> {
        self.run_scheduled_background_schema_maintenance(
            self.schema_maintenance_estimated_operations()?,
        )
    }

    pub(super) fn schema_maintenance_estimated_operations(&self) -> Result<usize> {
        Ok({
            self.runtime
                .get()?
                .store
                .plan_schema_maintenance(&self.runtime.get()?.catalog)
                .into_iter()
                .map(|item| item.estimated_operations)
                .fold(0usize, usize::saturating_add)
        })
    }

    pub(super) fn bounded_schema_maintenance_estimated_operations(
        &self,
        max_estimated_operations: usize,
    ) -> Result<usize> {
        Ok({
            let mut used_estimated_operations = 0usize;
            for item in self
                .runtime
                .get()?
                .store
                .plan_schema_maintenance(&self.runtime.get()?.catalog)
            {
                let Some(next) = used_estimated_operations.checked_add(item.estimated_operations)
                else {
                    continue;
                };
                if next <= max_estimated_operations {
                    used_estimated_operations = next;
                }
            }
            used_estimated_operations
        })
    }

    #[cfg(test)]
    pub(crate) fn projected_graph_statuses(
        &self,
    ) -> Result<Vec<crate::store::ProjectedGraphStatus>> {
        Ok(self.runtime.get()?.store.projected_graph_statuses())
    }

    pub fn rebuild_projected_graph_artifacts(&mut self) -> Result<()> {
        self.ensure_writable()?;
        let _suspension = self.runtime.suspend_automatic_checkpoint()?;
        // This derived write has no WAL record, so the next candidate must
        // capture the rebuilt runtime rather than reuse the prior source.
        self.runtime.release_suspended_checkpoint_source()?;
        {
            let mut branch_runtime_access = self.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            branch_runtime
                .store
                .rebuild_projected_graph_artifacts(&branch_runtime.catalog)
        }
    }

    pub fn background_maintenance_candidates(
        &self,
        search_index: Option<&SearchIndex>,
        options: BackgroundMaintenanceOptions,
    ) -> Result<Vec<BackgroundMaintenanceCandidate>> {
        Ok({
            let mut candidates = Vec::new();

            if options.include_storage_checkpoint
                && let Some(plan) =
                    self.storage_checkpoint_background_work_plan(options.hint.clone())?
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::StorageCheckpoint,
                    plan,
                ));
            }

            if options.include_schema_maintenance
                && let Some(plan) =
                    self.schema_maintenance_background_work_plan(options.hint.clone())?
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::SchemaMaintenance,
                    plan,
                ));
            }

            if options.include_property_index_projection
                && let Some(plan) =
                    self.property_index_projection_background_work_plan(options.hint.clone())?
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::PropertyIndexProjection,
                    plan,
                ));
            }

            if options.include_optimizer_statistics_refresh
                && let Some(plan) =
                    self.optimizer_statistics_refresh_background_work_plan(options.hint.clone())?
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::OptimizerStatisticsRefresh,
                    plan,
                ));
            }

            if let Some(delta_request) = &options.search_projection_graph_delta {
                let plan = match search_index {
                    Some(search_index) => self
                        .search_projection_graph_delta_freshness_background_work_plan(
                            search_index,
                            delta_request,
                            options.hint.clone(),
                        )?,
                    None => self.search_projection_graph_delta_background_work_plan(
                        delta_request,
                        options.hint.clone(),
                    ),
                };
                if let Some(plan) = plan {
                    candidates.push(
                        BackgroundMaintenanceCandidate::new(
                            BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                            plan,
                        )
                        .with_search_projection_graph_delta(delta_request.clone()),
                    );
                }
            } else if options.include_search_projection_graph_delta_freshness
                && let Some(search_index) = search_index
            {
                let executable_request = self
                    .build_search_projection_graph_delta_request_from_freshness(search_index, None)
                    .ok()
                    .flatten();
                if let Some(request) = executable_request {
                    if let Some(plan) = self
                        .search_projection_graph_delta_freshness_background_work_plan(
                            search_index,
                            &request,
                            options.hint.clone(),
                        )?
                    {
                        candidates.push(
                            BackgroundMaintenanceCandidate::new(
                                BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                                plan,
                            )
                            .with_search_projection_graph_delta(request),
                        );
                    }
                } else if let Some(plan) = self
                    .search_projection_freshness_lag_background_work_plan(
                        search_index,
                        options.hint.clone(),
                    )?
                {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                        plan,
                    ));
                }
            }

            if let Some(search_index) = search_index {
                if options.include_search_projection_rebuild {
                    let mut hint = options.hint.clone();
                    if hint.source_graph_commit_lag == 0 {
                        hint.source_graph_commit_lag = search_projection_commit_lag(
                            search_index,
                            self.runtime
                                .get()?
                                .store
                                .search_projection_changefeed_status()
                                .required_projection_commit_epoch(),
                        );
                    }
                    if let Some(plan) =
                        self.search_projection_rebuild_background_work_plan(search_index, hint)?
                    {
                        candidates.push(BackgroundMaintenanceCandidate::new(
                            BackgroundMaintenanceKind::SearchProjectionRebuild,
                            plan,
                        ));
                    }
                }

                if options.include_search_projection_metadata_repair
                    && let Some(plan) = self
                        .search_projection_metadata_repair_background_work_plan(
                            search_index,
                            options.hint.clone(),
                        )?
                {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionMetadataRepair,
                        plan,
                    ));
                }
            }

            if options.include_hawdb_lightning_bootstrap_export
                && let Some(plan) = self
                    .hawdb_lightning_bootstrap_export_background_work_plan(options.hint.clone())?
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::HawDBLightningBootstrapExport,
                    plan,
                ));
            }

            if options.include_external_content_artifact_jobs
                && let Some(plan) = self.external_content_artifact_job_background_work_plan(
                    options.hint,
                    options.external_content_artifact_estimated_operations,
                )
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::ExternalContentArtifactJob,
                    plan,
                ));
            }

            candidates
        })
    }

    pub fn rank_background_maintenance(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Result<Vec<RankedBackgroundMaintenance>> {
        Ok({
            let candidates = self.background_maintenance_candidates(search_index, options)?;
            let plans = candidates
                .iter()
                .map(|candidate| candidate.plan.clone())
                .collect::<Vec<_>>();
            policy
                .rank_background_work(state, &plans)
                .into_iter()
                .map(|ranked| {
                    let candidate = &candidates[ranked.index];
                    RankedBackgroundMaintenance {
                        kind: candidate.kind,
                        name: candidate.name.clone(),
                        plan: candidate.plan.clone(),
                        decision: ranked.decision,
                        search_projection_graph_delta: candidate
                            .search_projection_graph_delta
                            .clone(),
                    }
                })
                .collect()
        })
    }

    pub fn background_maintenance_summary(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Result<BackgroundMaintenanceSummary> {
        Ok({
            let ranked = self.rank_background_maintenance(search_index, policy, state, options)?;
            BackgroundMaintenanceSummary::from_ranked(ranked, policy, state)
        })
    }
}
