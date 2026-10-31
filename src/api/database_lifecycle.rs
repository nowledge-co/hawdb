// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Database lifecycle implementation behind the embedded facade.

use super::*;

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Identifies the actual admitted selected runtime, independently of epoch.
    pub fn instance_identity(&self) -> Result<DatabaseInstanceIdentity> {
        self.runtime.get()?.store.ensure_usable()?;
        Ok(self.runtime.instance_identity())
    }

    pub fn into_concurrent(self) -> ConcurrentDatabase {
        ConcurrentDatabase::new(self)
    }

    pub fn new_with_config(config: DatabaseConfig) -> Self {
        let config = effective_database_config(config);
        let local_qos_scheduler = LocalQosScheduler::new(config.local_qos_policy);
        let mut store = GraphStore::default();
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        let optimizer = optimizer_from_database_config(&config);
        Self {
            runtime: runtime_cell::BranchRuntimeCell::admitted(
                runtime_cell::AdmittedBranchRuntime {
                    catalog: Catalog::default(),
                    branch_selection: None,
                    projection_consumers: search_projection_consumer::ConsumerRegistry::load(
                        store.search_projection_registry_root(),
                        store.search_projection_database_identity(),
                    ),
                    store,
                    optimizer,
                    plan_cache: Arc::new(SharedState::new(PlanCache::with_byte_limits(
                        config.max_plan_cache_entries,
                        Some(config.max_physical_plan_cache_bytes),
                        Some(config.max_physical_plan_cache_entry_bytes),
                    ))),
                    relational_plan_template_cache:
                        relational_plan_template_cache_from_database_config(&config),
                    optimizer_planning_cache: Arc::new(SharedState::new(
                        OptimizerPlanningCache::default(),
                    )),
                    reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
                },
            ),
            project_root_path: None,

            durability: DurabilityPolicy::default(),

            slow_query_log: Arc::new(SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            ))),
            statement_summary: Arc::new(SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            ))),
            config,
            local_qos_scheduler,
            system_variables: QuerySystemVariables::default(),

            derived_artifact_jobs: hawdb_artifact::DerivedArtifactJobQueue::default(),
            derived_artifact_root_pins: BTreeMap::new(),
            telemetry: None,
            runtime_governor: None,
            retained_runtime: Arc::new(retained_query::RetainedRuntime::default()),
            branch_create_recovery: None,
        }
    }

    /// Returns the selected branch's committed epoch, admitting default main
    /// first when this handle has only opened project metadata.
    pub fn commit_epoch(&self) -> Result<u64> {
        Ok(self.runtime.get()?.store.commit_epoch())
    }

    pub fn published_read_view(&self) -> Result<PublishedReadView> {
        Ok(self.runtime.get()?.store.published_read_view())
    }

    pub(crate) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        self.runtime.get_mut()?.store.begin_wal_sync_group()
    }

    pub(crate) fn finish_wal_sync_group(&mut self) -> Result<crate::store::WalSyncGroupFlush> {
        self.runtime.get_mut()?.store.finish_wal_sync_group()
    }

    /// Opens a persistent project with power-loss-safe writes by default.
    ///
    /// Published projects open metadata without taking a branch writer lease.
    /// Data access lazily recovers default main; busy, damaged, or resource-
    /// rejected admission returns an error and can be retried. Catalog SQL and
    /// `USE BRANCH` can run before main is admitted. Successful metadata open
    /// does not certify that the selected branch's data is ready. Writable opens
    /// also attempt bounded pending-child recovery; inspect
    /// `branch_create_recovery_report` for retained, busy or unattempted receipts.
    ///
    /// The first writable open publishes the project's main branch from the
    /// complete legacy state. Read-only legacy opens do not adopt that layout.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_durability(path, DurabilityPolicy::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Self::open_with_durability_and_config(path, DurabilityPolicy::default(), config)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Self::open_with_durability_and_config(path, durability, DatabaseConfig::default())
    }

    pub fn open_with_durability_and_config(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        if cfg!(all(target_arch = "wasm32", target_os = "unknown")) {
            return Err(HawDBError::Storage(
                "persistent storage is unavailable in browser WASM; use Database::new()".into(),
            ));
        }
        let config = effective_database_config(config);
        if config.read_only && !hawdb_storage::file_io::try_exists(path.as_ref())? {
            return Err(HawDBError::Storage(format!(
                "read-only database path does not exist: {}",
                path.as_ref().display()
            )));
        }
        let local_qos_scheduler = LocalQosScheduler::new(config.local_qos_policy);
        let mut catalog = Catalog::default();
        let files =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_with_namespace_anchor(
                path.as_ref(),
                config.max_open_files,
                config.strict_capacity_limit,
                !config.read_only,
                config.namespace_durability_anchor.as_deref(),
            )?;
        let project_root_path = files.root().to_path_buf();
        let _descriptor_operation = files.io_context().internal_operation()?;
        if matches!(
            hawdb_storage::branch_project::inspect_project_manifest(files.root())?,
            hawdb_storage::branch_project::ProjectManifest::Branch(_)
        ) {
            let metadata = hawdb_storage::branch_project::ProjectMetadata::from_files(files)?;
            return Self::from_project_metadata(metadata, durability, config);
        }
        let replay_config = config.wal_replay_config();
        let mut store = if config.read_only {
            GraphStore::open_read_only_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        } else {
            GraphStore::open_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        };
        let project_root_path = hawdb_storage::file_io::canonicalize(&project_root_path)?;
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        let mut database = Self {
            runtime: runtime_cell::BranchRuntimeCell::admitted(
                runtime_cell::AdmittedBranchRuntime {
                    catalog,
                    branch_selection: None,
                    projection_consumers: search_projection_consumer::ConsumerRegistry::load(
                        store.search_projection_registry_root(),
                        store.search_projection_database_identity(),
                    ),
                    store,
                    optimizer: optimizer_from_database_config(&config),
                    plan_cache: Arc::new(SharedState::new(PlanCache::with_byte_limits(
                        config.max_plan_cache_entries,
                        Some(config.max_physical_plan_cache_bytes),
                        Some(config.max_physical_plan_cache_entry_bytes),
                    ))),
                    relational_plan_template_cache:
                        relational_plan_template_cache_from_database_config(&config),
                    optimizer_planning_cache: Arc::new(SharedState::new(
                        OptimizerPlanningCache::default(),
                    )),
                    reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
                },
            ),
            project_root_path: Some(project_root_path),

            durability,

            slow_query_log: Arc::new(SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            ))),
            statement_summary: Arc::new(SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            ))),
            config,
            local_qos_scheduler,
            system_variables: QuerySystemVariables::default(),

            derived_artifact_jobs: hawdb_artifact::DerivedArtifactJobQueue::default(),
            derived_artifact_root_pins: BTreeMap::new(),
            telemetry: None,
            runtime_governor: None,
            retained_runtime: Arc::new(retained_query::RetainedRuntime::default()),
            branch_create_recovery: None,
        };
        if database.config.read_only {
            database.apply_engine_system_schema()?;
            return Ok(database);
        }
        let proposed = hawdb_storage::branch_project::ProjectSelector::new(
            hawdb_storage::branch_catalog::BranchId::new(hawdb_core::generate_uuidv7()?)
                .map_err(HawDBError::from_storage_error)?,
            hawdb_storage::branch_catalog::BranchId::new(hawdb_core::generate_uuidv7()?)
                .map_err(HawDBError::from_storage_error)?,
        )?;
        let selector = database
            .runtime
            .get()?
            .store
            .reserve_branch_project_identity(proposed)?;
        let head_path = database.branch_head_path(selector.main_branch_id().as_uuid())?;
        // Check uncertain bootstrap publication before an implicit schema
        // upgrade or row checkpoint can advance the retained legacy history.
        if !hawdb_storage::file_io::try_exists(&head_path)? {
            let catalog_path = database.branch_catalog_path()?;
            if hawdb_storage::file_io::try_exists(&catalog_path)? {
                let catalog = hawdb_storage::branch_catalog::read_catalog(&catalog_path)
                    .map_err(HawDBError::from_storage_error)?;
                let main = catalog
                    .branches
                    .iter()
                    .find(|record| record.id == selector.main_branch_id())
                    .ok_or_else(|| {
                        HawDBError::StorageIntegrity("bootstrap main is absent".into())
                    })?;
                if main.base_root_digest.is_some() {
                    return Err(HawDBError::StorageIntegrity(
                        "initialized main branch is missing its head".into(),
                    ));
                }
            }
        }
        database.complete_required_relational_row_checkpoint("writable recovery")?;
        database.apply_engine_system_schema()?;
        if !hawdb_storage::file_io::try_exists(&head_path)? {
            let watermark = database.storage_reclamation_watermark()?;
            // Generation zero has no checkpoint. A clean bootstrap retry must
            // reuse its checkpoint and private WAL generation: advancing it
            // here could bypass a damaged WAL left by interrupted publication.
            if watermark
                .checkpoint_epoch
                .is_none_or(|generation| generation == 0)
                || watermark.checkpoint_commit_epoch != Some(watermark.current_commit_epoch)
                || database.relational_row_schema_checkpoint_required()?
            {
                database.checkpoint()?;
            }
        }
        database.initialize_main_branch(
            selector.project_id().as_uuid(),
            selector.main_branch_id().as_uuid(),
        )?;
        database
            .runtime
            .get()?
            .store
            .publish_branch_project_selector(selector)?;
        // Release the old directory/WAL owner before admitting any data runtime.
        // Its immutable closure and old files remain durable recovery evidence.
        let config = database.config.clone();
        drop(database);
        let metadata = hawdb_storage::branch_project::ProjectMetadata::from_files(files)?;
        Self::from_project_metadata(metadata, durability, config)
    }

    pub(super) fn from_project_metadata(
        metadata: hawdb_storage::branch_project::ProjectMetadata,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        let branch_create_recovery = if config.read_only {
            None
        } else {
            Some(metadata.recover_pending_creates(config.branch_create_recovery_limits)?)
        };
        let files = metadata.file_descriptors().clone();
        let project_root_path = Some(files.root().to_path_buf());
        let project = metadata.into_capture();
        let local_qos_scheduler = LocalQosScheduler::new(config.local_qos_policy);
        Ok(Self {
            runtime: runtime_cell::BranchRuntimeCell::deferred(
                runtime_cell::DeferredBranchAdmission {
                    config: config.clone(),
                    durability,
                    scheduler: local_qos_scheduler.clone(),
                    telemetry: None,
                    governor: None,
                },
                project,
            ),
            project_root_path,
            durability,
            slow_query_log: Arc::new(SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            ))),
            statement_summary: Arc::new(SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            ))),
            config,
            local_qos_scheduler,
            system_variables: QuerySystemVariables::default(),
            derived_artifact_jobs: hawdb_artifact::DerivedArtifactJobQueue::default(),
            derived_artifact_root_pins: BTreeMap::new(),
            telemetry: None,
            runtime_governor: None,
            retained_runtime: Arc::new(retained_query::RetainedRuntime::default()),
            branch_create_recovery,
        })
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Engine-owned descriptors and reservations shared by this canonical project.
    pub fn file_descriptor_metrics(
        &self,
    ) -> Option<hawdb_storage::file_descriptors::FileDescriptorMetrics> {
        self.runtime.file_descriptor_metrics()
    }

    /// Returns a handle to the database-owned runtime QoS scheduler.
    /// Cloned handles share the same policy, counters, and permit ownership.
    pub fn local_qos_scheduler(&self) -> LocalQosScheduler {
        self.local_qos_scheduler.clone()
    }

    pub(crate) fn runtime_capabilities(&self) -> hawdb_core::RuntimeCapabilities {
        self.config.runtime_capabilities
    }

    pub(super) fn ensure_runtime_capability(
        &self,
        capability: hawdb_core::RuntimeCapability,
    ) -> Result<()> {
        self.config.runtime_capabilities.require(capability)
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        if let Some(governor) = &self.runtime_governor {
            governor.set_telemetry_sink(
                telemetry
                    .as_ref()
                    .map(|telemetry| runtime_telemetry_sink(telemetry.clone())),
            );
        }
        if let Some(telemetry) = &telemetry
            && let Some(runtime) = self.runtime.peek()
        {
            runtime_cell::record_recovery_telemetry(&runtime.store, telemetry.as_ref());
        }
        if let Some(mut runtime) = self.runtime.peek_mut() {
            crate::store::StoreTelemetry::set_telemetry_sink(&mut runtime.store, telemetry.clone());
        }
        if let Some(pending) = self.runtime.pending_mut() {
            pending.telemetry = telemetry.clone();
        }
        self.local_qos_scheduler.set_telemetry_sink(
            telemetry
                .as_ref()
                .map(|telemetry| qos_telemetry_sink(telemetry.clone())),
        );
        self.telemetry = telemetry;
    }

    pub fn telemetry_sink_configured(&self) -> bool {
        self.telemetry.is_some()
    }

    pub fn operations_telemetry_readiness(
        &self,
        search_index: Option<&SearchIndex>,
    ) -> OperationsTelemetryReadiness {
        operations_telemetry_readiness(
            self.telemetry_sink_configured(),
            search_index
                .map(SearchIndex::telemetry_sink_configured)
                .unwrap_or(false),
        )
    }

    pub(super) fn local_qos_scheduler_for_work(&self) -> LocalQosScheduler {
        self.local_qos_scheduler()
    }

    pub fn system_variables(&self) -> &QuerySystemVariables {
        &self.system_variables
    }

    /// The builder-lifetime byte reservation one shadow build needs; zero
    /// when `graph_columnar_shadow_checkpoint` is off.
    pub fn columnar_shadow_admission_bytes(&self) -> Result<u64> {
        Ok(self.runtime.get()?.store.columnar_shadow_admission_bytes())
    }

    pub(super) fn internal_descriptor_operation(
        &self,
    ) -> Result<Option<hawdb_storage::file_descriptors::DescriptorReservation>> {
        self.runtime
            .file_descriptor_context()
            .map(|context| context.internal_operation())
            .transpose()
            .map(Option::flatten)
            .map_err(Into::into)
    }

    pub fn storage_reclamation_watermark(&self) -> Result<StorageReclamationWatermark> {
        Ok({
            let oldest_reader_epoch = self
                .runtime
                .get()?
                .reader_pins
                .lock()
                .expect("database reader pins lock should not be poisoned")
                .oldest_epoch();
            self.runtime
                .get()?
                .store
                .storage_reclamation_watermark(oldest_reader_epoch)
        })
    }

    pub fn storage_recovery_report(&self) -> Result<StorageRecoveryReport> {
        Ok(self.runtime.get()?.store.storage_recovery_report())
    }

    /// Inventories regular files in the selected branch's durable artifact
    /// directory for storage qualification. The directory handle participates
    /// in the project's FD budget; immutable closure objects live separately.
    #[doc(hidden)]
    pub fn storage_artifact_file_sizes(&self) -> Result<BTreeMap<String, u64>> {
        let runtime = self.runtime.get()?;
        let root = runtime.store.durable_root_path().ok_or_else(|| {
            HawDBError::Execution("storage artifact inventory requires a durable database".into())
        })?;
        let mut files = BTreeMap::new();
        for entry in hawdb_storage::file_io::read_dir(root)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                let name = entry.file_name().into_string().map_err(|_| {
                    HawDBError::Storage("storage artifact inventory found a non-UTF-8 name".into())
                })?;
                files.insert(name, metadata.len());
            }
        }
        Ok(files)
    }

    pub(crate) fn poison_on_storage_error<T>(&self, result: &Result<T>) {
        if let Some(runtime) = self.runtime.peek() {
            runtime.store.poison_on_storage_error(result);
        }
    }

    pub fn storage_handle_poisoned(&self) -> Result<bool> {
        Ok(self.runtime.get()?.store.storage_handle_poisoned())
    }

    pub fn storage_residency_report(&self) -> Result<crate::store::StorageResidencyReport> {
        Ok(self.runtime.get()?.store.storage_residency_report())
    }

    /// Threads the engine's runtime governor into the storage layer so
    /// background columnar-shadow work can request admission. Called by the
    /// embedding layers that own the governor (`HawDBEmbedded`,
    /// `NowledgeMemGraph`); a second governor is never constructed here.
    pub fn set_runtime_governor(&mut self, governor: hawdb_qos::RuntimeGovernor) {
        self.retained_runtime.configure(governor.clone());
        if let Err(error) = self.runtime.set_automatic_governor(governor.clone())
            && let Some(runtime) = self.runtime.peek()
        {
            runtime.store.poison_on_storage_error(&Err::<(), _>(error));
        }
        if let Some(telemetry) = &self.telemetry {
            governor.set_telemetry_sink(Some(runtime_telemetry_sink(telemetry.clone())));
        }
        if let Some(mut runtime) = self.runtime.peek_mut() {
            runtime.store.set_runtime_governor(governor.clone());
        }
        if let Some(pending) = self.runtime.pending_mut() {
            pending.governor = Some(governor.clone());
        }
        self.runtime_governor = Some(governor);
    }

    /// What recovery observed about the columnar shadow catalog.
    pub fn columnar_shadow_recovery_status(
        &self,
    ) -> Result<crate::store::ColumnarShadowRecoveryStatus> {
        Ok(self.runtime.get()?.store.columnar_shadow_recovery_status())
    }

    /// Recovery's bounded manifest-only assessment of the relational index
    /// store, as an owned snapshot. Selected pages remain lazily validated on first SQL access.
    pub fn relational_index_shadow_recovery_status(
        &self,
    ) -> Result<crate::store::RelationalIndexShadowRecoveryStatus> {
        Ok({
            self.runtime
                .get()?
                .store
                .relational_index_shadow_recovery_status()
                .clone()
        })
    }

    /// Resource and publication evidence for WAL index deltas derived during
    /// the most recent open. The owned report can outlive this handle.
    pub fn relational_index_recovery_report(
        &self,
    ) -> Result<Option<hawdb_storage::relational::RelationalIndexRecoveryReport>> {
        Ok(self
            .runtime
            .get()?
            .store
            .relational_index_recovery_report()
            .cloned())
    }

    pub fn storage_pressure_snapshot(&self) -> Result<StoragePressureSnapshot> {
        Ok({
            let oldest_reader_epoch = self
                .runtime
                .get()?
                .reader_pins
                .lock()
                .expect("database reader pins lock should not be poisoned")
                .oldest_epoch();
            self.runtime
                .get()?
                .store
                .storage_pressure_snapshot(oldest_reader_epoch)
        })
    }

    pub fn segment_cache_snapshot(&self) -> Result<Option<SegmentCacheSnapshot>> {
        Ok(self.runtime.get()?.store.segment_cache_snapshot())
    }

    pub fn export_canonical_graph_snapshot(&self) -> Result<CanonicalGraphSnapshotExport> {
        Ok({
            export_canonical_graph_snapshot_for(
                &self.runtime.get()?.catalog,
                &self.runtime.get()?.store,
            )
        })
    }

    pub fn try_export_canonical_graph_snapshot(&self) -> Result<CanonicalGraphSnapshotExport> {
        canonical_snapshot::try_export_canonical_graph_snapshot_for(
            &self.runtime.get()?.catalog,
            &self.runtime.get()?.store,
        )
    }

    pub fn export_canonical_graph_snapshot_with_persisted_stable_ids(
        &mut self,
    ) -> Result<CanonicalGraphSnapshotExport> {
        self.ensure_writable()?;
        let snapshot = self.try_export_canonical_graph_snapshot()?;
        let duplicate_node_stable_ids = snapshot
            .stable_identity
            .duplicate_node_stable_ids
            .iter()
            .collect::<BTreeSet<_>>();
        let duplicate_relationship_stable_ids = snapshot
            .stable_identity
            .duplicate_relationship_stable_ids
            .iter()
            .collect::<BTreeSet<_>>();
        let required_node_ids = snapshot
            .nodes
            .iter()
            .filter(|node| {
                node.stable_id
                    .as_ref()
                    .is_none_or(|stable_id| duplicate_node_stable_ids.contains(stable_id))
            })
            .map(|node| NodeId(node.node_id))
            .collect::<BTreeSet<_>>();
        let required_relationship_ids = snapshot
            .relationships
            .iter()
            .filter(|relationship| {
                relationship
                    .stable_id
                    .as_ref()
                    .is_none_or(|stable_id| duplicate_relationship_stable_ids.contains(stable_id))
            })
            .map(|relationship| RelId(relationship.relationship_id))
            .collect::<BTreeSet<_>>();
        let mapping = CanonicalStableIdMapping::from(
            self.runtime
                .get_mut()?
                .store
                .ensure_stable_id_mapping(&required_node_ids, &required_relationship_ids)?,
        );
        Ok(snapshot.with_stable_id_mapping(&mapping))
    }

    pub fn storage_version(&self) -> Result<&'static str> {
        Ok(self.runtime.get()?.store.storage_version())
    }
}
