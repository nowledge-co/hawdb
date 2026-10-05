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

//! One admitted branch bundle, with recoverable deferred admission.
//!
//! A pending project contains identities/configuration and its shared FD domain.
//! It contains no empty dataset that could accidentally answer a data query.
//! Failed admission is never cached as success, and only a fully validated
//! candidate enters the cell. Readers keep the original runtime's snapshot pins.

use super::{
    branch_lifecycle::{BranchLifecycleError, BranchSelection},
    configure_relational_fast_paths, configure_search_projection_changefeed,
    search_projection_consumer, CascadesOptimizer, Catalog, Database, DatabaseConfig,
    DurabilityPolicy, GraphStore, LocalQosScheduler, OptimizerPlanningCache, PlanCache, ReaderPins,
    SharedState, TelemetrySink,
};
use crate::error::{HawDBError, Result};
use hawdb_storage::branch_project::{ProjectMetadata, ProjectSelector};
use hawdb_storage::file_descriptors::{
    FileDescriptorMetrics, FileOpenContext, ProjectFileDescriptors,
};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug)]
pub(super) struct AdmittedBranchRuntime {
    pub(super) catalog: Catalog,
    pub(super) store: GraphStore,
    pub(super) branch_selection: Option<BranchSelection>,
    pub(super) optimizer: CascadesOptimizer,
    pub(super) plan_cache: Arc<SharedState<PlanCache>>,
    pub(super) relational_plan_template_cache:
        Arc<crate::relational_sql::RelationalPlanTemplateCache>,
    pub(super) optimizer_planning_cache: Arc<SharedState<OptimizerPlanningCache>>,
    pub(super) reader_pins: Arc<Mutex<ReaderPins>>,
    pub(super) projection_consumers: search_projection_consumer::ConsumerRegistry,
}

#[derive(Debug)]
pub(super) struct DeferredBranchAdmission {
    pub(super) files: ProjectFileDescriptors,
    pub(super) selector: ProjectSelector,
    pub(super) config: DatabaseConfig,
    pub(super) durability: DurabilityPolicy,
    pub(super) scheduler: LocalQosScheduler,
    pub(super) telemetry: Option<Arc<dyn TelemetrySink>>,
    pub(super) governor: Option<hawdb_qos::RuntimeGovernor>,
}

#[derive(Debug)]
pub(super) struct BranchRuntimeCell {
    admitted: OnceLock<Box<AdmittedBranchRuntime>>,
    pending: Option<DeferredBranchAdmission>,
    admission: Mutex<()>,
}

impl BranchRuntimeCell {
    pub(super) fn admitted(runtime: AdmittedBranchRuntime) -> Self {
        Self {
            admitted: OnceLock::from(Box::new(runtime)),
            pending: None,
            admission: Mutex::new(()),
        }
    }

    pub(super) fn deferred(pending: DeferredBranchAdmission) -> Self {
        Self {
            admitted: OnceLock::new(),
            pending: Some(pending),
            admission: Mutex::new(()),
        }
    }

    pub(super) fn get(&self) -> Result<&AdmittedBranchRuntime> {
        if let Some(runtime) = self.admitted.get() {
            return Ok(runtime);
        }
        let admission = self.admission.lock().map_err(|_| {
            HawDBError::StorageIntegrity("deferred branch admission lock is poisoned".into())
        })?;
        if let Some(runtime) = self.admitted.get() {
            return Ok(runtime);
        }
        let pending = self.pending.as_ref().ok_or_else(|| {
            HawDBError::StorageIntegrity("database has no branch admission identity".into())
        })?;
        let candidate = recover_default_branch(pending)?;
        self.admitted.set(Box::new(candidate)).map_err(|_| {
            HawDBError::StorageIntegrity("branch runtime was concurrently published twice".into())
        })?;
        let runtime = self.admitted.get().map(Box::as_ref).ok_or_else(|| {
            HawDBError::StorageIntegrity("completed branch admission has no runtime".into())
        })?;
        // Host callbacks may read this database. Publish the complete bundle
        // and release admission before notifying them, including on a panic.
        drop(admission);
        if let Some(telemetry) = &pending.telemetry {
            record_recovery_telemetry(&runtime.store, telemetry.as_ref());
        }
        Ok(runtime)
    }

    pub(super) fn get_mut(&mut self) -> Result<&mut AdmittedBranchRuntime> {
        self.get()?;
        self.admitted.get_mut().map(Box::as_mut).ok_or_else(|| {
            HawDBError::StorageIntegrity("completed branch admission has no mutable runtime".into())
        })
    }

    pub(super) fn peek(&self) -> Option<&AdmittedBranchRuntime> {
        self.admitted.get().map(Box::as_ref)
    }

    pub(super) fn peek_mut(&mut self) -> Option<&mut AdmittedBranchRuntime> {
        self.admitted.get_mut().map(Box::as_mut)
    }

    pub(super) fn pending_mut(&mut self) -> Option<&mut DeferredBranchAdmission> {
        self.pending.as_mut()
    }

    pub(super) fn file_descriptor_metrics(&self) -> Option<FileDescriptorMetrics> {
        self.peek()
            .and_then(|runtime| runtime.store.file_descriptor_metrics())
            .or_else(|| self.pending.as_ref().map(|pending| pending.files.metrics()))
    }

    pub(super) fn file_descriptor_context(&self) -> Option<FileOpenContext> {
        self.peek()
            .and_then(|runtime| runtime.store.file_descriptor_context())
            .or_else(|| {
                self.pending
                    .as_ref()
                    .map(|pending| pending.files.io_context())
            })
    }

    pub(super) fn reserve_target_admission_resources(
        &self,
    ) -> Result<hawdb_storage::file_descriptors::DescriptorReservation> {
        if let Some(runtime) = self.peek() {
            return runtime.store.reserve_branch_admission_resources();
        }
        let pending = self.pending.as_ref().ok_or_else(|| {
            HawDBError::StorageIntegrity("database has no project admission domain".into())
        })?;
        GraphStore::reserve_project_branch_admission_resources(&pending.files)
    }

    pub(super) fn into_admitted(self) -> Result<AdmittedBranchRuntime> {
        self.admitted
            .into_inner()
            .map(|runtime| *runtime)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("candidate branch runtime was not admitted".into())
            })
    }
}

pub(super) fn record_recovery_telemetry(store: &GraphStore, telemetry: &dyn TelemetrySink) {
    let recovery = store.storage_recovery_report();
    if recovery.durable {
        telemetry.record_kernel(super::KernelTelemetry {
            operation: super::KernelTelemetryOperation::Recovery,
            success: true,
            elapsed_micros: 0,
            item_count: recovery.replayed_wal_entries,
            byte_count: recovery.replayed_wal_bytes,
            fsync_micros: 0,
            generation: recovery.wal_generation,
        });
    }
}

fn recover_default_branch(pending: &DeferredBranchAdmission) -> Result<AdmittedBranchRuntime> {
    let _resources = GraphStore::reserve_project_branch_admission_resources(&pending.files)?;
    let metadata = ProjectMetadata::from_files(pending.files.clone())?;
    if metadata.selector() != pending.selector {
        return Err(HawDBError::StorageIntegrity(
            "default branch project identity changed before admission".into(),
        ));
    }
    let record = metadata.main().clone();
    drop(metadata);
    let root = pending.files.root();
    let catalog_path = hawdb_storage::branch_project::catalog_path(root);
    let directory = root.join("branches").join(record.id.as_uuid().to_string());
    let head_path = directory.join("branch.head");
    let objects = root.join("branches").join("objects");
    let admit = if pending.config.read_only {
        GraphStore::admit_read_only_branch_from_head
    } else {
        GraphStore::admit_branch_from_head
    };
    let admitted = admit(hawdb_storage::store::BranchAdmissionRequest {
        catalog_path: &catalog_path,
        branch_id: record.id,
        expected_metadata_revision: record.metadata_revision,
        head_path: &head_path,
        immutable_store_root: &objects,
        durability: pending.durability,
        replay_config: pending.config.wal_replay_config(),
    })
    .map_err(|error| HawDBError::from(BranchLifecycleError::Admission(error)))?;
    let (mut store, schema) = admitted.into_parts();
    configure_search_projection_changefeed(&mut store, &pending.config);
    configure_relational_fast_paths(&mut store, &pending.config);
    let mut candidate = Database::new_with_config(pending.config.clone());
    {
        let runtime = candidate.runtime.get_mut()?;
        runtime.store = store;
        runtime.catalog = schema;
        runtime.branch_selection = Some(BranchSelection { record });
    }
    candidate.project_root_path = Some(root.to_path_buf());
    candidate.durability = pending.durability;
    candidate.local_qos_scheduler = pending.scheduler.clone();
    if let Some(governor) = &pending.governor {
        candidate.set_runtime_governor(governor.clone());
    }
    if !pending.config.read_only {
        candidate.complete_required_relational_row_checkpoint("default branch recovery")?;
    }
    candidate.apply_engine_system_schema()?;
    {
        let runtime = candidate.runtime.get_mut()?;
        runtime.projection_consumers = search_projection_consumer::ConsumerRegistry::load(
            runtime.store.search_projection_registry_root(),
            runtime.store.search_projection_database_identity(),
        );
    }
    crate::store::StoreTelemetry::set_telemetry_sink(
        &mut candidate.runtime.get_mut()?.store,
        pending.telemetry.clone(),
    );
    candidate.runtime.into_admitted()
}
