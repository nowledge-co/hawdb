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
    automatic_checkpoint::{Control, Owner, State, Suspension},
    branch_lifecycle::{BranchLifecycleError, BranchSelection},
    configure_relational_fast_paths, configure_search_projection_changefeed,
    search_projection_consumer, CascadesOptimizer, Catalog, Database, DatabaseConfig,
    DatabaseInstanceIdentity, DurabilityPolicy, GraphStore, LocalQosScheduler,
    OptimizerPlanningCache, PlanCache, ReaderPins, SharedState, TelemetrySink,
};
use crate::error::{HawDBError, Result};
use hawdb_storage::branch_project::CapturedProject;
use hawdb_storage::file_descriptors::{FileDescriptorMetrics, FileOpenContext};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

mod guards;
pub(super) use guards::SharedBranchRuntime;
use guards::{RuntimeRead, RuntimeWrite};

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
    pub(super) config: DatabaseConfig,
    pub(super) durability: DurabilityPolicy,
    pub(super) scheduler: LocalQosScheduler,
    pub(super) telemetry: Option<Arc<dyn TelemetrySink>>,
    pub(super) governor: Option<hawdb_qos::RuntimeGovernor>,
}

#[derive(Debug)]
pub(super) struct BranchRuntimeCell {
    admitted: Arc<SharedBranchRuntime>,
    pending: Option<DeferredBranchAdmission>,
    project: Option<CapturedProject>,
    admission: Mutex<()>,
    instance_identity: DatabaseInstanceIdentity,
    // Mutations and background selector publication share this barrier.
    // Reads borrow the stable frontend and only try to adopt a ready handoff.
    publication: Arc<Control>,
    automatic: OnceLock<Option<Owner>>,
    automatic_start: Mutex<()>,
}

#[derive(Debug)]
pub(super) struct AdmittedBranchRuntimeMut<'a> {
    runtime: RuntimeWrite<'a>,
    publication: Option<MutexGuard<'a, State>>,
    control: &'a Control,
}

impl Drop for AdmittedBranchRuntimeMut<'_> {
    fn drop(&mut self) {
        let retired = self.publication.take().and_then(|mut state| {
            self.control
                .submit(&mut state, &self.runtime.store, &self.runtime.catalog)
        });
        // The closure released its mutex guard. Captured-source destruction
        // must not extend the foreground publication critical section.
        drop(retired);
    }
}

impl Deref for AdmittedBranchRuntimeMut<'_> {
    type Target = AdmittedBranchRuntime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl DerefMut for AdmittedBranchRuntimeMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

/// Read execution may update local caches, but callers cannot access mutable
/// live storage or the catalog without the mutation/publication guard.
#[derive(Debug)]
pub(super) struct AdmittedBranchRuntimeRead<'a> {
    runtime: RuntimeWrite<'a>,
}

impl AdmittedBranchRuntimeRead<'_> {
    pub(super) fn catalog(&self) -> &Catalog {
        &self.runtime.catalog
    }

    pub(super) fn store(&self) -> &GraphStore {
        &self.runtime.store
    }

    pub(super) fn execute(
        &mut self,
        request: crate::executor::ExecutionRequest<'_>,
        external: &mut dyn crate::executor::ExternalReadOperator,
    ) -> Result<crate::executor::ProfiledQueryRows> {
        if crate::executor::requires_write_access(request.plan()) {
            return Err(HawDBError::Execution(
                "read runtime access does not permit storage writes".into(),
            ));
        }
        let runtime = &mut *self.runtime;
        crate::executor::execute_with_request(
            request,
            crate::executor::ExecutionResources::new(
                &mut runtime.catalog,
                &mut runtime.store,
                external,
            ),
        )
    }
}

impl BranchRuntimeCell {
    pub(super) fn admitted(runtime: AdmittedBranchRuntime) -> Self {
        let admitted = Arc::new(SharedBranchRuntime::new(Some(runtime)));
        let publication = Arc::new(Control::default());
        publication.attach_frontend(&admitted);
        Self {
            admitted,
            pending: None,
            project: None,
            admission: Mutex::new(()),
            publication,
            automatic: OnceLock::new(),
            automatic_start: Mutex::new(()),
            instance_identity: DatabaseInstanceIdentity::new(),
        }
    }

    pub(super) fn deferred(pending: DeferredBranchAdmission, project: CapturedProject) -> Self {
        let admitted = Arc::new(SharedBranchRuntime::default());
        let publication = Arc::new(Control::default());
        publication.attach_frontend(&admitted);
        Self {
            admitted,
            pending: Some(pending),
            project: Some(project),
            admission: Mutex::new(()),
            publication,
            automatic: OnceLock::new(),
            automatic_start: Mutex::new(()),
            instance_identity: DatabaseInstanceIdentity::new(),
        }
    }

    pub(super) fn instance_identity(&self) -> DatabaseInstanceIdentity {
        self.instance_identity.clone()
    }

    pub(super) fn get(&self) -> Result<RuntimeRead<'_>> {
        self.publication.ensure_healthy()?;
        if self.admitted.read(&self.publication)?.is_some() {
            if let Some(pending) = &self.pending {
                self.arm_automatic(
                    &pending.config,
                    pending.scheduler.clone(),
                    pending.governor.clone(),
                )?;
            }
            return self.admitted.read(&self.publication)?.ok_or_else(|| {
                HawDBError::StorageIntegrity("completed branch admission has no runtime".into())
            });
        }
        let admission = self.admission.lock().map_err(|_| {
            HawDBError::StorageIntegrity("deferred branch admission lock is poisoned".into())
        })?;
        if let Some(runtime) = self.admitted.read(&self.publication)? {
            return Ok(runtime);
        }
        let pending = self.pending.as_ref().ok_or_else(|| {
            HawDBError::StorageIntegrity("database has no branch admission identity".into())
        })?;
        let project = self.project.as_ref().ok_or_else(|| {
            HawDBError::StorageIntegrity("database has no captured project".into())
        })?;
        let candidate = recover_default_branch(pending, project)?;
        self.admitted.initialize(candidate)?;
        drop(admission);
        self.arm_automatic(
            &pending.config,
            pending.scheduler.clone(),
            pending.governor.clone(),
        )?;
        let runtime = self.admitted.read(&self.publication)?.ok_or_else(|| {
            HawDBError::StorageIntegrity("completed branch admission has no runtime".into())
        })?;
        if let Some(telemetry) = &pending.telemetry {
            record_recovery_telemetry(&runtime.store, telemetry.as_ref());
        }
        Ok(runtime)
    }

    pub(super) fn get_mut(&mut self) -> Result<AdmittedBranchRuntimeMut<'_>> {
        self.get()?;
        let mut publication =
            Self::publication_barrier(&self.publication, self.needs_publication_barrier())?;
        let mut runtime = self.admitted.write(&self.publication)?;
        if let Some(state) = &mut publication {
            self.publication.adopt(state, &mut runtime.store)?;
        }
        Ok(AdmittedBranchRuntimeMut {
            runtime,
            publication,
            control: &self.publication,
        })
    }

    pub(super) fn get_read(&mut self) -> Result<AdmittedBranchRuntimeRead<'_>> {
        self.get()?;
        let mut runtime = self.admitted.write(&self.publication)?;
        self.publication.adopt_for_read(&mut runtime.store)?;
        // &mut self exclusively owns the frontend. Read execution may update
        // its local caches without holding the owner's publication mutex.
        Ok(AdmittedBranchRuntimeRead { runtime })
    }

    pub(super) fn peek(&self) -> Option<RuntimeRead<'_>> {
        self.admitted.read(&self.publication).ok().flatten()
    }

    pub(super) fn peek_mut(&mut self) -> Option<AdmittedBranchRuntimeMut<'_>> {
        let mut publication =
            Self::publication_barrier(&self.publication, self.needs_publication_barrier()).ok()?;
        let mut runtime = self.admitted.write(&self.publication).ok()?;
        if let Some(state) = &mut publication {
            self.publication.adopt(state, &mut runtime.store).ok()?;
        }
        Some(AdmittedBranchRuntimeMut {
            runtime,
            publication,
            control: &self.publication,
        })
    }

    fn needs_publication_barrier(&self) -> bool {
        cfg!(all(
            feature = "background-maintenance",
            not(target_arch = "wasm32")
        )) && self
            .peek()
            .is_some_and(|runtime| runtime.store.storage_recovery_report().durable)
    }

    fn publication_barrier(
        publication: &Control,
        needed: bool,
    ) -> Result<Option<MutexGuard<'_, State>>> {
        if !needed {
            return Ok(None);
        }
        publication.lock_frontend().map(Some)
    }

    pub(super) fn arm_automatic(
        &self,
        config: &DatabaseConfig,
        scheduler: LocalQosScheduler,
        governor: Option<hawdb_qos::RuntimeGovernor>,
    ) -> Result<()> {
        if self.automatic.get().is_some() {
            return Ok(());
        }
        let _start = self.automatic_start.lock().map_err(|_| {
            HawDBError::StorageIntegrity("checkpoint startup lock is poisoned".into())
        })?;
        if self.automatic.get().is_some() {
            return Ok(());
        }
        let runtime = self.peek().ok_or_else(|| {
            HawDBError::StorageIntegrity(
                "checkpoint owner requires completed branch admission".into(),
            )
        })?;
        let owner = Owner::start(
            Arc::clone(&self.publication),
            &runtime.store,
            &runtime.catalog,
            Arc::clone(&runtime.reader_pins),
            config,
            scheduler,
            governor,
        )?;
        self.automatic
            .set(owner)
            .map_err(|_| HawDBError::StorageIntegrity("checkpoint owner started twice".into()))
    }

    pub(super) fn checkpoint_control(&self) -> Arc<Control> {
        Arc::clone(&self.publication)
    }

    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    pub(super) fn checkpoint_control_for_test(&self) -> Arc<Control> {
        self.checkpoint_control()
    }

    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    pub(super) fn shared_runtime_for_test(&self) -> Arc<SharedBranchRuntime> {
        Arc::clone(&self.admitted)
    }

    pub(super) fn automatic_checkpoint_report(
        &self,
    ) -> Result<Option<super::AutomaticCheckpointReport>> {
        self.publication.report()
    }

    pub(super) fn suspend_automatic_checkpoint(&mut self) -> Result<Option<Suspension>> {
        self.get()?;
        if self.automatic.get().is_none_or(Option::is_none) {
            return Ok(None);
        }
        let suspension = self.publication.suspend()?;
        // Selection can have completed while suspension was requested. Adopt
        // it before waiting for the worker's off-gate retirement to finish.
        drop(self.get_mut()?);
        suspension.wait_idle()?;
        Ok(Some(suspension))
    }

    pub(super) fn set_automatic_governor(
        &self,
        governor: hawdb_qos::RuntimeGovernor,
    ) -> Result<()> {
        self.publication.set_governor(governor)
    }

    pub(super) fn release_suspended_checkpoint_source(&self) -> Result<()> {
        self.publication.release_suspended_source()
    }

    pub(super) fn captured_project(&self) -> Option<&CapturedProject> {
        self.project.as_ref()
    }

    pub(super) fn retain_project_capture(&mut self, project: Option<CapturedProject>) {
        self.project = project;
    }

    pub(super) fn pending_mut(&mut self) -> Option<&mut DeferredBranchAdmission> {
        self.pending.as_mut()
    }

    pub(super) fn file_descriptor_metrics(&self) -> Option<FileDescriptorMetrics> {
        self.peek()
            .and_then(|runtime| runtime.store.file_descriptor_metrics())
            .or_else(|| {
                self.project
                    .as_ref()
                    .map(|project| project.file_descriptors().metrics())
            })
    }

    pub(super) fn file_descriptor_context(&self) -> Option<FileOpenContext> {
        self.peek()
            .and_then(|runtime| runtime.store.file_descriptor_context())
            .or_else(|| {
                self.project
                    .as_ref()
                    .map(|project| project.file_descriptors().io_context())
            })
    }

    pub(super) fn reserve_target_admission_resources(
        &self,
    ) -> Result<hawdb_storage::file_descriptors::DescriptorReservation> {
        if let Some(runtime) = self.peek() {
            return runtime.store.reserve_branch_admission_resources();
        }
        let project = self.project.as_ref().ok_or_else(|| {
            HawDBError::StorageIntegrity("database has no project admission domain".into())
        })?;
        project.require_original_namespace()?;
        GraphStore::reserve_project_branch_admission_resources(project.file_descriptors())
    }

    pub(super) fn into_admitted(mut self) -> Result<AdmittedBranchRuntime> {
        if let Some(Some(owner)) = self.automatic.get_mut() {
            owner.stop();
        }
        if self.peek().is_some() {
            drop(self.get_mut()?);
        }
        self.admitted
            .take()?
            .map(|runtime| *runtime)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("candidate branch runtime was not admitted".into())
            })
    }
}

impl Drop for BranchRuntimeCell {
    fn drop(&mut self) {
        if let Some(Some(owner)) = self.automatic.get_mut() {
            owner.stop();
        }
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

fn recover_default_branch(
    pending: &DeferredBranchAdmission,
    project: &CapturedProject,
) -> Result<AdmittedBranchRuntime> {
    let resources =
        GraphStore::reserve_project_branch_admission_resources(project.file_descriptors())?;
    project.require_original_namespace()?;
    let metadata = project.metadata()?;
    let record = metadata.main().clone();
    drop(metadata);
    let root = project.file_descriptors().root();
    let admitted =
        GraphStore::admit_captured_branch(hawdb_storage::store::CapturedBranchAdmissionRequest {
            project,
            branch_id: record.id,
            expected_metadata_revision: record.metadata_revision,
            durability: pending.durability,
            replay_config: pending.config.wal_replay_config(),
            read_only: pending.config.read_only,
        })
        .map_err(|error| HawDBError::from(BranchLifecycleError::Admission(error)))?;
    let (mut store, schema) = admitted.into_parts();
    // Branch admission has finished and its native owners are retained by the
    // store. Release the admission IO wave before recovery can require its own
    // checkpoint publication reservation.
    drop(resources);
    configure_search_projection_changefeed(&mut store, &pending.config);
    configure_relational_fast_paths(&mut store, &pending.config);
    let mut candidate = Database::new_with_config(pending.config.clone());
    {
        let mut runtime_access = candidate.runtime.get_mut()?;
        let runtime = &mut *runtime_access;
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
        let mut runtime_access = candidate.runtime.get_mut()?;
        let runtime = &mut *runtime_access;
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
