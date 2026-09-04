use super::{optional_u64_value, optional_usize_value, Database, QueryOutput};
use crate::error::{Result, SkeinError};
use crate::executor::Row;
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosState, QosAdmission, WorkClass,
    WorkRequest,
};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedArtifactJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl DerivedArtifactJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DerivedArtifactJobStatus::Pending => "pending",
            DerivedArtifactJobStatus::Running => "running",
            DerivedArtifactJobStatus::Succeeded => "succeeded",
            DerivedArtifactJobStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJob {
    pub id: u64,
    pub artifact_type: String,
    pub name: String,
    pub action: String,
    pub payload: BTreeMap<String, Value>,
    pub status: DerivedArtifactJobStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub last_output: Option<QueryOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJobReport {
    pub job: DerivedArtifactJob,
    pub output: QueryOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalContentArtifactJobCompletion {
    pub runtime_name: String,
    pub runtime_version: Option<String>,
    pub input_ref: Option<String>,
    pub input_checksum: Option<String>,
    pub output_ref: Option<String>,
    pub output_checksum: Option<String>,
    pub projection_kind: Option<String>,
    pub projection_ref: Option<String>,
    pub source_graph_commit_epoch: Option<u64>,
    pub rows_produced: Option<usize>,
    pub metadata: BTreeMap<String, Value>,
}

impl ExternalContentArtifactJobCompletion {
    pub fn new(runtime_name: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            runtime_version: None,
            input_ref: None,
            input_checksum: None,
            output_ref: None,
            output_checksum: None,
            projection_kind: None,
            projection_ref: None,
            source_graph_commit_epoch: None,
            rows_produced: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_runtime_version(mut self, runtime_version: impl Into<String>) -> Self {
        self.runtime_version = Some(runtime_version.into());
        self
    }

    pub fn with_input_ref(mut self, input_ref: impl Into<String>) -> Self {
        self.input_ref = Some(input_ref.into());
        self
    }

    pub fn with_input_checksum(mut self, input_checksum: impl Into<String>) -> Self {
        self.input_checksum = Some(input_checksum.into());
        self
    }

    pub fn with_output_ref(mut self, output_ref: impl Into<String>) -> Self {
        self.output_ref = Some(output_ref.into());
        self
    }

    pub fn with_output_checksum(mut self, output_checksum: impl Into<String>) -> Self {
        self.output_checksum = Some(output_checksum.into());
        self
    }

    pub fn with_projection(
        mut self,
        projection_kind: impl Into<String>,
        projection_ref: impl Into<String>,
    ) -> Self {
        self.projection_kind = Some(projection_kind.into());
        self.projection_ref = Some(projection_ref.into());
        self
    }

    pub fn with_source_graph_commit_epoch(mut self, commit_epoch: u64) -> Self {
        self.source_graph_commit_epoch = Some(commit_epoch);
        self
    }

    pub fn with_rows_produced(mut self, rows_produced: usize) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

impl DerivedArtifactJob {
    pub fn background_work_request(&self, estimated_operations: usize) -> WorkRequest {
        let class = if self.artifact_type == "projected_graph" {
            WorkClass::Projection
        } else {
            WorkClass::Import
        };
        WorkRequest::background(class, estimated_operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExternalContentArtifactJobSummary {
    pub total: usize,
    pub pending: usize,
    pub running: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub pending_by_action: BTreeMap<String, usize>,
    pub failed_by_action: BTreeMap<String, usize>,
    pub next_pending_job_id: Option<u64>,
    pub oldest_failed_job_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalContentArtifactRuntimeManifest {
    pub runtime_name: String,
    pub runtime_version: Option<String>,
    pub supported_actions: BTreeSet<String>,
    pub required_payload_keys: BTreeSet<String>,
    pub estimated_operations: usize,
}

impl ExternalContentArtifactRuntimeManifest {
    pub fn new(runtime_name: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            runtime_version: None,
            supported_actions: BTreeSet::new(),
            required_payload_keys: BTreeSet::new(),
            estimated_operations: 1,
        }
    }

    pub fn with_runtime_version(mut self, runtime_version: impl Into<String>) -> Self {
        self.runtime_version = Some(runtime_version.into());
        self
    }

    pub fn with_supported_action(mut self, action: impl Into<String>) -> Self {
        self.supported_actions.insert(action.into());
        self
    }

    pub fn with_required_payload_key(mut self, key: impl Into<String>) -> Self {
        self.required_payload_keys.insert(key.into());
        self
    }

    pub fn with_estimated_operations(mut self, estimated_operations: usize) -> Self {
        self.estimated_operations = estimated_operations;
        self
    }
}

impl Database {
    pub fn schedule_derived_artifact_rebuild(&mut self) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job("projected_graph", "*", "rebuild")
    }

    pub fn schedule_projected_graph_artifact_rebuild(
        &mut self,
        name: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job("projected_graph", name.into(), "rebuild")
    }

    pub fn schedule_external_content_artifact_job(
        &mut self,
        name: impl Into<String>,
        action: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.schedule_external_content_artifact_job_with_payload(name, action, BTreeMap::new())
    }

    pub fn schedule_external_content_artifact_job_with_payload(
        &mut self,
        name: impl Into<String>,
        action: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job_with_payload(
            "content_artifact",
            name.into(),
            action.into(),
            payload,
        )
    }

    pub fn derived_artifact_jobs(&self) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs.clone()
    }

    pub fn pending_external_content_artifact_jobs(&self, limit: usize) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn pending_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && job.action == action
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn pending_external_content_artifact_jobs_for_runtime(
        &self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && external_content_runtime_can_claim(manifest, job)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn failed_external_content_artifact_jobs(&self, limit: usize) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Failed
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn succeeded_external_content_artifact_jobs(
        &self,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Succeeded
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn failed_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Failed
                    && job.action == action
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn succeeded_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Succeeded
                    && job.action == action
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn external_content_artifact_job_summary(&self) -> ExternalContentArtifactJobSummary {
        let mut summary = ExternalContentArtifactJobSummary::default();
        for job in self
            .derived_artifact_jobs
            .iter()
            .filter(|job| is_external_content_artifact_job(&job.artifact_type))
        {
            summarize_external_content_artifact_job(&mut summary, job);
        }
        summary
    }

    pub fn external_content_artifact_job_summary_for_action(
        &self,
        action: &str,
    ) -> ExternalContentArtifactJobSummary {
        let mut summary = ExternalContentArtifactJobSummary::default();
        for job in self.derived_artifact_jobs.iter().filter(|job| {
            job.action == action && is_external_content_artifact_job(&job.artifact_type)
        }) {
            summarize_external_content_artifact_job(&mut summary, job);
        }
        summary
    }

    pub fn external_content_artifact_job_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
        estimated_operations: usize,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .iter()
            .any(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .then(|| BackgroundWorkPlan::background(WorkClass::Import, estimated_operations, hint))
    }

    pub fn external_content_artifact_job_background_work_plan_for_action(
        &self,
        action: &str,
        hint: BackgroundWorkHint,
        estimated_operations: usize,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .iter()
            .any(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && job.action == action
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .then(|| BackgroundWorkPlan::background(WorkClass::Import, estimated_operations, hint))
    }

    pub fn external_content_artifact_job_background_work_plan_for_runtime(
        &self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .iter()
            .any(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && external_content_runtime_can_claim(manifest, job)
            })
            .then(|| {
                BackgroundWorkPlan::background(
                    WorkClass::Import,
                    manifest.estimated_operations,
                    hint,
                )
            })
    }

    pub fn retry_failed_external_content_artifact_job(
        &mut self,
        job_id: u64,
    ) -> Option<DerivedArtifactJob> {
        let job = self
            .derived_artifact_jobs
            .iter_mut()
            .find(|job| job.id == job_id)?;
        if job.status != DerivedArtifactJobStatus::Failed
            || !is_external_content_artifact_job(&job.artifact_type)
        {
            return None;
        }

        job.status = DerivedArtifactJobStatus::Pending;
        job.last_error = None;
        job.last_output = None;
        Some(job.clone())
    }

    pub fn retry_failed_external_content_artifact_job_for_action(
        &mut self,
        action: &str,
        job_id: u64,
    ) -> Option<DerivedArtifactJob> {
        let job = self
            .derived_artifact_jobs
            .iter_mut()
            .find(|job| job.id == job_id)?;
        if job.status != DerivedArtifactJobStatus::Failed
            || job.action != action
            || !is_external_content_artifact_job(&job.artifact_type)
        {
            return None;
        }

        job.status = DerivedArtifactJobStatus::Pending;
        job.last_error = None;
        job.last_output = None;
        Some(job.clone())
    }

    pub fn run_next_derived_artifact_job(&mut self) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self
            .derived_artifact_jobs
            .iter()
            .position(|job| job.status == DerivedArtifactJobStatus::Pending)
        else {
            return Ok(None);
        };

        self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Running;
        self.derived_artifact_jobs[index].attempts += 1;
        self.derived_artifact_jobs[index].last_error = None;
        self.derived_artifact_jobs[index].last_output = None;

        let artifact_type = self.derived_artifact_jobs[index].artifact_type.clone();
        let name = self.derived_artifact_jobs[index].name.clone();
        let action = self.derived_artifact_jobs[index].action.clone();
        let result = self.execute_derived_artifact_job(&artifact_type, &name, &action);

        match result {
            Ok(output) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Succeeded;
                self.derived_artifact_jobs[index].last_output = Some(output.clone());
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output,
                }))
            }
            Err(error) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Failed;
                self.derived_artifact_jobs[index].last_error = Some(error.to_string());
                self.derived_artifact_jobs[index].last_output = None;
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output: QueryOutput {
                        rows: vec![derived_artifact_job_failure_row(
                            &self.derived_artifact_jobs[index],
                            &error.to_string(),
                        )]
                        .into(),
                    },
                }))
            }
        }
    }

    pub fn run_next_background_derived_artifact_job(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(job) = self
            .derived_artifact_jobs
            .iter()
            .find(|job| job.status == DerivedArtifactJobStatus::Pending)
        else {
            return Ok(None);
        };

        match policy.admit(state, &job.background_work_request(estimated_operations)) {
            QosAdmission::Admit => self.run_next_derived_artifact_job(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background derived artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background derived artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_derived_artifact_job(
        &mut self,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(job) = self
            .derived_artifact_jobs
            .iter()
            .find(|job| job.status == DerivedArtifactJobStatus::Pending)
        else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(job.background_work_request(estimated_operations)) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background derived artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background derived artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_next_derived_artifact_job();
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_next_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_at_index(index, &mut runtime)
    }

    pub fn run_next_background_external_content_artifact_job_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        match policy.admit(
            state,
            &self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            QosAdmission::Admit => {
                self.run_external_content_artifact_job_at_index(index, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(
            self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_external_content_artifact_job_at_index(index, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_next_external_content_artifact_job_for_action_with(
        &mut self,
        action: &str,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && job.action == action
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_at_index(index, &mut runtime)
    }

    pub fn run_next_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && external_content_runtime_can_claim(manifest, job)
        }) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_at_index(index, &mut runtime)
    }

    pub fn run_next_background_external_content_artifact_job_for_action_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        action: &str,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && job.action == action
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        match policy.admit(
            state,
            &self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            QosAdmission::Admit => {
                self.run_external_content_artifact_job_at_index(index, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_external_content_artifact_job_for_action_with(
        &mut self,
        action: &str,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && job.action == action
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(
            self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_external_content_artifact_job_at_index(index, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_next_background_external_content_artifact_job_for_runtime_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && external_content_runtime_can_claim(manifest, job)
        }) else {
            return Ok(None);
        };

        match policy.admit(
            state,
            &self.derived_artifact_jobs[index]
                .background_work_request(manifest.estimated_operations),
        ) {
            QosAdmission::Admit => {
                self.run_external_content_artifact_job_at_index(index, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && external_content_runtime_can_claim(manifest, job)
        }) else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(
            self.derived_artifact_jobs[index]
                .background_work_request(manifest.estimated_operations),
        ) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_external_content_artifact_job_at_index(index, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_external_content_artifact_job_with(
        &mut self,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.id == job_id
                && job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_at_index(index, &mut runtime)
    }

    pub fn complete_next_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_external_content_artifact_job_with(|job| {
            let completion = runtime(job)?;
            Ok(external_content_artifact_completion_output(job, completion))
        })
    }

    pub fn complete_external_content_artifact_job_with(
        &mut self,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_external_content_artifact_job_with(job_id, |job| {
            let completion = runtime(job)?;
            Ok(external_content_artifact_completion_output(job, completion))
        })
    }

    pub fn complete_next_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_external_content_artifact_job_for_runtime_with(manifest, |job| {
            let completion = runtime(job)?;
            Ok(external_content_artifact_completion_output(job, completion))
        })
    }

    pub fn complete_next_background_external_content_artifact_job_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_background_external_content_artifact_job_with(
            policy,
            state,
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
            estimated_operations,
        )
    }

    pub fn complete_next_background_external_content_artifact_job_for_runtime_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_background_external_content_artifact_job_for_runtime_with(
            policy,
            state,
            manifest,
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
        )
    }

    pub fn complete_next_scheduled_background_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_scheduled_background_external_content_artifact_job_for_runtime_with(
            manifest,
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
        )
    }

    pub fn complete_next_scheduled_background_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_next_scheduled_background_external_content_artifact_job_with(
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
            estimated_operations,
        )
    }

    pub fn complete_background_external_content_artifact_job_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_background_external_content_artifact_job_with(
            policy,
            state,
            job_id,
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
            estimated_operations,
        )
    }

    pub fn complete_scheduled_background_external_content_artifact_job_with(
        &mut self,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<ExternalContentArtifactJobCompletion>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.run_scheduled_background_external_content_artifact_job_with(
            job_id,
            |job| {
                let completion = runtime(job)?;
                Ok(external_content_artifact_completion_output(job, completion))
            },
            estimated_operations,
        )
    }

    pub fn run_background_external_content_artifact_job_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.id == job_id
                && job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        match policy.admit(
            state,
            &self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            QosAdmission::Admit => {
                self.run_external_content_artifact_job_at_index(index, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_scheduled_background_external_content_artifact_job_with(
        &mut self,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.id == job_id
                && job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(
            self.derived_artifact_jobs[index].background_work_request(estimated_operations),
        ) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_external_content_artifact_job_at_index(index, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    fn run_external_content_artifact_job_at_index(
        &mut self,
        index: usize,
        runtime: &mut impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Running;
        self.derived_artifact_jobs[index].attempts += 1;
        self.derived_artifact_jobs[index].last_error = None;
        self.derived_artifact_jobs[index].last_output = None;

        let runtime_job = self.derived_artifact_jobs[index].clone();
        match runtime(&runtime_job) {
            Ok(output) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Succeeded;
                self.derived_artifact_jobs[index].last_output = Some(output.clone());
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output,
                }))
            }
            Err(error) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Failed;
                self.derived_artifact_jobs[index].last_error = Some(error.to_string());
                self.derived_artifact_jobs[index].last_output = None;
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output: QueryOutput {
                        rows: vec![derived_artifact_job_failure_row(
                            &self.derived_artifact_jobs[index],
                            &error.to_string(),
                        )]
                        .into(),
                    },
                }))
            }
        }
    }

    pub fn rebuild_derived_artifacts(&mut self) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let before = self
            .store
            .projected_graph_statuses()
            .into_iter()
            .map(|status| (status.name.clone(), status))
            .collect::<BTreeMap<_, _>>();
        self.store
            .rebuild_projected_graph_artifacts(&self.catalog)?;
        let rows = self
            .store
            .projected_graph_statuses()
            .into_iter()
            .map(|status| {
                let before_reusable = before
                    .get(&status.name)
                    .map(|status| status.reusable)
                    .unwrap_or(false);
                BTreeMap::from([
                    (
                        "artifact_type".to_string(),
                        Value::String("projected_graph".to_string()),
                    ),
                    ("name".to_string(), Value::String(status.name)),
                    ("action".to_string(), Value::String("rebuilt".to_string())),
                    ("before_reusable".to_string(), Value::Bool(before_reusable)),
                    ("after_reusable".to_string(), Value::Bool(status.reusable)),
                    (
                        "projection_epoch".to_string(),
                        optional_u64_value(status.projection_epoch),
                    ),
                    (
                        "commit_epoch".to_string(),
                        optional_u64_value(status.commit_epoch),
                    ),
                    (
                        "node_count".to_string(),
                        optional_usize_value(status.node_count),
                    ),
                    (
                        "edge_count".to_string(),
                        optional_usize_value(status.edge_count),
                    ),
                ])
            })
            .collect();
        Ok(QueryOutput { rows })
    }

    fn enqueue_derived_artifact_job(
        &mut self,
        artifact_type: impl Into<String>,
        name: impl Into<String>,
        action: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job_with_payload(artifact_type, name, action, BTreeMap::new())
    }

    fn enqueue_derived_artifact_job_with_payload(
        &mut self,
        artifact_type: impl Into<String>,
        name: impl Into<String>,
        action: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> DerivedArtifactJob {
        let job = DerivedArtifactJob {
            id: self.next_derived_artifact_job_id,
            artifact_type: artifact_type.into(),
            name: name.into(),
            action: action.into(),
            payload,
            status: DerivedArtifactJobStatus::Pending,
            attempts: 0,
            last_error: None,
            last_output: None,
        };
        self.next_derived_artifact_job_id += 1;
        self.derived_artifact_jobs.push(job.clone());
        job
    }

    fn execute_derived_artifact_job(
        &mut self,
        artifact_type: &str,
        name: &str,
        action: &str,
    ) -> Result<QueryOutput> {
        if is_external_content_artifact_job(artifact_type) {
            return Err(SkeinError::Semantic(format!(
                "derived artifact job {artifact_type}.{name} action {action} is outside the graph kernel; run it in the content artifact job runtime"
            )));
        }

        if artifact_type != "projected_graph" || action != "rebuild" {
            return Err(SkeinError::Semantic(format!(
                "unsupported derived artifact job {artifact_type}.{name} action {action}"
            )));
        }

        if name != "*"
            && !self
                .store
                .projected_graph_statuses()
                .iter()
                .any(|status| status.name == name)
        {
            return Err(SkeinError::Semantic(format!(
                "unknown projected graph artifact '{name}'"
            )));
        }

        let mut output = self.rebuild_derived_artifacts()?;
        if name != "*" {
            output.rows.retain(|row| {
                row.get("name")
                    .is_some_and(|value| value == &Value::String(name.to_string()))
            });
        }
        Ok(output)
    }
}

fn derived_artifact_job_failure_row(job: &DerivedArtifactJob, error: &str) -> Row {
    BTreeMap::from([
        ("job_id".to_string(), Value::Int(job.id as i64)),
        (
            "artifact_type".to_string(),
            Value::String(job.artifact_type.clone()),
        ),
        ("name".to_string(), Value::String(job.name.clone())),
        ("action".to_string(), Value::String(job.action.clone())),
        ("payload".to_string(), Value::Map(job.payload.clone())),
        (
            "status".to_string(),
            Value::String(job.status.as_str().to_string()),
        ),
        ("attempts".to_string(), Value::Int(job.attempts as i64)),
        ("error".to_string(), Value::String(error.to_string())),
    ])
}

fn external_content_artifact_completion_output(
    job: &DerivedArtifactJob,
    completion: ExternalContentArtifactJobCompletion,
) -> QueryOutput {
    QueryOutput {
        rows: vec![external_content_artifact_completion_row(job, completion)].into(),
    }
}

fn external_content_artifact_completion_row(
    job: &DerivedArtifactJob,
    completion: ExternalContentArtifactJobCompletion,
) -> Row {
    BTreeMap::from([
        ("job_id".to_string(), Value::Int(job.id as i64)),
        (
            "artifact_type".to_string(),
            Value::String(job.artifact_type.clone()),
        ),
        ("name".to_string(), Value::String(job.name.clone())),
        ("action".to_string(), Value::String(job.action.clone())),
        (
            "runtime_name".to_string(),
            Value::String(completion.runtime_name),
        ),
        (
            "runtime_version".to_string(),
            optional_string_value(completion.runtime_version),
        ),
        (
            "input_ref".to_string(),
            optional_string_value(completion.input_ref),
        ),
        (
            "input_checksum".to_string(),
            optional_string_value(completion.input_checksum),
        ),
        (
            "output_ref".to_string(),
            optional_string_value(completion.output_ref),
        ),
        (
            "output_checksum".to_string(),
            optional_string_value(completion.output_checksum),
        ),
        (
            "projection_kind".to_string(),
            optional_string_value(completion.projection_kind),
        ),
        (
            "projection_ref".to_string(),
            optional_string_value(completion.projection_ref),
        ),
        (
            "source_graph_commit_epoch".to_string(),
            optional_u64_value(completion.source_graph_commit_epoch),
        ),
        (
            "rows_produced".to_string(),
            optional_usize_value(completion.rows_produced),
        ),
        ("metadata".to_string(), Value::Map(completion.metadata)),
    ])
}

fn optional_string_value(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

fn is_external_content_artifact_job(artifact_type: &str) -> bool {
    matches!(
        artifact_type,
        "content_artifact" | "artifact_parse" | "content_parse" | "blob_parse" | "crawler"
    )
}

fn external_content_runtime_can_claim(
    manifest: &ExternalContentArtifactRuntimeManifest,
    job: &DerivedArtifactJob,
) -> bool {
    is_external_content_artifact_job(&job.artifact_type)
        && manifest.supported_actions.contains(&job.action)
        && manifest
            .required_payload_keys
            .iter()
            .all(|key| job.payload.contains_key(key))
}

fn summarize_external_content_artifact_job(
    summary: &mut ExternalContentArtifactJobSummary,
    job: &DerivedArtifactJob,
) {
    summary.total += 1;
    match job.status {
        DerivedArtifactJobStatus::Pending => {
            summary.pending += 1;
            *summary
                .pending_by_action
                .entry(job.action.clone())
                .or_default() += 1;
            summary.next_pending_job_id.get_or_insert(job.id);
        }
        DerivedArtifactJobStatus::Running => {
            summary.running += 1;
        }
        DerivedArtifactJobStatus::Succeeded => {
            summary.succeeded += 1;
        }
        DerivedArtifactJobStatus::Failed => {
            summary.failed += 1;
            *summary
                .failed_by_action
                .entry(job.action.clone())
                .or_default() += 1;
            summary.oldest_failed_job_id.get_or_insert(job.id);
        }
    }
}
