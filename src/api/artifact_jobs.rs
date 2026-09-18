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

use super::{optional_u64_value, optional_usize_value, Database, QueryOutput};
use crate::error::{HawDBError, Result};
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosState, QosAdmission, WorkClass,
};
use crate::value::Value;
use std::collections::BTreeMap;

use hawdb_artifact::{
    external_content_artifact_completion_output, is_external_content_artifact_job,
    DerivedArtifactJobClaim,
};
pub use hawdb_artifact::{
    DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest,
};

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
        self.derived_artifact_jobs.jobs()
    }

    pub fn pending_external_content_artifact_jobs(&self, limit: usize) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs.pending_external(limit)
    }

    pub fn pending_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .pending_external_for_action(action, limit)
    }

    pub fn pending_external_content_artifact_jobs_for_runtime(
        &self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .pending_external_for_runtime(manifest, limit)
    }

    pub fn failed_external_content_artifact_jobs(&self, limit: usize) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs.failed_external(limit)
    }

    pub fn succeeded_external_content_artifact_jobs(
        &self,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs.succeeded_external(limit)
    }

    pub fn failed_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .failed_external_for_action(action, limit)
    }

    pub fn succeeded_external_content_artifact_jobs_for_action(
        &self,
        action: &str,
        limit: usize,
    ) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .succeeded_external_for_action(action, limit)
    }

    pub fn external_content_artifact_job_summary(&self) -> ExternalContentArtifactJobSummary {
        self.derived_artifact_jobs.external_summary(None)
    }

    pub fn external_content_artifact_job_summary_for_action(
        &self,
        action: &str,
    ) -> ExternalContentArtifactJobSummary {
        self.derived_artifact_jobs.external_summary(Some(action))
    }

    pub fn external_content_artifact_job_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
        estimated_operations: usize,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .has_pending_external()
            .then(|| BackgroundWorkPlan::background(WorkClass::Import, estimated_operations, hint))
    }

    pub fn external_content_artifact_job_background_work_plan_for_action(
        &self,
        action: &str,
        hint: BackgroundWorkHint,
        estimated_operations: usize,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .has_pending_external_for_action(action)
            .then(|| BackgroundWorkPlan::background(WorkClass::Import, estimated_operations, hint))
    }

    pub fn external_content_artifact_job_background_work_plan_for_runtime(
        &self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        self.derived_artifact_jobs
            .has_pending_external_for_runtime(manifest)
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
        self.derived_artifact_jobs
            .retry_failed_external(job_id, None)
    }

    pub fn retry_failed_external_content_artifact_job_for_action(
        &mut self,
        action: &str,
        job_id: u64,
    ) -> Option<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .retry_failed_external(job_id, Some(action))
    }

    pub fn run_next_derived_artifact_job(&mut self) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(claim) = self.derived_artifact_jobs.claim_pending() else {
            return Ok(None);
        };
        let job = claim.job().clone();
        let result = self.execute_derived_artifact_job(&job.artifact_type, &job.name, &job.action);
        Ok(Some(self.derived_artifact_jobs.complete(claim, result)))
    }

    pub fn run_next_background_derived_artifact_job(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(job) = self.derived_artifact_jobs.next_pending() else {
            return Ok(None);
        };

        match policy.admit(state, &job.background_work_request(estimated_operations)) {
            QosAdmission::Admit => self.run_next_derived_artifact_job(),
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background derived artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background derived artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_derived_artifact_job(
        &mut self,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(job) = self.derived_artifact_jobs.next_pending() else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(job.background_work_request(estimated_operations)) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background derived artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
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
        let Some(claim) = self.derived_artifact_jobs.claim_external() else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
    }

    pub fn run_next_background_external_content_artifact_job_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self.derived_artifact_jobs.next_external_claimable() else {
            return Ok(None);
        };

        match policy.admit(state, &job.background_work_request(estimated_operations)) {
            QosAdmission::Admit => {
                let claim = self
                    .derived_artifact_jobs
                    .claim_external()
                    .expect("admitted external artifact job must remain pending");
                self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self.derived_artifact_jobs.next_external_claimable() else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(job.background_work_request(estimated_operations)) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let claim = self
            .derived_artifact_jobs
            .claim_external()
            .expect("admitted external artifact job must remain pending");
        let result = self.run_external_content_artifact_job_with_claim(claim, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_next_external_content_artifact_job_for_action_with(
        &mut self,
        action: &str,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(claim) = self.derived_artifact_jobs.claim_external_for_action(action) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
    }

    pub fn run_next_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(claim) = self
            .derived_artifact_jobs
            .claim_external_for_runtime(manifest)
        else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
    }

    pub fn run_next_background_external_content_artifact_job_for_action_with(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        action: &str,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
        estimated_operations: usize,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_for_action_claimable(action)
        else {
            return Ok(None);
        };

        match policy.admit(state, &job.background_work_request(estimated_operations)) {
            QosAdmission::Admit => {
                let claim = self
                    .derived_artifact_jobs
                    .claim_external_for_action(action)
                    .expect("admitted external artifact job must remain pending");
                self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
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
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_for_action_claimable(action)
        else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(job.background_work_request(estimated_operations)) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let claim = self
            .derived_artifact_jobs
            .claim_external_for_action(action)
            .expect("admitted external artifact job must remain pending");
        let result = self.run_external_content_artifact_job_with_claim(claim, &mut runtime);
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
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_for_runtime_claimable(manifest)
        else {
            return Ok(None);
        };

        match policy.admit(
            state,
            &job.background_work_request(manifest.estimated_operations),
        ) {
            QosAdmission::Admit => {
                let claim = self
                    .derived_artifact_jobs
                    .claim_external_for_runtime(manifest)
                    .expect("admitted external artifact job must remain pending");
                self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job rejected: {reason}"
            ))),
        }
    }

    pub fn run_next_scheduled_background_external_content_artifact_job_for_runtime_with(
        &mut self,
        manifest: &ExternalContentArtifactRuntimeManifest,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_for_runtime_claimable(manifest)
        else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit =
            match scheduler.try_start(job.background_work_request(manifest.estimated_operations)) {
                Ok(permit) => permit,
                Err(QosAdmission::Defer { reason, .. }) => {
                    return Err(HawDBError::Storage(format!(
                        "background external content artifact job deferred: {reason}"
                    )));
                }
                Err(QosAdmission::Reject { reason, .. }) => {
                    return Err(HawDBError::Storage(format!(
                        "background external content artifact job rejected: {reason}"
                    )));
                }
                Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
            };

        let claim = self
            .derived_artifact_jobs
            .claim_external_for_runtime(manifest)
            .expect("admitted external artifact job must remain pending");
        let result = self.run_external_content_artifact_job_with_claim(claim, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn run_external_content_artifact_job_with(
        &mut self,
        job_id: u64,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(claim) = self.derived_artifact_jobs.claim_external_by_id(job_id) else {
            return Ok(None);
        };

        self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
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
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_by_id_claimable(job_id)
        else {
            return Ok(None);
        };

        match policy.admit(state, &job.background_work_request(estimated_operations)) {
            QosAdmission::Admit => {
                let claim = self
                    .derived_artifact_jobs
                    .claim_external_by_id(job_id)
                    .expect("admitted external artifact job must remain pending");
                self.run_external_content_artifact_job_with_claim(claim, &mut runtime)
            }
            QosAdmission::Defer { reason, .. } => Err(HawDBError::Storage(format!(
                "background external content artifact job deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(HawDBError::Storage(format!(
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
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        self.ensure_writable()?;
        let Some(job) = self
            .derived_artifact_jobs
            .next_external_by_id_claimable(job_id)
        else {
            return Ok(None);
        };

        let scheduler = self.local_qos_scheduler_for_work();
        let permit = match scheduler.try_start(job.background_work_request(estimated_operations)) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(HawDBError::Storage(format!(
                    "background external content artifact job rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let claim = self
            .derived_artifact_jobs
            .claim_external_by_id(job_id)
            .expect("admitted external artifact job must remain pending");
        let result = self.run_external_content_artifact_job_with_claim(claim, &mut runtime);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    fn run_external_content_artifact_job_with_claim(
        &mut self,
        claim: DerivedArtifactJobClaim,
        runtime: &mut impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        Ok(Some(
            self.derived_artifact_jobs.run_external_with(claim, runtime),
        ))
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
        self.derived_artifact_jobs
            .enqueue(artifact_type, name, action, payload)
    }

    fn execute_derived_artifact_job(
        &mut self,
        artifact_type: &str,
        name: &str,
        action: &str,
    ) -> Result<QueryOutput> {
        if is_external_content_artifact_job(artifact_type) {
            return Err(HawDBError::Semantic(format!(
                "derived artifact job {artifact_type}.{name} action {action} is outside the graph kernel; run it in the content artifact job runtime"
            )));
        }

        if artifact_type != "projected_graph" || action != "rebuild" {
            return Err(HawDBError::Semantic(format!(
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
            return Err(HawDBError::Semantic(format!(
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
