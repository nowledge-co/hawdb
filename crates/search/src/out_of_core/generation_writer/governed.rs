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

//! Governor-owned lifetimes for complete search-generation operations.

use super::{
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationUpdate,
    SearchOutOfCoreGenerationWriter, SearchOutOfCorePreparedGeneration,
    SearchOutOfCorePreparedUpdate,
};
use crate::{
    Result, SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreMetrics, SearchOutOfCoreReader,
    SearchProjectionDelta, SearchProjectionDeltaReport,
};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{RuntimeAdmissionError, RuntimeGovernor, RuntimePermit, RuntimeWorkRequest};
use std::path::Path;

/// A shared-runtime admission held for one complete search-generation operation.
///
/// The supplied request must reserve the operation's complete working set,
/// including the input document floor, decoded records, retained build state,
/// and any analyzer workspace. Build options remain component limits; they do
/// not add up to a whole-operation memory estimate. The host selects the
/// request from its current policy and may share a process-memory policy among
/// governors.
#[derive(Debug)]
pub struct SearchGenerationAdmission {
    permit: std::sync::Arc<RuntimePermit>,
}

impl SearchGenerationAdmission {
    /// Acquires one governor permit before creating a writer or update.
    ///
    /// The returned admission keeps the permit until its governed operation
    /// finishes or is dropped. A failed writer/update setup releases it.
    pub fn acquire(
        governor: &RuntimeGovernor,
        request: RuntimeWorkRequest,
    ) -> std::result::Result<Self, RuntimeAdmissionError> {
        governor.try_admit(request).map(|permit| Self {
            permit: std::sync::Arc::new(permit),
        })
    }

    /// Returns the exact request retained by this admission.
    pub fn request(&self) -> RuntimeWorkRequest {
        self.permit.request()
    }

    /// Verifies and stages one selected body while retaining shared admission
    /// through the consumer's complete transfer and scratch-file closure.
    pub fn open_verified_body<'reader>(
        self,
        reader: &'reader SearchOutOfCoreReader,
        candidate: &crate::SearchOutOfCoreCandidate,
        options: crate::SearchBodyReadOptions,
        task: RuntimeTaskContext,
    ) -> Result<crate::SearchVerifiedBody<'reader>> {
        let task = self.permit.bind_task_context(task);
        let mut body = reader.open_verified_body(candidate, options, task)?;
        body._admission = Some(self);
        Ok(body)
    }

    /// Creates a governed writer with a default task context.
    pub fn create_writer(
        self,
        root: impl AsRef<Path>,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<GovernedSearchGenerationWriter> {
        self.create_writer_with_context(root, options, RuntimeTaskContext::default())
    }

    /// Creates a governed writer while retaining caller cancellation and deadline state.
    pub fn create_writer_with_context(
        self,
        root: impl AsRef<Path>,
        options: SearchOutOfCoreGenerationBuildOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<GovernedSearchGenerationWriter> {
        let task_context = self.permit.bind_task_context(task_context);
        let mut memory = crate::build_memory::BuildMemory::new(&task_context)?;
        memory.host_admission = Some(self.permit.clone());
        let options = super::context_memory::Options::new(options, &memory, &task_context)?;
        let writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            root,
            options,
            task_context,
            memory,
        )?;
        Ok(GovernedSearchGenerationWriter {
            writer,
            admission: self,
        })
    }

    /// Starts an ordered streamed batch under this admission, including cleanup.
    pub fn prepare_streamed_update<'reader>(
        self,
        reader: &'reader SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        task: RuntimeTaskContext,
    ) -> Result<super::SearchOutOfCoreMutationWriter<'reader>> {
        let task = self.permit.bind_task_context(task);
        let mut memory = crate::build_memory::BuildMemory::new(&task)?;
        memory.host_admission = Some(self.permit.clone());
        super::mutations::SearchOutOfCoreMutationWriter::create(reader, options, task, memory)
    }

    /// Owns unordered intake, sorting and its prepared artifact under one permit.
    pub fn prepare_unordered_update<'reader>(
        self,
        reader: &'reader SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        limits: super::SearchUnorderedMutationLimits,
        task: RuntimeTaskContext,
    ) -> Result<super::SearchOutOfCoreUnorderedMutationWriter<'reader>> {
        let task = self.permit.bind_task_context(task);
        let mut memory = crate::build_memory::BuildMemory::new(&task)?;
        memory.host_admission = Some(self.permit.clone());
        let inner =
            super::mutations::SearchOutOfCoreMutationWriter::create(reader, options, task, memory)?;
        super::unordered::from_inner(
            inner,
            limits,
            reader.config().max_document_header_bytes.get(),
        )
    }

    /// Complete replacement: no old targets or mutation closure are retained.
    pub fn prepare_unordered_replacement(
        self,
        reader: &SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        limits: super::SearchUnorderedMutationLimits,
        task: RuntimeTaskContext,
    ) -> Result<super::SearchOutOfCoreUnorderedReplacementWriter> {
        let source_epoch = options.source_graph_commit_epoch;
        let task = self.permit.bind_task_context(task);
        let mut memory = crate::build_memory::BuildMemory::new(&task)?;
        memory.host_admission = Some(self.permit.clone());
        let inner = super::mutations::SearchOutOfCoreMutationWriter::create_replacement(
            reader, options, task, memory,
        )?;
        super::unordered::replacement::from_inner(
            inner,
            limits,
            source_epoch,
            reader.config().max_document_header_bytes.get(),
        )
    }

    /// Artifact-only owner/phase overlay followed by strict document ordering.
    /// The facade must separately certify source owners, intervals and mapping.
    pub fn prepare_owner_replacement(
        self,
        reader: &SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        limits: super::SearchOwnerOverlayLimits,
        task: RuntimeTaskContext,
    ) -> Result<super::SearchOutOfCoreOwnerReplacementWriter> {
        if options.source_graph_commit_epoch.is_some() {
            return Err(crate::HawDBError::Execution(
                "owner replacement binds its epoch when input closes".into(),
            ));
        }
        let task = self.permit.bind_task_context(task);
        let mut memory = crate::build_memory::BuildMemory::new(&task)?;
        memory.host_admission = Some(self.permit.clone());
        let inner = super::mutations::SearchOutOfCoreMutationWriter::create_replacement(
            reader, options, task, memory,
        )?;
        super::unordered::owner::from_inner(
            inner,
            limits,
            reader.config().max_document_header_bytes.get(),
        )
    }

    /// Prepares a governed update with a default task context.
    pub fn prepare_update(
        self,
        reader: &SearchOutOfCoreReader,
        delta: SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<GovernedSearchGenerationUpdate> {
        self.prepare_update_with_context(reader, delta, options, RuntimeTaskContext::default())
    }

    /// Prepares a governed update while retaining caller cancellation and deadline state.
    pub fn prepare_update_with_context(
        self,
        reader: &SearchOutOfCoreReader,
        delta: SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<GovernedSearchGenerationUpdate> {
        let task_context = self.permit.bind_task_context(task_context);
        let mut memory = crate::build_memory::BuildMemory::new(&task_context)?;
        memory.host_admission = Some(self.permit.clone());
        let update = SearchOutOfCoreGenerationUpdate::prepare_with_memory(
            reader,
            delta,
            options,
            task_context,
            memory,
        )?;
        Ok(GovernedSearchGenerationUpdate {
            update,
            admission: self,
        })
    }
}

/// A generation writer that retains its shared-runtime admission for its lifetime.
pub struct GovernedSearchGenerationWriter {
    // Drop the writer before the permit so cleanup remains within the admission.
    writer: SearchOutOfCoreGenerationWriter,
    admission: SearchGenerationAdmission,
}

impl GovernedSearchGenerationWriter {
    /// Returns the underlying writer while retaining the admission.
    pub fn writer(&self) -> &SearchOutOfCoreGenerationWriter {
        &self.writer
    }

    /// Returns the underlying writer while retaining the admission.
    pub fn writer_mut(&mut self) -> &mut SearchOutOfCoreGenerationWriter {
        &mut self.writer
    }

    /// Builds an invisible candidate that retains this governor permit through
    /// its owned build memory and stage. Queuing, activation and drop cleanup
    /// remain admitted even after this wrapper has been consumed.
    pub fn prepare_publication(self) -> Result<SearchOutOfCorePreparedGeneration> {
        let Self { writer, admission } = self;
        let candidate = writer.prepare_publication();
        drop(admission);
        candidate
    }

    /// Finalizes the generation and releases admission after immediate cleanup.
    /// Deferred debt keeps only its separately accounted metadata and disk limits.
    pub fn finish(self) -> Result<SearchOutOfCoreGenerationBuildReport> {
        let Self { writer, admission } = self;
        let result = writer.finish();
        drop(admission);
        result
    }
}

/// A generation update that retains its shared-runtime admission for its lifetime.
pub struct GovernedSearchGenerationUpdate {
    // Drop the update before the permit so failed publication cleanup is governed.
    update: SearchOutOfCoreGenerationUpdate,
    admission: SearchGenerationAdmission,
}

impl GovernedSearchGenerationUpdate {
    /// Returns the underlying prepared update while retaining the admission.
    pub fn update(&self) -> &SearchOutOfCoreGenerationUpdate {
        &self.update
    }

    /// Returns the underlying prepared update while retaining the admission.
    pub fn update_mut(&mut self) -> &mut SearchOutOfCoreGenerationUpdate {
        &mut self.update
    }

    /// Builds an invisible update retaining the shared admission until its
    /// candidate and immediate cleanup have completed.
    pub fn prepare_publication(self) -> Result<SearchOutOfCorePreparedUpdate> {
        let Self { update, admission } = self;
        let candidate = update.prepare_publication();
        drop(admission);
        candidate
    }

    /// Finalizes the update and releases admission after immediate cleanup.
    /// Deferred debt keeps only its separately accounted metadata and disk limits.
    pub fn finish(
        self,
    ) -> Result<(
        SearchProjectionDeltaReport,
        SearchOutOfCoreGenerationBuildReport,
        SearchOutOfCoreMetrics,
    )> {
        let Self { update, admission } = self;
        let result = update.finish();
        drop(admission);
        result
    }
}

#[cfg(feature = "vector-search")]
mod reader;
