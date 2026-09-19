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
    SearchOutOfCoreGenerationWriter,
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
    permit: RuntimePermit,
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
        governor.try_admit(request).map(|permit| Self { permit })
    }

    /// Returns the exact request retained by this admission.
    pub fn request(&self) -> RuntimeWorkRequest {
        self.permit.request()
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
        let writer =
            SearchOutOfCoreGenerationWriter::create_with_context(root, options, task_context)?;
        Ok(GovernedSearchGenerationWriter {
            writer,
            admission: self,
        })
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
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            reader,
            delta,
            options,
            task_context,
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

    /// Finalizes the generation and releases its admission after all cleanup.
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

    /// Finalizes the update and releases its admission after all cleanup.
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
