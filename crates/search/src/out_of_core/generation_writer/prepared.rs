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

//! Owned invisible content preparation, followed by selector-fenced activation.

use super::*;

enum Content {
    Single {
        artifacts: GenerationArtifacts,
        private_partition: bool,
    },
    Partitioned(SearchOutOfCoreGenerationBuildReport),
}

/// An invisible Search candidate tied to the selector observed at preparation.
///
/// Owns private stages, admission leases and cleanup until consumed or dropped.
/// It holds no target publication lock while queued. This token validates the
/// Search base only; it does not authorize a Graph source or host target binding.
/// Those checks require the embedded owner's validation-to-publication fence.
#[must_use = "publish the candidate or drop it to release its private resources"]
pub struct SearchOutOfCorePreparedGeneration {
    // Content drops before the writer's stage and resource leases.
    content: Content,
    base: CapturedBase,
    generation: u64,
    writer: SearchOutOfCoreGenerationWriter,
}

impl std::fmt::Debug for SearchOutOfCorePreparedGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SearchOutOfCorePreparedGeneration")
            .field("expected_active_generation", &self.base.active)
            .field("generation", &self.generation)
            .field(
                "source_graph_commit_epoch",
                &self.source_graph_commit_epoch(),
            )
            .finish_non_exhaustive()
    }
}

impl SearchOutOfCorePreparedGeneration {
    pub(super) fn single(
        writer: SearchOutOfCoreGenerationWriter,
        base: CapturedBase,
        generation: u64,
        private_partition: bool,
        artifacts: GenerationArtifacts,
    ) -> Self {
        Self {
            content: Content::Single {
                artifacts,
                private_partition,
            },
            base,
            generation,
            writer,
        }
    }

    pub(super) fn partitioned(
        writer: SearchOutOfCoreGenerationWriter,
        base: CapturedBase,
        generation: u64,
        report: SearchOutOfCoreGenerationBuildReport,
    ) -> Self {
        Self {
            content: Content::Partitioned(report),
            base,
            generation,
            writer,
        }
    }

    pub fn expected_active_generation(&self) -> Option<u64> {
        self.base.active
    }

    pub fn source_graph_commit_epoch(&self) -> Option<u64> {
        self.writer.options.source_graph_commit_epoch
    }

    pub(super) fn source_watermark_floor(&self) -> Result<Option<u64>> {
        if self.base.damaged.is_some() {
            return Err(HawDBError::StorageIntegrity(
                "source-validated Search activation requires a valid base selector".into(),
            ));
        }
        Ok(self.base.source_watermark)
    }

    /// Rechecks the captured selector under publication exclusion before any
    /// real-root artifact installation, then publishes the complete closure.
    /// An advanced base returns a typed conflict; damaged/missing/regressing
    /// bases fail integrity checks. Rejection releases the private stage.
    ///
    /// Publication still includes artifact verification, installation and
    /// synchronization; this lifecycle split alone does not bound lock-held I/O
    /// independently of artifact size. A successful selector commit cannot be
    /// undone by cancellation arriving during that commit's durability barrier.
    pub fn publish(self) -> Result<SearchOutOfCoreGenerationBuildReport> {
        let _lease = super::super::SearchProjectionPublishLease::acquire_with_context(
            &self.writer.root,
            &self.writer.memory,
            &self.writer.task_context,
        )?;
        self.base.validate(&self.writer)?;
        match self.content {
            Content::Single {
                artifacts,
                private_partition,
            } => self
                .writer
                .publish_artifacts(self.generation, private_partition, artifacts),
            Content::Partitioned(report) => initial::publish(self.writer, self.base.active, report),
        }
    }
}

pub(super) struct CapturedBase {
    pub(super) active: Option<u64>,
    damaged: Option<io::AdmittedBytes>,
    source_watermark: Option<u64>,
}

impl CapturedBase {
    pub(super) fn capture(writer: &SearchOutOfCoreGenerationWriter) -> Result<Self> {
        match discovery::active_identity(&writer.root, &writer.memory, &writer.task_context) {
            Ok(identity) => {
                let active = identity.as_ref().map(|identity| identity.generation);
                let source_watermark =
                    identity.and_then(|identity| identity.source_graph_commit_epoch);
                if let Some(expected) = writer.expected_active_generation {
                    check_active(Some(expected), active)?;
                }
                Ok(Self {
                    active,
                    damaged: None,
                    source_watermark,
                })
            }
            Err(error)
                if writer.expected_active_generation.is_some()
                    || matches!(error, HawDBError::Execution(_)) =>
            {
                Err(error)
            }
            Err(_) => {
                // Standalone full replacement already supports rebuilding a
                // corrupt selector using validated lexical generation discovery.
                // Retain its exact bounded bytes, not a wildcard 'corrupt' base.
                let io = io::GenerationIo::new(&writer.memory, &writer.task_context);
                let path = io.path(
                    &writer.root,
                    Path::new(super::super::OUT_OF_CORE_MANIFEST_FILE),
                )?;
                let damaged = io.read(&path, super::super::MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
                Ok(Self {
                    active: None,
                    damaged: Some(damaged),
                    source_watermark: None,
                })
            }
        }
    }

    fn validate(&self, writer: &SearchOutOfCoreGenerationWriter) -> Result<()> {
        if let Some(damaged) = &self.damaged {
            let io = io::GenerationIo::new(&writer.memory, &writer.task_context);
            let path = io.path(
                &writer.root,
                Path::new(super::super::OUT_OF_CORE_MANIFEST_FILE),
            )?;
            let current = io.read(&path, super::super::MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
            if current.bytes != damaged.bytes {
                if let Ok(Some(actual)) =
                    discovery::active(&writer.root, &writer.memory, &writer.task_context)
                {
                    return check_active(None, Some(actual));
                }
                return Err(HawDBError::StorageIntegrity(
                    "damaged search selector changed during replacement preparation".into(),
                ));
            }
            return Ok(());
        }
        check_active(
            self.active,
            discovery::active(&writer.root, &writer.memory, &writer.task_context)?,
        )
    }
}

pub(super) fn check_active(expected: Option<u64>, actual: Option<u64>) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    match (expected, actual) {
        (Some(expected), Some(actual)) => Err(generation_changed(expected, actual)),
        (None, Some(actual)) => Err(HawDBError::TransactionConflict {
            read_epoch: 0,
            committed_epoch: actual,
            key: "search projection base changed".into(),
        }),
        (Some(_), None) => Err(HawDBError::StorageIntegrity(
            "published search generation disappeared".into(),
        )),
        (None, None) => unreachable!("equal selectors already returned"),
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Closed Search activation operations accepted by the embedded source fence.
/// Hosts use `Database::publish_prepared_search`; this trait cannot be implemented
/// by arbitrary callbacks that mutate state after source validation.
#[doc(hidden)]
pub trait PreparedSearchPublication: sealed::Sealed {
    type Report;
    fn source_graph_commit_epoch(&self) -> Option<u64>;
    fn source_watermark_floor(&self) -> Result<Option<u64>>;
    fn publish(self) -> Result<Self::Report>;
}

impl sealed::Sealed for SearchOutOfCorePreparedGeneration {}
impl PreparedSearchPublication for SearchOutOfCorePreparedGeneration {
    type Report = SearchOutOfCoreGenerationBuildReport;
    fn source_graph_commit_epoch(&self) -> Option<u64> {
        self.source_graph_commit_epoch()
    }
    fn source_watermark_floor(&self) -> Result<Option<u64>> {
        self.source_watermark_floor()
    }
    fn publish(self) -> Result<Self::Report> {
        self.publish()
    }
}

impl sealed::Sealed for super::SearchOutOfCorePreparedUpdate {}
impl PreparedSearchPublication for super::SearchOutOfCorePreparedUpdate {
    type Report = (
        crate::SearchProjectionDeltaReport,
        SearchOutOfCoreGenerationBuildReport,
        crate::SearchOutOfCoreMetrics,
    );
    fn source_graph_commit_epoch(&self) -> Option<u64> {
        self.source_graph_commit_epoch()
    }
    fn source_watermark_floor(&self) -> Result<Option<u64>> {
        self.source_watermark_floor()
    }
    fn publish(self) -> Result<Self::Report> {
        self.publish()
    }
}
