// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::search::{
    SearchBaselineComparisonLimits, SearchBaselineComparisonReport, SearchOutOfCoreDeltaComparison,
};

/// Additional, cumulative admission for independent full-E verification.
/// Sorting shares the original staging session's cumulative admission.
#[derive(Debug, Clone, Copy)]
pub struct ProjectionCoverageConfig {
    pub feed: ProjectionFeedLimits,
    pub source: ProjectionFullSourceLimits,
    pub mapping: ProjectionBaselineMappingLimits,
    pub comparison: SearchBaselineComparisonLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionCoverageStep {
    Input,
    Ordering,
    Comparing,
    Complete,
}

/// Complete C..E intake and complete E enumeration stay under one source pin.
/// Comparison never turns a partial delta into a replacement: missing deletes,
/// conflicting content, and duplicate full-source IDs refuse the attempt.
pub struct ProjectionCoverageSession<'reader> {
    source: PreparedProjectionFullSource,
    comparison: SearchOutOfCoreDeltaComparison<'reader>,
    mapper: ProjectionBaselineMappingFunction,
    reads: PreparedReadLimits,
    limits: ProjectionBaselineMappingLimits,
    mapping: ProjectionMappingUsage,
    verification: ProjectionMappingUsage,
    ended: bool,
    closed: bool,
    complete: bool,
    failed: bool,
    _source_memory: QueryMemoryLease,
    memory: QueryMemoryLease,
}

/// Native complete-source proof bound to exactly one candidate, source and
/// expected Search selector. No candidate or dependency substitution API exists.
/// The host must still hold its binding/contract authority across activation;
/// this is not a registered Consumer ACK or an independent-vector proof.
///
/// ```compile_fail
/// fn substitute(db: &mut hawdb::Database, deps: &hawdb::PreparedReadDependencies,
///               covered: hawdb::ProjectionCoveredUpdate) {
///     db.publish_prepared_search(deps, covered);
/// }
/// ```
pub struct ProjectionCoveredUpdate {
    dependencies: PreparedReadDependencies,
    candidate: SearchOutOfCorePreparedUpdate,
    mapping: ProjectionMappingUsage,
    verification: ProjectionMappingUsage,
    comparison: SearchBaselineComparisonReport,
    sorting: SearchUnorderedMutationReport,
    _memory: QueryMemoryLease,
}
impl ProjectionCoveredUpdate {
    pub fn source_epoch(&self) -> u64 {
        self.dependencies.source_epoch()
    }
    pub fn expected_active_generation(&self) -> Option<u64> {
        self.candidate.expected_active_generation()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.mapping
    }
    pub fn verification_usage(&self) -> ProjectionMappingUsage {
        self.verification
    }
    pub fn comparison_report(&self) -> SearchBaselineComparisonReport {
        self.comparison
    }
    pub fn sorting_report(&self) -> SearchUnorderedMutationReport {
        self.sorting
    }
}

impl<'reader> ProjectionStagingSession<'reader> {
    /// Consume a successfully closed stage without releasing its pinned E.
    /// The fixed full-source mapper defines the expected projection contract;
    /// it must be deterministic and use only the supplied tracked source reads.
    pub fn verify_complete_source(
        self,
        config: ProjectionCoverageConfig,
        mapper: ProjectionBaselineMappingFunction,
    ) -> std::result::Result<ProjectionCoverageSession<'reader>, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.end || self.pending.is_some() || !self.closed || !self.stage.ordering_complete() {
            return Err(ProjectionFeedError::Incomplete);
        }
        let memory = self
            .stage
            .reserve_session_metadata(
                std::mem::size_of::<ProjectionCoverageSession<'reader>>()
                    .max(std::mem::size_of::<ProjectionCoveredUpdate>()),
            )
            .map_err(ProjectionFeedError::Source)?;
        let source_memory = self
            .stage
            .reserve_session_metadata(config.source.metadata_bytes.get())
            .map_err(ProjectionFeedError::Source)?;
        let source = self
            .source
            .into_full_source(config.feed, config.source, self.task)?;
        let comparison = self
            .stage
            .compare_complete_delta(config.comparison)
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionCoverageSession {
            source,
            comparison,
            mapper,
            reads: self.reads,
            limits: config.mapping,
            mapping: self.usage,
            verification: Default::default(),
            ended: false,
            closed: false,
            complete: false,
            failed: false,
            _source_memory: source_memory,
            memory,
        })
    }
}
impl ProjectionCoverageSession<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.source.source_epoch()
    }
    pub fn feed_usage(&self) -> ProjectionFeedUsage {
        self.source.usage()
    }
    pub fn verification_usage(&self) -> ProjectionMappingUsage {
        self.verification
    }
    pub fn advance(
        &mut self,
        mut feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> std::result::Result<ProjectionCoverageStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        self.comparison
            .task_context()
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let result = if self.complete {
            ProjectionCoverageStep::Complete
        } else if self.comparison.ordering_complete() {
            let step = self
                .comparison
                .advance(ordering)
                .map_err(ProjectionFeedError::Source)?;
            self.complete = step.complete;
            if step.complete {
                ProjectionCoverageStep::Complete
            } else {
                ProjectionCoverageStep::Comparing
            }
        } else if self.ended || !self.comparison.ready_for_input() {
            if self.ended && !self.closed {
                self.comparison
                    .finish_input()
                    .map_err(ProjectionFeedError::Source)?;
                self.closed = true;
            }
            self.comparison
                .advance_ordering(ordering)
                .map_err(ProjectionFeedError::Source)?;
            ProjectionCoverageStep::Ordering
        } else {
            feed.items = NonZeroUsize::new(1).unwrap();
            let comparison = &mut self.comparison;
            let usage = &mut self.verification;
            let ended = &mut self.ended;
            self.source.visit_next(feed, |read, item| {
                if let Some(document) = baseline::map_full_input(
                    read,
                    item,
                    self.mapper,
                    self.limits,
                    self.reads,
                    usage,
                )? {
                    comparison.queue_expected(document)?;
                }
                if matches!(item, ProjectionFullSourceItem::End { .. }) {
                    *ended = true;
                }
                Ok(())
            })?;
            ProjectionCoverageStep::Input
        };
        self.failed = false;
        Ok(result)
    }
    pub fn finish(self) -> std::result::Result<ProjectionCoveredUpdate, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.ended || !self.complete {
            return Err(ProjectionFeedError::Incomplete);
        }
        let dependencies = self.source.finish()?;
        let comparison = self.comparison.report();
        let (sorting, candidate) = self
            .comparison
            .finish()
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionCoveredUpdate {
            dependencies,
            candidate,
            mapping: self.mapping,
            verification: self.verification,
            comparison,
            sorting,
            _memory: self.memory,
        })
    }
}
impl Database {
    /// Revalidate source instance, E, dependencies, durability and the held base
    /// before publishing E. Full verification conservatively rejects all source
    /// commits after E. No intermediate feed position is a visible watermark.
    pub fn publish_projection_covered_update(
        &mut self,
        covered: ProjectionCoveredUpdate,
        durability: ProjectionSourceDurability,
    ) -> std::result::Result<
        (
            crate::search::SearchProjectionDeltaReport,
            crate::search::SearchOutOfCoreGenerationBuildReport,
            crate::search::SearchOutOfCoreMetrics,
        ),
        ProjectionPublicationError,
    > {
        self.publish_prepared_search_with_source_durability(
            &covered.dependencies,
            covered.candidate,
            durability,
        )
    }
}
