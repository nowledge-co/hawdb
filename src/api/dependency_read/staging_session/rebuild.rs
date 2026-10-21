// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::search::{SearchOutOfCorePreparedGeneration, SearchOutOfCoreUnorderedReplacementWriter};

#[derive(Debug, Clone)]
pub struct ProjectionFullRebuildConfig {
    pub reads: PreparedReadLimits,
    pub feed: ProjectionFeedLimits,
    pub source: ProjectionFullSourceLimits,
    pub mapping: ProjectionBaselineMappingLimits,
    pub sorting: SearchUnorderedMutationLimits,
    pub build: SearchOutOfCoreGenerationBuildOptions,
    pub task: RuntimeTaskContext,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionFullRebuildStep {
    Input,
    Ordering,
    Complete,
}

/// Genuine complete replacement from one pinned C, including empty sources.
/// All source commits after C conflict at activation under the conservative
/// Database dependency. The session owns its source and copied target policies,
/// so it can leave the host owner's read closure and the old reader may close.
/// C..E composition, host binding/contract and independent
/// vector authority remain separate requirements before Mem adoption.
pub struct ProjectionFullRebuildSession {
    source: PreparedProjectionFullSource,
    stage: SearchOutOfCoreUnorderedReplacementWriter,
    mapper: ProjectionBaselineMappingFunction,
    reads: PreparedReadLimits,
    mapping: ProjectionBaselineMappingLimits,
    task: RuntimeTaskContext,
    usage: ProjectionMappingUsage,
    ended: bool,
    closed: bool,
    complete: bool,
    failed: bool,
    memory: QueryMemoryLease,
}

/// Complete privately built content plus its native source proof inputs. Only
/// the Database's fenced activation can consume this result. It is not a
/// versioned host-contract baseline, incremental-coverage or durable retry receipt.
///
/// ```compile_fail
/// fn publish(db: &mut hawdb::Database, deps: &hawdb::PreparedReadDependencies,
///            prepared: hawdb::ProjectionPreparedFullRebuild) {
///     db.publish_prepared_search(deps, prepared);
/// }
/// ```
pub struct ProjectionPreparedFullRebuild {
    dependencies: PreparedReadDependencies,
    candidate: SearchOutOfCorePreparedGeneration,
    usage: ProjectionMappingUsage,
    sorting: SearchUnorderedMutationReport,
    _memory: QueryMemoryLease,
}
impl ProjectionPreparedFullRebuild {
    pub fn source_epoch(&self) -> u64 {
        self.dependencies.source_epoch()
    }
    pub fn expected_active_generation(&self) -> Option<u64> {
        self.candidate.expected_active_generation()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.usage
    }
    pub fn sorting_report(&self) -> SearchUnorderedMutationReport {
        self.sorting
    }
}
impl Database {
    pub fn begin_projection_full_rebuild(
        &self,
        reader: &SearchOutOfCoreReader,
        config: ProjectionFullRebuildConfig,
        mapper: ProjectionBaselineMappingFunction,
        admission: crate::search::SearchGenerationAdmission,
    ) -> std::result::Result<ProjectionFullRebuildSession, ProjectionFeedError> {
        if config.build.source_graph_commit_epoch.is_some() {
            return Err(ProjectionFeedError::Source(refusal(
                "rebuild epoch belongs to the native source",
            )));
        }
        let mut stage = admission
            .prepare_unordered_replacement(reader, config.build, config.sorting, config.task)
            .map_err(ProjectionFeedError::Source)?;
        let task = stage.task_context().clone();
        let source = self.begin_prepared_full_projection_source(
            config.reads,
            config.feed,
            config.source,
            task.clone(),
        )?;
        stage
            .bind_source_epoch(source.source_epoch())
            .map_err(ProjectionFeedError::Source)?;
        let memory = stage
            .reserve_session_metadata(
                std::mem::size_of::<ProjectionFullRebuildSession>()
                    .max(std::mem::size_of::<ProjectionPreparedFullRebuild>()),
            )
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionFullRebuildSession {
            source,
            stage,
            mapper,
            reads: config.reads,
            mapping: config.mapping,
            task,
            usage: Default::default(),
            ended: false,
            closed: false,
            complete: false,
            failed: false,
            memory,
        })
    }
    /// Revalidates the canonical instance, whole-C source, durability and held
    /// Search base before manifest-last replacement. Failures consume and clean
    /// the candidate; a lost response still requires the future receipt protocol.
    pub fn publish_projection_full_rebuild(
        &mut self,
        prepared: ProjectionPreparedFullRebuild,
        durability: ProjectionSourceDurability,
    ) -> std::result::Result<
        crate::search::SearchOutOfCoreGenerationBuildReport,
        ProjectionPublicationError,
    > {
        self.publish_prepared_search_with_source_durability(
            &prepared.dependencies,
            prepared.candidate,
            durability,
        )
    }
}
impl ProjectionFullRebuildSession {
    pub fn source_epoch(&self) -> u64 {
        self.source.source_epoch()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.usage
    }
    pub fn feed_usage(&self) -> ProjectionFeedUsage {
        self.source.usage()
    }
    pub fn advance(
        &mut self,
        mut feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> std::result::Result<ProjectionFullRebuildStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let result = if self.complete {
            ProjectionFullRebuildStep::Complete
        } else {
            if self.ended && !self.closed {
                self.stage
                    .finish_input()
                    .map_err(ProjectionFeedError::Source)?;
                self.closed = true;
            }
            if self.closed || !self.stage.ready_for_input() {
                self.stage
                    .advance_ordering(ordering)
                    .map_err(ProjectionFeedError::Source)?;
                self.complete = self.stage.ordering_complete();
                if self.complete {
                    ProjectionFullRebuildStep::Complete
                } else {
                    ProjectionFullRebuildStep::Ordering
                }
            } else {
                feed.items = NonZeroUsize::MIN;
                let mapper = self.mapper;
                let mapping = self.mapping;
                let reads = self.reads;
                let usage = &mut self.usage;
                let stage = &mut self.stage;
                let ended = &mut self.ended;
                self.source.visit_next(feed, |read, item| {
                    if let Some(document) =
                        baseline::map_full_input(read, item, mapper, mapping, reads, usage)?
                    {
                        stage.queue_upsert(document)?;
                    }
                    if matches!(item, ProjectionFullSourceItem::End { .. }) {
                        *ended = true;
                    }
                    Ok(())
                })?;
                ProjectionFullRebuildStep::Input
            }
        };
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        self.failed = false;
        Ok(result)
    }
    /// Complete ordering is required; artifact building/sealing after it is
    /// blocking and does not inherit advance's logical-work quantum.
    pub fn finish(self) -> std::result::Result<ProjectionPreparedFullRebuild, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.ended || !self.complete {
            return Err(ProjectionFeedError::Incomplete);
        }
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let dependencies = self.source.finish()?;
        let (sorting, candidate) = self
            .stage
            .prepare_publication()
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionPreparedFullRebuild {
            dependencies,
            candidate,
            usage: self.usage,
            sorting,
            _memory: self.memory,
        })
    }
}
