// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::search::{
    SearchBaselineComparisonLimits, SearchBaselineComparisonReport, SearchMatchedBaseline,
    SearchOutOfCoreBaselineComparison,
};

#[derive(Debug, Clone, Copy)]
pub struct ProjectionBaselineMappingLimits {
    pub calls: NonZeroUsize,
    pub queries: NonZeroUsize,
    pub reserved_rows: NonZeroUsize,
    pub reserved_payload_bytes: NonZeroUsize,
    pub queries_per_call: NonZeroUsize,
}
impl ProjectionBaselineMappingLimits {
    fn read_limits(self) -> ProjectionMappingLimits {
        ProjectionMappingLimits {
            calls: self.calls,
            queries: self.queries,
            reserved_rows: self.reserved_rows,
            reserved_payload_bytes: self.reserved_payload_bytes,
            queries_per_call: self.queries_per_call,
            input_bytes: NonZeroUsize::MIN,
            continuation_bytes: NonZeroUsize::MIN,
        }
    }
}
#[derive(Debug, Clone)]
pub struct ProjectionBaselineComparisonConfig {
    pub reads: PreparedReadLimits,
    pub feed: ProjectionFeedLimits,
    pub source: ProjectionFullSourceLimits,
    pub mapping: ProjectionBaselineMappingLimits,
    pub sorting: SearchUnorderedMutationLimits,
    pub build: SearchOutOfCoreGenerationBuildOptions,
    pub comparison: SearchBaselineComparisonLimits,
    pub task: RuntimeTaskContext,
}

/// Full enumeration maps each native node/key at most once to zero or one
/// document. Parent metadata does not fan out here: children are enumerated
/// independently. Table boundaries and End must return None. The fixed host
/// mapping and explicit table exclusions remain a trusted semantic contract.
pub type ProjectionBaselineMappingFunction = fn(
    &mut ProjectionBaselineMappingContext<'_, '_>,
    ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>>;

pub struct ProjectionBaselineMappingContext<'a, 'source> {
    read: verification::ProjectionOutputVerificationContext<'a, 'source>,
}
impl ProjectionBaselineMappingContext<'_, '_> {
    pub fn source_epoch(&self) -> u64 {
        self.read.source_epoch()
    }
    pub fn query_with_params(
        &mut self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
        self.read.query_with_params(text, parameters)
    }
    pub fn query_sql_with_params(
        &mut self,
        text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.read.query_sql_with_params(text, parameters)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionBaselineComparisonStep {
    Input,
    Ordering,
    Comparing,
    Complete,
}

/// Owns complete canonical enumeration, unique private expected output and exact
/// comparison to a held Search C head. The live source may advance during work.
/// Historical C comparison is not current-E activation authority.
pub struct ProjectionBaselineComparisonSession<'reader> {
    source: PreparedProjectionFullSource,
    stage: Option<SearchOutOfCoreUnorderedMutationWriter<'reader>>,
    comparison: Option<SearchOutOfCoreBaselineComparison<'reader>>,
    mapper: ProjectionBaselineMappingFunction,
    config: ProjectionBaselineComparisonConfig,
    usage: ProjectionMappingUsage,
    ended: bool,
    closed: bool,
    complete: bool,
    failed: bool,
    _memory: QueryMemoryLease,
}

/// Exact historical set/content match for the fixed mapper. No publication
/// trait, candidate conversion or durable baseline/contract receipt is exposed.
/// Independent vector authority, binding/incarnation and complete capture/
/// mapping contracts must still be qualified before coverage activation.
///
/// ```compile_fail
/// fn publish(db: &mut hawdb::Database, reads: &hawdb::PreparedReadDependencies,
///            matched: hawdb::ProjectionMatchedBaseline<'_>) {
///     db.publish_prepared_search(reads, matched);
/// }
/// ```
pub struct ProjectionMatchedBaseline<'reader> {
    source: PreparedReadDependencies,
    base: SearchMatchedBaseline<'reader>,
    usage: ProjectionMappingUsage,
    _mapper: ProjectionBaselineMappingFunction,
}
impl ProjectionMatchedBaseline<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.source.source_epoch()
    }
    pub fn generation(&self) -> u64 {
        self.base.generation()
    }
    pub fn report(&self) -> SearchBaselineComparisonReport {
        self.base.report()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.usage
    }
}
impl Database {
    pub fn begin_projection_baseline_comparison<'reader>(
        &self,
        reader: &'reader SearchOutOfCoreReader,
        mut config: ProjectionBaselineComparisonConfig,
        mapper: ProjectionBaselineMappingFunction,
        admission: crate::search::SearchGenerationAdmission,
    ) -> std::result::Result<ProjectionBaselineComparisonSession<'reader>, ProjectionFeedError>
    {
        if config.build.source_graph_commit_epoch.is_some() {
            return Err(ProjectionFeedError::Source(refusal(
                "baseline epoch belongs to the native source",
            )));
        }
        let mut stage = admission
            .prepare_unordered_update(
                reader,
                config.build.clone(),
                config.sorting,
                config.task.clone(),
            )
            .map_err(ProjectionFeedError::Source)?;
        stage
            .require_unique_upserts()
            .map_err(ProjectionFeedError::Source)?;
        config.task = stage.task_context().clone();
        let source = self.begin_prepared_full_projection_source(
            config.reads,
            config.feed,
            config.source,
            config.task.clone(),
        )?;
        if reader.source_graph_commit_epoch() != Some(source.source_epoch()) {
            return Err(ProjectionFeedError::Source(refusal("existing baseline comparison requires a matching pinned source epoch; unknown/lagged heads need rebuild")));
        }
        let memory = stage
            .reserve_session_metadata(std::mem::size_of::<
                ProjectionBaselineComparisonSession<'reader>,
            >())
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionBaselineComparisonSession {
            source,
            stage: Some(stage),
            comparison: None,
            mapper,
            config,
            usage: Default::default(),
            ended: false,
            closed: false,
            complete: false,
            failed: false,
            _memory: memory,
        })
    }
}
impl ProjectionBaselineComparisonSession<'_> {
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
    ) -> std::result::Result<ProjectionBaselineComparisonStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        self.config
            .task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let result = if self.complete {
            ProjectionBaselineComparisonStep::Complete
        } else if let Some(compare) = &mut self.comparison {
            let step = compare
                .advance(ordering)
                .map_err(ProjectionFeedError::Source)?;
            self.complete = step.complete;
            if step.complete {
                ProjectionBaselineComparisonStep::Complete
            } else {
                ProjectionBaselineComparisonStep::Comparing
            }
        } else {
            let stage = self
                .stage
                .as_mut()
                .expect("baseline stage before comparison");
            if self.ended && !self.closed {
                stage.finish_input().map_err(ProjectionFeedError::Source)?;
                self.closed = true;
            }
            if self.closed || !stage.ready_for_input() {
                stage
                    .advance_ordering(ordering)
                    .map_err(ProjectionFeedError::Source)?;
                if stage.ordering_complete() {
                    self.comparison = Some(
                        SearchOutOfCoreBaselineComparison::take_from(
                            &mut self.stage,
                            self.config.comparison,
                        )
                        .map_err(ProjectionFeedError::Source)?,
                    );
                }
                ProjectionBaselineComparisonStep::Ordering
            } else {
                feed.items = NonZeroUsize::new(1).unwrap();
                let usage = &mut self.usage;
                let mapper = self.mapper;
                let config = &self.config;
                let ended = &mut self.ended;
                self.source.visit_next(feed, |read, item| {
                    if let Some(document) =
                        map_full_input(read, item, mapper, config.mapping, config.reads, usage)?
                    {
                        stage.queue_upsert(document)?;
                    }
                    if matches!(item, ProjectionFullSourceItem::End { .. }) {
                        *ended = true;
                    }
                    Ok(())
                })?;
                ProjectionBaselineComparisonStep::Input
            }
        };
        self.config
            .task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        self.failed = false;
        Ok(result)
    }
}
impl<'reader> ProjectionBaselineComparisonSession<'reader> {
    pub fn finish(
        self,
    ) -> std::result::Result<ProjectionMatchedBaseline<'reader>, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.complete || !self.ended {
            return Err(ProjectionFeedError::Incomplete);
        }
        self.config
            .task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let source = self.source.finish()?;
        let base = self
            .comparison
            .ok_or(ProjectionFeedError::Incomplete)?
            .finish()
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionMatchedBaseline {
            source,
            base,
            usage: self.usage,
            _mapper: self.mapper,
        })
    }
}

// Shared complete-source mapping rules for comparison and genuine replacement.
pub(super) fn map_full_input(
    read: &ProjectionFeedRead<'_>,
    item: ProjectionFullSourceItem<'_>,
    mapper: ProjectionBaselineMappingFunction,
    limits: ProjectionBaselineMappingLimits,
    reads: PreparedReadLimits,
    usage: &mut ProjectionMappingUsage,
) -> Result<Option<SearchDocument>> {
    increment(&mut usage.calls, 1, limits.calls.get())?;
    let mut context = ProjectionBaselineMappingContext {
        read: verification::ProjectionOutputVerificationContext::new(
            read,
            limits.read_limits(),
            reads,
            usage,
        ),
    };
    let document = mapper(&mut context, item)?;
    if context.read.is_failed() {
        return Err(refusal("full-source mapper ignored a failed read"));
    }
    if document.is_some()
        && matches!(
            item,
            ProjectionFullSourceItem::RelationalTable { .. } | ProjectionFullSourceItem::End { .. }
        )
    {
        return Err(refusal(
            "table boundary or End cannot own a baseline document",
        ));
    }
    Ok(document)
}
