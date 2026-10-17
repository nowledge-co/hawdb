// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Joint source/mapping/stage custody. This does not certify a baseline or permit activation.
use super::*;
use crate::search::{
    SearchDocument, SearchMutationSortStepLimits, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCorePreparedUpdate, SearchOutOfCoreReader, SearchOutOfCoreUnorderedMutationWriter,
    SearchUnorderedMutationLimits, SearchUnorderedMutationReport,
};
#[path = "staging_session/baseline.rs"]
mod baseline;
pub use baseline::{
    ProjectionBaselineComparisonConfig, ProjectionBaselineComparisonSession,
    ProjectionBaselineComparisonStep, ProjectionBaselineMappingContext,
    ProjectionBaselineMappingFunction, ProjectionBaselineMappingLimits, ProjectionMatchedBaseline,
};
#[path = "staging_session/rebuild.rs"]
mod rebuild;
pub use rebuild::{
    ProjectionFullRebuildConfig, ProjectionFullRebuildSession, ProjectionFullRebuildStep,
    ProjectionPreparedFullRebuild,
};
#[path = "staging_session/verification.rs"]
mod verification;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use hawdb_storage::relational::RelationalKey;
pub use verification::{
    ProjectionOutputVerificationContext, ProjectionOutputVerificationFunction,
    ProjectionOutputVerificationSession,
};

/// Invocation/output reservations, not a bound on physical query work or RSS.
#[derive(Debug, Clone, Copy)]
pub struct ProjectionMappingLimits {
    pub calls: NonZeroUsize,
    pub queries: NonZeroUsize,
    pub reserved_rows: NonZeroUsize,
    pub reserved_payload_bytes: NonZeroUsize,
    pub queries_per_call: NonZeroUsize,
    pub input_bytes: NonZeroUsize,
    pub continuation_bytes: NonZeroUsize,
}

#[derive(Debug, Clone)]
pub struct ProjectionStagingSessionConfig {
    pub after_epoch: u64,
    pub reads: PreparedReadLimits,
    pub feed: ProjectionFeedLimits,
    pub mapping: ProjectionMappingLimits,
    pub build: SearchOutOfCoreGenerationBuildOptions,
    pub sorting: SearchUnorderedMutationLimits,
    pub task: RuntimeTaskContext,
}

/// Every input remains owned by the session until the mapper acknowledges Done.
#[derive(Debug, Clone, Copy)]
pub enum ProjectionMappingInput<'a> {
    GraphNode {
        epoch: u64,
        node_id: u64,
    },
    DeleteDocument {
        epoch: u64,
        document_id: &'a str,
    },
    RelationalKey {
        epoch: u64,
        table: &'a str,
        key: &'a RelationalKey,
    },
    End {
        source_epoch: u64,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionMappingStatus {
    More,
    Done,
}
/// A fixed mapping function with session-owned, admitted continuation bytes.
/// Mapping/business correctness is a trusted host premise; acknowledgments do
/// not confer output coverage, external-version or binding authority.
pub type ProjectionMappingFunction = fn(
    &mut ProjectionMappingContext<'_, '_, '_>,
    ProjectionMappingInput<'_>,
    &[u8],
) -> Result<ProjectionMappingStatus>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionMappingUsage {
    pub calls: usize,
    pub queries: usize,
    pub reserved_rows: usize,
    pub reserved_payload_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionStagingSessionStep {
    Input,
    Mapping,
    Ordering,
    Complete,
}

struct Continuation {
    bytes: Box<[u8]>,
    _memory: QueryMemoryLease,
}

/// No raw prepared read or stage escape. At most one output and one continuation
/// replacement per mapper invocation; ignored failures poison that invocation.
pub struct ProjectionMappingContext<'a, 'source, 'reader> {
    read: &'a ProjectionFeedRead<'source>,
    stage: &'a mut SearchOutOfCoreUnorderedMutationWriter<'reader>,
    limits: ProjectionMappingLimits,
    reads: PreparedReadLimits,
    usage: &'a mut ProjectionMappingUsage,
    queries: usize,
    emitted: bool,
    failed: bool,
    next: Option<Continuation>,
}
fn refusal(message: &str) -> HawDBError {
    HawDBError::Execution(format!("projection staging session: {message}"))
}
fn increment(value: &mut usize, amount: usize, cap: usize) -> Result<()> {
    *value = value
        .checked_add(amount)
        .filter(|n| *n <= cap)
        .ok_or_else(|| refusal("cumulative mapping admission exhausted"))?;
    Ok(())
}
fn reserve_query(
    limits: ProjectionMappingLimits,
    reads: PreparedReadLimits,
    usage: &mut ProjectionMappingUsage,
    queries: &mut usize,
) -> Result<()> {
    if *queries == limits.queries_per_call.get() {
        return Err(refusal("query quantum exhausted"));
    }
    increment(&mut usage.queries, 1, limits.queries.get())?;
    increment(
        &mut usage.reserved_rows,
        reads.statement_rows.get(),
        limits.reserved_rows.get(),
    )?;
    increment(
        &mut usage.reserved_payload_bytes,
        reads.statement_payload_bytes.get(),
        limits.reserved_payload_bytes.get(),
    )?;
    *queries += 1;
    Ok(())
}
impl ProjectionMappingContext<'_, '_, '_> {
    fn query(&mut self) -> Result<()> {
        self.failed = true;
        reserve_query(self.limits, self.reads, self.usage, &mut self.queries)
    }
    pub fn source_epoch(&self) -> u64 {
        self.read.source_epoch()
    }
    pub fn query_with_params(
        &mut self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
        if self.failed {
            return Err(refusal("mapper ignored an earlier failure"));
        }
        self.query()?;
        let result = self.read.query_with_params(text, parameters);
        self.failed = result.is_err();
        result
    }
    pub fn query_sql_with_params(
        &mut self,
        text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        if self.failed {
            return Err(refusal("mapper ignored an earlier failure"));
        }
        self.query()?;
        let result = self.read.query_sql_with_params(text, parameters);
        self.failed = result.is_err();
        result
    }
    fn output(&mut self) -> Result<()> {
        if self.failed || self.emitted {
            self.failed = true;
            return Err(refusal("mapper output quantum exhausted"));
        }
        self.failed = true;
        self.emitted = true;
        Ok(())
    }
    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        self.output()?;
        self.stage.queue_upsert(document)?;
        self.failed = false;
        Ok(())
    }
    pub fn delete(&mut self, id: &str) -> Result<()> {
        self.output()?;
        self.stage.queue_delete(id)?;
        self.failed = false;
        Ok(())
    }
    pub fn set_continuation(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed || self.next.is_some() || bytes.len() > self.limits.continuation_bytes.get()
        {
            self.failed = true;
            return Err(refusal("mapper continuation admission exhausted"));
        }
        self.failed = true;
        let memory = self.stage.reserve_session_metadata(bytes.len())?;
        self.next = Some(Continuation {
            bytes: bytes.into(),
            _memory: memory,
        });
        self.failed = false;
        Ok(())
    }
}

#[derive(Debug)]
enum OwnedInput {
    GraphNode(u64, u64),
    DeleteDocument(u64, String),
    Key(u64, String, RelationalKey),
    End(u64),
}
impl OwnedInput {
    fn borrow(&self) -> ProjectionMappingInput<'_> {
        match self {
            Self::GraphNode(epoch, node_id) => ProjectionMappingInput::GraphNode {
                epoch: *epoch,
                node_id: *node_id,
            },
            Self::DeleteDocument(epoch, document_id) => ProjectionMappingInput::DeleteDocument {
                epoch: *epoch,
                document_id,
            },
            Self::Key(epoch, table, key) => ProjectionMappingInput::RelationalKey {
                epoch: *epoch,
                table,
                key,
            },
            Self::End(source_epoch) => ProjectionMappingInput::End {
                source_epoch: *source_epoch,
            },
        }
    }
    fn capture(
        input: ProjectionMappingInput<'_>,
        stage: &SearchOutOfCoreUnorderedMutationWriter<'_>,
        cap: usize,
    ) -> Result<(Self, QueryMemoryLease)> {
        let mut bytes = std::mem::size_of::<Self>();
        match input {
            ProjectionMappingInput::DeleteDocument { document_id, .. } => {
                bytes = bytes.saturating_add(document_id.len())
            }
            ProjectionMappingInput::RelationalKey { table, key, .. } => {
                if key.0.len() > 32 {
                    return Err(refusal("input key component admission exhausted"));
                }
                bytes = bytes.saturating_add(table.len());
                for field in &key.0 {
                    bytes = bytes
                        .saturating_add(std::mem::size_of_val(field))
                        .saturating_add(field.estimated_payload_bytes());
                }
            }
            _ => {}
        }
        if bytes > cap {
            return Err(refusal("pending input byte admission exhausted"));
        }
        let memory = stage.reserve_session_metadata(bytes)?;
        let owned = match input {
            ProjectionMappingInput::GraphNode { epoch, node_id } => Self::GraphNode(epoch, node_id),
            ProjectionMappingInput::DeleteDocument { epoch, document_id } => {
                Self::DeleteDocument(epoch, document_id.into())
            }
            ProjectionMappingInput::RelationalKey { epoch, table, key } => {
                Self::Key(epoch, table.into(), key.clone())
            }
            ProjectionMappingInput::End { source_epoch } => Self::End(source_epoch),
        };
        Ok((owned, memory))
    }
}
fn capture_input(
    source: &mut PreparedProjectionFeed,
    mut step: ProjectionFeedLimits,
    stage: &SearchOutOfCoreUnorderedMutationWriter<'_>,
    cap: usize,
) -> std::result::Result<Option<(OwnedInput, QueryMemoryLease)>, ProjectionFeedError> {
    step.items = NonZeroUsize::new(1).unwrap();
    let mut pending = None;
    source.visit_next(step, |_, item| {
        let item = match item {
            ProjectionFeedItem::GraphNode { epoch, node_id } => {
                ProjectionMappingInput::GraphNode { epoch, node_id }
            }
            ProjectionFeedItem::DeleteDocument { epoch, document_id } => {
                ProjectionMappingInput::DeleteDocument { epoch, document_id }
            }
            ProjectionFeedItem::RelationalKey { epoch, table, key } => {
                ProjectionMappingInput::RelationalKey { epoch, table, key }
            }
            ProjectionFeedItem::End { source_epoch } => {
                ProjectionMappingInput::End { source_epoch }
            }
        };
        pending = Some(OwnedInput::capture(item, stage, cap)?);
        Ok(())
    })?;
    Ok(pending)
}

/// Owns intake, a fixed mapper, one pending input/continuation, and an invisible
/// stage. Completion is staging only; it cannot authorize a visible watermark.
pub struct ProjectionStagingSession<'reader> {
    source: PreparedProjectionFeed,
    mapper: ProjectionMappingFunction,
    reads: PreparedReadLimits,
    limits: ProjectionMappingLimits,
    pending: Option<(OwnedInput, QueryMemoryLease)>,
    continuation: Option<Continuation>,
    usage: ProjectionMappingUsage,
    end: bool,
    closed: bool,
    failed: bool,
    stage: SearchOutOfCoreUnorderedMutationWriter<'reader>,
    _memory: QueryMemoryLease,
    task: RuntimeTaskContext,
}

/// Opaque artifact custody for subsequent baseline/coverage verification. It
/// deliberately implements no Search publication trait and exposes no candidate.
///
/// ```compile_fail
/// fn activate(db: &mut hawdb::Database, reads: &hawdb::PreparedReadDependencies,
///             staged: hawdb::ProjectionStagedInputs) {
///     db.publish_prepared_search(reads, staged);
/// }
/// ```
pub struct ProjectionStagedInputs {
    dependencies: PreparedReadDependencies,
    candidate: SearchOutOfCorePreparedUpdate,
    usage: ProjectionMappingUsage,
    sorting: SearchUnorderedMutationReport,
    inspected_operations: Option<usize>,
    _memory: QueryMemoryLease,
}
impl ProjectionStagedInputs {
    pub fn source_epoch(&self) -> u64 {
        self.dependencies.source_epoch()
    }
    /// Native final-output traversal, not baseline/coverage activation authority.
    pub fn inspected_operations(&self) -> Option<usize> {
        self.inspected_operations
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.usage
    }
    pub fn sorting_report(&self) -> SearchUnorderedMutationReport {
        self.sorting
    }
    /// Observes stage identity only, never activation authority.
    pub fn expected_active_generation(&self) -> Option<u64> {
        self.candidate.expected_active_generation()
    }
}

impl Database {
    pub fn begin_projection_staging_session<'reader>(
        &self,
        reader: &'reader SearchOutOfCoreReader,
        mut config: ProjectionStagingSessionConfig,
        mapper: ProjectionMappingFunction,
        admission: crate::search::SearchGenerationAdmission,
    ) -> std::result::Result<ProjectionStagingSession<'reader>, ProjectionFeedError> {
        if config.build.source_graph_commit_epoch.is_some() {
            return Err(ProjectionFeedError::Source(refusal(
                "source watermark belongs to the session",
            )));
        }
        config
            .task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        let mut source =
            self.begin_prepared_projection_feed(config.after_epoch, config.reads, config.feed)?;
        if reader.source_graph_commit_epoch() != Some(config.after_epoch) {
            return Err(ProjectionFeedError::Source(refusal(
                "Search base watermark does not match the requested source interval",
            )));
        }
        config.build.source_graph_commit_epoch = Some(source.source_epoch());
        let stage = admission
            .prepare_unordered_update(reader, config.build, config.sorting, config.task.clone())
            .map_err(ProjectionFeedError::Source)?;
        config.task = stage.task_context().clone();
        source.set_task_context(config.task.clone());
        let memory = stage
            .reserve_session_metadata(std::mem::size_of::<ProjectionStagingSession<'reader>>())
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionStagingSession {
            source,
            stage,
            mapper,
            reads: config.reads,
            limits: config.mapping,
            task: config.task,
            pending: None,
            continuation: None,
            usage: Default::default(),
            end: false,
            closed: false,
            failed: false,
            _memory: memory,
        })
    }
}
impl ProjectionStagingSession<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.source.source_epoch()
    }
    pub fn feed_usage(&self) -> ProjectionFeedUsage {
        self.source.usage()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.usage
    }
    pub fn sorting_report(&self) -> SearchUnorderedMutationReport {
        self.stage.report()
    }
    pub fn advance(
        &mut self,
        feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> std::result::Result<ProjectionStagingSessionStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        let result = self.advance_inner(feed, ordering);
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    fn advance_inner(
        &mut self,
        feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> std::result::Result<ProjectionStagingSessionStep, ProjectionFeedError> {
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        if self.end && !self.closed {
            self.stage
                .finish_input()
                .map_err(ProjectionFeedError::Source)?;
            self.closed = true;
        }
        if self.closed || !self.stage.ready_for_input() {
            self.stage
                .advance_ordering(ordering)
                .map_err(ProjectionFeedError::Source)?;
            return Ok(if self.stage.ordering_complete() {
                ProjectionStagingSessionStep::Complete
            } else {
                ProjectionStagingSessionStep::Ordering
            });
        }
        let Some((input, _)) = &self.pending else {
            self.pending = capture_input(
                &mut self.source,
                feed,
                &self.stage,
                self.limits.input_bytes.get(),
            )?;
            return Ok(ProjectionStagingSessionStep::Input);
        };
        increment(&mut self.usage.calls, 1, self.limits.calls.get())
            .map_err(ProjectionFeedError::Source)?;
        let mut mapped = None;
        let mut next = None;
        let continuation = self
            .continuation
            .as_ref()
            .map_or(&[][..], |c| c.bytes.as_ref());
        let stage = &mut self.stage;
        let usage = &mut self.usage;
        let mapper = self.mapper;
        let limits = self.limits;
        let reads = self.reads;
        self.source
            .with_tracked_read(|read| {
                let mut context = ProjectionMappingContext {
                    read,
                    stage,
                    limits,
                    reads,
                    usage,
                    queries: 0,
                    emitted: false,
                    failed: false,
                    next: None,
                };
                let status = mapper(&mut context, input.borrow(), continuation)?;
                if context.failed {
                    return Err(refusal("mapper ignored a failed operation"));
                }
                if status == ProjectionMappingStatus::More
                    && context.queries == 0
                    && !context.emitted
                    && context.next.is_none()
                {
                    return Err(refusal("mapper yielded without progress"));
                }
                next = context.next;
                mapped = Some(status);
                Ok(())
            })
            .map_err(ProjectionFeedError::Callback)?;
        if mapped == Some(ProjectionMappingStatus::Done) {
            self.end = matches!(input, OwnedInput::End(_));
            self.pending = None;
            self.continuation = None;
        } else if next.is_some() {
            self.continuation = next;
        }
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        Ok(ProjectionStagingSessionStep::Mapping)
    }
    /// Artifact construction is currently blocking after bounded intake/ordering.
    /// Returns opaque custody, not a baseline or complete-through receipt.
    pub fn finish_staging(
        self,
    ) -> std::result::Result<ProjectionStagedInputs, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.end || self.pending.is_some() || !self.stage.ordering_complete() {
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
        Ok(ProjectionStagedInputs {
            dependencies,
            candidate,
            usage: self.usage,
            sorting,
            inspected_operations: None,
            _memory: self._memory,
        })
    }
}
