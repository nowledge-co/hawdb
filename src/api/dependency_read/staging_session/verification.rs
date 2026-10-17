// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::search::{SearchMutationInspectionInput, SearchMutationInspectionStep};

/// Fixed per-final-mutation verification, including explicit End. Business
/// ownership and expected-document derivation are trusted host contracts.
/// Success records traversal only; it does not authorize visible publication.
pub type ProjectionOutputVerificationFunction = fn(
    &mut ProjectionOutputVerificationContext<'_, '_>,
    &mut SearchMutationInspectionInput<'_>,
) -> Result<()>;

/// Only tracked reads from the still-pinned E source; no output, feed or stage
/// escape. Reservations share the original session's cumulative mapping budget.
pub struct ProjectionOutputVerificationContext<'a, 'source> {
    read: &'a ProjectionFeedRead<'source>,
    limits: ProjectionMappingLimits,
    reads: PreparedReadLimits,
    usage: &'a mut ProjectionMappingUsage,
    queries: usize,
    failed: bool,
}
impl<'a, 'source> ProjectionOutputVerificationContext<'a, 'source> {
    pub(super) fn new(
        read: &'a ProjectionFeedRead<'source>,
        limits: ProjectionMappingLimits,
        reads: PreparedReadLimits,
        usage: &'a mut ProjectionMappingUsage,
    ) -> Self {
        Self {
            read,
            limits,
            reads,
            usage,
            queries: 0,
            failed: false,
        }
    }
    pub(super) fn is_failed(&self) -> bool {
        self.failed
    }
}
impl ProjectionOutputVerificationContext<'_, '_> {
    pub fn source_epoch(&self) -> u64 {
        self.read.source_epoch()
    }
    fn query(&mut self) -> Result<()> {
        if self.failed {
            return Err(refusal("verifier ignored an earlier failure"));
        }
        self.failed = true;
        reserve_query(self.limits, self.reads, self.usage, &mut self.queries)
    }
    pub fn query_with_params(
        &mut self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
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
        self.query()?;
        let result = self.read.query_sql_with_params(text, parameters);
        self.failed = result.is_err();
        result
    }
}

pub struct ProjectionOutputVerificationSession<'reader> {
    session: ProjectionStagingSession<'reader>,
    verifier: ProjectionOutputVerificationFunction,
    mutations: usize,
    complete: bool,
    _memory: QueryMemoryLease,
}
impl<'reader> ProjectionStagingSession<'reader> {
    /// Consume closed, ordered intake into a fixed read-only output checker.
    /// Checks run before closing the pinned query context. No caller ordinal,
    /// count or mutable output list can substitute for the native traversal.
    pub fn verify_outputs(
        self,
        verifier: ProjectionOutputVerificationFunction,
    ) -> std::result::Result<ProjectionOutputVerificationSession<'reader>, ProjectionFeedError>
    {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.end || self.pending.is_some() || !self.stage.ordering_complete() {
            return Err(ProjectionFeedError::Incomplete);
        }
        let memory = self
            .stage
            .reserve_session_metadata(std::mem::size_of::<
                ProjectionOutputVerificationSession<'reader>,
            >())
            .map_err(ProjectionFeedError::Source)?;
        Ok(ProjectionOutputVerificationSession {
            session: self,
            verifier,
            mutations: 0,
            complete: false,
            _memory: memory,
        })
    }
}
impl ProjectionOutputVerificationSession<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.session.source_epoch()
    }
    pub fn mapping_usage(&self) -> ProjectionMappingUsage {
        self.session.mapping_usage()
    }
    pub fn inspected_operations(&self) -> usize {
        self.mutations
    }
    pub fn advance(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> std::result::Result<SearchMutationInspectionStep, ProjectionFeedError> {
        let s = &mut self.session;
        if s.failed {
            return Err(ProjectionFeedError::Failed);
        }
        s.failed = true;
        let verifier = self.verifier;
        let source = &mut s.source;
        let usage = &mut s.usage;
        let mapping = s.limits;
        let reads = s.reads;
        let mut mutation_visited = false;
        let result = s
            .stage
            .inspect_reduced_next(limits, |input| {
                increment(&mut usage.calls, 1, mapping.calls.get())?;
                source.with_tracked_read(|read| {
                    let mut context = ProjectionOutputVerificationContext {
                        read,
                        limits: mapping,
                        reads,
                        usage,
                        queries: 0,
                        failed: false,
                    };
                    verifier(&mut context, input)?;
                    if context.failed {
                        return Err(refusal("verifier ignored a failed query"));
                    }
                    Ok(())
                })?;
                mutation_visited = input.id().is_some();
                Ok(())
            })
            .map_err(ProjectionFeedError::Callback)?;
        s.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(refusal(&e.to_string())))?;
        if mutation_visited {
            self.mutations = self
                .mutations
                .checked_add(1)
                .ok_or_else(|| ProjectionFeedError::Source(refusal("inspection count overflow")))?;
        }
        self.complete = result.complete;
        s.failed = false;
        Ok(result)
    }
    /// Final artifact construction remains blocking. Even a fully checked run
    /// yields only opaque custody, not verified baseline or coverage authority.
    pub fn finish(self) -> std::result::Result<ProjectionStagedInputs, ProjectionFeedError> {
        if self.session.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.complete {
            return Err(ProjectionFeedError::Incomplete);
        }
        let mut staged = self.session.finish_staging()?;
        staged.inspected_operations = Some(self.mutations);
        Ok(staged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }
    fn mapped(
        ctx: &mut ProjectionMappingContext<'_, '_, '_>,
        input: ProjectionMappingInput<'_>,
        _: &[u8],
    ) -> Result<ProjectionMappingStatus> {
        if let ProjectionMappingInput::DeleteDocument { document_id, .. } = input {
            ctx.delete(document_id)?;
        }
        Ok(ProjectionMappingStatus::Done)
    }
    fn absent(
        ctx: &mut ProjectionOutputVerificationContext<'_, '_>,
        input: &mut SearchMutationInspectionInput<'_>,
    ) -> Result<()> {
        if let Some(id) = input.id() {
            assert_eq!(id, "memory:a");
            assert!(input.is_delete());
            let rows = ctx.query_with_params(
                "MATCH (n:Memory) WHERE n.id=$id RETURN n.id AS id",
                &BTreeMap::from([("id".into(), Value::String("a".into()))]),
            )?;
            assert!(rows.output.rows.is_empty());
        }
        Ok(())
    }
    #[test]
    fn logical_owner_verification_captures_absence_and_rejects_new_physical_owner() {
        for related in [false, true] {
            let mut db = Database::new();
            db.query("CREATE INDEX ON :Memory(id)").unwrap();
            db.query("CREATE (:Memory {id:'a'})").unwrap();
            let c = db.commit_epoch().unwrap();
            static SEQUENCE: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "hawdb-output-owner-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            let mut base = SearchOutOfCoreGenerationWriter::create(
                &root,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(c),
                    ..Default::default()
                },
            )
            .unwrap();
            base.push(SearchDocument {
                id: "memory:a".into(),
                title: "old".into(),
                content: "old".into(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
            base.finish().unwrap();
            let reader = SearchOutOfCoreReader::open(&root).unwrap();
            db.query("MATCH (n:Memory) DETACH DELETE n").unwrap();
            let resources = RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(nz(2), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(1024 * 1024 * 1024),
                    Some(1024 * 1024 * 1024),
                    None,
                    None,
                    None,
                ),
            );
            let governor = RuntimeGovernor::new(
                RuntimeGovernorConfig::shared_host(),
                resources,
                IoConcurrencyBudget::new(2, 1),
            );
            let admission = SearchGenerationAdmission::acquire(
                &governor,
                RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024).with_io_wave_slots(1),
            )
            .unwrap();
            let feed = ProjectionFeedLimits {
                work: nz(10000),
                items: nz(100),
                bytes: nz(65536),
            };
            let config = ProjectionStagingSessionConfig {
                after_epoch: c,
                reads: PreparedReadLimits {
                    dependency_entries: nz(64),
                    dependency_bytes: nz(8192),
                    statement_rows: nz(4),
                    statement_payload_bytes: nz(4096),
                },
                feed,
                mapping: ProjectionMappingLimits {
                    calls: nz(64),
                    queries: nz(16),
                    reserved_rows: nz(64),
                    reserved_payload_bytes: nz(65536),
                    queries_per_call: nz(1),
                    input_bytes: nz(4096),
                    continuation_bytes: nz(128),
                },
                build: Default::default(),
                sorting: Default::default(),
                task: Default::default(),
            };
            let mut session = db
                .begin_projection_staging_session(&reader, config, mapped, admission)
                .unwrap();
            for _ in 0..100 {
                if session.advance(feed, Default::default()).unwrap()
                    == ProjectionStagingSessionStep::Complete
                {
                    break;
                }
            }
            let mut verifying = session.verify_outputs(absent).unwrap();
            for _ in 0..100 {
                if verifying.advance(Default::default()).unwrap().complete {
                    break;
                }
            }
            let staged = verifying.finish().unwrap();
            assert_eq!(staged.inspected_operations(), Some(1));
            assert!(!staged.dependencies.requires_unchanged_database_epoch());
            if related {
                db.query("CREATE (:Memory {id:'a'})").unwrap();
            } else {
                db.query("CREATE (:Memory {id:'b'})").unwrap();
            }
            let result = db
                .runtime
                .get_mut()
                .unwrap()
                .store
                .validate_read_dependencies(&staged.dependencies.graph);
            assert_eq!(
                result.is_err(),
                related,
                "logical-owner membership must include absence"
            );
            drop(staged);
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
