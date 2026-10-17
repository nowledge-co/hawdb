// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! One pinned query context and its bounded immutable feed. Enumeration is not
//! authority to publish a projection assembled by arbitrary callback code.

use super::*;
use hawdb_storage::store::ProjectionFeedCursor;
pub use hawdb_storage::store::{
    ProjectionFeedError, ProjectionFeedItem, ProjectionFeedLimits, ProjectionFeedResource,
    ProjectionFeedStep, ProjectionFeedUsage,
};
use std::cell::{Cell, RefCell};

/// Enumerates C..E in bounded steps while all queries use the same pinned E.
/// Owns source/history pins and cumulative feed admission. It does not own or
/// certify staged output, host mapping, external bindings or a Search base.
#[derive(Debug)]
pub struct PreparedProjectionFeed {
    prepared: PreparedRead,
    cursor: ProjectionFeedCursor,
    failed: bool,
}

/// Tracked reads for a feed callback. No mutable PreparedRead or raw transaction
/// escape permits substituting another source or finishing an incomplete feed.
pub struct ProjectionFeedRead<'a> {
    read: RefCell<&'a mut PreparedRead>,
    source_epoch: u64,
    failed: Cell<bool>,
}
impl ProjectionFeedRead<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }
    /// Resolves a known mapping label against this same source catalog, including
    /// when an owner's node has been deleted. Returns a scalar without copying
    /// catalog metadata. Finished dependencies validate explicit schema changes;
    /// absent names also retain a node-domain guard for implicit label creation.
    pub fn graph_label_id(&self, name: &str) -> Result<Option<u32>> {
        self.with_read(|read| {
            read.read.store.ensure_usable()?;
            let label = read.read.catalog.label_id(name).map(|label| label.0);
            if label.is_none() {
                // CREATE can introduce a label implicitly without a Schema WAL
                // op. Protect the observed absence with its exact node domain.
                super::record_node_domain(name, &mut read.dependencies.graph)?;
            }
            Ok(label)
        })
    }
    fn with_read<T>(&self, operation: impl FnOnce(&mut PreparedRead) -> Result<T>) -> Result<T> {
        let result = self
            .read
            .try_borrow_mut()
            .map_err(|_| {
                HawDBError::Execution("projection feed read context cannot be reentered".into())
            })
            .and_then(|mut read| operation(&mut read));
        if result.is_err() {
            self.failed.set(true);
        }
        result
    }
    pub fn query_with_params(
        &self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
        self.with_read(|read| read.query_with_params(text, parameters))
    }
    pub fn query_sql_with_params(&self, text: &str, parameters: &[Value]) -> Result<QueryOutput> {
        self.with_read(|read| read.query_sql_with_params(text, parameters))
    }
    pub fn hydrate_search_projection_nodes(
        &self,
        ids: &[u64],
        max_output_payload_bytes: NonZeroUsize,
    ) -> Result<Vec<crate::search::SearchProjectionRow>> {
        self.with_read(|read| read.hydrate_search_projection_nodes(ids, max_output_payload_bytes))
    }
}

pub(super) fn with_projection_read(
    read: &mut PreparedRead,
    visitor: impl FnOnce(&ProjectionFeedRead<'_>) -> Result<()>,
) -> Result<()> {
    let context = ProjectionFeedRead {
        source_epoch: read.source_epoch(),
        read: RefCell::new(read),
        failed: Cell::new(false),
    };
    visitor(&context)?;
    let read = context.read.borrow();
    if context.failed.get() || read.failed {
        return Err(HawDBError::Execution(
            "projection feed callback ignored a tracked read failure".into(),
        ));
    }
    read.read.store.ensure_usable()
}

impl Database {
    /// Captures source and feed under one immutable owner borrow. Both graph and
    /// relational keys are split without eagerly cloning the retained log.
    pub fn begin_prepared_projection_feed(
        &self,
        start_epoch: u64,
        reads: PreparedReadLimits,
        total_feed: ProjectionFeedLimits,
    ) -> std::result::Result<PreparedProjectionFeed, ProjectionFeedError> {
        let prepared = self
            .begin_prepared_read(reads)
            .map_err(ProjectionFeedError::Source)?;
        let cursor = prepared
            .read
            .store
            .capture_projection_feed(start_epoch, total_feed)?;
        Ok(PreparedProjectionFeed {
            prepared,
            cursor,
            failed: false,
        })
    }
}
impl PreparedProjectionFeed {
    pub(super) fn set_task_context(&mut self, task: hawdb_core::RuntimeTaskContext) {
        self.prepared.read.task_context = Some(task);
    }

    pub(super) fn with_tracked_read(
        &mut self,
        visitor: impl FnOnce(&ProjectionFeedRead<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.failed {
            return Err(HawDBError::Execution("projection feed is unusable".into()));
        }
        self.failed = true;
        with_projection_read(&mut self.prepared, visitor)?;
        self.failed = false;
        Ok(())
    }
    pub fn source_epoch(&self) -> u64 {
        self.cursor.source_epoch()
    }
    pub fn start_epoch(&self) -> u64 {
        self.cursor.start_epoch()
    }
    pub fn usage(&self) -> ProjectionFeedUsage {
        self.cursor.usage()
    }
    pub fn is_complete(&self) -> bool {
        !self.failed && self.cursor.is_complete()
    }

    /// Visits borrowed graph/relational inputs and an explicit pinned End.
    /// Work includes repeated keys, event/table visits and key-field preflight;
    /// bytes count borrowed key input, not total query/stage allocation or RSS.
    /// Statements retain their configured per-statement row/payload limits and
    /// cumulative dependency limits. Hosts additionally bound cross-statement
    /// query work, mapping and staging.
    /// An ignored query error, callback failure or panic invalidates the feed.
    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(&ProjectionFeedRead<'_>, ProjectionFeedItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        self.prepared
            .read
            .store
            .ensure_usable()
            .map_err(ProjectionFeedError::Source)?;
        let read = &mut self.prepared;
        let result = self.cursor.visit_next(step, |item| {
            with_projection_read(read, |context| visitor(context, item))
        });
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    /// Returns only tracked read dependencies after successful enumeration.
    /// This is not a batch coverage receipt and cannot certify callback output.
    pub fn finish(self) -> std::result::Result<PreparedReadDependencies, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.cursor.is_complete() {
            return Err(ProjectionFeedError::Incomplete);
        }
        self.prepared
            .read
            .store
            .ensure_usable()
            .map_err(ProjectionFeedError::Source)?;
        self.prepared.finish().map_err(ProjectionFeedError::Source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_source_poison_prevents_visits_and_completion() {
        let db = Database::new();
        let one = NonZeroUsize::new(1).unwrap();
        let reads = PreparedReadLimits {
            dependency_entries: NonZeroUsize::new(32).unwrap(),
            dependency_bytes: NonZeroUsize::new(4096).unwrap(),
            statement_rows: one,
            statement_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        };
        let budget = ProjectionFeedLimits {
            work: one,
            items: one,
            bytes: one,
        };
        let mut feed = db
            .begin_prepared_projection_feed(db.commit_epoch().unwrap(), reads, budget)
            .unwrap();
        db.runtime
            .get()
            .unwrap()
            .store
            .poison_on_storage_error(&Err::<(), _>(HawDBError::StorageIntegrity(
                "injected shared-source uncertainty".into(),
            )));
        let mut visited = false;
        assert!(matches!(
            feed.visit_next(budget, |_, _| {
                visited = true;
                Ok(())
            }),
            Err(ProjectionFeedError::Source(_))
        ));
        assert!(!visited);
        assert!(!feed.is_complete());
        assert!(matches!(feed.finish(), Err(ProjectionFeedError::Failed)));
    }
}
