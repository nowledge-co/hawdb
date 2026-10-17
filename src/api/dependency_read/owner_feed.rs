// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Enumeration and reads share one E. Completion does not certify mapped output.

use super::projection_feed::{with_projection_read, ProjectionFeedRead};
use super::*;
use hawdb_storage::store::ProjectionOwnerFeedCursor;
pub use hawdb_storage::store::{ProjectionOwnerBefore, ProjectionOwnerFeedItem, ProjectionOwnerId};

#[derive(Debug)]
pub struct PreparedProjectionOwnerFeed {
    prepared: PreparedRead,
    cursor: ProjectionOwnerFeedCursor,
    failed: bool,
}
impl Database {
    /// Adds admitted node/type/table guards at this feed's same immutable E.
    /// Query dependencies outside these domains still accumulate normally.
    /// This returns enumeration/read evidence, never a coverage receipt.
    pub fn begin_prepared_projection_owner_feed_with_scopes(
        &self,
        start_epoch: u64,
        reads: PreparedReadLimits,
        total_feed: ProjectionFeedLimits,
        scopes: ProjectionReadScopes<'_>,
    ) -> std::result::Result<PreparedProjectionOwnerFeed, ProjectionFeedError> {
        scopes.admit(reads).map_err(ProjectionFeedError::Source)?;
        let mut feed = self.begin_prepared_projection_owner_feed(start_epoch, reads, total_feed)?;
        scopes
            .register(&mut feed.prepared)
            .map_err(ProjectionFeedError::Source)?;
        Ok(feed)
    }
    /// Pins one immutable E and enumerates endpoint-complete owner facts after C.
    /// Capture contract V2 retains the V1 preimage payload encoding.
    /// Old capture contracts or trimmed intervals require a full baseline. This
    /// method does not pin future appends or certify a host mapping or stage.
    pub fn begin_prepared_projection_owner_feed(
        &self,
        start_epoch: u64,
        reads: PreparedReadLimits,
        total_feed: ProjectionFeedLimits,
    ) -> std::result::Result<PreparedProjectionOwnerFeed, ProjectionFeedError> {
        let prepared = self
            .begin_prepared_read(reads)
            .map_err(ProjectionFeedError::Source)?;
        let cursor = prepared
            .read
            .store
            .capture_projection_owner_feed(start_epoch, total_feed)?;
        Ok(PreparedProjectionOwnerFeed {
            prepared,
            cursor,
            failed: false,
        })
    }
}
impl PreparedProjectionOwnerFeed {
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
    /// Every callback query uses the same pinned source as the borrowed event.
    /// Ignored query errors, callback errors and panics invalidate completion.
    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(&ProjectionFeedRead<'_>, ProjectionOwnerFeedItem<'_>) -> Result<()>,
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
    /// Returns tracked dependencies only after enumeration's End callback.
    /// This is not a C..E coverage receipt and cannot authorize a watermark.
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
    fn limits() -> (PreparedReadLimits, ProjectionFeedLimits) {
        let n = |value| std::num::NonZeroUsize::new(value).unwrap();
        (
            PreparedReadLimits {
                dependency_entries: n(32),
                dependency_bytes: n(4096),
                statement_rows: n(8),
                statement_payload_bytes: n(4096),
            },
            ProjectionFeedLimits {
                work: n(100),
                items: n(10),
                bytes: n(4096),
            },
        )
    }
    #[test]
    fn owner_source_poison_invalidates_before_visit_and_after_acknowledged_end() {
        for ended in [false, true] {
            let db = Database::new();
            let (reads, budget) = limits();
            let mut feed = db
                .begin_prepared_projection_owner_feed(db.commit_epoch().unwrap(), reads, budget)
                .unwrap();
            if ended {
                feed.visit_next(budget, |_, item| {
                    assert!(matches!(item, ProjectionOwnerFeedItem::End { .. }));
                    Ok(())
                })
                .unwrap();
                assert!(feed.is_complete());
            }
            db.runtime
                .get()
                .unwrap()
                .store
                .poison_on_storage_error(&Err::<(), _>(HawDBError::StorageIntegrity(
                    "injected source uncertainty".into(),
                )));
            if ended {
                assert!(matches!(feed.finish(), Err(ProjectionFeedError::Source(_))));
            } else {
                assert!(matches!(
                    feed.visit_next(budget, |_, _| panic!("poison must precede callback")),
                    Err(ProjectionFeedError::Source(_))
                ));
                assert!(matches!(feed.finish(), Err(ProjectionFeedError::Failed)));
            }
        }
    }
    #[test]
    fn scalar_owner_label_lookup_remains_pinned_and_validates_schema_changes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id:'a'})").unwrap();
        let (reads, budget) = limits();
        let mut feed = db
            .begin_prepared_projection_owner_feed(db.commit_epoch().unwrap(), reads, budget)
            .unwrap();
        // This absent label is added on the live source before callback use.
        db.query("CREATE (:Later {id:'b'})").unwrap();
        feed.visit_next(budget, |read, item| {
            assert!(matches!(item, ProjectionOwnerFeedItem::End { .. }));
            assert!(read.graph_label_id("Memory")?.is_some());
            assert!(read.graph_label_id("Later")?.is_none());
            Ok(())
        })
        .unwrap();
        let dependencies = feed.finish().unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        assert!(db
            .runtime
            .get()
            .unwrap()
            .store
            .validate_read_dependencies(&dependencies.graph)
            .is_err());
    }
}

#[cfg(test)]
mod label_absence_tests {
    use super::*;
    fn capture(db: &Database, name: &str) -> PreparedReadDependencies {
        let n = |value| std::num::NonZeroUsize::new(value).unwrap();
        let budget = ProjectionFeedLimits {
            work: n(100),
            items: n(10),
            bytes: n(4096),
        };
        let reads = PreparedReadLimits {
            dependency_entries: n(32),
            dependency_bytes: n(4096),
            statement_rows: n(8),
            statement_payload_bytes: n(4096),
        };
        let mut feed = db
            .begin_prepared_projection_owner_feed(db.commit_epoch().unwrap(), reads, budget)
            .unwrap();
        feed.visit_next(budget, |read, _| {
            assert_eq!(read.graph_label_id(name)?.is_none(), name == "Later");
            Ok(())
        })
        .unwrap();
        feed.finish().unwrap()
    }
    #[test]
    fn absent_label_lookup_detects_implicit_create_with_a_precise_domain() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
        let dependencies = capture(&db, "Later");
        assert_eq!(dependencies.node_domain_count(), 1);
        assert!(!dependencies.requires_unchanged_database_epoch());
        db.query("CREATE (:Later {id:'b'})").unwrap();
        assert!(db
            .runtime
            .get()
            .unwrap()
            .store
            .validate_read_dependencies(&dependencies.graph)
            .is_err());
    }
    #[test]
    fn label_lookup_does_not_conflict_with_unrelated_property_writes() {
        for name in ["Later", "Memory"] {
            let mut db = Database::new();
            db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
            let dependencies = capture(&db, name);
            assert_eq!(
                dependencies.node_domain_count(),
                usize::from(name == "Later")
            );
            assert!(!dependencies.requires_unchanged_database_epoch());
            db.query("MATCH (n:Memory) SET n.value=2").unwrap();
            db.runtime
                .get()
                .unwrap()
                .store
                .validate_read_dependencies(&dependencies.graph)
                .unwrap();
        }
    }
}
