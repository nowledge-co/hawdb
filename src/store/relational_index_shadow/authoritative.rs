use super::*;
use skein_storage::{RelationalConstraintIndex, RelationalError};
use std::cell::RefCell;

#[derive(Debug, Default)]
struct AuthoritativeReadUsage {
    logical_pages: usize,
    logical_bytes: usize,
    rows: usize,
    file_bytes: usize,
}

#[derive(Debug)]
pub(super) struct AuthoritativeReadLedger {
    limits: RelationalIndexReadLimits,
    usage: RefCell<AuthoritativeReadUsage>,
}

impl AuthoritativeReadLedger {
    pub(super) fn new(limits: RelationalIndexReadLimits) -> Self {
        Self {
            limits,
            usage: RefCell::new(AuthoritativeReadUsage::default()),
        }
    }

    pub(super) fn remaining_limits(&self) -> Result<RelationalIndexReadLimits, RelationalError> {
        let usage = self.usage.borrow();
        let max_pages = self
            .limits
            .max_pages
            .get()
            .checked_sub(usage.logical_pages)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index page budget is exhausted".to_string(),
                )
            })?;
        let max_rows = self
            .limits
            .max_rows
            .get()
            .checked_sub(usage.rows)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index row budget is exhausted".to_string(),
                )
            })?;
        let max_bytes = self
            .limits
            .max_bytes
            .get()
            .checked_sub(usage.logical_bytes)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative relational index byte budget is exhausted".to_string(),
                )
            })?;
        Ok(RelationalIndexReadLimits {
            max_pages,
            max_rows,
            max_bytes,
            max_file_bytes: self
                .limits
                .max_file_bytes
                .checked_sub(usage.file_bytes)
                .ok_or_else(|| {
                    RelationalError::Admission(
                        "authoritative relational index file-byte accounting overflow".to_string(),
                    )
                })?,
            max_tree_height: self.limits.max_tree_height,
        })
    }

    pub(super) fn record(
        &self,
        report: &RelationalIndexReadViewReport,
    ) -> Result<(), RelationalError> {
        let (backend_pages, backend_bytes, file_bytes) = match &report.backend {
            RelationalIndexReadViewBackendReport::Base(report) => {
                (report.pages_read, report.bytes_read, report.file_bytes_read)
            }
            RelationalIndexReadViewBackendReport::Recovered(report) => {
                let pages = report
                    .base
                    .pages_read
                    .checked_add(report.delta_pages_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index page accounting overflow".to_string(),
                        )
                    })?;
                let bytes = report
                    .base
                    .bytes_read
                    .checked_add(report.delta_bytes_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index byte accounting overflow".to_string(),
                        )
                    })?;
                let file_bytes = report
                    .base
                    .file_bytes_read
                    .checked_add(report.delta_file_bytes_read)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "authoritative index file-byte accounting overflow".to_string(),
                        )
                    })?;
                (pages, bytes, file_bytes)
            }
        };
        let logical_bytes = backend_bytes
            .checked_add(report.live_bytes_visited)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index live-byte accounting overflow".to_string(),
                )
            })?;
        let mut usage = self.usage.borrow_mut();
        usage.logical_pages = usage
            .logical_pages
            .checked_add(backend_pages)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index page accounting overflow".to_string(),
                )
            })?;
        usage.logical_bytes = usage
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative index byte accounting overflow".to_string(),
                )
            })?;
        usage.rows = usage.rows.checked_add(report.rows_visited).ok_or_else(|| {
            RelationalError::Admission("authoritative index row accounting overflow".to_string())
        })?;
        usage.file_bytes = usage.file_bytes.checked_add(file_bytes).ok_or_else(|| {
            RelationalError::Admission(
                "authoritative index file-byte accounting overflow".to_string(),
            )
        })?;
        if usage.logical_pages > self.limits.max_pages.get()
            || usage.logical_bytes > self.limits.max_bytes.get()
            || usage.rows > self.limits.max_rows.get()
            || usage.file_bytes > self.limits.max_file_bytes
        {
            return Err(RelationalError::Admission(
                "authoritative relational index reader exceeded its transaction budget".to_string(),
            ));
        }
        Ok(())
    }
}

/// Transaction-scoped persistent constraint reader.
///
/// The owned `Arc` pins one immutable visibility epoch. A single ledger spans
/// every primary, unique, UPSERT, and foreign-key probe in the transaction so
/// a sequence of individually small lookups cannot bypass admission.
pub(in crate::store) struct AuthoritativeRelationalConstraintIndex {
    view: Arc<RelationalIndexReadView>,
    ledger: AuthoritativeReadLedger,
}

impl AuthoritativeRelationalConstraintIndex {
    fn new(view: Arc<RelationalIndexReadView>, limits: RelationalIndexReadLimits) -> Self {
        Self {
            view,
            ledger: AuthoritativeReadLedger::new(limits),
        }
    }
}

impl RelationalConstraintIndex for AuthoritativeRelationalConstraintIndex {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        let report = self
            .view
            .visit_exact_postings(table, index, key, self.ledger.remaining_limits()?, visit)
            .map_err(map_constraint_read_error)?;
        self.ledger.record(&report)
    }
}

pub(super) fn map_constraint_read_error(error: RelationalIndexShadowError) -> RelationalError {
    match error {
        RelationalIndexShadowError::Admission(message) => RelationalError::Admission(message),
        RelationalIndexShadowError::Durability(message) => RelationalError::Durability(message),
        RelationalIndexShadowError::Corrupt(message) => RelationalError::Corruption(message),
        RelationalIndexShadowError::MissingIndex { table, index } => RelationalError::Corruption(
            format!("authoritative relational index {table}.{index} is missing"),
        ),
        RelationalIndexShadowError::StaleGeneration {
            expected_previous,
            actual_previous,
        } => RelationalError::Corruption(format!(
            "authoritative relational index generation changed: expected {expected_previous:?}, got {actual_previous:?}"
        )),
    }
}

impl GraphStore {
    pub(in crate::store) fn authoritative_relational_constraint_index(
        &self,
    ) -> crate::Result<Option<AuthoritativeRelationalConstraintIndex>> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(None);
        }
        self.validate_authoritative_relational_index_open()?;
        let view = Arc::clone(
            self.relational_index_shadow
                .current_read_view(self.commit_epoch)
                .expect("validated authoritative view must remain current"),
        );
        Ok(Some(AuthoritativeRelationalConstraintIndex::new(
            view,
            RelationalIndexReadLimits::default(),
        )))
    }

    pub(in crate::store) fn validate_authoritative_relational_index_open(
        &self,
    ) -> crate::Result<()> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(());
        }
        let binding = self
            .relational_index_shadow
            .generation_artifacts
            .ok_or_else(|| {
                SkeinError::StorageIntegrity(
                    "authoritative relational indexes require a canonical generation binding"
                        .to_string(),
                )
            })?;
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.relational_index_generation_artifacts != Some(binding))
        {
            return Err(SkeinError::StorageIntegrity(
                "authoritative relational index snapshot binding does not match durable state"
                    .to_string(),
            ));
        }
        let view = self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
            .ok_or_else(|| {
                SkeinError::StorageIntegrity(format!(
                    "authoritative relational index view is unavailable at commit epoch {}",
                    self.commit_epoch
                ))
            })?;
        let identity = view.identity();
        if identity.base_generation != binding.generation
            || identity.base_commit_epoch != binding.source_commit_epoch
            || identity.root_set_digest != binding.root_set_digest
            || identity.visible_commit_epoch != self.commit_epoch
        {
            return Err(SkeinError::StorageIntegrity(format!(
                "authoritative relational index identity {}/{}/{} does not match canonical binding {}/{}/{} at visible epoch {}",
                identity.base_generation,
                identity.base_commit_epoch,
                identity.root_set_digest,
                binding.generation,
                binding.source_commit_epoch,
                binding.root_set_digest,
                self.commit_epoch,
            )));
        }
        if view.is_poisoned() {
            return Err(SkeinError::StorageIntegrity(
                "authoritative relational index view is poisoned".to_string(),
            ));
        }
        Ok(())
    }

    pub(in crate::store) fn require_authoritative_relational_index_candidate(
        &self,
        prepared: &Option<PreparedRelationalIndexCandidate>,
    ) -> crate::Result<()> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(());
        }
        match prepared {
            Some(PreparedRelationalIndexCandidate {
                candidate: Some(_),
                ..
            }) => Ok(()),
            Some(prepared) => Err(SkeinError::Storage(format!(
                "authoritative relational index checkpoint candidate failed before canonical publication: {}",
                prepared
                    .report
                    .error
                    .as_deref()
                    .unwrap_or("candidate is unavailable")
            ))),
            None => Err(SkeinError::Storage(
                "authoritative relational index checkpoint did not prepare a required candidate"
                    .to_string(),
            )),
        }
    }

    pub(in crate::store) fn require_authoritative_relational_index_live_publication(
        &self,
        next_epoch: u64,
        publication: &Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>>,
    ) -> crate::Result<()> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(());
        }
        match publication {
            Some(Ok(view))
                if view.identity().visible_commit_epoch == next_epoch && !view.is_poisoned() =>
            {
                Ok(())
            }
            Some(Ok(view)) => Err(SkeinError::StorageIntegrity(format!(
                "authoritative relational index staged visible epoch {} for commit {next_epoch}",
                view.identity().visible_commit_epoch
            ))),
            Some(Err(unavailable)) => Err(SkeinError::Storage(format!(
                "authoritative relational index could not stage commit {next_epoch}: {}",
                unavailable.reason
            ))),
            None => Err(SkeinError::StorageIntegrity(format!(
                "authoritative relational index has no current view for commit {next_epoch}"
            ))),
        }
    }
}
