use super::*;
use skein_storage::relational_index_view::AuthoritativeRelationalConstraintIndex;

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
