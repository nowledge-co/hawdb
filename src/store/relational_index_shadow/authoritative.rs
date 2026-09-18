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

use super::*;
use hawdb_storage::relational_index_view::AuthoritativeRelationalConstraintIndex;

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
                HawDBError::StorageIntegrity(
                    "authoritative relational indexes require a canonical generation binding"
                        .to_string(),
                )
            })?;
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.relational_index_generation_artifacts != Some(binding))
        {
            return Err(HawDBError::StorageIntegrity(
                "authoritative relational index snapshot binding does not match durable state"
                    .to_string(),
            ));
        }
        let view = self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
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
            return Err(HawDBError::StorageIntegrity(format!(
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
            return Err(HawDBError::StorageIntegrity(
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
            Some(prepared) => Err(HawDBError::Storage(format!(
                "authoritative relational index checkpoint candidate failed before canonical publication: {}",
                prepared
                    .report
                    .error
                    .as_deref()
                    .unwrap_or("candidate is unavailable")
            ))),
            None => Err(HawDBError::Storage(
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
            Some(Ok(view)) => Err(HawDBError::StorageIntegrity(format!(
                "authoritative relational index staged visible epoch {} for commit {next_epoch}",
                view.identity().visible_commit_epoch
            ))),
            Some(Err(unavailable)) => Err(HawDBError::Storage(format!(
                "authoritative relational index could not stage commit {next_epoch}: {}",
                unavailable.reason
            ))),
            None => Err(HawDBError::StorageIntegrity(format!(
                "authoritative relational index has no current view for commit {next_epoch}"
            ))),
        }
    }
}
