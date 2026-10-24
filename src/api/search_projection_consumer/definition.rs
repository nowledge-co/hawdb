// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One versioned host projection contract, shared by bootstrap and catch-up.

use super::*;
use crate::{
    ProjectionBaselineMappingFunction, ProjectionConsumerCatchUpConfig,
    ProjectionConsumerCatchUpError, ProjectionConsumerCoverageSession,
    ProjectionConsumerStagingSession, ProjectionCoverageConfig, ProjectionMappingFunction,
    ProjectionSourceScope, ProjectionStagingSessionStep,
};
use hawdb_search::SearchMutationSortStepLimits;
use std::num::NonZeroU64;

/// Trusted bootstrap for the same deterministic definition as both mappers.
pub type SearchProjectionInitializer =
    fn(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>;

/// Immutable host-owned projection semantics. Change `version` whenever any
/// mapper or initializer changes. The native registration ID binds the logical
/// name, version and canonical source scope; function addresses are deliberately
/// excluded because they are neither stable identities nor semantic proofs.
///
/// Reusing a target with another version or scope is refused by the existing
/// native registration/target fences. A new definition requires a fresh target
/// and a complete bootstrap; it never adopts a legacy directory implicitly.
#[derive(Clone)]
pub struct SearchProjectionDefinition {
    name: SearchProjectionConsumerId,
    version: NonZeroU64,
    id: SearchProjectionConsumerId,
    scope: ProjectionSourceScope,
    initialize: SearchProjectionInitializer,
    delta: ProjectionMappingFunction,
    full: ProjectionBaselineMappingFunction,
}

impl std::fmt::Debug for SearchProjectionDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchProjectionDefinition")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("consumer_id", &self.id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl SearchProjectionDefinition {
    pub fn new(
        name: SearchProjectionConsumerId,
        version: NonZeroU64,
        mut scope: ProjectionSourceScope,
        initialize: SearchProjectionInitializer,
        delta: ProjectionMappingFunction,
        full: ProjectionBaselineMappingFunction,
    ) -> Result<Self> {
        let domains = match &mut scope {
            ProjectionSourceScope::Database => None,
            ProjectionSourceScope::Domains {
                node_labels,
                relational_tables,
            } => {
                for names in [&mut *node_labels, &mut *relational_tables] {
                    if names.iter().any(|name| name.is_empty()) {
                        return Err(HawDBError::Semantic(
                            "projection definition source names must be non-empty".into(),
                        ));
                    }
                    names.sort();
                    names.dedup();
                }
                Some((&*node_labels, &*relational_tables))
            }
        };
        // JSON tuples retain field boundaries, graph/table domain separation,
        // and the distinction between Database and explicitly empty Domains.
        let identity = serde_json::to_vec(&(
            "hawdb-projection-definition-v1",
            name.as_str(),
            version.get(),
            domains,
        ))
        .map_err(|error| HawDBError::Semantic(error.to_string()))?;
        let id = SearchProjectionConsumerId::new(format!(
            "definition.{}",
            hawdb_integrity::sha256(&identity)
        ))?;
        Ok(Self {
            name,
            version,
            id,
            scope,
            initialize,
            delta,
            full,
        })
    }

    pub fn name(&self) -> &SearchProjectionConsumerId {
        &self.name
    }
    pub fn version(&self) -> NonZeroU64 {
        self.version
    }
    pub fn consumer_id(&self) -> &SearchProjectionConsumerId {
        &self.id
    }
    pub fn scope(&self) -> &ProjectionSourceScope {
        &self.scope
    }

    pub(crate) fn initialize(
        &self,
        source: &mut DatabaseReadTransaction,
        index: &mut SearchIndex,
    ) -> Result<()> {
        (self.initialize)(source, index)
    }
}

/// Private bounded progress whose complete mapper and scope cannot be replaced
/// independently of the versioned definition that captured the interval.
pub struct DefinedProjectionStagingSession<'consumer> {
    session: ProjectionConsumerStagingSession<'consumer>,
    definition: &'consumer SearchProjectionDefinition,
}

impl<'consumer> DefinedProjectionStagingSession<'consumer> {
    pub fn source_epoch(&self) -> u64 {
        self.session.source_epoch()
    }

    pub fn advance(
        &mut self,
        feed: crate::ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> std::result::Result<ProjectionStagingSessionStep, ProjectionConsumerCatchUpError> {
        self.session.advance(feed, ordering)
    }

    pub fn verify_complete_source(
        self,
        config: ProjectionCoverageConfig,
    ) -> std::result::Result<
        ProjectionConsumerCoverageSession<'consumer>,
        ProjectionConsumerCatchUpError,
    > {
        self.session
            .verify_complete_source(config, self.definition.full)
    }
}

impl Database {
    pub fn open_or_create_defined_search_projection_consumer(
        &mut self,
        definition: &SearchProjectionDefinition,
        projection_directory: impl AsRef<Path>,
        options: SearchProjectionConsumerOptions,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer> {
        self.open_or_create_search_projection_consumer(
            definition.consumer_id().clone(),
            projection_directory,
            options,
            |source, index| definition.initialize(source, index),
        )
    }

    pub fn begin_defined_search_projection_consumer_catch_up<'consumer>(
        &mut self,
        consumer: &'consumer SearchProjectionConsumer,
        definition: &'consumer SearchProjectionDefinition,
        config: ProjectionConsumerCatchUpConfig,
    ) -> std::result::Result<
        DefinedProjectionStagingSession<'consumer>,
        ProjectionConsumerCatchUpError,
    > {
        if consumer.id() != definition.consumer_id() {
            return Err(SearchProjectionConsumerError::InvalidHandle.into());
        }
        Ok(DefinedProjectionStagingSession {
            session: self.begin_search_projection_consumer_catch_up_in(
                consumer,
                config,
                definition.delta,
                definition.scope.clone(),
            )?,
            definition,
        })
    }
}
