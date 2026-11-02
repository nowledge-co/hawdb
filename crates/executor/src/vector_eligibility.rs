// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One admitted, complete eligible domain shared by relational and graph reads.

use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{
    HawDBError, Result, RuntimeTaskContext, VectorIndexDescriptor, VectorIndexQueryMode, VectorRef,
};
use std::collections::BTreeMap;

/// Counts input rows separately from unique non-NULL vector identities. An
/// upstream LIMIT is applied before this boundary, including duplicate rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorEligibilityReport {
    pub input_rows: usize,
    pub work_units: usize,
    pub max_work: usize,
    pub source_strategy: &'static str,
    pub eligible_vectors: usize,
    pub scored_vectors: usize,
    pub complete: bool,
}

#[derive(Debug)]
pub struct VectorEligibleDomain<'a> {
    definition: &'a VectorIndexDescriptor,
    identity: hawdb_core::VectorIndexReadIdentity,
    embedding: VectorRef<'a>,
    mode: VectorIndexQueryMode,
    entries: BTreeMap<String, Option<f64>>,
    report: VectorEligibilityReport,
    task: RuntimeTaskContext,
    // Payloads are destroyed before their original allocation owner.
    memory: QueryMemoryLease,
}

impl<'a> VectorEligibleDomain<'a> {
    pub fn new(
        definition: &'a VectorIndexDescriptor,
        identity: hawdb_core::VectorIndexReadIdentity,
        embedding: VectorRef<'a>,
        mode: VectorIndexQueryMode,
        max_work: usize,
        account: &QueryMemoryAccount,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        if !mode.requires_eligible_domain() {
            return Err(HawDBError::Semantic(
                "ann_window does not accept an eligible domain".into(),
            ));
        }
        if embedding.dimension() != definition.dimension() {
            return Err(HawDBError::Semantic(
                "vector query dimension differs from the bound index".into(),
            ));
        }
        // Validate even an empty eligible domain, without allocating coordinates.
        definition
            .distance()
            .distance_with_work(embedding, embedding, || {
                crate::pipeline::runtime_checkpoint(task)
            })?;
        let memory = account.reserve(std::mem::size_of::<Self>().saturating_add(256))?;
        Ok(Self {
            definition,
            identity,
            embedding,
            mode,
            entries: BTreeMap::new(),
            report: VectorEligibilityReport {
                max_work,
                source_strategy: "bounded_canonical",
                ..Default::default()
            },
            task: task.cloned().unwrap_or_default().with_work_limit(max_work),
            memory,
        })
    }

    pub fn observe_input(&mut self) -> Result<()> {
        if self.report.complete {
            return Err(HawDBError::Execution(
                "vector eligible domain is already complete".into(),
            ));
        }
        self.task.charge_work(1)?;
        self.report.input_rows += 1;
        Ok(())
    }

    /// The caller supplies the identity and vector from the same canonical row.
    /// Retain identities and, in exact mode only, scores. Canonical payloads
    /// remain with their reader; filtered ANN must not score the whole domain.
    pub fn insert(&mut self, identity: &str, vector: VectorRef<'_>) -> Result<()> {
        crate::pipeline::runtime_checkpoint(Some(&self.task))?;
        if self.report.complete {
            return Err(HawDBError::Execution(
                "vector eligible domain is already complete".into(),
            ));
        }
        if self.entries.contains_key(identity) {
            return Ok(());
        }
        if vector.dimension() != self.definition.dimension() {
            return Err(HawDBError::StorageIntegrity(
                "eligible vector dimension differs from the bound index".into(),
            ));
        }
        let distance =
            if self.mode == VectorIndexQueryMode::Exact {
                Some(self.definition.distance().distance_with_work(
                    self.embedding,
                    vector,
                    || crate::pipeline::runtime_checkpoint(Some(&self.task)),
                )?)
            } else {
                None
            };
        // Conservative B-tree node/split bookkeeping is admitted before insertion.
        self.memory.grow(identity.len().saturating_add(256))?;
        self.entries.insert(identity.to_owned(), distance);
        self.report.eligible_vectors += 1;
        self.report.scored_vectors += usize::from(distance.is_some());
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        crate::pipeline::runtime_checkpoint(Some(&self.task))?;
        self.task.check_work_limit()?;
        self.report.complete = true;
        Ok(())
    }

    pub fn validate(
        &self,
        definition: &VectorIndexDescriptor,
        embedding: &[f32],
        mode: VectorIndexQueryMode,
    ) -> Result<()> {
        crate::pipeline::runtime_checkpoint(Some(&self.task))?;
        self.task.check_work_limit()?;
        if !self.report.complete
            || self.mode != mode
            || self.definition != definition
            || !self.embedding.coordinates().eq(embedding.iter().copied())
        {
            return Err(HawDBError::Execution(
                "vector eligible domain differs from the completed query binding".into(),
            ));
        }
        Ok(())
    }

    pub fn identity(&self) -> hawdb_core::VectorIndexReadIdentity {
        self.identity
    }

    pub fn task(&self) -> &RuntimeTaskContext {
        &self.task
    }

    pub fn report(&self) -> VectorEligibilityReport {
        VectorEligibilityReport {
            work_units: self.task.work_units(),
            ..self.report
        }
    }

    pub fn find_identity(&self, identity: &str) -> Option<&str> {
        self.entries
            .get_key_value(identity)
            .map(|(key, _)| key.as_str())
    }

    pub fn distances(&self) -> impl Iterator<Item = Result<(&str, f64)>> {
        self.entries.iter().map(|(id, distance)| {
            distance
                .map(|distance| (id.as_str(), distance))
                .ok_or_else(|| HawDBError::Execution("eligible identity has no exact score".into()))
        })
    }
}
