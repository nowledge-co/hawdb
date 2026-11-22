// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Complete source capture under the existing registered Consumer publication.

use super::*;
use crate::{
    Database, QueryStreamOptions, SearchIndex, SearchProjectionConsumer,
    SearchProjectionConsumerError, SearchProjectionConsumerId, SearchProjectionConsumerOptions,
};
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext, Value};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use hawdb_search::SearchDocument;
use hawdb_storage::store::{
    ProjectionFeedError, ProjectionFeedLimits, ProjectionFullSourceItem, ProjectionFullSourceLimits,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;

#[cfg(all(test, feature = "vector-search"))]
mod tests;

mod incremental;
pub use incremental::VectorIndexUpdateReport;

#[derive(Debug, Clone)]
pub struct VectorIndexBuildOptions {
    pub distance: VectorDistance,
    pub backend: VectorIndexBackend,
    /// Private source mapping and resident producer envelope, separately funded
    /// from the Consumer publication and retained-reader envelopes.
    /// A larger explicit envelope also raises canonical publication admission;
    /// publication remains a separate host charge and honors the task ceiling.
    pub memory_bytes: NonZeroUsize,
    pub max_documents: NonZeroUsize,
    pub max_row_payload_bytes: NonZeroUsize,
    pub source: ProjectionFullSourceLimits,
    pub feed: ProjectionFeedLimits,
    pub task: RuntimeTaskContext,
}

impl Default for VectorIndexBuildOptions {
    fn default() -> Self {
        Self {
            distance: VectorDistance::Cosine,
            backend: VectorIndexBackend::default(),
            memory_bytes: NonZeroUsize::new(32 << 20).unwrap(),
            max_documents: NonZeroUsize::new(100_000).unwrap(),
            max_row_payload_bytes: NonZeroUsize::new(1 << 20).unwrap(),
            source: ProjectionFullSourceLimits::default(),
            feed: ProjectionFeedLimits {
                work: NonZeroUsize::new(1_000_000).unwrap(),
                items: NonZeroUsize::new(1_000_000).unwrap(),
                bytes: NonZeroUsize::new(64 << 20).unwrap(),
            },
            task: RuntimeTaskContext::default(),
        }
    }
}

#[derive(Debug)]
pub enum VectorIndexBuildError {
    Consumer(SearchProjectionConsumerError),
    Source(ProjectionFeedError),
    Query(HawDBError),
    Admission(hawdb_qos::RuntimeAdmissionError),
}
impl std::fmt::Display for VectorIndexBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Consumer(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::Query(error) => error.fmt(formatter),
            Self::Admission(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for VectorIndexBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Consumer(error) => error,
            Self::Source(error) => error,
            Self::Query(error) => error,
            Self::Admission(error) => error,
        })
    }
}
impl From<HawDBError> for VectorIndexBuildError {
    fn from(error: HawDBError) -> Self {
        Self::Query(error)
    }
}
impl From<ProjectionFeedError> for VectorIndexBuildError {
    fn from(error: ProjectionFeedError) -> Self {
        Self::Source(error)
    }
}

impl Database {
    /// Build one declared field from a complete pinned canonical source and
    /// publish it through the registered Consumer's existing native selector.
    pub fn create_vector_index(
        &mut self,
        name: &str,
        field: &VectorIndexField,
        destination: impl AsRef<Path>,
        consumer_options: SearchProjectionConsumerOptions,
        mut build: VectorIndexBuildOptions,
    ) -> std::result::Result<SearchProjectionConsumer, VectorIndexBuildError> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::VectorSearch)?;
        build.bind_memory_limit()?;
        let id = SearchProjectionConsumerId::new(name)?;
        let mut input_error = None;
        // Keep the producer envelope after initialization through checkpoint,
        // installation and publication, until the admitted reader owns the data.
        let mut producer_memory = None;
        let result = self.create_consumer_with_initializer::<
            { hawdb_search::projection_consumer::MAX_CONSUMERS }, _
        >(id, destination.as_ref(), consumer_options, build.task.clone(), hawdb_search::consumer::RegisteredConsumerProjector::CanonicalVectorField, |source, index, permit| {
            let mapped = (|| {
                producer_memory = Some(permit.reserve_retained_memory(build.memory_bytes.get() as u64)
                    .map_err(VectorIndexBuildError::Admission)?);
                build_source(source, index, name, field, &build, None)
            })();
            mapped.map_err(|error| {
                // Retain the typed input failure across the generic initializer
                // callback and its original-custody cleanup.
                input_error = Some(error);
                HawDBError::Execution("vector index source construction failed".into())
            })
        });
        drop(producer_memory);
        match input_error {
            Some(error) => Err(error),
            None => result.map_err(VectorIndexBuildError::Consumer),
        }
    }

    /// Rebuild the same target and algorithm from a complete current snapshot.
    /// Deletes, updates and transitions to NULL are reflected in the successor;
    /// previously bound read domains retain their own source and generation.
    /// Retryable pre-effect admission and `BranchBusy` handling follow
    /// [`Self::catch_up_vector_index`]; an uncertain publication requires
    /// Consumer recovery, never an unconditional rebuild retry.
    pub fn rebuild_vector_index(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
        mut build: VectorIndexBuildOptions,
    ) -> std::result::Result<(), VectorIndexBuildError> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::VectorSearch)?;
        build.bind_memory_limit()?;
        let mut input_error = None;
        let mut producer_memory = None;
        let mut admission_error = None;
        let result = self.rebuild_consumer_with_initializer(
            consumer,
            build.task.clone(),
            &mut admission_error,
            |source, index, permit, previous| {
                let mapped = (|| {
                    producer_memory = Some(
                        permit
                            .reserve_retained_memory(build.memory_bytes.get() as u64)
                            .map_err(VectorIndexBuildError::Admission)?,
                    );
                    let definition = previous;
                    let field = VectorIndexField::from_target(definition.target());
                    build_source(
                        source,
                        index,
                        definition.name(),
                        &field,
                        &build,
                        Some(definition),
                    )
                })();
                mapped.map_err(|error| {
                    input_error = Some(error);
                    HawDBError::Execution("vector index source reconstruction failed".into())
                })
            },
        );
        drop(producer_memory);
        match (input_error, admission_error) {
            (Some(error), _) => Err(error),
            (None, Some(error)) => Err(VectorIndexBuildError::Admission(error)),
            (None, None) => result.map_err(VectorIndexBuildError::Consumer),
        }
    }
}

fn build_source(
    source: &mut DatabaseReadTransaction,
    index: &mut SearchIndex,
    name: &str,
    field: &VectorIndexField,
    options: &VectorIndexBuildOptions,
    previous: Option<&VectorIndexDescriptor>,
) -> std::result::Result<(), VectorIndexBuildError> {
    let ledger = QueryMemoryLedger::new(options.memory_bytes);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "vector index source",
        options.memory_bytes,
    );
    let _metadata = account.reserve(add(
        options.source.metadata_bytes.get(),
        hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2,
    )?)?;
    let binding = source.resolve_vector_index(name, field, options.distance, options.backend)?;
    if let Some(previous) = previous
        && (binding.target() != previous.target()
            || binding.dimension() != previous.dimension()
            || binding.distance() != previous.distance()
            || binding.backend() != previous.backend())
    {
        return Err(
            invalid("rebuild cannot change the target identity, dimension or algorithm").into(),
        );
    }
    let (statement, _statement_memory) = source_statement(source, &binding, &account)?;
    index.bind_vector_index(binding.clone())?;
    let mut documents = account.reserve(0)?;
    let mut count = 0usize;
    // Field completeness excludes unrelated schema and data domains.
    let (labels, tables) = match binding.target() {
        VectorIndexTarget::NodeProperty { table, .. } => {
            let label = source.catalog.label_id(table).ok_or_else(|| {
                invalid("vector index label is absent from the canonical catalog")
            })?;
            (vec![label], Vec::new())
        }
        VectorIndexTarget::Relational { table, .. } => (Vec::new(), vec![table.clone()]),
    };
    let mut cursor = source.store.capture_projection_source_domains(
        options.feed,
        options.source,
        options.task.clone(),
        Some(labels),
        Some(tables),
    )?;
    let row_bytes = add(
        options
            .max_row_payload_bytes
            .get()
            .checked_mul(2)
            .ok_or_else(overflow)?,
        add(
            options
                .source
                .key_bytes
                .get()
                .checked_mul(16)
                .ok_or_else(overflow)?,
            4096,
        )?,
    )?;
    while !cursor.is_complete() {
        cursor.visit_next(options.feed, |item| {
            checkpoint(&options.task)?;
            let applicable = match (binding.target(), item) {
                (
                    VectorIndexTarget::NodeProperty { .. },
                    ProjectionFullSourceItem::GraphNodeCandidate { .. },
                ) => true,
                (
                    VectorIndexTarget::Relational { table: target, .. },
                    ProjectionFullSourceItem::RelationalKey { table, .. },
                ) => target == table,
                _ => false,
            };
            if !applicable {
                return Ok(());
            }
            let _row_memory = account.reserve(row_bytes)?;
            let task = options
                .task
                .clone()
                .with_memory_reservation(RuntimeMemoryReservation::new(
                    account.available_bytes() as u64,
                    options.max_row_payload_bytes.get() as u64,
                ));
            let limits = QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(options.max_row_payload_bytes.get()),
            };
            match item {
                ProjectionFullSourceItem::GraphNodeCandidate { node_id } => {
                    let node = i64::try_from(node_id)
                        .map_err(|_| invalid("node ID exceeds query integer range"))?;
                    let parameters = BTreeMap::from([("node".into(), Value::Int(node))]);
                    source.query_with_params_streaming_context(
                        &statement,
                        &parameters,
                        limits,
                        &task,
                        |row| {
                            append(
                                index,
                                row.get("embedding"),
                                &format!("n:{node_id:016x}"),
                                &binding,
                                options,
                                &mut documents,
                                &mut count,
                            )
                        },
                    )?;
                }
                ProjectionFullSourceItem::RelationalKey { key, .. } => {
                    let encoded = hawdb_storage::relational::encode_relational_primary_key(key)
                        .map_err(|error| HawDBError::Storage(error.to_string()))?;
                    let mut id = String::with_capacity(add(
                        2,
                        encoded.len().checked_mul(2).ok_or_else(overflow)?,
                    )?);
                    id.push_str("r:");
                    use std::fmt::Write;
                    for byte in encoded {
                        write!(&mut id, "{byte:02x}").expect("String write");
                    }
                    let parameters = key
                        .0
                        .iter()
                        .map(super::super::relational_value_to_query_value)
                        .collect::<Result<Vec<_>>>()?;
                    let result = source.query_sql_with_params_options_context(
                        &statement,
                        &parameters,
                        limits,
                        &task,
                    )?;
                    if result.rows.len() != 1 {
                        return Err(HawDBError::StorageIntegrity(
                            "canonical primary key failed to hydrate its vector row".into(),
                        ));
                    }
                    append(
                        index,
                        result.rows[0].get("embedding"),
                        &id,
                        &binding,
                        options,
                        &mut documents,
                        &mut count,
                    )?;
                }
                _ => unreachable!("source kind was checked before hydration"),
            }
            Ok(())
        })?;
    }
    cursor.finish()?;
    index.finish_canonical_vector_source(source.commit_epoch(), None, Vec::new())?;
    Ok(())
}

fn append(
    index: &mut SearchIndex,
    value: Option<&Value>,
    id: &str,
    binding: &VectorIndexDescriptor,
    options: &VectorIndexBuildOptions,
    memory: &mut QueryMemoryLease,
    count: &mut usize,
) -> Result<()> {
    let vector = match value {
        Some(Value::Null) => return Ok(()),
        Some(Value::Vector(vector)) if vector.dimension() == binding.dimension() => vector,
        _ => {
            return Err(HawDBError::StorageIntegrity(
                "vector source differs from its declared field".into(),
            ))
        }
    };
    binding
        .distance()
        .distance_with_work(vector.as_ref(), vector.as_ref(), || {
            checkpoint(&options.task)
        })?;
    if *count == options.max_documents.get() {
        return Err(hawdb_core::ResourceBudgetError {
            resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
            scope: hawdb_core::ResourceBudgetScope::Operator,
            owner: "vector index documents".into(),
            memory_class: None,
            requested: 1,
            used: *count as u64,
            budget: options.max_documents.get() as u64,
            budget_option: "VectorIndexBuildOptions.max_documents",
            message: "complete vector source exceeds document admission".into(),
        }
        .into());
    }
    // Conservative B-tree node/split allowance plus actual retained payloads.
    memory.grow(add(2048, add(id.len(), vector.encoded().len())?)?)?;
    let embedding = vector.as_ref().coordinates().collect();
    index.upsert(SearchDocument {
        id: id.into(),
        title: String::new(),
        content: String::new(),
        embedding: Some(embedding),
        metadata: BTreeMap::new(),
    })?;
    *count += 1;
    Ok(())
}

fn source_statement(
    source: &DatabaseReadTransaction,
    binding: &VectorIndexDescriptor,
    account: &QueryMemoryAccount,
) -> Result<(String, QueryMemoryLease)> {
    let (table, field) = match binding.target() {
        VectorIndexTarget::Relational { table, column, .. } => (table, column),
        VectorIndexTarget::NodeProperty {
            table, property, ..
        } => (table, property),
    };
    let mut size = add(
        256,
        add(table.len(), field.len())?
            .checked_mul(2)
            .ok_or_else(overflow)?,
    )?;
    let schema = match binding.target() {
        VectorIndexTarget::Relational { .. } => Some(
            source
                .store
                .relational_state()
                .table_schema(table)
                .ok_or_else(|| invalid("bound relational table disappeared"))?,
        ),
        _ => None,
    };
    if let Some(schema) = schema {
        for key in &schema.primary_key {
            size = add(
                size,
                add(32, key.len().checked_mul(2).ok_or_else(overflow)?)?,
            )?;
        }
    }
    let memory = account.reserve(size.checked_mul(3).ok_or_else(overflow)?)?;
    let mut statement = String::with_capacity(size);
    use std::fmt::Write;
    if let Some(schema) = schema {
        write!(
            &mut statement,
            "SELECT {} AS embedding FROM {} WHERE ",
            sql_name(field),
            sql_name(table)
        )
        .expect("String write");
        for (ordinal, key) in schema.primary_key.iter().enumerate() {
            if ordinal != 0 {
                statement.push_str(" AND ");
            }
            write!(&mut statement, "{} = ${}", sql_name(key), ordinal + 1).expect("String write");
        }
    } else {
        write!(
            &mut statement,
            "MATCH (n:{}) WHERE id(n) = $node RETURN n.{} AS embedding",
            cypher_name(table)?,
            cypher_name(field)?
        )
        .expect("String write");
    }
    if statement.len() > size {
        return Err(overflow());
    }
    Ok((statement, memory))
}

fn sql_name(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn cypher_name(name: &str) -> Result<&str> {
    let mut characters = name.chars();
    if !characters
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        || !characters.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(invalid(
            "target name cannot be expressed by the Cypher identifier grammar",
        ));
    }
    Ok(name)
}
fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(overflow)
}
fn overflow() -> HawDBError {
    HawDBError::Execution("vector source admission size overflow".into())
}
fn checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)
}

impl VectorIndexBuildOptions {
    fn bind_memory_limit(&mut self) -> Result<()> {
        if self.task.has_resource_controllers() {
            return Err(HawDBError::Semantic(
                "vector index build requires an unbound task".into(),
            ));
        }
        let requested = RuntimeMemoryReservation::new(
            self.memory_bytes.get() as u64,
            self.memory_bytes.get() as u64,
        );
        let effective = self
            .task
            .memory_reservation()
            .map_or(requested, |parent| requested.intersect(parent));
        self.memory_bytes =
            NonZeroUsize::new(usize::try_from(effective.memory_bytes()).map_err(|_| overflow())?)
                .ok_or_else(|| HawDBError::Semantic("vector build has no working memory".into()))?;
        self.task = self.task.clone().with_memory_reservation(effective);
        Ok(())
    }
}
