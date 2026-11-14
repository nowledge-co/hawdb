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

pub mod access;
pub mod access_control;
pub mod cancellation;
pub mod capability;
pub mod conversion_function;
pub mod decimal;
pub mod error;
pub mod graph;
pub mod graph_rag;
pub mod histogram;
pub mod ids;
pub mod list_function;
pub mod logical_type;
pub mod numeric;
pub mod numeric_function;
pub mod projection;
pub mod regex;
mod retained_heap;
pub mod schema;
mod shared_allocation;
pub mod sql_timestamp;
pub mod string_function;
pub mod temporal;
pub mod time;
pub mod uuidv7;
pub mod value;
pub mod vector;
pub mod vector_index;

pub use access_control::QueryAccessControlContext;
pub use cancellation::{
    RuntimeCancellationFuture, RuntimeCancellationReason, RuntimeCancellationToken,
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
    RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit, RuntimeMemoryReservation,
    RuntimeMemoryTarget, RuntimeTaskContext,
};
pub use capability::{RuntimeCapabilities, RuntimeCapability};
pub use conversion_function::ConversionFunction;
pub use decimal::{Decimal, DecimalError, DecimalType};
pub use error::{
    HawDBError, ReadBudgetError, ReadBudgetResource, RelationalErrorKind, RelationalErrorSurface,
    ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, Result,
    SearchProjectionEpochAdvanceRefusal, SearchProjectionRebuildRefusal, StorageFormatError,
    TransactionRetryError, TransactionRetryReason, APPEND_WAL_FORMAT, LOGICAL_DUMP_FORMAT,
    RELATIONAL_CHECKPOINT_LAYOUT_FORMAT, RELATIONAL_DURABLE_CODEC_FORMAT,
    RELATIONAL_ROW_PAGE_FORMAT, RELATIONAL_SCHEMA_FORMAT, SEARCH_PROJECTION_MANIFEST_FORMAT,
    STORAGE_MANIFEST_FORMAT,
};
pub use graph::{LabelFunction, RelationshipDirection, RelationshipEndpoint};
pub use graph_rag::{
    build_graph_rag_schema_context, GraphRagCommonPathSummary, GraphRagGeneratedQuery,
    GraphRagLabelSummary, GraphRagPropertySubject, GraphRagPropertySummary, GraphRagQueryBinding,
    GraphRagQueryDraft, GraphRagQueryGenerationError, GraphRagQueryParameterCardinality,
    GraphRagQueryParameterError, GraphRagQueryParameterRequirement, GraphRagQueryPattern,
    GraphRagQueryPredicate, GraphRagQueryPredicateOperator, GraphRagQueryProjection,
    GraphRagRelationshipTypeSummary, GraphRagRouteSummary, GraphRagSchemaContext,
    GraphRagSchemaContextOptions, GraphRagSchemaContextTruncation,
    DEFAULT_GRAPH_RAG_MAX_COMMON_PATHS, DEFAULT_GRAPH_RAG_MAX_LABELS,
    DEFAULT_GRAPH_RAG_MAX_PROPERTIES_PER_SUBJECT, DEFAULT_GRAPH_RAG_MAX_RELATIONSHIP_TYPES,
    DEFAULT_GRAPH_RAG_MAX_ROUTES, GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL, MAX_GRAPH_RAG_QUERY_LIMIT,
};
pub use histogram::{
    CompressedHistogram, HistogramBucket, MostCommonValue, PropertyCardinalityEstimate,
    PropertyEstimateSource,
};
pub use list_function::{
    ListFunction, MAX_LIST_EXPRESSION_WORK, MAX_RANGE_LENGTH, MAX_UNACCOUNTED_LIST_BYTES,
};
pub use logical_type::{LogicalType, VectorDimension};
pub use numeric_function::NumericFunction;
pub use regex::ValidatedRegex;
#[doc(hidden)]
pub use retained_heap::{checked_retained_heap_sum, RetainedHeapBytes};
pub use schema::{
    AdvancedStatisticsFreshness, BasicGraphStatistics, Catalog, CompositeIndexDescriptor,
    ConstraintDescriptor, ConstraintId, ConstraintKind, ConstraintSubject, DeclaredPropertyDefault,
    GraphStatistics, IndexDescriptor, IndexId, IndexKind, IndexStatisticsSample, Label, LabelId,
    PropertyDefault, PropertyDescriptor, PropertyId, PropertySchema, PropertyType, RelType,
    RelTypeId, SchemaObjectState, TableDescriptor, TableId, TableKind,
};
pub use sql_timestamp::TimestampPrecision;
pub use string_function::StringFunction;
pub use temporal::{Duration as TemporalDuration, TemporalFunction, TemporalKind};
pub use uuid::Uuid;
pub use uuidv7::generate_uuidv7;
pub use value::{Value, ValueRef};
pub use vector::{Vector, VectorDistance, VectorRef};
pub use vector_index::{
    VectorIndexBackend, VectorIndexDescriptor, VectorIndexLsg, VectorIndexQueryMode,
    VectorIndexReadIdentity, VectorIndexTarget,
};

#[doc(hidden)]
pub use shared_allocation::SharedAllocation;
