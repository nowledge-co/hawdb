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

use crate::schema::{Catalog, GraphStatistics, PropertyType, SchemaObjectState, TableKind};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

mod fingerprint;
mod query_generation;
mod topology;

use fingerprint::schema_context_fingerprint;
pub use query_generation::{
    GraphRagGeneratedQuery, GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryGenerationError,
    GraphRagQueryParameterCardinality, GraphRagQueryParameterError,
    GraphRagQueryParameterRequirement, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, MAX_GRAPH_RAG_QUERY_LIMIT,
};
use topology::{common_path_summaries, route_summaries};

pub const GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL: &str = "hawdb-graph-rag-schema-context-v1";
pub const DEFAULT_GRAPH_RAG_MAX_LABELS: usize = 32;
pub const DEFAULT_GRAPH_RAG_MAX_RELATIONSHIP_TYPES: usize = 32;
pub const DEFAULT_GRAPH_RAG_MAX_PROPERTIES_PER_SUBJECT: usize = 12;
pub const DEFAULT_GRAPH_RAG_MAX_ROUTES: usize = 64;
pub const DEFAULT_GRAPH_RAG_MAX_COMMON_PATHS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphRagSchemaContextOptions {
    pub max_labels: usize,
    pub max_relationship_types: usize,
    pub max_properties_per_subject: usize,
    pub max_routes: usize,
    pub max_common_paths: usize,
}

impl Default for GraphRagSchemaContextOptions {
    fn default() -> Self {
        Self {
            max_labels: DEFAULT_GRAPH_RAG_MAX_LABELS,
            max_relationship_types: DEFAULT_GRAPH_RAG_MAX_RELATIONSHIP_TYPES,
            max_properties_per_subject: DEFAULT_GRAPH_RAG_MAX_PROPERTIES_PER_SUBJECT,
            max_routes: DEFAULT_GRAPH_RAG_MAX_ROUTES,
            max_common_paths: DEFAULT_GRAPH_RAG_MAX_COMMON_PATHS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagLabelSummary {
    pub name: String,
    pub node_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagRelationshipTypeSummary {
    pub name: String,
    pub relationship_count: u64,
    pub distinct_source_count: u64,
    pub distinct_target_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphRagPropertySubject {
    Node,
    Relationship,
}

impl GraphRagPropertySubject {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagPropertySummary {
    pub subject: GraphRagPropertySubject,
    pub subject_name: String,
    pub name: String,
    pub value_type: PropertyType,
    pub nullable: bool,
    pub declared: bool,
    pub distinct_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagRouteSummary {
    pub source_label: String,
    pub relationship_type: String,
    pub target_label: String,
    pub observed_count: u64,
    pub distinct_source_count: u64,
    pub distinct_target_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagCommonPathSummary {
    pub source_label: String,
    pub relationship_type: String,
    pub target_label: String,
    pub hops: usize,
    pub observed_count: u64,
    pub distinct_source_count: u64,
    pub distinct_target_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphRagSchemaContextTruncation {
    pub labels: bool,
    pub relationship_types: bool,
    pub properties: bool,
    pub routes: bool,
    pub common_paths: bool,
}

impl GraphRagSchemaContextTruncation {
    pub const fn any(self) -> bool {
        self.labels
            || self.relationship_types
            || self.properties
            || self.routes
            || self.common_paths
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagSchemaContext {
    pub protocol: &'static str,
    pub computed_at_commit_epoch: u64,
    pub fingerprint: u64,
    pub labels: Vec<GraphRagLabelSummary>,
    pub relationship_types: Vec<GraphRagRelationshipTypeSummary>,
    pub properties: Vec<GraphRagPropertySummary>,
    pub routes: Vec<GraphRagRouteSummary>,
    pub common_paths: Vec<GraphRagCommonPathSummary>,
    pub truncation: GraphRagSchemaContextTruncation,
}

impl GraphRagSchemaContext {
    pub fn generate_query(
        &self,
        draft: &GraphRagQueryDraft,
    ) -> Result<GraphRagGeneratedQuery, GraphRagQueryGenerationError> {
        query_generation::generate_query(self, draft)
    }

    pub fn render_compact_cypher_guidance(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(
            output,
            "SCHEMA protocol={} epoch={} fingerprint={:016x}",
            self.protocol, self.computed_at_commit_epoch, self.fingerprint
        );
        output.push_str(
            "RULES read_only=true parameterize_values=true identifiers_from_schema_only=true max_hops=2\n",
        );
        for label in &self.labels {
            let _ = writeln!(output, "LABEL {} count={}", label.name, label.node_count);
        }
        for relationship in &self.relationship_types {
            let _ = writeln!(
                output,
                "RELATIONSHIP_TYPE {} count={} sources={} targets={}",
                relationship.name,
                relationship.relationship_count,
                relationship.distinct_source_count,
                relationship.distinct_target_count
            );
        }
        for property in &self.properties {
            let _ = writeln!(
                output,
                "PROPERTY {} {}.{} type={} nullable={} declared={} distinct={}",
                property.subject.as_str(),
                property.subject_name,
                property.name,
                property_type_name(property.value_type),
                property.nullable,
                property.declared,
                optional_count(property.distinct_count)
            );
        }
        for route in &self.routes {
            let _ = writeln!(
                output,
                "ROUTE ({})-[:{}]->({}) count={} sources={} targets={}",
                route.source_label,
                route.relationship_type,
                route.target_label,
                route.observed_count,
                route.distinct_source_count,
                route.distinct_target_count
            );
        }
        for path in &self.common_paths {
            let _ = writeln!(
                output,
                "COMMON_PATH ({})-[:{}*{}]->({}) count={} sources={} targets={}",
                path.source_label,
                path.relationship_type,
                path.hops,
                path.target_label,
                path.observed_count,
                path.distinct_source_count,
                path.distinct_target_count
            );
        }
        if self.truncation.any() {
            let _ = writeln!(
                output,
                "TRUNCATED labels={} relationship_types={} properties={} routes={} common_paths={}",
                self.truncation.labels,
                self.truncation.relationship_types,
                self.truncation.properties,
                self.truncation.routes,
                self.truncation.common_paths
            );
        }
        output
    }
}

pub fn build_graph_rag_schema_context(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    options: GraphRagSchemaContextOptions,
) -> GraphRagSchemaContext {
    let mut labels = catalog
        .labels()
        .map(|label| GraphRagLabelSummary {
            name: label.name.clone(),
            node_count: statistics
                .label_counts
                .get(&label.id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    labels.sort_by(|left, right| {
        right
            .node_count
            .cmp(&left.node_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    let labels_truncated = truncate(&mut labels, options.max_labels);
    let selected_labels = labels
        .iter()
        .map(|label| label.name.clone())
        .collect::<BTreeSet<_>>();

    let mut relationship_types = catalog
        .rel_types()
        .map(|rel_type| GraphRagRelationshipTypeSummary {
            name: rel_type.name.clone(),
            relationship_count: statistics
                .rel_type_counts
                .get(&rel_type.id)
                .copied()
                .unwrap_or_default(),
            distinct_source_count: statistics
                .rel_type_source_counts
                .get(&rel_type.id)
                .copied()
                .unwrap_or_default(),
            distinct_target_count: statistics
                .rel_type_target_counts
                .get(&rel_type.id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    relationship_types.sort_by(|left, right| {
        right
            .relationship_count
            .cmp(&left.relationship_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    let relationship_types_truncated =
        truncate(&mut relationship_types, options.max_relationship_types);
    let selected_relationship_types = relationship_types
        .iter()
        .map(|rel_type| rel_type.name.clone())
        .collect::<BTreeSet<_>>();

    let (properties, properties_truncated) = property_summaries(
        catalog,
        statistics,
        &selected_labels,
        &selected_relationship_types,
        options.max_properties_per_subject,
    );
    let (routes, routes_truncated) = route_summaries(
        catalog,
        statistics,
        &selected_labels,
        &selected_relationship_types,
        options.max_routes,
    );
    let (common_paths, common_paths_truncated) = common_path_summaries(
        catalog,
        statistics,
        &selected_labels,
        &selected_relationship_types,
        options.max_common_paths,
    );

    let mut context = GraphRagSchemaContext {
        protocol: GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL,
        computed_at_commit_epoch: statistics.computed_at_commit_epoch,
        fingerprint: 0,
        labels,
        relationship_types,
        properties,
        routes,
        common_paths,
        truncation: GraphRagSchemaContextTruncation {
            labels: labels_truncated,
            relationship_types: relationship_types_truncated,
            properties: properties_truncated,
            routes: routes_truncated,
            common_paths: common_paths_truncated,
        },
    };
    context.fingerprint = schema_context_fingerprint(&context);
    context
}

fn property_summaries(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    selected_labels: &BTreeSet<String>,
    selected_relationship_types: &BTreeSet<String>,
    max_properties_per_subject: usize,
) -> (Vec<GraphRagPropertySummary>, bool) {
    let public_tables = catalog
        .table_descriptors()
        .filter(|table| table.state == SchemaObjectState::Public)
        .map(|table| (table.id, (table.kind, table.name.clone())))
        .collect::<BTreeMap<_, _>>();
    let mut summaries =
        BTreeMap::<(GraphRagPropertySubject, String, String), GraphRagPropertySummary>::new();

    for property in catalog
        .property_descriptors()
        .filter(|property| property.state == SchemaObjectState::Public)
    {
        let Some((table_kind, table_name)) = public_tables.get(&property.table_id) else {
            continue;
        };
        let subject = match table_kind {
            TableKind::Node if selected_labels.contains(table_name) => {
                GraphRagPropertySubject::Node
            }
            TableKind::Relationship if selected_relationship_types.contains(table_name) => {
                GraphRagPropertySubject::Relationship
            }
            _ => continue,
        };
        let distinct_count =
            property_distinct_count(catalog, statistics, subject, table_name, &property.name);
        summaries.insert(
            (subject, table_name.clone(), property.name.clone()),
            GraphRagPropertySummary {
                subject,
                subject_name: table_name.clone(),
                name: property.name.clone(),
                value_type: property.value_type,
                nullable: property.nullable,
                declared: true,
                distinct_count,
            },
        );
    }

    for ((label_id, property), distinct_count) in &statistics.property_distinct_counts {
        let Some(label) = catalog.label_name(*label_id) else {
            continue;
        };
        if !selected_labels.contains(label) {
            continue;
        }
        summaries
            .entry((
                GraphRagPropertySubject::Node,
                label.to_string(),
                property.clone(),
            ))
            .or_insert_with(|| GraphRagPropertySummary {
                subject: GraphRagPropertySubject::Node,
                subject_name: label.to_string(),
                name: property.clone(),
                value_type: PropertyType::Any,
                nullable: true,
                declared: false,
                distinct_count: Some(*distinct_count),
            });
    }
    for ((rel_type_id, property), distinct_count) in &statistics.rel_property_distinct_counts {
        let Some(rel_type) = catalog.rel_type_name(*rel_type_id) else {
            continue;
        };
        if !selected_relationship_types.contains(rel_type) {
            continue;
        }
        summaries
            .entry((
                GraphRagPropertySubject::Relationship,
                rel_type.to_string(),
                property.clone(),
            ))
            .or_insert_with(|| GraphRagPropertySummary {
                subject: GraphRagPropertySubject::Relationship,
                subject_name: rel_type.to_string(),
                name: property.clone(),
                value_type: PropertyType::Any,
                nullable: true,
                declared: false,
                distinct_count: Some(*distinct_count),
            });
    }

    let mut grouped =
        BTreeMap::<(GraphRagPropertySubject, String), Vec<GraphRagPropertySummary>>::new();
    for summary in summaries.into_values() {
        grouped
            .entry((summary.subject, summary.subject_name.clone()))
            .or_default()
            .push(summary);
    }
    let mut output = Vec::new();
    let mut truncated = false;
    for properties in grouped.values_mut() {
        properties.sort_by(|left, right| {
            right
                .declared
                .cmp(&left.declared)
                .then_with(|| left.name.cmp(&right.name))
        });
        truncated |= truncate(properties, max_properties_per_subject);
        output.append(properties);
    }
    (output, truncated)
}

fn property_distinct_count(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    subject: GraphRagPropertySubject,
    subject_name: &str,
    property: &str,
) -> Option<u64> {
    match subject {
        GraphRagPropertySubject::Node => catalog.label_id(subject_name).and_then(|label_id| {
            statistics
                .property_distinct_counts
                .get(&(label_id, property.to_string()))
                .copied()
        }),
        GraphRagPropertySubject::Relationship => {
            catalog.rel_type_id(subject_name).and_then(|rel_type_id| {
                statistics
                    .rel_property_distinct_counts
                    .get(&(rel_type_id, property.to_string()))
                    .copied()
            })
        }
    }
}

fn truncate<T>(values: &mut Vec<T>, limit: usize) -> bool {
    let truncated = values.len() > limit;
    values.truncate(limit);
    truncated
}

const fn property_type_name(value_type: PropertyType) -> &'static str {
    match value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::Text => "text",
        PropertyType::List => "list",
    }
}

fn optional_count(count: Option<u64>) -> String {
    count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod query_generation_tests;
#[cfg(test)]
mod tests;
