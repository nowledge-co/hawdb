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
use crate::schema::{LabelId, RelTypeId, TableKind};

#[test]
fn context_is_bounded_ranked_and_does_not_render_property_values() {
    let mut catalog = Catalog::default();
    let memory = catalog.get_or_create_label("Memory");
    let entity = catalog.get_or_create_label("Entity");
    let mentions = catalog.get_or_create_rel_type("MENTIONS");
    let memory_table = catalog.get_or_create_table(TableKind::Node, "Memory");
    catalog.get_or_create_property(memory_table, "id", PropertyType::String, false);
    let statistics = GraphStatistics {
        computed_at_commit_epoch: 7,
        label_counts: BTreeMap::from([(memory, 4), (entity, 2)]),
        rel_type_counts: BTreeMap::from([(mentions, 3)]),
        rel_type_source_counts: BTreeMap::from([(mentions, 2)]),
        rel_type_target_counts: BTreeMap::from([(mentions, 2)]),
        path_counts: BTreeMap::from([((memory, mentions, entity), 3)]),
        path_source_distinct_counts: BTreeMap::from([((memory, mentions, entity), 2)]),
        path_target_distinct_counts: BTreeMap::from([((memory, mentions, entity), 2)]),
        bounded_path_counts: BTreeMap::from([
            ((memory, mentions, entity, 1), 3),
            ((memory, mentions, entity, 2), 1),
            ((entity, mentions, entity, 2), 2),
        ]),
        bounded_path_source_distinct_counts: BTreeMap::from([
            ((memory, mentions, entity, 1), 2),
            ((memory, mentions, entity, 2), 1),
            ((entity, mentions, entity, 2), 1),
        ]),
        bounded_path_target_distinct_counts: BTreeMap::from([
            ((memory, mentions, entity, 1), 2),
            ((memory, mentions, entity, 2), 1),
            ((entity, mentions, entity, 2), 1),
        ]),
        property_distinct_counts: BTreeMap::from([
            ((memory, "id".to_string()), 4),
            ((memory, "secret".to_string()), 1),
        ]),
        property_histograms: BTreeMap::from([(
            (memory, "secret".to_string()),
            vec![crate::Value::String("must-not-render".to_string())],
        )]),
        ..GraphStatistics::default()
    };

    let context = build_graph_rag_schema_context(
        &catalog,
        &statistics,
        GraphRagSchemaContextOptions {
            max_properties_per_subject: 1,
            max_common_paths: 1,
            ..GraphRagSchemaContextOptions::default()
        },
    );

    assert_eq!(context.labels[0].name, "Memory");
    assert_eq!(context.properties.len(), 1);
    assert_eq!(context.properties[0].name, "id");
    assert!(context.truncation.properties);
    assert!(context.truncation.common_paths);
    let rendered = context.render_compact_cypher_guidance();
    assert!(rendered.contains("ROUTE (Memory)-[:MENTIONS]->(Entity)"));
    assert!(rendered.contains("identifiers_from_schema_only=true"));
    assert!(!rendered.contains("must-not-render"));
}

#[test]
fn context_ignores_statistics_with_unknown_catalog_tokens() {
    let catalog = Catalog::default();
    let statistics = GraphStatistics {
        label_counts: BTreeMap::from([(LabelId(99), 4)]),
        rel_type_counts: BTreeMap::from([(RelTypeId(99), 3)]),
        path_counts: BTreeMap::from([((LabelId(99), RelTypeId(99), LabelId(100)), 2)]),
        ..GraphStatistics::default()
    };

    let context = build_graph_rag_schema_context(
        &catalog,
        &statistics,
        GraphRagSchemaContextOptions::default(),
    );

    assert!(context.labels.is_empty());
    assert!(context.relationship_types.is_empty());
    assert!(context.routes.is_empty());
}
