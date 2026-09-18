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

use super::super::super::*;
use super::entities::lookup_entities_via_query_runtime_strict;

pub(in crate::api) fn knowledge_property_batch_via_query_runtime(
    db: &Database,
    request: &KnowledgePropertyBatchRequest,
) -> Result<KnowledgePropertyBatchOutput> {
    knowledge_scoped_property_batch_via_query_runtime(
        db,
        &KnowledgeScopedPropertyBatchRequest {
            projection: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(in crate::api) fn knowledge_scoped_property_batch_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedPropertyBatchRequest,
) -> Result<KnowledgePropertyBatchOutput> {
    let property_names = dedup_property_names(&request.projection.property_names);
    let (graph_commit_epoch, found) =
        lookup_entities_via_query_runtime_strict(db, &request.projection.entities)?;
    Ok(shape_property_batch_output(
        graph_commit_epoch,
        request,
        property_names,
        &found,
    ))
}

fn shape_property_batch_output(
    graph_commit_epoch: u64,
    request: &KnowledgeScopedPropertyBatchRequest,
    property_names: Vec<String>,
    found: &BTreeMap<(String, String), KnowledgeEntity>,
) -> KnowledgePropertyBatchOutput {
    let mut rows = Vec::with_capacity(request.projection.entities.len());
    let mut found_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    for entity_request in &request.projection.entities {
        let key = (
            entity_request.label.clone(),
            entity_request.external_id.clone(),
        );
        let Some(entity) = found.get(&key) else {
            missing_count += 1;
            rows.push(KnowledgePropertyRow {
                entity: entity_request.clone(),
                node_id: None,
                filtered_out: false,
                properties: empty_property_projection(&property_names),
            });
            continue;
        };
        if !request.metadata_filters.is_empty()
            && !knowledge_entity_matches_filters(entity, &request.metadata_filters)
        {
            filtered_out_count += 1;
            rows.push(KnowledgePropertyRow {
                entity: entity_request.clone(),
                node_id: Some(entity.node_id),
                filtered_out: true,
                properties: empty_property_projection(&property_names),
            });
            continue;
        }
        found_count += 1;
        rows.push(KnowledgePropertyRow {
            entity: entity_request.clone(),
            node_id: Some(entity.node_id),
            filtered_out: false,
            properties: project_knowledge_entity_properties(entity, &property_names),
        });
    }
    KnowledgePropertyBatchOutput {
        graph_commit_epoch,
        rows,
        found_count,
        missing_count,
        filtered_out_count,
        property_names,
    }
}
