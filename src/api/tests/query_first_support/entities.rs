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

type EntityKey = (String, String);
pub(in crate::api) type KnowledgeEntityLookup = (u64, BTreeMap<EntityKey, KnowledgeEntity>);

struct EntityLookupQuery {
    label: String,
    cypher: String,
    parameters: BTreeMap<String, Value>,
}

pub(in crate::api) fn knowledge_entity_via_query_runtime(
    db: &Database,
    request: &KnowledgeEntityRequest,
) -> Result<KnowledgeEntityOutput> {
    let output = knowledge_scoped_entity_batch_via_query_runtime(
        db,
        &KnowledgeScopedEntityBatchRequest {
            entities: vec![request.clone()],
            metadata_filters: BTreeMap::new(),
        },
    )?;
    Ok(KnowledgeEntityOutput {
        graph_commit_epoch: output.graph_commit_epoch,
        entity: output.entities.into_iter().next().flatten(),
    })
}

pub(in crate::api) fn knowledge_entity_details_via_query_runtime(
    db: &Database,
    request: &KnowledgeEntityDetailsRequest,
) -> Result<KnowledgeEntityDetailsOutput> {
    if request.external_id.is_empty() {
        return Err(HawDBError::Semantic(
            "knowledge entity details requires a non-empty external_id".to_string(),
        ));
    }
    validate_cypher_identifier(&request.label, "label")?;
    let parameters = BTreeMap::from([(
        "external_id".to_string(),
        Value::String(request.external_id.clone()),
    )]);
    let entity_output = db.query_read_only_with_params_bounded(
        &format!(
            "MATCH (n:{label}) WHERE n.id = $external_id RETURN n AS entity",
            label = request.label,
        ),
        &parameters,
        Some(1),
    )?;
    let Some(entity) = entity_output
        .rows
        .first()
        .and_then(|row| row.get("entity"))
        .and_then(knowledge_entity_from_value)
    else {
        return Ok(KnowledgeEntityDetailsOutput {
            graph_commit_epoch: db.store.commit_epoch(),
            entity: None,
            neighbor_count: 0,
            relationship_count: 0,
        });
    };
    let neighbor_count = db
        .query_read_only_with_params_bounded(
            &format!(
                "MATCH (n:{label}) WHERE n.id = $external_id \
                 OPTIONAL MATCH (n)-[r]-(neighbor) \
                 RETURN COUNT(DISTINCT neighbor) AS neighbor_count",
                label = request.label,
            ),
            &parameters,
            Some(1),
        )?
        .rows
        .first()
        .and_then(|row| row.get("neighbor_count"))
        .and_then(value_to_non_negative_u64)
        .unwrap_or(0);
    let relationship_count = db
        .query_read_only_with_params_bounded(
            &format!(
                "MATCH (n:{label}) WHERE n.id = $external_id \
                 OPTIONAL MATCH (n)-[r]-() \
                 RETURN COUNT(r) AS relationship_count",
                label = request.label,
            ),
            &parameters,
            Some(1),
        )?
        .rows
        .first()
        .and_then(|row| row.get("relationship_count"))
        .and_then(value_to_non_negative_u64)
        .unwrap_or(0);
    Ok(KnowledgeEntityDetailsOutput {
        graph_commit_epoch: db.store.commit_epoch(),
        entity: Some(entity),
        neighbor_count,
        relationship_count,
    })
}

pub(in crate::api) fn knowledge_scoped_entity_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedEntityRequest,
) -> Result<KnowledgeEntityOutput> {
    let output = knowledge_scoped_entity_batch_via_query_runtime(
        db,
        &KnowledgeScopedEntityBatchRequest {
            entities: vec![request.entity.clone()],
            metadata_filters: request.metadata_filters.clone(),
        },
    )?;
    Ok(KnowledgeEntityOutput {
        graph_commit_epoch: output.graph_commit_epoch,
        entity: output.entities.into_iter().next().flatten(),
    })
}

pub(in crate::api) fn knowledge_entity_batch_via_query_runtime(
    db: &Database,
    request: &KnowledgeEntityBatchRequest,
) -> Result<KnowledgeEntityBatchOutput> {
    knowledge_scoped_entity_batch_via_query_runtime(
        db,
        &KnowledgeScopedEntityBatchRequest {
            entities: request.entities.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(in crate::api) fn knowledge_scoped_entity_batch_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedEntityBatchRequest,
) -> Result<KnowledgeEntityBatchOutput> {
    let (graph_commit_epoch, found) =
        lookup_entities_via_query_runtime_strict(db, &request.entities)?;
    Ok(shape_entity_batch_output(
        graph_commit_epoch,
        request,
        &found,
    ))
}

fn shape_entity_batch_output(
    graph_commit_epoch: u64,
    request: &KnowledgeScopedEntityBatchRequest,
    found: &BTreeMap<EntityKey, KnowledgeEntity>,
) -> KnowledgeEntityBatchOutput {
    let mut entities = Vec::with_capacity(request.entities.len());
    let mut found_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    for entity_request in &request.entities {
        let key = (
            entity_request.label.clone(),
            entity_request.external_id.clone(),
        );
        if let Some(entity) = found
            .get(&key)
            .filter(|entity| {
                request.metadata_filters.is_empty()
                    || knowledge_entity_matches_filters(entity, &request.metadata_filters)
            })
            .cloned()
        {
            found_count += 1;
            entities.push(Some(entity));
        } else if found.contains_key(&key) {
            filtered_out_count += 1;
            entities.push(None);
        } else {
            missing_count += 1;
            entities.push(None);
        }
    }

    KnowledgeEntityBatchOutput {
        graph_commit_epoch,
        entities,
        found_count,
        missing_count,
        filtered_out_count,
    }
}

pub(in crate::api) fn lookup_entities_via_query_runtime_strict(
    db: &Database,
    entities: &[KnowledgeEntityRequest],
) -> Result<KnowledgeEntityLookup> {
    let graph_commit_epoch = db.store.commit_epoch();
    let mut found = BTreeMap::new();
    for query in build_entity_lookup_queries(entities) {
        let output =
            db.query_read_only_with_params_bounded(&query.cypher, &query.parameters, None)?;
        decode_entity_lookup_rows(&query.label, &output.rows, &mut found)?;
    }
    Ok((graph_commit_epoch, found))
}

fn build_entity_lookup_queries(entities: &[KnowledgeEntityRequest]) -> Vec<EntityLookupQuery> {
    let mut entities_by_label = BTreeMap::<String, BTreeSet<String>>::new();
    for entity in entities {
        if entity.external_id.is_empty()
            || validate_cypher_identifier(&entity.label, "label").is_err()
        {
            continue;
        }
        entities_by_label
            .entry(entity.label.clone())
            .or_default()
            .insert(entity.external_id.clone());
    }

    entities_by_label
        .into_iter()
        .map(|(label, external_ids)| {
            let node_ids = external_ids
                .iter()
                .filter_map(|external_id| external_id.parse::<i64>().ok())
                .filter(|node_id| *node_id >= 0)
                .map(Value::Int)
                .collect::<Vec<_>>();
            let cypher = format!(
                "MATCH (n:{label}) WHERE n.id IN $external_ids OR id(n) IN $node_ids RETURN n AS entity"
            );
            let parameters = BTreeMap::from([
                (
                    "external_ids".to_string(),
                    Value::List(external_ids.into_iter().map(Value::String).collect()),
                ),
                ("node_ids".to_string(), Value::List(node_ids)),
            ]);
            EntityLookupQuery {
                label,
                cypher,
                parameters,
            }
        })
        .collect()
}

fn decode_entity_lookup_rows(
    label: &str,
    rows: &executor::QueryRows,
    found: &mut BTreeMap<EntityKey, KnowledgeEntity>,
) -> Result<()> {
    for row in rows {
        let entity = row
            .get("entity")
            .and_then(knowledge_entity_from_value)
            .ok_or_else(|| {
                HawDBError::Execution(
                    "knowledge entity lookup returned an invalid entity row".to_string(),
                )
            })?;
        let external_id = entity.external_id.clone().ok_or_else(|| {
            HawDBError::Execution(
                "knowledge entity lookup returned an entity without identity".to_string(),
            )
        })?;
        found.insert((label.to_string(), external_id), entity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_details_reads_entity_and_exact_neighbor_counts() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'details-memory'})\
             -[:MENTIONS]->(:Entity {id: 'details-entity', name: 'Entity'})",
        )
        .unwrap();

        let output = knowledge_entity_details_via_query_runtime(
            &db,
            &KnowledgeEntityDetailsRequest {
                label: "Entity".to_string(),
                external_id: "details-entity".to_string(),
            },
        )
        .unwrap();

        assert_eq!(
            output.entity.unwrap().external_id.as_deref(),
            Some("details-entity")
        );
        assert_eq!(output.neighbor_count, 1);
        assert_eq!(output.relationship_count, 1);
    }

    #[test]
    fn rejects_invalid_query_result_rows() {
        let rows = executor::QueryRows::from(vec![BTreeMap::new()]);
        let error = decode_entity_lookup_rows("Memory", &rows, &mut BTreeMap::new()).unwrap_err();

        assert_eq!(
            error,
            HawDBError::Execution(
                "knowledge entity lookup returned an invalid entity row".to_string()
            )
        );
    }
}
