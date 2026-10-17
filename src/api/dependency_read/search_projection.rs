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

//! Query-owned graph hydration; this does not certify changefeed coverage.

use super::*;
use crate::search::{
    projection_business_label_value_from_properties, projection_row_from_node_with_business_labels,
    SearchProjectionRow,
};

// Graph selection, optional expansion and grouping remain in the query runtime.
// The host only converts each complete row using the Search kernel's shaping.
const NODES: &str = "MATCH (n) WHERE id(n) IN $ids RETURN id(n) AS node_id, properties(n) AS properties, labels(n) AS labels";
const NODES_WITH_LABELS: &str = "MATCH (n) WHERE id(n) IN $ids OPTIONAL MATCH (n)-[:HAS_LABEL]-(l:Label) RETURN id(n) AS node_id, properties(n) AS properties, labels(n) AS labels, COLLECT(DISTINCT l) AS business_labels";

impl PreparedRead {
    /// Hydrates graph Search rows from this pinned source with tracked dependencies.
    /// Missing/non-Search nodes are omitted; repeated IDs return one row per node.
    /// Root IDs are capped at 1024. The statement's configured row/payload/runtime
    /// limits apply in addition to the retained output's UTF-8 payload cap.
    ///
    /// No watermark or batch-coverage authority is returned. The query result and
    /// one candidate shaped row can coexist with admitted retained output; this
    /// is not a total allocation/RSS bound. Qualified internal-ID roots use
    /// admitted point reads; cold segment validation and adjacency work retain
    /// their storage limits rather than a total physical-I/O bound.
    /// Any failure makes `finish` refuse.
    pub fn hydrate_search_projection_nodes(
        &mut self,
        node_ids: &[u64],
        max_output_payload_bytes: NonZeroUsize,
    ) -> Result<Vec<SearchProjectionRow>> {
        let result = self.hydrate_search_nodes_inner(node_ids, max_output_payload_bytes.get());
        self.failed |= result.is_err();
        result
    }

    fn hydrate_search_nodes_inner(
        &mut self,
        node_ids: &[u64],
        output_limit: usize,
    ) -> Result<Vec<SearchProjectionRow>> {
        if node_ids.len() > 1024 {
            return Err(refusal("Search graph hydration root limit exceeded"));
        }
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let values = node_ids
            .iter()
            .map(|id| {
                i64::try_from(*id)
                    .map(Value::Int)
                    .map_err(|_| refusal("Search graph hydration ID is not representable"))
            })
            .collect::<Result<Vec<_>>>()?;
        let has_labels = self.read.catalog.rel_type_id("HAS_LABEL").is_some()
            && self.read.catalog.label_id("Label").is_some();
        if !has_labels {
            self.collect_absent_business_labels(node_ids)?;
        }
        let output = self.query_with_params(
            if has_labels { NODES_WITH_LABELS } else { NODES },
            &BTreeMap::from([("ids".into(), Value::List(values))]),
        )?;
        let mut result = Vec::new();
        let mut payload = 0usize;
        for row in output.output.rows {
            let Some(Value::Int(id)) = row.get("node_id") else {
                return Err(refusal("Search graph hydration lost node identity"));
            };
            let Some(Value::Map(properties)) = row.get("properties") else {
                return Err(refusal("Search graph hydration lost raw properties"));
            };
            let Some(Value::List(names)) = row.get("labels") else {
                return Err(refusal("Search graph hydration lost native labels"));
            };
            let labels = names
                .iter()
                .map(|name| {
                    let Value::String(name) = name else {
                        return Err(refusal("Search graph hydration label is not a string"));
                    };
                    self.read.catalog.label_id(name).ok_or_else(|| {
                        refusal("Search graph hydration label is absent from the captured catalog")
                    })
                })
                .collect::<Result<BTreeSet<_>>>()?;
            let node = NodeRecord {
                id: NodeId(
                    u64::try_from(*id)
                        .map_err(|_| refusal("Search graph hydration node ID is negative"))?,
                ),
                labels,
                properties: properties.clone(),
            };
            let mut business = BTreeSet::new();
            if has_labels {
                let Some(Value::List(values)) = row.get("business_labels") else {
                    return Err(refusal(
                        "Search graph hydration lost business-label collection",
                    ));
                };
                for value in values {
                    let Value::Map(values) = value else {
                        return Err(refusal(
                            "Search graph hydration business label is not a node",
                        ));
                    };
                    // The query already selected/grouped nodes; Search owns
                    // scalar precedence and formatting without copying the map.
                    if let Some(value) = projection_business_label_value_from_properties(values) {
                        business.insert(value);
                    }
                }
            }
            let Some(projected) = projection_row_from_node_with_business_labels(
                &self.read.catalog,
                &node,
                &business.into_iter().collect::<Vec<_>>(),
            ) else {
                continue;
            };
            payload = payload
                .checked_add(row_payload_bytes(&projected)?)
                .ok_or_else(|| refusal("Search graph hydration payload overflow"))?;
            if payload > output_limit {
                return Err(HawDBError::read_budget_exceeded(
                    hawdb_core::ReadBudgetResource::PayloadBytes,
                    output_limit,
                    "complete Search graph hydration exceeds retained payload budget",
                ));
            }
            result.push(projected);
        }
        Ok(result)
    }

    fn collect_absent_business_labels(&mut self, node_ids: &[u64]) -> Result<()> {
        let kind = self.read.catalog.rel_type_id("HAS_LABEL");
        let mut work = 1024;
        for id in node_ids {
            for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
                if let Some(kind) = kind {
                    // Even without Label in the catalog, retain every possible
                    // endpoint before any later label membership can change.
                    self.read.store.collect_prepared_adjacency_dependencies(
                        &mut self.dependencies.graph,
                        NodeId(*id),
                        Some(kind),
                        direction,
                        &mut work,
                    )?;
                } else {
                    // Implicit type creation need not emit a Schema intent. A
                    // future first HAS_LABEL edge must conflict with this empty
                    // directed scope rather than passing as a disjoint write.
                    self.dependencies
                        .graph
                        .record(VersionKey::GraphAdjacency {
                            node_id: NodeId(*id),
                            direction,
                        })
                        .map_err(|error| HawDBError::Execution(error.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

fn row_payload_bytes(row: &SearchProjectionRow) -> Result<usize> {
    let fields = [
        row.kind.as_str(),
        row.external_id.as_str(),
        row.title.as_str(),
        row.body.as_str(),
        row.source_id.as_deref().unwrap_or(""),
    ];
    fields
        .into_iter()
        .map(str::len)
        .chain(
            row.metadata
                .iter()
                .flat_map(|(key, value)| [key.len(), value.len()]),
        )
        .try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| refusal("Search graph hydration payload overflow"))
        })
}

fn refusal(message: &str) -> HawDBError {
    HawDBError::Execution(message.into())
}
