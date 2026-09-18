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

use hawdb::{
    Database, ExternalShadowProjectGraphReply, ExternalShadowProjectGraphRequest,
    ExternalShadowProtocolBackend, ExternalShadowProtocolServer, ExternalShadowStatementRequest,
    QueryOutput, Result,
};
use std::io::{self, BufReader};

fn main() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut server = ExternalShadowProtocolServer::new(SelfShadowBackend::default());
    server.run_json_lines(BufReader::new(stdin.lock()), stdout.lock())
}

#[derive(Default)]
struct SelfShadowBackend {
    db: Database,
}

impl ExternalShadowProtocolBackend for SelfShadowBackend {
    fn engine_kind(&self) -> &'static str {
        "protocol_smoke"
    }

    fn execute(&mut self, statement: ExternalShadowStatementRequest) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }

    fn execute_session(
        &mut self,
        statements: Vec<ExternalShadowStatementRequest>,
    ) -> Result<Vec<QueryOutput>> {
        let mut session = self.db.session();
        statements
            .into_iter()
            .map(|statement| session.query_with_params(&statement.cypher, &statement.parameters))
            .collect()
    }

    fn project_graph(
        &mut self,
        request: ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        let graph = self.db.project_graph(request.rel_type.as_deref());
        let page_rank_scores = graph
            .page_rank(Default::default())
            .into_iter()
            .map(|score| serde_json::json!([score.node.0, score.score]))
            .collect::<Vec<_>>();
        let page_rank_top_node = page_rank_scores
            .first()
            .and_then(|score| score.as_array())
            .and_then(|score| score.first())
            .and_then(serde_json::Value::as_u64);

        Ok(ExternalShadowProjectGraphReply::Ok(serde_json::json!({
            "node_count": graph.node_count(),
            "edge_count": graph.edge_count(),
            "incoming": request
                .expected_incoming_nodes
                .into_iter()
                .map(|node| {
                    let sources = graph
                        .incoming_sources(hawdb::store::NodeId(node))
                        .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                        .unwrap_or_default();
                    serde_json::json!([node, sources])
                })
                .collect::<Vec<_>>(),
            "communities": if request.include_communities {
                graph
                    .louvain_communities(Default::default())
                    .into_iter()
                    .map(|assignment| serde_json::json!([assignment.node.0, assignment.community.0]))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            },
            "hierarchical_communities": if request.include_hierarchical_communities {
                graph
                    .hierarchical_louvain_communities(Default::default())
                    .into_iter()
                    .map(|assignment| {
                        serde_json::json!([
                            assignment.level,
                            assignment.node.0,
                            assignment.community.0
                        ])
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            },
            "page_rank_scores": page_rank_scores,
            "page_rank_top_node": page_rank_top_node,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::EXTERNAL_SHADOW_PROTOCOL_VERSION;

    #[test]
    fn rejects_missing_protocol_version() {
        let mut server = ExternalShadowProtocolServer::new(SelfShadowBackend::default());
        let response = server.handle_request(&serde_json::json!({
            "op": "execute",
            "cypher": "RETURN 1 AS value",
            "parameters": {}
        }));

        assert_eq!(response["error"]["class"], "execution");
        assert_eq!(
            response["error"]["message"],
            "shadow request missing protocol_version"
        );
    }

    #[test]
    fn rejects_unsupported_protocol_version() {
        let mut server = ExternalShadowProtocolServer::new(SelfShadowBackend::default());
        let response = server.handle_request(&serde_json::json!({
            "protocol_version": 2,
            "op": "execute",
            "cypher": "RETURN 1 AS value",
            "parameters": {}
        }));

        assert_eq!(response["error"]["class"], "execution");
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unsupported shadow protocol version 2"));
    }

    #[test]
    fn accepts_current_protocol_version() {
        let mut server = ExternalShadowProtocolServer::new(SelfShadowBackend::default());
        let response = server.handle_request(&serde_json::json!({
            "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "execute",
            "cypher": "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
            "parameters": {}
        }));

        assert_eq!(
            response["ok"]["rows"],
            serde_json::json!([{ "node_id": 0 }])
        );
    }

    #[test]
    fn reports_ready_capabilities() {
        let mut server = ExternalShadowProtocolServer::new(SelfShadowBackend::default());
        let response = server.handle_request(&serde_json::json!({
            "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "ready"
        }));

        assert_eq!(
            response["ok"]["protocol_version"],
            EXTERNAL_SHADOW_PROTOCOL_VERSION
        );
        assert_eq!(
            response["ok"]["capabilities"],
            serde_json::json!(["execute", "execute_session", "project_graph"])
        );
        assert_eq!(response["ok"]["engine_kind"], "protocol_smoke");
    }
}
