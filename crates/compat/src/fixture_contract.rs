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

use crate::{
    CompatibilityCheck, CompatibilityFixture, CypherExecutionMode, CypherFixtureStatement,
    ExpectedErrorClass, ExpectedRows, ProjectedGraphFixtureCheck,
};
use hawdb_core::Value;
use std::collections::BTreeMap;

pub fn nowledge_fixture_contract_usage() -> String {
    "nowledge-fixture-contract requires [nowledge-memory-core]".to_string()
}

pub fn nowledge_fixture_contract_json(fixture: &CompatibilityFixture) -> serde_json::Value {
    serde_json::json!({
        "protocol": "hawdb-nowledge-fixture-contract",
        "protocol_version": 1,
        "fixture": fixture.name,
        "setup_count": fixture.setup.len(),
        "check_count": fixture.checks.len(),
        "setup": fixture
            .setup
            .iter()
            .enumerate()
            .map(|(index, statement)| fixture_statement_contract_json(index, statement))
            .collect::<Vec<_>>(),
        "checks": fixture
            .checks
            .iter()
            .enumerate()
            .map(|(index, check)| fixture_check_contract_json(index, check))
            .collect::<Vec<_>>(),
        "command_bridge": {
            "adapter_example": "cargo run --example nowledge_previous_wrapper_shadow_adapter -- --command <program> [args...]",
            "query_request_shape": {
                "op": "query",
                "cypher": "<cypher>",
                "parameters": {}
            },
            "query_response_shape": {
                "rows": []
            },
            "session_request_shape": {
                "op": "execute_session",
                "statements": []
            },
            "session_response_shape": {
                "results": []
            },
            "project_graph_response_shapes": [
                {
                    "primary_only": true,
                    "reason": "<reason>"
                },
                {
                    "ok": {}
                }
            ]
        }
    })
}

fn fixture_check_contract_json(index: usize, check: &CompatibilityCheck) -> serde_json::Value {
    match check {
        CompatibilityCheck::Cypher(check) => serde_json::json!({
            "index": index,
            "kind": "cypher",
            "name": check.name,
            "execution_mode": cypher_execution_mode_json(check.execution_mode),
            "setup": check
                .setup_queries
                .iter()
                .enumerate()
                .map(|(setup_index, statement)| fixture_statement_contract_json(setup_index, statement))
                .collect::<Vec<_>>(),
            "statement": fixture_statement_contract_json(0, &check.statement),
            "expected_rows": expected_rows_contract_json(&check.expected_rows),
            "expected_error": check.expected_error.map(expected_error_contract_json),
            "effect": check
                .effect_query
                .as_ref()
                .zip(check.effect_expected_rows.as_ref())
                .map(|(statement, expected_rows)| serde_json::json!({
                    "statement": fixture_statement_contract_json(0, statement),
                    "expected_rows": expected_rows_contract_json(expected_rows),
                })),
            "expected_plan_contains": check.expected_plan_contains,
            "tolerance": {
                "float_abs": check.tolerance.float_abs,
            },
        }),
        CompatibilityCheck::ProjectedGraph(check) => serde_json::json!({
            "index": index,
            "kind": "projected_graph",
            "name": check.name,
            "request": {
                "op": "project_graph",
                "rel_type": check.rel_type,
                "expected_incoming_nodes": check
                    .expected_incoming
                    .iter()
                    .map(|(node_id, _)| *node_id)
                    .collect::<Vec<_>>(),
                "include_communities": !check.expected_communities.is_empty(),
                "include_hierarchical_communities": !check.expected_hierarchical_communities.is_empty(),
            },
            "expected_projected_graph": projected_graph_expected_contract_json(check),
            "tolerance": {
                "float_abs": check.tolerance.float_abs,
            },
        }),
    }
}

fn fixture_statement_contract_json(
    index: usize,
    statement: &CypherFixtureStatement,
) -> serde_json::Value {
    serde_json::json!({
        "index": index,
        "cypher": statement.cypher,
        "parameters": parameters_json(&statement.parameters),
        "access": statement_access(&statement.cypher),
        "command_request": {
            "op": "query",
            "cypher": statement.cypher,
            "parameters": parameters_json(&statement.parameters),
        }
    })
}

fn expected_rows_contract_json(expected_rows: &ExpectedRows) -> serde_json::Value {
    match expected_rows {
        ExpectedRows::Exact(rows) => serde_json::json!({
            "kind": "exact",
            "rows": rows_contract_json(rows),
        }),
        ExpectedRows::Unordered(rows) => serde_json::json!({
            "kind": "unordered",
            "rows": rows_contract_json(rows),
        }),
        ExpectedRows::RowCount(count) => serde_json::json!({
            "kind": "row_count",
            "count": count,
        }),
    }
}

fn rows_contract_json(rows: &[BTreeMap<String, Value>]) -> serde_json::Value {
    serde_json::Value::Array(
        rows.iter()
            .map(|row| {
                serde_json::Value::Object(
                    row.iter()
                        .map(|(key, value)| (key.clone(), value_json(value)))
                        .collect(),
                )
            })
            .collect(),
    )
}

fn projected_graph_expected_contract_json(check: &ProjectedGraphFixtureCheck) -> serde_json::Value {
    serde_json::json!({
        "node_count": check.expected_node_count,
        "edge_count": check.expected_edge_count,
        "incoming": check.expected_incoming,
        "communities": check.expected_communities,
        "hierarchical_communities": check.expected_hierarchical_communities,
        "page_rank_scores": check.expected_page_rank_scores,
        "page_rank_top_node": check.page_rank_top_node,
    })
}

fn cypher_execution_mode_json(mode: CypherExecutionMode) -> &'static str {
    match mode {
        CypherExecutionMode::Database => "database",
        CypherExecutionMode::Session => "session",
    }
}

fn expected_error_contract_json(error: ExpectedErrorClass) -> &'static str {
    match error {
        ExpectedErrorClass::Parse => "parse",
        ExpectedErrorClass::Semantic => "semantic",
        ExpectedErrorClass::Storage => "storage",
        ExpectedErrorClass::Execution => "execution",
        ExpectedErrorClass::CapabilityUnavailable => "capability_unavailable",
    }
}

fn statement_access(cypher: &str) -> &'static str {
    let upper = cypher.to_ascii_uppercase();
    if upper.contains(" CREATE ")
        || upper.starts_with("CREATE ")
        || upper.contains(" MERGE ")
        || upper.starts_with("MERGE ")
        || upper.contains(" SET ")
        || upper.starts_with("SET ")
        || upper.contains(" DELETE ")
        || upper.starts_with("DELETE ")
        || upper.contains(" DETACH DELETE ")
        || upper.contains(" ON CREATE SET ")
        || upper.contains(" ON MATCH SET ")
        || upper.contains("DROP ")
    {
        "mutation"
    } else {
        "read"
    }
}

fn parameters_json(parameters: &BTreeMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), value_json(value)))
            .collect(),
    )
}

fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(value),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::Binary(value) => serde_json::json!({
            "$binary": value.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        Value::Uuid(value) => serde_json::json!({ "$uuid": value.to_string() }),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(value_json).collect::<Vec<_>>())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::nowledge_fixture_contract_json;

    #[test]
    fn renders_nowledge_fixture_contract_for_wrapper_shims() {
        let fixture = crate::nowledge_memory_core_fixture();
        let json = nowledge_fixture_contract_json(&fixture);

        assert_eq!(json["protocol"], "hawdb-nowledge-fixture-contract");
        assert_eq!(json["fixture"], "nowledge-memory-core");
        assert_eq!(json["setup_count"], fixture.setup.len());
        assert_eq!(json["check_count"], fixture.checks.len());
        assert_eq!(json["check_count"], 644);
        assert_eq!(json["setup"][0]["command_request"]["op"], "query");
        assert_eq!(json["setup"][0]["access"], "mutation");
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["kind"] == "cypher"
                && check["execution_mode"] == "session"
                && check["statement"]["command_request"]["op"] == "query"
        }));
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["kind"] == "projected_graph"
                && check["request"]["op"] == "project_graph"
                && check["expected_projected_graph"]["node_count"]
                    .as_u64()
                    .unwrap_or_default()
                    > 0
        }));
    }
}
