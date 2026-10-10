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

//! Development-only browser binding over the ordinary embedded facade.
use hawdb::{
    Database, DatabaseConfig, HawDBError, QueryOutput, RuntimeTaskContext, Value, ValueRef,
};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};
use std::io::{self, Write};
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;

const MAX_QUERY_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen)]
pub struct QueryBridge {
    database: Database,
}

impl Default for QueryBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen)]
impl QueryBridge {
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen(constructor)
    )]
    pub fn new() -> Self {
        Self {
            database: Database::new_with_config(DatabaseConfig {
                max_read_result_rows: Some(256),
                max_read_result_payload_bytes: Some(1024 * 1024),
                max_plan_cache_entries: Some(32),
                ..DatabaseConfig::default()
            }),
        }
    }

    /// Returns complete tagged JSON, including explicit admission/query errors.
    pub fn execute(&mut self, query: &str) -> String {
        if query.len() > MAX_QUERY_BYTES {
            return error_response("query_limit", "Query exceeds the 64 KiB input limit.");
        }
        let context = RuntimeTaskContext::with_timeout(Duration::from_secs(5));
        match self.database.query_with_context(query, &context) {
            Ok(output) => {
                let mut writer = BoundedJson(Vec::new());
                if serde_json::to_writer(&mut writer, &DisplayOutput(&output)).is_err() {
                    return serde_json::json!({
                        "status": "error",
                        "error": {
                            "kind": "response_limit",
                            "engine_query_succeeded": true,
                            "max_encoded_bytes": MAX_RESPONSE_BYTES,
                            "message": "The engine query succeeded, but tagged JSON exceeds the separate 8 MiB display limit. Return fewer columns/rows or smaller nested values. Successful writes remain committed.",
                        },
                    }).to_string();
                }
                String::from_utf8(writer.0).expect("JSON serialization emits UTF-8")
            }
            Err(error) => {
                let kind = match &error {
                    HawDBError::Parse(_) => "parse",
                    HawDBError::Semantic(_) => "semantic",
                    HawDBError::Storage(_) => "storage",
                    HawDBError::StorageIntegrity(_) => "storage_integrity",
                    HawDBError::FileDescriptors(_) => "file_descriptors",
                    HawDBError::Execution(_)
                    | HawDBError::GraphExpansionCandidateLimitExceeded { .. }
                    | HawDBError::GraphExpansionPayloadLimitExceeded { .. } => "execution",
                    HawDBError::BranchCommandUnsupported { .. } => "branch_command_unsupported",
                    HawDBError::BranchBusy { .. } => "branch_busy",
                    HawDBError::TransactionConflict { .. } => "transaction_conflict",
                    HawDBError::AppendSequenceExhausted { .. } => "append_sequence_exhausted",
                    HawDBError::CapabilityUnavailable { .. } => "capability_unavailable",
                };
                error_response(kind, &error.to_string())
            }
        }
    }
}

fn error_response(kind: &str, message: &str) -> String {
    serde_json::json!({"status": "error", "error": {"kind": kind, "message": message}}).to_string()
}

struct BoundedJson(Vec<u8>);

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RESPONSE_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("display response limit exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DisplayOutput<'a>(&'a QueryOutput);

impl Serialize for DisplayOutput<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_struct("QueryOutput", 3)?;
        output.serialize_field("status", "ok")?;
        output.serialize_field("columns", self.0.schema().columns())?;
        output.serialize_field("rows", &DisplayRows(self.0))?;
        output.end()
    }
}

struct DisplayRows<'a>(&'a QueryOutput);

impl Serialize for DisplayRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut rows = serializer.serialize_seq(Some(self.0.rows.len()))?;
        for row in self.0.value_rows() {
            rows.serialize_element(&DisplayList(row))?;
        }
        rows.end()
    }
}

struct DisplayValue<'a>(ValueRef<'a>);

impl Serialize for DisplayValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serializer.serialize_struct("Value", 2)?;
        match self.0 {
            ValueRef::Null => {
                value.serialize_field("type", "null")?;
                value.serialize_field("value", &())?;
            }
            ValueRef::Bool(inner) => {
                value.serialize_field("type", "bool")?;
                value.serialize_field("value", &inner)?;
            }
            ValueRef::Int(inner) => {
                value.serialize_field("type", "int64")?;
                value.serialize_field("value", &inner.to_string())?;
            }
            ValueRef::Float(inner) => {
                value.serialize_field("type", "float64")?;
                value.serialize_field("value", &inner.to_string())?;
            }
            ValueRef::String(inner) => {
                value.serialize_field("type", "string")?;
                value.serialize_field("value", inner)?;
            }
            ValueRef::Binary(inner) => {
                value.serialize_field("type", "binary")?;
                value.serialize_field("value", inner)?;
            }
            ValueRef::Uuid(inner) => {
                value.serialize_field("type", "uuid")?;
                value.serialize_field("value", &inner.to_string())?;
            }
            ValueRef::List(inner) => {
                value.serialize_field("type", "list")?;
                value.serialize_field("value", &DisplayList(inner))?;
            }
            ValueRef::Map(inner) => {
                value.serialize_field("type", "map")?;
                value.serialize_field("value", &DisplayMap(inner))?;
            }
        }
        value.end()
    }
}

struct DisplayList<'a>(&'a [Value]);

impl Serialize for DisplayList<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut list = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            list.serialize_element(&DisplayValue(value.as_ref()))?;
        }
        list.end()
    }
}

struct DisplayMap<'a>(&'a std::collections::BTreeMap<String, Value>);

impl Serialize for DisplayMap<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, &DisplayValue(value.as_ref()))?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as Json;

    fn execute(bridge: &mut QueryBridge, query: &str) -> Json {
        serde_json::from_str(&bridge.execute(query)).unwrap()
    }

    #[test]
    fn writes_traversal_values_and_error_recovery_share_one_instance() {
        let mut bridge = QueryBridge::new();
        let created = execute(
            &mut bridge,
            "CREATE (:Memory {id: 9223372036854775807, title: 'Browser'})-[:MENTIONS]->(:Entity {name: 'HawDB'})",
        );
        assert_eq!(created["status"], "ok");
        assert_eq!(
            execute(&mut bridge, "invalid Cypher")["error"]["kind"],
            "parse"
        );
        let output = execute(
            &mut bridge,
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS id, e.name AS name",
        );
        assert_eq!(output["status"], "ok");
        assert_eq!(output["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(
            output["rows"][0][0],
            serde_json::json!({"type": "int64", "value": "9223372036854775807"})
        );
        assert_eq!(output["rows"][0][1]["value"], "HawDB");
        assert_eq!(
            execute(
                &mut QueryBridge::new(),
                "MATCH (m:Memory) RETURN m.id AS id"
            )["rows"],
            serde_json::json!([])
        );
    }

    #[test]
    fn nested_values_keep_types_and_non_finite_floats() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Int(i64::MIN),
            Value::Float(f64::INFINITY),
            Value::Binary(vec![0, 255]),
            Value::Map(std::collections::BTreeMap::from([(
                "nested".into(),
                Value::List(vec![Value::String("<script>".into())]),
            )])),
        ];
        let output = serde_json::to_value(DisplayList(&values)).unwrap();
        assert_eq!(output[0]["value"], Json::Null);
        assert_eq!(output[1]["value"], true);
        assert_eq!(output[2]["value"], i64::MIN.to_string());
        assert_eq!(output[3]["value"], "inf");
        assert_eq!(output[4]["value"], serde_json::json!([0, 255]));
        assert_eq!(
            output[5]["value"]["nested"]["value"][0]["value"],
            "<script>"
        );
    }

    #[test]
    fn admission_errors_return_no_partial_rows_and_preserve_the_database() {
        let mut bridge = QueryBridge::new();
        bridge.database = Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(1),
            ..DatabaseConfig::default()
        });
        assert_eq!(
            execute(&mut bridge, "CREATE (:Memory {id: 1})")["status"],
            "ok"
        );
        assert_eq!(
            execute(&mut bridge, "CREATE (:Memory {id: 2})")["status"],
            "ok"
        );
        let output = execute(&mut bridge, "MATCH (m:Memory) RETURN m.id AS id");
        assert_eq!(output["status"], "error");
        assert!(output["error"]["message"]
            .as_str()
            .unwrap()
            .contains("max_read_result_rows"));
        assert!(output.get("rows").is_none());
        assert_eq!(
            execute(&mut bridge, "MATCH (m:Memory) RETURN m.id AS id LIMIT 1")["rows"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            execute(&mut bridge, &"x".repeat(MAX_QUERY_BYTES + 1))["error"]["kind"],
            "query_limit"
        );
        let mut writer = BoundedJson(vec![0; MAX_RESPONSE_BYTES]);
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.0.len(), MAX_RESPONSE_BYTES);
    }

    #[test]
    fn nested_tagged_results_have_an_explicit_display_budget_and_allow_recovery() {
        let mut bridge = QueryBridge::new();
        for index in 0..256 {
            assert_eq!(
                execute(&mut bridge, &format!("CREATE (:Wide {{id: {index}}})"))["status"],
                "ok"
            );
        }
        let values = vec!["true"; 1365].join(", ");
        let output = execute(
            &mut bridge,
            &format!("MATCH (n:Wide) RETURN [{values}] AS values"),
        );
        assert_eq!(output["error"]["kind"], "response_limit", "{output}");
        assert_eq!(output["error"]["engine_query_succeeded"], true);
        assert_eq!(output["error"]["max_encoded_bytes"], MAX_RESPONSE_BYTES);
        assert!(output.get("rows").is_none());
        assert_eq!(
            execute(&mut bridge, "MATCH (n:Wide) RETURN true AS value LIMIT 1")["status"],
            "ok"
        );
    }

    #[test]
    fn payload_admission_returns_an_error_then_allows_a_smaller_read() {
        let mut bridge = QueryBridge {
            database: Database::new_with_config(DatabaseConfig {
                max_read_result_payload_bytes: Some(64),
                ..DatabaseConfig::default()
            }),
        };
        let created = execute(
            &mut bridge,
            &format!("CREATE (:Memory {{id: 1, title: '{}'}})", "x".repeat(256)),
        );
        assert_eq!(created["status"], "ok");
        let output = execute(&mut bridge, "MATCH (m:Memory) RETURN m.title AS title");
        assert_eq!(output["status"], "error");
        assert!(output["error"]["message"]
            .as_str()
            .unwrap()
            .contains("max_read_result_payload_bytes"));
        assert!(output.get("rows").is_none());
        assert_eq!(
            execute(&mut bridge, "MATCH (m:Memory) RETURN m.id AS id")["status"],
            "ok"
        );
    }
}
