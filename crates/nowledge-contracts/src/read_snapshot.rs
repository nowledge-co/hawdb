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

//! Host-neutral bounded read snapshot reporting contract.

pub const NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL: &str =
    "hawdb-nowledge-mem-read-snapshot-report-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemReadSnapshotBudget {
    pub max_rows: usize,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemReadSnapshotReport {
    pub commit_epoch: u64,
    pub search_projection_present: bool,
    pub search_projection_source_graph_commit_epoch: Option<u64>,
    pub search_projection_durable_source_graph_commit_epoch: Option<u64>,
    pub max_rows: usize,
    pub max_payload_bytes: usize,
    pub cypher_statement_count: usize,
    pub sql_statement_count: usize,
    pub vector_seed_execution_count: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub remaining_rows: usize,
    pub remaining_payload_bytes: usize,
}

impl NowledgeMemReadSnapshotReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
            "commit_epoch": self.commit_epoch,
            "search_projection_present": self.search_projection_present,
            "search_projection_source_graph_commit_epoch": self.search_projection_source_graph_commit_epoch,
            "search_projection_durable_source_graph_commit_epoch": self.search_projection_durable_source_graph_commit_epoch,
            "max_rows": self.max_rows,
            "max_payload_bytes": self.max_payload_bytes,
            "cypher_statement_count": self.cypher_statement_count,
            "sql_statement_count": self.sql_statement_count,
            "vector_seed_execution_count": self.vector_seed_execution_count,
            "output_rows": self.output_rows,
            "output_payload_bytes": self.output_payload_bytes,
            "remaining_rows": self.remaining_rows,
            "remaining_payload_bytes": self.remaining_payload_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_json_preserves_protocol_and_budget_accounting() {
        let report = NowledgeMemReadSnapshotReport {
            commit_epoch: 42,
            search_projection_present: true,
            search_projection_source_graph_commit_epoch: Some(41),
            search_projection_durable_source_graph_commit_epoch: Some(40),
            max_rows: 32,
            max_payload_bytes: 4096,
            cypher_statement_count: 2,
            sql_statement_count: 1,
            vector_seed_execution_count: 3,
            output_rows: 7,
            output_payload_bytes: 512,
            remaining_rows: 25,
            remaining_payload_bytes: 3584,
        };

        assert_eq!(
            report.json(),
            serde_json::json!({
                "protocol": NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
                "commit_epoch": 42,
                "search_projection_present": true,
                "search_projection_source_graph_commit_epoch": 41,
                "search_projection_durable_source_graph_commit_epoch": 40,
                "max_rows": 32,
                "max_payload_bytes": 4096,
                "cypher_statement_count": 2,
                "sql_statement_count": 1,
                "vector_seed_execution_count": 3,
                "output_rows": 7,
                "output_payload_bytes": 512,
                "remaining_rows": 25,
                "remaining_payload_bytes": 3584,
            }),
        );
    }
}
