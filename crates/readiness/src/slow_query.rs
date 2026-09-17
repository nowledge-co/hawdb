//! Redacted slow-query reporting contract for the embedded readiness surface.

use crate::bounded_read_evidence::NowledgeMemGraphMode;
use skein_system_sql::SlowQueryLogRecordSummary;

pub const NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-slow-query-report-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryRecord {
    pub sequence: u64,
    pub query_language: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub slow_log_candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub present: bool,
    pub ready: bool,
    pub capacity: usize,
    pub threshold_micros: u128,
    pub record_count: usize,
    pub latest_sequence: Option<u64>,
    pub max_elapsed_micros: Option<i64>,
    pub total_row_count: i64,
    pub records: Vec<NowledgeMemSlowQueryRecord>,
}

impl NowledgeMemSlowQueryReport {
    #[doc(hidden)]
    pub fn from_summaries(
        mode: NowledgeMemGraphMode,
        capacity: usize,
        threshold_micros: u128,
        records: Vec<SlowQueryLogRecordSummary>,
    ) -> Self {
        let records = records
            .into_iter()
            .map(|record| NowledgeMemSlowQueryRecord {
                sequence: record.sequence,
                query_language: record.query_language,
                statement_kind: record.statement_kind,
                query_digest: record.query_digest,
                started_unix_micros: record.started_unix_micros,
                elapsed_micros: record.elapsed_micros,
                row_count: record.row_count,
                success: record.success,
                slow_log_candidate: record.slow_log_candidate,
            })
            .collect::<Vec<_>>();
        let latest_sequence = records.iter().map(|record| record.sequence).max();
        let max_elapsed_micros = records.iter().map(|record| record.elapsed_micros).max();
        let total_row_count = records
            .iter()
            .map(|record| record.row_count)
            .fold(0i64, i64::saturating_add);

        Self {
            protocol: NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL.to_string(),
            mode,
            present: true,
            ready: true,
            capacity,
            threshold_micros,
            record_count: records.len(),
            latest_sequence,
            max_elapsed_micros,
            total_row_count,
            records,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "present": self.present,
            "ready": self.ready,
            "capacity": self.capacity,
            "threshold_micros": self.threshold_micros,
            "record_count": self.record_count,
            "latest_sequence": self.latest_sequence,
            "max_elapsed_micros": self.max_elapsed_micros,
            "total_row_count": self.total_row_count,
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
            "records": self.records.iter().map(|record| {
                serde_json::json!({
                    "sequence": record.sequence,
                    "query_language": record.query_language,
                    "statement_kind": record.statement_kind,
                    "query_digest": record.query_digest,
                    "started_unix_micros": record.started_unix_micros,
                    "elapsed_micros": record.elapsed_micros,
                    "row_count": record.row_count,
                    "success": record.success,
                    "slow_log_candidate": record.slow_log_candidate,
                })
            }).collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{NowledgeMemSlowQueryReport, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL};
    use crate::bounded_read_evidence::NowledgeMemGraphMode;
    use skein_system_sql::SlowQueryLogRecordSummary;

    #[test]
    fn report_preserves_summary_aggregates_and_redaction() {
        let report = NowledgeMemSlowQueryReport::from_summaries(
            NowledgeMemGraphMode::ShadowReadOnly,
            16,
            10_000,
            vec![
                SlowQueryLogRecordSummary {
                    sequence: 3,
                    query_language: "cypher".to_string(),
                    statement_kind: "read".to_string(),
                    query_digest: "first-digest".to_string(),
                    started_unix_micros: 11,
                    elapsed_micros: 40,
                    row_count: 2,
                    success: true,
                    slow_log_candidate: true,
                    access_control_policy_epoch: Some(7),
                },
                SlowQueryLogRecordSummary {
                    sequence: 5,
                    query_language: "sql".to_string(),
                    statement_kind: "write".to_string(),
                    query_digest: "second-digest".to_string(),
                    started_unix_micros: 13,
                    elapsed_micros: 70,
                    row_count: 3,
                    success: false,
                    slow_log_candidate: true,
                    access_control_policy_epoch: Some(8),
                },
            ],
        );

        let json = report.json();
        assert_eq!(report.protocol, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.latest_sequence, Some(5));
        assert_eq!(report.max_elapsed_micros, Some(70));
        assert_eq!(report.total_row_count, 5);
        assert_eq!(json["mode"], "shadow_read_only");
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["records"][0]["query_digest"], "first-digest");
        assert!(json["records"][0]
            .get("access_control_policy_epoch")
            .is_none());
    }
}
