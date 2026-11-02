// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Provider-attested coverage and actual grouped selection work.

use hawdb_plan_cypher::PhysicalOperatorId;

/// Optional native source evidence. A generation is diagnostic identity, not
/// permission to combine this source with a different canonical snapshot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TextSeedSourceReport {
    pub generation: Option<u64>,
    pub source_commit_epoch: Option<u64>,
    pub corpus_documents: Option<usize>,
    pub matching_documents: Option<usize>,
    pub postings_visited: Option<u64>,
    pub bytes_read: Option<u64>,
    pub document_bytes_read: Option<u64>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TextSeedExecutionReport {
    pub operator_id: Option<PhysicalOperatorId>,
    pub complete_requested: bool,
    pub executed: bool,
    pub source_exhausted: bool,
    pub candidate_limit: usize,
    /// The provider's result before canonical filtering or a downstream LIMIT.
    pub candidate_count: usize,
    pub retained_result_bytes: usize,
    pub source: Option<TextSeedSourceReport>,
}

impl TextSeedExecutionReport {
    pub fn status(&self) -> &'static str {
        if !self.executed {
            "skipped"
        } else if !self.complete_requested {
            "window"
        } else if self.source_exhausted {
            "complete"
        } else {
            "incomplete"
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GroupedEvidenceExecutionReport {
    pub operator_id: Option<PhysicalOperatorId>,
    pub executed: bool,
    /// Exhaustion of the aggregate's declared input, including semantic LIMITs.
    pub input_exhausted: bool,
    pub input_rows: usize,
    /// Repeated empty or single scalar-column keys reused without an owned copy.
    pub borrowed_group_key_rows: usize,
    pub groups_considered: usize,
    pub retained_evidence: usize,
    pub output_groups: usize,
    pub peak_state_bytes: usize,
    pub spilled_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::{grouped_evidence_report_value, text_seed_execution_report_value};
    use hawdb_core::Value;

    #[test]
    fn coverage_status_and_value_encoding_preserve_unknown_source_evidence() {
        let mut report = TextSeedExecutionReport {
            complete_requested: true,
            operator_id: Some(PhysicalOperatorId::from_ordinal(4)),
            ..Default::default()
        };
        assert_eq!(report.status(), "skipped");
        report.executed = true;
        assert_eq!(report.status(), "incomplete");
        report.source_exhausted = true;
        assert_eq!(report.status(), "complete");
        report.source = Some(TextSeedSourceReport {
            generation: Some(19),
            postings_visited: Some(32),
            ..Default::default()
        });
        let Value::Map(encoded) = text_seed_execution_report_value(&report) else {
            panic!()
        };
        assert_eq!(encoded["operator_id"], Value::Int(4));
        assert_eq!(encoded["status"], Value::String("complete".into()));
        let Value::Map(source) = &encoded["source"] else {
            panic!()
        };
        assert_eq!(source["generation"], Value::Int(19));
        assert_eq!(source["source_commit_epoch"], Value::Null);
        assert_eq!(source["bytes_read"], Value::Null);
        report.complete_requested = false;
        assert_eq!(report.status(), "window");
        let Value::Map(grouped) =
            grouped_evidence_report_value(&GroupedEvidenceExecutionReport::default())
        else {
            panic!()
        };
        assert_eq!(grouped["executed"], Value::Bool(false));
        assert_eq!(grouped["input_exhausted"], Value::Bool(false));
    }
}
