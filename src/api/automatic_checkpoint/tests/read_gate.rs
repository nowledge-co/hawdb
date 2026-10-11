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
use crate::executor::{
    ExternalReadOperator, VectorCompressionMode, VectorExecutionBackend, VectorExecutionReport,
    VectorScoreSource, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
    VectorSeedExecutionRow,
};
use crate::Value;
use std::collections::BTreeMap;
use std::sync::mpsc;

struct ReadProbe {
    control: Arc<Control>,
    gate_available: bool,
    calls: usize,
}

impl ExternalReadOperator for ReadProbe {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        request.resources.checkpoint()?;
        assert_eq!(request.embedding, &[1.0, 0.0]);
        self.calls += 1;
        self.gate_available = self.control.state.try_lock().is_ok();
        Ok(VectorSeedExecutionOutput {
            rows: vec![VectorSeedExecutionRow {
                id: "memory-1".into(),
                external_id: None,
                score: 0.75,
            }],
            report: VectorExecutionReport {
                backend: VectorExecutionBackend::ScalarFlat,
                compression_mode: VectorCompressionMode::Disabled,
                candidate_source: crate::planner::VectorCandidateSource::Scalar,
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                candidate_score_source: VectorScoreSource::RawVector,
                final_score_source: VectorScoreSource::RawVector,
                generated_candidate_count: 1,
                descriptor_pruned_count: 0,
                scalar_filtered_count: 0,
                residual_filtered_count: 0,
                candidate_scan_rounds: 1,
                reranked_candidate_count: 1,
                returned_count: 1,
                raw_vector_bytes_read: 0,
                candidate_scan_metrics: None,
                index_covered_document_count: None,
                index_candidate_document_count: None,
                index_coverage_complete: None,
                fallback_reason_codes: Vec::new(),
            },
        })
    }
}

fn read_does_not_hold_publication_gate(analyze: bool) {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    db.query("CREATE (:Memory {id: 'memory-1', body: 'complete'})")
        .unwrap();
    let mut old = db.begin_read_transaction().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let (sealed, paused) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    drop(suspension);
    paused.recv_timeout(Duration::from_secs(15)).unwrap();
    assert_eq!(control.lock().unwrap().phase, Phase::Preparing);
    let mut external = ReadProbe {
        control: control.clone(),
        gate_available: false,
        calls: 0,
    };
    let query = "CALL vector_search($embedding, topK := 1) RETURN id, score";
    let query = if analyze {
        format!("EXPLAIN ANALYZE {query}")
    } else {
        query.to_owned()
    };
    let output = db.query_with_params_trace_and_external_with_context(
        &query,
        &BTreeMap::from([(
            "embedding".into(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]),
        true,
        &mut external,
        None,
        None,
    );
    // Always release the actual owner before any assertion can unwind.
    control.lock().unwrap().prefix_seal_probe = None;
    resume.send(()).unwrap();
    let output = output.unwrap().0;
    assert_eq!(external.calls, 1);
    assert!(
        external.gate_available,
        "a read callback must not execute under the checkpoint publication mutex"
    );
    assert_eq!(output.rows.len(), 1);
    if !analyze {
        assert_eq!(output.rows[0]["id"], Value::String("memory-1".into()));
        assert_eq!(output.rows[0]["score"], Value::Float(0.75));
    }
    wait_for(&db, |r| r.waiting_for_handoff);
    // A following write still goes through adoption and the writer gate.
    db.query("CREATE (:Memory {id: 'memory-2', body: 'later'})")
        .unwrap();
    assert_eq!(
        old.query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
        2
    );
    drop(old);
    db.checkpoint().unwrap();
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[test]
fn cypher_read_does_not_hold_checkpoint_publication_gate() {
    read_does_not_hold_publication_gate(false);
}

#[test]
fn explain_analyze_does_not_hold_checkpoint_publication_gate() {
    read_does_not_hold_publication_gate(true);
}
