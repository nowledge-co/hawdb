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

struct PublicationRead {
    control: Arc<Control>,
    resume: Option<mpsc::Sender<()>>,
}

impl ExternalReadOperator for PublicationRead {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        request.resources.checkpoint()?;
        assert_eq!(request.embedding, &[1.0, 0.0]);
        self.resume.take().unwrap().send(()).unwrap();
        wait_control(&self.control, |state| state.phase == Phase::Handoff);
        assert!(self.control.has_pending_handoff());
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

fn wait_control(control: &Control, predicate: impl Fn(&State) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut state = control.lock().unwrap();
    while !predicate(&state) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "checkpoint owner did not progress: {:?}",
            state.report
        );
        state = control.changed.wait_timeout(state, remaining).unwrap().0;
    }
}

fn fixture_with_seal_pause() -> (Fixture, Database, Arc<Control>, mpsc::Sender<()>) {
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
    db.query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("INSERT INTO records (id, body) VALUES (1, 'complete')")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-1', body: 'complete'})")
        .unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let (sealed, paused) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    let (error_sender, errors) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.preparation_error_probe = Some(error_sender);
        state.prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
            sealed,
            resume: Mutex::new(continuation),
        }));
    }
    drop(suspension);
    paused
        .recv_timeout(Duration::from_secs(15))
        .unwrap_or_else(|error| {
            let state = control.state.try_lock();
            let status = state.as_ref().ok().map(|state| {
                (
                    state.phase,
                    state.report,
                    state.governor.as_ref().map(RuntimeGovernor::snapshot),
                )
            });
            drop(state);
            panic!(
                "seal pause failed: {error}; status: {status:?}; errors: {:?}",
                errors.try_iter().collect::<Vec<_>>()
            );
        });
    {
        let mut state = control.lock().unwrap();
        state.prefix_seal_probe = None;
        state.preparation_error_probe = None;
    }
    (fixture, db, control, resume)
}

#[test]
fn checkpoint_publishes_while_read_callback_is_running() {
    let (fixture, mut db, control, resume) = fixture_with_seal_pause();
    let mut old = db.begin_read_transaction().unwrap();
    let mut external = PublicationRead {
        control: control.clone(),
        resume: Some(resume),
    };
    let output = db
        .query_with_params_trace_and_external_with_context(
            "CALL vector_search($embedding, topK := 1) RETURN id, score",
            &BTreeMap::from([(
                "embedding".into(),
                Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            )]),
            true,
            &mut external,
            None,
            None,
        )
        .unwrap()
        .0;
    assert_eq!(output.rows[0]["id"], Value::String("memory-1".into()));
    assert_eq!(output.rows[0]["score"], Value::Float(0.75));
    assert!(control.has_pending_handoff());
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.body AS body")
            .unwrap()
            .rows[0]["body"],
        Value::String("complete".into())
    );
    assert!(!control.has_pending_handoff());
    wait_control(&control, |state| {
        state.phase == Phase::Idle && state.retired.is_none()
    });
    let resources = control
        .lock()
        .unwrap()
        .governor
        .as_ref()
        .unwrap()
        .snapshot();
    assert_eq!(resources.active_background_tasks, 0);
    assert_eq!(resources.active_background_io_slots, 0);
    assert_eq!(resources.admitted_memory_bytes, 0);
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
    assert_eq!(
        reopened.query_sql("SELECT body FROM records").unwrap().rows[0]["body"],
        Value::String("complete".into())
    );
}

#[test]
fn concurrent_read_only_traffic_adopts_and_releases_checkpoint_admission() {
    let (fixture, db, control, resume) = fixture_with_seal_pause();
    let db = crate::ConcurrentDatabase::new(db);
    let mut old = db.begin_read_transaction().unwrap();
    let before = db.published_read_view().unwrap();
    resume.send(()).unwrap();
    wait_control(&control, |state| state.phase == Phase::Handoff);
    assert!(control.has_pending_handoff());
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
        1
    );
    assert!(!control.has_pending_handoff());
    wait_control(&control, |state| {
        state.phase == Phase::Idle && state.retired.is_none()
    });
    let resources = control
        .lock()
        .unwrap()
        .governor
        .as_ref()
        .unwrap()
        .snapshot();
    assert_eq!(resources.active_background_tasks, 0);
    assert_eq!(resources.active_background_io_slots, 0);
    assert_eq!(resources.admitted_memory_bytes, 0);
    let after = db.published_read_view().unwrap();
    assert_eq!(after.visible_commit_epoch(), before.visible_commit_epoch());
    assert_ne!(after.physical_generation(), before.physical_generation());
    assert_eq!(
        after.checkpoint_commit_epoch(),
        Some(after.visible_commit_epoch())
    );
    assert!(after.physical_base_is_current());
    assert_eq!(
        db.query_sql("SELECT body FROM records").unwrap().rows[0]["body"],
        Value::String("complete".into())
    );
    db.query("CREATE (:Memory {id: 'memory-2', body: 'later'})")
        .unwrap();
    assert_eq!(
        old.query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        1
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
fn invalid_read_handoff_fails_closed_and_preserves_disk_authority() {
    let (fixture, mut db, control, resume) = fixture_with_seal_pause();
    resume.send(()).unwrap();
    wait_control(&control, |state| state.phase == Phase::Handoff);
    let other_fixture = Fixture::new();
    let mut other = Database::open(&other_fixture.0).unwrap();
    other.query("CREATE (:Memory {id: 'other'})").unwrap();
    let wrong_identity = other
        .runtime
        .get()
        .unwrap()
        .store
        .checkpoint_source_identity()
        .unwrap();
    control.lock().unwrap().selected.as_mut().unwrap().expected = wrong_identity;
    let error = db.query("MATCH (n:Memory) RETURN n.id").unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    assert!(control.ensure_healthy().is_err());
    assert!(!control.has_pending_handoff());
    assert_eq!(control.lock().unwrap().report.failed_attempts, 1);
    assert!(matches!(
        db.query("MATCH (n:Memory) RETURN n.id"),
        Err(HawDBError::StorageIntegrity(_))
    ));
    assert!(matches!(
        db.checkpoint(),
        Err(HawDBError::StorageIntegrity(_))
    ));
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        reopened.query_sql("SELECT body FROM records").unwrap().rows[0]["body"],
        Value::String("complete".into())
    );
}

#[test]
fn typed_explain_analyze_runs_without_the_owner_mutex() {
    let (_fixture, mut db, control, resume) = fixture_with_seal_pause();
    let (sent, received) = mpsc::channel();
    let result = std::thread::scope(|scope| {
        let state = control.lock().unwrap();
        scope.spawn(|| {
            sent.send(db.explain_analyze_query("MATCH (n:Memory) RETURN n.id AS id"))
                .unwrap();
        });
        let result = received.recv_timeout(Duration::from_secs(1));
        drop(state);
        result
    });
    resume.send(()).unwrap();
    let output = result
        .expect("typed read waited on the owner mutex")
        .unwrap();
    assert_eq!(
        output.output.rows[0]["id"],
        Value::String("memory-1".into())
    );
    wait_control(&control, |state| state.phase == Phase::Handoff);
    db.checkpoint().unwrap();
}

#[test]
fn plain_sql_only_reads_adopt_the_ready_physical_generation() {
    let (fixture, mut db, control, resume) = fixture_with_seal_pause();
    let before = db.runtime.get().unwrap().store.published_read_view();
    resume.send(()).unwrap();
    wait_control(&control, |state| state.phase == Phase::Handoff);
    assert!(control.has_pending_handoff());
    assert_eq!(
        db.query_sql("SELECT body FROM records").unwrap().rows[0]["body"],
        Value::String("complete".into())
    );
    assert!(!control.has_pending_handoff());
    let after = db.runtime.get().unwrap().store.published_read_view();
    assert_eq!(after.visible_commit_epoch(), before.visible_commit_epoch());
    assert_ne!(after.physical_generation(), before.physical_generation());
    wait_control(&control, |state| {
        state.phase == Phase::Idle && state.retired.is_none()
    });
    assert_eq!(
        control
            .lock()
            .unwrap()
            .governor
            .as_ref()
            .unwrap()
            .snapshot()
            .admitted_memory_bytes,
        0
    );
    db.checkpoint().unwrap();
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(
        reopened.query_sql("SELECT body FROM records").unwrap().rows[0]["body"],
        Value::String("complete".into())
    );
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        1
    );
}
