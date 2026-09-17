use super::*;
use crate::build_control::observation;
use std::time::{Duration, Instant};

fn merge_measurement(per_run: usize, repetitions: usize) {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::with_timeout(Duration::from_secs(3600))
        .with_memory_reservation(RuntimeMemoryReservation::new(BUDGET as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut pool = SpillRuns::with_context(
        &fixture.0,
        1,
        Default::default(),
        memory.clone(),
        task.clone(),
    )
    .unwrap();
    const RUNS: usize = 4;
    let mut pending = PendingPostings::new(Some(&memory)).unwrap();
    for run in 0..RUNS {
        let id = format!("doc-{run:02}");
        for index in 0..per_run {
            let text = format!("term-{index:08}");
            pool.prepare(text.len(), id.len()).unwrap();
            pending
                .push(Term::copy(&text, Some(&memory)).unwrap(), &id, 1, 1)
                .unwrap();
        }
        pending.flush(&mut pool).unwrap();
    }
    drop(pending);
    let mut expected = RUN_HEADER.to_vec();
    for index in 0..per_run {
        for run in 0..RUNS {
            encode_posting(
                &mut expected,
                &Posting {
                    term: format!("term-{index:08}").into(),
                    document_id: format!("doc-{run:02}"),
                    term_frequency: 1,
                    document_len: 1,
                },
            )
            .unwrap();
        }
    }
    let competitor = fill(&memory);
    for iteration in 0..repetitions {
        let output = pool.next_guard().unwrap();
        let start = Instant::now();
        let (result, checkpoints) = observation::measure(|| {
            merge_runs_with_progress(
                &pool.paths,
                &output.path,
                pool.config,
                pool.bytes,
                &mut FileSpillIo,
                pool.progress.as_ref(),
                Some(&task),
            )
        });
        let elapsed = start.elapsed();
        assert_eq!(result.unwrap(), pool.bytes + expected.len() as u64);
        assert_eq!(fs::read(&output.path).unwrap(), expected);
        assert_eq!(memory.ledger.snapshot().used_bytes, BUDGET);
        assert!(
            checkpoints <= 3 * per_run * RUNS + 16,
            "short records must not checkpoint separately for each encoded field"
        );
        eprintln!("corpus merge iteration={iteration} records={} bytes={} checkpoints={checkpoints} elapsed_ns={}",
            per_run * RUNS, expected.len(), elapsed.as_nanos());
    }
    drop((pool, competitor));
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn controlled_corpus_merge_preserves_exact_bytes_at_an_exhausted_root() {
    merge_measurement(64, 1);
}

#[test]
#[ignore = "explicit local optimized merge measurement"]
fn controlled_corpus_merge_checkpoint_measurement() {
    merge_measurement(4096, 9);
}
