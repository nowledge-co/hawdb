use super::*;

fn oracle(
    query: &[f32; 2],
    filtered: bool,
    limit: Option<usize>,
) -> (BTreeMap<String, f64>, usize) {
    let mut rows = Vec::new();
    for number in 0..9 {
        if filtered && number % 2 != 0 {
            continue;
        }
        let raw = [number as f64 + 1.0, (16 - number) as f64];
        let query = query.map(f64::from);
        let dot = raw[0] * query[0] + raw[1] * query[1];
        let norms = (raw[0] * raw[0] + raw[1] * raw[1]).sqrt()
            * (query[0] * query[0] + query[1] * query[1]).sqrt();
        if norms > 0.0 && dot > 0.0 {
            rows.push((format!("memory:{number:03}"), (dot / norms).clamp(0.0, 1.0)));
        }
    }
    rows.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let count = rows.len();
    (
        rows.into_iter().take(limit.unwrap_or(count)).collect(),
        count,
    )
}

fn query(
    reader: &SearchOutOfCoreReader,
    memory: &QueryMemory,
    embedding: &[f32; 2],
    mode: CompressedVectorSearchMode,
    filtered: bool,
    limit: Option<usize>,
) -> Result<VectorScoreScan> {
    let candidates = if filtered {
        candidate_admission::candidates(reader, memory, None)?
    } else {
        CandidateSet::All(reader.document_count())
    };
    reader.scan_vector_scores(
        embedding,
        &candidates,
        limit,
        mode,
        VectorQuery {
            execution: VectorSearchExecutionOptions::default(),
            memory,
        },
        &mut SearchOutOfCoreMetrics::default(),
    )
}

fn parity(output: &VectorScoreScan, expected: &(BTreeMap<String, f64>, usize)) {
    assert_eq!(output.matching_count, expected.1);
    assert_eq!(
        output.scores.keys().collect::<Vec<_>>(),
        expected.0.keys().collect::<Vec<_>>()
    );
    for (id, score) in output.scores.iter() {
        assert!(
            (score - expected.0[id]).abs() < 1e-6,
            "cosine mismatch for {id}"
        );
    }
}

#[test]
#[ignore = "local raw-vector and result-owner admission campaign"]
fn vector_admission_campaign() {
    let (root, reader) = fixture("vector-owner-fuzz");
    let mut seed = 0x206cec70u64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut scans = 0;
    let mut scores = 0;
    for case in 0..128 {
        let embedding = if case % 16 == 0 {
            [0.0, 0.0]
        } else {
            [
                (next() % 129) as f32 / 16.0 - 4.0,
                (next() % 129) as f32 / 16.0 - 4.0,
            ]
        };
        let limit = [None, Some(0), Some(1), Some(3), Some(9)][case % 5];
        let filtered = case % 2 == 0;
        let expected = oracle(&embedding, filtered, limit);
        for &mode in modes() {
            let baseline = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
            let competing = next() as usize % 1024;
            let other = baseline.scores.reserve(competing).unwrap();
            let result = query(&reader, &baseline, &embedding, mode, filtered, limit).unwrap();
            parity(&result, &expected);
            scores += result.scores.len();
            let peak = baseline.ledger.snapshot().peak_bytes;
            for bound in [peak, peak - 1] {
                let bounded = memory(bound, bound);
                let guard = bounded.scores.reserve(competing).unwrap();
                let output = query(&reader, &bounded, &embedding, mode, filtered, limit);
                assert_eq!(output.is_ok(), bound == peak, "case={case} mode={mode:?}");
                if let Ok(output) = &output {
                    parity(output, &expected);
                }
                drop(output);
                assert_eq!(bounded.ledger.snapshot().used_bytes, competing);
                drop(guard);
                assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            }
            drop(result);
            assert_eq!(baseline.ledger.snapshot().used_bytes, competing);
            drop(other);
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            assert_eq!(baseline.ledger.snapshot().account_count, 2);
            scans += 1;
        }
    }
    drop(reader);
    assert!(fs::read_dir(root.join("spill")).unwrap().next().is_none());
    fs::remove_dir_all(root).unwrap();
    eprintln!("vector seed=0x206cec70 queries=128 scans={scans} exact={scans} short={scans} scores={scores}");
}
