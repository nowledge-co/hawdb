use super::*;
use lexical::{LexicalProjectionConfig, MergedPostings, Posting};
use std::num::NonZeroUsize;

fn build_runs(
    root: &Path,
    generation: u64,
    count: usize,
    fan_in: usize,
    memory: &BuildMemory,
) -> Result<()> {
    let competing = memory.ledger.snapshot().used_bytes;
    let config = LexicalProjectionConfig {
        max_merge_fan_in: NonZeroUsize::new(fan_in).unwrap(),
        ..Default::default()
    };
    let mut runs = SpillRuns::new(root, generation, config, memory.clone())?;
    for ordinal in 0..count as u64 {
        let term = "graph\0\u{130}";
        runs.spill(&mut vec![Posting {
            term: term.into(),
            ordinal,
            term_frequency: 3,
        }])?;
        let mut expected = b"SKNLEXR1".to_vec();
        expected.extend_from_slice(&(term.len() as u32).to_le_bytes());
        expected.extend_from_slice(term.as_bytes());
        expected.extend_from_slice(&ordinal.to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        assert_eq!(fs::read(&runs.paths[ordinal as usize]).unwrap(), expected);
    }
    runs.compact()?;
    let mut cursor = MergedPostings::new(
        &runs.paths,
        config,
        memory.clone(),
        RuntimeTaskContext::default(),
    )?;
    for ordinal in 0..count as u64 {
        let posting = cursor.next()?.unwrap();
        assert_eq!(posting.ordinal, ordinal);
        assert_eq!(posting.term, "graph\0\u{130}");
        assert_eq!(posting.term_frequency, 3);
    }
    assert!(cursor.next()?.is_none());
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        competing + runs.paths.retained_bytes()
    );
    Ok(())
}

#[test]
#[ignore = "local lexical backend path and merge ownership campaign"]
fn lexical_path_admission_campaign() {
    let root = root("lexical-path-campaign");
    fs::create_dir(&root).unwrap();
    let mut random = 0x206_bac4_u64;
    for case in 0..128 {
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let generation = if case % 16 == 0 { u64::MAX } else { random };
        let native = root.join(format!("path-{}-\u{130}", "x".repeat(case % 90)));
        publication_case(&native, generation);
        let count = 3 + random as usize % 16;
        let fan_in = 2 + (random >> 32) as usize % 4;
        let baseline = memory(1024 * 1024);
        build_runs(&root, generation, count, fan_in, &baseline).unwrap();
        let peak = baseline.ledger.snapshot().peak_bytes;
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        for limit in [peak - 1, peak] {
            let memory = memory(limit + 137);
            let competing = memory.input.reserve(137).unwrap();
            let result = build_runs(&root, generation, count, fan_in, &memory);
            assert_eq!(result.is_ok(), limit == peak, "case {case}, limit {limit}");
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(competing);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
            assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        }
    }
    eprintln!("lexical paths seed=0x206bac4 cases=128 publication_exact=128 publication_short=128 merge_exact=128 merge_short=128");
    fs::remove_dir(root).unwrap();
}
