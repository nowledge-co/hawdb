use super::super::artifact_admission::memory;
use super::super::merge_admission::{drain, run_bytes};
use super::*;

fn reference(bytes: &[u8]) -> Option<Vec<Posting>> {
    fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
        let prefix = bytes.get(..count)?;
        *bytes = bytes.get(count..)?;
        Some(prefix)
    }
    let mut bytes = bytes;
    if take(&mut bytes, 8)? != b"SKNLEXR1" {
        return None;
    }
    let mut output = Vec::new();
    let mut previous = None;
    while !bytes.is_empty() {
        let length = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().ok()?) as usize;
        if length == 0 || length as u64 > LexicalProjectionConfig::default().max_term_bytes.get() {
            return None;
        }
        let term = std::str::from_utf8(take(&mut bytes, length)?)
            .ok()?
            .to_owned();
        let ordinal = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().ok()?);
        let tf = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().ok()?);
        let key = (term.clone(), ordinal, tf);
        if tf == 0 || previous.as_ref().is_some_and(|previous| previous > &key) {
            return None;
        }
        if previous.as_ref() != Some(&key) {
            output.push(Posting {
                term,
                ordinal,
                term_frequency: tf,
            });
        }
        previous = Some(key);
    }
    Some(output)
}

#[test]
#[ignore = "local external-merge admission campaign; run the explicit Bazel fuzz suite"]
fn merge_admission_campaign() {
    let root = temporary_root("merge");
    let mut random = Random(0x206_ae12);
    let mut records = 0;
    let mut cancellations = 0;
    let alphabet = ['a', 'z', '\0', '\n', '\u{4e2d}', '\u{1f600}'];
    for case in 0..6000 {
        let mut expected = BTreeSet::new();
        let mut paths = Vec::new();
        for source in 0..1 + random.index(8) {
            let mut entries = Vec::new();
            for _ in 0..random.index(20) {
                let term = (0..1 + random.index(24))
                    .map(|_| alphabet[random.index(alphabet.len())])
                    .collect::<String>();
                let ordinal =
                    [0, u64::from(u32::MAX) + 1, u64::MAX, random.next()][random.index(4)];
                let tf = (random.next() as u32).max(1);
                let posting = Posting {
                    term: term.clone(),
                    ordinal,
                    term_frequency: tf,
                };
                expected.insert((term, ordinal, tf));
                entries.push(posting.clone());
                if random.index(3) == 0 {
                    entries.push(posting);
                }
            }
            entries.sort();
            records += entries.len();
            let path = root.join(format!("{source}.tmp"));
            fs::write(&path, run_bytes(&entries)).unwrap();
            paths.push(path);
        }
        let expected = expected
            .into_iter()
            .map(|(term, ordinal, term_frequency)| Posting {
                term,
                ordinal,
                term_frequency,
            })
            .collect::<Vec<_>>();
        let baseline = memory(4 * 1024 * 1024);
        assert_eq!(drain(&paths, &baseline).unwrap(), expected, "case {case}");
        let peak = baseline.ledger.snapshot().peak_bytes;
        for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
            let memory = memory(limit);
            let result = drain(&paths, &memory);
            assert_eq!(
                result.is_ok(),
                succeeds,
                "case {case}, limit {limit}, {result:?}"
            );
            if let Ok(observed) = result {
                assert_eq!(observed, expected);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert!(memory.ledger.snapshot().peak_bytes <= limit);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
        }
        if case % 32 == 0 {
            let task = RuntimeTaskContext::default();
            let mut cursor =
                MergedPostings::new(&paths, Default::default(), baseline.clone(), task.clone())
                    .unwrap();
            let _ = cursor.next().unwrap();
            super::super::super::merge::evidence::take();
            task.cancellation().cancel();
            assert!(cursor.next().unwrap_err().to_string().contains("cancelled"));
            assert_eq!(super::super::super::merge::evidence::take(), 0);
            drop(cursor);
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            cancellations += 1;
        }
        for path in paths {
            fs::remove_file(path).unwrap();
        }
    }
    let canonical = run_bytes(&[
        Posting {
            term: "a\0".to_owned(),
            ordinal: 0,
            term_frequency: 1,
        },
        Posting {
            term: "graph".to_owned(),
            ordinal: u64::MAX,
            term_frequency: u32::MAX,
        },
    ]);
    let path = root.join("mutant.tmp");
    let mut outcomes = Outcomes::default();
    for case in 0..12_000 {
        let bytes = if case % 11 == 0 {
            canonical.clone()
        } else {
            random.mutate(&canonical, case)
        };
        fs::write(&path, &bytes).unwrap();
        let memory = memory(4 * 1024 * 1024);
        let expected = reference(&bytes);
        let actual = drain(std::slice::from_ref(&path), &memory);
        assert_eq!(
            actual.is_ok(),
            expected.is_some(),
            "case {case}: {bytes:?}, {actual:?}"
        );
        outcomes.record(expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(actual.unwrap(), expected, "case {case}");
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
    }
    assert!(records > 100_000);
    assert_eq!(cancellations, 188);
    outcomes.finish("external run mutations", 12_000);
    eprintln!("merge seed=0x206ae12 cases=6000 records={records} exact=6000 short=6000 cancelled={cancellations}");
    fs::remove_dir_all(root).unwrap();
}
