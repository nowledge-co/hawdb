use super::super::super::dictionary_memory::{self as admitted_dictionary, Term};
use super::super::artifact_admission::memory;
use super::super::dictionary_admission::stored_keys;
use super::*;

#[test]
#[ignore = "local dictionary admission campaign; run the explicit Bazel fuzz suite"]
fn dictionary_admission_campaign() {
    let root = temporary_root("dictionary-memory");
    let mut random = Random(0x206_fc7);
    let alphabet = ['a', 'Z', '\0', '\n', '\u{0130}', '\u{4e2d}', '\u{1f600}'];
    let mut terms = 0;
    let mut builds = 0;
    let mut validations = 0;
    let mut cancellations = 0;
    for case in 0..2000 {
        let count = 1 + random.index(64);
        let mut keys = BTreeSet::new();
        while keys.len() < count {
            let mut key = if case % 3 == 0 {
                "shared-prefix:".to_owned()
            } else {
                String::new()
            };
            for _ in 0..1 + random.index(20) {
                key.push(alphabet[random.index(alphabet.len())]);
            }
            keys.insert(key);
        }
        let config = LexicalProjectionConfig {
            max_block_bytes: NonZeroU64::new([512, 1024, 4096, 65536][case % 4]).unwrap(),
            ..Default::default()
        };
        let mut offset = [24, u64::from(u32::MAX) + 1, u64::MAX - 65536][case % 3];
        let entries = keys
            .into_iter()
            .map(|key| {
                let posting_bytes = 48 + random.index(128) as u64;
                let metadata = dictionary::Metadata {
                    df: (random.next() / count as u64).max(1),
                    posting_offset: offset,
                    posting_bytes,
                    skip_offset: random.next() % posting_bytes,
                };
                offset += posting_bytes;
                (key, metadata)
            })
            .collect::<Vec<_>>();
        terms += entries.len();
        let expected = entries.iter().cloned().collect::<BTreeMap<_, _>>();
        let baseline = memory(16 * 1024 * 1024);
        admitted_dictionary::evidence::take();
        assert_eq!(
            stored_keys(&root, &entries, config, &baseline).unwrap(),
            expected,
            "case {case}"
        );
        let (cloned, built, validated) = admitted_dictionary::evidence::take();
        assert_eq!(cloned, entries.len());
        builds += built;
        validations += validated;
        let peak = baseline.ledger.snapshot().peak_bytes;
        for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
            let memory = memory(limit);
            let result = stored_keys(&root, &entries, config, &memory);
            assert_eq!(
                result.is_ok(),
                succeeds,
                "case {case}, limit {limit}: {result:?}"
            );
            if let Ok(actual) = result {
                assert_eq!(actual, expected);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert!(memory.ledger.snapshot().peak_bytes <= limit);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
            assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        }
        if case % 32 == 0 {
            let task = RuntimeTaskContext::default();
            let directory = dictionary_store::DirectoryBudget::new(u64::MAX)
                .with_memory(&baseline)
                .unwrap();
            let mut writer = dictionary_store::Writer::new(
                &root.join("dictionary.tmp"),
                config,
                dictionary_store::SpillBudget::new(0, u64::MAX),
                directory.clone(),
                baseline.clone(),
            )
            .unwrap()
            .with_context(task.clone());
            for (key, metadata) in &entries[..random.index(entries.len() + 1)] {
                writer
                    .push(Term::new(key, &baseline).unwrap(), *metadata)
                    .unwrap();
            }
            task.cancellation().cancel();
            admitted_dictionary::evidence::take();
            let mut output = Vec::new();
            assert!(writer
                .finish(&mut output, &mut 0)
                .unwrap_err()
                .to_string()
                .contains("cancelled"));
            assert_eq!(admitted_dictionary::evidence::take(), (0, 0, 0));
            assert!(output.is_empty());
            drop(directory);
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
            cancellations += 1;
        }
    }
    assert!(terms > 50_000);
    assert!(builds > validations && validations > 2000);
    assert_eq!(cancellations, 63);
    eprintln!("dictionary seed=0x206fc7 cases=2000 terms={terms} builds={builds} validated={validations} exact=2000 short=2000 cancelled={cancellations}");
    fs::remove_dir(root).unwrap();
}
