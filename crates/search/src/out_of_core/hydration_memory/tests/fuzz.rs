use super::*;

#[test]
#[ignore = "local hydration record and shared-owner campaign"]
fn hydration_admission_campaign() {
    let mut seed = 0x206d0c51u64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut returned = 0;
    for case in 0..128 {
        let mut input = Vec::new();
        for id in 0..1 + next() as usize % 9 {
            let mut document = document(id);
            document.content =
                ["plain", "\u{130}", "a\0B", ""][next() as usize % 4].repeat(next() as usize % 65);
            if next() % 3 == 0 {
                document.embedding = None;
            }
            for field in 0..next() as usize % 7 {
                document.metadata.insert(
                    format!("field:{field}"),
                    "value\u{130}\0".repeat(next() as usize % 17),
                );
            }
            input.push(document);
        }
        let descriptor = descriptor(&input);
        let text = text(&input);
        let memory = memory(8 * 1024 * 1024);
        let other = memory.scores.reserve(137).unwrap();
        let decoded = Segment::decode(
            &text,
            &descriptor,
            &memory.working,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
        assert_eq!(decoded.documents, input);
        returned += decoded.documents.len();
        let peak = memory.ledger.snapshot().peak_bytes;
        drop(decoded);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        for limit in [peak, peak - 1] {
            let bounded = super::memory(limit);
            let result = bounded.scores.reserve(137).and_then(|_lease| {
                Segment::decode(
                    &text,
                    &descriptor,
                    &bounded.working,
                    &RuntimeTaskContext::default(),
                )
            });
            assert_eq!(result.is_ok(), limit == peak, "record case={case}");
            drop(result);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
    }
    let mut selected = 0;
    for case in 0..48 {
        let count = 1 + next() as usize % 17;
        let (root, reader) = fixture("fuzz", count);
        let mut ids = (0..count)
            .filter(|_| next() % 3 != 0)
            .map(|id| document(id).id)
            .collect::<Vec<_>>();
        for index in 0..ids.len() {
            let target = next() as usize % ids.len();
            ids.swap(index, target);
        }
        let expected = ids
            .iter()
            .map(|id| document(id.strip_prefix("memory:").unwrap().parse().unwrap()))
            .collect::<Vec<_>>();
        let memory = memory(8 * 1024 * 1024);
        let other = memory.scores.reserve(137).unwrap();
        let result = read(&reader, &ids, &memory).unwrap();
        assert_eq!(&*result, expected);
        selected += result.len();
        let peak = memory.ledger.snapshot().peak_bytes;
        drop(result);
        drop(other);
        for limit in [peak, peak - 1] {
            let bounded = super::memory(limit);
            let result = bounded
                .scores
                .reserve(137)
                .and_then(|_lease| read(&reader, &ids, &bounded));
            assert_eq!(result.is_ok(), limit == peak, "reader case={case}");
            drop(result);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
        assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, expected);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
    assert!(returned > 0 && selected > 0);
    eprintln!("hydration seed=0x206d0c51 record_cases=128 reader_cases=48 exact=176 short=176 decoded={returned} selected={selected}");
}
