use super::super::artifact_admission::{legacy_document_block, memory};
use super::*;
use artifact_memory::Documents;

fn encode_blocks(
    input: &[(String, u32)],
    generation: u64,
    memory: &BuildMemory,
    cancelled: bool,
) -> Result<usize> {
    let task = RuntimeTaskContext::default();
    let mut documents = Documents::new(memory.clone())?;
    let config = LexicalProjectionConfig::default();
    let mut blocks = 0;
    for (block, rows) in input.chunks(8).enumerate() {
        for (id, length) in rows {
            Documents::record_bytes(id, config)?;
            documents.push(id, *length)?;
        }
        if cancelled {
            task.cancellation().cancel();
        }
        let encoded = documents.encode(generation, block as u64, config, &task)?;
        assert_eq!(
            encoded.payload,
            legacy_document_block(generation, block as u64, rows)
        );
        assert_eq!(encoded.min_key, rows.first().unwrap().0);
        assert_eq!(encoded.max_key, rows.last().unwrap().0);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            documents.retained_bytes() + encoded._memory.bytes()
        );
        drop(encoded);
        documents.clear();
        blocks += 1;
    }
    documents.release();
    Ok(blocks)
}

#[test]
#[ignore = "local document-mapping admission campaign; run the explicit Bazel fuzz suite"]
fn document_mapping_admission_campaign() {
    let mut random = Random(0x206a47);
    let mut blocks = 0;
    let mut cancellations = 0;
    let alphabet = [
        'a',
        'Z',
        '_',
        '\0',
        '\n',
        '"',
        '\\',
        '\u{0130}',
        '\u{4e2d}',
        '\u{20000}',
        '\u{1f600}',
    ];
    for case in 0..12_000 {
        let generation = random.next();
        let input = (0..1 + random.index(24))
            .map(|_| {
                let id = (0..random.index(96))
                    .map(|_| alphabet[random.index(alphabet.len())])
                    .collect::<String>();
                (id, random.next() as u32)
            })
            .collect::<Vec<_>>();
        let baseline = memory(1024 * 1024);
        blocks += encode_blocks(&input, generation, &baseline, false).unwrap();
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        let peak = baseline.ledger.snapshot().peak_bytes;
        assert!(peak > 1);
        for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
            let memory = memory(limit);
            let result = encode_blocks(&input, generation, &memory, false);
            assert_eq!(
                result.is_ok(),
                succeeds,
                "case {case}, limit {limit}, result {result:?}"
            );
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert!(memory.ledger.snapshot().peak_bytes <= limit);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
        }
        if case % 64 == 0 {
            assert!(encode_blocks(&input, generation, &baseline, true).is_err());
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            cancellations += 1;
        }
    }
    assert!(blocks > 20_000);
    assert_eq!(cancellations, 188);
    eprintln!("artifact seed=0x206a47 cases=12000 blocks={blocks} exact=12000 short=12000 cancelled={cancellations}");
}
