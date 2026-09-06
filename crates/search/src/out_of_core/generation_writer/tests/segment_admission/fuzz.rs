use super::*;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn text(&mut self) -> String {
        let alphabet = [
            'a',
            'Z',
            ' ',
            ',',
            '\0',
            '\n',
            '\t',
            '"',
            '\u{0130}',
            '\u{03a3}',
            '\u{4e2d}',
            '\u{1f680}',
        ];
        (0..self.next() % 60)
            .map(|_| alphabet[self.next() as usize % alphabet.len()])
            .collect()
    }
}

fn payload_trial(
    input: &[SearchDocument],
    kind: segment_io::Kind,
    base: u64,
    limit: usize,
    succeeds: bool,
) -> usize {
    let expected = reference_body(kind, input, base);
    let expected_compressed = encode_search_snapshot_text(&expected).unwrap();
    let memory = memory(limit);
    let task = RuntimeTaskContext::default();
    let documents = admit(input.to_vec(), &memory);
    let result = (|| -> Result<()> {
        let body = segment_io::body(
            kind,
            &documents,
            base,
            expected.len() as u64,
            &memory,
            &task,
        )?;
        assert_eq!(body.as_ref(), expected.as_bytes());
        let compressed = segment_io::compress(
            body.as_ref(),
            expected_compressed.len() as u64,
            &memory,
            &task,
        )?;
        assert_eq!(compressed.as_ref(), expected_compressed);
        assert_eq!(
            crate::decode_search_snapshot_text(compressed.as_ref())?,
            expected
        );
        Ok(())
    })();
    assert_eq!(result.is_ok(), succeeds, "limit={limit}, result={result:?}");
    drop(documents);
    let snapshot = memory.ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert_eq!(snapshot.account_count, 3);
    assert!(snapshot.peak_bytes <= limit);
    snapshot.peak_bytes
}

#[test]
#[ignore = "local segment admission campaign; run the explicit Bazel fuzz suite"]
fn segment_admission_campaign() {
    let mut random = Random(0x206_5e67);
    let mut documents = 0;
    let mut payloads = 0;
    let mut cancellations = 0;
    for case in 0..1000 {
        let input = (0..random.next() as usize % 9)
            .map(|index| {
                let mut document = document(index);
                document.id = random.text();
                document.title = random.text();
                document.content = random.text();
                document.embedding = if random.next().is_multiple_of(3) {
                    None
                } else {
                    Some(
                        (0..random.next() % 10)
                            .map(|_| f32::from_bits(random.next() as u32))
                            .collect(),
                    )
                };
                let labels = if case % 3 == 0 {
                    serde_json::to_string(&[random.text(), random.text(), String::new()]).unwrap()
                } else if case % 3 == 1 {
                    random.text()
                } else {
                    format!("[\"{}\",42]", random.text())
                };
                document.metadata = BTreeMap::from([
                    ("labels".to_owned(), labels),
                    ("metadata.tags".to_owned(), random.text()),
                    (
                        "kind".to_owned(),
                        [" Chunk ", "SOURCE", "MEMORY", " unknown "][case % 4].to_owned(),
                    ),
                    ("score".to_owned(), format!("{}", random.next() as i64)),
                    (
                        "created_at".to_owned(),
                        ["", "2026-09-07", "2026-09-07T01:02:03Z", "-1"][case % 4].to_owned(),
                    ),
                ]);
                document
            })
            .collect::<Vec<_>>();
        documents += input.len();
        let peak = descriptor_trial(&input, 1024 * 1024, true);
        assert_eq!(descriptor_trial(&input, peak, true), peak);
        descriptor_trial(&input, peak - 1, false);
        let base = [0, 1_u64 << 40, u64::MAX - 10][case % 3];
        for kind in [
            segment_io::Kind::Document,
            segment_io::Kind::Metadata,
            segment_io::Kind::Vector,
        ] {
            let peak = payload_trial(&input, kind, base, 16 * 1024 * 1024, true);
            assert_eq!(payload_trial(&input, kind, base, peak, true), peak);
            payload_trial(&input, kind, base, peak - 1, false);
            payloads += 1;
        }
        if case % 32 == 0 {
            let memory = memory(16 * 1024 * 1024);
            let task = RuntimeTaskContext::default();
            let documents = admit(input, &memory);
            let body = segment_io::body(
                segment_io::Kind::Document,
                &documents,
                base,
                u64::MAX,
                &memory,
                &task,
            )
            .unwrap();
            task.cancellation().cancel();
            segment_io::evidence::take();
            assert!(matches!(
                segment_io::compress(body.as_ref(), u64::MAX, &memory, &task),
                Err(SkeinError::Execution(message)) if message.contains("cancelled")
            ));
            assert_eq!(segment_io::evidence::take(), 0);
            drop(body);
            drop(documents);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            cancellations += 1;
        }
    }
    assert!(documents > 3000);
    assert_eq!(payloads, 3000);
    assert_eq!(cancellations, 32);
    eprintln!("segment seed=0x2065e67 cases=1000 documents={documents} descriptors=1000 payloads={payloads} exact=4000 short=4000 cancelled={cancellations}");
}
