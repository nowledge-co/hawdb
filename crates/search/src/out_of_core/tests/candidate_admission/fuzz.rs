use super::*;
use crate::query_memory::Admitted;

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 ^ (self.0 >> 29)
    }
    fn index(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

struct Output {
    bytes: Admitted<Vec<u8>>,
    entries: Admitted<Vec<CandidateEntry>>,
    count: usize,
}
fn roundtrip(
    documents: &[SearchMetadataDocument],
    predicates: &crate::SearchPredicateSet,
    memory: &QueryMemory,
    task: &RuntimeTaskContext,
) -> Result<Output> {
    let (bytes, count) = candidate_memory::encode(
        documents,
        predicates,
        1024 * 1024,
        1024 * 1024,
        &memory.working,
        task,
    )?;
    let entries = candidate_codec::entries(&bytes, count, &memory.working, task)?;
    Ok(Output {
        bytes,
        entries,
        count,
    })
}

// Independent cursor decoder: no production visitor, sizing or ordering helper.
fn reference(bytes: &[u8], expected: usize) -> Option<Vec<String>> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut entries = Vec::new();
    while cursor.position() < bytes.len() as u64 {
        let mut word = [0; 4];
        cursor.read_exact(&mut word).ok()?;
        let length = u32::from_le_bytes(word) as usize;
        let start = cursor.position() as usize;
        let id = std::str::from_utf8(bytes.get(start..start.checked_add(length)?)?).ok()?;
        if entries
            .last()
            .is_some_and(|previous: &String| previous.as_str() >= id)
        {
            return None;
        }
        cursor.set_position((start + length) as u64);
        let mut ordinal = [0; 8];
        cursor.read_exact(&mut ordinal).ok()?;
        entries.push(id.to_owned());
    }
    (entries.len() == expected).then_some(entries)
}

#[test]
#[ignore = "local candidate admission campaign; run the explicit Bazel fuzz suite"]
fn candidate_admission_campaign() {
    let mut random = Random(0x206ca11);
    let task = RuntimeTaskContext::default();
    let predicates = search_metadata_predicate_pushdown(&BTreeMap::from([(
        "space_id".to_owned(),
        "team".to_owned(),
    )]));
    let mut row_count = 0;
    let mut decoded_count = 0;
    let mut accepted = 0;
    let mut rejected = 0;
    let mut cancelled = 0;
    for case in 0..1000 {
        let count = 1 + random.index(130);
        let mut documents = Vec::new();
        let mut ordinal = 0u64;
        for index in 0..count {
            let kept = random.index(3) != 0;
            let vector_ordinal = if random.index(3) == 0 {
                None
            } else {
                let value = ordinal;
                ordinal += 1;
                Some(value)
            };
            documents.push(SearchMetadataDocument {
                document: SearchDocument {
                    id: format!("id:{index:04}:{}", "雪\0x".repeat(random.index(20))),
                    title: String::new(),
                    content: String::new(),
                    embedding: None,
                    metadata: BTreeMap::from([(
                        "space_id".to_owned(),
                        if kept { "team" } else { "private" }.to_owned(),
                    )]),
                },
                vector_ordinal,
            });
        }
        row_count += count;
        let expected_rows = documents
            .iter()
            .filter(|entry| entry.document.metadata["space_id"] == "team")
            .map(|entry| (entry.document.id.as_str(), entry.vector_ordinal))
            .collect::<Vec<_>>();
        let expected = wire(&expected_rows);
        let baseline = memory(128 * 1024 * 1024);
        let competing = random.index(4096);
        let guard = baseline.scores.reserve(competing).unwrap();
        let output = roundtrip(&documents, &predicates.predicates, &baseline, &task).unwrap();
        assert_eq!(output.bytes.as_slice(), expected);
        assert_eq!(output.count, expected_rows.len());
        for (entry, expected) in output.entries.iter().zip(&expected_rows) {
            assert_eq!(entry.id, expected.0);
        }
        decoded_count += output.count;
        let retained = output.bytes.capacity()
            + output.entries.capacity() * std::mem::size_of::<CandidateEntry>()
            + output
                .entries
                .iter()
                .map(|entry| entry.id.capacity())
                .sum::<usize>();
        assert_eq!(baseline.ledger.snapshot().used_bytes, competing + retained);
        let peak = baseline.ledger.snapshot().peak_bytes;
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let other = bounded.scores.reserve(competing).unwrap();
            let actual = roundtrip(&documents, &predicates.predicates, &bounded, &task);
            assert_eq!(
                actual.is_ok(),
                limit == peak,
                "budget mismatch at case {case}"
            );
            if let Ok(actual) = &actual {
                assert_eq!(actual.bytes.as_slice(), expected);
            }
            drop(actual);
            assert_eq!(bounded.ledger.snapshot().used_bytes, competing);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
        for variant in 0..4 {
            let mut bytes = expected.clone();
            match variant {
                0 if !bytes.is_empty() => {
                    let end = random.index(bytes.len());
                    bytes.truncate(end);
                }
                1 => bytes.push(0xff),
                2 if bytes.len() >= 4 => bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes()),
                _ if !bytes.is_empty() => {
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1;
                }
                _ => {}
            }
            let oracle = reference(&bytes, expected_rows.len());
            let decoded =
                candidate_codec::entries(&bytes, expected_rows.len(), &baseline.working, &task);
            assert_eq!(decoded.is_ok(), oracle.is_some());
            match (decoded, oracle) {
                (Ok(decoded), Some(ids)) => {
                    accepted += 1;
                    assert_eq!(
                        decoded.iter().map(|entry| &entry.id).collect::<Vec<_>>(),
                        ids.iter().collect::<Vec<_>>()
                    );
                }
                (Err(_), None) => rejected += 1,
                _ => unreachable!(),
            }
            assert_eq!(baseline.ledger.snapshot().used_bytes, competing + retained);
        }
        drop(output);
        drop(guard);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        if case % 16 == 0 {
            let cancelled_task = RuntimeTaskContext::default();
            cancelled_task.cancellation().cancel();
            assert!(roundtrip(
                &documents,
                &predicates.predicates,
                &baseline,
                &cancelled_task
            )
            .is_err());
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            cancelled += 1;
        }
    }
    assert!(accepted > 0 && rejected > 0);
    eprintln!("candidate seed=0x206ca11 cases=1000 rows={row_count} decoded={decoded_count} exact=1000 short=1000 mutated=4000 accepted={accepted} rejected={rejected} cancelled={cancelled}");
    envelope_campaign();
}

fn envelope_campaign() {
    let mut random = Random(0x206ec0de);
    let task = RuntimeTaskContext::default();
    let mut rejected = 0;
    let mut byte_count = 0;
    for case in 0..256 {
        let mut body = String::new();
        for _ in 0..random.index(4096) {
            body.push(['a', '\0', '雪', 'İ', ',', ' ', '\t'][random.index(7)]);
        }
        byte_count += body.len();
        let split = random.index(body.len() + 1);
        let mut compressed = zstd::stream::encode_all(&body.as_bytes()[..split], 0).unwrap();
        compressed.extend(zstd::stream::encode_all(&body.as_bytes()[split..], 0).unwrap());
        let envelope = metadata::envelope(&compressed, body.as_bytes(), body.len() as u64);
        let competing = random.index(4096);
        let peak = competing + query_io::DECODE_WORKSPACE_BYTES + body.len();
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let other = bounded.scores.reserve(competing).unwrap();
            let output = query_io::decode(&envelope, body.len() as u64, &bounded.working, &task);
            assert_eq!(output.is_ok(), limit == peak);
            if let Ok(decoded) = &output {
                assert_eq!(***decoded, body);
                assert_eq!(bounded.ledger.snapshot().used_bytes, competing + body.len());
            }
            drop(output);
            assert_eq!(bounded.ledger.snapshot().used_bytes, competing);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        }
        let bounded = memory(peak + 1);
        for bytes in [
            metadata::envelope(&compressed, body.as_bytes(), body.len() as u64 + 1),
            metadata::envelope(
                &compressed[..compressed.len() - 1],
                body.as_bytes(),
                body.len() as u64,
            ),
            metadata::envelope(&compressed, b"wrong checksum", body.len() as u64),
        ] {
            let oracle = crate::decode_search_snapshot_text_bounded(&bytes, body.len() as u64 + 1);
            let output = query_io::decode(&bytes, body.len() as u64 + 1, &bounded.working, &task);
            assert!(oracle.is_err() && output.is_err());
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            rejected += 1;
        }
        if case % 16 == 0 {
            let cancelled = RuntimeTaskContext::default();
            cancelled.cancellation().cancel();
            query_io::evidence::take();
            assert!(
                query_io::decode(&envelope, body.len() as u64, &bounded.working, &cancelled)
                    .is_err()
            );
            assert_eq!(query_io::evidence::take().1, 0);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        }
    }
    eprintln!("candidate envelopes seed=0x206ec0de cases=256 bytes={byte_count} exact=256 short=256 rejected={rejected} cancelled=16");
}
