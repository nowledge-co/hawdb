use super::*;
use skein_core::{RuntimeCancellationToken, RuntimeMemoryReservation};
use std::io::Cursor;

fn compressed(frames: &[&[u8]]) -> Vec<u8> {
    let mut bytes = format!("{SEARCH_COMPRESSION_HEADER}\n\n").into_bytes();
    for frame in frames {
        bytes.extend(zstd::stream::encode_all(*frame, 3).unwrap());
    }
    bytes
}

fn outcome(result: Result<()>) -> &'static str {
    match result {
        Ok(()) => "unregistered",
        Err(error) if error.to_string().contains("requires its consumer owner") => "registered",
        Err(_) => "invalid",
    }
}

#[test]
fn control_probe_preserves_prefix_semantics_and_multi_frame_bindings() {
    let plain = b"SKEIN_SEARCH_PROJECTION_V1\nsource_graph_commit_epoch\t1\n";
    let registered = b"SKEIN_SEARCH_PROJECTION_V1\nprojection_consumer_binding\towner\n";
    let mut skipped = compressed(&[&registered[..16]]);
    skipped.extend_from_slice(&0x184d2a5fu32.to_le_bytes());
    skipped.extend_from_slice(&3u32.to_le_bytes());
    skipped.extend_from_slice(b"abc");
    skipped.extend(zstd::stream::encode_all(&registered[16..], 3).unwrap());
    let mut inputs = vec![
        plain.to_vec(),
        registered.to_vec(),
        compressed(&[plain]),
        compressed(&[registered]),
        compressed(&[&registered[..16], &registered[16..]]),
        compressed(&[&registered[..37], &registered[37..]]),
        skipped,
        b"SKEIN_SEARCH_PROJECTION_V1\n".to_vec(),
        b"SKEIN_SEARCH_PROJECTION_V1\n\xfftail".to_vec(),
        b"wrong header\nsource_graph_commit_epoch\t1\n".to_vec(),
        b"\xff\nsource_graph_commit_epoch\t1\n".to_vec(),
        format!("{SEARCH_COMPRESSION_HEADER}\n").into_bytes(),
        format!("{SEARCH_COMPRESSION_HEADER}\n{}\n\n", "x".repeat(1024)).into_bytes(),
        format!("{SEARCH_COMPRESSION_HEADER}\n{}\n", "x\n".repeat(2048)).into_bytes(),
    ];
    let encoded = compressed(&[registered]);
    for length in 0..encoded.len() {
        inputs.push(encoded[..length].to_vec());
    }
    for (index, bytes) in inputs.into_iter().enumerate() {
        let expected = outcome(oracle(Cursor::new(&bytes)));
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        assert_eq!(
            outcome(probe(Cursor::new(&bytes), &memory, &task)),
            expected,
            "case {index}"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn control_probe_admits_exact_peak_and_rejects_one_short() {
    let plain = b"SKEIN_SEARCH_PROJECTION_V1\nsource_graph_commit_epoch\t1\n";
    for bytes in [plain.to_vec(), compressed(&[plain])] {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        probe(Cursor::new(&bytes), &memory, &task).unwrap();
        let peak = memory.ledger.snapshot().peak_bytes;
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        for short in [false, true] {
            let task = RuntimeTaskContext::default().with_memory_reservation(
                RuntimeMemoryReservation::new((peak - usize::from(short)) as u64, 0),
            );
            let memory = BuildMemory::new(&task).unwrap();
            let result = probe(Cursor::new(&bytes), &memory, &task);
            if short {
                assert!(result.unwrap_err().to_string().contains("memory"));
            } else {
                result.unwrap();
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

struct CancellingReader<'a> {
    input: Cursor<&'a [u8]>,
    cancellation: RuntimeCancellationToken,
    cancel_after: u64,
}

impl Read for CancellingReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let length = bytes.len().min(1);
        let read = self.input.read(&mut bytes[..length])?;
        if self.input.position() >= self.cancel_after {
            self.cancellation.cancel();
        }
        Ok(read)
    }
}

#[test]
fn control_probe_observes_cancellation_during_plain_and_native_input() {
    let plain = b"SKEIN_SEARCH_PROJECTION_V1\nsource_graph_commit_epoch\t1\n";
    let encoded = compressed(&[plain]);
    for (bytes, cancel_after) in [
        (&plain[..], 1),
        (encoded.as_slice(), encoded.len() as u64 - 2),
    ] {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let input = CancellingReader {
            input: Cursor::new(bytes),
            cancellation: task.cancellation().clone(),
            cancel_after,
        };
        assert!(probe(input, &memory, &task)
            .unwrap_err()
            .to_string()
            .contains("cancel"));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

// Frozen pre-admission probe is the behavioral oracle, including prefix-only reads.
fn oracle(input: impl Read) -> Result<()> {
    let mut reader = BufReader::new(input);
    let first = oracle_line(&mut reader)?;
    if first.trim_end() == SEARCH_COMPRESSION_HEADER {
        let mut total = first.len();
        loop {
            let line = oracle_line(&mut reader)?;
            total += line.len();
            if total > 4096 {
                return Err(invalid("snapshot envelope exceeds header limit"));
            }
            if line == "\n" {
                break;
            }
            if line.is_empty() {
                return Err(invalid("incomplete snapshot envelope"));
            }
        }
        let decoder = zstd::stream::read::Decoder::new(reader)?;
        let mut reader = BufReader::new(decoder);
        let first = oracle_line(&mut reader)?;
        oracle_records(&first, &oracle_prefix(&mut reader)?)
    } else {
        oracle_records(&first, &oracle_prefix(&mut reader)?)
    }
}

fn oracle_records(first: &str, second: &str) -> Result<()> {
    if first != "SKEIN_SEARCH_PROJECTION_V1\n" {
        return Err(invalid("invalid snapshot header"));
    }
    if second.starts_with("projection_consumer_binding") {
        return Err(invalid("registered projection requires its consumer owner"));
    }
    Ok(())
}

fn oracle_prefix(reader: &mut impl Read) -> Result<String> {
    let mut prefix = Vec::with_capacity(27);
    reader.take(27).read_to_end(&mut prefix)?;
    Ok(String::from_utf8_lossy(&prefix).into_owned())
}

fn oracle_line(reader: &mut impl BufRead) -> Result<String> {
    let mut line = String::new();
    reader.take(1025).read_line(&mut line)?;
    if line.len() > 1024 {
        return Err(invalid("snapshot control record exceeds limit"));
    }
    Ok(line)
}
