// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(feature = "builtin"))]

use hawdb::compression::*;
use hawdb::{Database, Value};
use hawdb_storage::compression as codec;
use std::io;
use std::sync::Arc;

#[cfg(feature = "host-zstd")]
mod host;

#[cfg(not(feature = "host-zstd"))]
#[derive(Debug)]
struct RefusingBackend;

#[cfg(not(feature = "host-zstd"))]
impl CompressionBackend for RefusingBackend {
    fn encoder(&self, _: i32, _: Option<&CheckpointWorkContext>) -> io::Result<EncoderState> {
        Err(io::Error::other("host refusal"))
    }
    fn decoder(&self, _: Option<&CheckpointWorkContext>) -> io::Result<DecoderState> {
        Err(io::Error::other("host refusal"))
    }
    fn encoder_workspace_bytes(&self, _: i32) -> io::Result<usize> {
        Err(io::Error::other("host refusal"))
    }
    fn decoder_context_bytes(&self) -> io::Result<usize> {
        Err(io::Error::other("host refusal"))
    }
    fn decoder_buffer_bytes(&self, _: &FrameHeader) -> io::Result<usize> {
        Err(io::Error::other("host refusal"))
    }
    fn compress_bound(&self, _: usize) -> io::Result<usize> {
        Err(io::Error::other("host refusal"))
    }
}

// One test owns this process-wide initialization contract. Each feature mode
// builds a separate consumer artifact, outside HawDB's workspace/dev features.
#[test]
fn process_backend_contract() {
    let error = codec::encode_all(&b"before registration"[..], 3).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    let mut db = Database::new();
    db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO items VALUES (42)").unwrap();
    assert_eq!(
        db.query_sql("SELECT id FROM items").unwrap().rows[0].get("id"),
        Some(&Value::Int(42))
    );

    #[cfg(not(feature = "host-zstd"))]
    {
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let winners = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        install_compression_backend(Arc::new(RefusingBackend)).is_ok()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>()
        });
        assert_eq!(winners, 1);
        assert!(install_compression_backend(Arc::new(RefusingBackend)).is_err());
        assert!(codec::encode_all(&b"after registration"[..], 3)
            .unwrap_err()
            .to_string()
            .contains("host refusal"));
    }

    #[cfg(feature = "host-zstd")]
    injected_backend();
}

#[cfg(feature = "host-zstd")]
fn injected_backend() {
    use std::io::{Read, Write};
    use std::sync::atomic::Ordering;

    let text = "\u{4e2d}\u{6587}\u{5206}\u{8bcd}";
    let mut pending = hawdb::SearchIndex::default();
    pending
        .upsert(hawdb::SearchDocument {
            id: "dictionary-retry".into(),
            title: text.into(),
            content: text.into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    let error = pending
        .search(text, None, hawdb::SearchMode::Text, 1)
        .unwrap_err();
    assert!(error.to_string().contains("compression backend"), "{error}");

    let backend = Arc::new(host::HostBackend::default());
    install_compression_backend(backend.clone()).unwrap();
    assert_eq!(
        pending
            .search(text, None, hawdb::SearchMode::Text, 1)
            .unwrap()
            .len(),
        1
    );
    let input = vec![42; 300_000];
    let encoded = codec::encode_all(input.as_slice(), 3).unwrap();
    assert_eq!(zstd::stream::decode_all(encoded.as_slice()).unwrap(), input);
    let reference = zstd::stream::encode_all(input.as_slice(), 3).unwrap();
    let mut decoder = codec::Decoder::new(reference.as_slice()).unwrap();
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, input);
    for fault in [1, 2, 3, 5] {
        backend.fault.store(fault, Ordering::SeqCst);
        let mut encoder = codec::Encoder::new(Vec::new(), 3).unwrap();
        assert!(encoder.write_all(&input).is_err());
        assert!(encoder.finish().is_err());
        let mut decoder = codec::Decoder::new(reference.as_slice()).unwrap();
        assert!(decoder.read_to_end(&mut Vec::new()).is_err());
        assert!(decoder.read(&mut [0]).is_err());
    }
    backend.fault.store(4, Ordering::SeqCst);
    assert!(codec::encode_all(input.as_slice(), 3).is_err());
    let mut frames = codec::FrameEncoder::new(3).unwrap();
    let mut frame = frames.frame(Some(input.len() as u64), Vec::new()).unwrap();
    frame.write_all(&input).unwrap();
    frame.finish().unwrap();
    assert!(frames.close().is_err());
    backend.fault.store(0, Ordering::SeqCst);
    assert!(install_compression_backend(Arc::new(host::HostBackend::default())).is_err());

    let mut db = Database::new();
    db.query("CREATE (:Memory {title: '\u{4e2d}\u{6587}\u{5206}\u{8bcd}'})")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.title AS title")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(backend.encoders.load(Ordering::SeqCst) > 0);
    assert!(backend.decoders.load(Ordering::SeqCst) > 0);
}
