// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::compression as codec;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Ledger {
    live: Arc<AtomicUsize>,
    limit: usize,
    panic_release: Arc<AtomicBool>,
}

#[derive(Debug)]
struct Permit {
    live: Arc<AtomicUsize>,
    bytes: usize,
    panic_release: Arc<AtomicBool>,
}

impl RuntimeMemoryPermit for Permit {
    fn bytes(&self) -> u64 {
        self.bytes as u64
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.live.fetch_sub(self.bytes, Ordering::SeqCst);
        assert!(
            !self.panic_release.swap(false, Ordering::SeqCst),
            "host refund panic"
        );
    }
}

impl hawdb::RuntimeMemoryController for Ledger {
    fn reserve(
        &self,
        bytes: u64,
        _: u64,
    ) -> Result<Box<dyn RuntimeMemoryPermit>, hawdb::RuntimeMemoryError> {
        let bytes = bytes as usize;
        let available = self.limit - self.live.load(Ordering::SeqCst);
        if bytes > available {
            return Err(hawdb::RuntimeMemoryError::ReservationExceeded {
                requested_bytes: bytes as u64,
                available_bytes: available as u64,
            });
        }
        self.live.fetch_add(bytes, Ordering::SeqCst);
        Ok(Box::new(Permit {
            live: self.live.clone(),
            bytes,
            panic_release: self.panic_release.clone(),
        }))
    }
}

#[test]
fn binding_admission_precedes_growth_and_cleanup_preserves_failure() {
    let ledger = Arc::new(Ledger {
        live: Arc::new(AtomicUsize::new(0)),
        limit: 2 * MIB,
        panic_release: Arc::new(AtomicBool::new(false)),
    });
    let work = CheckpointWorkContext::new(
        hawdb::RuntimeTaskContext::default().with_memory_controller(ledger.clone()),
    );
    let mut decoder = Decoder::new(Some(&work)).unwrap();
    let small = zstd::stream::encode_all(&b"small"[..], 3).unwrap();
    assert_eq!(decoder.step(&small, &mut [0; 100]).unwrap().remaining, 0);
    assert_eq!(
        ledger.live.load(Ordering::SeqCst),
        DECODER_NATIVE_BYTES + MIB
    );
    decoder.reset().unwrap();
    let before = decoder.retained_bytes();
    // Unknown-size frame with a 128 MiB window, followed by an empty last block.
    let large = [0x28, 0xb5, 0x2f, 0xfd, 0, 17 << 3, 1, 0, 0];
    assert!(decoder.step(&large, &mut [0; 100]).is_err());
    assert_eq!(decoder.retained_bytes(), before);
    drop(decoder);
    assert_eq!(ledger.live.load(Ordering::SeqCst), 0);
    assert!(ZstdBackend.encoder(3, Some(&work)).is_err());
    assert_eq!(ledger.live.load(Ordering::SeqCst), 0);
    let failure = work.classify(|scoped| -> io::Result<()> {
        let decoder = Decoder::new(Some(scoped))?;
        ledger.panic_release.store(true, Ordering::SeqCst);
        drop(decoder);
        Ok(())
    });
    assert!(failure.is_err());
    assert_eq!(ledger.live.load(Ordering::SeqCst), 0);
}

#[test]
fn binding_codec_streaming_and_reuse() {
    install();
    install();
    let mut decoder = codec::DecompressionContext::new().unwrap();
    for len in [0, 1, 100, 300_000, 2_000_000, 10] {
        let payload: Vec<_> = (0..len).map(|i| ((i * 31 + i / 123) % 251) as u8).collect();
        let encoded = codec::encode_all(payload.as_slice(), 3).unwrap();
        assert_eq!(
            zstd::stream::decode_all(encoded.as_slice()).unwrap(),
            payload
        );
        let reference = zstd::stream::encode_all(payload.as_slice(), 3).unwrap();
        decoder.reset().unwrap();
        let mut position = 0;
        let mut decoded = Vec::new();
        loop {
            let end = (position + 1).min(reference.len());
            let mut output = [0; 79];
            let progress = decoder
                .step(&reference[position..end], &mut output)
                .unwrap();
            position += progress.consumed;
            decoded.extend_from_slice(&output[..progress.produced]);
            if progress.remaining == 0 {
                break;
            }
            assert!(progress.consumed != 0 || progress.produced != 0);
        }
        assert_eq!(position, reference.len());
        assert_eq!(decoded, payload);
        assert!(decoder.sizeof() < 4 * MIB);
    }
    let mut frames = codec::encode_all(&b"first"[..], 3).unwrap();
    frames.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3]);
    frames.extend(codec::encode_all(&b"second"[..], 3).unwrap());
    let mut output = Vec::new();
    codec::Decoder::new(frames.as_slice())
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();
    assert_eq!(output, b"firstsecond");
    frames.pop();
    assert!(codec::Decoder::new(frames.as_slice())
        .unwrap()
        .read_to_end(&mut Vec::new())
        .is_err());
    assert!(ZstdBackend.encoder(22, None).is_err());
    assert!(ZstdBackend
        .decoder_buffer_bytes(&FrameHeader {
            window_bytes: MAX_WINDOW + 1,
            content_bytes: None,
            block_bytes: 128 * 1024,
            skippable: false,
        })
        .is_err());
    assert!(
        size_of::<Decoder>() + DECODER_NATIVE_BYTES < ZstdBackend.decoder_context_bytes().unwrap()
    );
}
