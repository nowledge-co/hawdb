// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use std::cell::Cell;
use std::ptr;

#[derive(Clone, Copy, Default, Debug)]
struct Activity {
    allocations: usize,
    frees: usize,
    allocated_bytes: usize,
    freed_bytes: usize,
    fail_after: Option<usize>,
}

thread_local! {
    static ACTIVITY: Cell<Option<Activity>> = const { Cell::new(None) };
}

pub(super) fn reject_allocation() -> bool {
    ACTIVITY
        .try_with(|cell| {
            let Some(mut activity) = cell.get() else {
                return false;
            };
            let Some(remaining) = activity.fail_after else {
                return false;
            };
            if remaining == 0 {
                return true;
            }
            activity.fail_after = Some(remaining - 1);
            cell.set(Some(activity));
            false
        })
        .unwrap_or(false)
}

pub(super) fn record_allocation(bytes: usize, allocate: bool) {
    let _ = ACTIVITY.try_with(|cell| {
        if let Some(mut activity) = cell.get() {
            if allocate {
                activity.allocations = activity.allocations.saturating_add(1);
                activity.allocated_bytes = activity.allocated_bytes.saturating_add(bytes);
            } else {
                activity.frees = activity.frees.saturating_add(1);
                activity.freed_bytes = activity.freed_bytes.saturating_add(bytes);
            }
            cell.set(Some(activity));
        }
    });
}

fn observe<T>(fail_after: Option<usize>, run: impl FnOnce() -> T) -> (T, Activity) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVITY.set(None);
        }
    }
    assert!(ACTIVITY
        .replace(Some(Activity {
            fail_after,
            ..Activity::default()
        }))
        .is_none());
    let reset = Reset;
    let result = run();
    let activity = ACTIVITY.get().unwrap();
    drop(reset);
    assert_eq!(activity.allocations, activity.frees, "{activity:?}");
    assert_eq!(
        activity.allocated_bytes, activity.freed_bytes,
        "{activity:?}"
    );
    (result, activity)
}

#[test]
fn allocation_prefix_preserves_alignment_and_handles_null_and_overflow() {
    let (_, activity) = observe(None, || {
        // SAFETY: each successful allocation is freed once with its original pointer.
        unsafe {
            context::free(ptr::null_mut(), ptr::null_mut());
            assert!(context::allocate(ptr::null_mut(), usize::MAX).is_null());
            assert!(context::allocate(ptr::null_mut(), isize::MAX as usize).is_null());
            for size in [0, 1, 15, 16, 17, 4096] {
                let allocation = context::allocate(ptr::null_mut(), size);
                assert!(!allocation.is_null());
                assert_eq!(allocation as usize % 16, 0);
                ptr::write_bytes(allocation.cast::<u8>(), 0xa5, size);
                context::free(ptr::null_mut(), allocation);
            }
        }
    });
    assert_eq!(activity.allocations, 6);
}

#[test]
fn streams_match_the_existing_codec_and_release_all_native_allocations() {
    let input: Vec<_> = (0..300_000).map(|i| (i * 37) as u8).collect();
    for level in [-1, 0, 3, 9] {
        let oracle = zstd::stream::encode_all(input.as_slice(), level).unwrap();
        let (encoded, activity) = observe(None, || encode_all(input.as_slice(), level).unwrap());
        assert_eq!(encoded, oracle);
        assert!(activity.allocations >= 2);
        let (decoded, activity) = observe(None, || {
            let mut decoder = Decoder::new(encoded.as_slice()).unwrap();
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded).unwrap();
            decoded
        });
        assert_eq!(decoded, input);
        assert!(activity.allocations >= 2);
    }
    let empty = encode_all(&[][..], 3).unwrap();
    assert_eq!(empty, zstd::stream::encode_all(&[][..], 3).unwrap());
}

#[test]
fn tiny_io_buffers_support_flush_concatenation_and_skippable_frames() {
    let mut encoder = Encoder::new(Vec::new(), 3).unwrap();
    encoder.write_all(b"first").unwrap();
    encoder.flush().unwrap();
    encoder.write_all(b"second").unwrap();
    let mut frames = encoder.finish().unwrap();
    frames.extend(0x184d2a50u32.to_le_bytes());
    frames.extend(3u32.to_le_bytes());
    frames.extend(b"xyz");
    frames.extend(zstd::stream::encode_all(&b"third"[..], 3).unwrap());
    let (decoded, _) = observe(None, || {
        let mut decoder =
            Decoder::with_buffer(BufReader::with_capacity(1, frames.as_slice())).unwrap();
        let mut decoded = Vec::new();
        let mut byte = [0];
        while decoder.read(&mut byte).unwrap() != 0 {
            decoded.push(byte[0]);
        }
        decoded
    });
    assert_eq!(decoded, b"firstsecondthird");
}

#[test]
fn context_and_workspace_failures_release_previous_allocations() {
    let input = vec![7; 300_000];
    let encoded = zstd::stream::encode_all(input.as_slice(), 3).unwrap();
    for fail_after in [0, 1] {
        let (result, _) = observe(Some(fail_after), || encode_all(input.as_slice(), 3));
        assert!(result.is_err());
        let (result, _) = observe(Some(fail_after), || {
            let mut decoder = Decoder::new(encoded.as_slice())?;
            decoder.read_to_end(&mut Vec::new())
        });
        assert!(result.is_err());
    }
}

#[test]
fn malformed_truncated_and_failed_sink_paths_release_workspaces() {
    let encoded = encode_all(&b"test payload"[..], 3).unwrap();
    for input in [&b"invalid frame"[..], &encoded[..encoded.len() - 1]] {
        let (result, _) = observe(None, || {
            let mut decoder = Decoder::new(input)?;
            decoder.read_to_end(&mut Vec::new())
        });
        assert!(result.is_err());
    }
    struct FailedSink;
    impl Write for FailedSink {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected sink failure"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (result, _) = observe(None, || {
        let mut encoder = Encoder::new(FailedSink, 3)?;
        encoder.write_all(b"test payload")?;
        encoder.finish()
    });
    assert!(result.is_err());
}

#[test]
fn short_magic_prefixes_report_incomplete_frame_and_release_context() {
    let magic = 0xfd2fb528u32.to_le_bytes();
    for length in 1..magic.len() {
        let (_, activity) = observe(None, || {
            let input = BufReader::with_capacity(1, &magic[..length]);
            let mut decoder = Decoder::with_buffer(input).unwrap();
            let mut decoded = Vec::new();
            let error = decoder.read_to_end(&mut decoded).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(error.to_string(), "incomplete frame");
            assert!(decoded.is_empty());
        });
        assert_eq!(activity.allocations, 1);
    }
}

#[test]
fn native_error_requires_reset_before_context_reuse() {
    let mut context = DecompressionContext::new().unwrap();
    let mut bytes = [0; 64];
    let mut output = OutBuffer::around(&mut bytes[..]);
    assert!(context
        .decompress_stream(&mut output, &mut InBuffer::around(b"invalid frame"))
        .is_err());
    assert!(context
        .decompress_stream(&mut output, &mut InBuffer::around(&[]))
        .is_err());
    context.reset().unwrap();
    let encoded = encode_all(&b"valid"[..], 3).unwrap();
    let mut output = OutBuffer::around(&mut bytes[..]);
    let mut input = InBuffer::around(&encoded);
    while context.decompress_stream(&mut output, &mut input).unwrap() != 0 {}
    let written = output.pos();
    assert_eq!(&bytes[..written], b"valid");
}

#[test]
fn legacy_magic_is_rejected_before_creating_a_libc_owned_decoder() {
    let (_, activity) = observe(None, || {
        let mut decoder = Decoder::with_buffer(&[0x27, 0xb5, 0x2f, 0xfd][..]).unwrap();
        let error = decoder.read(&mut [0; 8]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    });
    assert_eq!(activity.allocations, 1);
}

#[test]
fn partially_used_context_can_move_to_another_host_thread() {
    let encoded = encode_all(&b"cross-thread ownership"[..], 3).unwrap();
    let mut decoder = Decoder::new(encoded.as_slice()).unwrap();
    let mut first = [0];
    decoder.read_exact(&mut first).unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut remaining = Vec::new();
                decoder.read_to_end(&mut remaining).unwrap();
                assert_eq!(remaining, b"ross-thread ownership");
            })
            .join()
            .unwrap();
    });
    assert_eq!(first, [b'c']);
}
