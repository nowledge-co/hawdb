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
use std::io::{self, IoSlice, Write};

#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    calls: usize,
    short: Option<usize>,
    interrupt_first: bool,
    fail_after: Option<usize>,
}

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_vectored(&[IoSlice::new(bytes)])
    }

    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        self.calls += 1;
        if self.interrupt_first && self.calls == 1 {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if let Some(limit) = self.fail_after
            && self.bytes.len() >= limit
        {
            return Err(io::Error::other("injected write failure"));
        }
        let count = slices.iter().map(|slice| slice.len()).sum::<usize>();
        let count = count.min(self.short.unwrap_or(count));
        let count = count.min(
            self.fail_after
                .map_or(count, |limit| limit.saturating_sub(self.bytes.len())),
        );
        self.bytes.extend(
            slices
                .iter()
                .flat_map(|slice| slice.iter())
                .take(count)
                .copied(),
        );
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn checkpoint_fragment_vectored_writes_preserve_padding_headers_and_payload_under_short_writes() {
    let work = CheckpointWorkContext::default();
    for length in [0, 1, 511, WAL_BLOCK_BYTES + 17] {
        let payload = vec![0xa5; length];
        for position in [0, (WAL_BLOCK_BYTES - 3) as u64] {
            let mut stream = CheckpointWalFrameStream::new(7, &payload, position, &work).unwrap();
            let mut expected = Vec::new();
            let mut full = Sink::default();
            let mut short = Sink {
                short: Some(5),
                interrupt_first: true,
                ..Default::default()
            };
            let mut fragments = 0;
            while let Some(fragment) = stream.next().unwrap() {
                for part in fragment.parts() {
                    expected.extend_from_slice(part);
                }
                fragment.write_to(&mut full).unwrap();
                fragment.write_to(&mut short).unwrap();
                fragments += 1;
            }
            assert_eq!(full.bytes, expected);
            assert_eq!(short.bytes, expected);
            assert_eq!(full.calls, fragments);
            assert!(short.calls > full.calls);
        }
    }
}

#[test]
fn checkpoint_fragment_vectored_writes_propagate_partial_failure_and_write_zero() {
    let work = CheckpointWorkContext::default();
    let mut stream = CheckpointWalFrameStream::new(7, b"payload", 0, &work).unwrap();
    let fragment = stream.next().unwrap().unwrap();
    let expected: Vec<_> = fragment.parts().into_iter().flatten().copied().collect();
    let mut failing = Sink {
        fail_after: Some(5),
        ..Default::default()
    };
    assert_eq!(
        fragment.write_to(&mut failing).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    assert_eq!(failing.bytes, expected[..5]);
    let mut zero = Sink {
        short: Some(0),
        ..Default::default()
    };
    assert_eq!(
        fragment.write_to(&mut zero).unwrap_err().kind(),
        io::ErrorKind::WriteZero
    );
    assert!(zero.bytes.is_empty());
}
