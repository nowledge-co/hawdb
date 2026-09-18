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
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug)]
enum Action {
    Read(usize),
    Interrupt,
    Eof,
    Deny,
}

#[derive(Clone)]
struct ScriptedReader {
    cursor: Cursor<Vec<u8>>,
    actions: VecDeque<Action>,
    chunk: usize,
    calls: usize,
}

impl Read for ScriptedReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.calls += 1;
        match self.actions.pop_front().unwrap_or(Action::Read(self.chunk)) {
            Action::Read(limit) => {
                let length = buffer.len().min(limit);
                self.cursor.read(&mut buffer[..length])
            }
            Action::Interrupt => Err(ErrorKind::Interrupted.into()),
            Action::Eof => Ok(0),
            Action::Deny => Err(ErrorKind::PermissionDenied.into()),
        }
    }
}

fn compare_with_read_exact(
    length: usize,
    offset: u64,
    chunk: usize,
    actions: Vec<Action>,
) -> Option<ErrorKind> {
    let mut positioned = ScriptedReader {
        cursor: Cursor::new(
            (0u8..64)
                .map(|byte| byte.wrapping_mul(37).wrapping_add(length as u8))
                .collect(),
        ),
        actions: actions.into(),
        chunk,
        calls: 0,
    };
    let mut reference = positioned.clone();
    let mut actual = vec![0xcc; length];
    let result = read_exact_with(&mut actual, offset, |buffer, actual_offset| {
        assert_eq!(actual_offset, offset + positioned.cursor.position());
        positioned.read(buffer)
    });
    let error = result.as_ref().err().map(io::Error::kind);
    // A wider independent calculation determines the offset-domain boundary.
    if u128::from(offset) + length as u128 > u128::from(u64::MAX) {
        assert_eq!(error, Some(ErrorKind::InvalidInput));
        assert_eq!(positioned.calls, 0);
        assert_eq!(actual, vec![0xcc; length]);
    } else {
        let mut expected = vec![0xcc; length];
        let expected_result = reference.read_exact(&mut expected);
        assert_eq!(error, expected_result.err().map(|error| error.kind()));
        assert_eq!(actual, expected);
        assert_eq!(positioned.cursor.position(), reference.cursor.position());
        assert_eq!(positioned.calls, reference.calls);
    }
    error
}

#[test]
fn empty_read_never_invokes_the_backend() {
    for offset in [0, 17, u64::MAX] {
        read_exact_with(&mut [], offset, |_, _| panic!("empty read performed I/O")).unwrap();
    }
}

#[test]
fn short_reads_and_interruptions_preserve_the_next_offset() {
    assert_eq!(
        compare_with_read_exact(
            17,
            29,
            3,
            vec![
                Action::Interrupt,
                Action::Interrupt,
                Action::Read(2),
                Action::Interrupt,
                Action::Read(1),
                Action::Interrupt,
            ],
        ),
        None
    );
}

#[test]
fn short_read_eof_is_not_a_successful_partial_payload() {
    for actions in [vec![Action::Eof], vec![Action::Read(3), Action::Eof]] {
        assert_eq!(
            compare_with_read_exact(8, 13, 2, actions),
            Some(ErrorKind::UnexpectedEof)
        );
    }
}

#[test]
fn hard_errors_are_preserved_without_retry() {
    for actions in [vec![Action::Deny], vec![Action::Read(3), Action::Deny]] {
        assert_eq!(
            compare_with_read_exact(8, 13, 2, actions),
            Some(ErrorKind::PermissionDenied)
        );
    }
}

#[test]
fn overflow_is_rejected_before_reading_or_modifying_the_buffer() {
    for (offset, length) in [(u64::MAX, 1), (u64::MAX - 1, 2), (u64::MAX - 4, 8)] {
        assert_eq!(
            compare_with_read_exact(length, offset, 1, Vec::new()),
            Some(ErrorKind::InvalidInput)
        );
    }
    assert_eq!(
        compare_with_read_exact(4, u64::MAX - 4, 1, Vec::new()),
        None
    );
}

struct TestFile {
    file: Option<File>,
    path: PathBuf,
}

impl TestFile {
    fn new(bytes: &[u8]) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        for _ in 0..128 {
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "hawdb-positioned-read-{}-{sequence}",
                std::process::id()
            ));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    let fixture = Self {
                        file: Some(file.try_clone().unwrap()),
                        path,
                    };
                    file.write_all(bytes).unwrap();
                    file.seek(SeekFrom::Start(0)).unwrap();
                    return fixture;
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create positioned-read fixture: {error}"),
            }
        }
        panic!("positioned-read fixture collision retries exhausted");
    }

    fn file(&self) -> &File {
        self.file.as_ref().unwrap()
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn native_file_reads_exact_ranges_and_rejects_eof() {
    let bytes: Vec<_> = (0u8..=255).collect();
    let fixture = TestFile::new(&bytes);
    let mut cursor = fixture.file();
    cursor.seek(SeekFrom::Start(9)).unwrap();
    let mut buffer = [0; 31];
    read_exact_at(fixture.file(), &mut buffer, 17).unwrap();
    assert_eq!(buffer, bytes[17..48]);
    #[cfg(unix)]
    assert_eq!(cursor.stream_position().unwrap(), 9);
    #[cfg(windows)]
    assert_eq!(cursor.stream_position().unwrap(), 48);
    assert_eq!(
        read_exact_at(fixture.file(), &mut buffer, 250)
            .unwrap_err()
            .kind(),
        ErrorKind::UnexpectedEof
    );
    assert_eq!(
        read_exact_at(fixture.file(), &mut buffer, u64::MAX)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let position = cursor.stream_position().unwrap();
    read_exact_at(fixture.file(), &mut [], u64::MAX).unwrap();
    assert_eq!(cursor.stream_position().unwrap(), position);
}

fn concurrent_clone_reads(read: fn(&File, &mut [u8], u64) -> io::Result<()>) {
    let bytes: Vec<_> = (0..1024).map(|index| (index % 251) as u8).collect();
    let fixture = TestFile::new(&bytes);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let file = fixture.file().try_clone().unwrap();
            let bytes = &bytes;
            scope.spawn(move || {
                for round in 0..64 {
                    for length in [0, 1, 8, 31, 64] {
                        let offset = (worker * 17 + round * 13) % (bytes.len() - length + 1);
                        let mut buffer = vec![0; length];
                        read(&file, &mut buffer, offset as u64).unwrap();
                        assert_eq!(buffer, bytes[offset..offset + length]);
                    }
                }
            });
        }
    });
}

#[test]
fn native_positioned_reads_are_safe_across_file_clones() {
    concurrent_clone_reads(read_exact_at);
}

#[test]
fn fallback_serializes_shared_cursor_reads_across_file_clones() {
    concurrent_clone_reads(read_exact_at_fallback);
}

#[test]
#[ignore = "manual local scripted positioned-read differential campaign"]
fn positioned_read_differential_campaign() {
    let mut cases = 0;
    let mut passed = 0;
    let mut eof = 0;
    let mut denied = 0;
    let mut overflow = 0;
    for length in 0..=32 {
        for offset in [0, 17, u64::MAX - 32, u64::MAX - 1, u64::MAX] {
            for chunk in [1, 2, 5, 64] {
                for interruptions in [0, 1, 3] {
                    for fault in [None, Some(Action::Eof), Some(Action::Deny)] {
                        let mut actions = vec![Action::Interrupt; interruptions];
                        actions.push(Action::Read(chunk));
                        actions.extend(std::iter::repeat_n(Action::Interrupt, interruptions));
                        actions.extend(fault);
                        match compare_with_read_exact(length, offset, chunk, actions) {
                            None => passed += 1,
                            Some(ErrorKind::UnexpectedEof) => eof += 1,
                            Some(ErrorKind::PermissionDenied) => denied += 1,
                            Some(ErrorKind::InvalidInput) => overflow += 1,
                            error => panic!("unexpected campaign outcome: {error:?}"),
                        }
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 5940);
    assert_eq!(overflow, 2268);
    assert!(passed > 0 && eof > 0 && denied > 0);
    println!("cases={cases}; success={passed}; eof={eof}; denied={denied}; overflow={overflow}");
}
