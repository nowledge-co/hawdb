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

//! The ordinary checkpoint decode and format-validation passes read the
//! payload in blocks (#1440) with the same results and diagnostics as the
//! unbuffered field-by-field input.

use super::*;

const BLOCK: usize = ReadAheadFileInput::READ_AHEAD_BYTES;

fn checkpoint_file(name: &str, payload: &[u8]) -> (std::path::PathBuf, Vec<u8>) {
    let path = std::env::temp_dir().join(format!(
        "hawdb-relational-read-ahead-{name}-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let encoded = encode_envelope(CHECKPOINT_MAGIC, 7, payload.to_vec()).unwrap();
    std::fs::write(&path, &encoded).unwrap();
    (path, encoded)
}

fn open(path: &Path, encoded: &[u8]) -> ReadAheadFileInput {
    let (_, input) = FileDecodeInput::open_checkpoint(
        path,
        encoded.len() as u64,
        RelationalDecodeLimits::checkpoint(),
    )
    .unwrap();
    ReadAheadFileInput::new(input)
}

#[test]
fn fields_across_blocks_match_the_payload_and_reads_are_block_sized() {
    let payload: Vec<u8> = (0..3 * BLOCK + 257)
        .map(|offset| (offset * 31) as u8)
        .collect();
    let (path, encoded) = checkpoint_file("fields", &payload);
    let mut input = open(&path, &encoded);
    assert_eq!(input.len(), payload.len());

    // The first one-byte field reads one whole block from the file.
    let mut byte = [0_u8; 1];
    input.read_exact(&mut byte).unwrap();
    assert_eq!(byte[0], payload[0]);
    assert_eq!(input.position(), 1);
    assert_eq!(input.inner.offset, BLOCK);

    // Mixed field sizes, including fields that straddle block boundaries
    // and one larger than a block.
    let mut actual = byte.to_vec();
    let mut sizes = [3, 8, 7_000, BLOCK + 11, 1, 2, 4096].into_iter().cycle();
    while input.position() < input.len() {
        let size = sizes.next().unwrap().min(input.len() - input.position());
        let mut field = vec![0; size];
        input.read_exact(&mut field).unwrap();
        actual.extend_from_slice(&field);
        assert_eq!(input.position(), actual.len());
        assert!(input.inner.offset - input.position() <= BLOCK);
    }
    assert_eq!(actual, payload);
    assert_eq!(input.inner.offset, payload.len());
    input.finish().unwrap();
    std::fs::remove_file(path).unwrap();
}

#[test]
fn truncation_and_checksum_errors_are_unchanged() {
    let payload: Vec<u8> = (0..BLOCK + 100).map(|offset| offset as u8).collect();
    let (path, mut encoded) = checkpoint_file("errors", &payload);

    // A request past the payload end fails before any read-ahead and leaves
    // the logical position where it was.
    let mut input = open(&path, &encoded);
    let mut head = vec![0; 10];
    input.read_exact(&mut head).unwrap();
    let underlying = input.inner.offset;
    let mut too_long = vec![0xa5; payload.len()];
    let error = input.read_exact(&mut too_long).unwrap_err();
    assert!(
        matches!(&error, RelationalError::Corruption(message) if message == "truncated relational durable payload"),
        "{error:?}"
    );
    assert_eq!(input.position(), 10);
    // No read-ahead happened for the refused request, and the destination
    // was not written.
    assert_eq!(input.inner.offset, underlying);
    assert!(too_long.iter().all(|byte| *byte == 0xa5));
    drop(input);

    // A flipped byte in the last block is reported by `finish`, after every
    // block has been hashed, as the unbuffered input reports it.
    *encoded.last_mut().unwrap() ^= 0x5a;
    std::fs::write(&path, &encoded).unwrap();
    let mut input = open(&path, &encoded);
    let mut all = vec![0; payload.len()];
    input.read_exact(&mut all).unwrap();
    let error = input.finish().unwrap_err();
    assert!(
        matches!(&error, RelationalError::Corruption(message) if message == "relational durable record checksum mismatch"),
        "{error:?}"
    );
    std::fs::remove_file(path).unwrap();
}

#[test]
fn verify_remaining_hashes_the_unread_tail() {
    let payload: Vec<u8> = (0..2 * BLOCK + 9)
        .map(|offset| (offset ^ 0x33) as u8)
        .collect();
    let (path, mut encoded) = checkpoint_file("remaining", &payload);
    let mut input = open(&path, &encoded);
    let mut prefix = vec![0; 5];
    input.read_exact(&mut prefix).unwrap();
    input.verify_remaining().unwrap();

    let middle = encoded.len() - BLOCK;
    encoded[middle] ^= 1;
    std::fs::write(&path, &encoded).unwrap();
    let mut input = open(&path, &encoded);
    input.read_exact(&mut prefix).unwrap();
    assert!(input.verify_remaining().is_err());
    std::fs::remove_file(path).unwrap();
}
