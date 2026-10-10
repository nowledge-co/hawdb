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

//! One WAL record envelope for ordinary recovery and cooperative checkpoints.
//! Allocation and work admission remain statically dispatched backend hooks.

use super::*;

pub(super) trait RecordDecoder {
    fn with_unit<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T>;
    fn decode_body(&self, code: u64, body: &[u8]) -> Result<WalOp>;
    fn batch(&self, count: u32) -> Vec<WalOp>;
    fn push(&self, batch: &mut Vec<WalOp>, op: WalOp) -> Result<()>;
}

pub(super) fn decode_record<D: RecordDecoder>(
    bytes: &[u8],
    decoder: &D,
) -> Result<BinaryWalRecordDecode> {
    let (lsn, record_kind, commit_epoch, op_count) = decoder.with_unit(|| {
        if bytes.len() < 21 {
            return Err(HawDBError::Storage(
                "binary WAL record envelope is truncated".to_string(),
            ));
        }
        let lsn = u64::from_le_bytes(bytes[0..8].try_into().expect("8-byte lsn"));
        let record_kind = bytes[8];
        let commit_epoch =
            u64::from_le_bytes(bytes[9..17].try_into().expect("8-byte commit epoch"));
        let op_count = u32::from_le_bytes(bytes[17..21].try_into().expect("4-byte op count"));
        Ok((lsn, record_kind, commit_epoch, op_count))
    })?;
    let mut pos = 21usize;
    let op = match record_kind {
        RECORD_KIND_SINGLE => {
            if op_count != 1 {
                return Err(HawDBError::Storage(format!(
                    "single-op WAL record declares op_count {op_count}"
                )));
            }
            let op = decode_frame(bytes, &mut pos, decoder)?;
            if let WalOp::Batch(_) = op {
                return Err(HawDBError::Storage(
                    "single-op WAL record carries a batch envelope".to_string(),
                ));
            }
            op
        }
        RECORD_KIND_BATCH => {
            let mut ops = decoder.batch(op_count);
            for _ in 0..op_count {
                decoder.push(&mut ops, decode_frame(bytes, &mut pos, decoder)?)?;
            }
            WalOp::Batch(ops)
        }
        kind => {
            return Err(HawDBError::Storage(format!(
                "unknown WAL record kind {kind}"
            )));
        }
    };
    if pos != bytes.len() {
        return Err(HawDBError::Storage(format!(
            "binary WAL record has {} trailing bytes",
            bytes.len() - pos
        )));
    }
    Ok(BinaryWalRecordDecode::Entry {
        entry: WalEntry { lsn, op },
        commit_epoch,
    })
}

pub(super) fn decode_frame<D: RecordDecoder>(
    bytes: &[u8],
    pos: &mut usize,
    decoder: &D,
) -> Result<WalOp> {
    let (code, body) = decoder.with_unit(|| {
        let code = decode_varint_u64(bytes, pos)?;
        let body = decode_len_body(bytes, pos)?;
        Ok((code, body))
    })?;
    decoder.decode_body(code, body)
}
