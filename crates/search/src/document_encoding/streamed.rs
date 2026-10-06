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

//! Capture a one-shot UTF-8 body in the existing spool wire grammar.

use super::*;
use crate::analyzer_stream::Control;
use hawdb_integrity::Crc32cHasher;
#[cfg(test)]
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write as IoWrite};

struct DigestWriter<'a, W> {
    output: &'a mut W,
    digest: Crc32cHasher,
}

impl<W: IoWrite> IoWrite for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.output.write(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(crate) fn record_len(header: Header<'_>, body_bytes: u64) -> Result<u64> {
    let body_encoded = body_bytes
        .checked_mul(2)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| HawDBError::Storage("streamed document length overflow".into()))?;
    let mut length = EncodedLength::default();
    write_document_prefix(&mut length, header.id, header.title)
        .and_then(|()| write_document_suffix(&mut length, header.embedding, header.metadata))
        .map_err(|_| HawDBError::Storage("streamed document header length overflow".into()))?;
    length
        .0
        .checked_add(body_encoded)
        .map(|length| length as u64)
        .ok_or_else(|| HawDBError::Storage("streamed record length overflow".into()))
}

pub(crate) struct Receipt {
    pub(crate) bytes: u64,
    pub(crate) checksum: u64,
    pub(crate) needs_chinese: bool,
}

pub(crate) fn write_frame(
    output: &mut (impl IoWrite + Seek),
    header: Header<'_>,
    body: &mut impl Read,
    source: crate::SearchDocumentBody,
    max_record_bytes: u64,
    control: Control<'_>,
) -> Result<Receipt> {
    let bytes = record_len(header, source.bytes)?;
    if bytes > max_record_bytes {
        return Err(HawDBError::Storage(
            "streamed record exceeds encoded byte admission".into(),
        ));
    }
    control.check()?;
    let start = output.stream_position()?;
    output.write_all(&bytes.to_le_bytes())?;
    output.write_all(&0u64.to_le_bytes())?;
    let receipt = write_record(output, header, body, source, control)?;
    let end = output.stream_position()?;
    control.check()?;
    let checksum_offset = start
        .checked_add(8)
        .ok_or_else(|| HawDBError::Storage("streamed frame offset overflow".into()))?;
    output.seek(SeekFrom::Start(checksum_offset))?;
    output.write_all(&receipt.checksum.to_le_bytes())?;
    output.seek(SeekFrom::Start(end))?;
    control.check()?;
    Ok(receipt)
}

pub(crate) fn write_record(
    output: &mut impl IoWrite,
    header: Header<'_>,
    body: &mut impl Read,
    source: crate::SearchDocumentBody,
    control: Control<'_>,
) -> Result<Receipt> {
    let bytes = record_len(header, source.bytes)?;
    let bytes = usize::try_from(bytes)
        .map_err(|_| HawDBError::Storage("streamed record exceeds usize".into()))?;
    let _scratch = control
        .memory
        .map(|memory| memory.spool.reserve(HEX_BUFFER_BYTES))
        .transpose()?;
    if let Some(task) = control.task {
        crate::build_control::checkpoint(task)?;
    }
    let mut digest = DigestWriter {
        output,
        digest: Crc32cHasher::new(),
    };
    let mut sink = IoSink {
        writer: &mut digest,
        error: None,
        remaining: bytes,
    };
    write_document_prefix(&mut sink, header.id, header.title).map_err(|_| {
        sink.error
            .take()
            .unwrap_or_else(|| io::Error::other("streamed header write failed"))
    })?;
    let mut body_checksum = Crc32cHasher::new();
    let mut needs_chinese = false;
    let read = crate::analyzer_stream::reader::utf8::visit(body, control, source.bytes, |text| {
        body_checksum.update(text.as_bytes());
        needs_chinese |= text.chars().any(crate::cjk_tokenizer::is_han_search_char);
        sink.write_hex(text)
            .map_err(|_| {
                sink.error
                    .take()
                    .unwrap_or_else(|| io::Error::other("streamed body write failed"))
            })
            .map_err(Into::into)
    })?;
    if read != source.bytes {
        return Err(HawDBError::Storage(
            "streamed source length mismatch".into(),
        ));
    }
    if source
        .expected_checksum
        .is_some_and(|expected| expected != body_checksum.finish())
    {
        return Err(HawDBError::Storage(
            "streamed source checksum mismatch".into(),
        ));
    }
    sink.write_checked(|sink| write_document_suffix(sink, header.embedding, header.metadata))?;
    Ok(Receipt {
        bytes: bytes as u64,
        checksum: digest.digest.finish(),
        needs_chinese,
    })
}

#[test]
fn streamed_frame_preserves_owned_wire_bytes_and_checksum() {
    let document = SearchDocument {
        id: "id\t\n".into(),
        title: "title".into(),
        content: "source\u{4e2d}\u{6587}\t\n".into(),
        embedding: Some(vec![0.5, -1.25, 0.0]),
        metadata: BTreeMap::from([("kind".into(), "memo".into())]),
    };
    let encoded = DocumentEncoding::new(&document).unwrap().encode().unwrap();
    let mut expected = Crc32cHasher::new();
    expected.update(encoded.as_bytes());
    let mut output = io::Cursor::new(Vec::new());
    let receipt = write_frame(
        &mut output,
        Header {
            id: &document.id,
            title: &document.title,
            embedding: document.embedding.as_deref(),
            metadata: &document.metadata,
        },
        &mut document.content.as_bytes(),
        crate::SearchDocumentBody {
            bytes: document.content.len() as u64,
            expected_checksum: None,
        },
        u64::MAX,
        Control::default(),
    )
    .unwrap();
    let (length, checksum) = (receipt.bytes, receipt.checksum);
    assert_eq!(length, encoded.len() as u64);
    assert_eq!(checksum, expected.finish());
    assert_eq!(&output.get_ref()[..8], &length.to_le_bytes());
    assert_eq!(&output.get_ref()[8..16], &checksum.to_le_bytes());
    assert_eq!(&output.get_ref()[16..], encoded.as_bytes());
    assert_eq!(output.position(), output.get_ref().len() as u64);
}

#[test]
fn rejected_encoded_length_never_reads_or_writes() {
    struct NoRead;
    impl Read for NoRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("source read before admission")
        }
    }
    for body_bytes in [32 * 1024 * 1024, u64::MAX] {
        let mut output = io::Cursor::new(Vec::new());
        let metadata = BTreeMap::new();
        assert!(write_frame(
            &mut output,
            Header {
                id: "id",
                title: "",
                embedding: None,
                metadata: &metadata
            },
            &mut NoRead,
            crate::SearchDocumentBody {
                bytes: body_bytes,
                expected_checksum: None
            },
            16 * 1024 * 1024,
            Control::default()
        )
        .is_err());
        assert!(output.get_ref().is_empty());
    }
}
