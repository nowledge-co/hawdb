use super::*;
use crate::{SearchSegmentDescriptor, SearchSegmentPayloadRange};
use skein_integrity::Crc32cHasher;

pub(crate) struct DescriptorEncoding<'a> {
    descriptor: &'a SearchSegmentDescriptor,
    body_bytes: usize,
    footer: String,
    bytes: usize,
}

impl<'a> DescriptorEncoding<'a> {
    pub(crate) fn new(descriptor: &'a SearchSegmentDescriptor, max_bytes: u64) -> Result<Self> {
        let mut length = EncodedLength::default();
        write_body(&mut length, descriptor).map_err(|_| size_overflow())?;
        let mut encoding = Self {
            descriptor,
            body_bytes: length.0,
            footer: String::new(),
            bytes: 0,
        };
        let mut digest = DigestWriter(Crc32cHasher::new());
        encoding.write_body_to(&mut digest)?;
        encoding.footer = format!("checksum\t{}\n", digest.0.finish());
        encoding.bytes = encoding
            .body_bytes
            .checked_add(encoding.footer.len())
            .ok_or_else(size_overflow)?;
        if encoding.bytes as u64 > max_bytes {
            return Err(SkeinError::Storage(format!(
                "search generation descriptor requires {} bytes, exceeding {max_bytes}",
                encoding.bytes,
            )));
        }
        Ok(encoding)
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes
    }

    pub(crate) fn write_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        self.write_body_to(writer)?;
        writer.write_all(self.footer.as_bytes())
    }

    fn write_body_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        IoSink {
            writer,
            error: None,
            remaining: self.body_bytes,
        }
        .write_checked(|sink| write_body(sink, self.descriptor))
    }
}

fn size_overflow() -> SkeinError {
    SkeinError::Storage("search segment descriptor encoded size overflow".into())
}

struct DigestWriter(Crc32cHasher);

impl io::Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_body(sink: &mut impl DocumentSink, descriptor: &SearchSegmentDescriptor) -> fmt::Result {
    sink.write_str("SKEIN_SEARCH_SEGMENTS_V3\n")?;
    writeln!(sink, "target_documents\t{}", descriptor.target_documents)?;
    writeln!(sink, "document_count\t{}", descriptor.document_count)?;
    for segment in &descriptor.segments {
        let range = segment.payload_range.unwrap_or(SearchSegmentPayloadRange {
            artifact_id: 0,
            offset: 0,
            length: 0,
            checksum: 0,
        });
        write!(sink, "segment\t{}\t", segment.segment_id)?;
        sink.write_hex(&segment.first_document_id)?;
        sink.write_char('\t')?;
        sink.write_hex(&segment.last_document_id)?;
        writeln!(
            sink,
            "\t{}\t{}\t{}\t{}\t{}",
            segment.document_count, range.artifact_id, range.offset, range.length, range.checksum
        )?;
        for (field, summary) in &segment.metadata {
            sink.write_str("field\t")?;
            sink.write_hex(field)?;
            write!(sink, "\t{}\t", summary.present_count)?;
            for (index, value) in summary.values.iter().enumerate() {
                if index != 0 {
                    sink.write_char(',')?;
                }
                sink.write_hex(value)?;
            }
            sink.write_char('\t')?;
            if let Some(range) = summary.numeric_range {
                write!(sink, "{}\t{}", range.min, range.max)?;
            } else {
                sink.write_char('\t')?;
            }
            sink.write_char('\t')?;
            if let Some(range) = summary.timestamp_range {
                write!(
                    sink,
                    "{}\t{}",
                    range.min_epoch_millis, range.max_epoch_millis
                )?;
            } else {
                sink.write_char('\t')?;
            }
            sink.write_char('\n')?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
