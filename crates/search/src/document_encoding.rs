use crate::{Result, SearchDocument, SkeinError};
use std::fmt::{self, Write};
use std::io;

const HEX_BUFFER_BYTES: usize = 8192;

struct IoSink<'a, W> {
    writer: &'a mut W,
    error: Option<io::Error>,
    remaining: usize,
}

impl<W: io::Write> IoSink<'_, W> {
    fn bytes(&mut self, bytes: &[u8]) -> fmt::Result {
        if bytes.len() > self.remaining {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "search document encoding exceeded its admitted length",
            ));
            return Err(fmt::Error);
        }
        self.writer.write_all(bytes).map_err(|error| {
            self.error = Some(error);
            fmt::Error
        })?;
        self.remaining -= bytes.len();
        Ok(())
    }
}

impl<W: io::Write> Write for IoSink<'_, W> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.bytes(value.as_bytes())
    }
}

impl<W: io::Write> DocumentSink for IoSink<'_, W> {
    fn write_hex(&mut self, value: &str) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut buffer = [0u8; HEX_BUFFER_BYTES];
        for input in value.as_bytes().chunks(HEX_BUFFER_BYTES / 2) {
            for (byte, output) in input.iter().zip(buffer.chunks_exact_mut(2)) {
                output[0] = HEX[usize::from(byte >> 4)];
                output[1] = HEX[usize::from(byte & 15)];
            }
            self.bytes(&buffer[..input.len() * 2])?;
        }
        Ok(())
    }
}

// Counting, streaming and materializing share the wire grammar. Hex fields can
// be sized without scanning their bytes or allocating an encoded string.
trait DocumentSink: Write {
    fn write_hex(&mut self, value: &str) -> fmt::Result;
}

impl DocumentSink for String {
    fn write_hex(&mut self, value: &str) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in value.bytes() {
            self.push(char::from(HEX[usize::from(byte >> 4)]));
            self.push(char::from(HEX[usize::from(byte & 15)]));
        }
        Ok(())
    }
}

#[derive(Default)]
struct EncodedLength(usize);

impl Write for EncodedLength {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

impl DocumentSink for EncodedLength {
    fn write_hex(&mut self, value: &str) -> fmt::Result {
        let bytes = value.len().checked_mul(2).ok_or(fmt::Error)?;
        self.0 = self.0.checked_add(bytes).ok_or(fmt::Error)?;
        Ok(())
    }
}

fn write_document(sink: &mut impl DocumentSink, document: &SearchDocument) -> fmt::Result {
    sink.write_str("doc\t")?;
    sink.write_hex(&document.id)?;
    sink.write_char('\t')?;
    sink.write_hex(&document.title)?;
    sink.write_char('\t')?;
    sink.write_hex(&document.content)?;
    sink.write_char('\t')?;
    for (index, value) in document.embedding.iter().flatten().enumerate() {
        if index != 0 {
            sink.write_char(',')?;
        }
        write!(sink, "{value}")?;
    }
    sink.write_char('\t')?;
    for (index, (key, value)) in document.metadata.iter().enumerate() {
        if index != 0 {
            sink.write_char(';')?;
        }
        sink.write_hex(key)?;
        sink.write_char('=')?;
        sink.write_hex(value)?;
    }
    sink.write_char('\n')
}

pub(super) struct DocumentEncoding<'a> {
    document: &'a SearchDocument,
    bytes: usize,
}

impl<'a> DocumentEncoding<'a> {
    pub(super) fn new(document: &'a SearchDocument) -> Result<Self> {
        let mut length = EncodedLength::default();
        write_document(&mut length, document).map_err(|_| {
            SkeinError::Storage("search document encoded size overflow".to_string())
        })?;
        Ok(Self {
            document,
            bytes: length.0,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.bytes
    }

    pub(super) fn write_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        #[cfg(test)]
        STREAMING_ATTEMPTS.set(STREAMING_ATTEMPTS.get() + 1);
        let mut sink = IoSink {
            writer,
            error: None,
            remaining: self.bytes,
        };
        write_document(&mut sink, self.document).map_err(|_| {
            sink.error.take().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "search document formatting failed",
                )
            })
        })?;
        if sink.remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "search document encoding did not fill its admitted length",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn encode(self) -> Result<String> {
        ENCODING_ATTEMPTS.set(ENCODING_ATTEMPTS.get() + 1);
        let mut record = String::new();
        record.try_reserve_exact(self.bytes).map_err(|error| {
            SkeinError::Storage(format!("cannot allocate search document record: {error}"))
        })?;
        write_document(&mut record, self.document).expect("writing to a String cannot fail");
        debug_assert_eq!(record.len(), self.bytes);
        Ok(record)
    }
}

pub(super) fn encode_search_document_line(document: &SearchDocument) -> String {
    #[cfg(test)]
    ENCODING_ATTEMPTS.set(ENCODING_ATTEMPTS.get() + 1);
    let mut record = String::new();
    write_document(&mut record, document).expect("writing to a String cannot fail");
    record
}

#[cfg(test)]
thread_local! {
    pub(super) static ENCODING_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static STREAMING_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::legacy_encode;

#[cfg(test)]
mod io_tests;
