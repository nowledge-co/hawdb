use crate::{Result, SearchDocument, SkeinError};
use std::fmt::{self, Write};

// Counting and materializing share the wire grammar. Hex fields can be sized
// without scanning their bytes or allocating an intermediate encoded string.
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

    pub(super) fn encode(self) -> Result<String> {
        #[cfg(test)]
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
}

#[cfg(test)]
mod tests;
