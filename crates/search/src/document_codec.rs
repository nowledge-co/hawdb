//! One wire encoder with allocation-free sizing for admitted generation input.

use crate::build_control::checkpoint;
use crate::error::{Result, SkeinError};
use crate::SearchDocument;
use skein_core::RuntimeTaskContext;
use std::fmt::{self, Write};

pub(crate) fn write_line(output: &mut impl Write, document: &SearchDocument) -> fmt::Result {
    output.write_str("doc\t")?;
    for field in [&document.id, &document.title, &document.content] {
        write_hex(output, field)?;
        output.write_char('\t')?;
    }
    if let Some(values) = &document.embedding {
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                output.write_char(',')?;
            }
            write!(output, "{value}")?;
        }
    }
    output.write_char('\t')?;
    for (index, (key, value)) in document.metadata.iter().enumerate() {
        if index != 0 {
            output.write_char(';')?;
        }
        write_hex(output, key)?;
        output.write_char('=')?;
        write_hex(output, value)?;
    }
    output.write_char('\n')
}

fn write_hex(output: &mut impl Write, input: &str) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buffer = [0; 1024];
    for chunk in input.as_bytes().chunks(buffer.len() / 2) {
        for (byte, pair) in chunk.iter().zip(buffer.chunks_exact_mut(2)) {
            pair[0] = HEX[usize::from(byte >> 4)];
            pair[1] = HEX[usize::from(byte & 15)];
        }
        output.write_str(std::str::from_utf8(&buffer[..chunk.len() * 2]).unwrap())?;
    }
    Ok(())
}

pub(crate) fn encoded_len(
    document: &SearchDocument,
    limit: u64,
    task: Option<&RuntimeTaskContext>,
) -> Result<usize> {
    let mut size = Size { bytes: 0, limit };
    // The fixed prefix, field separators and final newline are nine bytes.
    size.add(9)?;
    for field in [&document.id, &document.title, &document.content] {
        size.add_hex(field)?;
    }
    for (index, (key, value)) in document.metadata.iter().enumerate() {
        check(task)?;
        size.add_hex(key)?;
        size.add_hex(value)?;
        size.add(1 + u64::from(index != 0))?;
    }
    if let Some(values) = &document.embedding {
        for (index, value) in values.iter().enumerate() {
            check(task)?;
            size.add(u64::from(index != 0))?;
            // Display matches the wire encoder, including subnormals and signed
            // zero, without constructing a String for each floating-point value.
            write!(&mut size, "{value}").map_err(|_| size.exceeded())?;
        }
    }
    check(task)?;
    usize::try_from(size.bytes)
        .map_err(|_| SkeinError::Storage("search document record exceeds usize".to_string()))
}

#[cfg(test)]
pub(crate) fn encode_bounded(
    document: &SearchDocument,
    limit: u64,
    task: Option<&RuntimeTaskContext>,
) -> Result<String> {
    check(task)?;
    let length = encoded_len(document, limit, task)?;
    encode_admitted(document, length, task)
}

pub(crate) fn encode_admitted(
    document: &SearchDocument,
    length: usize,
    task: Option<&RuntimeTaskContext>,
) -> Result<String> {
    check(task)?;
    #[cfg(test)]
    allocation_evidence::record();
    let mut output = Output {
        bytes: String::new(),
        length,
        task,
    };
    output.bytes.try_reserve_exact(length).map_err(|error| {
        SkeinError::Storage(format!("search document record allocation failed: {error}"))
    })?;
    write_line(&mut output, document).map_err(|_| {
        check(task).err().unwrap_or_else(|| {
            SkeinError::Storage("search document record size disagrees with preflight".to_string())
        })
    })?;
    check(task)?;
    if output.bytes.len() != length {
        return Err(SkeinError::Storage(
            "search document record size disagrees with preflight".to_string(),
        ));
    }
    Ok(output.bytes)
}

fn check(task: Option<&RuntimeTaskContext>) -> Result<()> {
    task.map_or(Ok(()), checkpoint)
}

pub(crate) struct Fields<'a> {
    pub(crate) id: &'a str,
    pub(crate) title: &'a str,
    pub(crate) content: &'a str,
    pub(crate) embedding: &'a str,
    pub(crate) metadata: &'a str,
}

impl<'a> Fields<'a> {
    pub(crate) fn parse(line: &'a str) -> Result<Self> {
        let mut fields = line.strip_suffix('\n').unwrap_or(line).split('\t');
        match (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) {
            (
                Some("doc"),
                Some(id),
                Some(title),
                Some(content),
                Some(embedding),
                Some(metadata),
                None,
            ) => Ok(Self {
                id,
                title,
                content,
                embedding,
                metadata,
            }),
            _ => Err(SkeinError::Storage(
                "invalid search document line".to_string(),
            )),
        }
    }
}

struct Size {
    bytes: u64,
    limit: u64,
}

impl Size {
    fn exceeded(&self) -> SkeinError {
        SkeinError::Storage(format!(
            "search document record exceeds its admitted {} encoded bytes",
            self.limit
        ))
    }

    fn add(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| self.exceeded())?;
        Ok(())
    }

    fn add_hex(&mut self, field: &str) -> Result<()> {
        let bytes = (field.len() as u64)
            .checked_mul(2)
            .ok_or_else(|| self.exceeded())?;
        self.add(bytes)
    }
}

impl Write for Size {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.add(text.len() as u64).map_err(|_| fmt::Error)
    }
}

struct Output<'a> {
    bytes: String,
    length: usize,
    task: Option<&'a RuntimeTaskContext>,
}

impl Write for Output<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        check(self.task).map_err(|_| fmt::Error)?;
        if text.len() > self.length - self.bytes.len() {
            return Err(fmt::Error);
        }
        self.bytes.push_str(text);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod allocation_evidence {
    use std::cell::Cell;
    thread_local! {
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }
    pub(super) fn record() {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
    pub(crate) fn take() -> usize {
        ALLOCATIONS.with(|count| count.replace(0))
    }
}

#[cfg(test)]
mod tests;
