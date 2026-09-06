use super::{Digest, ManifestBody, ManifestEnvelope, MAX_MANIFEST_BYTES};
use crate::build_memory::BuildMemory;
use crate::error::{Result, SkeinError};
use serde::Serialize;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

struct DigestWriter(Digest);

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn body_checksum(body: &ManifestBody) -> Result<u64> {
    let mut writer = DigestWriter(Digest::new());
    serde_json::to_writer(&mut writer, body)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    Ok(writer.0.finish())
}

pub(super) struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
    memory: Option<BuildMemory>,
    _lease: Option<skein_executor::QueryMemoryLease>,
}

impl BoundedBytes {
    fn new(limit: u64) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            limit: usize::try_from(limit.min(MAX_MANIFEST_BYTES)).map_err(|_| {
                SkeinError::Storage("lexical manifest budget exceeds the address space".to_string())
            })?,
            memory: None,
            _lease: None,
        })
    }
}

impl AsRef<[u8]> for BoundedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|&end| end <= self.limit)
            .ok_or_else(|| io::Error::other("lexical manifest byte budget exceeded"))?;
        if end > self.bytes.capacity() {
            let capacity = end
                .max(self.bytes.capacity().max(1024).saturating_mul(2))
                .min(self.limit);
            let next_lease = self
                .memory
                .as_ref()
                .map(|memory| memory.retained.reserve(capacity))
                .transpose()
                .map_err(io::Error::other)?;
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::Error::other("lexical manifest allocation failed"))?;
            if self.bytes.capacity() > self.limit {
                return Err(io::Error::other(
                    "lexical manifest capacity exceeds its budget",
                ));
            }
            // The old allocation stays admitted until replacement completes.
            self._lease = next_lease;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn read(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut input = File::open(path)?;
    let mut output = BoundedBytes::new(limit)?;
    if input.metadata()?.len() > output.limit as u64 {
        return Err(SkeinError::Storage(
            "lexical projection manifest exceeds its read budget".to_string(),
        ));
    }
    // Continue enforcing the bound if the file grows after its metadata check.
    let mut buffer = [0; 8192];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count])?;
    }
    Ok(output.bytes)
}

impl ManifestBody {
    pub(super) fn directory_resident_bytes(&self) -> u64 {
        let mut bytes = std::mem::size_of::<Self>() as u64;
        for string in [&self.format, &self.layout, &self.artifact_file] {
            bytes = bytes.saturating_add(string.capacity() as u64);
        }
        bytes = bytes.saturating_add(
            (self.blocks.capacity() as u64)
                .saturating_mul(std::mem::size_of::<super::BlockDescriptor>() as u64),
        );
        bytes = bytes.saturating_add(
            (self.dictionaries.capacity() as u64)
                .saturating_mul(std::mem::size_of::<super::dictionary_store::Descriptor>() as u64),
        );
        for block in &self.blocks {
            bytes = bytes
                .saturating_add(block.min_key.capacity() as u64)
                .saturating_add(block.max_key.capacity() as u64);
        }
        for block in &self.dictionaries {
            bytes = bytes
                .saturating_add(block.min_term.capacity() as u64)
                .saturating_add(block.max_term.capacity() as u64);
        }
        bytes
    }

    #[cfg(test)]
    pub(super) fn encode_bounded(&self, limit: u64) -> Result<Vec<u8>> {
        Ok(self.encode_writer(limit, None)?.bytes)
    }

    pub(super) fn encode_admitted(&self, limit: u64, memory: &BuildMemory) -> Result<BoundedBytes> {
        self.encode_writer(limit, Some(memory))
    }

    fn encode_writer(&self, limit: u64, memory: Option<&BuildMemory>) -> Result<BoundedBytes> {
        self.validate()?;
        if self.directory_resident_bytes() > limit.min(MAX_MANIFEST_BYTES) {
            return Err(SkeinError::Storage(
                "lexical manifest directory budget exceeded".to_string(),
            ));
        }
        #[derive(Serialize)]
        struct Envelope<'a> {
            body: &'a ManifestBody,
            checksum: u64,
        }
        let envelope = Envelope {
            body: self,
            checksum: body_checksum(self)?,
        };
        let mut writer = BoundedBytes::new(limit)?;
        writer.memory = memory.cloned();
        serde_json::to_writer(&mut writer, &envelope)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        Ok(writer)
    }

    pub(super) fn decode_bounded(bytes: &[u8], limit: u64) -> Result<Self> {
        let limit = limit.min(MAX_MANIFEST_BYTES);
        if bytes.len() as u64 > limit {
            return Err(SkeinError::Storage(
                "lexical manifest byte budget exceeded".to_string(),
            ));
        }
        let envelope: ManifestEnvelope = serde_json::from_slice(bytes)
            .map_err(|error| SkeinError::Storage(format!("invalid lexical manifest: {error}")))?;
        if envelope.body.directory_resident_bytes() > limit {
            return Err(SkeinError::Storage(
                "lexical manifest directory budget exceeded".to_string(),
            ));
        }
        if body_checksum(&envelope.body)? != envelope.checksum {
            return Err(SkeinError::Storage(
                "lexical projection manifest checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate()?;
        Ok(envelope.body)
    }
}
