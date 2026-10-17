// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

type Body = Option<(Box<spool::SpoolRecord>, bool)>;

fn checksum(stamp: &OwnerStamp, body: Option<(u64, u64, bool)>) -> u64 {
    let mut crc = Crc32cHasher::new();
    crc.update(&(stamp.key.len() as u64).to_le_bytes());
    crc.update(&stamp.phase.to_le_bytes());
    crc.update(stamp.key.as_bytes());
    if let Some((digest, bytes, flag)) = body {
        crc.update(&[1, u8::from(flag)]);
        crc.update(&digest.to_le_bytes());
        crc.update(&bytes.to_le_bytes());
    } else {
        crc.update(&[0]);
    }
    crc.finish()
}

impl Output {
    fn owner_prefix(&mut self, stamp: &OwnerStamp, present: bool) -> Result<()> {
        self.file.write_all(&[2])?;
        self.file
            .write_all(&(stamp.key.len() as u64).to_le_bytes())?;
        self.file.write_all(&stamp.phase.to_le_bytes())?;
        self.file.write_all(stamp.key.as_bytes())?;
        self.file.write_all(&[u8::from(present)])?;
        Ok(())
    }
    fn owner_finish(
        &mut self,
        stamp: &OwnerStamp,
        body: Option<(u64, u64, bool)>,
        extent: u64,
    ) -> Result<()> {
        let crc = checksum(stamp, body);
        self.file.write_all(&crc.to_le_bytes())?;
        evidence(&mut self.digest, crc, extent, 4);
        Ok(())
    }
    pub(super) fn owner_buffered(
        &mut self,
        stamp: OwnerStamp,
        body: Option<(AdmittedDocument, u64)>,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        let extent = (stamp.key.len() as u64)
            .checked_add(26)
            .and_then(|n| {
                body.as_ref()
                    .map_or(Some(n), |(_, bytes)| n.checked_add(17)?.checked_add(*bytes))
            })
            .ok_or_else(|| invalid("owner frame overflow"))?;
        self.reserve(extent, control, writer)?;
        self.owner_prefix(&stamp, body.is_some())?;
        let evidence = if let Some((document, bytes)) = body {
            if bytes > writer.options.max_record_bytes.get() {
                return Err(invalid("owner body exceeds admission"));
            }
            let encoding = DocumentEncoding::new_with_context(&document, Some(&control.task))?;
            if encoding.len() as u64 != bytes {
                return Err(invalid("owner body extent changed"));
            }
            let flag = crate::analyzer_workspace::document_needs_workspace(&document);
            self.file.write_all(&[u8::from(flag)])?;
            let mut digest = DocumentsDigest::default();
            spool::write_frame_with_context(
                &mut self.file,
                &encoding,
                &mut digest,
                &control.memory,
                &control.task,
            )?;
            Some((digest.finish(), bytes, flag))
        } else {
            None
        };
        self.owner_finish(&stamp, evidence, extent)
    }
    pub(super) fn owner_copied(
        &mut self,
        stamp: &OwnerStamp,
        body: Option<&(Box<spool::SpoolRecord>, bool)>,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        let extent =
            stamp.key.len() as u64 + 26 + body.map_or(0, |(record, _)| record.encoded_bytes + 17);
        self.reserve(extent, control, writer)?;
        self.owner_prefix(stamp, body.is_some())?;
        let evidence = if let Some((record, flag)) = body {
            control.read(record.encoded_bytes)?;
            self.file.write_all(&[u8::from(*flag)])?;
            self.file.write_all(&record.encoded_bytes.to_le_bytes())?;
            self.file.write_all(&record.checksum().to_le_bytes())?;
            record.write_to(&mut self.file, &control.memory, &control.task)?;
            let mut digest = DocumentsDigest::default();
            record.add_digest(&mut digest);
            Some((digest.finish(), record.encoded_bytes, *flag))
        } else {
            None
        };
        self.owner_finish(stamp, evidence, extent)
    }
}

impl Reader {
    fn owner_length(&self, control: &mut Control) -> Result<(usize, u64)> {
        let mut header = [0; 16];
        self.raw(self.position + 1, &mut header, control)?;
        let length = u64::from_le_bytes(header[..8].try_into().unwrap());
        let size = usize::try_from(length).map_err(|_| invalid("owner length overflow"))?;
        if size == 0 || size > control.max_owner_bytes {
            return Err(invalid("owner key exceeds admission"));
        }
        let phase = u64::from_le_bytes(header[8..].try_into().unwrap());
        Ok((size, phase))
    }
    pub(super) fn inspect_owner(&self, control: &mut Control) -> Result<Option<u64>> {
        let (size, _) = self.owner_length(control)?;
        let body = self
            .position
            .checked_add(17)
            .and_then(|n| n.checked_add(size as u64))
            .ok_or_else(|| invalid("owner offset overflow"))?;
        let mut tag = [0];
        self.raw(body, &mut tag, control)?;
        match tag[0] {
            0 => Ok((size as u64).checked_add(26)),
            1 => {
                let mut frame = [0; 17];
                self.raw(body + 1, &mut frame, control)?;
                if frame[0] > 1 {
                    return Err(invalid("owner analyzer marker"));
                }
                let bytes = u64::from_le_bytes(frame[1..9].try_into().unwrap());
                Ok(bytes
                    .checked_add(size as u64)
                    .and_then(|n| n.checked_add(59)))
            }
            _ => Err(invalid("owner body tag")),
        }
    }
    pub(super) fn read_owner(&mut self, control: &mut Control) -> Result<(Entry, u64)> {
        let start = self.position;
        let (size, phase) = self.owner_length(control)?;
        let _scratch = self.memory.spool.reserve(size)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| invalid("owner allocation failed"))?;
        if bytes.capacity() != size {
            return Err(invalid("owner allocation exceeds admission"));
        }
        bytes.resize(size, 0);
        self.raw(start + 17, &mut bytes, control)?;
        let key = std::str::from_utf8(&bytes).map_err(|_| invalid("owner key is not UTF-8"))?;
        let stamp = OwnerStamp {
            key: Term::copy(key, Some(&self.memory))?,
            phase,
        };
        let mut tag = [0];
        let offset = start
            .checked_add(17)
            .and_then(|n| n.checked_add(size as u64))
            .ok_or_else(|| invalid("owner offset overflow"))?;
        self.raw(offset, &mut tag, control)?;
        let (body, crc_offset, value) = match tag[0] {
            0 => (None, offset + 1, None),
            1 => {
                let mut frame = [0; 17];
                self.raw(offset + 1, &mut frame, control)?;
                if frame[0] > 1 {
                    return Err(invalid("owner analyzer marker"));
                }
                let length = u64::from_le_bytes(frame[1..9].try_into().unwrap());
                let end = offset
                    .checked_add(18)
                    .and_then(|n| n.checked_add(length))
                    .filter(|n| *n <= self.run.bytes)
                    .ok_or_else(|| invalid("owner body truncated"))?;
                control.read(
                    length
                        .checked_add(16)
                        .ok_or_else(|| invalid("owner read overflow"))?,
                )?;
                let record = self.cursor.record_at(offset + 2, end)?;
                validate_id(&record.header.id, control.max_id_bytes)?;
                let mut digest = DocumentsDigest::default();
                record.add_digest(&mut digest);
                let value = (digest.finish(), length, frame[0] == 1);
                (Some((Box::new(record), frame[0] == 1)), end, Some(value))
            }
            _ => return Err(invalid("owner body tag")),
        };
        let end = crc_offset
            .checked_add(8)
            .filter(|n| *n <= self.run.bytes)
            .ok_or_else(|| invalid("owner checksum truncated"))?;
        let mut expected = [0; 8];
        self.raw(crc_offset, &mut expected, control)?;
        let crc = checksum(&stamp, value);
        if crc != u64::from_le_bytes(expected) {
            return Err(invalid("owner frame checksum mismatch"));
        }
        evidence(&mut self.digest, crc, end - start, 4);
        Ok((Entry::Owner(stamp, body), end))
    }
}

pub(super) fn reduce(
    a: OwnerStamp,
    left: Body,
    b: OwnerStamp,
    right: Body,
    control: &mut Control,
) -> Result<Entry> {
    if a.phase > b.phase {
        return Err(invalid("owner phases reordered"));
    }
    if a.phase < b.phase {
        return Ok(Entry::Owner(b, right));
    }
    let body = match (left, right) {
        (None, None) => None,
        (Some((left, x)), Some((right, y))) => {
            control.read(
                left.encoded_bytes
                    .checked_add(right.encoded_bytes)
                    .ok_or_else(|| invalid("owner comparison overflow"))?,
            )?;
            if !left.encoded_eq(&right, &control.memory, &control.task)? {
                return Err(invalid("conflicting states for one owner phase"));
            }
            Some((left, x || y))
        }
        _ => return Err(invalid("conflicting absence for one owner phase")),
    };
    Ok(Entry::Owner(a, body))
}
