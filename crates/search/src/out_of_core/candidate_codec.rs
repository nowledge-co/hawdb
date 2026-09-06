use super::{query_io, CandidateEntry};
use crate::query_memory::Admitted;
use crate::{Result, RuntimeTaskContext, SkeinError};
use skein_executor::QueryMemoryAccount;
use std::mem::size_of;

// Borrow and validate the complete block before any result allocation. A forged
// count cannot drive capacity, nor can an invalid tail return an accepted prefix.
pub(super) fn visit(
    bytes: &[u8],
    expected: usize,
    task: &RuntimeTaskContext,
    mut consume: impl FnMut(&str, Option<u64>) -> Result<()>,
) -> Result<usize> {
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut previous: Option<&str> = None;
    let mut id_bytes = 0usize;
    while offset < bytes.len() {
        query_io::checkpoint(task)?;
        if count == expected {
            return Err(error());
        }
        let raw = bytes
            .get(offset..query_io::add(offset, 4)?)
            .ok_or_else(error)?;
        let length = u32::from_le_bytes(raw.try_into().unwrap()) as usize;
        offset += 4;
        let end = query_io::add(offset, length)?;
        let id =
            std::str::from_utf8(bytes.get(offset..end).ok_or_else(error)?).map_err(|_| error())?;
        if previous.is_some_and(|value| value >= id) {
            return Err(error());
        }
        previous = Some(id);
        let raw = bytes.get(end..query_io::add(end, 8)?).ok_or_else(error)?;
        let ordinal = u64::from_le_bytes(raw.try_into().unwrap());
        consume(id, (ordinal != u64::MAX).then_some(ordinal))?;
        id_bytes = query_io::add(id_bytes, id.len())?;
        offset = end + 8;
        count += 1;
    }
    if count != expected {
        return Err(error());
    }
    Ok(id_bytes)
}

pub(super) fn entries(
    bytes: &[u8],
    expected: usize,
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
) -> Result<Admitted<Vec<CandidateEntry>>> {
    let id_bytes = visit(bytes, expected, task, |_, _| Ok(()))?;
    let lease = memory.reserve(query_io::add(
        id_bytes,
        query_io::mul(expected, size_of::<CandidateEntry>())?,
    )?)?;
    #[cfg(test)]
    evidence::entries();
    let mut entries = Vec::with_capacity(expected);
    visit(bytes, expected, task, |id, _| {
        entries.push(CandidateEntry { id: id.to_owned() });
        Ok(())
    })?;
    Ok(Admitted::new(entries, lease))
}

fn error() -> SkeinError {
    SkeinError::Storage("search candidate block count, encoding or ordering mismatch".to_owned())
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static ENTRIES: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn entries() {
        ENTRIES.with(|v| v.set(v.get() + 1));
    }
    pub(in super::super) fn take() -> usize {
        ENTRIES.with(|v| v.replace(0))
    }
}
