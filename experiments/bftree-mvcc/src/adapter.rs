// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::index::{Index, Layout, Previous, Rows, View};
use hawdb_storage::wal::frame::{
    decode_binary_wal_header, encode_binary_wal_header, frame_binary_wal_record,
    BinaryWalReadEvent, BinaryWalReader, WAL_BINARY_FILE_HEADER_BYTES,
};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const MAX_KEY: usize = 64;
pub const MAX_VALUE: usize = 1024;
pub const MAX_WRITES: usize = 128;
pub const MAX_SCAN_ROWS: usize = 4096;
pub const MAX_SCAN_RECORDS: usize = 200_000;
const MAX_BATCH: usize = 256 * 1024;
const MAX_JOURNAL: u64 = 32 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"BFMVCC01";
const PREFIX: u64 = 8;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Invalid(&'static str),
    Index(String),
    Journal(String),
    Conflict(Vec<u8>),
    Budget(&'static str),
    Busy,
    NeedsRecovery,
    CommitUncertain(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O: {e}"),
            Self::Invalid(e) => write!(f, "invalid request: {e}"),
            Self::Index(e) => write!(f, "Bf-Tree: {e}"),
            Self::Journal(e) => write!(f, "invalid experiment journal: {e}"),
            Self::Conflict(key) => write!(f, "write conflict on {key:?}"),
            Self::Budget(e) => write!(f, "budget exceeded: {e}"),
            Self::Busy => f.write_str("journal already has an owner"),
            Self::CommitUncertain(reason) => write!(
                f,
                "commit outcome uncertain ({reason}); close all handles and reopen"
            ),
            Self::NeedsRecovery => f.write_str(
                "commit outcome may be durable; close all handles and reopen to recover",
            ),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mutation {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Debug)]
pub struct Batch {
    pub epoch: u64,
    pub operation: u64,
    pub writes: Vec<Mutation>,
}
impl Batch {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.operation.to_le_bytes());
        out.extend_from_slice(&(self.writes.len() as u32).to_le_bytes());
        for m in &self.writes {
            out.extend_from_slice(&(m.key.len() as u16).to_le_bytes());
            out.extend_from_slice(
                &m.value
                    .as_ref()
                    .map_or(u32::MAX, |v| v.len() as u32)
                    .to_le_bytes(),
            );
            out.extend_from_slice(&m.key);
            if let Some(v) = &m.value {
                out.extend_from_slice(v);
            }
        }
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            let (head, tail) = bytes
                .split_at_checked(n)
                .ok_or(Error::Invalid("truncated batch"))?;
            *bytes = tail;
            Ok(head)
        }
        let mut input = bytes;
        let epoch = u64::from_le_bytes(take(&mut input, 8)?.try_into().expect("8 bytes"));
        let operation = u64::from_le_bytes(take(&mut input, 8)?.try_into().expect("8 bytes"));
        let n = u32::from_le_bytes(take(&mut input, 4)?.try_into().expect("4 bytes")) as usize;
        if n == 0 || n > MAX_WRITES {
            return Err(Error::Budget("batch entries"));
        }
        let mut writes = Vec::with_capacity(n);
        for _ in 0..n {
            let k = u16::from_le_bytes(take(&mut input, 2)?.try_into().expect("2 bytes")) as usize;
            let v = u32::from_le_bytes(take(&mut input, 4)?.try_into().expect("4 bytes"));
            if k > MAX_KEY || (v != u32::MAX && v as usize > MAX_VALUE) {
                return Err(Error::Budget("batch field"));
            }
            let key = take(&mut input, k)?.to_vec();
            let value = if v == u32::MAX {
                None
            } else {
                Some(take(&mut input, v as usize)?.to_vec())
            };
            if writes
                .last()
                .is_some_and(|previous: &Mutation| previous.key >= key)
            {
                return Err(Error::Invalid("batch keys must be unique and ordered"));
            }
            writes.push(Mutation { key, value });
        }
        if !input.is_empty() {
            return Err(Error::Invalid("extra batch bytes"));
        }
        Ok(Self {
            epoch,
            operation,
            writes,
        })
    }
}

struct Latest {
    epoch: u64,
    position: usize,
}
struct Journal {
    file: File,
    len: u64,
    latest: BTreeMap<Vec<u8>, Latest>,
    // Bounded by MAX_JOURNAL; exact payload equality avoids hash-based replay ambiguity.
    receipts: BTreeMap<u64, u64>,
    commits: Vec<Batch>,
}
impl Journal {
    fn previous(&self, key: &[u8]) -> Result<Option<Previous<'_>>> {
        let Some(latest) = self.latest.get(key) else {
            return Ok(None);
        };
        let change = self
            .batch(latest.epoch)?
            .writes
            .get(latest.position)
            .filter(|change| change.key == key)
            .ok_or(Error::Invalid("missing latest commit receipt"))?;
        Ok(Some(Previous {
            epoch: latest.epoch,
            value: &change.value,
        }))
    }
    fn batch(&self, epoch: u64) -> Result<&Batch> {
        let index = usize::try_from(epoch)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .ok_or(Error::Invalid("invalid receipt epoch"))?;
        self.commits
            .get(index)
            .filter(|batch| batch.epoch == epoch)
            .ok_or(Error::Invalid("missing commit receipt"))
    }
    fn record(&mut self, batch: Batch) {
        for (position, change) in batch.writes.iter().enumerate() {
            let latest = Latest {
                epoch: batch.epoch,
                position,
            };
            if let Some(entry) = self.latest.get_mut(change.key.as_slice()) {
                *entry = latest;
            } else {
                self.latest.insert(change.key.clone(), latest);
            }
        }
        self.receipts.insert(batch.operation, batch.epoch);
        self.commits.push(batch);
    }
}
struct Inner {
    index: Index,
    layout: Layout,
    published: AtomicU64,
    poisoned: AtomicBool,
    journal: Mutex<Journal>,
}
#[derive(Clone)]
pub struct Store(Arc<Inner>);
pub struct ReadTxn {
    inner: Arc<Inner>,
    epoch: u64,
    view: View,
}
pub struct WriteTxn {
    read: ReadTxn,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Durable,
    Installed(usize),
    Published,
}
struct Uncertain<'a> {
    poisoned: &'a AtomicBool,
    armed: bool,
}
impl Drop for Uncertain<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_layout(path, Layout::Hybrid)
    }
    pub fn open_layout(path: &Path, layout: Layout) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock().map_err(|_| Error::Busy)?;
        let len = file.metadata()?.len();
        if len > MAX_JOURNAL {
            return Err(Error::Budget("journal bytes"));
        }
        if len == 0 {
            file.write_all(MAGIC)?;
            file.write_all(&encode_binary_wal_header(layout.id(), 1))?;
            file.sync_all()?;
            #[cfg(unix)]
            File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?
            .sync_all()?;
        }
        let len = file.metadata()?.len();
        let mut prefix = [0; 8];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut prefix)?;
        if &prefix != MAGIC {
            return Err(Error::Invalid("not an experiment journal"));
        }
        let mut header = [0; WAL_BINARY_FILE_HEADER_BYTES];
        file.read_exact(&mut header)?;
        let (generation, start) =
            decode_binary_wal_header(&header).map_err(|e| Error::Journal(e.to_string()))?;
        if generation != layout.id() || start != 1 {
            return Err(Error::Invalid("journal layout/header mismatch"));
        }
        let index = Index::new(layout)?;
        let published = AtomicU64::new(0);
        let mut journal = Journal {
            file,
            len,
            latest: BTreeMap::new(),
            receipts: BTreeMap::new(),
            commits: Vec::new(),
        };
        let reader = journal.file.try_clone()?;
        let mut reader = BinaryWalReader::new(reader, generation, Some(MAX_BATCH));
        let mut valid_end = WAL_BINARY_FILE_HEADER_BYTES as u64;
        loop {
            match reader
                .next_event()
                .map_err(|e| Error::Journal(e.to_string()))?
            {
                BinaryWalReadEvent::Record {
                    payload,
                    end_offset,
                    ..
                } => {
                    let batch = Batch::decode(&payload)?;
                    if batch.epoch != published.load(Ordering::Relaxed) + 1
                        || journal.receipts.contains_key(&batch.operation)
                    {
                        return Err(Error::Invalid(
                            "non-contiguous epoch or duplicate operation",
                        ));
                    }
                    let candidate =
                        index.prepare(&batch, |key| journal.previous(key), |_| Ok(()))?;
                    index.publish(candidate, batch.epoch, &published)?;
                    journal.record(batch);
                    valid_end = end_offset;
                }
                BinaryWalReadEvent::Eof => break,
                BinaryWalReadEvent::TornTail { reason, .. }
                | BinaryWalReadEvent::Corrupt { reason, .. } => return Err(Error::Journal(reason)),
            }
        }
        if valid_end + PREFIX != len {
            return Err(Error::Invalid("unverified trailing journal bytes"));
        }
        journal.file.seek(SeekFrom::End(0))?;
        Ok(Self(Arc::new(Inner {
            index,
            layout,
            published,
            poisoned: AtomicBool::new(false),
            journal: Mutex::new(journal),
        })))
    }
    pub fn begin_read(&self) -> Result<ReadTxn> {
        self.0.usable()?;
        let (epoch, view) = self.0.index.capture(&self.0.published)?;
        self.0.usable()?;
        Ok(ReadTxn {
            inner: Arc::clone(&self.0),
            epoch,
            view,
        })
    }
    pub fn begin_write(&self) -> Result<WriteTxn> {
        Ok(WriteTxn {
            read: self.begin_read()?,
            writes: BTreeMap::new(),
        })
    }
    pub fn journal_bytes(&self) -> Result<u64> {
        Ok(self.0.journal.lock().map_err(|_| Error::NeedsRecovery)?.len)
    }
}
impl Inner {
    fn usable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::NeedsRecovery)
        } else {
            Ok(())
        }
    }
}
impl ReadTxn {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() > MAX_KEY {
            return Err(Error::Budget("key bytes"));
        }
        self.inner.usable()?;
        let value = self.view.get(key)?;
        self.inner.usable()?;
        Ok(value)
    }
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Rows> {
        if start.len() > MAX_KEY || end.len() > MAX_KEY {
            return Err(Error::Budget("scan key bytes"));
        }
        self.inner.usable()?;
        let rows = self.view.scan(start, end)?;
        self.inner.usable()?;
        Ok(rows)
    }
}
impl WriteTxn {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if value.len() > MAX_VALUE {
            return Err(Error::Budget("value bytes"));
        }
        self.write(key, Some(value.to_vec()))
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }
    fn write(&mut self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if key.len() > MAX_KEY {
            return Err(Error::Budget("key bytes"));
        }
        if !self.writes.contains_key(key) && self.writes.len() == MAX_WRITES {
            return Err(Error::Budget("transaction entries"));
        }
        self.writes.insert(key.to_vec(), value);
        Ok(())
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.writes.get(key) {
            Some(value) => Ok(value.clone()),
            None => self.read.get(key),
        }
    }
    pub fn rollback(self) {}
    pub fn commit(self, operation: u64) -> Result<u64> {
        self.commit_with_hook(operation, |_| Ok(()))
    }
    pub fn commit_with_hook(
        self,
        operation: u64,
        mut hook: impl FnMut(Stage) -> Result<()>,
    ) -> Result<u64> {
        let inner = &self.read.inner;
        inner.usable()?;
        let mut journal = inner.journal.lock().map_err(|_| Error::NeedsRecovery)?;
        inner.usable()?;
        let writes: Vec<_> = self
            .writes
            .into_iter()
            .map(|(key, value)| Mutation { key, value })
            .collect();
        if writes.is_empty() {
            return Err(Error::Invalid("empty write transaction"));
        }
        if let Some(&previous_epoch) = journal.receipts.get(&operation) {
            let previous = journal.batch(previous_epoch)?;
            return if previous.writes == writes {
                Ok(previous.epoch)
            } else {
                Err(Error::Invalid("operation ID reused with different writes"))
            };
        }
        for change in &writes {
            if journal
                .latest
                .get(&change.key)
                .is_some_and(|latest| latest.epoch > self.read.epoch)
            {
                return Err(Error::Conflict(change.key.clone()));
            }
        }
        let epoch = inner
            .published
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or(Error::Budget("epoch"))?;
        let batch = Batch {
            epoch,
            operation,
            writes,
        };
        let payload = batch.encode();
        if payload.len() > MAX_BATCH {
            return Err(Error::Budget("encoded batch"));
        }
        let frame = frame_binary_wal_record(
            inner.layout.id(),
            &payload,
            journal.len - PREFIX - WAL_BINARY_FILE_HEADER_BYTES as u64,
        );
        let new_len = journal
            .len
            .checked_add(frame.len() as u64)
            .filter(|n| *n <= MAX_JOURNAL)
            .ok_or(Error::Budget("journal bytes"))?;
        // From the first write onward, an error may leave a durable commit.
        // Poison all handles until authoritative journal replay resolves it.
        let mut uncertain = Uncertain {
            poisoned: &inner.poisoned,
            armed: true,
        };
        let outcome: Result<()> = (|| {
            journal.file.write_all(&frame)?;
            journal.file.sync_all()?;
            hook(Stage::Durable)?;
            let candidate = inner.index.prepare(
                &batch,
                |key| journal.previous(key),
                |n| hook(Stage::Installed(n)),
            )?;
            inner.index.publish(candidate, epoch, &inner.published)?;
            hook(Stage::Published)?;
            Ok(())
        })();
        outcome.map_err(|error| Error::CommitUncertain(error.to_string()))?;
        journal.len = new_len;
        journal.record(batch);
        uncertain.armed = false;
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests;
