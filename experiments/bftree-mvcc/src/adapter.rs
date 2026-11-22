// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::index::{BatchRead, Index, Layout, Previous, Rows, View};
use crate::storage::{BfTreeStorage, Storage};
use hawdb_storage::wal::frame::{
    decode_binary_wal_header, encode_binary_wal_header, frame_binary_wal_record,
    BinaryWalReadEvent, BinaryWalReader, WAL_BINARY_FILE_HEADER_BYTES,
};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub const MAX_KEY: usize = 64;
pub const MAX_VALUE: usize = 1024;
pub const MAX_WRITES: usize = 128;
pub const MAX_SCAN_ROWS: usize = 4096;
pub const MAX_SCAN_RECORDS: usize = 200_000;
pub const MAX_BATCH_LOOKUPS: usize = 128;
const MAX_BATCH: usize = 256 * 1024;
const MAX_JOURNAL: u64 = 32 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"BFMVCC02";
const PREFIX: u64 = 8;

pub mod audit;
mod maintenance;
pub use maintenance::MAX_GC_WORK;

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
            Self::Index(e) => write!(f, "index storage: {e}"),
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
        let mut out = vec![0];
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
        let mut input = bytes
            .strip_prefix(&[0])
            .ok_or(Error::Invalid("not a transaction record"))?;
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

#[derive(Clone)]
struct Latest {
    epoch: u64,
    position: usize,
    batch: Arc<Batch>,
    versions: VecDeque<u64>,
}
#[derive(Clone, Copy)]
struct Receipt {
    epoch: u64,
    start: u64,
    end: u64,
}
// Present the authoritative file after the experiment's eight-byte identity.
// Clones share a cursor, so use only under the journal coordinator. Appends
// always re-seek to the verified end before writing.
struct WalFile(File);
impl Read for WalFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buffer)
    }
}
impl Seek for WalFile {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let position = match position {
            SeekFrom::Start(offset) => SeekFrom::Start(
                offset
                    .checked_add(PREFIX)
                    .ok_or_else(|| std::io::Error::other("WAL offset overflow"))?,
            ),
            other => other,
        };
        self.0
            .seek(position)?
            .checked_sub(PREFIX)
            .ok_or_else(|| std::io::Error::other("WAL seek precedes identity"))
    }
}
struct Journal {
    file: File,
    len: u64,
    latest: BTreeMap<Vec<u8>, Latest>,
    // Mutation payloads remain in the bounded journal for exact retries. Only
    // batches referenced by current heads retain their payload in memory.
    receipts: BTreeMap<u64, Receipt>,
    retained_versions: usize,
    gc_cursor: Option<Vec<u8>>,
    gc_pass_horizon: Option<u64>,
}
impl Journal {
    fn frame(&self, payload: &[u8], generation: u64) -> Result<(Vec<u8>, u64)> {
        let frame = frame_binary_wal_record(
            generation,
            payload,
            self.len - PREFIX - WAL_BINARY_FILE_HEADER_BYTES as u64,
        );
        let new_len = self
            .len
            .checked_add(frame.len() as u64)
            .filter(|n| *n <= MAX_JOURNAL)
            .ok_or(Error::Budget("journal bytes"))?;
        Ok((frame, new_len))
    }
    fn write_frame(&mut self, frame: &[u8]) -> Result<()> {
        if self.file.metadata()?.len() != self.len {
            return Err(Error::Invalid("journal length changed outside its owner"));
        }
        self.file.seek(SeekFrom::Start(self.len))?;
        self.file.write_all(frame)?;
        self.file.sync_all()?;
        Ok(())
    }
    fn previous(&self, key: &[u8]) -> Result<Option<Previous<'_>>> {
        let Some(latest) = self.latest.get(key) else {
            return Ok(None);
        };
        let change = latest
            .batch
            .writes
            .get(latest.position)
            .filter(|change| change.key == key)
            .ok_or(Error::Invalid("missing latest commit receipt"))?;
        Ok(Some(Previous {
            epoch: latest.epoch,
            value: &change.value,
        }))
    }
    fn batch(&self, receipt: Receipt, key_hint: &[u8], generation: u64) -> Result<Arc<Batch>> {
        let epoch = receipt.epoch;
        if let Some(latest) = self.latest.get(key_hint).filter(|head| head.epoch == epoch) {
            return Ok(Arc::clone(&latest.batch));
        }
        let mut reader = BinaryWalReader::range(
            WalFile(self.file.try_clone()?),
            generation,
            Some(MAX_BATCH),
            receipt.start,
            receipt.end,
        )
        .map_err(|error| Error::Journal(error.to_string()))?;
        let batch = match reader
            .next_event()
            .map_err(|error| Error::Journal(error.to_string()))?
        {
            BinaryWalReadEvent::Record {
                payload,
                end_offset,
                ..
            } if end_offset == receipt.end => Batch::decode(&payload)?,
            _ => return Err(Error::Invalid("missing complete commit receipt")),
        };
        if batch.epoch != epoch
            || !matches!(
                reader
                    .next_event()
                    .map_err(|error| Error::Journal(error.to_string()))?,
                BinaryWalReadEvent::Eof
            )
        {
            return Err(Error::Invalid("receipt epoch or boundary mismatch"));
        }
        Ok(Arc::new(batch))
    }
    fn record(&mut self, batch: Batch, start: u64, end: u64) {
        let batch = Arc::new(batch);
        for (position, change) in batch.writes.iter().enumerate() {
            if let Some(entry) = self.latest.get_mut(change.key.as_slice()) {
                entry.epoch = batch.epoch;
                entry.position = position;
                entry.batch = Arc::clone(&batch);
                entry.versions.push_back(batch.epoch);
            } else {
                self.latest.insert(
                    change.key.clone(),
                    Latest {
                        epoch: batch.epoch,
                        position,
                        batch: Arc::clone(&batch),
                        versions: VecDeque::from([batch.epoch]),
                    },
                );
            }
            self.retained_versions += 1;
        }
        self.receipts.insert(
            batch.operation,
            Receipt {
                epoch: batch.epoch,
                start,
                end,
            },
        );
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        // Ownership follows live Store/transaction handles. Descriptor copies
        // made by readers or briefly inherited before a child's close-on-exec
        // must not extend it after the last owning handle has closed.
        let _ = self.file.unlock();
    }
}

struct Inner<S: Storage> {
    index: RwLock<Index<S>>,
    snapshots: Mutex<BTreeMap<u64, usize>>,
    index_generation: AtomicU64,
    recovery: maintenance::RecoveryReport,
    layout: Layout,
    published: AtomicU64,
    poisoned: AtomicBool,
    journal: Mutex<Journal>,
}
pub struct Store<S: Storage = BfTreeStorage>(Arc<Inner<S>>);
pub struct ReadTxn<S: Storage = BfTreeStorage> {
    inner: Arc<Inner<S>>,
    epoch: u64,
    view: View<S>,
}
pub struct WriteTxn<S: Storage = BfTreeStorage> {
    read: ReadTxn<S>,
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

impl<S: Storage> Clone for Store<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Store<BfTreeStorage> {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_layout(path, Layout::Hybrid)
    }
    pub fn open_layout(path: &Path, layout: Layout) -> Result<Self> {
        Self::open_with_storage(path, layout)
    }
}

impl<S: Storage> Store<S> {
    /// Select S for the versioned and hybrid layouts. The COW reference
    /// layout uses its own immutable map snapshots.
    pub fn open_with_storage(path: &Path, layout: Layout) -> Result<Self> {
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
        let mut journal = Journal {
            file,
            len,
            latest: BTreeMap::new(),
            receipts: BTreeMap::new(),
            retained_versions: 0,
            gc_cursor: None,
            gc_pass_horizon: None,
        };
        let (index, epoch, recovery) = maintenance::recover(&mut journal, layout)?;
        let published = AtomicU64::new(epoch);
        Ok(Self(Arc::new(Inner {
            index: RwLock::new(index),
            snapshots: Mutex::new(BTreeMap::new()),
            index_generation: AtomicU64::new(1),
            recovery,
            layout,
            published,
            poisoned: AtomicBool::new(false),
            journal: Mutex::new(journal),
        })))
    }
    pub fn begin_read(&self) -> Result<ReadTxn<S>> {
        self.0.usable()?;
        // Collection uses this same gate while choosing a horizon. Register
        // before releasing it, so no snapshot is captured but unprotected.
        let mut snapshots = self.0.snapshots.lock().map_err(|_| Error::NeedsRecovery)?;
        let (epoch, view) = self
            .0
            .index
            .read()
            .map_err(|_| Error::NeedsRecovery)?
            .capture(&self.0.published)?;
        self.0.usable()?;
        let count = snapshots.entry(epoch).or_default();
        *count = count
            .checked_add(1)
            .ok_or(Error::Budget("snapshot references"))?;
        Ok(ReadTxn {
            inner: Arc::clone(&self.0),
            epoch,
            view,
        })
    }
    pub fn begin_write(&self) -> Result<WriteTxn<S>> {
        Ok(WriteTxn {
            read: self.begin_read()?,
            writes: BTreeMap::new(),
        })
    }
    pub fn journal_bytes(&self) -> Result<u64> {
        Ok(self.0.journal.lock().map_err(|_| Error::NeedsRecovery)?.len)
    }
}
impl<S: Storage> Inner<S> {
    fn usable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire)
            || self.journal.is_poisoned()
            || self.index.is_poisoned()
            || self.snapshots.is_poisoned()
        {
            Err(Error::NeedsRecovery)
        } else {
            Ok(())
        }
    }
}
impl<S: Storage> Drop for ReadTxn<S> {
    fn drop(&mut self) {
        let Ok(mut snapshots) = self.inner.snapshots.lock() else {
            self.inner.poisoned.store(true, Ordering::Release);
            return;
        };
        if let Some(count) = snapshots.get_mut(&self.epoch) {
            *count -= 1;
            if *count == 0 {
                snapshots.remove(&self.epoch);
            }
        } else {
            self.inner.poisoned.store(true, Ordering::Release);
        }
    }
}
impl<S: Storage> ReadTxn<S> {
    pub fn get_many(&self, keys: &[&[u8]]) -> Result<BatchRead> {
        if keys.len() > MAX_BATCH_LOOKUPS || keys.iter().any(|key| key.len() > MAX_KEY) {
            return Err(Error::Budget("batch lookup keys"));
        }
        self.inner.usable()?;
        let batch = self.view.get_many(keys)?;
        self.inner.usable()?;
        Ok(batch)
    }
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
impl<S: Storage> WriteTxn<S> {
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
        if let Some(&receipt) = journal.receipts.get(&operation) {
            let previous = journal
                .batch(receipt, &writes[0].key, inner.layout.id())
                .inspect_err(|_| {
                    inner.poisoned.store(true, Ordering::Release);
                })?;
            return if previous.operation == operation && previous.writes == writes {
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
        let (frame, new_len) = journal.frame(&payload, inner.layout.id())?;
        // From the first write onward, an error may leave a durable commit.
        // Poison all handles until authoritative journal replay resolves it.
        let mut uncertain = Uncertain {
            poisoned: &inner.poisoned,
            armed: true,
        };
        let outcome: Result<()> = (|| {
            journal.write_frame(&frame)?;
            hook(Stage::Durable)?;
            let index = inner.index.read().map_err(|_| Error::NeedsRecovery)?;
            let candidate = index.prepare(
                &batch,
                |key| journal.previous(key),
                |n| hook(Stage::Installed(n)),
            )?;
            index.publish(candidate, epoch, &inner.published)?;
            hook(Stage::Published)?;
            Ok(())
        })();
        outcome.map_err(|error| Error::CommitUncertain(error.to_string()))?;
        let start = journal.len - PREFIX;
        journal.len = new_len;
        journal.record(batch, start, new_len - PREFIX);
        uncertain.armed = false;
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests;
