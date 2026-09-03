//! Typed, memory-bounded external ordering shared by executor front ends.

use crate::blocking::spill_backed_report;
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::pipeline::runtime_checkpoint;
use crate::spill::{SpillReader, SpillRun, SpillWriter};
use crate::{
    BlockingOperatorMemoryReport, ExecutionMemoryConfig, QueryMemoryAccount, QueryMemoryClass,
    QueryMemoryLedger,
};
use skein_core::{Result, RuntimeTaskContext, SkeinError};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

const EXTERNAL_ORDER_RECORD_VERSION: u8 = 1;
const EXTERNAL_ORDER_HEADER_BYTES: usize = 1 + std::mem::size_of::<u64>();

/// A compact typed record that can participate in a stable external order.
///
/// Implementations own only comparison and a symmetric payload codec. The
/// executor owns ordinals, memory admission, spill lifecycle, merge fan-in,
/// cancellation, and cleanup.
pub trait ExternalOrderRecord: Sized {
    fn compare(&self, other: &Self) -> Ordering;

    fn memory_bytes(&self) -> usize;

    fn encoded_len(&self) -> Result<usize>;

    fn encode(&self, output: &mut Vec<u8>) -> Result<()>;

    fn decode(input: &[u8]) -> Result<Self>;
}

struct StableOrderRecord<R> {
    ordinal: u64,
    record: R,
}

impl<R: ExternalOrderRecord> StableOrderRecord<R> {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<u64>().saturating_add(self.record.memory_bytes())
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        self.record
            .compare(&other.record)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

impl<R: ExternalOrderRecord> PartialEq for StableOrderRecord<R> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_key(other) == Ordering::Equal
    }
}

impl<R: ExternalOrderRecord> Eq for StableOrderRecord<R> {}

impl<R: ExternalOrderRecord> Ord for StableOrderRecord<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_key(other)
    }
}

impl<R: ExternalOrderRecord> PartialOrd for StableOrderRecord<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct MergeEntry<R> {
    row: StableOrderRecord<R>,
    run_index: usize,
}

impl<R: ExternalOrderRecord> PartialEq for MergeEntry<R> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row && self.run_index == other.run_index
    }
}

impl<R: ExternalOrderRecord> Eq for MergeEntry<R> {}

impl<R: ExternalOrderRecord> Ord for MergeEntry<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl<R: ExternalOrderRecord> PartialOrd for MergeEntry<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Stable TopN/full-sort execution over compact typed records.
///
/// `limit = usize::MAX` gives a full external order. Finite limits retain only
/// `offset + limit` candidates across runs.
pub struct ExternalTopN<'runtime, R: ExternalOrderRecord> {
    operator: &'static str,
    file_operator: &'static str,
    offset: usize,
    retained: usize,
    memory: &'runtime ExecutionMemoryConfig,
    task_context: Option<&'runtime RuntimeTaskContext>,
    blocking_account: QueryMemoryAccount,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    runs: Vec<SpillRun>,
    heap: BinaryHeap<StableOrderRecord<R>>,
    input_rows: u64,
    spilled_rows: usize,
    merge_peak_bytes: usize,
}

impl<'runtime, R: ExternalOrderRecord> ExternalTopN<'runtime, R> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operator: &'static str,
        file_operator: &'static str,
        offset: usize,
        limit: usize,
        memory: &'runtime ExecutionMemoryConfig,
        memory_ledger: &QueryMemoryLedger,
        task_context: Option<&'runtime RuntimeTaskContext>,
    ) -> Self {
        let blocking_account = memory_ledger.account(
            QueryMemoryClass::BlockingState,
            operator,
            memory.blocking_operator_bytes,
        );
        Self {
            operator,
            file_operator,
            offset,
            retained: offset.saturating_add(limit),
            memory,
            task_context,
            tracker: OperatorMemoryTracker::with_account(
                memory.blocking_operator_bytes,
                blocking_account.clone(),
            ),
            blocking_account,
            spill_budget: SpillBudgetTracker::with_ledger(operator, memory, memory_ledger),
            runs: Vec::new(),
            heap: BinaryHeap::new(),
            input_rows: 0,
            spilled_rows: 0,
            merge_peak_bytes: 0,
        }
    }

    pub fn push(&mut self, record: R) -> Result<()> {
        if self.retained == 0 {
            return Ok(());
        }
        runtime_checkpoint(self.task_context)?;
        let candidate = StableOrderRecord {
            ordinal: self.input_rows,
            record,
        };
        self.input_rows = self.input_rows.saturating_add(1);
        let bytes = candidate.memory_bytes();
        ensure_operator_item_fits(self.operator, bytes, &self.tracker)?;
        if self.heap.len() < self.retained {
            if self.tracker.would_exceed(bytes) {
                self.spill_heap()?;
            }
            self.tracker.try_charge(bytes)?;
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            let worst_bytes = self
                .heap
                .peek()
                .map(StableOrderRecord::memory_bytes)
                .unwrap_or(0);
            if self
                .tracker
                .used_bytes
                .saturating_sub(worst_bytes)
                .saturating_add(bytes)
                > self.tracker.budget_bytes
            {
                self.spill_heap()?;
            } else {
                self.heap.pop();
                self.tracker.release(worst_bytes);
            }
            self.tracker.try_charge(bytes)?;
            self.heap.push(candidate);
        }
        Ok(())
    }

    pub fn finish(
        mut self,
        mut visit: impl FnMut(R) -> Result<bool>,
    ) -> Result<BlockingOperatorMemoryReport> {
        runtime_checkpoint(self.task_context)?;
        if self.runs.is_empty() {
            let mut selected = std::mem::take(&mut self.heap).into_vec();
            selected.sort_unstable();
            for (rank, row) in selected.into_iter().enumerate() {
                runtime_checkpoint(self.task_context)?;
                let bytes = row.memory_bytes();
                let should_visit = rank >= self.offset && rank < self.retained;
                let keep_going = !should_visit || visit(row.record)?;
                self.tracker.release(bytes);
                if !keep_going || rank.saturating_add(1) >= self.retained {
                    break;
                }
            }
            self.tracker.reset();
        } else {
            if !self.heap.is_empty() {
                self.spill_heap()?;
            }
            self.compact_runs()?;
            self.merge_peak_bytes = self.merge_peak_bytes.max(self.merge_runs(&mut visit)?);
        }
        Ok(spill_backed_report(
            self.operator,
            &self.tracker,
            self.tracker.peak_bytes.max(self.merge_peak_bytes),
            self.input_rows as usize,
            &self.spill_budget,
            self.spilled_rows,
        ))
    }

    fn spill_heap(&mut self) -> Result<()> {
        runtime_checkpoint(self.task_context)?;
        let mut rows = std::mem::take(&mut self.heap).into_vec();
        rows.sort_unstable();
        self.spilled_rows = self.spilled_rows.saturating_add(rows.len());
        let (run, mut writer) = self.spill_budget.create_run(self.file_operator)?;
        for row in rows {
            runtime_checkpoint(self.task_context)?;
            write_record(&mut writer, &row, &mut self.spill_budget)?;
        }
        writer.finish()?;
        self.runs.push(run);
        self.tracker.reset();
        Ok(())
    }

    fn compact_runs(&mut self) -> Result<()> {
        while self.runs.len() > 2 {
            runtime_checkpoint(self.task_context)?;
            let mut compacted = Vec::with_capacity(self.runs.len().div_ceil(2));
            let mut pending = std::mem::take(&mut self.runs).into_iter();
            while let Some(left) = pending.next() {
                let Some(right) = pending.next() else {
                    compacted.push(left);
                    break;
                };
                let (run, peak) = merge_run_pair::<R>(
                    &left,
                    &right,
                    self.retained,
                    self.operator,
                    self.file_operator,
                    self.memory,
                    self.blocking_account.clone(),
                    &mut self.spill_budget,
                    self.task_context,
                )?;
                self.merge_peak_bytes = self.merge_peak_bytes.max(peak);
                compacted.push(run);
            }
            self.runs = compacted;
        }
        Ok(())
    }

    fn merge_runs(&self, visit: &mut impl FnMut(R) -> Result<bool>) -> Result<usize> {
        let mut readers = self
            .runs
            .iter()
            .map(SpillRun::reader)
            .collect::<Result<Vec<_>>>()?;
        let mut tracker = OperatorMemoryTracker::with_account(
            self.memory.blocking_operator_bytes,
            self.blocking_account.clone(),
        );
        let mut heap = BinaryHeap::new();
        for (run_index, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = read_record::<R>(
                reader,
                self.operator,
                self.memory,
                &self.spill_budget,
                &mut tracker,
            )? {
                heap.push(MergeEntry { row, run_index });
            }
        }
        let mut rank = 0usize;
        while let Some(entry) = heap.pop() {
            runtime_checkpoint(self.task_context)?;
            let bytes = entry.row.memory_bytes();
            let run_index = entry.run_index;
            let should_visit = rank >= self.offset && rank < self.retained;
            let keep_going = !should_visit || visit(entry.row.record)?;
            tracker.release(bytes);
            rank = rank.saturating_add(1);
            if !keep_going || rank >= self.retained {
                break;
            }
            if let Some(row) = read_record::<R>(
                &mut readers[run_index],
                self.operator,
                self.memory,
                &self.spill_budget,
                &mut tracker,
            )? {
                heap.push(MergeEntry { row, run_index });
            }
        }
        Ok(tracker.peak_bytes)
    }
}

#[allow(clippy::too_many_arguments)]
fn merge_run_pair<R: ExternalOrderRecord>(
    left: &SpillRun,
    right: &SpillRun,
    retained: usize,
    operator: &'static str,
    file_operator: &'static str,
    memory: &ExecutionMemoryConfig,
    blocking_account: QueryMemoryAccount,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<(SpillRun, usize)> {
    let mut readers = [left.reader()?, right.reader()?];
    let mut tracker =
        OperatorMemoryTracker::with_account(memory.blocking_operator_bytes, blocking_account);
    let mut heap = BinaryHeap::new();
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_record::<R>(reader, operator, memory, spill_budget, &mut tracker)? {
            heap.push(MergeEntry { row, run_index });
        }
    }
    let (run, mut writer) = spill_budget.create_run(file_operator)?;
    let mut written = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let bytes = entry.row.memory_bytes();
        let run_index = entry.run_index;
        write_record(&mut writer, &entry.row, spill_budget)?;
        tracker.release(bytes);
        written = written.saturating_add(1);
        if written >= retained {
            break;
        }
        if let Some(row) = read_record::<R>(
            &mut readers[run_index],
            operator,
            memory,
            spill_budget,
            &mut tracker,
        )? {
            heap.push(MergeEntry { row, run_index });
        }
    }
    writer.finish()?;
    Ok((run, tracker.peak_bytes))
}

fn write_record<R: ExternalOrderRecord>(
    writer: &mut SpillWriter,
    row: &StableOrderRecord<R>,
    spill_budget: &mut SpillBudgetTracker,
) -> Result<()> {
    let record_len = row.record.encoded_len()?;
    let payload_len = EXTERNAL_ORDER_HEADER_BYTES
        .checked_add(record_len)
        .ok_or_else(|| SkeinError::Execution("external order record size overflow".to_string()))?;
    let _staging_lease = spill_budget.reserve_staging(payload_len)?;
    let mut payload = Vec::with_capacity(payload_len);
    payload.push(EXTERNAL_ORDER_RECORD_VERSION);
    payload.extend_from_slice(&row.ordinal.to_le_bytes());
    row.record.encode(&mut payload)?;
    if payload.len() != payload_len {
        return Err(SkeinError::Execution(format!(
            "external order codec declared {record_len} bytes but encoded {} bytes",
            payload.len().saturating_sub(EXTERNAL_ORDER_HEADER_BYTES)
        )));
    }
    writer.write_record_payload(&payload, spill_budget)?;
    Ok(())
}

fn read_record<R: ExternalOrderRecord>(
    reader: &mut SpillReader,
    operator: &'static str,
    memory: &ExecutionMemoryConfig,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
) -> Result<Option<StableOrderRecord<R>>> {
    let Some(payload) =
        reader.read_record_payload(memory.blocking_operator_bytes.get(), spill_budget)?
    else {
        return Ok(None);
    };
    let bytes = payload.as_slice();
    if bytes.len() < EXTERNAL_ORDER_HEADER_BYTES {
        return Err(invalid_record("truncated header"));
    }
    if bytes[0] != EXTERNAL_ORDER_RECORD_VERSION {
        return Err(invalid_record("unsupported version"));
    }
    let ordinal = u64::from_le_bytes(
        bytes[1..EXTERNAL_ORDER_HEADER_BYTES]
            .try_into()
            .expect("validated external order header width"),
    );
    let row = StableOrderRecord {
        ordinal,
        record: R::decode(&bytes[EXTERNAL_ORDER_HEADER_BYTES..])?,
    };
    let row_bytes = row.memory_bytes();
    ensure_operator_item_fits(operator, row_bytes, tracker)?;
    tracker.try_charge(row_bytes)?;
    Ok(Some(row))
}

fn invalid_record(reason: &str) -> SkeinError {
    SkeinError::Execution(format!("external order spill record is invalid: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, PartialEq, Eq)]
    struct TestRecord {
        key: i64,
        payload: Vec<u8>,
    }

    impl ExternalOrderRecord for TestRecord {
        fn compare(&self, other: &Self) -> Ordering {
            self.key.cmp(&other.key)
        }

        fn memory_bytes(&self) -> usize {
            std::mem::size_of::<Self>().saturating_add(self.payload.len())
        }

        fn encoded_len(&self) -> Result<usize> {
            Ok(12usize.saturating_add(self.payload.len()))
        }

        fn encode(&self, output: &mut Vec<u8>) -> Result<()> {
            output.extend_from_slice(&self.key.to_le_bytes());
            output.extend_from_slice(
                &u32::try_from(self.payload.len())
                    .map_err(|_| SkeinError::Execution("test payload is too large".to_string()))?
                    .to_le_bytes(),
            );
            output.extend_from_slice(&self.payload);
            Ok(())
        }

        fn decode(input: &[u8]) -> Result<Self> {
            if input.len() < 12 {
                return Err(SkeinError::Execution(
                    "test external record is truncated".to_string(),
                ));
            }
            let key = i64::from_le_bytes(input[..8].try_into().expect("checked key width"));
            let len =
                u32::from_le_bytes(input[8..12].try_into().expect("checked length width")) as usize;
            if input.len() != 12usize.saturating_add(len) {
                return Err(SkeinError::Execution(
                    "test external record length mismatch".to_string(),
                ));
            }
            Ok(Self {
                key,
                payload: input[12..].to_vec(),
            })
        }
    }

    fn test_memory(name: &str, blocking_bytes: usize) -> ExecutionMemoryConfig {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(64).unwrap(),
            max_total_spill_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            max_total_spill_runs: NonZeroUsize::new(128).unwrap(),
            min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
            spill_directory: std::env::temp_dir().join(format!(
                "skein-external-order-{}-{name}-{nonce}",
                std::process::id()
            )),
            ..ExecutionMemoryConfig::default()
        }
    }

    #[test]
    fn typed_top_n_spills_preserves_stability_and_releases_query_memory() {
        let mut memory = test_memory("stable", 256);
        memory.spill_free_space_probe_interval_bytes = NonZeroU64::new(1024).unwrap();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut order = ExternalTopN::new("TypedTopN", "typed-topn", 1, 3, &memory, &ledger, None);
        for (key, marker) in [(5, 0), (1, 1), (1, 2), (4, 3), (2, 4), (3, 5)] {
            order
                .push(TestRecord {
                    key,
                    payload: vec![marker; 80],
                })
                .unwrap();
        }
        let mut output = Vec::new();
        let report = order
            .finish(|record| {
                output.push((record.key, record.payload[0]));
                Ok(true)
            })
            .unwrap();

        assert_eq!(output, [(1, 2), (2, 4), (3, 5)]);
        assert!(report.spill_run_count > 0);
        let pool = memory.spill_pool_snapshot().unwrap();
        assert!(pool.free_space_probe_count < report.spilled_rows as u64);
        assert!(
            pool.free_space_probe_count
                <= report
                    .spilled_bytes
                    .div_ceil(memory.spill_free_space_probe_interval_bytes.get())
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(&memory.spill_directory).unwrap();
    }

    #[test]
    fn typed_record_codec_rejects_truncation() {
        let error = TestRecord::decode(&[0; 11]).unwrap_err();
        assert!(error.to_string().contains("truncated"));
    }
}
