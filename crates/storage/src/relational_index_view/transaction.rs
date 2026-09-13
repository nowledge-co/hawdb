use super::authoritative::{map_constraint_read_error, AuthoritativeReadLedger};
use super::{
    RelationalIndexProbeStatistics, RelationalIndexReadSelector, RelationalIndexReadView,
    RelationalIndexReadViewReport,
};
use crate::{
    RelationalConstraintIndex, RelationalError, RelationalIndexChange,
    RelationalIndexChangeCapture, RelationalIndexChangeCaptureLimits, RelationalIndexChangeKind,
    RelationalIndexRangeScan, RelationalIndexReadLimits, RelationalIndexScanDirection,
    RelationalIndexShadowError, RelationalKey,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug)]
struct RelationalTransactionIndexBatch {
    changes: Box<[RelationalIndexChange]>,
    encoded_bytes: usize,
}

#[derive(Debug)]
struct RelationalTransactionIndexOverlay {
    batches: Vec<RelationalTransactionIndexBatch>,
    entry_count: usize,
    encoded_bytes: usize,
    limits: RelationalIndexChangeCaptureLimits,
}

impl RelationalTransactionIndexOverlay {
    fn new(limits: RelationalIndexChangeCaptureLimits) -> Self {
        Self {
            batches: Vec::new(),
            entry_count: 0,
            encoded_bytes: 0,
            limits,
        }
    }

    fn append(&mut self, capture: RelationalIndexChangeCapture) -> Result<(), RelationalError> {
        let (changes, encoded_bytes) = match capture {
            RelationalIndexChangeCapture::Captured {
                changes,
                encoded_bytes,
            } => (changes, encoded_bytes),
            RelationalIndexChangeCapture::Invalidated { reason } => {
                return Err(RelationalError::Admission(format!(
                    "authoritative transaction index overlay is unavailable: {reason}"
                )));
            }
        };
        if changes.is_empty() && encoded_bytes != 0 {
            return Err(RelationalError::Corruption(
                "empty authoritative transaction index capture reports resident bytes".to_string(),
            ));
        }
        let next_entries = self.entry_count.checked_add(changes.len()).ok_or_else(|| {
            RelationalError::Admission(
                "authoritative transaction index entry accounting overflow".to_string(),
            )
        })?;
        let next_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "authoritative transaction index byte accounting overflow".to_string(),
                )
            })?;
        if next_entries > self.limits.max_entries.get() || next_bytes > self.limits.max_bytes.get()
        {
            return Err(RelationalError::Admission(format!(
                "authoritative transaction index overlay exceeds max_entries={} or max_bytes={}",
                self.limits.max_entries, self.limits.max_bytes
            )));
        }
        if !changes.is_empty() {
            self.batches.push(RelationalTransactionIndexBatch {
                changes: changes.into_boxed_slice(),
                encoded_bytes,
            });
        }
        self.entry_count = next_entries;
        self.encoded_bytes = next_bytes;
        Ok(())
    }

    fn touches(&self, table: &str, index: &str) -> bool {
        self.batches.iter().any(|batch| {
            batch
                .changes
                .iter()
                .any(|change| change.table == table && change.index == index)
        })
    }
}

struct TransactionOverlayMerge<'a, F> {
    overlay: BTreeMap<RelationalKey, RelationalIndexChangeKind>,
    visit: &'a mut F,
    overlay_rows_emitted: usize,
    stopped_early: bool,
}

#[derive(Clone, Copy)]
struct TransactionPostingState {
    base_present: bool,
    current_present: bool,
}

impl TransactionPostingState {
    fn first(kind: RelationalIndexChangeKind) -> Self {
        match kind {
            RelationalIndexChangeKind::Delete => Self {
                base_present: true,
                current_present: false,
            },
            RelationalIndexChangeKind::Insert => Self {
                base_present: false,
                current_present: true,
            },
        }
    }

    fn apply(&mut self, kind: RelationalIndexChangeKind) -> Result<(), RelationalIndexShadowError> {
        match (self.current_present, kind) {
            (true, RelationalIndexChangeKind::Delete) => self.current_present = false,
            (false, RelationalIndexChangeKind::Insert) => self.current_present = true,
            (true, RelationalIndexChangeKind::Insert) => {
                return Err(RelationalIndexShadowError::Corrupt(
                    "transaction index overlay inserts an already-present posting".to_string(),
                ));
            }
            (false, RelationalIndexChangeKind::Delete) => {
                return Err(RelationalIndexShadowError::Corrupt(
                    "transaction index overlay deletes an absent posting".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn effect(self) -> Option<RelationalIndexChangeKind> {
        match (self.base_present, self.current_present) {
            (true, false) => Some(RelationalIndexChangeKind::Delete),
            (false, true) => Some(RelationalIndexChangeKind::Insert),
            (true, true) | (false, false) => None,
        }
    }
}

impl<F> TransactionOverlayMerge<'_, F>
where
    F: FnMut(&RelationalKey) -> bool,
{
    fn emit_overlay(&mut self, primary_key: &RelationalKey) -> bool {
        self.overlay_rows_emitted = self
            .overlay_rows_emitted
            .checked_add(1)
            .expect("reserved transaction overlay row count cannot overflow");
        if !(self.visit)(primary_key) {
            self.stopped_early = true;
            return false;
        }
        true
    }

    fn visit_base(&mut self, primary_key: &RelationalKey) -> bool {
        match self.overlay.remove(primary_key) {
            Some(RelationalIndexChangeKind::Delete) => true,
            Some(RelationalIndexChangeKind::Insert) | None => {
                if !(self.visit)(primary_key) {
                    self.stopped_early = true;
                    return false;
                }
                true
            }
        }
    }

    fn finish(&mut self) {
        while !self.stopped_early {
            let Some((primary_key, kind)) = self.overlay.pop_first() else {
                break;
            };
            if kind == RelationalIndexChangeKind::Insert && !self.emit_overlay(&primary_key) {
                break;
            }
        }
    }
}

/// A transaction-private authoritative index view.
///
/// The committed base is pinned when the transaction begins. Successful SQL
/// statements append bounded index-change batches; failed statements do not
/// mutate this view. Reads merge the pinned base and every prior statement so
/// read-your-own-writes never requires database-sized materialized postings.
#[derive(Debug)]
pub struct RelationalTransactionIndexView {
    base: Arc<RelationalIndexReadView>,
    overlay: RelationalTransactionIndexOverlay,
    read_ledger: AuthoritativeReadLedger,
}

impl RelationalTransactionIndexView {
    pub fn new(
        base: Arc<RelationalIndexReadView>,
        capture_limits: RelationalIndexChangeCaptureLimits,
        read_limits: RelationalIndexReadLimits,
    ) -> Self {
        Self {
            base,
            overlay: RelationalTransactionIndexOverlay::new(capture_limits),
            read_ledger: AuthoritativeReadLedger::new(read_limits),
        }
    }

    pub fn capture_limits(&self) -> RelationalIndexChangeCaptureLimits {
        self.overlay.limits
    }

    pub fn fresh_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        if self.overlay.touches(table, index) {
            return None;
        }
        self.base.fresh_probe_statistics(table, index, prefix_len)
    }

    pub fn append(&mut self, capture: RelationalIndexChangeCapture) -> Result<(), RelationalError> {
        self.overlay.append(capture)
    }

    pub fn visit_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_range_entries(
            table,
            index,
            &RelationalIndexRangeScan {
                prefix: prefix.clone(),
                exclusive_bound: None,
                direction: RelationalIndexScanDirection::Forward,
            },
            limits,
            visit,
        )
    }

    pub fn visit_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        let transaction_remaining = self
            .read_ledger
            .remaining_limits()
            .map_err(relational_read_error)?;
        let limits = intersect_read_limits(limits, transaction_remaining);
        let report =
            self.visit_prefix_entries_many_with_overlay(table, index, prefixes, limits, visit)?;
        self.read_ledger
            .record(&report)
            .map_err(relational_read_error)?;
        Ok(report)
    }

    pub fn visit_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        let transaction_remaining = self
            .read_ledger
            .remaining_limits()
            .map_err(relational_read_error)?;
        let limits = intersect_read_limits(limits, transaction_remaining);
        let report = self.visit_range_entries_with_overlay(table, index, scan, limits, visit)?;
        self.read_ledger
            .record(&report)
            .map_err(relational_read_error)?;
        Ok(report)
    }

    fn visit_range_entries_with_overlay(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        let selector = RelationalIndexReadSelector::Range(scan);
        let mut posting_states =
            BTreeMap::<(RelationalKey, RelationalKey), TransactionPostingState>::new();
        let mut entries_visited = 0usize;
        let mut entries_matched = 0usize;
        let mut bytes_visited = 0usize;
        for batch in &self.overlay.batches {
            bytes_visited = bytes_visited
                .checked_add(batch.encoded_bytes)
                .ok_or_else(|| admission("transaction index byte accounting overflow"))?;
            for change in batch.changes.iter() {
                entries_visited = entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("transaction index entry accounting overflow"))?;
                if change.table != table
                    || change.index != index
                    || !selector.matches(&change.index_key)
                {
                    continue;
                }
                entries_matched = entries_matched.checked_add(1).ok_or_else(|| {
                    admission("transaction index matched-entry accounting overflow")
                })?;
                let entry_key = (change.index_key.clone(), change.primary_key.clone());
                match posting_states.entry(entry_key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(TransactionPostingState::first(change.kind));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().apply(change.kind)?;
                    }
                }
            }
        }
        let overlay = posting_states
            .into_iter()
            .filter_map(|(entry, state)| state.effect().map(|kind| (entry, kind)))
            .collect::<BTreeMap<_, _>>();
        let reserved_inserts = overlay
            .values()
            .filter(|kind| **kind == RelationalIndexChangeKind::Insert)
            .count();
        let backend_rows = limits
            .max_rows
            .get()
            .checked_sub(reserved_inserts)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction index ordered overlay reserves {reserved_inserts} rows, exhausting row limit {}",
                    limits.max_rows
                ))
            })?;
        let backend_bytes = limits
            .max_bytes
            .get()
            .checked_sub(bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction index overlay needs {bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_rows,
            max_bytes: backend_bytes,
            ..limits
        };
        let mut merge = super::OrderedIndexEntryMerge {
            pending: overlay,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: scan.direction,
        };
        let mut report = {
            let mut emit_base = |index_key: &RelationalKey, primary_key: &RelationalKey| {
                merge.visit_base(index_key, primary_key)
            };
            self.base
                .visit_range_entries(table, index, scan, backend_limits, &mut emit_base)?
        };
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        report.live_batches_visited = checked_add(
            report.live_batches_visited,
            self.overlay.batches.len(),
            "transaction index batch count",
        )?;
        report.live_entries_visited = checked_add(
            report.live_entries_visited,
            entries_visited,
            "transaction index entry count",
        )?;
        report.live_entries_matched = checked_add(
            report.live_entries_matched,
            entries_matched,
            "transaction index matched-entry count",
        )?;
        report.live_bytes_visited = checked_add(
            report.live_bytes_visited,
            bytes_visited,
            "transaction index byte count",
        )?;
        report.rows_visited = merge.rows_visited;
        report.stopped_early |= merge.stopped_early;
        Ok(report)
    }

    fn visit_prefix_entries_many_with_overlay(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        if prefixes.is_empty() {
            return Err(admission(
                "transaction batch index lookup requires at least one prefix",
            ));
        }
        let prefix_width = prefixes[0].0.len();
        if prefixes.iter().any(|prefix| prefix.0.len() != prefix_width) {
            return Err(admission(
                "batch index prefixes must have one common key width",
            ));
        }
        let mut posting_states =
            BTreeMap::<(RelationalKey, RelationalKey), TransactionPostingState>::new();
        let mut entries_visited = 0usize;
        let mut entries_matched = 0usize;
        let mut bytes_visited = 0usize;
        for batch in &self.overlay.batches {
            bytes_visited = bytes_visited
                .checked_add(batch.encoded_bytes)
                .ok_or_else(|| admission("transaction batch index byte accounting overflow"))?;
            for change in batch.changes.iter() {
                entries_visited = entries_visited.checked_add(1).ok_or_else(|| {
                    admission("transaction batch index entry accounting overflow")
                })?;
                if change.table != table
                    || change.index != index
                    || !prefixes
                        .iter()
                        .any(|prefix| change.index_key.0.starts_with(&prefix.0))
                {
                    continue;
                }
                entries_matched = entries_matched.checked_add(1).ok_or_else(|| {
                    admission("transaction batch index matched-entry accounting overflow")
                })?;
                let entry_key = (change.index_key.clone(), change.primary_key.clone());
                match posting_states.entry(entry_key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(TransactionPostingState::first(change.kind));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().apply(change.kind)?;
                    }
                }
            }
        }
        let overlay = posting_states
            .into_iter()
            .filter_map(|(entry, state)| state.effect().map(|kind| (entry, kind)))
            .collect::<BTreeMap<_, _>>();
        let reserved_inserts = overlay
            .values()
            .filter(|kind| **kind == RelationalIndexChangeKind::Insert)
            .count();
        let backend_rows = limits
            .max_rows
            .get()
            .checked_sub(reserved_inserts)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction batch index overlay reserves {reserved_inserts} rows, exhausting row limit {}",
                    limits.max_rows
                ))
            })?;
        let backend_bytes = limits
            .max_bytes
            .get()
            .checked_sub(bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction batch index overlay needs {bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_rows,
            max_bytes: backend_bytes,
            ..limits
        };
        let mut merge = super::OrderedIndexEntryMerge {
            pending: overlay,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: RelationalIndexScanDirection::Forward,
        };
        let mut report = {
            let mut emit_base = |index_key: &RelationalKey, primary_key: &RelationalKey| {
                merge.visit_base(index_key, primary_key)
            };
            self.base.visit_prefix_entries_many(
                table,
                index,
                prefixes,
                backend_limits,
                &mut emit_base,
            )?
        };
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        report.live_batches_visited = checked_add(
            report.live_batches_visited,
            self.overlay.batches.len(),
            "transaction batch index count",
        )?;
        report.live_entries_visited = checked_add(
            report.live_entries_visited,
            entries_visited,
            "transaction batch index entry count",
        )?;
        report.live_entries_matched = checked_add(
            report.live_entries_matched,
            entries_matched,
            "transaction batch index matched-entry count",
        )?;
        report.live_bytes_visited = checked_add(
            report.live_bytes_visited,
            bytes_visited,
            "transaction batch index byte count",
        )?;
        report.rows_visited = merge.rows_visited;
        report.stopped_early |= merge.stopped_early;
        Ok(report)
    }

    fn visit_with_overlay(
        &self,
        table: &str,
        index: &str,
        selector: RelationalIndexReadSelector<'_>,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        let mut posting_states = BTreeMap::<RelationalKey, TransactionPostingState>::new();
        let mut entries_visited = 0usize;
        let mut entries_matched = 0usize;
        let mut bytes_visited = 0usize;
        for batch in &self.overlay.batches {
            bytes_visited = bytes_visited
                .checked_add(batch.encoded_bytes)
                .ok_or_else(|| admission("transaction index byte accounting overflow"))?;
            for change in batch.changes.iter() {
                entries_visited = entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("transaction index entry accounting overflow"))?;
                if change.table != table
                    || change.index != index
                    || !selector.matches(&change.index_key)
                {
                    continue;
                }
                entries_matched = entries_matched.checked_add(1).ok_or_else(|| {
                    admission("transaction index matched-entry accounting overflow")
                })?;
                match posting_states.entry(change.primary_key.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(TransactionPostingState::first(change.kind));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().apply(change.kind)?;
                    }
                }
            }
        }
        let overlay = posting_states
            .into_iter()
            .filter_map(|(primary_key, state)| state.effect().map(|kind| (primary_key, kind)))
            .collect::<BTreeMap<_, _>>();
        let reserved_inserts = overlay
            .values()
            .filter(|kind| **kind == RelationalIndexChangeKind::Insert)
            .count();
        let backend_rows = limits
            .max_rows
            .get()
            .checked_sub(reserved_inserts)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction index overlay reserves {reserved_inserts} rows, exhausting row limit {}",
                    limits.max_rows
                ))
            })?;
        let backend_bytes = limits
            .max_bytes
            .get()
            .checked_sub(bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "transaction index overlay needs {bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_rows,
            max_bytes: backend_bytes,
            ..limits
        };

        let mut merge = TransactionOverlayMerge {
            overlay,
            visit: &mut visit,
            overlay_rows_emitted: 0,
            stopped_early: false,
        };
        let mut report = {
            let mut emit_base = |primary_key: &RelationalKey| merge.visit_base(primary_key);
            match selector {
                RelationalIndexReadSelector::Exact(key) => self.base.visit_exact_postings(
                    table,
                    index,
                    key,
                    backend_limits,
                    &mut emit_base,
                )?,
                RelationalIndexReadSelector::Prefix(prefix) => self.base.visit_prefix_postings(
                    table,
                    index,
                    prefix,
                    backend_limits,
                    &mut emit_base,
                )?,
                RelationalIndexReadSelector::Range(_) => {
                    return Err(RelationalIndexShadowError::Admission(
                        "range selectors require ordered entry traversal".to_string(),
                    ));
                }
            }
        };
        merge.finish();
        report.live_batches_visited = checked_add(
            report.live_batches_visited,
            self.overlay.batches.len(),
            "transaction index batch count",
        )?;
        report.live_entries_visited = checked_add(
            report.live_entries_visited,
            entries_visited,
            "transaction index entry count",
        )?;
        report.live_entries_matched = checked_add(
            report.live_entries_matched,
            entries_matched,
            "transaction index matched-entry count",
        )?;
        report.live_bytes_visited = checked_add(
            report.live_bytes_visited,
            bytes_visited,
            "transaction index byte count",
        )?;
        report.rows_visited = checked_add(
            report.rows_visited,
            merge.overlay_rows_emitted,
            "transaction index row count",
        )?;
        report.stopped_early |= merge.stopped_early;
        Ok(report)
    }
}

impl RelationalConstraintIndex for RelationalTransactionIndexView {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        let limits = self.read_ledger.remaining_limits()?;
        let report = self
            .visit_with_overlay(
                table,
                index,
                RelationalIndexReadSelector::Exact(key),
                limits,
                visit,
            )
            .map_err(map_constraint_read_error)?;
        self.read_ledger.record(&report)
    }
}

fn intersect_read_limits(
    requested: RelationalIndexReadLimits,
    remaining: RelationalIndexReadLimits,
) -> RelationalIndexReadLimits {
    RelationalIndexReadLimits {
        max_pages: requested.max_pages.min(remaining.max_pages),
        max_rows: requested.max_rows.min(remaining.max_rows),
        max_bytes: requested.max_bytes.min(remaining.max_bytes),
        max_file_bytes: requested.max_file_bytes.min(remaining.max_file_bytes),
        max_tree_height: requested.max_tree_height.min(remaining.max_tree_height),
    }
}

fn checked_add(
    current: usize,
    additional: usize,
    label: &str,
) -> Result<usize, RelationalIndexShadowError> {
    current
        .checked_add(additional)
        .ok_or_else(|| admission(format!("{label} overflow")))
}

fn admission(message: impl Into<String>) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Admission(message.into())
}

fn relational_read_error(error: RelationalError) -> RelationalIndexShadowError {
    match error {
        RelationalError::Admission(message) => RelationalIndexShadowError::Admission(message),
        RelationalError::Durability(message) => RelationalIndexShadowError::Durability(message),
        RelationalError::Corruption(message)
        | RelationalError::Schema(message)
        | RelationalError::Constraint(message) => RelationalIndexShadowError::Corrupt(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelationalValue;

    #[test]
    fn transaction_overlay_admission_is_cumulative_and_atomic() {
        let limits = RelationalIndexChangeCaptureLimits {
            max_entries: NonZeroUsize::new(2).unwrap(),
            max_bytes: NonZeroUsize::new(10).unwrap(),
        };
        let mut overlay = RelationalTransactionIndexOverlay::new(limits);
        overlay.append(capture(vec![change(1)], 4)).unwrap();

        let entry_error = overlay
            .append(capture(vec![change(2), change(3)], 4))
            .expect_err("cumulative entry admission must fail");
        assert!(entry_error.to_string().contains("max_entries=2"));
        assert_eq!(overlay.entry_count, 1);
        assert_eq!(overlay.encoded_bytes, 4);
        assert_eq!(overlay.batches.len(), 1);
        assert!(overlay.touches("documents", "documents_owner_idx"));
        assert!(!overlay.touches("documents", "documents_missing_idx"));

        let byte_error = overlay
            .append(capture(vec![change(2)], 7))
            .expect_err("cumulative byte admission must fail");
        assert!(byte_error.to_string().contains("max_bytes=10"));
        assert_eq!(overlay.entry_count, 1);
        assert_eq!(overlay.encoded_bytes, 4);
        assert_eq!(overlay.batches.len(), 1);

        overlay.append(capture(vec![change(2)], 6)).unwrap();
        assert_eq!(overlay.entry_count, 2);
        assert_eq!(overlay.encoded_bytes, 10);
        assert_eq!(overlay.batches.len(), 2);
    }

    #[test]
    fn transaction_overlay_delete_merge_is_independent_of_prefix_visit_order() {
        let deleted_early = RelationalKey(vec![RelationalValue::BigInt(1)]);
        let retained = RelationalKey(vec![RelationalValue::BigInt(2)]);
        let deleted_late = RelationalKey(vec![RelationalValue::BigInt(3)]);
        let mut emitted = Vec::new();
        let mut visit = |key: &RelationalKey| {
            emitted.push(key.clone());
            true
        };
        let mut merge = TransactionOverlayMerge {
            overlay: BTreeMap::from([
                (deleted_early.clone(), RelationalIndexChangeKind::Delete),
                (deleted_late.clone(), RelationalIndexChangeKind::Delete),
            ]),
            visit: &mut visit,
            overlay_rows_emitted: 0,
            stopped_early: false,
        };

        assert!(merge.visit_base(&deleted_late));
        assert!(merge.visit_base(&retained));
        assert!(merge.visit_base(&deleted_early));
        merge.finish();
        drop(merge);

        assert_eq!(emitted, vec![retained]);
    }

    fn capture(
        changes: Vec<RelationalIndexChange>,
        encoded_bytes: usize,
    ) -> RelationalIndexChangeCapture {
        RelationalIndexChangeCapture::Captured {
            changes,
            encoded_bytes,
        }
    }

    fn change(ordinal: i64) -> RelationalIndexChange {
        let primary_key = RelationalKey(vec![RelationalValue::BigInt(ordinal)]);
        RelationalIndexChange {
            table: "documents".to_string(),
            index: "documents_owner_idx".to_string(),
            index_key: RelationalKey(vec![RelationalValue::Text("owner".to_string())]),
            primary_key,
            kind: RelationalIndexChangeKind::Insert,
        }
    }
}
