// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use hawdb_core::Value;
use hawdb_storage::relational_index_view::RelationalIndexReadView;
use hawdb_storage::{
    RelationalIndexChangeCapture, RelationalIndexShadowConfig, RelationalIndexShadowReader,
    RelationalIndexShadowWriter, RelationalTransaction,
};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) type Oracle = BTreeMap<i64, (i64, i64)>;
pub(super) const TABLE: &str = "docs";
pub(super) const INDEX: &str = "docs_bucket_rank";

pub(super) fn key(values: &[i64]) -> RelationalKey {
    RelationalKey(
        values
            .iter()
            .copied()
            .map(RelationalValue::BigInt)
            .collect(),
    )
}

pub(super) struct Fixture {
    directory: PathBuf,
    pub state: RelationalState,
    pub oracle: Oracle,
    pub reader: Reader,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-index-runtime-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut state = RelationalState::default();
        apply(
            &mut state,
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, bucket BIGINT, rank BIGINT)",
            &[],
        );
        apply(
            &mut state,
            "CREATE INDEX docs_bucket_rank ON docs (bucket, rank)",
            &[],
        );
        let oracle: Oracle = (0..24).map(|id| (id, (id % 3, id * 7 % 5))).collect();
        for (&id, &(bucket, rank)) in &oracle {
            apply(
                &mut state,
                "INSERT INTO docs (id, bucket, rank) VALUES ($1, $2, $3)",
                &[Value::Int(id), Value::Int(bucket), Value::Int(rank)],
            );
        }
        let config = RelationalIndexShadowConfig::default();
        RelationalIndexShadowWriter::new(config)
            .publish(&directory, &state, 1, 40, None)
            .unwrap();
        let view = RelationalIndexReadView::from_base(
            RelationalIndexShadowReader::open(&directory, 1, 40, config).unwrap(),
        );
        Self {
            directory,
            state,
            oracle,
            reader: Reader::pinned(Arc::new(view)),
        }
    }

    pub fn transaction(&self) -> RelationalTransactionIndexView {
        RelationalTransactionIndexView::new(
            Arc::clone(self.reader.view()),
            Default::default(),
            Default::default(),
        )
    }

    pub fn recovered(&self) -> (Reader, RelationalState, Oracle) {
        use hawdb_storage::{
            RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig,
            RelationalIndexRecoveryReader, RelationalRecoveryFence,
            RelationalRecoverySourceIdentity,
        };
        let config = RelationalIndexRecoveryConfig::default();
        let mut builder =
            RelationalIndexRecoveryBuilder::new(&self.directory, 1, 40, config).unwrap();
        let mut state = self.state.clone();
        let mut oracle = self.oracle.clone();
        for (epoch, id, next) in [(41, 1, Some((2, 9))), (42, 4, None), (43, 25, Some((1, 6)))] {
            builder
                .record(epoch, replace(&mut state, &mut oracle, id, next))
                .unwrap();
        }
        // This fixture binds a synthetic source identity; it does not simulate WAL replay.
        let source = RelationalRecoverySourceIdentity {
            wal_generation: 1,
            start_lsn: 40,
            end_lsn: 43,
            record_sequence_sha256: "42".repeat(32).parse().unwrap(),
        };
        builder.finish_with_recovery_source(43, source).unwrap();
        let recovered = RelationalIndexRecoveryReader::open_latest(
            &self.directory,
            RelationalRecoveryFence::new(43, source),
            Default::default(),
            config,
        )
        .unwrap();
        (
            Reader::pinned(Arc::new(RelationalIndexReadView::from_recovered(recovered))),
            state,
            oracle,
        )
    }

    pub fn mode<'a>(
        &'a self,
        mode: usize,
        transaction: &'a RelationalTransactionIndexView,
    ) -> Mode<'a> {
        match mode {
            0 => Mode::Materialized,
            1 => Mode::Shadow(&self.reader),
            2 => Mode::DemandPaged(&self.reader),
            3 => Mode::Authoritative(&self.reader),
            4 => Mode::TransactionWorkspace,
            5 => Mode::AuthoritativeTransaction(transaction),
            _ => unreachable!(),
        }
    }

    pub fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

fn apply(state: &mut RelationalState, sql: &str, parameters: &[Value]) {
    let transaction = crate::compile_relational_statement_sql(sql, parameters, state).unwrap();
    *state = state
        .stage_transaction(transaction, Default::default(), Default::default())
        .unwrap();
}

pub(super) fn replace(
    state: &mut RelationalState,
    oracle: &mut Oracle,
    id: i64,
    next: Option<(i64, i64)>,
) -> RelationalIndexChangeCapture {
    let mut transaction = crate::compile_relational_statement_sql(
        "DELETE FROM docs WHERE id = $1",
        &[Value::Int(id)],
        state,
    )
    .unwrap();
    oracle.remove(&id);
    if let Some((bucket, rank)) = next {
        transaction.writes.extend(
            crate::compile_relational_statement_sql(
                "INSERT INTO docs (id, bucket, rank) VALUES ($1, $2, $3)",
                &[Value::Int(id), Value::Int(bucket), Value::Int(rank)],
                state,
            )
            .unwrap()
            .writes,
        );
        oracle.insert(id, (bucket, rank));
    }
    let (next_state, capture) = state
        .stage_transaction_with_index_changes(
            RelationalTransaction {
                writes: transaction.writes,
            },
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
    *state = next_state;
    capture
}

pub(super) fn expected(oracle: &Oracle, scan: &RelationalIndexRangeScan) -> Vec<Entry> {
    let mut rows = oracle
        .iter()
        .map(|(&id, &(bucket, rank))| (key(&[bucket, rank]), key(&[id])))
        .filter(|(index, _)| {
            index.0.starts_with(&scan.prefix.0)
                && scan
                    .exclusive_bound
                    .as_ref()
                    .is_none_or(|bound| match scan.direction {
                        RelationalIndexScanDirection::Forward => index > bound,
                        RelationalIndexScanDirection::Backward => index < bound,
                    })
        })
        .collect::<Vec<_>>();
    rows.sort();
    if scan.direction == RelationalIndexScanDirection::Backward {
        // Backward scans reverse index keys, not each key's posting list.
        rows.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    }
    rows
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Outcome {
    Success,
    Unavailable,
    Admission,
    Missing,
    Corrupt,
    Durability,
    Stale,
}

#[derive(Debug)]
enum Source {
    View(Arc<RelationalIndexReadView>),
    Script { outcome: Outcome, emit: bool },
}

#[derive(Debug)]
pub(super) struct Reader {
    source: Source,
    pub limits: RefCell<Vec<RelationalIndexReadLimits>>,
    pub batches: RefCell<Vec<Vec<RelationalKey>>>,
}

impl Reader {
    pub fn pinned(view: Arc<RelationalIndexReadView>) -> Self {
        Self::new(Source::View(view))
    }
    pub fn script(outcome: Outcome, emit: bool) -> Self {
        Self::new(Source::Script { outcome, emit })
    }
    fn new(source: Source) -> Self {
        Self {
            source,
            limits: RefCell::new(Vec::new()),
            batches: RefCell::new(Vec::new()),
        }
    }
    pub fn view(&self) -> &Arc<RelationalIndexReadView> {
        match &self.source {
            Source::View(view) => view,
            Source::Script { .. } => panic!("not a pinned view"),
        }
    }
    fn attempt(
        &self,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        let Source::Script { outcome, emit } = self.source else {
            panic!("not a scripted reader")
        };
        assert!(!matches!(outcome, Outcome::Unavailable) || !emit);
        let stopped = emit && !visit(&key(&[1, 2]), &key(&[4]));
        Some(match outcome {
            Outcome::Success => {
                let mut report = report();
                report.rows_visited = usize::from(emit);
                report.stopped_early = stopped;
                Ok(report)
            }
            Outcome::Unavailable => return None,
            Outcome::Admission => Err(RelationalIndexShadowError::Admission(
                "injected admission".into(),
            )),
            Outcome::Missing => Err(RelationalIndexShadowError::MissingIndex {
                table: TABLE.into(),
                index: INDEX.into(),
            }),
            Outcome::Corrupt => Err(RelationalIndexShadowError::Corrupt(
                "injected corruption".into(),
            )),
            Outcome::Durability => Err(RelationalIndexShadowError::Durability(
                "injected durability".into(),
            )),
            Outcome::Stale => Err(RelationalIndexShadowError::StaleGeneration {
                expected_previous: Some(1),
                actual_previous: Some(2),
            }),
        })
    }
}

impl RelationalIndexStoreReader for Reader {
    fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        self.view().fresh_probe_statistics(table, index, prefix_len)
    }
    fn visit_relational_index_read_view_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.limits.borrow_mut().push(limits);
        match &self.source {
            Source::View(view) => {
                Some(view.visit_prefix_entries(table, index, prefix, limits, visit))
            }
            Source::Script { .. } => self.attempt(visit),
        }
    }
    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.limits.borrow_mut().push(limits);
        self.batches.borrow_mut().push(prefixes.to_vec());
        match &self.source {
            Source::View(view) => {
                Some(view.visit_prefix_entries_many(table, index, prefixes, limits, visit))
            }
            Source::Script { .. } => self.attempt(visit),
        }
    }
    fn visit_relational_index_read_view_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        self.limits.borrow_mut().push(limits);
        match &self.source {
            Source::View(view) => Some(view.visit_range_entries(table, index, scan, limits, visit)),
            Source::Script { .. } => self.attempt(visit),
        }
    }
}

pub(super) fn report() -> RelationalIndexReadViewReport {
    RelationalIndexReadViewReport {
        base_generation: 1,
        delta_generation: None,
        base_commit_epoch: 40,
        visible_commit_epoch: 40,
        root_set_digest: "fixture-root".into(),
        backend: RelationalIndexReadViewBackendReport::Base(RelationalIndexReadReport::default()),
        live_batches_visited: 0,
        live_entries_visited: 0,
        live_entries_matched: 0,
        live_bytes_visited: 0,
        rows_visited: 0,
        stopped_early: false,
    }
}
