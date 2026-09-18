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
use hawdb_executor::{BlockingOperatorMemoryReport, QueryRows};
use hawdb_optimizer::{RelationalJoinEnumerationConfig, RelationalOperatorKind};
use hawdb_storage::RelationalHydrationBudget;
use std::collections::{BTreeMap, BTreeSet};

type ExpectedNode = (
    &'static str,
    &'static str,
    Option<usize>,
    String,
    String,
    bool,
    Option<usize>,
);

pub(super) const QUERIES: [&str; 8] = [
    "SELECT x FROM docs",
    "SELECT x FROM docs LIMIT 2 OFFSET 1",
    "SELECT x FROM docs WHERE x > 1",
    "SELECT DISTINCT x FROM docs",
    "SELECT x FROM docs ORDER BY x DESC",
    "SELECT COUNT(DISTINCT x) FROM docs",
    "SELECT x, COUNT(*) FROM docs GROUP BY x HAVING COUNT(*) > 1",
    "SELECT d.x FROM docs d JOIN other o ON d.x = o.x",
];

pub(super) fn select(sql: &str) -> SelectStatement {
    match hawdb_sql::prepare_postgres_sql(sql).unwrap().statement {
        hawdb_sql::SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    }
}

pub(super) fn limits() -> RelationalQueryLimits {
    RelationalQueryLimits {
        max_output_rows: 64,
        max_output_payload_bytes: 1 << 20,
        max_intermediate_rows: 32,
        max_candidate_work: 64,
        hydration: Default::default(),
        index_read: Default::default(),
        row_read: Default::default(),
    }
}

pub(super) fn number(value: Option<usize>, estimated: bool) -> Value {
    match value {
        None => Value::Null,
        Some(value) => Value::Int(
            i64::try_from(if estimated { value.max(1) } else { value }).unwrap_or(i64::MAX),
        ),
    }
}

pub(super) fn text(value: impl ToString) -> Value {
    Value::String(value.to_string())
}

pub(super) struct Case {
    pub shape: usize,
    pub n: usize,
    pub kind: usize,
    pub profile: bool,
    pub estimated: usize,
    pub actual: Option<usize>,
    pub present: bool,
    pub spilled: bool,
}

impl Case {
    pub fn descriptor(&self) -> RelationalAccessPathDescriptor {
        RelationalAccessPathDescriptor {
            kind: [
                RelationalAccessPathKind::FullScan,
                RelationalAccessPathKind::PrimaryKey,
                RelationalAccessPathKind::Index,
            ][self.kind],
            name: "idx_x".into(),
            index_columns: vec!["x".into()],
            access_columns: BTreeSet::from(["x".into()]),
            equality_prefix_len: self.n % 3,
            order_prefix_len: 0,
            exclusive_range: self.present,
            reverse_order: self.spilled,
            unique_point: self.kind == 1,
            covering: self.present,
            requires_row_fetch: !self.present,
            estimated_rows: self.estimated,
        }
    }

    pub fn profile(&self, id: usize, table: &str) -> RelationalOperatorCardinalityProfile {
        RelationalOperatorCardinalityProfile {
            operator_id: RelationalOperatorId::from_plan_index(id),
            operator: if id == 0 {
                [
                    RelationalOperatorKind::TableFullScan,
                    RelationalOperatorKind::TablePointGet,
                    RelationalOperatorKind::IndexRangeScan,
                ][self.kind]
            } else {
                RelationalOperatorKind::HashJoin
            },
            table: table.into(),
            access_path: self.descriptor(),
            estimated_rows: self.estimated,
            actual_rows: self.actual,
            fully_consumed: self.present,
        }
    }

    pub fn input(&self) -> RelationalQueryOutput {
        let n = self.n;
        let optional = |offset: u64| self.present.then_some(n as u64 + offset);
        let mut planning = RelationalJoinPlanningOutcome::explicit_syntax_order(
            vec!["docs".into()],
            RelationalJoinEnumerationConfig::default(),
        );
        planning.budget.max_groups = 19;
        planning.budget.max_expressions = 23;
        let profiles = if self.shape == 7 {
            vec![self.profile(0, "docs"), self.profile(1, "other")]
        } else if self.profile {
            vec![self.profile(0, "docs")]
        } else {
            Vec::new()
        };
        let index = |table: &str| RelationalIndexExecutionEvidence {
            table: table.into(),
            index: if self.kind == 1 {
                hawdb_storage::RELATIONAL_PRIMARY_INDEX_NAME.into()
            } else {
                "idx_x".into()
            },
            lookups: n,
            demand_paged_lookups: 1,
            canonical_fallback_lookups: 1,
            fallback_reasons: BTreeSet::from(["budget", "missing"]),
            base_generation: optional(1),
            delta_generation: optional(2),
            base_commit_epoch: optional(3),
            visible_commit_epoch: optional(4),
            root_set_digest: self.present.then(|| "index-root".into()),
            logical_pages: n + 1,
            logical_bytes: n + 2,
            file_pages: n + 3,
            file_bytes: n + 4,
            cache_hits: n + 5,
            cache_misses: n + 6,
            cache_admission_rejections: n + 7,
            delta_pages_skipped: n + 8,
            delta_entries_visited: n + 9,
            live_batches_visited: n + 10,
            live_entries_visited: n + 11,
            live_entries_matched: n + 12,
            live_bytes_visited: n + 13,
            rows_visited: n + 14,
            range_lookups: n + 15,
            exclusive_seek_lookups: n + 16,
            backward_lookups: n + 17,
            early_stop_lookups: n + 18,
            ..Default::default()
        };
        RelationalQueryOutput {
            rows: (0..n % 3)
                .map(|i| Row::from([("x".into(), Value::Int(i as i64))]))
                .collect::<Vec<_>>()
                .into(),
            stage_timings: RelationalSqlStageTimings {
                parse_nanos: n as u64,
                bind_nanos: n as u64 + 1,
                plan_nanos: n as u64 + 2,
                execute_nanos: n as u64 + 3,
            },
            join_planning: planning,
            operator_cardinality_profiles: profiles,
            intermediate_rows: n + 1,
            hydration: RelationalHydrationBudget {
                hydrated_rows: n + 2,
                compressed_bytes: n + 3,
                decompressed_bytes: n + 4,
                ..Default::default()
            },
            access_path: self.descriptor(),
            join_access_paths: if self.shape == 7 {
                vec![self.descriptor()]
            } else {
                Vec::new()
            },
            index_execution_evidence: vec![index("docs"), index("other")],
            row_execution_evidence: RelationalRowExecutionEvidence {
                runtime_path: "authoritative",
                base_generation: optional(1),
                delta_generation: optional(2),
                base_commit_epoch: optional(3),
                visible_commit_epoch: optional(4),
                root_set_digest: self.present.then(|| "row-root".into()),
                descriptor_reads: n + 1,
                logical_pages: n + 2,
                logical_bytes: n + 3,
                file_pages: n + 4,
                file_bytes: n + 5,
                cache_hits: n + 6,
                cache_misses: n + 7,
                cache_admission_rejections: n + 8,
                overlay_entries: n + 9,
                overlay_resident_bytes: n + 10,
                rows_visited: n + 11,
                borrowed_rows_visited: n + 12,
                owned_rows_visited: n + 13,
                index_covered_rows: n + 14,
                projection_generation: self.present.then(|| "projection".into()),
                projection_source_watermark: optional(5),
                projection_version: optional(6),
                projection_publication_commit_epoch: optional(7),
            },
            blocking_operator_memory_reports: [
                "TopNExec",
                "DistinctExec",
                "RelationalAggregateExec",
                "SortExec",
            ]
            .into_iter()
            .map(|operator| BlockingOperatorMemoryReport {
                operator: operator.into(),
                budget_bytes: 100 + n,
                peak_tracked_bytes: 50 + n,
                input_rows: n + 2,
                max_spill_bytes: 1000,
                max_spill_runs: 4,
                spilled_bytes: 70 + n as u64,
                spill_run_count: usize::from(self.spilled),
                spilled_rows: n + 3,
            })
            .collect(),
        }
    }

    // Ordered key-value tables are an independent oracle for the evidence wire text.
    pub fn access_text(&self) -> String {
        let n = self.n;
        let optional = |offset: usize| {
            if self.present {
                (n + offset).to_string()
            } else {
                "none".into()
            }
        };
        let mut entries = vec![
            ("equality_prefix", (n % 3).to_string()),
            ("order_prefix", "0".into()),
            ("exclusive_seek", self.present.to_string()),
            (
                "direction",
                if self.spilled { "backward" } else { "forward" }.into(),
            ),
            ("unique_point", (self.kind == 1).to_string()),
            ("covering", self.present.to_string()),
            ("row_fetch", (!self.present).to_string()),
        ];
        if self.kind != 0 {
            entries.extend([
                ("runtime_path", "mixed".into()),
                ("lookups", n.to_string()),
                ("range_lookups", (n + 15).to_string()),
                ("exclusive_seek_lookups", (n + 16).to_string()),
                ("backward_lookups", (n + 17).to_string()),
                ("early_stop_lookups", (n + 18).to_string()),
                ("demand_paged", "1".into()),
                ("authoritative", "0".into()),
                ("transaction_workspace", "0".into()),
                ("canonical_fallback", "1".into()),
                ("fallback_reasons", "budget|missing".into()),
                ("base_generation", optional(1)),
                ("delta_generation", optional(2)),
                ("base_epoch", optional(3)),
                ("visible_epoch", optional(4)),
                (
                    "root_set_digest",
                    if self.present { "index-root" } else { "none" }.into(),
                ),
            ]);
            for (key, offset) in [
                ("logical_pages", 1),
                ("logical_bytes", 2),
                ("physical_pages", 3),
                ("physical_bytes", 4),
                ("cache_hits", 5),
                ("cache_misses", 6),
                ("cache_admission_rejections", 7),
                ("delta_pages_skipped", 8),
                ("delta_entries", 9),
                ("live_batches", 10),
                ("live_entries", 11),
                ("live_matches", 12),
                ("live_bytes", 13),
                ("index_rows", 14),
            ] {
                entries.push((key, (n + offset).to_string()));
            }
        }
        entries.extend([
            ("row_runtime_path", "authoritative".into()),
            (
                "row_projection_generation",
                if self.present { "projection" } else { "none" }.into(),
            ),
            ("row_projection_source_watermark", optional(5)),
            ("row_projection_version", optional(6)),
            ("row_projection_publication_epoch", optional(7)),
            ("row_base_generation", optional(1)),
            ("row_delta_generation", optional(2)),
            ("row_base_epoch", optional(3)),
            ("row_visible_epoch", optional(4)),
            (
                "row_root_set_digest",
                if self.present { "row-root" } else { "none" }.into(),
            ),
        ]);
        for (key, offset) in [
            ("row_descriptor_reads", 1),
            ("row_logical_pages", 2),
            ("row_logical_bytes", 3),
            ("row_physical_pages", 4),
            ("row_physical_bytes", 5),
            ("row_cache_hits", 6),
            ("row_cache_misses", 7),
            ("row_cache_admission_rejections", 8),
            ("row_overlay_entries", 9),
            ("row_overlay_bytes", 10),
            ("row_rows", 11),
            ("row_borrowed_rows", 12),
            ("row_owned_rows", 13),
            ("row_index_covered_rows", 14),
        ] {
            entries.push((key, (n + offset).to_string()));
        }
        entries
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn expected_rows(&self, analyze: bool, covered: bool) -> QueryRows {
        // These node lists are explicit expected shapes, not production AST walkers.
        let mut nodes: Vec<ExpectedNode> = Vec::new();
        let empty = String::new;
        match self.shape {
            1 => nodes.push((
                "LimitExec",
                "logical_limit",
                Some(2),
                empty(),
                "implementation=fused, offset=1, count=2".into(),
                false,
                None,
            )),
            3 => nodes.push((
                "DistinctExec",
                "blocking_distinct",
                Some(self.estimated),
                empty(),
                "scope=statement".into(),
                true,
                None,
            )),
            4 => nodes.push((
                "TopNExec",
                "blocking_top_n",
                None,
                empty(),
                "order_by=x DESC, offset=0".into(),
                true,
                None,
            )),
            5 => {
                nodes.push((
                    "RelationalAggregateExec",
                    "blocking_aggregate",
                    None,
                    empty(),
                    "group_by=[], aggregates=[count(x)]".into(),
                    true,
                    None,
                ));
                nodes.push((
                    "DistinctExec",
                    "blocking_distinct",
                    Some(self.estimated),
                    empty(),
                    "scope=aggregate_argument".into(),
                    true,
                    None,
                ));
            }
            6 => {
                nodes.push((
                    "SelectionExec",
                    "logical_having",
                    None,
                    empty(),
                    "implementation=fused, phase=having, predicate=count(*) > 1".into(),
                    false,
                    None,
                ));
                nodes.push((
                    "RelationalAggregateExec",
                    "blocking_aggregate",
                    Some(self.estimated.min(32)),
                    empty(),
                    "group_by=[x], aggregates=[count(*)]".into(),
                    true,
                    None,
                ));
                nodes.push((
                    "SortExec",
                    "blocking_group_sort",
                    Some(self.estimated),
                    empty(),
                    "group_keys=[x]".into(),
                    true,
                    None,
                ));
            }
            _ => {}
        }
        nodes.push((
            "ProjectionExec",
            "logical_projection",
            Some(self.estimated),
            empty(),
            format!(
                "implementation=fused, columns={}",
                if self.shape == 6 { 2 } else { 1 }
            ),
            false,
            None,
        ));
        if self.shape == 2 && !covered {
            nodes.push((
                "SelectionExec",
                "logical_selection",
                Some(self.estimated),
                empty(),
                "implementation=fused, residual_predicate=x > 1".into(),
                false,
                None,
            ));
        }
        let object = |table: &str| match self.kind {
            0 => format!("table:{table}"),
            1 => format!("table:{table}, primary_key"),
            _ => format!("table:{table}, index:idx_x"),
        };
        if self.shape == 7 {
            let n = self.n;
            let planning = format!("join_order=syntax, planning_strategy=syntax_order, planning_status=selected, planning_reason=explicit_syntax_order, memo_groups=unavailable, memo_expressions=unavailable, max_groups=19, max_expressions=23, selected_order=[docs], attempts=[0:syntax_order:selected:explicit_syntax_order:fallback_class=none:memo_groups=unavailable:memo_expressions=unavailable:cost=unavailable], parse_nanos={n}, bind_nanos={}, plan_nanos={}, execute_nanos={}, plan_cost=unavailable", n + 1, n + 2, n + 3);
            nodes.push((
                "HashJoinExec",
                "2",
                Some(self.estimated),
                object("other"),
                format!("{planning}, {}", self.access_text()),
                false,
                Some(2),
            ));
        }
        let has_profile = self.profile || self.shape == 7;
        nodes.push((
            [
                "TableFullScanExec",
                "TablePointGetExec",
                "IndexRangeScanExec",
            ][self.kind],
            "1",
            has_profile.then_some(self.estimated),
            object("docs"),
            self.access_text(),
            false,
            has_profile.then_some(1),
        ));
        nodes.into_iter().enumerate().map(|(index, (operator, id, estimated, object, info, report, profile))| {
            let prefix = if index == 0 { String::new() } else { format!("{}└─", "  ".repeat(index - 1)) };
            let mut row = BTreeMap::from([
                ("id".into(), text(format!("{prefix}{operator}_{id}"))),
                ("estRows".into(), number(estimated, true)), ("task".into(), text("root")),
                ("access object".into(), text(object)), ("operator info".into(), text(info)),
            ]);
            if analyze {
                row.insert("actRows".into(), if profile.is_some() { number(self.actual, false) } else { Value::Null });
                let n = self.n;
                let execution = if let Some(id) = profile {
                    text(format!("operator_id={id}, fully_consumed={}", self.present))
                } else if index == 0 {
                    text(format!("statement_output_rows={}, intermediate_rows={}, hydrated_rows={}, compressed_bytes={}, decompressed_bytes={}", n % 3, n + 1, n + 2, n + 3, n + 4))
                } else if report { text(format!("input_rows={}", n + 2)) } else { Value::Null };
                row.insert("execution info".into(), execution);
                row.insert("memory".into(), if report { text(format!("peak={}/budget={}", 50 + n, 100 + n)) } else { Value::Null });
                row.insert("disk".into(), if report && self.spilled { text(format!("runs=1, rows={}, bytes={}", n + 3, 70 + n)) } else { Value::Null });
            }
            row
        }).collect::<Vec<_>>().into()
    }
}
