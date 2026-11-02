// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::Value;
use serde::{Deserialize, Serialize};

pub const PROTOCOL: &str = "hawdb-mvcc-ranges-v1";
pub const FAMILIES: &[&str] = &[
    "primary_prefix",
    "primary_interval",
    "secondary_interval",
    "secondary_rekey",
    "secondary_primary",
    "reverse_rekey",
    "keyless_table",
    "stale_range",
    "discovery_escalation",
    "escalation_stale",
];
pub const COLUMNS: &str = "w, k, tail, band, tag, value";
pub const READ: &str = "SELECT w, k, tail, band, tag, value FROM records";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub w: i64,
    pub k: i64,
    pub tail: i64,
    pub band: i64,
    pub tag: Option<i64>,
    pub value: i64,
}
impl Record {
    pub fn key(&self) -> [i64; 3] {
        [self.w, self.k, self.tail]
    }
    pub fn values(&self) -> Vec<Value> {
        vec![
            Value::Int(self.w),
            Value::Int(self.k),
            Value::Int(self.tail),
            Value::Int(self.band),
            self.tag.map_or(Value::Null, Value::Int),
            Value::Int(self.value),
        ]
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Path {
    Primary,
    Secondary,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub path: Path,
    pub tenant: i64,
    pub lower: Option<i64>,
    pub upper: Option<i64>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub exclusive: bool,
}
impl Selection {
    fn empty(&self) -> bool {
        self.lower.zip(self.upper).is_some_and(|(lower, upper)| {
            lower == upper && !(self.lower_inclusive && self.upper_inclusive)
        })
    }
    pub fn matches(&self, row: &Record) -> bool {
        let key = match self.path {
            Path::Primary => row.k,
            Path::Secondary => row.band,
        };
        row.w == self.tenant
            && self
                .lower
                .is_none_or(|v| key > v || (self.lower_inclusive && key == v))
            && self
                .upper
                .is_none_or(|v| key < v || (self.upper_inclusive && key == v))
    }
    pub fn rows(&self, rows: &[Record]) -> Vec<Record> {
        rows.iter()
            .filter(|row| self.matches(row))
            .cloned()
            .collect()
    }
    pub fn sql(&self) -> (String, Vec<Value>) {
        let column = match self.path {
            Path::Primary => "k",
            Path::Secondary => "band",
        };
        let mut sql = format!("SELECT {COLUMNS} FROM records WHERE w = $1");
        let mut params = vec![Value::Int(self.tenant)];
        for (bound, op) in [
            (self.lower, if self.lower_inclusive { ">=" } else { ">" }),
            (self.upper, if self.upper_inclusive { "<=" } else { "<" }),
        ] {
            if let Some(bound) = bound {
                params.push(Value::Int(bound));
                sql.push_str(&format!(" AND {column} {op} ${}", params.len()));
            }
        }
        sql.push_str(if self.exclusive {
            " FOR UPDATE"
        } else {
            " FOR SHARE"
        });
        (sql, params)
    }
    fn overlaps(&self, other: &Self) -> bool {
        if self.tenant != other.tenant || self.empty() || other.empty() {
            return false;
        }
        let before = |upper: Option<i64>, upper_inc: bool, lower: Option<i64>, lower_inc: bool| {
            upper
                .zip(lower)
                .is_some_and(|(u, l)| u < l || (u == l && !(upper_inc && lower_inc)))
        };
        !before(
            self.upper,
            self.upper_inclusive,
            other.lower,
            other.lower_inclusive,
        ) && !before(
            other.upper,
            other.upper_inclusive,
            self.lower,
            self.lower_inclusive,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Insert {
        row: Record,
    },
    Update {
        key: [i64; 3],
        band: i64,
        value: i64,
    },
    Read {
        selection: Selection,
    },
}
impl Action {
    pub fn sql(&self) -> (String, Vec<Value>) {
        match self {
            Self::Insert { row } => (
                "INSERT INTO records VALUES ($1, $2, $3, $4, $5, $6)".into(),
                row.values(),
            ),
            Self::Update { key, band, value } => (
                "UPDATE records SET band = $1, value = $2 WHERE w = $3 AND k = $4 AND tail = $5"
                    .into(),
                vec![*band, *value, key[0], key[1], key[2]]
                    .into_iter()
                    .map(Value::Int)
                    .collect(),
            ),
            Self::Read { selection } => selection.sql(),
        }
    }
    pub fn apply(&self, rows: &mut Vec<Record>) {
        match self {
            Self::Insert { row } => rows.push(row.clone()),
            Self::Update { key, band, value } => {
                for row in rows {
                    if row.key() == *key {
                        row.band = *band;
                        row.value = *value;
                    }
                }
            }
            Self::Read { .. } => {}
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub protocol: String,
    pub seed: u64,
    pub family: String,
    pub initial: Vec<Record>,
    pub holder: Selection,
    pub contender: Action,
    pub rollback: bool,
    pub nullable_suffix: bool,
}
impl Case {
    pub fn keyless(&self) -> bool {
        self.family == "keyless_table"
    }
    pub fn reverse(&self) -> bool {
        self.family == "reverse_rekey"
    }
    pub fn stale(&self) -> bool {
        matches!(self.family.as_str(), "stale_range" | "escalation_stale")
    }
    pub fn expected_conflict(&self) -> bool {
        // Nullable secondary stamp summaries deliberately validate the table.
        // This conservative snapshot rule is distinct from live lock overlap.
        self.conflicts()
            || (self.stale()
                && !self.holder.empty()
                && self.holder.path == Path::Secondary
                && self.nullable_suffix)
    }
    pub fn table_guard(&self) -> bool {
        // The secondary interval itself occupies one entry before discovered
        // primary identities are granted from the same per-table budget.
        self.keyless()
            || (self.holder.path == Path::Secondary
                && self.holder.rows(&self.initial).len().saturating_add(1) > 64)
    }
    /// Derive conflicts from logical intervals and both mutation images. The
    /// model never asks HawDB which locks it chose or reads its lock encoding.
    pub fn conflicts(&self) -> bool {
        if !self.keyless() && self.holder.empty() {
            return false;
        }
        match &self.contender {
            Action::Read { selection } => {
                if !self.keyless() && selection.empty() {
                    return false;
                }
                if !self.holder.exclusive && !selection.exclusive {
                    return false;
                }
                self.table_guard()
                    || (self.holder.path == selection.path && self.holder.overlaps(selection))
                    || self
                        .initial
                        .iter()
                        .any(|row| self.holder.matches(row) && selection.matches(row))
            }
            Action::Insert { row } => self.table_guard() || self.holder.matches(row),
            Action::Update { key, band, .. } => {
                self.table_guard()
                    || self
                        .initial
                        .iter()
                        .filter(|row| row.key() == *key)
                        .any(|old| {
                            let mut new = old.clone();
                            new.band = *band;
                            self.holder.matches(old) || self.holder.matches(&new)
                        })
            }
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.protocol != PROTOCOL || !FAMILIES.contains(&self.family.as_str()) {
            return Err("unsupported range replay protocol/family".into());
        }
        let bounded = |n: i64| (-1_000_000..=1_000_000).contains(&n);
        let valid_row = |row: &Record| {
            row.key()
                .into_iter()
                .chain([row.band, row.value])
                .chain(row.tag)
                .all(bounded)
        };
        let valid_selection = |s: &Selection| {
            bounded(s.tenant)
                && s.lower.into_iter().chain(s.upper).all(bounded)
                && s.lower.zip(s.upper).is_none_or(|(l, u)| l <= u)
        };
        if self.holder.path == Path::Secondary
            && self.holder.lower.is_none()
            && self.holder.upper.is_none()
        {
            return Err(
                "a secondary selection needs a band bound to distinguish it from a primary prefix"
                    .into(),
            );
        }
        if self.initial.len() > 96
            || self
                .initial
                .iter()
                .any(|r| !valid_row(r) || (!self.nullable_suffix && r.tag.is_none()))
            || !valid_selection(&self.holder)
        {
            return Err("range case exceeds bounded model".into());
        }
        if !self.keyless() {
            let keys = self
                .initial
                .iter()
                .map(Record::key)
                .collect::<std::collections::BTreeSet<_>>();
            if keys.len() != self.initial.len() {
                return Err("duplicate primary keys".into());
            }
        }
        match &self.contender {
            Action::Insert { row }
                if !valid_row(row)
                    || (!self.nullable_suffix && row.tag.is_none())
                    || (!self.keyless()
                        && self.initial.iter().any(|old| old.key() == row.key())) =>
            {
                return Err("invalid or duplicate insert".into())
            }
            Action::Update { key, band, value }
                if !key.iter().copied().chain([*band, *value]).all(bounded)
                    || !self.initial.iter().any(|r| r.key() == *key) =>
            {
                return Err("update must name a bounded existing key".into())
            }
            Action::Read { selection } if !valid_selection(selection) => {
                return Err("invalid read interval".into())
            }
            _ => {}
        }
        if (self.reverse() || self.stale()) && !matches!(self.contender, Action::Update { .. }) {
            return Err("reverse/stale families require an update".into());
        }
        if self.family == "escalation_stale"
            && (self.keyless() || self.holder.path != Path::Primary || !self.conflicts())
        {
            return Err("escalation_stale must select the changed primary range".into());
        }
        Ok(())
    }
}

pub fn generate(seed: u64, index: u64) -> Case {
    let mut state = seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 16
    };
    let family = FAMILIES[(index % FAMILIES.len() as u64) as usize];
    let tenant = (next() % 17) as i64;
    let lower = (next() % 21) as i64 - 10;
    let upper = lower + 2 + (next() % 4) as i64;
    let mut holder = Selection {
        path: if matches!(
            family,
            "primary_prefix" | "primary_interval" | "escalation_stale"
        ) {
            Path::Primary
        } else {
            Path::Secondary
        },
        tenant,
        lower: Some(lower),
        upper: Some(upper),
        lower_inclusive: next().is_multiple_of(2),
        upper_inclusive: next().is_multiple_of(2),
        exclusive: next().is_multiple_of(2),
    };
    if matches!(family, "primary_prefix" | "discovery_escalation") {
        holder.lower = None;
        holder.upper = None;
    }
    let count = if family == "discovery_escalation" {
        63 + next() % 5
    } else {
        6
    };
    let mut initial = (0..count)
        .map(|i| Record {
            w: tenant,
            k: lower + i as i64,
            tail: i as i64,
            band: lower + i as i64,
            tag: if i.is_multiple_of(2) {
                None
            } else {
                Some(i as i64)
            },
            value: i as i64,
        })
        .collect::<Vec<_>>();
    let candidate = (next() % 6) as usize;
    let new_band = lower - 1 + (next() % (upper - lower + 3) as u64) as i64;
    let mut contender = match family {
        "secondary_rekey" | "reverse_rekey" | "stale_range" | "escalation_stale" => {
            Action::Update {
                key: initial[candidate].key(),
                band: new_band,
                value: 100 + candidate as i64,
            }
        }
        "secondary_primary" => Action::Read {
            selection: Selection {
                path: Path::Primary,
                tenant,
                lower: Some(initial[candidate].k),
                upper: Some(initial[candidate].k),
                lower_inclusive: true,
                upper_inclusive: true,
                exclusive: next().is_multiple_of(2),
            },
        },
        _ => Action::Insert {
            row: Record {
                w: tenant + i64::from(next().is_multiple_of(3)),
                k: new_band,
                tail: 999,
                band: new_band,
                tag: if next().is_multiple_of(2) {
                    None
                } else {
                    Some(99)
                },
                value: 77,
            },
        },
    };
    if family == "keyless_table" {
        initial.extend(initial.clone());
    }
    if family == "escalation_stale" {
        holder.lower = Some(initial[candidate].k);
        holder.upper = holder.lower;
        holder.lower_inclusive = true;
        holder.upper_inclusive = true;
    }
    if family == "discovery_escalation" {
        holder.lower = Some(lower - 1);
        holder.upper = Some(lower + count as i64);
        // A different tenant is a negative control for an ordinary prefix but
        // must conflict after the bounded discovery contract escalates.
        if let Action::Insert { row } = &mut contender {
            row.w = tenant + 1;
        }
    }
    let nullable_suffix = next().is_multiple_of(2);
    if !nullable_suffix {
        for row in &mut initial {
            row.tag.get_or_insert(0);
        }
        if let Action::Insert { row } = &mut contender {
            row.tag.get_or_insert(0);
        }
    }
    Case {
        nullable_suffix,
        protocol: PROTOCOL.into(),
        seed,
        family: family.into(),
        initial,
        holder,
        contender,
        rollback: next().is_multiple_of(2),
    }
}

pub fn reduce_with(
    mut case: Case,
    limit: usize,
    signature: &str,
    mut observe: impl FnMut(&Case) -> Option<String>,
) -> (Case, usize) {
    let mut attempts = 0;
    let mut position = 0;
    while position < case.initial.len() && attempts < limit {
        let mut candidate = case.clone();
        candidate.initial.remove(position);
        if candidate.validate().is_err() {
            position += 1;
            continue;
        }
        attempts += 1;
        if observe(&candidate).as_deref() == Some(signature) {
            case = candidate;
        } else {
            position += 1;
        }
    }
    (case, attempts)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_generates_overlap_and_disjoint_controls_with_replay_stability() {
        let mut outcomes = std::collections::BTreeSet::new();
        for seed in 0..30 {
            for index in 0..10 {
                let case = generate(seed, index);
                case.validate().unwrap();
                let decoded: Case =
                    serde_json::from_value(serde_json::to_value(&case).unwrap()).unwrap();
                assert_eq!(case.conflicts(), decoded.conflicts());
                outcomes.insert((case.family.clone(), case.conflicts()));
            }
        }
        for family in &FAMILIES[..6] {
            assert!(outcomes.contains(&(family.to_string(), true)));
            assert!(outcomes.contains(&(family.to_string(), false)));
        }
    }
    #[test]
    fn range_endpoints_and_old_new_images_are_independent_controls() {
        let mut case = generate(42, 3);
        case.holder = Selection {
            path: Path::Secondary,
            tenant: 1,
            lower: Some(10),
            upper: Some(20),
            lower_inclusive: false,
            upper_inclusive: true,
            exclusive: true,
        };
        case.initial = vec![Record {
            w: 1,
            k: 1,
            tail: 1,
            band: 10,
            tag: None,
            value: 0,
        }];
        for (band, conflict) in [(9, false), (10, false), (11, true), (20, true), (21, false)] {
            case.contender = Action::Update {
                key: [1, 1, 1],
                band,
                value: 1,
            };
            assert_eq!(case.conflicts(), conflict);
        }
        let mut empty = case.holder.clone();
        empty.upper = empty.lower;
        assert!(!empty.overlaps(&case.holder));
        case.initial[0].band = 11;
        case.contender = Action::Update {
            key: [1, 1, 1],
            band: 99,
            value: 1,
        };
        assert!(case.conflicts());
    }
    #[test]
    fn reduction_cannot_change_failure_kind_or_remove_required_update_key() {
        let case = generate(42, 3);
        let required = match &case.contender {
            Action::Update { key, .. } => *key,
            _ => unreachable!(),
        };
        let (reduced, _) = reduce_with(case, 64, "conflict", |_| Some("conflict".into()));
        assert_eq!(reduced.initial.len(), 1);
        assert_eq!(reduced.initial[0].key(), required);
        let (unchanged, _) = reduce_with(generate(42, 0), 64, "conflict", |_| {
            Some("different".into())
        });
        assert_eq!(unchanged.initial.len(), 6);
    }
}
