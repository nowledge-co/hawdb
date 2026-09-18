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

/// Exact persistent-index lookup used while staging authoritative relational
/// constraints. Implementations must represent one immutable visibility epoch,
/// emit ordered and duplicate-free primary keys, stop when the visitor returns
/// `false`, and fail closed on errors.
pub trait RelationalConstraintIndex {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        visit: &mut dyn FnMut(&RelationalKey) -> bool,
    ) -> Result<(), RelationalError>;
}

type ConstraintOverlayPostings<'a> = BTreeMap<&'a RelationalKey, RelationalIndexChangeKind>;
type ConstraintIndexOverlay<'a> = BTreeMap<&'a RelationalKey, ConstraintOverlayPostings<'a>>;
type ConstraintTableOverlay<'a> = BTreeMap<&'a str, ConstraintIndexOverlay<'a>>;

struct TransactionConstraintOverlay<'a> {
    postings: BTreeMap<&'a str, ConstraintTableOverlay<'a>>,
    deleted_keys: BTreeMap<&'a str, BTreeMap<&'a str, BTreeSet<&'a RelationalKey>>>,
}

impl<'a> TransactionConstraintOverlay<'a> {
    fn new(changes: &'a [RelationalIndexChange]) -> Result<Self, RelationalError> {
        let mut postings = BTreeMap::<_, ConstraintTableOverlay<'a>>::new();
        let mut deleted_keys = BTreeMap::<_, BTreeMap<_, BTreeSet<_>>>::new();
        for change in changes {
            if postings
                .entry(change.table.as_str())
                .or_default()
                .entry(change.index.as_str())
                .or_default()
                .entry(&change.index_key)
                .or_default()
                .insert(&change.primary_key, change.kind)
                .is_some()
            {
                return Err(RelationalError::Corruption(format!(
                    "authoritative transaction overlay contains duplicate changes for {}.{}",
                    change.table, change.index
                )));
            }
            if change.kind == RelationalIndexChangeKind::Delete {
                deleted_keys
                    .entry(change.table.as_str())
                    .or_default()
                    .entry(change.index.as_str())
                    .or_default()
                    .insert(&change.index_key);
            }
        }
        Ok(Self {
            postings,
            deleted_keys,
        })
    }

    fn postings(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
    ) -> Option<&ConstraintOverlayPostings<'a>> {
        self.postings.get(table)?.get(index)?.get(key)
    }

    fn deleted_keys(&self, table: &str, index: &str) -> Option<&BTreeSet<&'a RelationalKey>> {
        self.deleted_keys.get(table)?.get(index)
    }
}

pub(super) fn authoritative_conflict_primary_key(
    state: &RelationalState,
    table: &str,
    definition: &RelationalIndexDefinition,
    conflict_columns: &[String],
    conflict_key: &RelationalKey,
    changed_keys: &BTreeSet<RelationalKey>,
    constraint_index: &dyn RelationalConstraintIndex,
) -> Result<Option<RelationalKey>, RelationalError> {
    let positions = column_positions(
        state.table_schema(table).expect("validated table"),
        conflict_columns,
    )?;
    let mut candidates = BTreeSet::new();
    let mut previous = None;
    let mut traversal_error = None;
    constraint_index.visit_exact_primary_keys(
        table,
        &definition.name,
        conflict_key,
        &mut |primary_key| {
            if previous
                .as_ref()
                .is_some_and(|previous| previous >= primary_key)
            {
                traversal_error = Some(RelationalError::Corruption(format!(
                    "authoritative index {table}.{} returned unordered or duplicate primary keys",
                    definition.name
                )));
                return false;
            }
            previous = Some(primary_key.clone());
            match state.row(table, primary_key) {
                Some(row) if row_key(row, &positions) == *conflict_key => {
                    candidates.insert(primary_key.clone());
                }
                Some(_) | None if !changed_keys.contains(primary_key) => {
                    traversal_error = Some(RelationalError::Corruption(format!(
                        "authoritative index {table}.{} returned a stale primary key",
                        definition.name
                    )));
                    return false;
                }
                Some(_) | None => {}
            }
            candidates.len() < 2
        },
    )?;
    if let Some(error) = traversal_error {
        return Err(error);
    }
    if candidates.len() > 1 {
        return Err(RelationalError::Corruption(format!(
            "authoritative unique index {} on table {table} has multiple visible rows",
            definition.name
        )));
    }
    Ok(candidates.into_iter().next())
}

pub(super) fn validate_authoritative_constraints(
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    changes: &[RelationalIndexChange],
    constraint_index: &dyn RelationalConstraintIndex,
) -> Result<(), RelationalError> {
    let transaction_overlay = TransactionConstraintOverlay::new(changes)?;
    for (table, indexes) in &transaction_overlay.postings {
        let schema = next.schemas.get(*table).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "authoritative index change references unknown table {table}"
            ))
        })?;
        let definitions = schema.required_index_definitions();
        for (index, keys) in indexes {
            let definition = definitions
                .iter()
                .find(|definition| definition.name == **index)
                .ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "authoritative index change references unknown index {table}.{index}"
                    ))
                })?;
            if definition.role.is_unique() {
                for key in keys.keys() {
                    let postings = bounded_authoritative_postings_after_changes(
                        constraint_index,
                        table,
                        index,
                        key,
                        &transaction_overlay,
                        2,
                    )?;
                    if postings.len() > 1 {
                        return Err(RelationalError::Constraint(format!(
                            "unique index {index} on table {table} has a duplicate key"
                        )));
                    }
                    validate_authoritative_postings(
                        next,
                        table,
                        &definition.columns,
                        key,
                        &postings,
                    )?;
                }
            }
        }
    }
    validate_authoritative_foreign_keys(next, changed_keys, &transaction_overlay, constraint_index)
}

fn validate_authoritative_foreign_keys(
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    transaction_overlay: &TransactionConstraintOverlay<'_>,
    constraint_index: &dyn RelationalConstraintIndex,
) -> Result<(), RelationalError> {
    for schema in next.table_schemas() {
        let Some(segment) = next.segments.get(&schema.name) else {
            continue;
        };
        for (ordinal, foreign_key) in schema.foreign_keys.iter().enumerate() {
            let local_positions = column_positions(schema, &foreign_key.columns)?;
            let referenced_schema =
                next.schemas
                    .get(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign_key.referenced_table
                        ))
                    })?;
            let referenced_positions =
                column_positions(referenced_schema, &foreign_key.referenced_columns)?;
            validate_foreign_key_shape(
                &schema.name,
                schema,
                foreign_key,
                referenced_schema,
                &local_positions,
                &referenced_positions,
            )?;
            let referenced_definition = referenced_schema
                .unique_index_definition(&foreign_key.referenced_columns)
                .expect("validated foreign key target is unique");

            if let Some(local_changes) = changed_keys.get(&schema.name) {
                let mut target_keys = BTreeSet::new();
                for primary_key in local_changes {
                    let Some(row) = segment.rows.get(primary_key) else {
                        continue;
                    };
                    let key = row_key(row, &local_positions);
                    if key_contains_null(&key) {
                        continue;
                    }
                    target_keys.insert(key);
                }
                for key in target_keys {
                    let targets = bounded_authoritative_postings_after_changes(
                        constraint_index,
                        &foreign_key.referenced_table,
                        &referenced_definition.name,
                        &key,
                        transaction_overlay,
                        2,
                    )?;
                    if targets.len() > 1 {
                        return Err(RelationalError::Corruption(format!(
                            "authoritative foreign-key target index {}.{} has multiple visible rows",
                            foreign_key.referenced_table, referenced_definition.name
                        )));
                    }
                    if targets.is_empty() {
                        return Err(RelationalError::Constraint(format!(
                            "foreign key from {} to {} has no visible target",
                            schema.name, foreign_key.referenced_table
                        )));
                    }
                    validate_authoritative_postings(
                        next,
                        &foreign_key.referenced_table,
                        &referenced_definition.columns,
                        &key,
                        &targets,
                    )?;
                }
            }

            let local_index = relational_foreign_key_index_name(ordinal);
            let Some(removed_targets) = transaction_overlay
                .deleted_keys(&foreign_key.referenced_table, &referenced_definition.name)
            else {
                continue;
            };
            for target in removed_targets {
                if !bounded_authoritative_postings_after_changes(
                    constraint_index,
                    &foreign_key.referenced_table,
                    &referenced_definition.name,
                    target,
                    transaction_overlay,
                    1,
                )?
                .is_empty()
                {
                    continue;
                }
                if !bounded_authoritative_postings_after_changes(
                    constraint_index,
                    &schema.name,
                    &local_index,
                    target,
                    transaction_overlay,
                    1,
                )?
                .is_empty()
                {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {} to {} prevents removing a visible target",
                        schema.name, foreign_key.referenced_table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn bounded_authoritative_postings_after_changes(
    constraint_index: &dyn RelationalConstraintIndex,
    table: &str,
    index: &str,
    key: &RelationalKey,
    transaction_overlay: &TransactionConstraintOverlay<'_>,
    max_results: usize,
) -> Result<BTreeSet<RelationalKey>, RelationalError> {
    debug_assert!(max_results > 0);
    let empty_overlay = BTreeMap::new();
    let mut overlay = transaction_overlay
        .postings(table, index, key)
        .unwrap_or(&empty_overlay)
        .iter()
        .peekable();
    let mut postings = BTreeSet::new();
    let mut previous = None;
    let mut traversal_error = None;
    let mut stopped_early = false;
    constraint_index.visit_exact_primary_keys(table, index, key, &mut |primary_key| {
        if previous
            .as_ref()
            .is_some_and(|previous| previous >= primary_key)
        {
            traversal_error = Some(RelationalError::Corruption(format!(
                "authoritative index {table}.{index} returned unordered or duplicate primary keys"
            )));
            return false;
        }
        previous = Some(primary_key.clone());
        while let Some((overlay_primary_key, kind)) = overlay.peek().copied() {
            match (*overlay_primary_key).cmp(primary_key) {
                Ordering::Less => {
                    overlay.next();
                    match kind {
                        RelationalIndexChangeKind::Delete => {
                            traversal_error = Some(RelationalError::Corruption(format!(
                                "authoritative index {table}.{index} omitted a deleted posting"
                            )));
                            return false;
                        }
                        RelationalIndexChangeKind::Insert => {
                            postings.insert((*overlay_primary_key).clone());
                            if postings.len() >= max_results {
                                stopped_early = true;
                                return false;
                            }
                        }
                    }
                }
                Ordering::Equal => {
                    overlay.next();
                    return match kind {
                        RelationalIndexChangeKind::Delete => true,
                        RelationalIndexChangeKind::Insert => {
                            traversal_error = Some(RelationalError::Corruption(format!(
                                "authoritative transaction overlay inserts an existing posting for {table}.{index}"
                            )));
                            false
                        }
                    };
                }
                Ordering::Greater => break,
            }
        }
        postings.insert(primary_key.clone());
        if postings.len() >= max_results {
            stopped_early = true;
            return false;
        }
        true
    })?;
    if let Some(error) = traversal_error {
        return Err(error);
    }
    if stopped_early {
        return Ok(postings);
    }
    for (primary_key, kind) in overlay {
        match kind {
            RelationalIndexChangeKind::Delete => {
                return Err(RelationalError::Corruption(format!(
                    "authoritative index {table}.{index} omitted a deleted posting"
                )));
            }
            RelationalIndexChangeKind::Insert => {
                postings.insert((*primary_key).clone());
                if postings.len() >= max_results {
                    break;
                }
            }
        }
    }
    Ok(postings)
}

fn validate_authoritative_postings(
    state: &RelationalState,
    table: &str,
    columns: &[String],
    expected_key: &RelationalKey,
    postings: &BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let schema = state
        .schemas
        .get(table)
        .ok_or_else(|| RelationalError::Corruption(format!("unknown indexed table {table}")))?;
    let positions = column_positions(schema, columns)?;
    for primary_key in postings {
        let row = state.row(table, primary_key).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "authoritative index posting for {table} references a missing row"
            ))
        })?;
        if row_key(row, &positions) != *expected_key {
            return Err(RelationalError::Corruption(format!(
                "authoritative index posting for {table} references a row with a different key"
            )));
        }
    }
    Ok(())
}
