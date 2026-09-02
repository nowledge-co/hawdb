use super::{
    qualification_probe_error, relational_keys_digest, RelationalIndexReadViewReport,
    RelationalIndexViewQualificationOptions,
};
use crate::store::{GraphStore, SkeinError};
use skein_storage::{
    relational_foreign_key_index_name, RelationalForeignKeySchema, RelationalIndexDefinition,
    RelationalIndexRole, RelationalKey, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue, RELATIONAL_PRIMARY_INDEX_NAME,
};
use std::collections::{BTreeMap, BTreeSet};

pub const RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL: &str =
    "skein-relational-constraint-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalConstraintQualificationUse {
    PrimaryKeyIdentity,
    UniqueEnforcement,
    UpsertConflict,
    ForeignKeyTarget,
    ForeignKeyReferrers,
    NullableUniqueNoConflict,
    AbsentKeyNoConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConstraintQualificationProbeReport {
    pub ordinal: usize,
    pub table: String,
    pub index: String,
    pub uses: Vec<RelationalConstraintQualificationUse>,
    pub key_digest: String,
    pub candidate_rows: usize,
    pub oracle_rows: usize,
    pub candidate_digest: String,
    pub oracle_digest: String,
    pub semantic_valid: bool,
    pub matched: bool,
    pub read: RelationalIndexReadViewReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConstraintQualificationReport {
    pub protocol: &'static str,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub tables_discovered: usize,
    pub tables_sampled: usize,
    pub rows_sampled: usize,
    pub unique_targets_discovered: usize,
    pub nullable_unique_targets_discovered: usize,
    pub foreign_keys_discovered: usize,
    pub probes: Vec<RelationalConstraintQualificationProbeReport>,
    pub uses_covered: usize,
    pub mismatches: usize,
    pub truncated: bool,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConstraintProbeIdentity {
    table: String,
    index: String,
    key: RelationalKey,
}

impl GraphStore {
    /// Differentially checks bounded representative constraint lookups through
    /// the current generation-pinned relational index view without changing
    /// mutation routing or making the candidate authoritative.
    pub fn qualify_relational_constraint_read_view(
        &self,
        options: RelationalIndexViewQualificationOptions,
    ) -> crate::Result<RelationalConstraintQualificationReport> {
        self.relational_state
            .require_materialized_rows("relational constraint differential qualification")
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let view = self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "relational constraint read view is unavailable at commit epoch {}",
                    self.commit_epoch
                ))
            })?;
        let schemas = self.relational_state.table_schemas().collect::<Vec<_>>();
        let tables_discovered = schemas.len();
        let unique_targets_discovered = schemas
            .iter()
            .map(|schema| unique_definitions(schema).count())
            .sum();
        let nullable_unique_targets_discovered = schemas
            .iter()
            .flat_map(|schema| {
                unique_definitions(schema).map(move |definition| (schema, definition))
            })
            .filter(|(schema, definition)| definition_has_nullable_column(schema, definition))
            .count();
        let foreign_keys_discovered = schemas.iter().map(|schema| schema.foreign_keys.len()).sum();
        let mut scheduled = BTreeMap::<
            ConstraintProbeIdentity,
            BTreeSet<RelationalConstraintQualificationUse>,
        >::new();
        let mut truncated = tables_discovered > options.max_tables.get();
        let tables_sampled = tables_discovered.min(options.max_tables.get());
        let mut rows_sampled = 0usize;

        'tables: for schema in schemas.into_iter().take(options.max_tables.get()) {
            rows_sampled = rows_sampled
                .checked_add(
                    self.relational_state
                        .rows(&schema.name)
                        .take(options.max_rows_per_table.get())
                        .count(),
                )
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "relational constraint qualification row counter overflow".to_string(),
                    )
                })?;
            for definition in unique_definitions(schema) {
                if !schedule_unique_target_probes(
                    &self.relational_state,
                    schema,
                    &definition,
                    options.max_rows_per_table.get(),
                    options.max_probes.get(),
                    &mut scheduled,
                )? {
                    truncated = true;
                    break 'tables;
                }
            }
            for (ordinal, foreign_key) in schema.foreign_keys.iter().enumerate() {
                let referenced_schema = self
                    .relational_state
                    .table_schema(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "foreign key from {} references missing table {}",
                            schema.name, foreign_key.referenced_table
                        ))
                    })?;
                let referenced_definition = referenced_schema
                    .unique_index_definition(&foreign_key.referenced_columns)
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "foreign key from {} references non-unique columns on {}",
                            schema.name, foreign_key.referenced_table
                        ))
                    })?;
                if !schedule_foreign_key_probes(
                    ForeignKeyProbeContext {
                        state: &self.relational_state,
                        schema,
                        ordinal,
                        foreign_key,
                        referenced_schema,
                        referenced_definition: &referenced_definition,
                        max_rows: options.max_rows_per_table.get(),
                        max_probes: options.max_probes.get(),
                    },
                    &mut scheduled,
                )? {
                    truncated = true;
                    break 'tables;
                }
            }
        }

        let mut probes = Vec::with_capacity(scheduled.len());
        let mut mismatches = 0usize;
        let mut uses_covered = 0usize;
        for (ordinal, (identity, uses)) in scheduled.into_iter().enumerate() {
            let mut candidate = Vec::new();
            let read = view
                .visit_exact_postings(
                    &identity.table,
                    &identity.index,
                    &identity.key,
                    options.read_limits,
                    |primary_key| {
                        candidate.push(primary_key.clone());
                        true
                    },
                )
                .map_err(|error| {
                    qualification_probe_error(ordinal, &identity.table, &identity.index, error)
                })?;
            candidate.sort();
            candidate.dedup();
            let oracle = relational_constraint_oracle_rows(
                &self.relational_state,
                &identity.table,
                &identity.index,
                &identity.key,
                options.read_limits.max_rows.get(),
            )?;
            let semantic_valid = constraint_probe_semantics_valid(&uses, &candidate, &oracle);
            let matched = candidate == oracle && semantic_valid;
            mismatches = mismatches
                .checked_add(usize::from(!matched))
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "relational constraint mismatch counter overflow".to_string(),
                    )
                })?;
            uses_covered = uses_covered.checked_add(uses.len()).ok_or_else(|| {
                SkeinError::Storage("relational constraint use counter overflow".to_string())
            })?;
            let uses = uses.into_iter().collect::<Vec<_>>();
            probes.push(RelationalConstraintQualificationProbeReport {
                ordinal,
                table: identity.table,
                index: identity.index,
                uses,
                key_digest: relational_keys_digest(std::slice::from_ref(&identity.key)),
                candidate_rows: candidate.len(),
                oracle_rows: oracle.len(),
                candidate_digest: relational_keys_digest(&candidate),
                oracle_digest: relational_keys_digest(&oracle),
                semantic_valid,
                matched,
                read,
            });
        }
        let identity = view.identity();
        let ready = mismatches == 0 && !truncated;
        Ok(RelationalConstraintQualificationReport {
            protocol: RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL,
            base_generation: identity.base_generation,
            delta_generation: identity.delta_generation,
            base_commit_epoch: identity.base_commit_epoch,
            visible_commit_epoch: identity.visible_commit_epoch,
            root_set_digest: identity.root_set_digest.to_string(),
            tables_discovered,
            tables_sampled,
            rows_sampled,
            unique_targets_discovered,
            nullable_unique_targets_discovered,
            foreign_keys_discovered,
            probes,
            uses_covered,
            mismatches,
            truncated,
            ready,
        })
    }
}

fn unique_definitions(
    schema: &RelationalTableSchema,
) -> impl Iterator<Item = RelationalIndexDefinition> + '_ {
    schema
        .required_index_definitions()
        .into_iter()
        .filter(|definition| definition.role.is_unique())
}

fn definition_has_nullable_column(
    schema: &RelationalTableSchema,
    definition: &RelationalIndexDefinition,
) -> bool {
    definition.columns.iter().any(|column| {
        let position = schema
            .column_position(column)
            .expect("validated unique-index column");
        schema.columns[position].nullable
    })
}

fn schedule_unique_target_probes(
    state: &RelationalState,
    schema: &RelationalTableSchema,
    definition: &RelationalIndexDefinition,
    max_rows: usize,
    max_probes: usize,
    scheduled: &mut BTreeMap<
        ConstraintProbeIdentity,
        BTreeSet<RelationalConstraintQualificationUse>,
    >,
) -> crate::Result<bool> {
    if let Some((primary_key, row)) = state.rows(&schema.name).take(max_rows).find(|(_, row)| {
        let key = key_from_row(schema, row, &definition.columns);
        !key_contains_null(&key)
    }) {
        let key = if definition.role == RelationalIndexRole::Primary {
            primary_key.clone()
        } else {
            key_from_row(schema, row, &definition.columns)
        };
        let mut uses = BTreeSet::from([RelationalConstraintQualificationUse::UpsertConflict]);
        uses.insert(if definition.role == RelationalIndexRole::Primary {
            RelationalConstraintQualificationUse::PrimaryKeyIdentity
        } else {
            RelationalConstraintQualificationUse::UniqueEnforcement
        });
        if !schedule_probe(
            scheduled,
            ConstraintProbeIdentity {
                table: schema.name.clone(),
                index: definition.name.clone(),
                key,
            },
            uses,
            max_probes,
        ) {
            return Ok(false);
        }
    }

    if definition_has_nullable_column(schema, definition) {
        let mut key = synthetic_key(schema, &definition.columns, 0);
        let nullable_position = definition
            .columns
            .iter()
            .position(|column| {
                let position = schema
                    .column_position(column)
                    .expect("validated nullable unique-index column");
                schema.columns[position].nullable
            })
            .expect("nullable unique-index column was discovered");
        key.0[nullable_position] = RelationalValue::Null;
        if !schedule_probe(
            scheduled,
            ConstraintProbeIdentity {
                table: schema.name.clone(),
                index: definition.name.clone(),
                key,
            },
            BTreeSet::from([
                RelationalConstraintQualificationUse::UniqueEnforcement,
                RelationalConstraintQualificationUse::UpsertConflict,
                RelationalConstraintQualificationUse::NullableUniqueNoConflict,
            ]),
            max_probes,
        ) {
            return Ok(false);
        }
    }

    let Some(key) = find_absent_key(state, schema, definition)? else {
        return Ok(false);
    };
    let mut uses = BTreeSet::from([
        RelationalConstraintQualificationUse::UpsertConflict,
        RelationalConstraintQualificationUse::AbsentKeyNoConflict,
    ]);
    uses.insert(if definition.role == RelationalIndexRole::Primary {
        RelationalConstraintQualificationUse::PrimaryKeyIdentity
    } else {
        RelationalConstraintQualificationUse::UniqueEnforcement
    });
    Ok(schedule_probe(
        scheduled,
        ConstraintProbeIdentity {
            table: schema.name.clone(),
            index: definition.name.clone(),
            key,
        },
        uses,
        max_probes,
    ))
}

struct ForeignKeyProbeContext<'a> {
    state: &'a RelationalState,
    schema: &'a RelationalTableSchema,
    ordinal: usize,
    foreign_key: &'a RelationalForeignKeySchema,
    referenced_schema: &'a RelationalTableSchema,
    referenced_definition: &'a RelationalIndexDefinition,
    max_rows: usize,
    max_probes: usize,
}

fn schedule_foreign_key_probes(
    context: ForeignKeyProbeContext<'_>,
    scheduled: &mut BTreeMap<
        ConstraintProbeIdentity,
        BTreeSet<RelationalConstraintQualificationUse>,
    >,
) -> crate::Result<bool> {
    let ForeignKeyProbeContext {
        state,
        schema,
        ordinal,
        foreign_key,
        referenced_schema,
        referenced_definition,
        max_rows,
        max_probes,
    } = context;
    let local_index = relational_foreign_key_index_name(ordinal);
    if let Some((_, row)) = state.rows(&schema.name).take(max_rows).find(|(_, row)| {
        let key = key_from_row(schema, row, &foreign_key.columns);
        !key_contains_null(&key)
    }) {
        let key = key_from_row(schema, row, &foreign_key.columns);
        if !schedule_probe(
            scheduled,
            ConstraintProbeIdentity {
                table: referenced_schema.name.clone(),
                index: referenced_definition.name.clone(),
                key: key.clone(),
            },
            BTreeSet::from([RelationalConstraintQualificationUse::ForeignKeyTarget]),
            max_probes,
        ) || !schedule_probe(
            scheduled,
            ConstraintProbeIdentity {
                table: schema.name.clone(),
                index: local_index.clone(),
                key,
            },
            BTreeSet::from([RelationalConstraintQualificationUse::ForeignKeyReferrers]),
            max_probes,
        ) {
            return Ok(false);
        }
    }

    let Some(key) = find_absent_key(state, referenced_schema, referenced_definition)? else {
        return Ok(false);
    };
    if !schedule_probe(
        scheduled,
        ConstraintProbeIdentity {
            table: referenced_schema.name.clone(),
            index: referenced_definition.name.clone(),
            key: key.clone(),
        },
        BTreeSet::from([
            RelationalConstraintQualificationUse::ForeignKeyTarget,
            RelationalConstraintQualificationUse::AbsentKeyNoConflict,
        ]),
        max_probes,
    ) || !schedule_probe(
        scheduled,
        ConstraintProbeIdentity {
            table: schema.name.clone(),
            index: local_index,
            key,
        },
        BTreeSet::from([
            RelationalConstraintQualificationUse::ForeignKeyReferrers,
            RelationalConstraintQualificationUse::AbsentKeyNoConflict,
        ]),
        max_probes,
    ) {
        return Ok(false);
    }
    Ok(true)
}

fn schedule_probe(
    scheduled: &mut BTreeMap<
        ConstraintProbeIdentity,
        BTreeSet<RelationalConstraintQualificationUse>,
    >,
    identity: ConstraintProbeIdentity,
    uses: BTreeSet<RelationalConstraintQualificationUse>,
    max_probes: usize,
) -> bool {
    if let Some(existing) = scheduled.get_mut(&identity) {
        existing.extend(uses);
        return true;
    }
    if scheduled.len() >= max_probes {
        return false;
    }
    scheduled.insert(identity, uses);
    true
}

fn find_absent_key(
    state: &RelationalState,
    schema: &RelationalTableSchema,
    definition: &RelationalIndexDefinition,
) -> crate::Result<Option<RelationalKey>> {
    for salt in 0..64 {
        let key = synthetic_key(schema, &definition.columns, salt);
        if relational_constraint_oracle_rows(state, &schema.name, &definition.name, &key, 1)?
            .is_empty()
        {
            return Ok(Some(key));
        }
    }
    Ok(None)
}

fn synthetic_key(schema: &RelationalTableSchema, columns: &[String], salt: usize) -> RelationalKey {
    RelationalKey(
        columns
            .iter()
            .enumerate()
            .map(|(ordinal, column)| {
                let position = schema
                    .column_position(column)
                    .expect("validated synthetic-key column");
                synthetic_value(schema.columns[position].scalar_type, ordinal, salt)
            })
            .collect(),
    )
}

fn synthetic_value(kind: RelationalScalarType, ordinal: usize, salt: usize) -> RelationalValue {
    match kind {
        RelationalScalarType::Boolean => RelationalValue::Boolean(
            salt.checked_shr(u32::try_from(ordinal).unwrap_or(u32::MAX))
                .unwrap_or(0)
                & 1
                != 0,
        ),
        RelationalScalarType::BigInt => RelationalValue::BigInt(
            i64::MIN
                .saturating_add(i64::try_from(salt).unwrap_or(i64::MAX))
                .saturating_add(i64::try_from(ordinal).unwrap_or(i64::MAX)),
        ),
        RelationalScalarType::DoublePrecision => {
            RelationalValue::DoublePrecision(-1.0e300 + salt as f64 + ordinal as f64 / 64.0)
        }
        RelationalScalarType::Text => {
            RelationalValue::Text(format!("__skein_constraint_absent_{ordinal}_{salt}"))
        }
        RelationalScalarType::Bytea => RelationalValue::Bytea(vec![
            0xff,
            u8::try_from(ordinal % 256).expect("ordinal was reduced modulo 256"),
            u8::try_from(salt % 256).expect("salt was reduced modulo 256"),
        ]),
        RelationalScalarType::Uuid => RelationalValue::Uuid(skein_core::Uuid::from_u128(
            ((salt as u128) << 64) | ordinal as u128,
        )),
    }
}

fn key_from_row(
    schema: &RelationalTableSchema,
    row: &skein_storage::RelationalRow,
    columns: &[String],
) -> RelationalKey {
    RelationalKey(
        columns
            .iter()
            .map(|column| {
                let position = schema
                    .column_position(column)
                    .expect("validated constraint column");
                row.values()[position].clone()
            })
            .collect(),
    )
}

fn key_contains_null(key: &RelationalKey) -> bool {
    key.0
        .iter()
        .any(|value| matches!(value, RelationalValue::Null))
}

fn relational_constraint_oracle_rows(
    state: &RelationalState,
    table: &str,
    index: &str,
    key: &RelationalKey,
    max_rows: usize,
) -> crate::Result<Vec<RelationalKey>> {
    let mut rows = if index == RELATIONAL_PRIMARY_INDEX_NAME {
        state
            .row(table, key)
            .map(|_| vec![key.clone()])
            .unwrap_or_default()
    } else {
        state
            .index_prefix_lookup(table, index, key, max_rows.saturating_add(1))
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "relational constraint oracle is missing {table}.{index}"
                ))
            })?
            .into_iter()
            .cloned()
            .collect()
    };
    if rows.len() > max_rows {
        return Err(SkeinError::Storage(format!(
            "relational constraint oracle on {table}.{index} contains {} rows, exceeding limit {max_rows}",
            rows.len()
        )));
    }
    rows.sort();
    rows.dedup();
    Ok(rows)
}

fn constraint_probe_semantics_valid(
    uses: &BTreeSet<RelationalConstraintQualificationUse>,
    candidate: &[RelationalKey],
    oracle: &[RelationalKey],
) -> bool {
    let requires_empty = uses.contains(&RelationalConstraintQualificationUse::AbsentKeyNoConflict)
        || uses.contains(&RelationalConstraintQualificationUse::NullableUniqueNoConflict);
    if requires_empty {
        return candidate.is_empty() && oracle.is_empty();
    }
    let requires_unique_row = uses.iter().any(|use_kind| {
        matches!(
            use_kind,
            RelationalConstraintQualificationUse::PrimaryKeyIdentity
                | RelationalConstraintQualificationUse::UniqueEnforcement
                | RelationalConstraintQualificationUse::UpsertConflict
                | RelationalConstraintQualificationUse::ForeignKeyTarget
        )
    });
    if requires_unique_row {
        return candidate.len() == 1 && oracle.len() == 1;
    }
    if uses.contains(&RelationalConstraintQualificationUse::ForeignKeyReferrers) {
        return !candidate.is_empty() && !oracle.is_empty();
    }
    true
}
