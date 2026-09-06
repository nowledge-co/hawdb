use super::query_io::{add, checkpoint, mul};
use crate::build_memory::{MAP_ENTRY_BYTES, SET_ENTRY_BYTES};
use crate::query_memory::Admitted;
use crate::{
    search_metadata_predicate_pushdown, Result, RuntimeTaskContext, SearchAccessControlContext,
    SearchPredicate, SearchPredicatePushdownReport, SearchPredicateSet, SkeinError,
};
use skein_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::collections::BTreeMap;
use std::mem::size_of;

#[derive(Debug)]
pub(super) struct Input {
    requested: Admitted<BTreeMap<String, String>>,
    pub predicates: SearchPredicateSet,
    _predicates_memory: QueryMemoryLease,
}

impl Input {
    pub fn new(
        filters: BTreeMap<String, String>,
        access: Option<&SearchAccessControlContext>,
        policy_epoch: Option<u64>,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<(Self, SearchPredicatePushdownReport)> {
        checkpoint(task)?;
        if let Some(access) = access {
            if let Some(epoch) = policy_epoch
                && epoch != access.policy_epoch
            {
                return Err(SkeinError::Storage(format!(
                    "search options policy epoch {epoch} does not match access control policy epoch {}",
                    access.policy_epoch
                )));
            }
            access.validate()?;
        }
        // An emptied B-tree may retain a leaf. Drop it instead of retaining an
        // unknown allocation or charging ordinary empty queries a node allowance.
        let filters = if filters.is_empty() {
            drop(filters);
            BTreeMap::new()
        } else {
            filters
        };
        let lease = memory.reserve(map_bytes(&filters, true, task)?)?;
        let requested = Admitted::new(filters, lease);
        let effective = access
            .map(|access| {
                let required = acl_bytes(&requested, access, task)?;
                let mut lease = memory.reserve(required)?;
                checkpoint(task)?;
                #[cfg(test)]
                tests::record_merge();
                let filters = access.effective_metadata_filters(&requested)?;
                checkpoint(task)?;
                let retained = map_bytes(&filters, true, task)?;
                if retained > required {
                    return Err(capacity_error());
                }
                lease.shrink(required - retained);
                Ok(Admitted::new(filters, lease))
            })
            .transpose()?;
        let filters = effective.as_deref().unwrap_or(&requested);
        let budget = parser_bytes(filters, task)?;
        let retained = memory.reserve(budget.retained)?;
        let scratch = memory.reserve(budget.scratch)?;
        checkpoint(task)?;
        #[cfg(test)]
        tests::record_parse();
        let parsed = search_metadata_predicate_pushdown(filters);
        #[cfg(test)]
        tests::after_parse();
        checkpoint(task)?;
        drop(scratch);
        drop(effective);
        // Reports retain their existing public return contract, not a new owner.
        // This lease covers the parsed predicate set, not later report growth.
        Ok((
            Self {
                requested,
                predicates: parsed.predicates,
                _predicates_memory: retained,
            },
            parsed.report,
        ))
    }

    pub fn requested(&self) -> &BTreeMap<String, String> {
        &self.requested
    }
}

fn capacity_error() -> SkeinError {
    SkeinError::Execution("search filter capacity exceeds admission".to_owned())
}

fn slots<T>(count: usize) -> Result<usize> {
    let bytes = mul(count, size_of::<T>())?;
    if bytes > isize::MAX as usize {
        return Err(capacity_error());
    }
    Ok(bytes)
}

fn map_bytes(
    filters: &BTreeMap<String, String>,
    owned: bool,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let mut bytes = mul(filters.len(), MAP_ENTRY_BYTES)?;
    for (key, value) in filters {
        checkpoint(task)?;
        bytes = add(bytes, if owned { key.capacity() } else { key.len() })?;
        bytes = add(bytes, if owned { value.capacity() } else { value.len() })?;
    }
    Ok(bytes)
}

fn acl_bytes(
    filters: &BTreeMap<String, String>,
    access: &SearchAccessControlContext,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let mut bytes = add(map_bytes(filters, false, task)?, MAP_ENTRY_BYTES)?;
    if access.allowed_visibility_values.len() == 1 {
        bytes = add(bytes, access.visibility_metadata_field.len())?;
        bytes = add(
            bytes,
            access.allowed_visibility_values.first().unwrap().len(),
        )?;
    } else {
        // serde_json 1.0.150 emits a sequence directly from BTreeSet. Its Vec
        // starts at 128; include old/new buffers during amortized growth, plus
        // the formatted field key. Every input byte needs at most six JSON bytes.
        let mut encoded = add(2, mul(access.allowed_visibility_values.len(), 3)?)?;
        for value in &access.allowed_visibility_values {
            checkpoint(task)?;
            encoded = add(encoded, mul(value.len(), 6)?)?;
        }
        bytes = add(bytes, mul(encoded.max(128), 3)?)?;
        bytes = add(
            bytes,
            mul(add(access.visibility_metadata_field.len(), 4)?, 3)?,
        )?;
    }
    Ok(bytes)
}

struct ParserBytes {
    retained: usize,
    scratch: usize,
}

fn parser_bytes(
    filters: &BTreeMap<String, String>,
    task: &RuntimeTaskContext,
) -> Result<ParserBytes> {
    let mut budget = ParserBytes {
        retained: slots::<SearchPredicate>(filters.len())?,
        scratch: 0,
    };
    for (key, value) in filters {
        checkpoint(task)?;
        // Canonical aliases may expand to temporal_context. Boolean values may
        // expand from one byte to "false"; enum normalization does not grow bytes.
        budget.retained = add(budget.retained, key.len().max("temporal_context".len()))?;
        budget.retained = add(budget.retained, add(value.len(), 5)?)?;
        // Include native parse-error formatting (six-byte debug escaping and
        // conservative buffer overlap), field temporaries and error conversion.
        let mut scratch = add(1024, add(mul(value.len(), 24)?, mul(key.len(), 4)?)?)?;
        if key.ends_with("__in") || key.ends_with("__not_in") {
            let count = list_slots(value, task)?;
            budget.retained = add(budget.retained, mul(count, SET_ENTRY_BYTES + 5)?)?;
            // serde 1.0.228's JSON sequence has no size hint. Vec<String> grows
            // from four slots; cover old/new allocations. Normalization reuses
            // that Vec and predicates insert into BTreeSet incrementally.
            scratch = add(scratch, slots::<String>(mul(count.max(4), 4)?)?)?;
        }
        // The parser consumes one filter at a time; only its retained predicate
        // payload overlaps subsequent filters, not its temporary workspace.
        budget.scratch = budget.scratch.max(scratch);
    }
    Ok(budget)
}

fn list_slots(value: &str, task: &RuntimeTaskContext) -> Result<usize> {
    let mut count = 1usize;
    let mut quoted = false;
    let mut escaped = false;
    // This only bounds the valid prefix's element count; the optimizer remains
    // the syntax authority. Commas inside quoted strings are not elements.
    for chunk in value.as_bytes().chunks(4096) {
        checkpoint(task)?;
        for byte in chunk {
            if escaped {
                escaped = false;
            } else if quoted && *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                quoted = !quoted;
            } else if !quoted && *byte == b',' {
                count = add(count, 1)?;
            }
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests;
