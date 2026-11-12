// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The `hawdb_open` options JSON, mapped onto the typed Rust open options.
//!
//! Every key names the `DatabaseConfig` / `HawDBEmbeddedOpenOptions` field it
//! sets. Absent keys keep the embedded defaults, so a host that passes no
//! options keeps power-loss-safe `sync_on_every_write` commits and the default
//! read budgets. Unknown keys and ill-typed values are rejected rather than
//! ignored, so a misspelt durability or budget option can never be silently
//! dropped.

use std::num::NonZeroUsize;

use hawdb::{DurabilityPolicy, HawDBEmbeddedOpenOptions, RecoveryMode};

use super::FfiError;

/// Keys accepted by `hawdb_open`, in documentation order.
pub(crate) const OPEN_OPTION_KEYS: [&str; 6] = [
    "read_only",
    "durability",
    "max_read_result_rows",
    "max_read_intermediate_rows",
    "max_read_result_payload_bytes",
    "relational_monotonic_append_fast_path",
];

/// Apply a parsed `options_json` object to `options`.
pub(crate) fn apply_open_options(
    options: &mut HawDBEmbeddedOpenOptions,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), FfiError> {
    if let Some(unknown) = object
        .keys()
        .find(|key| !OPEN_OPTION_KEYS.contains(&key.as_str()))
    {
        return Err(FfiError::invalid(format!(
            "hawdb: unknown open option \"{unknown}\"; supported options are {}",
            OPEN_OPTION_KEYS
                .iter()
                .map(|key| format!("\"{key}\""))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    if let Some(value) = object.get("read_only") {
        let read_only = bool_option(value, "read_only")?;
        options.config.read_only = read_only;
        if read_only {
            options.config.recovery_mode = RecoveryMode::Strict;
        }
    }
    if let Some(value) = object.get("durability") {
        options.durability = match value.as_str() {
            Some("sync_on_every_write") => DurabilityPolicy::SyncOnEveryWrite,
            Some("sync_on_checkpoint") => DurabilityPolicy::SyncOnCheckpoint,
            _ => {
                return Err(FfiError::invalid(
                    "hawdb: \"durability\" must be \"sync_on_every_write\" or \"sync_on_checkpoint\"",
                ));
            }
        };
    }
    if let Some(value) = object.get("max_read_result_rows") {
        // `null` and 0 both mean "no host row ceiling", as in DatabaseConfig.
        options.config.max_read_result_rows = if value.is_null() {
            None
        } else {
            Some(usize_option(
                value,
                "max_read_result_rows",
                "a non-negative integer or null",
            )?)
        };
    }
    if let Some(value) = object.get("max_read_intermediate_rows") {
        // `null` inherits the result-row ceiling; 0 removes only the
        // relational intermediate-row ceiling, as in DatabaseConfig.
        options.config.max_read_intermediate_rows = if value.is_null() {
            None
        } else {
            Some(usize_option(
                value,
                "max_read_intermediate_rows",
                "a non-negative integer or null",
            )?)
        };
    }
    if let Some(value) = object.get("max_read_result_payload_bytes") {
        // The payload budget can be tuned but never removed through the C ABI.
        let bytes = usize_option(value, "max_read_result_payload_bytes", "a positive integer")?;
        let bytes = NonZeroUsize::new(bytes).ok_or_else(|| {
            FfiError::invalid("hawdb: \"max_read_result_payload_bytes\" must be a positive integer")
        })?;
        options.config.max_read_result_payload_bytes = Some(bytes.get());
    }
    if let Some(value) = object.get("relational_monotonic_append_fast_path") {
        options.config.relational_monotonic_append_fast_path =
            bool_option(value, "relational_monotonic_append_fast_path")?;
    }
    Ok(())
}

fn bool_option(value: &serde_json::Value, name: &str) -> Result<bool, FfiError> {
    value
        .as_bool()
        .ok_or_else(|| FfiError::invalid(format!("hawdb: \"{name}\" must be a bool")))
}

fn usize_option(value: &serde_json::Value, name: &str, expected: &str) -> Result<usize, FfiError> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| FfiError::invalid(format!("hawdb: \"{name}\" must be {expected}")))
}
