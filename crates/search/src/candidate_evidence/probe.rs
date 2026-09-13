use super::{NowledgeMemSearchCandidateFieldSummary, NowledgeMemSearchCandidateShadowAccumulator};
use crate::SearchMode;
use skein_core::{Result, SkeinError};

pub fn parse_search_candidate_shadow_probe(
    value: &serde_json::Value,
) -> Result<NowledgeMemSearchCandidateShadowAccumulator> {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    let requests = value
        .get("requests")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field("requests", "array"))?;
    for request in requests {
        let primary_candidate_ids = required_string_array(request, "primary_candidate_ids")?;
        let shadow_candidate_ids = required_string_array(request, "shadow_candidate_ids")?;
        accumulator.record_compare_candidate_ids(&primary_candidate_ids, &shadow_candidate_ids);
    }
    let filter_pushdown = value
        .get("filter_pushdown")
        .ok_or_else(|| invalid_field("filter_pushdown", "object"))?;
    let pushed_predicate_count = required_u64(filter_pushdown, "pushed_predicate_count")?;
    if let Some(summaries) = optional_field_summaries(filter_pushdown)? {
        accumulator.record_filter_pushdown_summaries(pushed_predicate_count, true, summaries);
    } else {
        accumulator.record_filter_pushdown_fields(
            pushed_predicate_count,
            required_string_array(filter_pushdown, "fields")?,
        );
    }
    let retriever_legs = required_object(value, "retriever_legs")?;
    parse_retriever_leg(retriever_legs, "text", &mut accumulator)?;
    parse_retriever_leg(retriever_legs, "vector", &mut accumulator)?;
    let top_k_overlap = required_object(value, "top_k_overlap")?;
    parse_top_k_overlap(top_k_overlap, "fts", SearchMode::Text, &mut accumulator)?;
    parse_top_k_overlap(
        top_k_overlap,
        "vector",
        SearchMode::Vector,
        &mut accumulator,
    )?;
    let candidate_readiness = required_object(value, "candidate_readiness")?;
    accumulator.record_candidate_readiness_signals(
        required_bool(candidate_readiness, "source_chunk_identity_ready")?,
        required_bool(candidate_readiness, "fail_soft_observed")?,
        required_bool(candidate_readiness, "projection_marker_status_visible")?,
        required_bool(candidate_readiness, "projection_watermark_ready")?,
        required_bool(candidate_readiness, "embedding_identity_ready")?,
    );
    for blocker in optional_string_array(value, "blocker_codes")? {
        accumulator.add_blocker_code(blocker);
    }
    Ok(accumulator)
}

fn parse_top_k_overlap(
    value: &serde_json::Value,
    name: &'static str,
    mode: SearchMode,
    accumulator: &mut NowledgeMemSearchCandidateShadowAccumulator,
) -> Result<()> {
    let top_k = required_object(value, name)?;
    accumulator.record_top_k_overlap_candidate_ids(
        mode,
        &required_string_array(top_k, "primary_candidate_ids")?,
        &required_string_array(top_k, "shadow_candidate_ids")?,
    );
    Ok(())
}

fn parse_retriever_leg(
    value: &serde_json::Value,
    name: &'static str,
    accumulator: &mut NowledgeMemSearchCandidateShadowAccumulator,
) -> Result<()> {
    let leg = required_object(value, name)?;
    let available = required_bool(leg, "available")?;
    let candidate_count = required_u64(leg, "candidate_count")?;
    accumulator.record_retriever_leg(name, available, candidate_count);
    if available && candidate_count > 0 {
        return Ok(());
    }
    accumulator.add_blocker_code(format!("search_candidate_{name}_retriever_unavailable"));
    Ok(())
}

fn optional_field_summaries(
    value: &serde_json::Value,
) -> Result<Option<Vec<NowledgeMemSearchCandidateFieldSummary>>> {
    let Some(items) = value.get("field_summaries") else {
        return Ok(None);
    };
    let items = items
        .as_array()
        .ok_or_else(|| invalid_field("field_summaries", "object array"))?;
    items
        .iter()
        .map(parse_field_summary)
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn parse_field_summary(
    value: &serde_json::Value,
) -> Result<NowledgeMemSearchCandidateFieldSummary> {
    Ok(NowledgeMemSearchCandidateFieldSummary {
        field: required_string(value, "field")?,
        source: value
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("search_candidate_shadow_probe")
            .to_string(),
        segment_count: optional_usize(value, "segment_count")?.unwrap_or(1),
        value_summary_used: optional_bool(value, "value_summary_used")?.unwrap_or(false),
        value_summary_segment_count: optional_usize(value, "value_summary_segment_count")?
            .unwrap_or(0),
        numeric_range_summary_used: optional_bool(value, "numeric_range_summary_used")?
            .unwrap_or(false),
        numeric_range_segment_count: optional_usize(value, "numeric_range_segment_count")?
            .unwrap_or(0),
        timestamp_range_summary_used: optional_bool(value, "timestamp_range_summary_used")?
            .unwrap_or(false),
        timestamp_range_segment_count: optional_usize(value, "timestamp_range_segment_count")?
            .unwrap_or(0),
    })
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field(field, "string array"))?;
    string_array_items(items, field)
}

fn optional_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let Some(items) = value.get(field) else {
        return Ok(Vec::new());
    };
    let items = items
        .as_array()
        .ok_or_else(|| invalid_field(field, "string array"))?;
    string_array_items(items, field)
}

fn string_array_items(items: &[serde_json::Value], field: &str) -> Result<Vec<String>> {
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_field(field, "string array"))
        })
        .collect()
}

fn required_string(value: &serde_json::Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid_field(field, "string"))
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| invalid_field(field, "integer"))
}

fn required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn required_object<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a serde_json::Value> {
    value
        .get(field)
        .filter(|raw| raw.is_object())
        .ok_or_else(|| invalid_field(field, "object"))
}

fn optional_bool(value: &serde_json::Value, field: &str) -> Result<Option<bool>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    raw.as_bool()
        .map(Some)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn optional_usize(value: &serde_json::Value, field: &str) -> Result<Option<usize>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    let raw = raw
        .as_u64()
        .ok_or_else(|| invalid_field(field, "integer"))?;
    usize::try_from(raw)
        .map(Some)
        .map_err(|_| invalid_field(field, "usize-sized integer"))
}

fn invalid_field(field: &str, expected: &str) -> SkeinError {
    SkeinError::Semantic(format!(
        "search candidate shadow probe field '{field}' must be a {expected}"
    ))
}
