//! Graph checkpoint text encoding.

use crate::text::{
    decode_bool, decode_index_kind, decode_nullable, decode_properties, decode_property_type,
    decode_schema_object_state, decode_string, decode_string_vec, decode_table_kind,
    decode_u64_vec, decode_value_vec, encode_bool, encode_bytes, encode_index_kind,
    encode_nullable, encode_property_type, encode_schema_object_state, encode_string,
    encode_string_vec, encode_table_kind, encode_u64_vec, encode_value_vec, parse_u32, parse_u64,
    parse_usize,
};
use crate::{
    artifact_binding::DurableArtifactMetadata, decode_relational_primary_key,
    durable_manifest::validate_storage_version, durable_manifest::STORAGE_VERSION,
    encode_relational_primary_key, NodeId, NodeRecord, ProjectedGraphDefinition, RelId, RelRecord,
    RelationalPrimaryKeyChangeCapture, RelationalPrimaryKeyChangeRebuildReason,
    RelationalTablePrimaryKeyChanges, SearchProjectionGraphChange,
};
use skein_core::{
    BasicGraphStatistics, Catalog, ConstraintId, ConstraintSubject, GraphStatistics, IndexId,
    IndexStatisticsSample, LabelId, PropertyId, RelTypeId, Result, SkeinError, TableId, TableKind,
    Uuid,
};
use std::collections::{BTreeMap, BTreeSet};

pub const CHECKPOINT_HEADER_V1: &str = "SKEIN_CHECKPOINT_V1";

pub struct CheckpointImage<'a> {
    pub catalog: &'a Catalog,
    pub commit_epoch: u64,
    pub next_node_id: u64,
    pub next_rel_id: u64,
    pub search_projection_change_log_start_epoch: u64,
    pub search_projection_graph_changes: &'a [SearchProjectionGraphChange],
    pub statistics: &'a GraphStatistics,
    pub projected_graphs: &'a BTreeMap<String, ProjectedGraphDefinition>,
    pub initial_import_source_fingerprint: Option<&'a str>,
    pub search_projection_database_identity: Option<Uuid>,
    pub relational_checkpoint: Option<DurableArtifactMetadata>,
}

pub fn encode_search_projection_relational_primary_key_changes(
    capture: &RelationalPrimaryKeyChangeCapture,
) -> Result<(String, String)> {
    match capture {
        RelationalPrimaryKeyChangeCapture::Captured { tables, .. } => {
            let mut encoded_tables = Vec::with_capacity(tables.len());
            for table in tables {
                let encoded_keys = table
                    .primary_keys
                    .iter()
                    .map(|key| {
                        encode_relational_primary_key(key)
                            .map(|encoded| encode_bytes(&encoded))
                            .map_err(|error| SkeinError::Storage(error.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                encoded_tables.push(format!(
                    "{}={}",
                    encode_string(&table.table),
                    encoded_keys.join(":")
                ));
            }
            Ok(("exact".to_string(), encoded_tables.join(";")))
        }
        RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason } => Ok((
            match reason {
                RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite => "rebuild_schema_rewrite",
                RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded => {
                    "rebuild_capture_limit"
                }
                RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding => {
                    "rebuild_key_encoding"
                }
                RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded => {
                    "rebuild_wal_encoding_limit"
                }
                RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture => {
                    "rebuild_missing_wal_capture"
                }
                RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement => {
                    "rebuild_snapshot_replacement"
                }
                RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions => {
                    "rebuild_multiple_relational_transactions"
                }
            }
            .to_string(),
            String::new(),
        )),
    }
}

fn validate_search_projection_checkpoint_changes(
    start_epoch: u64,
    checkpoint_commit_epoch: u64,
    changes: &[SearchProjectionGraphChange],
) -> Result<()> {
    if start_epoch > checkpoint_commit_epoch {
        return Err(SkeinError::Storage(format!(
            "search projection change log start epoch {start_epoch} exceeds checkpoint commit epoch {checkpoint_commit_epoch}"
        )));
    }
    let mut previous_epoch = start_epoch;
    for change in changes {
        if change.commit_epoch <= previous_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change commit epoch {} is not greater than previous epoch {previous_epoch}",
                change.commit_epoch
            )));
        }
        if change.commit_epoch > checkpoint_commit_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change commit epoch {} exceeds checkpoint commit epoch {checkpoint_commit_epoch}",
                change.commit_epoch
            )));
        }
        if !change
            .upsert_node_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(SkeinError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate upsert node ids",
                change.commit_epoch
            )));
        }
        if !change
            .delete_document_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(SkeinError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate delete document ids",
                change.commit_epoch
            )));
        }
        if let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
            &change.relational_primary_key_changes
        {
            if !tables.windows(2).all(|pair| pair[0].table < pair[1].table) {
                return Err(SkeinError::Storage(format!(
                    "search projection change at commit epoch {} has unordered or duplicate relational tables",
                    change.commit_epoch
                )));
            }
            for table in tables {
                if table.primary_keys.is_empty()
                    || !table.primary_keys.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(SkeinError::Storage(format!(
                        "search projection change at commit epoch {} has empty, unordered, or duplicate primary keys for table {}",
                        change.commit_epoch, table.table
                    )));
                }
            }
        }
        previous_epoch = change.commit_epoch;
    }
    Ok(())
}

pub fn encode_checkpoint_body(image: &CheckpointImage<'_>, generation: u64) -> Result<String> {
    validate_search_projection_checkpoint_changes(
        image.search_projection_change_log_start_epoch,
        image.commit_epoch,
        image.search_projection_graph_changes,
    )?;
    let mut body = String::new();
    body.push_str(&format!("{CHECKPOINT_HEADER_V1}\n"));
    body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
    body.push_str(&format!("generation\t{generation}\n"));
    body.push_str(&format!("commit_epoch\t{}\n", image.commit_epoch));
    if let Some(relational) = image.relational_checkpoint {
        body.push_str(&format!(
            "relational_checkpoint_encoded_len\t{}\n",
            relational.encoded_len
        ));
        body.push_str(&format!(
            "relational_checkpoint_encoded_checksum\t{}\n",
            relational.encoded_checksum
        ));
        body.push_str(&format!(
            "relational_checkpoint_encoded_sha256\t{}\n",
            relational.encoded_sha256
        ));
    }
    body.push_str(&format!("next_node_id\t{}\n", image.next_node_id));
    body.push_str(&format!("next_rel_id\t{}\n", image.next_rel_id));
    body.push_str("canonical_records\ttrue\n");
    body.push_str(&format!(
        "search_projection_change_log_start_epoch\t{}\n",
        image.search_projection_change_log_start_epoch
    ));
    if let Some(identity) = image.search_projection_database_identity {
        body.push_str(&format!(
            "search_projection_database_identity\t{identity}\n"
        ));
    }
    if let Some(source_fingerprint) = image.initial_import_source_fingerprint {
        body.push_str(&format!(
            "initial_import_source_fingerprint\t{}\n",
            encode_string(source_fingerprint)
        ));
    }
    for change in image.search_projection_graph_changes {
        let (relational_kind, relational_changes) =
            encode_search_projection_relational_primary_key_changes(
                &change.relational_primary_key_changes,
            )?;
        body.push_str(&format!(
            "search_projection_change\t{}\t{}\t{}\t{}\t{}\n",
            change.commit_epoch,
            encode_u64_vec(change.upsert_node_ids.iter().copied()),
            encode_string_vec(&change.delete_document_ids),
            relational_kind,
            relational_changes,
        ));
    }
    for label in image.catalog.labels() {
        if !label.name.is_empty() {
            body.push_str(&format!(
                "label\t{}\t{}\n",
                label.id.0,
                encode_string(&label.name)
            ));
        }
    }
    for rel_type in image.catalog.rel_types() {
        if !rel_type.name.is_empty() {
            body.push_str(&format!(
                "rel_type\t{}\t{}\n",
                rel_type.id.0,
                encode_string(&rel_type.name)
            ));
        }
    }
    for index in image.catalog.property_indexes() {
        body.push_str(&format!(
            "property_index\t{}\t{}\t{}\t{}\n",
            index.id.0,
            index.label_id.0,
            encode_string(&index.property),
            encode_index_kind(index.kind)
        ));
    }
    for index in image.catalog.composite_property_indexes() {
        body.push_str(&format!(
            "composite_property_index\t{}\t{}\t{}\n",
            index.id.0,
            index.label_id.0,
            encode_string_vec(&index.properties)
        ));
    }
    for table in image.catalog.table_descriptors() {
        body.push_str(&format!(
            "table\t{}\t{}\t{}\t{}\n",
            table.id.0,
            encode_table_kind(table.kind),
            encode_string(&table.name),
            encode_schema_object_state(table.state)
        ));
    }
    for property in image.catalog.property_descriptors() {
        body.push_str(&format!(
            "property\t{}\t{}\t{}\t{}\t{}\t{}\n",
            property.id.0,
            property.table_id.0,
            encode_string(&property.name),
            encode_property_type(property.value_type),
            encode_nullable(property.nullable),
            encode_schema_object_state(property.state)
        ));
    }
    for constraint in image.catalog.unique_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        body.push_str(&format!(
            "unique_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            label_id.0,
            encode_string(&constraint.property)
        ));
    }
    for constraint in image.catalog.node_property_exists_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        body.push_str(&format!(
            "node_property_exists_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            label_id.0,
            encode_string(&constraint.property)
        ));
    }
    for constraint in image.catalog.relationship_property_exists_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        body.push_str(&format!(
            "relationship_property_exists_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            rel_type_id.0,
            encode_string(&constraint.property)
        ));
    }
    for constraint in image.catalog.relationship_unique_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        body.push_str(&format!(
            "relationship_unique_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            rel_type_id.0,
            encode_string(&constraint.property)
        ));
    }
    let statistics = image.statistics;
    body.push_str(&format!(
        "stat_commit_epoch\t{}\n",
        statistics.computed_at_commit_epoch
    ));
    body.push_str(&format!(
        "stat_advanced_complete\t{}\n",
        statistics.advanced_statistics_complete
    ));
    body.push_str(&format!(
        "stat_histogram_sample_limit\t{}\n",
        statistics.histogram_sample_limit
    ));
    body.push_str(&format!("stat_node_count\t{}\n", statistics.node_count));
    body.push_str(&format!(
        "stat_relationship_count\t{}\n",
        statistics.relationship_count
    ));
    for (label_id, count) in &statistics.label_counts {
        body.push_str(&format!("stat_label_count\t{}\t{}\n", label_id.0, count));
    }
    for (rel_type_id, count) in &statistics.rel_type_counts {
        body.push_str(&format!(
            "stat_rel_type_count\t{}\t{}\n",
            rel_type_id.0, count
        ));
    }
    for (rel_type_id, count) in &statistics.rel_type_source_counts {
        body.push_str(&format!(
            "stat_rel_type_source_count\t{}\t{}\n",
            rel_type_id.0, count
        ));
    }
    for (rel_type_id, count) in &statistics.rel_type_target_counts {
        body.push_str(&format!(
            "stat_rel_type_target_count\t{}\t{}\n",
            rel_type_id.0, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in &statistics.path_counts {
        body.push_str(&format!(
            "stat_path_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in
        &statistics.path_source_distinct_counts
    {
        body.push_str(&format!(
            "stat_path_source_distinct_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in
        &statistics.path_target_distinct_counts
    {
        body.push_str(&format!(
            "stat_path_target_distinct_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_counts
    {
        body.push_str(&format!(
            "stat_bounded_path_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_source_distinct_counts
    {
        body.push_str(&format!(
            "stat_bounded_path_source_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ));
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_target_distinct_counts
    {
        body.push_str(&format!(
            "stat_bounded_path_target_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ));
    }
    for (index_id, sample) in &statistics.index_samples {
        body.push_str(&format!(
            "stat_index_sample\t{}\t{}\t{}\t{}\t{}\n",
            index_id.0,
            sample.index_size,
            sample.unique_values,
            sample.sample_size,
            sample.updates_since_sample
        ));
    }
    for ((label_id, property), count) in &statistics.property_distinct_counts {
        body.push_str(&format!(
            "stat_property_distinct_count\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property),
            count
        ));
    }
    for ((rel_type_id, property), count) in &statistics.rel_property_distinct_counts {
        body.push_str(&format!(
            "stat_rel_property_distinct_count\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property),
            count
        ));
    }
    for ((rel_type_id, property), values) in &statistics.rel_property_histograms {
        body.push_str(&format!(
            "stat_rel_property_histogram\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property),
            encode_value_vec(values)
        ));
    }
    for ((rel_type_id, property), sampled) in &statistics.sampled_rel_property_histograms {
        body.push_str(&format!(
            "stat_rel_property_histogram_sampled\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property),
            encode_bool(*sampled)
        ));
    }
    for ((label_id, property), values) in &statistics.property_histograms {
        body.push_str(&format!(
            "stat_property_histogram\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property),
            encode_value_vec(values)
        ));
    }
    for ((label_id, property), sampled) in &statistics.sampled_property_histograms {
        body.push_str(&format!(
            "stat_property_histogram_sampled\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property),
            encode_bool(*sampled)
        ));
    }
    for (name, definition) in image.projected_graphs {
        body.push_str(&format!(
            "project_graph\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types)
        ));
    }
    Ok(body)
}

#[doc(hidden)]
pub fn split_checkpoint_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "checkpoint missing checksum footer".to_string(),
        ));
    };
    let checksum = crate::text::parse_u64(footer.trim(), "checkpoint checksum")?;
    Ok((body, checksum))
}

#[doc(hidden)]
pub fn relational_checkpoint_metadata(body: &str) -> Result<Option<DurableArtifactMetadata>> {
    let mut encoded_len = None;
    let mut encoded_checksum = None;
    let mut encoded_sha256 = None;
    for line in body.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["relational_checkpoint_encoded_len", raw] if encoded_len.is_none() => {
                encoded_len = Some(crate::text::parse_u64(
                    raw,
                    "relational checkpoint encoded length",
                )?);
            }
            ["relational_checkpoint_encoded_checksum", raw] if encoded_checksum.is_none() => {
                encoded_checksum = Some(crate::text::parse_u64(
                    raw,
                    "relational checkpoint encoded checksum",
                )?);
            }
            ["relational_checkpoint_encoded_sha256", raw] if encoded_sha256.is_none() => {
                encoded_sha256 = Some(raw.parse().map_err(|error| {
                    SkeinError::Storage(format!(
                        "invalid relational checkpoint encoded SHA-256: {error}"
                    ))
                })?);
            }
            ["relational_checkpoint_encoded_len", _]
            | ["relational_checkpoint_encoded_checksum", _]
            | ["relational_checkpoint_encoded_sha256", _] => {
                return Err(SkeinError::Storage(
                    "checkpoint contains duplicate relational artifact metadata".to_string(),
                ));
            }
            _ => {}
        }
    }
    if !crate::durable_manifest::artifact_metadata_presence_consistent(
        encoded_len,
        encoded_checksum,
        encoded_sha256,
    ) {
        return Err(SkeinError::Storage(
            "checkpoint relational artifact metadata is incomplete".to_string(),
        ));
    }
    Ok(encoded_len.map(|encoded_len| DurableArtifactMetadata {
        encoded_len,
        encoded_checksum: encoded_checksum.expect("validated relational checksum"),
        encoded_sha256: encoded_sha256.expect("validated relational SHA-256"),
    }))
}

#[doc(hidden)]
pub fn parse_label_set(input: &str) -> Result<BTreeSet<LabelId>> {
    if input.is_empty() {
        return Ok(BTreeSet::new());
    }
    input
        .split(',')
        .map(|raw| crate::text::parse_u32(raw, "label id").map(LabelId))
        .collect()
}

#[doc(hidden)]
pub fn decode_search_projection_relational_primary_key_changes(
    raw_kind: &str,
    raw_changes: &str,
) -> Result<RelationalPrimaryKeyChangeCapture> {
    let rebuild_reason = match raw_kind {
        "exact" => None,
        "rebuild_schema_rewrite" => Some(RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite),
        "rebuild_capture_limit" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded)
        }
        "rebuild_key_encoding" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding)
        }
        "rebuild_wal_encoding_limit" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded)
        }
        "rebuild_missing_wal_capture" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture)
        }
        "rebuild_snapshot_replacement" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement)
        }
        "rebuild_multiple_relational_transactions" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions)
        }
        _ => {
            return Err(SkeinError::Storage(format!(
                "invalid search projection relational change kind: {raw_kind}"
            )))
        }
    };
    if let Some(reason) = rebuild_reason {
        if !raw_changes.is_empty() {
            return Err(SkeinError::Storage(format!(
                "search projection rebuild marker {raw_kind} contains unexpected key payload"
            )));
        }
        return Ok(RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason });
    }

    const TABLE_FIXED_BYTES: usize = 4;
    const KEY_FIXED_BYTES: usize = 4;
    let mut tables = Vec::new();
    let mut encoded_bytes = 0usize;
    if !raw_changes.is_empty() {
        for raw_table in raw_changes.split(';') {
            let Some((raw_name, raw_keys)) = raw_table.split_once('=') else {
                return Err(SkeinError::Storage(format!(
                    "invalid search projection relational table change: {raw_table}"
                )));
            };
            let table = crate::text::decode_string(raw_name)?;
            if raw_keys.is_empty() {
                return Err(SkeinError::Storage(format!(
                    "search projection relational table {table} contains no primary keys"
                )));
            }
            encoded_bytes = encoded_bytes
                .checked_add(TABLE_FIXED_BYTES)
                .and_then(|bytes| bytes.checked_add(table.len()))
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "search projection relational change byte count overflow".to_string(),
                    )
                })?;
            let mut primary_keys = Vec::new();
            for raw_key in raw_keys.split(':') {
                let key_bytes = crate::text::decode_bytes(raw_key)?;
                let key = decode_relational_primary_key(&key_bytes)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                encoded_bytes = encoded_bytes
                    .checked_add(KEY_FIXED_BYTES)
                    .and_then(|bytes| bytes.checked_add(key_bytes.len()))
                    .ok_or_else(|| {
                        SkeinError::Storage(
                            "search projection relational change byte count overflow".to_string(),
                        )
                    })?;
                primary_keys.push(key);
            }
            tables.push(RelationalTablePrimaryKeyChanges {
                table,
                primary_keys,
            });
        }
    }
    Ok(RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    })
}

#[doc(hidden)]
#[derive(Default)]
pub struct DecodedCheckpoint {
    pub generation: Option<u64>,
    pub commit_epoch: Option<u64>,
    pub next_node_id: u64,
    pub next_rel_id: u64,
    pub search_projection_change_log_start_epoch: Option<u64>,
    pub search_projection_change_log_retained_bytes: usize,
    pub search_projection_graph_changes: Vec<SearchProjectionGraphChange>,
    pub search_projection_database_identity: Option<Uuid>,
    pub initial_import_source_fingerprint: Option<String>,
    pub basic_statistics: BasicGraphStatistics,
    pub checkpoint_statistics: GraphStatistics,
    pub projected_graphs: BTreeMap<String, ProjectedGraphDefinition>,
    pub nodes: Vec<NodeRecord>,
    pub relationships: Vec<RelRecord>,
    pub canonical_records: bool,
    pub saw_checkpoint_statistics: bool,
    pub statistics_complete: Option<bool>,
}

#[doc(hidden)]
pub fn parse_statistics_path_key(
    source: &str,
    rel_type: &str,
    target: &str,
) -> Result<(LabelId, RelTypeId, LabelId)> {
    Ok((
        LabelId(parse_u32(source, "statistics source label id")?),
        RelTypeId(parse_u32(rel_type, "statistics relationship type id")?),
        LabelId(parse_u32(target, "statistics target label id")?),
    ))
}

#[doc(hidden)]
pub fn parse_statistics_bounded_path_key(
    source: &str,
    rel_type: &str,
    target: &str,
    hops: &str,
) -> Result<(LabelId, RelTypeId, LabelId, usize)> {
    let (source, rel_type, target) = parse_statistics_path_key(source, rel_type, target)?;
    Ok((
        source,
        rel_type,
        target,
        parse_usize(hops, "statistics bounded path hop count")?,
    ))
}

#[doc(hidden)]
pub fn parse_checkpoint(
    body: &str,
    catalog: &mut Catalog,
    state: &mut DecodedCheckpoint,
) -> Result<()> {
    let mut saw_storage_version = false;
    let mut lines = body.lines();
    if lines.next() != Some(CHECKPOINT_HEADER_V1) {
        return Err(SkeinError::Storage(
            "checkpoint is missing the V1 format header".to_string(),
        ));
    }
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["version", version] => {
                if saw_storage_version {
                    return Err(SkeinError::Storage(
                        "checkpoint has duplicate storage version".to_string(),
                    ));
                }
                validate_storage_version(version)?;
                saw_storage_version = true;
            }
            ["generation", raw] => {
                state.generation = Some(parse_u64(raw, "checkpoint generation")?);
            }
            ["next_node_id", raw] => {
                state.next_node_id = parse_u64(raw, "next_node_id")?;
            }
            ["next_rel_id", raw] => {
                state.next_rel_id = parse_u64(raw, "next_rel_id")?;
            }
            ["canonical_records", "true"] => {
                state.canonical_records = true;
            }
            ["commit_epoch", raw] => {
                state.commit_epoch = Some(parse_u64(raw, "commit_epoch")?);
            }
            ["relational_checkpoint_encoded_len", _]
            | ["relational_checkpoint_encoded_checksum", _]
            | ["relational_checkpoint_encoded_sha256", _] => {}
            ["search_projection_change_log_start_epoch", raw] => {
                if state.search_projection_change_log_start_epoch.is_some() {
                    return Err(SkeinError::Storage(
                        "checkpoint contains duplicate search projection change log start epoch"
                            .to_string(),
                    ));
                }
                state.search_projection_change_log_start_epoch =
                    Some(parse_u64(raw, "search projection change log start epoch")?);
            }
            ["search_projection_database_identity", raw] => {
                if state.search_projection_database_identity.is_some() {
                    return Err(SkeinError::Storage(
                        "checkpoint contains duplicate search projection database identity".into(),
                    ));
                }
                let identity = raw.parse::<Uuid>().map_err(|_| {
                    SkeinError::Storage("invalid search projection database identity".into())
                })?;
                if identity.is_nil() || identity.to_string() != *raw {
                    return Err(SkeinError::Storage(
                        "noncanonical search projection database identity".into(),
                    ));
                }
                state.search_projection_database_identity = Some(identity);
            }
            ["initial_import_source_fingerprint", raw] => {
                if state.initial_import_source_fingerprint.is_some() {
                    return Err(SkeinError::Storage(
                        "checkpoint contains duplicate initial import source fingerprint"
                            .to_string(),
                    ));
                }
                state.initial_import_source_fingerprint = Some(decode_string(raw)?);
            }
            ["search_projection_change", raw_commit_epoch, raw_upsert_node_ids, raw_delete_document_ids, raw_relational_kind, raw_relational_changes] =>
            {
                let change = SearchProjectionGraphChange {
                    commit_epoch: parse_u64(
                        raw_commit_epoch,
                        "search projection change commit epoch",
                    )?,
                    upsert_node_ids: decode_u64_vec(
                        raw_upsert_node_ids,
                        "search projection change upsert node id",
                    )?,
                    delete_document_ids: decode_string_vec(raw_delete_document_ids)?,
                    relational_primary_key_changes:
                        decode_search_projection_relational_primary_key_changes(
                            raw_relational_kind,
                            raw_relational_changes,
                        )?,
                };
                state.search_projection_change_log_retained_bytes = state
                    .search_projection_change_log_retained_bytes
                    .checked_add(change.estimated_retained_bytes())
                    .ok_or_else(|| {
                        SkeinError::Storage(
                            "search projection change log retained byte count overflow".to_string(),
                        )
                    })?;
                state.search_projection_graph_changes.push(change);
            }
            ["label", raw_id, raw_name] => {
                catalog.import_label(
                    LabelId(parse_u32(raw_id, "label id")?),
                    decode_string(raw_name)?,
                );
            }
            ["rel_type", raw_id, raw_name] => {
                catalog.import_rel_type(
                    RelTypeId(parse_u32(raw_id, "rel type id")?),
                    decode_string(raw_name)?,
                );
            }
            ["property_index", raw_id, raw_label_id, raw_property] => {
                catalog.import_property_index(
                    IndexId(parse_u32(raw_id, "property index id")?),
                    LabelId(parse_u32(raw_label_id, "property index label id")?),
                    decode_string(raw_property)?,
                );
            }
            ["property_index", raw_id, raw_label_id, raw_property, raw_kind] => {
                catalog.import_property_index_with_kind(
                    IndexId(parse_u32(raw_id, "property index id")?),
                    LabelId(parse_u32(raw_label_id, "property index label id")?),
                    decode_string(raw_property)?,
                    decode_index_kind(raw_kind)?,
                );
            }
            ["composite_property_index", raw_id, raw_label_id, raw_properties] => {
                catalog.import_composite_property_index(
                    IndexId(parse_u32(raw_id, "composite property index id")?),
                    LabelId(parse_u32(
                        raw_label_id,
                        "composite property index label id",
                    )?),
                    decode_string_vec(raw_properties)?,
                );
            }
            ["table", raw_id, raw_kind, raw_name, raw_state] => {
                let kind = decode_table_kind(raw_kind)?;
                let name = decode_string(raw_name)?;
                match kind {
                    TableKind::Node => {
                        catalog.get_or_create_label(&name);
                    }
                    TableKind::Relationship => {
                        catalog.get_or_create_rel_type(&name);
                    }
                }
                catalog.import_table(
                    TableId(parse_u32(raw_id, "table id")?),
                    kind,
                    name,
                    decode_schema_object_state(raw_state)?,
                );
            }
            ["property", raw_id, raw_table_id, raw_name, raw_type, raw_nullable, raw_state] => {
                catalog.import_property_descriptor(
                    PropertyId(parse_u32(raw_id, "property id")?),
                    TableId(parse_u32(raw_table_id, "property table id")?),
                    decode_string(raw_name)?,
                    decode_property_type(raw_type)?,
                    decode_nullable(raw_nullable)?,
                    decode_schema_object_state(raw_state)?,
                );
            }
            ["unique_constraint", raw_id, raw_label_id, raw_property] => {
                catalog.import_unique_constraint(
                    ConstraintId(parse_u32(raw_id, "unique constraint id")?),
                    LabelId(parse_u32(raw_label_id, "unique constraint label id")?),
                    decode_string(raw_property)?,
                );
            }
            ["node_property_exists_constraint", raw_id, raw_label_id, raw_property] => {
                catalog.import_node_property_exists_constraint(
                    ConstraintId(parse_u32(raw_id, "node property exists constraint id")?),
                    LabelId(parse_u32(
                        raw_label_id,
                        "node property exists constraint label id",
                    )?),
                    decode_string(raw_property)?,
                );
            }
            ["relationship_property_exists_constraint", raw_id, raw_rel_type_id, raw_property] => {
                catalog.import_relationship_property_exists_constraint(
                    ConstraintId(parse_u32(
                        raw_id,
                        "relationship property exists constraint id",
                    )?),
                    RelTypeId(parse_u32(
                        raw_rel_type_id,
                        "relationship property exists constraint rel type id",
                    )?),
                    decode_string(raw_property)?,
                );
            }
            ["relationship_unique_constraint", raw_id, raw_rel_type_id, raw_property] => {
                catalog.import_relationship_unique_constraint(
                    ConstraintId(parse_u32(raw_id, "relationship unique constraint id")?),
                    RelTypeId(parse_u32(
                        raw_rel_type_id,
                        "relationship unique constraint rel type id",
                    )?),
                    decode_string(raw_property)?,
                );
            }
            ["stat_commit_epoch", raw] => {
                state.saw_checkpoint_statistics = true;
                let epoch = parse_u64(raw, "statistics commit epoch")?;
                state.basic_statistics.computed_at_commit_epoch = epoch;
                state.checkpoint_statistics.computed_at_commit_epoch = epoch;
            }
            ["stat_advanced_complete", raw] => {
                if state.statistics_complete.is_some() {
                    return Err(SkeinError::Storage(
                        "checkpoint contains duplicate statistics completeness flag".to_string(),
                    ));
                }
                state.statistics_complete =
                    Some(decode_bool(raw, "statistics advanced completeness flag")?);
            }
            ["stat_histogram_sample_limit", raw] => {
                state.checkpoint_statistics.histogram_sample_limit =
                    parse_usize(raw, "statistics histogram sample limit")?;
            }
            ["stat_node_count", raw] => {
                let count = parse_u64(raw, "statistics node count")?;
                state.basic_statistics.node_count = count;
                state.checkpoint_statistics.node_count = count;
            }
            ["stat_relationship_count", raw] => {
                let count = parse_u64(raw, "statistics relationship count")?;
                state.basic_statistics.relationship_count = count;
                state.checkpoint_statistics.relationship_count = count;
            }
            ["stat_label_count", raw_label_id, raw_count] => {
                let label_id = LabelId(parse_u32(raw_label_id, "statistics label id")?);
                let count = parse_u64(raw_count, "statistics label count")?;
                state.basic_statistics.label_counts.insert(label_id, count);
                state
                    .checkpoint_statistics
                    .label_counts
                    .insert(label_id, count);
            }
            ["stat_rel_type_count", raw_rel_type_id, raw_count] => {
                let rel_type_id = RelTypeId(parse_u32(
                    raw_rel_type_id,
                    "statistics relationship type id",
                )?);
                let count = parse_u64(raw_count, "statistics relationship type count")?;
                state
                    .basic_statistics
                    .rel_type_counts
                    .insert(rel_type_id, count);
                state
                    .checkpoint_statistics
                    .rel_type_counts
                    .insert(rel_type_id, count);
            }
            ["stat_rel_type_source_count", raw_rel_type_id, raw_count] => {
                state.checkpoint_statistics.rel_type_source_counts.insert(
                    RelTypeId(parse_u32(
                        raw_rel_type_id,
                        "statistics relationship type id",
                    )?),
                    parse_u64(raw_count, "statistics relationship source count")?,
                );
            }
            ["stat_rel_type_target_count", raw_rel_type_id, raw_count] => {
                state.checkpoint_statistics.rel_type_target_counts.insert(
                    RelTypeId(parse_u32(
                        raw_rel_type_id,
                        "statistics relationship type id",
                    )?),
                    parse_u64(raw_count, "statistics relationship target count")?,
                );
            }
            ["stat_path_count", raw_source, raw_rel_type, raw_target, raw_count] => {
                state.checkpoint_statistics.path_counts.insert(
                    parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                    parse_u64(raw_count, "statistics path count")?,
                );
            }
            ["stat_path_source_distinct_count", raw_source, raw_rel_type, raw_target, raw_count] => {
                state
                    .checkpoint_statistics
                    .path_source_distinct_counts
                    .insert(
                        parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                        parse_u64(raw_count, "statistics path source distinct count")?,
                    );
            }
            ["stat_path_target_distinct_count", raw_source, raw_rel_type, raw_target, raw_count] => {
                state
                    .checkpoint_statistics
                    .path_target_distinct_counts
                    .insert(
                        parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                        parse_u64(raw_count, "statistics path target distinct count")?,
                    );
            }
            ["stat_bounded_path_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
            {
                state.checkpoint_statistics.bounded_path_counts.insert(
                    parse_statistics_bounded_path_key(
                        raw_source,
                        raw_rel_type,
                        raw_target,
                        raw_hops,
                    )?,
                    parse_u64(raw_count, "statistics bounded path count")?,
                );
            }
            ["stat_bounded_path_source_distinct_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
            {
                state
                    .checkpoint_statistics
                    .bounded_path_source_distinct_counts
                    .insert(
                        parse_statistics_bounded_path_key(
                            raw_source,
                            raw_rel_type,
                            raw_target,
                            raw_hops,
                        )?,
                        parse_u64(raw_count, "statistics bounded path source distinct count")?,
                    );
            }
            ["stat_bounded_path_target_distinct_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
            {
                state
                    .checkpoint_statistics
                    .bounded_path_target_distinct_counts
                    .insert(
                        parse_statistics_bounded_path_key(
                            raw_source,
                            raw_rel_type,
                            raw_target,
                            raw_hops,
                        )?,
                        parse_u64(raw_count, "statistics bounded path target distinct count")?,
                    );
            }
            ["stat_index_sample", raw_index_id, raw_index_size, raw_unique_values, raw_sample_size, raw_updates] =>
            {
                state.checkpoint_statistics.index_samples.insert(
                    IndexId(parse_u32(raw_index_id, "statistics index id")?),
                    IndexStatisticsSample {
                        index_size: parse_u64(raw_index_size, "statistics index size")?,
                        unique_values: parse_u64(
                            raw_unique_values,
                            "statistics index unique values",
                        )?,
                        sample_size: parse_u64(raw_sample_size, "statistics index sample size")?,
                        updates_since_sample: parse_u64(raw_updates, "statistics index updates")?,
                    },
                );
            }
            ["stat_property_distinct_count", raw_label_id, raw_property, raw_count] => {
                state.checkpoint_statistics.property_distinct_counts.insert(
                    (
                        LabelId(parse_u32(raw_label_id, "statistics label id")?),
                        decode_string(raw_property)?,
                    ),
                    parse_u64(raw_count, "statistics property distinct count")?,
                );
            }
            ["stat_rel_property_distinct_count", raw_rel_type_id, raw_property, raw_count] => {
                state
                    .checkpoint_statistics
                    .rel_property_distinct_counts
                    .insert(
                        (
                            RelTypeId(parse_u32(
                                raw_rel_type_id,
                                "statistics relationship type id",
                            )?),
                            decode_string(raw_property)?,
                        ),
                        parse_u64(raw_count, "statistics relationship property distinct count")?,
                    );
            }
            ["stat_rel_property_histogram", raw_rel_type_id, raw_property, raw_values] => {
                state.checkpoint_statistics.rel_property_histograms.insert(
                    (
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "statistics relationship type id",
                        )?),
                        decode_string(raw_property)?,
                    ),
                    decode_value_vec(raw_values)?,
                );
            }
            ["stat_rel_property_histogram_sampled", raw_rel_type_id, raw_property, raw_sampled] => {
                state
                    .checkpoint_statistics
                    .sampled_rel_property_histograms
                    .insert(
                        (
                            RelTypeId(parse_u32(
                                raw_rel_type_id,
                                "statistics relationship type id",
                            )?),
                            decode_string(raw_property)?,
                        ),
                        decode_bool(raw_sampled, "statistics sampled flag")?,
                    );
            }
            ["stat_property_histogram", raw_label_id, raw_property, raw_values] => {
                state.checkpoint_statistics.property_histograms.insert(
                    (
                        LabelId(parse_u32(raw_label_id, "statistics label id")?),
                        decode_string(raw_property)?,
                    ),
                    decode_value_vec(raw_values)?,
                );
            }
            ["stat_property_histogram_sampled", raw_label_id, raw_property, raw_sampled] => {
                state
                    .checkpoint_statistics
                    .sampled_property_histograms
                    .insert(
                        (
                            LabelId(parse_u32(raw_label_id, "statistics label id")?),
                            decode_string(raw_property)?,
                        ),
                        decode_bool(raw_sampled, "statistics sampled flag")?,
                    );
            }
            ["project_graph", raw_name, raw_node_labels, raw_rel_types] => {
                state.projected_graphs.insert(
                    decode_string(raw_name)?,
                    ProjectedGraphDefinition {
                        node_labels: decode_string_vec(raw_node_labels)?,
                        rel_types: decode_string_vec(raw_rel_types)?,
                    },
                );
            }
            ["node", raw_id, raw_labels, raw_properties] => {
                let id = NodeId(parse_u64(raw_id, "node id")?);
                let labels = parse_label_set(raw_labels)?;
                let properties = decode_properties(raw_properties)?;
                state.nodes.push(NodeRecord {
                    id,
                    labels,
                    properties,
                });
            }
            ["rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                state.relationships.push(RelRecord {
                    id: RelId(parse_u64(raw_id, "rel id")?),
                    source: NodeId(parse_u64(raw_source, "rel source")?),
                    target: NodeId(parse_u64(raw_target, "rel target")?),
                    rel_type: RelTypeId(parse_u32(raw_type, "rel type")?),
                    properties: decode_properties(raw_properties)?,
                });
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid checkpoint line: {line}"
                )));
            }
        }
    }
    if !saw_storage_version {
        return Err(SkeinError::Storage(
            "checkpoint is missing its storage version".to_string(),
        ));
    }
    Ok(())
}
