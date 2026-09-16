//! Graph checkpoint text encoding.

use crate::text::{
    encode_bool, encode_bytes, encode_index_kind, encode_nullable, encode_property_type,
    encode_schema_object_state, encode_string, encode_string_vec, encode_table_kind,
    encode_u64_vec, encode_value_vec,
};
use crate::{
    artifact_binding::DurableArtifactMetadata, durable_manifest::STORAGE_VERSION,
    encode_relational_primary_key, ProjectedGraphDefinition, RelationalPrimaryKeyChangeCapture,
    RelationalPrimaryKeyChangeRebuildReason, SearchProjectionGraphChange,
};
use skein_core::{Catalog, ConstraintSubject, GraphStatistics, Result, SkeinError, Uuid};
use std::collections::BTreeMap;

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
