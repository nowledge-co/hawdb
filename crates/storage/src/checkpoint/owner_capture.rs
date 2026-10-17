// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Checkpoint extension for owner facts. Ordinary legacy encoders remain an
//! independent old-format oracle and confer no owner completeness contract.

use super::*;
use crate::background::CheckpointWorkContext;
use crate::projection::owner_capture::{CapturedProjectionChange, OwnerBeforeV1, OwnerEffectV1};

pub(crate) fn append(
    body: &mut String,
    floor: u64,
    head: u64,
    changes: impl Iterator<Item = impl AsRef<CapturedProjectionChange>>,
    work: &CheckpointWorkContext,
) -> Result<()> {
    if floor > head {
        return Err(HawDBError::Storage(
            "owner capture floor exceeds checkpoint head".into(),
        ));
    }
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    body.push_str(&format!("projection_owner_capture\t2\t{floor}\n"));
    unit.finish();
    for change in changes {
        let change = change.as_ref();
        if change.commit_epoch <= floor {
            continue;
        }
        let owners = change.owners_v1.as_ref().ok_or_else(|| {
            HawDBError::Storage("unqualified projection record above owner capture floor".into())
        })?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        body.push_str(&format!(
            "projection_owner_change_v1\t{}\t{}\n",
            change.commit_epoch,
            owners.len()
        ));
        unit.finish();
        for owner in owners {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let json = serde_json::to_string(owner)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            body.push_str(&format!(
                "projection_owner_effect_v1\t{}\t{}\n",
                change.commit_epoch,
                encode_string(&json)
            ));
            unit.finish();
        }
    }
    Ok(())
}

pub(crate) fn parse(fields: &[&str], state: &mut DecodedCheckpoint) -> Result<()> {
    let invalid =
        || HawDBError::Storage("invalid projection owner capture checkpoint extension".into());
    match fields {
        ["projection_owner_capture", version @ ("1" | "2"), raw_floor] => {
            if state.projection_owner_capture_floor.is_some() {
                return Err(invalid());
            }
            state.projection_owner_capture_floor =
                Some(parse_u64(raw_floor, "owner capture floor")?);
            state.projection_owner_capture_version =
                Some(parse_u64(version, "owner capture version")?);
        }
        ["projection_owner_change_v1", raw_epoch, raw_count] => {
            let epoch = parse_u64(raw_epoch, "owner change epoch")?;
            if state
                .projection_owner_changes
                .last_key_value()
                .is_some_and(|(previous, _)| *previous >= epoch)
            {
                return Err(invalid());
            }
            state
                .projection_owner_changes
                .insert(epoch, (parse_usize(raw_count, "owner count")?, Vec::new()));
        }
        ["projection_owner_effect_v1", raw_epoch, raw_json] => {
            let epoch = parse_u64(raw_epoch, "owner effect epoch")?;
            let Some((last, (expected, owners))) =
                state.projection_owner_changes.last_entry().map(|entry| {
                    let key = *entry.key();
                    (key, entry.into_mut())
                })
            else {
                return Err(invalid());
            };
            if last != epoch || owners.len() >= *expected {
                return Err(invalid());
            }
            let owner: OwnerEffectV1 = serde_json::from_str(&decode_string(raw_json)?)
                .map_err(|error| HawDBError::Storage(format!("invalid owner effect: {error}")))?;
            if owners
                .last()
                .is_some_and(|previous| previous.node_id >= owner.node_id)
            {
                return Err(invalid());
            }
            if let OwnerBeforeV1::Present { labels, .. } = &owner.before
                && !labels.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(invalid());
            }
            owners.push(owner);
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

pub(crate) fn validate(state: &DecodedCheckpoint) -> Result<()> {
    let invalid =
        || HawDBError::Storage("incomplete projection owner capture checkpoint contract".into());
    let Some(floor) = state.projection_owner_capture_floor else {
        return if state.projection_owner_changes.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        };
    };
    if !matches!(state.projection_owner_capture_version, Some(1 | 2)) {
        return Err(invalid());
    }
    let head = state.commit_epoch.ok_or_else(invalid)?;
    let legacy_floor = state
        .search_projection_change_log_start_epoch
        .ok_or_else(|| {
            HawDBError::Storage(
                "checkpoint search projection changes are missing their start epoch".into(),
            )
        })?;
    if floor < legacy_floor || floor > head {
        return Err(invalid());
    }
    let mut owners = state.projection_owner_changes.iter();
    for legacy in &state.search_projection_graph_changes {
        if legacy.commit_epoch <= floor {
            continue;
        }
        let Some((epoch, (expected, effects))) = owners.next() else {
            return Err(invalid());
        };
        if *epoch != legacy.commit_epoch || *expected != effects.len() {
            return Err(invalid());
        }
        for node in &legacy.upsert_node_ids {
            if effects
                .binary_search_by_key(node, |owner| owner.node_id)
                .is_err()
            {
                return Err(invalid());
            }
        }
    }
    if owners.next().is_some() {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::owner_capture::OwnerIdV1;
    use crate::store::search_projection_change_log::SearchProjectionChangeLog;
    use std::sync::Arc;

    fn legacy_body() -> String {
        format!("{CHECKPOINT_HEADER_V1}\nversion\t{STORAGE_VERSION}\ngeneration\t1\ncommit_epoch\t3\nnext_node_id\t1\nnext_rel_id\t0\nsearch_projection_change_log_start_epoch\t0\nsearch_projection_change\t1\t1\t\texact\t\nsearch_projection_change\t3\t\t\texact\t\n")
    }
    fn decode(body: &str) -> Result<DecodedCheckpoint> {
        let mut state = DecodedCheckpoint::default();
        parse_checkpoint(body, &mut Catalog::default(), &mut state)?;
        Ok(state)
    }
    fn record(epoch: u64, owners: Vec<OwnerEffectV1>) -> Arc<CapturedProjectionChange> {
        Arc::new(CapturedProjectionChange {
            legacy: Arc::new(SearchProjectionGraphChange {
                commit_epoch: epoch,
                upsert_node_ids: if epoch == 1 { vec![1] } else { vec![] },
                delete_document_ids: vec![],
                relational_primary_key_changes: RelationalPrimaryKeyChangeCapture::Captured {
                    tables: vec![],
                    encoded_bytes: 0,
                },
            }),
            owners_v1: Some(owners),
        })
    }
    fn native_body() -> String {
        let records = [
            record(
                1,
                vec![OwnerEffectV1 {
                    node_id: 1,
                    before: OwnerBeforeV1::Absent,
                }],
            ),
            record(
                3,
                vec![OwnerEffectV1 {
                    node_id: 1,
                    before: OwnerBeforeV1::Present {
                        labels: vec![1, 2],
                        id: OwnerIdV1::String("id\t\nλ".into()),
                    },
                }],
            ),
        ];
        let mut body = legacy_body();
        append(
            &mut body,
            0,
            3,
            records.iter(),
            &CheckpointWorkContext::default(),
        )
        .unwrap();
        body
    }
    #[test]
    fn legacy_checkpoint_keeps_canonical_head_but_cannot_certify_old_owner_history() {
        let mut state = decode(&legacy_body()).unwrap();
        let mut log = SearchProjectionChangeLog::recover_owner_capture(&mut state, 3);
        assert_eq!(log.owner_floor(), 3);
        assert_eq!(log.iter().count(), 2);
        assert!(log.iter().all(|record| record.owners_v1.is_none()));
        log.push(record(
            4,
            vec![OwnerEffectV1 {
                node_id: 1,
                before: OwnerBeforeV1::Absent,
            }],
        ));
        assert_eq!(log.owner_floor(), 3);
        // A subsequent new checkpoint must preserve the fence, even though its
        // retained legacy records predate the new contract.
        let mut body = legacy_body().replace("commit_epoch\t3", "commit_epoch\t4");
        body.push_str("search_projection_change\t4\t\t\texact\t\n");
        append(
            &mut body,
            log.owner_floor(),
            4,
            log.iter(),
            &CheckpointWorkContext::default(),
        )
        .unwrap();
        let mut recovered = decode(&body).unwrap();
        let next = SearchProjectionChangeLog::recover_owner_capture(&mut recovered, 4);
        assert_eq!(next.owner_floor(), 3);
        assert!(next.last().unwrap().owners_v1.is_some());
    }
    #[test]
    fn v1_facts_preserve_legacy_history_but_require_a_new_owner_baseline() {
        let body = native_body().replace(
            "projection_owner_capture\t2\t0",
            "projection_owner_capture\t1\t0",
        );
        let mut state = decode(&body).unwrap();
        assert_eq!(state.projection_owner_capture_version, Some(1));
        let legacy = state.search_projection_graph_changes.clone();
        let log = SearchProjectionChangeLog::recover_owner_capture(&mut state, 3);
        assert_eq!(log.owner_floor(), 3);
        assert_eq!(
            log.iter()
                .map(|record| record.legacy.as_ref())
                .collect::<Vec<_>>(),
            legacy.iter().collect::<Vec<_>>()
        );
        assert!(log.iter().all(|record| record.owners_v1.is_none()));
        assert!(state.projection_owner_changes.is_empty());
        let mut upgraded = body
            .lines()
            .filter(|line| !line.starts_with("projection_owner_"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        append(
            &mut upgraded,
            log.owner_floor(),
            3,
            log.iter(),
            &CheckpointWorkContext::default(),
        )
        .unwrap();
        let mut state = decode(&upgraded).unwrap();
        assert_eq!(state.projection_owner_capture_version, Some(2));
        let log = SearchProjectionChangeLog::recover_owner_capture(&mut state, 3);
        assert_eq!(log.owner_floor(), 3);
        assert!(log.iter().all(|record| record.owners_v1.is_none()));
    }

    #[test]
    fn native_checkpoint_round_trip_and_count_fences_reject_missing_or_unknown_facts() {
        let body = native_body();
        let mut state = decode(&body).unwrap();
        let log = SearchProjectionChangeLog::recover_owner_capture(&mut state, 3);
        assert_eq!(log.owner_floor(), 0);
        assert_eq!(
            log.last().unwrap().owners_v1.as_ref().unwrap()[0].before,
            OwnerBeforeV1::Present {
                labels: vec![1, 2],
                id: OwnerIdV1::String("id\t\nλ".into())
            }
        );
        let lines = body.lines().collect::<Vec<_>>();
        for index in lines
            .iter()
            .enumerate()
            .filter_map(|(index, line)| line.starts_with("projection_owner_").then_some(index))
        {
            let mut damaged = lines.clone();
            damaged.remove(index);
            assert!(
                decode(&(damaged.join("\n") + "\n")).is_err(),
                "missing line {index}"
            );
        }
        for damaged in [
            body.replace(
                "projection_owner_capture\t2\t0",
                "projection_owner_capture\t3\t0",
            ),
            body.replace(
                "projection_owner_capture\t2\t0",
                "projection_owner_capture\t2\t4",
            ),
            body.replace(
                "projection_owner_change_v1\t3\t1",
                "projection_owner_change_v1\t3\t0",
            ),
            body.clone() + "projection_owner_capture\t1\t0\n",
            body.clone() + "projection_owner_change_v1\t3\t0\n",
            body.clone() + "projection_owner_change_v1\t4\t0\n",
            body.clone() + "projection_owner_effect_v1\t4\t00\n",
        ] {
            assert!(decode(&damaged).is_err());
        }
        // A zero-owner transaction still has an explicit V1 header.
        let empty = [record(3, vec![])];
        let mut body = legacy_body();
        append(
            &mut body,
            1,
            3,
            empty.iter(),
            &CheckpointWorkContext::default(),
        )
        .unwrap();
        assert!(decode(&body).is_ok());
    }
    #[test]
    fn native_checkpoint_rejects_duplicate_unordered_owners_and_unknown_preimages() {
        let effect = |owner: OwnerEffectV1| {
            format!(
                "projection_owner_effect_v1\t3\t{}\n",
                encode_string(&serde_json::to_string(&owner).unwrap())
            )
        };
        let owner = |node_id| OwnerEffectV1 {
            node_id,
            before: OwnerBeforeV1::Absent,
        };
        for effects in [
            effect(owner(2)) + &effect(owner(2)),
            effect(owner(2)) + &effect(owner(1)),
        ] {
            assert!(decode(
                &(legacy_body()
                    + "projection_owner_capture\t1\t1\nprojection_owner_change_v1\t3\t2\n"
                    + &effects)
            )
            .is_err());
        }
        let unordered = OwnerEffectV1 {
            node_id: 1,
            before: OwnerBeforeV1::Present {
                labels: vec![2, 1],
                id: OwnerIdV1::Absent,
            },
        };
        assert!(decode(
            &(legacy_body()
                + "projection_owner_capture\t1\t1\nprojection_owner_change_v1\t3\t1\n"
                + &effect(unordered))
        )
        .is_err());
        let unknown = encode_string(r#"{"node_id":1,"before":"Unknown"}"#);
        assert!(decode(&(legacy_body() + "projection_owner_capture\t1\t1\nprojection_owner_change_v1\t3\t1\nprojection_owner_effect_v1\t3\t" + &unknown + "\n")).is_err());
    }
}
