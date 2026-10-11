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
use crate::projection::ProjectedRelationshipPredicate;
use crate::text::{
    decode_string, decode_string_vec, decode_u64_vec, encode_string, encode_string_vec,
};
use hawdb_core::Value;

fn checkpoint_unit_scheduler() -> hawdb_qos::LocalQosScheduler {
    hawdb_qos::LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..hawdb_qos::LocalQosPolicy::default()
    })
}

#[test]
fn checkpoint_units_projected_graph_codec_preserves_all_arrays_and_v1_bytes() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let scheduler = checkpoint_unit_scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let edges = (0..2048usize)
        .flat_map(|id| [(id, (id + 1) % 2048), (id, id)])
        .collect::<Vec<_>>();
    let data = edge_bag_data((0..2048).map(NodeId).collect(), &edges);
    let definition = definition();
    let name = "投影\t🦀";
    let encoded = encode_projected_graph_artifacts_with_work_context(
        19,
        23,
        [Ok((name, &definition, data.clone()))],
        &work,
    )
    .unwrap();
    assert_eq!(encoded, reference_body(name, &definition, &data, 19, 23));
    let (epoch, mut recovered) =
        decode_projected_graph_artifacts_with_work_context(&encoded, &work).unwrap();
    assert_eq!(epoch, 23);
    let artifact = recovered.remove(name).unwrap();
    assert!(recovered.is_empty());
    assert_eq!(artifact.projection_epoch, 19);
    assert_eq!(artifact.commit_epoch, 23);
    assert_eq!(artifact.definition, definition);
    assert_eq!(artifact.data, data);
    assert!(probe.completed.load(Ordering::SeqCst) > 20);
    assert!(probe.peak_units.load(Ordering::SeqCst) <= 4);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_projected_graph_codec_cancels_inside_numeric_arrays() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let values = (0..4096u64).collect::<Vec<_>>();
    let encoded = values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    for decoding in [false, true] {
        let scheduler = checkpoint_unit_scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let limit = 2;
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let error = if decoding {
            decode_number_vector(&encoded, &work, |value| {
                parse_u64(value, "test numeric value")
            })
            .unwrap_err()
        } else {
            append_number_vector(
                &mut CheckpointProjectedGraphText::new(),
                "nodes",
                values.iter().copied(),
                &work,
            )
            .unwrap_err()
        };
        assert_eq!(
            error,
            HawDBError::Storage("checkpoint build stopped: cancelled".into())
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&scheduler);
    }
}

#[test]
fn checkpoint_units_projected_graph_validates_chunk_boundary_offsets_and_indexes() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::Arc;

    let data = edge_bag_data(
        (0..2048).map(NodeId).collect(),
        &(0..2048usize).map(|id| (id, id)).collect::<Vec<_>>(),
    );
    for invalid_offsets in [true, false] {
        let scheduler = checkpoint_unit_scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(scheduler.clone());
        let mut bad = data.clone();
        if invalid_offsets {
            // Each chunk remains locally monotonic; the inversion crosses
            // the boundary between the first and second validation chunks.
            bad.csr_offsets[1024] = 1022;
        } else {
            bad.csr_targets[1024] = 2048;
        }
        let error = ProjectedGraphArtifactData::new_with_work_context(
            bad.nodes,
            bad.csr_offsets,
            bad.csr_targets,
            bad.csc_offsets,
            bad.csc_sources,
            &work,
        )
        .unwrap_err();
        assert_eq!(
            error,
            HawDBError::Storage(if invalid_offsets {
                "invalid projected graph csr_offsets".into()
            } else {
                "projected graph csr_targets contains an out-of-range node index".into()
            })
        );
        probe.assert_released(&scheduler);
    }
}

const V1_FIXTURE: &str = concat!(
    "HAWDB_PROJECTED_GRAPHS_V1\n",
    "artifact_version\t1\n",
    "projection_epoch\t7\n",
    "commit_epoch\t11\n",
    "graph\t47\t4d656d6f7279:456e74697479\t4c494e4b53\t2\t2\n",
    "nodes\t4,9\n",
    "csr_offsets\t0,1,2\n",
    "csr_targets\t1,1\n",
    "csc_offsets\t0,0,2\n",
    "csc_sources\t0,1\n",
);

const V2_FIXTURE: &str = concat!(
    "HAWDB_PROJECTED_GRAPHS_V1\n",
    "artifact_version\t2\n",
    "projection_epoch\t7\n",
    "commit_epoch\t11\n",
    "graph\t47\t4d656d6f7279:456e74697479\t4c494e4b53\tm\t2\t2\n",
    "nodes\t4,9\n",
    "csr_offsets\t0,1,2\n",
    "csr_targets\t1,1\n",
    "csc_offsets\t0,0,2\n",
    "csc_sources\t0,1\n",
);

fn definition() -> ProjectedGraphDefinition {
    ProjectedGraphDefinition {
        node_labels: vec!["Memory".into(), "Entity".into()],
        rel_types: vec!["LINKS".into()],
        relationship_predicates: BTreeMap::new(),
    }
}

// Build the two adjacency views directly from an edge bag, independently of the
// production graph constructor and artifact validator.
fn edge_bag_data(nodes: Vec<NodeId>, edges: &[(usize, usize)]) -> ProjectedGraphArtifactData {
    let mut csr_offsets = vec![0];
    let mut csr_targets = Vec::new();
    let mut csc_offsets = vec![0];
    let mut csc_sources = Vec::new();
    for index in 0..nodes.len() {
        for &(source, target) in edges {
            if source == index {
                csr_targets.push(target);
            }
            if target == index {
                csc_sources.push(source);
            }
        }
        csr_offsets.push(csr_targets.len());
        csc_offsets.push(csc_sources.len());
    }
    ProjectedGraphArtifactData {
        nodes,
        csr_offsets,
        csr_targets,
        csc_offsets,
        csc_sources,
    }
}

fn reference_hex(value: &str) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut result = String::new();
    for byte in value.bytes() {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}

fn reference_body(
    name: &str,
    definition: &ProjectedGraphDefinition,
    data: &ProjectedGraphArtifactData,
    projection_epoch: u64,
    commit_epoch: u64,
) -> String {
    // Delimit fields and rows independently; do not use the production codec or
    // list helpers to derive the expected bytes.
    let mut rows = vec![
        vec!["HAWDB_PROJECTED_GRAPHS_V1".into()],
        vec!["artifact_version".into(), "2".into()],
        vec!["projection_epoch".into(), projection_epoch.to_string()],
        vec!["commit_epoch".into(), commit_epoch.to_string()],
        vec![
            "graph".into(),
            reference_hex(name),
            definition
                .node_labels
                .iter()
                .map(|s| reference_hex(s))
                .collect::<Vec<_>>()
                .join(":"),
            definition
                .rel_types
                .iter()
                .map(|s| reference_hex(s))
                .collect::<Vec<_>>()
                .join(":"),
            "m".into(),
            data.nodes.len().to_string(),
            data.csr_targets.len().to_string(),
        ],
        vec![
            "nodes".into(),
            data.nodes
                .iter()
                .map(|n| n.0.to_string())
                .collect::<Vec<_>>()
                .join(","),
        ],
    ];
    for (label, values) in [
        ("csr_offsets", &data.csr_offsets),
        ("csr_targets", &data.csr_targets),
        ("csc_offsets", &data.csc_offsets),
        ("csc_sources", &data.csc_sources),
    ] {
        rows.push(vec![
            label.into(),
            values
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ]);
    }
    let mut text = String::new();
    for row in rows {
        text.push_str(&row.join("\t"));
        text.push('\n');
    }
    text
}

fn encode_one(
    name: &str,
    definition: &ProjectedGraphDefinition,
    data: &ProjectedGraphArtifactData,
) -> String {
    encode_projected_graph_artifacts(7, 11, [(name, definition, data.clone())])
}

fn assert_storage_error<T: std::fmt::Debug>(result: Result<T>, expected: &str) {
    match result {
        Err(HawDBError::Storage(message)) => assert_eq!(message, expected),
        other => panic!("expected storage error {expected:?}, got {other:?}"),
    }
}

fn replace_line(body: &str, index: usize, line: &str) -> String {
    let mut lines = body.lines().collect::<Vec<_>>();
    lines[index] = line;
    lines.join("\n") + "\n"
}

fn replace_graph_field(body: &str, field: usize, value: &str) -> String {
    let mut fields = body.lines().nth(4).unwrap().split('\t').collect::<Vec<_>>();
    fields[field] = value;
    replace_line(body, 4, &fields.join("\t"))
}

#[test]
fn frozen_v1_reads_and_v2_writes_roundtrip() {
    let data = edge_bag_data(vec![NodeId(4), NodeId(9)], &[(0, 1), (1, 1)]);
    assert_eq!(encode_one("G", &definition(), &data), V2_FIXTURE);
    assert_eq!(
        decode_projected_graph_artifacts(V2_FIXTURE).unwrap(),
        (
            11,
            BTreeMap::from([(
                "G".into(),
                ProjectedGraphArtifact {
                    projection_epoch: 7,
                    commit_epoch: 11,
                    definition: definition(),
                    data
                },
            )])
        )
    );
    assert_eq!(
        decode_projected_graph_artifacts(V1_FIXTURE).unwrap(),
        decode_projected_graph_artifacts(V2_FIXTURE).unwrap()
    );
    assert_eq!(
        encode_projected_graph_artifacts(0, u64::MAX, []),
        "HAWDB_PROJECTED_GRAPHS_V1\nartifact_version\t2\nprojection_epoch\t0\ncommit_epoch\t18446744073709551615\n",
    );
    for count in [0, 1, 4096] {
        let data = edge_bag_data((0..count).map(NodeId).collect(), &[]);
        let empty = ProjectedGraphDefinition {
            node_labels: vec![],
            rel_types: vec![],
            relationship_predicates: BTreeMap::new(),
        };
        let text = encode_one("", &empty, &data);
        let (_, decoded) = decode_projected_graph_artifacts(&text).unwrap();
        assert_eq!(decoded[""].data, data);
        assert_eq!(decoded[""].definition, empty);
    }
}

#[test]
fn relationship_predicates_round_trip_through_v2_artifacts() {
    let data = edge_bag_data(vec![NodeId(4), NodeId(9)], &[(0, 1)]);
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Entity".into()],
        rel_types: vec!["RELATES_TO".into()],
        relationship_predicates: BTreeMap::from([(
            "RELATES_TO".into(),
            ProjectedRelationshipPredicate::And(vec![
                ProjectedRelationshipPredicate::Gte {
                    property: "confidence".into(),
                    value: Value::Float(0.7),
                },
                ProjectedRelationshipPredicate::Gte {
                    property: "strength".into(),
                    value: Value::Float(0.5),
                },
            ]),
        )]),
    };

    let text = encode_one("EntityTopicGraph", &definition, &data);
    let (_, decoded) = decode_projected_graph_artifacts(&text).unwrap();

    assert_eq!(decoded["EntityTopicGraph"].definition, definition);
    assert_eq!(decoded["EntityTopicGraph"].data, data);
}

#[test]
fn decoding_tolerance_and_input_order_are_preserved() {
    let data = edge_bag_data(vec![NodeId(4), NodeId(9)], &[(0, 1), (1, 1)]);
    let def = definition();
    let text = encode_projected_graph_artifacts(
        7,
        11,
        [("z", &def, data.clone()), ("a", &def, data.clone())],
    );
    assert!(text.find("graph\t7a\t").unwrap() < text.find("graph\t61\t").unwrap());
    assert_eq!(decode_projected_graph_artifacts(&text).unwrap().1.len(), 2);
    for text in [
        V2_FIXTURE.replace('\n', "\r\n"),
        format!("{V2_FIXTURE}\n\n"),
        V2_FIXTURE.trim_end_matches('\n').into(),
        V2_FIXTURE.replace("artifact_version\t2", "artifact_version\t+002"),
        V2_FIXTURE.replace("nodes\t4,9", "nodes\t+004,09"),
        V2_FIXTURE.replace("4d656d6f7279", "4D656D6F7279"),
    ] {
        assert_eq!(
            decode_projected_graph_artifacts(&text).unwrap(),
            decode_projected_graph_artifacts(V2_FIXTURE).unwrap()
        );
    }
    let replacement = edge_bag_data(vec![NodeId(u64::MAX)], &[]);
    let text = encode_projected_graph_artifacts(
        7,
        11,
        [("G", &def, data.clone()), ("G", &def, replacement.clone())],
    );
    let (_, decoded) = decode_projected_graph_artifacts(&text).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded["G"].data, replacement);

    // Migration must not tighten the previous constructor's contract: node IDs
    // can repeat, and structurally valid CSR/CSC views need not describe the same
    // edges. A separate format change would be needed to reject these inputs.
    let mut tolerated = data;
    tolerated.nodes = vec![NodeId(9), NodeId(9)];
    tolerated.csc_sources = vec![1, 1];
    assert_eq!(
        decode_projected_graph_artifacts(&encode_one("G", &def, &tolerated))
            .unwrap()
            .1["G"]
            .data,
        tolerated
    );
    assert_eq!(
        decode_string_vec(&encode_string_vec(&[String::new()])).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(decode_string_vec(":").unwrap(), vec!["", ""]);
    assert_eq!(
        decode_u64_vec("+000,18446744073709551615", "ids").unwrap(),
        vec![0, u64::MAX]
    );
}

#[test]
fn malformed_headers_fields_and_structure_report_original_errors() {
    let headers = [
        ("", "invalid projected graph artifact header"),
        (
            "HAWDB_PROJECTED_GRAPHS_V1",
            "missing projected graph artifact artifact_version",
        ),
        (
            "HAWDB_PROJECTED_GRAPHS_V1\nartifact_version\t1",
            "missing projected graph artifact projection_epoch",
        ),
        (
            "HAWDB_PROJECTED_GRAPHS_V1\nartifact_version\t1\nprojection_epoch\t7",
            "missing projected graph artifact commit_epoch",
        ),
    ];
    for (text, expected) in headers {
        assert_storage_error(decode_projected_graph_artifacts(text), expected);
    }
    for (index, label) in [
        (5, "nodes"),
        (6, "csr_offsets"),
        (7, "csr_targets"),
        (8, "csc_offsets"),
        (9, "csc_sources"),
    ] {
        let truncated = V2_FIXTURE
            .lines()
            .take(index)
            .collect::<Vec<_>>()
            .join("\n");
        assert_storage_error(
            decode_projected_graph_artifacts(&truncated),
            &format!("missing projected graph artifact {label} line"),
        );
        let malformed = replace_line(V2_FIXTURE, index, "unknown\t0");
        assert_storage_error(
            decode_projected_graph_artifacts(&malformed),
            "invalid projected graph artifact line: unknown\t0",
        );
    }
    for (index, raw, expected) in [
        (
            1,
            "artifact_version\t3",
            "unsupported projected graph artifact version: 3",
        ),
        (
            1,
            "artifact_version\t-1",
            "invalid projected graph artifact version: -1",
        ),
        (
            2,
            "projection_epoch\t18446744073709551616",
            "invalid projected graph artifact projection epoch: 18446744073709551616",
        ),
        (
            3,
            "commit_epoch\tbad",
            "invalid projected graph artifact commit epoch: bad",
        ),
        (5, "nodes\t4,", "invalid projected graph artifact node id: "),
        (
            6,
            "csr_offsets\t0,,2",
            "invalid projected graph artifact index: ",
        ),
        (
            6,
            "csr_offsets\t0,3,2",
            "invalid projected graph csr_offsets",
        ),
        (6, "csr_offsets\t0,2", "invalid projected graph csr_offsets"),
        (
            8,
            "csc_offsets\t0,2,1",
            "invalid projected graph csc_offsets",
        ),
        (
            7,
            "csr_targets\t2,1",
            "projected graph csr_targets contains an out-of-range node index",
        ),
        (
            9,
            "csc_sources\t0,2",
            "projected graph csc_sources contains an out-of-range node index",
        ),
    ] {
        assert_storage_error(
            decode_projected_graph_artifacts(&replace_line(V2_FIXTURE, index, raw)),
            expected,
        );
    }
    for (field, raw, expected) in [
        (5, "3", "projected graph artifact node count mismatch for G"),
        (6, "3", "projected graph artifact edge count mismatch for G"),
        (5, "-1", "invalid projected graph artifact node count: -1"),
        (6, "bad", "invalid projected graph artifact edge count: bad"),
    ] {
        assert_storage_error(
            decode_projected_graph_artifacts(&replace_graph_field(V2_FIXTURE, field, raw)),
            expected,
        );
    }
    // Field parsing precedes count checks, and count checks precede structural
    // validation. Duplicate graph names cannot hide a malformed earlier entry.
    let count_and_offsets =
        replace_graph_field(&replace_line(V2_FIXTURE, 6, "csr_offsets\t1"), 5, "3");
    assert_storage_error(
        decode_projected_graph_artifacts(&count_and_offsets),
        "projected graph artifact node count mismatch for G",
    );
    let bad_index = replace_line(&count_and_offsets, 7, "csr_targets\tbad");
    assert_storage_error(
        decode_projected_graph_artifacts(&bad_index),
        "invalid projected graph artifact index: bad",
    );
    let duplicate = count_and_offsets + &V2_FIXTURE.lines().skip(4).collect::<Vec<_>>().join("\n");
    assert_storage_error(
        decode_projected_graph_artifacts(&duplicate),
        "projected graph artifact node count mismatch for G",
    );
}

#[test]
fn checksum_footer_split_preserves_last_match_and_whitespace() {
    assert_eq!(
        split_projected_graph_artifact_checksum("bodychecksum\t +003 \n").unwrap(),
        ("body", 3)
    );
    assert_eq!(
        split_projected_graph_artifact_checksum("checksum\t1\nchecksum\t2\n").unwrap(),
        ("checksum\t1\n", 2)
    );
    assert_storage_error(
        split_projected_graph_artifact_checksum(V2_FIXTURE),
        "projected graph artifact missing checksum footer",
    );
    assert_storage_error(
        split_projected_graph_artifact_checksum("checksum\t-1\n"),
        "invalid projected graph artifact checksum: -1",
    );
    assert_storage_error(
        split_projected_graph_artifact_checksum("checksum\t1\ntrailing"),
        "invalid projected graph artifact checksum: 1\ntrailing",
    );
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn run_campaign(seeds: u64, steps: usize) -> usize {
    let mut checks = 0;
    for seed in 0..seeds {
        let mut state = seed;
        for step in 0..steps {
            let count = 1 + (next_random(&mut state) % 12) as usize;
            let nodes = (0..count)
                .map(|i| NodeId(u64::MAX - i as u64 * 3))
                .collect();
            let mut edges = Vec::new();
            for source in 0..count {
                for target in 0..count {
                    for _ in 0..next_random(&mut state) % 3 {
                        edges.push((source, target));
                    }
                }
            }
            let data = edge_bag_data(nodes, &edges);
            let name = format!("graph-{seed}-{step}-\u{65e5}\u{672c}\0:\t\n");
            let def = ProjectedGraphDefinition {
                node_labels: vec!["Memory".into(), format!("label-\u{1f980}-{step}")],
                rel_types: if step % 2 == 0 {
                    vec![]
                } else {
                    vec!["\0:\t\n".into()]
                },
                relationship_predicates: BTreeMap::new(),
            };
            let projection_epoch = next_random(&mut state);
            let commit_epoch = next_random(&mut state);
            let expected = reference_body(&name, &def, &data, projection_epoch, commit_epoch);
            let encoded = encode_projected_graph_artifacts(
                projection_epoch,
                commit_epoch,
                [(name.as_str(), &def, data.clone())],
            );
            assert_eq!(encoded, expected, "seed={seed}, step={step}");
            checks += 1;
            assert_eq!(
                decode_projected_graph_artifacts(&expected).unwrap(),
                (
                    commit_epoch,
                    BTreeMap::from([(
                        name.clone(),
                        ProjectedGraphArtifact {
                            projection_epoch,
                            commit_epoch,
                            definition: def,
                            data: data.clone()
                        },
                    )])
                ),
                "seed={seed}, step={step}"
            );
            checks += 1;
            let mutations = [
                replace_line(&expected, 0, "HAWDB_PROJECTED_GRAPHS_V0"),
                replace_line(&expected, 1, "artifact_version\t3"),
                replace_line(&expected, 2, "projection_epoch\t-1"),
                replace_line(&expected, 3, "commit_epoch\t18446744073709551616"),
                replace_graph_field(&expected, 5, &(count + 1).to_string()),
                replace_graph_field(&expected, 6, &(edges.len() + 1).to_string()),
                replace_line(&expected, 6, "csr_offsets\t1"),
                replace_line(&expected, 8, "csc_offsets\t1"),
                replace_line(&expected, 5, "nodes\t18446744073709551616"),
                replace_line(&expected, 7, "csr_targets\t-1"),
                replace_line(&expected, 9, "csc_sources\t-1"),
                replace_graph_field(&expected, 1, "0g"),
                expected.lines().take(9).collect::<Vec<_>>().join("\n"),
                format!("{expected}unknown\t0\n"),
            ];
            for (mutation, text) in mutations.into_iter().enumerate() {
                assert!(
                    matches!(
                        decode_projected_graph_artifacts(&text),
                        Err(HawDBError::Storage(_))
                    ),
                    "accepted mutation={mutation}, seed={seed}, step={step}"
                );
                checks += 1;
            }
        }
    }
    checks
}

#[test]
fn projected_artifact_differential_smoke() {
    assert_eq!(run_campaign(4, 16), 1024);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn projected_artifact_differential_campaign() {
    let checks = run_campaign(128, 64);
    assert_eq!(checks, 131_072);
    println!("projected artifact campaign: 128 seeds, 64 steps, {checks} checks");
}

#[test]
fn checkpoint_units_projected_definitions_preserve_large_unicode_and_empty_names() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::Arc;
    let name = format!("{}🦀{}", "a".repeat(65535), "z".repeat(65536));
    let definition = ProjectedGraphDefinition {
        node_labels: vec![String::new(), name.clone(), "nul\0:tab\t".to_string()],
        rel_types: (0..4096).map(|index| format!("type-{index}")).collect(),
        relationship_predicates: Default::default(),
    };
    let data = edge_bag_data(Vec::new(), &[]);
    let scheduler = checkpoint_unit_scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let encoded = encode_projected_graph_artifacts_with_work_context(
        37,
        41,
        [Ok((name.as_str(), &definition, data.clone()))],
        &work,
    )
    .unwrap();
    assert_eq!(encoded, reference_body(&name, &definition, &data, 37, 41));
    let (epoch, mut recovered) =
        decode_projected_graph_artifacts_with_work_context(&encoded, &work).unwrap();
    assert_eq!(epoch, 41);
    let artifact = recovered.remove(&name).unwrap();
    assert!(recovered.is_empty());
    assert_eq!(artifact.definition, definition);
    assert_eq!(artifact.data, data);
    assert_eq!(probe.io_waves.load(std::sync::atomic::Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
    // Preserve the V1 ambiguity between no names and one empty name.
    for names in [
        Vec::new(),
        vec![String::new()],
        vec![String::new(), String::new()],
    ] {
        let mut encoded = CheckpointProjectedGraphText::new();
        append_projected_name_list(&mut encoded, &names, &work).unwrap();
        assert_eq!(encoded, format!("\t{}", encode_string_vec(&names)));
        assert_eq!(
            decode_projected_name_list(&encoded[1..], &work).unwrap(),
            decode_string_vec(&encoded[1..]).unwrap()
        );
    }
}

#[test]
fn checkpoint_units_projected_definition_names_cancel_and_preserve_legacy_errors() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    let name = "🦀".repeat(50000);
    let encoded = encode_string(&name);
    for decoding in [false, true] {
        let scheduler = checkpoint_unit_scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let error = if decoding {
            decode_projected_name(&encoded, &work).unwrap_err()
        } else {
            append_projected_name(&mut CheckpointProjectedGraphText::new(), &name, &work)
                .unwrap_err()
        };
        assert_eq!(
            error,
            HawDBError::Storage("checkpoint build stopped: cancelled".to_string())
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        probe.assert_released(&scheduler);
    }
    let scheduler = checkpoint_unit_scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    for input in [
        "".to_string(),
        "0".to_string(),
        "gg".to_string(),
        "é".to_string(),
        "+f".to_string(),
        "FF".to_string(),
        "c0af".to_string(),
        "f09f".to_string(),
        format!("{}c0af", "61".repeat(65535)),
        format!("{}e282", "61".repeat(65535)),
        format!("{}f09fa680", "61".repeat(65535)),
    ] {
        assert_eq!(
            decode_projected_name(&input, &work),
            decode_string(&input),
            "input length {}",
            input.len()
        );
    }
    let names = (0..1025)
        .map(|index| format!("label-{index}"))
        .collect::<Vec<_>>();
    for decoding in [false, true] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(7, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let error = if decoding {
            decode_projected_name_list(&encode_string_vec(&names), &work).unwrap_err()
        } else {
            append_projected_name_list(&mut CheckpointProjectedGraphText::new(), &names, &work)
                .unwrap_err()
        };
        assert!(error.to_string().contains("checkpoint build stopped"));
        assert_eq!(probe.completed.load(Ordering::SeqCst), 7);
        probe.assert_released(&scheduler);
    }
}

#[test]
fn checkpoint_units_projected_text_boundaries_match_standard_splits() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::Arc;
    let scheduler = checkpoint_unit_scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let mut cases = [
        "",
        "\n",
        "\r",
        "x\r",
        "x\r\n",
        "x\n",
        "x\n\n",
        "x\t\t",
        "🦀\r\n\t終",
    ]
    .map(str::to_string)
    .to_vec();
    for size in [65534, 65535, 65536, 65537, 131071] {
        cases.push(format!("{}🦀\r\n\nend\r", "x".repeat(size)));
        cases.push(format!("{}\r\n", "x".repeat(size)));
        cases.push(format!("{}\t\t🦀\tend\t", "x".repeat(size)));
    }
    for text in cases {
        let mut lines = ProjectedTextLines::new(&text);
        let mut actual = Vec::new();
        while let Some(line) = lines.next(&work).unwrap() {
            actual.push(line);
        }
        assert!(lines.next(&work).unwrap().is_none());
        assert_eq!(
            actual,
            text.lines().collect::<Vec<_>>(),
            "text bytes {}",
            text.len()
        );
        for max_fields in [3, 7] {
            assert_eq!(
                projected_text_fields(&text, max_fields, &work).unwrap(),
                text.split('\t').take(max_fields).collect::<Vec<_>>()
            );
        }
    }
    for fixture in [V1_FIXTURE, V2_FIXTURE] {
        let crlf = fixture.replace('\n', "\r\n");
        let (actual_epoch, actual) =
            decode_projected_graph_artifacts_with_work_context(&crlf, &work).unwrap();
        let (expected_epoch, expected) = decode_projected_graph_artifacts(fixture).unwrap();
        assert_eq!((actual_epoch, &*actual), (expected_epoch, &expected));
    }
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_projected_text_cancels_before_complete_line_or_field() {
    use crate::background::CheckpointWorkProbe;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    let text = format!("{}\n", "x".repeat(256 * 1024));
    for fields in [false, true] {
        let scheduler = checkpoint_unit_scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let error = if fields {
            projected_text_fields(&text, 7, &work).unwrap_err()
        } else {
            ProjectedTextLines::new(&text).next(&work).unwrap_err()
        };
        assert_eq!(
            error,
            HawDBError::Storage("checkpoint build stopped: cancelled".to_string())
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        probe.assert_released(&scheduler);
    }
}
