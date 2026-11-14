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
use std::io::Cursor;

fn source(with_headers: bool, terminator: u8) -> LoadCsvSource {
    LoadCsvSource {
        location: CsvLocation::Parameter("file".to_string()),
        with_headers,
        field_terminator: terminator,
        variable: "row".to_string(),
    }
}

fn records(input: &[u8], terminator: u8) -> Result<Vec<Vec<CsvField>>> {
    records_with(input, terminator, CsvImportLimits::default())
}

fn records_with(
    input: &[u8],
    terminator: u8,
    limits: CsvImportLimits,
) -> Result<Vec<Vec<CsvField>>> {
    let mut reader = CsvRecordReader::new(Cursor::new(input.to_vec()), terminator, limits)?;
    let mut output = Vec::new();
    while let Some(record) = reader.next_record()? {
        output.push(record);
    }
    Ok(output)
}

fn rows(input: &[u8], with_headers: bool, terminator: u8) -> Result<Vec<Value>> {
    let mut rows = CsvRows::new(
        Cursor::new(input.to_vec()),
        &source(with_headers, terminator),
        CsvImportLimits::default(),
    )?;
    let mut output = Vec::new();
    while let Some(row) = rows.next_row()? {
        output.push(row);
    }
    Ok(output)
}

fn text(value: &str) -> CsvField {
    Some(value.to_string())
}

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

#[test]
fn reads_unquoted_records_with_lf_crlf_and_cr_line_endings() {
    let expected = vec![vec![text("a"), text("b")], vec![text("c"), text("d")]];
    assert_eq!(records(b"a,b\nc,d\n", b',').unwrap(), expected);
    assert_eq!(records(b"a,b\r\nc,d\r\n", b',').unwrap(), expected);
    assert_eq!(records(b"a,b\rc,d", b',').unwrap(), expected);
    assert_eq!(records(b"a,b\nc,d", b',').unwrap(), expected);
}

#[test]
fn quoted_fields_keep_separators_newlines_and_doubled_quotes() {
    let input = b"\"a,b\",\"line1\nline2\",\"say \"\"hi\"\"\"\r\n\"\",x\n";
    assert_eq!(
        records(input, b',').unwrap(),
        vec![
            vec![text("a,b"), text("line1\nline2"), text("say \"hi\"")],
            vec![text(""), text("x")],
        ]
    );
}

#[test]
fn empty_unquoted_fields_are_null_and_quoted_empty_fields_are_strings() {
    assert_eq!(
        records(b",\"\",\n", b',').unwrap(),
        vec![vec![None, text(""), None]]
    );
}

#[test]
fn skips_utf8_bom_and_empty_lines() {
    let input = b"\xEF\xBB\xBFa,b\n\n\r\n\nc,d\n\n";
    assert_eq!(
        records(input, b',').unwrap(),
        vec![vec![text("a"), text("b")], vec![text("c"), text("d")]]
    );
    // A BOM is recognized only at the start of the input.
    assert_eq!(
        records(b"x\n\xEF\xBB\xBFy\n", b',').unwrap(),
        vec![vec![text("x")], vec![text("\u{FEFF}y")]]
    );
    assert!(records(b"", b',').unwrap().is_empty());
    assert!(records(b"\xEF\xBB\xBF", b',').unwrap().is_empty());
}

#[test]
fn custom_field_terminators_split_only_on_that_byte() {
    assert_eq!(
        records(b"1|Ann|a,b\n", b'|').unwrap(),
        vec![vec![text("1"), text("Ann"), text("a,b")]]
    );
    assert_eq!(
        records(b"1\tAnn\t\"x\ty\"\n", b'\t').unwrap(),
        vec![vec![text("1"), text("Ann"), text("x\ty")]]
    );
    for invalid in [b'"', b'\n', b'\r', 0xC3] {
        assert!(
            CsvRecordReader::new(Cursor::new(Vec::new()), invalid, Default::default()).is_err()
        );
    }
}

#[test]
fn literal_quotes_inside_unquoted_fields_are_kept() {
    assert_eq!(
        records(b"a\"b,c\n", b',').unwrap(),
        vec![vec![text("a\"b"), text("c")]]
    );
}

#[test]
fn malformed_quoting_and_invalid_utf8_fail_with_record_location() {
    let error = records(b"a\n\"open,b\n", b',').unwrap_err().to_string();
    assert!(error.contains("record 2 starting at line 2"), "{error}");
    assert!(error.contains("unterminated quoted field"), "{error}");

    let error = records(b"\"a\"b,c\n", b',').unwrap_err().to_string();
    assert!(error.contains("after a closing quote"), "{error}");

    let error = records(b"ok\n\"x\ny\"\nbad\xFF\n", b',')
        .unwrap_err()
        .to_string();
    assert!(error.contains("record 3 starting at line 4"), "{error}");
    assert!(error.contains("not valid UTF-8"), "{error}");
}

#[test]
fn record_field_and_file_limits_fail_closed() {
    let limits = CsvImportLimits {
        max_record_bytes: 4,
        ..CsvImportLimits::default()
    };
    assert_eq!(records_with(b"ab,cd\n", b',', limits).unwrap().len(), 1);
    let error = records_with(b"ab,cde\n", b',', limits)
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_record_bytes 4"), "{error}");

    let limits = CsvImportLimits {
        max_fields_per_record: 2,
        ..CsvImportLimits::default()
    };
    assert!(records_with(b"a,b\n", b',', limits).is_ok());
    let error = records_with(b"a,b,c\n", b',', limits)
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_fields_per_record 2"), "{error}");

    let limits = CsvImportLimits {
        max_file_bytes: 8,
        ..CsvImportLimits::default()
    };
    assert!(records_with(b"a,b\nc,d\n", b',', limits).is_ok());
    let error = records_with(b"a,b\nc,d\ne\n", b',', limits)
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_file_bytes 8"), "{error}");
}

#[test]
fn records_spanning_buffer_refills_are_reassembled() {
    let long = "x".repeat(CSV_READ_BUFFER_BYTES + 17);
    let input = format!("\"{long}\",tail\r\nnext\r\n");
    let parsed = records(input.as_bytes(), b',').unwrap();
    assert_eq!(parsed[0], vec![Some(long), text("tail")]);
    assert_eq!(parsed[1], vec![text("next")]);
}

#[test]
fn rows_without_headers_are_string_lists() {
    assert_eq!(
        rows(b"1,,\"\"\n2\n", false, b',').unwrap(),
        vec![
            Value::List(vec![string("1"), Value::Null, string("")]),
            Value::List(vec![string("2")]),
        ]
    );
}

#[test]
fn header_rows_are_maps_with_null_for_missing_and_empty_fields() {
    let parsed = rows(b"id|name|note\n1|Ann|x|extra\n2||\n3\n", true, b'|').unwrap();
    let map = |entries: &[(&str, Value)]| {
        Value::Map(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
        )
    };
    assert_eq!(
        parsed,
        vec![
            map(&[
                ("id", string("1")),
                ("name", string("Ann")),
                ("note", string("x"))
            ]),
            map(&[
                ("id", string("2")),
                ("name", Value::Null),
                ("note", Value::Null)
            ]),
            map(&[
                ("id", string("3")),
                ("name", Value::Null),
                ("note", Value::Null)
            ]),
        ]
    );
    assert!(rows(b"id,name\n", true, b',').unwrap().is_empty());
    assert!(rows(b"", true, b',').unwrap().is_empty());
    let error = rows(b"id,id\n1,2\n", true, b',').unwrap_err().to_string();
    assert!(error.contains("repeats column 'id'"), "{error}");
}

mod policy {
    use super::*;

    struct Fixture {
        base: PathBuf,
        root: PathBuf,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir().join(format!(
                "hawdb-load-csv-policy-{name}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&base);
            let root = base.join("import");
            std::fs::create_dir_all(root.join("nested")).unwrap();
            std::fs::write(root.join("people.csv"), "id\n1\n").unwrap();
            std::fs::write(root.join("nested/data.csv"), "id\n2\n").unwrap();
            std::fs::write(base.join("secret.csv"), "id\n3\n").unwrap();
            Self { base, root }
        }

        fn policy(&self) -> CsvImportPolicy {
            CsvImportPolicy::new(&self.root, CsvImportLimits::default())
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.base);
        }
    }

    fn semantic(error: HawDBError) -> String {
        assert!(matches!(error, HawDBError::Semantic(_)), "{error:?}");
        error.to_string()
    }

    fn execution(error: HawDBError) -> String {
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        error.to_string()
    }

    #[test]
    fn resolves_absolute_paths_file_urls_and_root_relative_paths() {
        let fixture = Fixture::new("resolve");
        let policy = fixture.policy();
        let expected = std::fs::canonicalize(fixture.root.join("people.csv")).unwrap();
        let absolute = fixture.root.join("people.csv");
        let absolute = absolute.to_str().unwrap();
        assert_eq!(policy.resolve(absolute).unwrap(), expected);
        assert_eq!(policy.resolve("people.csv").unwrap(), expected);
        assert_eq!(policy.resolve("nested/../people.csv").unwrap(), expected);
        // `file:` URL paths are relative to the import directory, as in Neo4j.
        assert_eq!(policy.resolve("file:///people.csv").unwrap(), expected);
        assert_eq!(
            policy.resolve("FILE://localhost/people.csv").unwrap(),
            expected
        );
        assert_eq!(policy.resolve("file:/people.csv").unwrap(), expected);
        assert_eq!(policy.resolve("file:///%70eople.csv").unwrap(), expected);
        assert_eq!(
            policy.resolve("file:///nested/../people.csv").unwrap(),
            expected
        );
        // An absolute filesystem path inside a file URL is not special-cased.
        assert!(policy.resolve(&format!("file://{absolute}")).is_err());
    }

    #[test]
    fn refuses_paths_outside_the_import_root() {
        let fixture = Fixture::new("escape");
        let policy = fixture.policy();
        let error = semantic(policy.resolve("../secret.csv").unwrap_err());
        assert!(
            error.contains("outside the configured import directory"),
            "{error}"
        );
        let outside = fixture.base.join("secret.csv");
        let error = semantic(policy.resolve(outside.to_str().unwrap()).unwrap_err());
        assert!(
            error.contains("outside the configured import directory"),
            "{error}"
        );
        // A missing target outside the root is still a policy violation.
        for location in ["../missing.csv", "nested/../../missing.csv"] {
            let error = semantic(policy.resolve(location).unwrap_err());
            assert!(
                error.contains("outside the configured import directory"),
                "{location}: {error}"
            );
        }
        let missing = fixture.base.join("missing.csv");
        semantic(policy.resolve(missing.to_str().unwrap()).unwrap_err());
        // A missing file inside the root is a run-time condition.
        let error = execution(policy.resolve("missing.csv").unwrap_err());
        assert!(error.contains("cannot access"), "{error}");
        let error = execution(policy.resolve("nested/missing.csv").unwrap_err());
        assert!(error.contains("cannot access"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinks_that_leave_the_import_root() {
        let fixture = Fixture::new("symlink");
        std::os::unix::fs::symlink(
            fixture.base.join("secret.csv"),
            fixture.root.join("link.csv"),
        )
        .unwrap();
        std::os::unix::fs::symlink(&fixture.base, fixture.root.join("up")).unwrap();
        let policy = fixture.policy();
        for location in ["link.csv", "up/secret.csv"] {
            let error = semantic(policy.resolve(location).unwrap_err());
            assert!(
                error.contains("outside the configured import directory"),
                "{error}"
            );
        }
        std::os::unix::fs::symlink(fixture.root.join("people.csv"), fixture.root.join("in.csv"))
            .unwrap();
        assert!(policy.resolve("in.csv").is_ok());
    }

    #[test]
    fn refuses_remote_urls_and_malformed_file_urls() {
        let fixture = Fixture::new("urls");
        let policy = fixture.policy();
        for location in [
            "http://example.com/a.csv",
            "https://example.com/a.csv",
            "ftp://example.com/a.csv",
            "s3://bucket/a.csv",
        ] {
            let error = semantic(policy.resolve(location).unwrap_err());
            assert!(
                error.contains("supports only local files"),
                "{location}: {error}"
            );
        }
        for location in [
            "file://example.com/a.csv",
            "file:relative.csv",
            "file:///a.csv?x=1",
            "file:///a%2.csv",
            "file:///a%00.csv",
            "",
        ] {
            semantic(policy.resolve(location).unwrap_err());
        }
    }

    #[test]
    fn refuses_without_a_policy_and_validates_parameter_sources() {
        let fixture = Fixture::new("open");
        let source = super::source(true, b',');
        let parameters = BTreeMap::from([("file".to_string(), string("people.csv"))]);
        let error = semantic(open_load_csv(None, &source, &parameters).err().unwrap());
        assert!(error.contains("csv_import_root"), "{error}");

        let policy = fixture.policy();
        let mut rows = open_load_csv(Some(&policy), &source, &parameters).unwrap();
        assert!(rows.next_row().unwrap().is_some());
        assert_eq!(rows.rows_read(), 1);

        let error = semantic(
            open_load_csv(Some(&policy), &source, &BTreeMap::new())
                .err()
                .unwrap(),
        );
        assert!(error.contains("missing parameter '$file'"), "{error}");
        let parameters = BTreeMap::from([("file".to_string(), Value::Int(1))]);
        let error = semantic(
            open_load_csv(Some(&policy), &source, &parameters)
                .err()
                .unwrap(),
        );
        assert!(error.contains("must be a string"), "{error}");
        let parameters = BTreeMap::from([("file".to_string(), string("nested"))]);
        let error = execution(
            open_load_csv(Some(&policy), &source, &parameters)
                .err()
                .unwrap(),
        );
        assert!(error.contains("not a regular file"), "{error}");

        let limited = CsvImportPolicy::new(
            &fixture.root,
            CsvImportLimits {
                max_file_bytes: 3,
                ..CsvImportLimits::default()
            },
        );
        let parameters = BTreeMap::from([("file".to_string(), string("people.csv"))]);
        let error = open_load_csv(Some(&limited), &source, &parameters)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("exceeding max_file_bytes 3"), "{error}");

        let missing_root = CsvImportPolicy::new(fixture.base.join("absent"), Default::default());
        let error = execution(missing_root.resolve("people.csv").unwrap_err());
        assert!(error.contains("cannot be resolved"), "{error}");
    }
}

mod mutations {
    use super::*;
    use crate::mutation::materialize_load_csv_mutations;
    use hawdb_core::ConversionFunction;
    use hawdb_plan_cypher::{BatchMatchedRelationship, BatchMutationOperation, BatchMutationValue};
    use hawdb_storage::mutation::{GraphMutation, MutationLimits};
    use std::num::NonZeroUsize;

    fn ledger() -> crate::QueryMemoryLedger {
        crate::QueryMemoryLedger::new(NonZeroUsize::new(64 * 1024 * 1024).unwrap())
    }

    fn csv(input: &str, with_headers: bool) -> CsvRows<Cursor<Vec<u8>>> {
        CsvRows::new(
            Cursor::new(input.as_bytes().to_vec()),
            &source(with_headers, b','),
            CsvImportLimits::default(),
        )
        .unwrap()
    }

    fn converted(property: &str) -> BatchMutationValue {
        BatchMutationValue::Converted {
            function: ConversionFunction::ToInteger,
            input: Box::new(BatchMutationValue::RowProperty(property.to_string())),
        }
    }

    fn create(properties: &[(&str, BatchMutationValue)]) -> BatchMutationOperation {
        BatchMutationOperation::CreateNode {
            label: "P".to_string(),
            properties: properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
        }
    }

    #[test]
    fn create_resolves_conversions_and_omits_null_properties() {
        let operation = create(&[
            ("id", converted("id")),
            ("name", BatchMutationValue::RowProperty("name".to_string())),
        ]);
        let mutations = materialize_load_csv_mutations(
            &mut csv("id,name\n1,Ann\nx,\n", true),
            "row",
            &operation,
            MutationLimits::default(),
            &ledger(),
            None,
        )
        .unwrap();
        let mutations = mutations.mutations();
        let properties = mutations
            .iter()
            .map(|mutation| match mutation {
                GraphMutation::CreateNode { properties, .. } => properties.clone(),
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            properties,
            vec![
                BTreeMap::from([
                    ("id".to_string(), Value::Int(1)),
                    ("name".to_string(), string("Ann")),
                ]),
                BTreeMap::new(),
            ]
        );
    }

    #[test]
    fn list_rows_resolve_indexes_and_reject_map_access() {
        let operation = create(&[
            ("first", BatchMutationValue::RowIndex(0)),
            ("last", BatchMutationValue::RowIndex(-1)),
            ("missing", BatchMutationValue::RowIndex(9)),
        ]);
        let mutations = materialize_load_csv_mutations(
            &mut csv("a,b,c\n", false),
            "line",
            &operation,
            MutationLimits::default(),
            &ledger(),
            None,
        )
        .unwrap();
        let mutations = mutations.mutations();
        let GraphMutation::CreateNode { properties, .. } = &mutations[0] else {
            panic!("{mutations:?}");
        };
        assert_eq!(properties["first"], string("a"));
        assert_eq!(properties["last"], string("c"));
        assert!(!properties.contains_key("missing"));

        let error = materialize_load_csv_mutations(
            &mut csv("a\n", false),
            "line",
            &create(&[("v", BatchMutationValue::RowProperty("x".to_string()))]),
            MutationLimits::default(),
            &ledger(),
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be a map"), "{error}");
    }

    #[test]
    fn matched_relationships_skip_rows_with_null_endpoints() {
        let operation =
            BatchMutationOperation::CreateMatchedRelationship(Box::new(BatchMatchedRelationship {
                source_label: "P".to_string(),
                source_properties: BTreeMap::from([("id".to_string(), converted("src"))]),
                target_label: "P".to_string(),
                target_properties: BTreeMap::from([("id".to_string(), converted("dst"))]),
                rel_type: "KNOWS".to_string(),
                rel_properties: BTreeMap::from([("since".to_string(), converted("since"))]),
            }));
        let mutations = materialize_load_csv_mutations(
            &mut csv("src,dst,since\n1,2,\n,2,5\n1,oops,5\n", true),
            "row",
            &operation,
            MutationLimits::default(),
            &ledger(),
            None,
        )
        .unwrap();
        let mutations = mutations.mutations();
        assert_eq!(mutations.len(), 1, "{mutations:?}");
        let GraphMutation::CreateRelationshipsBetweenMatches(create) = &mutations[0] else {
            panic!("{mutations:?}");
        };
        assert!(create.rel_properties.is_empty());
        assert!(create.source_filter.is_some() && create.target_filter.is_some());
    }

    #[test]
    fn merge_refuses_null_match_properties() {
        let operation = BatchMutationOperation::MergeNode {
            label: "P".to_string(),
            match_properties: BTreeMap::from([(
                "id".to_string(),
                BatchMutationValue::RowProperty("id".to_string()),
            )]),
            on_create_properties: BTreeMap::new(),
        };
        let error = materialize_load_csv_mutations(
            &mut csv("id\n1\n\"\"\n,\n", false),
            "row",
            &BatchMutationOperation::MergeNode {
                label: "P".to_string(),
                match_properties: BTreeMap::from([(
                    "id".to_string(),
                    BatchMutationValue::RowIndex(1),
                )]),
                on_create_properties: BTreeMap::new(),
            },
            MutationLimits::default(),
            &ledger(),
            None,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("NULL value for property 'id'"),
            "{error}"
        );
        assert_eq!(
            materialize_load_csv_mutations(
                &mut csv("id\n1\n\"\"\n", true),
                "row",
                &operation,
                MutationLimits::default(),
                &ledger(),
                None,
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn operation_and_payload_limits_fail_before_staging() {
        let operation = create(&[("v", BatchMutationValue::RowIndex(0))]);
        let limits = MutationLimits {
            max_operations: NonZeroUsize::new(2).unwrap(),
            ..MutationLimits::default()
        };
        assert_eq!(
            materialize_load_csv_mutations(
                &mut csv("a\nb\n", false),
                "l",
                &operation,
                limits,
                &ledger(),
                None
            )
            .unwrap()
            .len(),
            2
        );
        let error = materialize_load_csv_mutations(
            &mut csv("a\nb\nc\n", false),
            "l",
            &operation,
            limits,
            &ledger(),
            None,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("max_mutation_operations 2"),
            "{error}"
        );
        assert!(error.to_string().contains("batched commits"), "{error}");

        let limits = MutationLimits {
            max_result_payload_bytes: NonZeroUsize::new(4).unwrap(),
            ..MutationLimits::default()
        };
        let error = materialize_load_csv_mutations(
            &mut csv("abc\ndef\n", false),
            "l",
            &operation,
            limits,
            &ledger(),
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("max_mutation_result_payload_bytes 4"),
            "{error}"
        );
    }

    #[test]
    fn cancelled_tasks_stop_before_reading_records() {
        let token = hawdb_core::RuntimeCancellationToken::new();
        token.cancel();
        let task = hawdb_core::RuntimeTaskContext::new(token, None);
        let error = materialize_load_csv_mutations(
            &mut csv("a\n", false),
            "l",
            &create(&[("v", BatchMutationValue::RowIndex(0))]),
            MutationLimits::default(),
            &ledger(),
            Some(&task),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
    }
}
