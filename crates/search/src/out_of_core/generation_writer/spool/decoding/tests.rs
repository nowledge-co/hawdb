use super::*;
use std::io::Cursor;

fn assert_document(actual: SearchDocument, expected: SearchDocument) {
    assert_eq!(actual.id, expected.id);
    assert_eq!(actual.title, expected.title);
    assert_eq!(actual.content, expected.content);
    assert_eq!(actual.metadata, expected.metadata);
    let bits = |embedding: Option<Vec<f32>>| {
        embedding.map(|values| values.into_iter().map(f32::to_bits).collect::<Vec<_>>())
    };
    assert_eq!(bits(actual.embedding), bits(expected.embedding));
}

struct SplitReader<'a> {
    input: &'a [u8],
    position: usize,
    split: usize,
    max_read: usize,
    max_requested: usize,
    fail_at: Option<usize>,
    interrupted: bool,
}

impl<'a> SplitReader<'a> {
    fn new(input: &'a [u8], split: usize) -> Self {
        Self {
            input,
            position: 0,
            split,
            max_read: usize::MAX,
            max_requested: 0,
            fail_at: None,
            interrupted: true,
        }
    }
}

impl Read for SplitReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.max_requested = self.max_requested.max(output.len());
        if std::mem::take(&mut self.interrupted) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at == Some(self.position) {
            return Err(io::Error::other("injected spool read failure"));
        }
        let mut end = self.input.len();
        if self.position < self.split {
            end = end.min(self.split);
        }
        if let Some(fail_at) = self.fail_at {
            end = end.min(fail_at);
        }
        let count = (end - self.position).min(output.len()).min(self.max_read);
        output[..count].copy_from_slice(&self.input[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

fn decode(record: &[u8]) -> Result<SearchDocument> {
    read_frame(
        &mut Cursor::new(record),
        record.len(),
        checksum_bytes(record),
        7,
    )
}

#[test]
fn streamed_spool_decode_matches_legacy_grammar_at_every_split() {
    let records = [
        "doc\t\t\t\t\t",
        "doc\t00+F\t4142\tf09fa680e4b8ad\t-0,+1.250,NaN,inf,-inf,1e-45\t6B=31;6b=32;=\n",
        "doc\t61\t\t\t\t=\n",
        "doc\t61\t\t\t\t=",
    ];
    for record in records {
        for split in 0..=record.len() {
            let mut reader = SplitReader::new(record.as_bytes(), split);
            let actual = read_frame(
                &mut reader,
                record.len(),
                checksum_bytes(record.as_bytes()),
                0,
            )
            .unwrap();
            assert_document(actual, crate::decode_search_document_line(record).unwrap());
            assert_eq!(reader.position, record.len());
        }
    }
}

#[test]
fn streamed_spool_decode_rejects_all_truncations_and_preserves_io_errors() {
    let record = b"doc\t61\t62\t63\t1,-0\t6b=76\n";
    for boundary in 0..record.len() {
        assert!(read_frame(
            &mut Cursor::new(&record[..boundary]),
            record.len(),
            checksum_bytes(record),
            3,
        )
        .unwrap_err()
        .to_string()
        .contains("record 3 is truncated"));
        let mut reader = SplitReader::new(record, boundary);
        reader.fail_at = Some(boundary);
        assert!(
            read_frame(&mut reader, record.len(), checksum_bytes(record), 3)
                .unwrap_err()
                .to_string()
                .contains("injected spool read failure")
        );
    }
}

#[test]
fn streamed_spool_decode_validates_checksum_even_after_syntax_errors() {
    let valid = b"doc\t61\t\t\t\t\n";
    assert!(read_frame(
        &mut Cursor::new(valid),
        valid.len(),
        checksum_bytes(valid) ^ 1,
        2
    )
    .unwrap_err()
    .to_string()
    .contains("checksum mismatch"));
    let records: &[&[u8]] = &[
        b"bad\t61\t\t\t\t\n",
        b"doc\t0\t\t\t\t\n",
        b"doc\tff\t\t\t\t\n",
        b"doc\tgg\t\t\t\t\n",
        b"doc\t61\t\t\t1,\t\n",
        b"doc\t61\t\t\t\t61=62;\n",
        b"doc\t61\t\t\t\t61=62=\n",
        b"doc\t61\t\t\t\t\n\n",
        b"doc\t61\t\t\t\t\t\n",
        b"doc\t0\xe2\x82\xac\t\t\t\t\n",
        b"doc\t61\t\xff\t\t\t\n",
    ];
    for record in records {
        assert!(decode(record).is_err(), "{record:?}");
        let mut reader = SplitReader::new(record, 1);
        reader.max_read = 3;
        let error =
            read_frame(&mut reader, record.len(), checksum_bytes(record) ^ 1, 2).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"), "{error}");
        assert_eq!(reader.position, record.len());
        let mut reader = SplitReader::new(record, 1);
        reader.fail_at = Some(record.len() - 1);
        assert!(
            read_frame(&mut reader, record.len(), checksum_bytes(record), 2)
                .unwrap_err()
                .to_string()
                .contains("injected spool read failure")
        );
    }
}

#[test]
fn streamed_spool_decode_bounds_reads_and_does_not_consume_the_next_frame() {
    let document = SearchDocument {
        id: "large".into(),
        title: "\u{4e2d}\u{1f980}".repeat(4097),
        content: "body \u{4e2d}\u{1f980}\n".repeat(131_073),
        embedding: Some(vec![1.0, -0.0]),
        metadata: BTreeMap::from([("key".into(), "value;=\t\n".repeat(8193))]),
    };
    let record = crate::document_encoding::legacy_encode(&document);
    let mut input = record.as_bytes().to_vec();
    input.extend_from_slice(b"next frame sentinel");
    let mut reader = SplitReader::new(&input, 8191);
    let actual = read_frame(
        &mut reader,
        record.len(),
        checksum_bytes(record.as_bytes()),
        0,
    )
    .unwrap();
    assert_document(actual, document);
    assert_eq!(reader.position, record.len());
    assert!(reader.max_requested <= INPUT_BYTES);

    let record = format!("doc\t61\t\t\t1.{}1\t\n", "0".repeat(65_537));
    assert_document(
        decode(record.as_bytes()).unwrap(),
        crate::decode_search_document_line(&record).unwrap(),
    );
}

#[test]
#[ignore = "explicit local Bazel spool-decoding differential campaign"]
fn spool_decoding_differential_campaign() {
    let mut random = 0x0003_92de_c0de_u64;
    for case in 0..512 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let units = ["a", "\u{4e2d}", "\u{1f980}", "\0\t\n;=", "\u{e9}\u{301}"];
        let length = [0, 1, 4095, 4096, 4097, 8191, 8192, 8193][case % 8];
        let document = SearchDocument {
            id: format!("doc-{case}"),
            title: units[case % units.len()].repeat(case % 13),
            content: units[random as usize % units.len()].repeat(length),
            embedding: (case % 3 != 0).then(|| vec![f32::from_bits(random as u32), -0.0]),
            metadata: BTreeMap::from([
                (String::new(), units[case % units.len()].into()),
                ("key;=\t".into(), format!("value-{random}")),
            ]),
        };
        let record = crate::document_encoding::legacy_encode(&document);
        let record = if case % 2 == 0 {
            record.trim_end_matches('\n')
        } else {
            &record
        };
        let split = random as usize % record.len();
        let mut reader = SplitReader::new(record.as_bytes(), split);
        reader.max_read = 1 + (random as usize >> 8) % 8193;
        assert_document(
            read_frame(
                &mut reader,
                record.len(),
                checksum_bytes(record.as_bytes()),
                case,
            )
            .unwrap(),
            crate::decode_search_document_line(record).unwrap(),
        );
        assert_eq!(reader.position, record.len(), "case {case}");
        assert!(reader.max_requested <= INPUT_BYTES);

        // ASCII mutations are safe for the independent old decoder. Non-ASCII
        // corruption is checked separately, without inheriting its UTF-8 panic.
        let mut mutated = record.as_bytes().to_vec();
        mutated[split] = b"\t\n;=g+0F,"[case % 9];
        let text = std::str::from_utf8(&mutated).unwrap();
        let mut reader = SplitReader::new(&mutated, split);
        reader.max_read = 1 + case % 127;
        let actual = read_frame(&mut reader, mutated.len(), checksum_bytes(&mutated), case);
        match crate::decode_search_document_line(text) {
            Ok(expected) => assert_document(actual.unwrap(), expected),
            Err(_) => assert!(actual.is_err(), "case {case}, mutation at {split}"),
        }
        assert_eq!(reader.position, mutated.len());
        mutated[split] = 0xff;
        assert!(decode(&mutated).is_err(), "non-UTF-8 mutation case {case}");

        let mut reader = SplitReader::new(record.as_bytes(), split);
        reader.fail_at = Some(split);
        assert!(read_frame(
            &mut reader,
            record.len(),
            checksum_bytes(record.as_bytes()),
            case
        )
        .unwrap_err()
        .to_string()
        .contains("injected spool read failure"));
    }
}
