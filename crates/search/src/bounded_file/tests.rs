use super::{read_admitted_bytes, read_bounded_file, READ_BUFFER_BYTES};
use std::cell::RefCell;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

type AdmissionHook = Box<dyn FnOnce(&Path)>;

thread_local! {
    static ADMISSION_HOOK: RefCell<Option<AdmissionHook>> = RefCell::new(None);
    static READ_HOOK: RefCell<Option<AdmissionHook>> = RefCell::new(None);
}

pub(crate) fn after_admission(path: &Path) {
    let hook = ADMISSION_HOOK.with(|hook| hook.borrow_mut().take());
    if let Some(hook) = hook {
        hook(path);
    }
}

pub(crate) fn after_read(path: &Path) {
    let hook = READ_HOOK.with(|hook| hook.borrow_mut().take());
    if let Some(hook) = hook {
        hook(path);
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-bounded-manifest-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        ADMISSION_HOOK.with(|hook| hook.borrow_mut().take());
        READ_HOOK.with(|hook| hook.borrow_mut().take());
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn file_growth_after_admission_cannot_escape_the_byte_limit() {
    let directory = Directory::new();
    let path = directory.0.join("artifact");
    fs::write(&path, b"original").unwrap();
    ADMISSION_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(|path| {
            OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(&vec![b'x'; 2 * 1024 * 1024])
                .unwrap();
        }))
    });
    let result = read_bounded_file(&path, 16);
    assert!(result.is_err(), "growth bypassed the admitted byte limit");
}

#[test]
fn pathname_replacement_does_not_substitute_the_admitted_file() {
    let directory = Directory::new();
    let path = directory.0.join("artifact");
    fs::write(&path, b"original").unwrap();
    ADMISSION_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(|path| {
            fs::rename(path, path.with_extension("retained")).unwrap();
            fs::write(path, b"replaced").unwrap();
        }))
    });
    assert_eq!(read_bounded_file(&path, 8).unwrap(), b"original");
    assert_eq!(fs::read(path).unwrap(), b"replaced");
}

#[test]
fn stable_files_obey_exact_and_one_short_limits() {
    let directory = Directory::new();
    let path = directory.0.join("artifact");
    for length in [0, 1, 8191, 8192, 8193, 32769] {
        let data = (0..length).map(|index| index as u8).collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();
        assert_eq!(read_bounded_file(&path, length as u64).unwrap(), data);
        assert_eq!(read_bounded_file(&path, u64::MAX).unwrap(), data);
        if length > 0 {
            ADMISSION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(|_| panic!("oversize file was admitted")))
            });
            assert!(read_bounded_file(&path, (length - 1) as u64)
                .unwrap_err()
                .to_string()
                .contains("exceeding"));
            assert!(ADMISSION_HOOK.with(|hook| hook.borrow_mut().take().is_some()));
        }
    }
    assert!(read_bounded_file(&directory.0.join("missing"), 8).is_err());
}

#[test]
fn length_changes_fail_even_when_the_caller_has_spare_budget() {
    let directory = Directory::new();
    let path = directory.0.join("artifact");
    for changed_length in [0, 7, 9, 64] {
        fs::write(&path, b"original").unwrap();
        ADMISSION_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |path| {
                OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(changed_length)
                    .unwrap();
            }))
        });
        assert!(read_bounded_file(&path, 128).is_err());
    }
}

struct SplitReader<'a> {
    input: &'a [u8],
    position: usize,
    chunk: usize,
    fail_at: Option<usize>,
    interrupt_next: bool,
    max_requested: usize,
}

impl<'a> SplitReader<'a> {
    fn new(input: &'a [u8], chunk: usize) -> Self {
        Self {
            input,
            position: 0,
            chunk,
            fail_at: None,
            interrupt_next: true,
            max_requested: 0,
        }
    }
}

impl Read for SplitReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.max_requested = self.max_requested.max(output.len());
        self.interrupt_next = !self.interrupt_next;
        if !self.interrupt_next {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at == Some(self.position) {
            return Err(io::Error::other("injected admitted-file read failure"));
        }
        let end = self.input.len().min(self.fail_at.unwrap_or(usize::MAX));
        let count = (end - self.position).min(output.len()).min(self.chunk);
        output[..count].copy_from_slice(&self.input[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

#[test]
fn short_interrupted_reads_preserve_bytes_and_all_fault_prefixes_fail() {
    let data = b"manifest bytes including \x00 and \xff";
    let path = Path::new("fault-test");
    for chunk in 1..=data.len() {
        let mut reader = SplitReader::new(data, chunk);
        assert_eq!(
            read_admitted_bytes(&mut reader, data.len(), path).unwrap(),
            data
        );
        assert!(reader.max_requested <= READ_BUFFER_BYTES);
    }
    // Include failure during the EOF probe, after all admitted bytes were read.
    for boundary in 0..=data.len() {
        let mut reader = SplitReader::new(data, 3);
        reader.fail_at = Some(boundary);
        assert!(read_admitted_bytes(&mut reader, data.len(), path)
            .unwrap_err()
            .to_string()
            .contains("injected admitted-file read failure"));
        assert_eq!(reader.position, boundary);
        if boundary < data.len() {
            let mut reader = SplitReader::new(&data[..boundary], 3);
            assert!(read_admitted_bytes(&mut reader, data.len(), path).is_err());
        }
    }
}

#[test]
fn growth_probe_consumes_only_one_byte_beyond_the_admitted_length() {
    let data = vec![b'x'; 4 * READ_BUFFER_BYTES];
    for length in [0, 1, READ_BUFFER_BYTES - 1, READ_BUFFER_BYTES + 1] {
        let mut reader = SplitReader::new(&data, 7);
        assert!(
            read_admitted_bytes(&mut reader, length, Path::new("growing"))
                .unwrap_err()
                .to_string()
                .contains("grew beyond")
        );
        assert_eq!(reader.position, length + 1);
        assert!(reader.max_requested <= READ_BUFFER_BYTES);
    }
}

#[test]
fn empty_truncated_input_does_not_reserve_the_declared_length() {
    let mut reader = SplitReader::new(&[], 1);
    assert!(read_admitted_bytes(&mut reader, usize::MAX, Path::new("truncated")).is_err());
    assert_eq!(reader.position, 0);
    assert_eq!(reader.max_requested, READ_BUFFER_BYTES);
}

#[test]
#[ignore = "explicit local bounded-file differential campaign"]
fn bounded_file_differential_campaign() {
    let mut seed = 0x3c7f_a826_198b_4d51_u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let path = Path::new("seeded-read");
    for case in 0..512 {
        let length = next() as usize % (4 * READ_BUFFER_BYTES + 1);
        let chunk = 1 + next() as usize % (READ_BUFFER_BYTES + 7);
        let mut data = (0..length).map(|_| next() as u8).collect::<Vec<_>>();
        let mut reader = SplitReader::new(&data, chunk);
        assert_eq!(
            read_admitted_bytes(&mut reader, length, path).unwrap(),
            data
        );
        assert_eq!(reader.position, length);
        assert!(reader.max_requested <= READ_BUFFER_BYTES);

        let boundary = next() as usize % (length + 1);
        let mut reader = SplitReader::new(&data, chunk);
        reader.fail_at = Some(boundary);
        assert!(
            read_admitted_bytes(&mut reader, length, path).is_err(),
            "case {case}"
        );
        assert_eq!(reader.position, boundary);
        if boundary < length {
            let mut reader = SplitReader::new(&data[..boundary], chunk);
            assert!(
                read_admitted_bytes(&mut reader, length, path).is_err(),
                "case {case}"
            );
        }

        data.extend_from_slice(&[1, 2, 3, 4]);
        let mut reader = SplitReader::new(&data, chunk);
        assert!(
            read_admitted_bytes(&mut reader, length, path).is_err(),
            "case {case}"
        );
        assert_eq!(reader.position, length + 1);
        assert!(reader.max_requested <= READ_BUFFER_BYTES);
    }
}

fn create_generation(root: &Path) -> crate::SearchDocument {
    let mut writer =
        crate::SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
    let document = crate::SearchDocument {
        id: "document".to_string(),
        title: "title".to_string(),
        content: "content".to_string(),
        embedding: None,
        metadata: Default::default(),
    };
    writer.push(document.clone()).unwrap();
    writer.finish().unwrap();
    document
}

fn lexical_manifest(root: &Path) -> PathBuf {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("search_lexical.manifest.")
        })
        .unwrap()
}

fn marker_status(reader: &crate::SearchOutOfCoreReader, name: &str) -> (bool, Vec<String>) {
    let freshness = reader.projection_freshness();
    match name {
        crate::FULL_REINDEX_MARKER => (
            freshness.full_reindex_needed,
            freshness.full_reindex_reasons,
        ),
        crate::METADATA_REPAIR_MARKER => (
            freshness.metadata_repair_needed,
            freshness.metadata_repair_reasons,
        ),
        _ => panic!("unexpected marker"),
    }
}

#[test]
fn marker_growth_errors_cannot_report_a_fresh_projection() {
    for name in [crate::FULL_REINDEX_MARKER, crate::METADATA_REPAIR_MARKER] {
        let directory = Directory::new();
        create_generation(&directory.0);
        let reader = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
        let path = directory.0.join(name);
        fs::write(&path, b"reason").unwrap();
        ADMISSION_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |path| {
                assert_eq!(path.file_name().unwrap(), name);
                OpenOptions::new()
                    .append(true)
                    .open(path)
                    .unwrap()
                    .write_all(b" changed")
                    .unwrap();
            }));
        });
        let (needed, reasons) = marker_status(&reader, name);
        assert!(needed, "an unreadable marker was treated as absent");
        assert!(reasons.iter().any(|reason| reason.contains("grew beyond")));
        assert!(ADMISSION_HOOK.with(|hook| hook.borrow().is_none()));
    }
}

#[test]
fn marker_errors_fail_closed_without_changing_valid_or_missing_markers() {
    for name in [crate::FULL_REINDEX_MARKER, crate::METADATA_REPAIR_MARKER] {
        let directory = Directory::new();
        create_generation(&directory.0);
        let reader = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
        let path = directory.0.join(name);
        assert_eq!(marker_status(&reader, name), (false, Vec::new()));
        fs::write(&path, b"first\nsecond").unwrap();
        assert_eq!(
            marker_status(&reader, name),
            (true, vec!["first".into(), "second".into()])
        );
        for bytes in [vec![0xff], vec![b'x'; 64 * 1024 + 1]] {
            fs::write(&path, bytes).unwrap();
            let (needed, reasons) = marker_status(&reader, name);
            assert!(needed);
            assert_eq!(reasons.len(), 1);
            assert!(reasons[0].contains("failed to read marker"));
        }
        fs::write(&path, []).unwrap();
        assert_eq!(marker_status(&reader, name), (false, Vec::new()));
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let (needed, reasons) = marker_status(&reader, name);
        assert!(needed);
        assert!(reasons[0].contains("failed to read marker"));
    }
}

#[test]
fn marker_parent_lookup_errors_cannot_report_a_fresh_projection() {
    let directory = Directory::new();
    let root = directory.0.join("projection");
    create_generation(&root);
    let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
    fs::rename(&root, directory.0.join("retained")).unwrap();
    fs::write(&root, b"not a directory").unwrap();

    for name in [crate::FULL_REINDEX_MARKER, crate::METADATA_REPAIR_MARKER] {
        let error = fs::symlink_metadata(root.join(name)).unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::NotFound);
        let (needed, reasons) = marker_status(&reader, name);
        assert!(needed, "a marker lookup error was treated as absence");
        assert_eq!(reasons.len(), 1);
        assert!(reasons[0].contains(&format!("failed to read marker {name}")));
    }
}

#[cfg(unix)]
#[test]
fn marker_broken_symlinks_cannot_report_a_fresh_projection() {
    use std::os::unix::fs::symlink;

    for name in [crate::FULL_REINDEX_MARKER, crate::METADATA_REPAIR_MARKER] {
        let directory = Directory::new();
        create_generation(&directory.0);
        let reader = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
        let path = directory.0.join(name);
        let target = directory.0.join("marker-target");
        fs::write(&target, b"rebuild required").unwrap();
        symlink(&target, &path).unwrap();
        assert_eq!(
            marker_status(&reader, name),
            (true, vec!["rebuild required".into()])
        );
        fs::write(&target, []).unwrap();
        assert_eq!(marker_status(&reader, name), (false, Vec::new()));
        fs::remove_file(&target).unwrap();
        assert!(fs::symlink_metadata(&path).unwrap().is_symlink());
        let (needed, reasons) = marker_status(&reader, name);
        assert!(needed, "a dangling marker symlink was treated as absence");
        assert_eq!(reasons.len(), 1);
        assert!(reasons[0].contains(&format!("failed to read marker {name}")));
    }
}

#[cfg(unix)]
#[test]
fn marker_symlink_loops_cannot_report_a_fresh_projection() {
    use std::os::unix::fs::symlink;

    for name in [crate::FULL_REINDEX_MARKER, crate::METADATA_REPAIR_MARKER] {
        let directory = Directory::new();
        create_generation(&directory.0);
        let reader = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
        let path = directory.0.join(name);
        symlink(name, &path).unwrap();
        assert!(fs::metadata(&path).is_err());
        let (needed, reasons) = marker_status(&reader, name);
        assert!(needed, "a marker symlink loop was treated as absence");
        assert_eq!(reasons.len(), 1);
        assert!(reasons[0].contains(&format!("failed to read marker {name}")));
    }
}

#[test]
fn public_open_keeps_lexical_budget_and_checksum_admission() {
    use crate::{SearchOutOfCoreConfig, SearchOutOfCoreReader};
    let directory = Directory::new();
    create_generation(&directory.0);
    let path = lexical_manifest(&directory.0);
    let original = fs::read(&path).unwrap();
    let config = SearchOutOfCoreConfig {
        max_lexical_manifest_bytes: NonZeroU64::new(original.len() as u64).unwrap(),
        ..Default::default()
    };
    SearchOutOfCoreReader::open_with_config(&directory.0, config.clone()).unwrap();
    let one_short = SearchOutOfCoreConfig {
        max_lexical_manifest_bytes: NonZeroU64::new(original.len() as u64 - 1).unwrap(),
        ..config.clone()
    };
    assert!(
        SearchOutOfCoreReader::open_with_config(&directory.0, one_short)
            .unwrap_err()
            .to_string()
            .contains("search lexical manifest exceeds")
    );
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(
        SearchOutOfCoreReader::open_with_config(&directory.0, config.clone())
            .unwrap_err()
            .to_string()
            .contains("length or checksum mismatch")
    );
    fs::write(&path, &original[..original.len() - 1]).unwrap();
    assert!(
        SearchOutOfCoreReader::open_with_config(&directory.0, config)
            .unwrap_err()
            .to_string()
            .contains("length or checksum mismatch")
    );
}

#[test]
fn standalone_lexical_loaders_share_bounded_file_admission_and_identity_checks() {
    use crate::lexical_projection::{
        manifest_generation, LexicalProjectionConfig, LexicalProjectionReader, MANIFEST_FILE,
    };
    let directory = Directory::new();
    create_generation(&directory.0);
    let bytes = fs::read(lexical_manifest(&directory.0)).unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let body = &envelope["body"];
    let generation = body["generation"].as_u64().unwrap();
    let analyzer = body["analyzer_digest"].as_u64().unwrap();
    let documents = body["documents_digest"].as_u64().unwrap();
    let config = LexicalProjectionConfig::default();
    let load = || LexicalProjectionReader::load(&directory.0, None, analyzer, documents, config);
    assert!(load().unwrap().is_none());
    let canonical = directory.0.join(MANIFEST_FILE);
    fs::write(&canonical, &bytes).unwrap();
    assert_eq!(load().unwrap().unwrap().generation(), generation);
    assert_eq!(
        manifest_generation(
            &canonical,
            crate::lexical_projection::DEFAULT_MAX_MANIFEST_BYTES
        )
        .unwrap(),
        Some(generation)
    );
    for (epoch, analyzer, documents) in [
        (Some(1), analyzer, documents),
        (None, analyzer ^ 1, documents),
        (None, analyzer, documents ^ 1),
    ] {
        assert!(LexicalProjectionReader::load_manifest_bytes(
            &directory.0,
            &bytes,
            epoch,
            analyzer,
            documents,
            config
        )
        .unwrap()
        .is_none());
    }
    let tiny = LexicalProjectionConfig {
        max_block_bytes: NonZeroU64::new(1).unwrap(),
        ..config
    };
    assert!(LexicalProjectionReader::load_manifest_bytes(
        &directory.0,
        &bytes,
        None,
        analyzer,
        documents,
        tiny
    )
    .unwrap_err()
    .to_string()
    .contains("block above the read admission"));

    let artifact_path = directory.0.join(body["artifact_file"].as_str().unwrap());
    let artifact = fs::read(&artifact_path).unwrap();
    let mut damaged_artifact = artifact.clone();
    *damaged_artifact.last_mut().unwrap() ^= 1;
    fs::write(&artifact_path, &damaged_artifact).unwrap();
    assert!(load()
        .unwrap_err()
        .to_string()
        .contains("artifact checksum mismatch"));
    fs::write(&artifact_path, &artifact[..artifact.len() - 1]).unwrap();
    assert!(load()
        .unwrap_err()
        .to_string()
        .contains("artifact length mismatch"));
    fs::write(&artifact_path, artifact).unwrap();
    assert!(load().unwrap().is_some());

    let mut corrupt = envelope;
    corrupt["checksum"] = serde_json::json!(corrupt["checksum"].as_u64().unwrap() ^ 1);
    fs::write(&canonical, serde_json::to_vec(&corrupt).unwrap()).unwrap();
    assert!(load()
        .unwrap_err()
        .to_string()
        .contains("manifest checksum mismatch"));
    assert!(manifest_generation(
        &canonical,
        crate::lexical_projection::DEFAULT_MAX_MANIFEST_BYTES
    )
    .unwrap()
    .is_none());

    // A sparse over-limit manifest must fail before any content is admitted.
    OpenOptions::new()
        .write(true)
        .open(&canonical)
        .unwrap()
        .set_len(256 * 1024 * 1024 + 1)
        .unwrap();
    ADMISSION_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(|_| {
            panic!("oversize lexical manifest was admitted")
        }))
    });
    assert!(load().unwrap_err().to_string().contains("exceeding"));
    assert!(manifest_generation(
        &canonical,
        crate::lexical_projection::DEFAULT_MAX_MANIFEST_BYTES
    )
    .unwrap_err()
    .to_string()
    .contains("exceeding"));
    assert!(ADMISSION_HOOK.with(|hook| hook.borrow_mut().take().is_some()));
}

fn replace_lexical_after_read(path: &Path) {
    if path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("search_lexical.manifest.")
    {
        fs::write(path, b"replacement must not be reopened").unwrap();
    } else {
        READ_HOOK.with(|hook| *hook.borrow_mut() = Some(Box::new(replace_lexical_after_read)));
    }
}

#[test]
fn public_open_parses_the_verified_lexical_bytes_without_reopening() {
    use crate::SearchOutOfCoreReader;
    let directory = Directory::new();
    let document = create_generation(&directory.0);
    #[cfg(feature = "full-text-search")]
    let query = |reader: &SearchOutOfCoreReader| {
        reader
            .search_with_options(
                "content",
                None,
                crate::SearchMode::Text,
                crate::SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: Default::default(),
                    policy_epoch: None,
                },
            )
            .unwrap()
            .result
    };
    #[cfg(feature = "full-text-search")]
    let expected = query(&SearchOutOfCoreReader::open(&directory.0).unwrap());
    READ_HOOK.with(|hook| *hook.borrow_mut() = Some(Box::new(replace_lexical_after_read)));
    let reader = SearchOutOfCoreReader::open(&directory.0).unwrap();
    assert!(READ_HOOK.with(|hook| hook.borrow().is_none()));
    #[cfg(feature = "full-text-search")]
    {
        assert_eq!(expected.hits.len(), 1);
        assert_eq!(query(&reader), expected);
    }
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&document.id))
            .unwrap()
            .documents,
        vec![document]
    );
    assert!(SearchOutOfCoreReader::open(&directory.0).is_err());
}
