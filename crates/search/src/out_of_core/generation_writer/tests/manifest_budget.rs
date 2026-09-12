use super::*;
use crate::{SearchOutOfCoreConfig, SearchOutOfCoreReader};

fn budget(bytes: u64) -> NonZeroU64 {
    NonZeroU64::new(bytes).unwrap()
}

fn source(content: &str) -> SearchDocument {
    SearchDocument {
        id: "memory:record".into(),
        title: String::new(),
        content: content.into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn build(root: &Path, content: &str, bytes: u64) -> Result<SearchOutOfCoreGenerationBuildReport> {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default())?;
    writer.set_max_lexical_manifest_bytes(budget(bytes))?;
    writer.push(source(content))?;
    writer.finish()
}

fn open(root: &Path, bytes: u64) -> Result<SearchOutOfCoreReader> {
    SearchOutOfCoreReader::open_with_config(
        root,
        SearchOutOfCoreConfig {
            max_lexical_manifest_bytes: budget(bytes),
            ..Default::default()
        },
    )
}

fn delta(content: &str) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "record".into(),
            title: String::new(),
            body: content.into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        }],
        ..Default::default()
    }
}

fn content(reader: &SearchOutOfCoreReader) -> String {
    reader
        .hydrate_documents(&["memory:record".into()])
        .unwrap()
        .documents
        .remove(0)
        .content
}

#[test]
fn manifest_budget_setter_preserves_default_and_rejects_unrepresentable_limits_atomically() {
    let root = test_dir("manifest_budget_setter");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    assert_eq!(writer.max_lexical_manifest_bytes().get(), 256 * 1024 * 1024);
    assert_eq!(
        SearchOutOfCoreConfig::default().max_lexical_manifest_bytes,
        writer.max_lexical_manifest_bytes()
    );
    for bytes in [1, 512 * 1024 * 1024, isize::MAX as u64] {
        writer
            .set_max_lexical_manifest_bytes(budget(bytes))
            .unwrap();
        assert_eq!(writer.max_lexical_manifest_bytes().get(), bytes);
        for invalid in [isize::MAX as u64 + 1, u64::MAX] {
            assert!(writer
                .set_max_lexical_manifest_bytes(budget(invalid))
                .is_err());
            assert_eq!(writer.max_lexical_manifest_bytes().get(), bytes);
        }
    }
    drop(writer);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

fn assert_build_boundaries(label: &str, text: &str) {
    let reference_root = test_dir(label);
    let reference = build(&reference_root, text, 512 * 1024 * 1024).unwrap();
    let exact = reference.lexical_manifest_bytes;
    let manifest_name = format!("search_lexical.manifest.{}.skein", reference.generation);
    let expected = fs::read(reference_root.join(&manifest_name)).unwrap();
    assert_eq!(content(&open(&reference_root, exact).unwrap()), text);
    assert!(open(&reference_root, exact - 1).is_err());

    for limit in [exact - 1, exact, exact + 1] {
        let root = test_dir(label);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.set_max_lexical_manifest_bytes(budget(1)).unwrap();
        writer.push(source(text)).unwrap();
        writer
            .set_max_lexical_manifest_bytes(budget(limit))
            .unwrap();
        let result = writer.finish();
        if limit < exact {
            assert!(result.unwrap_err().to_string().contains(&format!(
                "manifest requires {exact} bytes, exceeding {limit}"
            )));
            assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
            assert!(!root.join(&manifest_name).exists());
            assert!(!root.join(lexical_artifact_file(1)).exists());
        } else {
            assert_eq!(result.unwrap().lexical_manifest_bytes, exact);
            assert_eq!(fs::read(root.join(&manifest_name)).unwrap(), expected);
            assert_eq!(content(&open(&root, exact).unwrap()), text);
        }
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
    fs::remove_dir_all(reference_root).unwrap();
}

#[test]
fn manifest_budget_final_selection_controls_exact_publication_and_reader_admission() {
    for text in ["", "graph storage", "quote\" slash\\ \u{e9} \u{4e2d}\n"] {
        assert_build_boundaries("manifest_budget_boundary", text);
    }
}

#[test]
fn manifest_budget_private_loader_checks_the_captured_bytes_against_its_configuration() {
    let root = test_dir("manifest_budget_inner_loader");
    let report = build(&root, "initial", 512 * 1024 * 1024).unwrap();
    let bytes = fs::read(root.join(format!(
        "search_lexical.manifest.{}.skein",
        report.generation
    )))
    .unwrap();
    for limit in [
        report.lexical_manifest_bytes - 1,
        report.lexical_manifest_bytes,
    ] {
        let result = crate::lexical_projection::LexicalProjectionReader::load_manifest_bytes(
            &root,
            &bytes,
            None,
            lexical_analyzer_digest(&Default::default()),
            report.documents_digest,
            LexicalProjectionConfig {
                max_manifest_bytes: budget(limit),
                ..Default::default()
            },
        );
        if limit < bytes.len() as u64 {
            assert!(result.unwrap_err().to_string().contains("read budget"));
        } else {
            assert!(result.unwrap().is_some());
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_budget_delta_inherits_reader_limit_and_retains_old_generation_on_failure() {
    let root = test_dir("manifest_budget_delta");
    let initial = build(&root, "initial", 512 * 1024 * 1024).unwrap();
    let exact = initial.lexical_manifest_bytes;
    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let old_reader = open(&root, exact).unwrap();
    let replacement = (0..100).map(|i| format!("term{i:04} ")).collect::<String>();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &old_reader,
        delta(&replacement),
        Default::default(),
    )
    .unwrap();
    assert!(update
        .finish()
        .unwrap_err()
        .to_string()
        .contains("manifest requires"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(content(&old_reader), "initial");
    assert_eq!(content(&open(&root, exact).unwrap()), "initial");
    assert_eq!(stage_directories(&root), 0);

    let reader = open(&root, 512 * 1024 * 1024).unwrap();
    let cancelled = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        delta(&replacement),
        Default::default(),
    )
    .unwrap();
    drop(cancelled);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        delta(&replacement),
        Default::default(),
    )
    .unwrap();
    drop(reader);
    let (_, report, _) = update.finish().unwrap();
    assert!(report.lexical_manifest_bytes > exact);
    assert_eq!(
        content(&open(&root, report.lexical_manifest_bytes).unwrap()),
        replacement
    );
    assert_eq!(content(&old_reader), "initial");
    assert!(open(&root, exact).is_err());
    drop(old_reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_budget_unrepresentable_delta_limit_rejects_without_a_stage() {
    let root = test_dir("manifest_budget_delta_invalid");
    build(&root, "initial", DEFAULT_MAX_MANIFEST_BYTES).unwrap();
    let reader = open(&root, u64::MAX).unwrap();
    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    assert!(SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        delta("replacement"),
        Default::default(),
    )
    .unwrap_err()
    .to_string()
    .contains("isize::MAX"));
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(content(&reader), "initial");
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_budget_recovery_never_reuses_a_generation_hidden_by_its_limit() {
    let root = test_dir("manifest_budget_recovery");
    let original = (0..100).map(|i| format!("term{i:04} ")).collect::<String>();
    let first = build(&root, &original, DEFAULT_MAX_MANIFEST_BYTES).unwrap();
    let old = open(&root, first.lexical_manifest_bytes).unwrap();
    let lexical_path = root.join(format!(
        "search_lexical.manifest.{}.skein",
        first.generation
    ));
    let lexical = fs::read(&lexical_path).unwrap();
    fs::write(root.join(OUT_OF_CORE_MANIFEST_FILE), b"invalid manifest").unwrap();
    fs::write(
        root.join("search_lexical.manifest.999.skein"),
        b"invalid candidate",
    )
    .unwrap();
    assert!(build(&root, "replacement", first.lexical_manifest_bytes - 1).is_err());
    assert_eq!(fs::read(&lexical_path).unwrap(), lexical);
    assert_eq!(content(&old), original);
    assert_eq!(stage_directories(&root), 0);
    let next = build(&root, "replacement", DEFAULT_MAX_MANIFEST_BYTES).unwrap();
    assert_eq!(next.generation, first.generation + 1);
    assert_eq!(
        content(&open(&root, next.lexical_manifest_bytes).unwrap()),
        "replacement"
    );
    assert_eq!(content(&old), original);
    drop(old);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "explicit local manifest budget lifecycle campaign"]
fn manifest_budget_lifecycle_campaign() {
    for case in 0..64 {
        let text = (0..case)
            .map(|i| format!("word{i}\" \\ \u{e9} "))
            .collect::<String>();
        assert_build_boundaries("manifest_budget_campaign", &text);
    }
}
