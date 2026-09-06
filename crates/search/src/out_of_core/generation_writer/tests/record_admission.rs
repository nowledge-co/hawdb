use super::*;
use crate::document_codec::allocation_evidence;

#[test]
fn every_spool_byte_limit_is_checked_before_encoding_or_writing() {
    let input = document(0);
    let length = encode_search_document_line(&input).len() as u64;
    for boundary in 0..4 {
        let root = test_dir("record_preflight");
        let mut options = SearchOutOfCoreGenerationBuildOptions::default();
        match boundary {
            0 => options.max_record_bytes = NonZeroU64::new(length - 1).unwrap(),
            1 => options.max_logical_document_bytes = NonZeroU64::new(length - 1).unwrap(),
            2 => {
                options.max_spool_bytes = NonZeroU64::new(
                    SPOOL_HEADER.len() as u64 + SPOOL_FRAME_HEADER_BYTES + length - 1,
                )
                .unwrap()
            }
            _ => {
                options.max_metadata_fields =
                    NonZeroUsize::new(required_descriptor_fields().len()).unwrap()
            }
        }
        let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
        let stage = writer.stage.path.clone();
        allocation_evidence::take();
        let mut input = input.clone();
        if boundary == 3 {
            input
                .metadata
                .insert("unadmitted_field".to_string(), "value".to_string());
        }
        assert!(writer.push(input).is_err());
        assert_eq!(allocation_evidence::take(), 0, "boundary {boundary}");
        assert_eq!(writer.document_count, 0);
        assert_eq!(writer.spool_bytes, SPOOL_HEADER.len() as u64);
        writer.spool.as_mut().unwrap().flush().unwrap();
        assert_eq!(fs::read(&writer.spool_path).unwrap(), SPOOL_HEADER);
        assert!(writer.finish().is_err());
        assert!(!stage.exists());
        assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn accumulated_spool_limits_are_rechecked_before_the_next_record_allocation() {
    let first = document(0);
    let length = encode_search_document_line(&first).len() as u64;
    for spool_limit in [false, true] {
        let root = test_dir("record_remaining_admission");
        let mut options = SearchOutOfCoreGenerationBuildOptions::default();
        if spool_limit {
            options.max_spool_bytes =
                NonZeroU64::new(SPOOL_HEADER.len() as u64 + SPOOL_FRAME_HEADER_BYTES + length)
                    .unwrap();
        } else {
            options.max_logical_document_bytes = NonZeroU64::new(length).unwrap();
        }
        let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
        writer.push(first.clone()).unwrap();
        allocation_evidence::take();
        assert!(writer.push(document(1)).is_err());
        assert_eq!(allocation_evidence::take(), 0);
        assert_eq!(writer.document_count, 1);
        assert!(writer.finish().is_err());
        assert_eq!(stage_directories(&root), 0);
        assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn exact_record_boundary_publishes_and_rechecksummed_non_ascii_spool_is_rejected() {
    let input = document(0);
    let length = encode_search_document_line(&input).len() as u64;
    let root = test_dir("record_exact_and_corrupt");
    let options = SearchOutOfCoreGenerationBuildOptions {
        max_record_bytes: NonZeroU64::new(length).unwrap(),
        max_logical_document_bytes: NonZeroU64::new(length).unwrap(),
        max_spool_bytes: NonZeroU64::new(
            SPOOL_HEADER.len() as u64 + SPOOL_FRAME_HEADER_BYTES + length,
        )
        .unwrap(),
        ..Default::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
    writer.push(input.clone()).unwrap();
    let report = writer.finish().unwrap();
    assert_eq!(report.peak_record_bytes, length);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(input).unwrap();
    let error = writer
        .finish_with_artifacts(|_, source, _| {
            let corrupt = "doc\t0\u{00e9}a\t\t\t\t\n".as_bytes();
            let mut spool = SPOOL_HEADER.to_vec();
            spool.extend_from_slice(&(corrupt.len() as u64).to_le_bytes());
            spool.extend_from_slice(&checksum_bytes(corrupt).to_le_bytes());
            spool.extend_from_slice(corrupt);
            fs::write(&source.path, spool)?;
            source.scan(&mut |_, _| panic!("corrupt record reached a sink"))?;
            panic!("corrupt spool accepted");
        })
        .unwrap_err();
    assert!(error.to_string().contains("invalid hex string"), "{error}");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}
