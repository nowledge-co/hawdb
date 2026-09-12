use super::*;
use std::fs;
use std::path::PathBuf;

const PAYLOAD: &[u8] = b"123456789";

fn storage_error<T: std::fmt::Debug>(result: Result<T>) -> String {
    let error = result.expect_err("invalid binding admitted");
    assert!(matches!(&error, SkeinError::Storage(_)), "{error:?}");
    error.to_string()
}

#[test]
fn frozen_metadata_and_integrity_failure_order_are_preserved() {
    let metadata = DurableArtifactMetadata::for_bytes(PAYLOAD);
    assert_eq!(metadata.encoded_len, 9);
    assert_eq!(metadata.encoded_checksum, 0xe306_9283);
    assert_eq!(
        metadata.encoded_sha256.to_string(),
        "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225"
    );
    verify_integrity(
        PAYLOAD,
        9,
        metadata.encoded_checksum,
        metadata.encoded_sha256,
        "fixture",
    )
    .unwrap();
    let wrong_sha = Sha256Digest::from_bytes([0; 32]);
    assert!(
        storage_error(verify_integrity(PAYLOAD, 8, 0, wrong_sha, "fixture"))
            .contains("fixture encoded length mismatch: expected 8, got 9")
    );
    assert!(
        storage_error(verify_integrity(PAYLOAD, 9, 0, wrong_sha, "fixture"))
            .contains("fixture CRC32C mismatch: expected 0")
    );
    assert!(storage_error(verify_integrity(
        PAYLOAD,
        9,
        metadata.encoded_checksum,
        wrong_sha,
        "fixture"
    ))
    .contains("fixture SHA-256 mismatch"));
    // The bound uses u64 even though the actual CRC is 32 bits.
    assert!(storage_error(verify_integrity(
        PAYLOAD,
        9,
        metadata.encoded_checksum + (1 << 32),
        metadata.encoded_sha256,
        "fixture"
    ))
    .contains("CRC32C mismatch"));
}

#[test]
fn rejected_admission_preserves_the_aggregate_budget() {
    let mut budget = GraphManifestOpenBudget::new(10);
    admit_graph_manifest_binding(6, 6, "first", &mut budget).unwrap();
    assert!(
        storage_error(admit_graph_manifest_binding(5, 5, "second", &mut budget))
            .contains("second requires 11 aggregate encoded graph manifest bytes")
    );
    assert_eq!(budget.admitted_encoded_bytes, 6);
    admit_graph_manifest_binding(4, 4, "third", &mut budget).unwrap();
    admit_graph_manifest_binding(0, 0, "empty", &mut budget).unwrap();
    assert_eq!(budget.admitted_encoded_bytes, 10);

    let mut budget = GraphManifestOpenBudget::new(u64::MAX);
    admit_graph_manifest_binding(u64::MAX, u64::MAX, "maximum", &mut budget).unwrap();
    assert!(
        storage_error(admit_graph_manifest_binding(1, 0, "format", &mut budget))
            .contains("format exceeds format limit 0 bytes")
    );
    assert!(
        storage_error(admit_graph_manifest_binding(1, 1, "overflow", &mut budget))
            .contains("aggregate graph manifest open bytes overflow u64")
    );
    assert_eq!(budget.admitted_encoded_bytes, u64::MAX);
}

#[test]
fn admission_and_read_limit_fail_before_file_open_without_refunds() {
    let directory = TestDirectory::new();
    let missing = directory.0.join("missing");
    let metadata = DurableArtifactMetadata::for_bytes(PAYLOAD);
    let mut budget = GraphManifestOpenBudget::new(8);
    assert!(storage_error(read_bound_graph_manifest(
        &missing,
        9,
        metadata.encoded_checksum,
        metadata.encoded_sha256,
        8,
        "manifest",
        &mut budget
    ))
    .contains("exceeds format limit"));
    assert_eq!(budget.admitted_encoded_bytes, 0);
    assert!(storage_error(read_bound_graph_manifest(
        &missing,
        9,
        metadata.encoded_checksum,
        metadata.encoded_sha256,
        9,
        "manifest",
        &mut budget
    ))
    .contains("exceeding configured limit 8"));
    assert_eq!(budget.admitted_encoded_bytes, 0);

    let mut budget = GraphManifestOpenBudget::new(u64::MAX);
    assert!(storage_error(read_bound_graph_manifest(
        &missing,
        u64::MAX,
        0,
        metadata.encoded_sha256,
        u64::MAX,
        "manifest",
        &mut budget
    ))
    .contains("manifest read limit overflows u64"));
    assert_eq!(budget.admitted_encoded_bytes, u64::MAX);

    let mut budget = GraphManifestOpenBudget::new(9);
    assert!(read_bound_graph_manifest(
        &missing,
        9,
        metadata.encoded_checksum,
        metadata.encoded_sha256,
        9,
        "manifest",
        &mut budget
    )
    .is_err());
    assert_eq!(budget.admitted_encoded_bytes, 9);
    assert!(!missing.exists());
}

#[test]
fn file_length_corruption_and_sparse_growth_fail_without_writes() {
    let directory = TestDirectory::new();
    let path = directory.0.join("manifest");
    let metadata = DurableArtifactMetadata::for_bytes(PAYLOAD);
    for (bytes, expected) in [
        (PAYLOAD, None),
        (&PAYLOAD[..8], Some("encoded length mismatch")),
        (b"123456780".as_slice(), Some("CRC32C mismatch")),
        (
            b"1234567890".as_slice(),
            Some("exceeding its admitted bound"),
        ),
    ] {
        fs::write(&path, bytes).unwrap();
        let mut budget = GraphManifestOpenBudget::new(9);
        let result = read_bound_graph_manifest(
            &path,
            9,
            metadata.encoded_checksum,
            metadata.encoded_sha256,
            9,
            "manifest",
            &mut budget,
        );
        match expected {
            Some(message) => assert!(storage_error(result).contains(message)),
            None => assert_eq!(result.unwrap(), PAYLOAD),
        }
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(budget.admitted_encoded_bytes, 9);
    }
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(1024 * 1024).unwrap();
    let mut budget = GraphManifestOpenBudget::new(9);
    assert!(storage_error(read_bound_graph_manifest(
        &path,
        9,
        metadata.encoded_checksum,
        metadata.encoded_sha256,
        9,
        "manifest",
        &mut budget
    ))
    .contains("contains 1048576 bytes, exceeding its admitted bound 9"));
    assert_eq!(fs::metadata(&path).unwrap().len(), 1024 * 1024);
}

#[test]
fn artifact_binding_state_machine_smoke() {
    assert_eq!(campaign(4, 16), 84);
}

#[test]
#[ignore = "explicit local artifact binding state machine campaign"]
fn artifact_binding_state_machine_campaign() {
    assert_eq!(campaign(128, 128), 17024);
}

fn campaign(seeds: u64, steps: usize) -> usize {
    let directory = TestDirectory::new();
    let path = directory.0.join("manifest");
    let mut cases = 0;
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        let mut max = rng.boundary();
        let mut budget = GraphManifestOpenBudget::new(max);
        let mut admitted = 0u128;
        for step in 0..steps {
            if step % 16 == 0 {
                max = rng.boundary();
                budget = GraphManifestOpenBudget::new(max);
                admitted = 0;
            }
            let length = rng.boundary();
            let format_max = rng.boundary();
            let required = admitted + u128::from(length);
            let expected = if length > format_max {
                Some("exceeds format limit")
            } else if required > u128::from(u64::MAX) {
                Some("aggregate graph manifest open bytes overflow u64")
            } else if required > u128::from(max) {
                Some("exceeding configured limit")
            } else {
                None
            };
            let result = admit_graph_manifest_binding(length, format_max, "manifest", &mut budget);
            match expected {
                Some(message) => assert!(storage_error(result).contains(message)),
                None => {
                    result.unwrap();
                    admitted = required;
                }
            }
            assert_eq!(u128::from(budget.admitted_encoded_bytes), admitted);
            cases += 1;
        }

        let payload = (0..1 + rng.next() % 96)
            .map(|_| rng.next() as u8)
            .collect::<Vec<_>>();
        let metadata = DurableArtifactMetadata::for_bytes(&payload);
        assert_eq!(metadata.encoded_len, payload.len() as u64);
        assert_eq!(
            metadata.encoded_checksum,
            u64::from(reference_crc32c(&payload))
        );
        let mut wrong_sha = metadata.encoded_sha256.to_string();
        wrong_sha.replace_range(..1, if wrong_sha.starts_with('0') { "1" } else { "0" });
        let wrong_sha = wrong_sha.parse().unwrap();
        for (length, crc, sha, expected) in [
            (
                metadata.encoded_len,
                metadata.encoded_checksum,
                metadata.encoded_sha256,
                None,
            ),
            (
                metadata.encoded_len + 1,
                metadata.encoded_checksum ^ 1,
                wrong_sha,
                Some("encoded length mismatch"),
            ),
            (
                metadata.encoded_len,
                metadata.encoded_checksum ^ 1,
                wrong_sha,
                Some("CRC32C mismatch"),
            ),
            (
                metadata.encoded_len,
                metadata.encoded_checksum,
                wrong_sha,
                Some("SHA-256 mismatch"),
            ),
        ] {
            let result = verify_integrity(&payload, length, crc, sha, "manifest");
            match expected {
                Some(message) => assert!(storage_error(result).contains(message)),
                None => result.unwrap(),
            }
            cases += 1;
        }
        fs::write(&path, &payload).unwrap();
        let mut budget = GraphManifestOpenBudget::new(metadata.encoded_len);
        assert_eq!(
            read_bound_graph_manifest(
                &path,
                metadata.encoded_len,
                metadata.encoded_checksum,
                metadata.encoded_sha256,
                metadata.encoded_len,
                "manifest",
                &mut budget
            )
            .unwrap(),
            payload
        );
        assert_eq!(budget.admitted_encoded_bytes, metadata.encoded_len);
        cases += 1;
    }
    eprintln!("artifact-binding-state-machine-v1 seeds={seeds} steps={steps} cases={cases}");
    cases
}

fn reference_crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 == 0 { 0 } else { 0x82f6_3b78 };
        }
    }
    !crc
}

struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn boundary(&mut self) -> u64 {
        match self.next() % 7 {
            0 => 0,
            1 => 1,
            2 => 2,
            3 => 17,
            4 => u64::MAX,
            5 => u64::MAX - 1,
            _ => self.next() % 4096,
        }
    }
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "skein-artifact-binding-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
