use super::*;

#[test]
#[ignore = "local committed cleanup admission and retention campaign"]
fn committed_cleanup_campaign() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let mut random = 0x206_c1ea_u64;
    let mut files = 0;
    let mut attempted = 0;
    for case in 0..128 {
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let native = root.join(format!(
            "case-{case}-{}-\u{130}",
            "x".repeat(random as usize % 70)
        ));
        fs::create_dir(&native).unwrap();
        let current = [
            Some(11),
            Some(17),
            if case % 5 == 0 { None } else { Some(9) },
        ];
        let remove_all = case % 4 == 0;
        let generations = SearchProjectionGenerations {
            lexical: current[0],
            out_of_core: current[1],
            rabitq: current[2],
            rabitq_remove_all: remove_all,
            ..Default::default()
        };
        let prefixes = [
            "search_lexical.",
            "search_lexical.manifest.",
            "search_rabitq.",
            "search_projection_segments.",
            "search_projection_segment_payloads.",
            "search_projection_metadata_payloads.",
            "search_projection_vector_payloads.",
            "search_projection_out_of_core_layout.",
        ];
        let mut oracle = Vec::new();
        for index in 0..32 {
            let kind = (index + case) % prefixes.len();
            let generation = if index == 31 { u64::MAX } else { index as u64 };
            let quarantined = index % 7 == 0;
            let suffix = if quarantined { ".corrupt.42.9" } else { "" };
            let name = format!("{}{generation}.skein{suffix}", prefixes[kind]);
            let family = match kind {
                0 | 1 => 0,
                2 => 2,
                _ => 1,
            };
            let obsolete = quarantined
                || (family == 2 && remove_all)
                || current[family].is_some_and(|active: u64| generation < active.saturating_sub(1));
            fs::write(native.join(&name), b"fixture").unwrap();
            oracle.push((name, obsolete));
        }
        for name in [
            "unrelated.txt",
            "search_lexical.overflow.skein",
            "search_rabitq.7.skein.corrupt.invalid.9",
        ] {
            fs::write(native.join(name), b"retained").unwrap();
            oracle.push((name.to_string(), false));
        }
        files += oracle.len();
        let peak = oracle
            .iter()
            .filter(|(_, obsolete)| *obsolete)
            .map(|(name, _)| join_peak(&native, name))
            .max()
            .unwrap();
        let eligible = oracle.iter().filter(|(_, obsolete)| *obsolete).count();
        for limit in [peak + 137 - 1, peak + 137] {
            let memory = memory(limit);
            let competing = memory.input.reserve(137).unwrap();
            let mut calls = 0;
            let result =
                cleanup_with_remover(&native, generations, Default::default(), &memory, |path| {
                    let name = path.file_name().unwrap().to_str().unwrap();
                    assert!(oracle
                        .iter()
                        .any(|(expected, obsolete)| expected == name && *obsolete));
                    calls += 1;
                    Ok(())
                });
            assert_eq!(result.deleted_files, calls);
            if limit == peak + 137 {
                assert_eq!(calls, eligible);
                assert_eq!(result.pending_after, 0);
                assert!(!result.retry_required);
                assert_eq!(memory.ledger.snapshot().peak_bytes, limit);
            } else {
                assert!(calls < eligible);
                assert_eq!(result.pending_after, eligible - calls);
                assert!(result.retry_required);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(competing);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
        let memory = memory(1024 * 1024);
        let competing = memory.input.reserve(137).unwrap();
        let cap = 1 + case % 7;
        let limit = 1 + (random as usize % 15);
        let options = SearchProjectionCleanupOptions {
            max_pending_files: NonZeroUsize::new(cap).unwrap(),
            max_delete_attempts: NonZeroUsize::new(limit).unwrap(),
        };
        let mut calls = 0;
        let mut deleted = 0;
        let result = cleanup_with_remover(&native, generations, options, &memory, |path| {
            calls += 1;
            if case % 3 == 0 {
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
            fs::remove_file(path)?;
            if case % 3 == 1 {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            deleted += 1;
            Ok(())
        });
        assert_eq!(calls, eligible.min(limit));
        attempted += calls;
        assert_eq!(result.deleted_files, deleted);
        let mut remaining = 0;
        for (name, obsolete) in &oracle {
            let exists = native.join(name).exists();
            if !obsolete {
                assert!(exists, "retained artifact removed: {name}");
            }
            remaining += usize::from(*obsolete && exists);
        }
        assert_eq!(result.pending_after, remaining.min(cap));
        assert_eq!(result.retry_required, remaining > 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(competing);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
        fs::remove_dir_all(native).unwrap();
    }
    fs::remove_dir(root).unwrap();
    eprintln!("committed cleanup seed=0x206c1ea cases=128 files={files} exact=128 short=128 io_attempts={attempted}");
}
