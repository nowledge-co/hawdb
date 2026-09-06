use super::*;

#[test]
#[ignore = "local RaBitQ admission campaign; run the explicit Bazel fuzz suite"]
fn rabitq_admission_campaign() {
    let mut seed = 0x206_4ab1_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        seed
    };
    let mut documents = 0;
    let mut vectors_checked = 0;
    let mut cancelled = 0;
    let mut coverage = BTreeSet::new();
    for case in 0..500 {
        let dimension = [1, 3, 7, 8, 9, 31, 64, 127][case % 8];
        let count = 1 + next() as usize % 24;
        let bits = if (case / 8) % 2 == 0 {
            RaBitQBitWidth::One
        } else {
            RaBitQBitWidth::Four
        };
        let rows = [1, 2, 7, 1024][(case / 16) % 4];
        coverage.insert((dimension, bits.bits(), rows));
        let mut options = options(rows, bits);
        options.rabitq_transform_seed = next();
        options.source_graph_commit_epoch = Some(next());
        options.embedding_manifest = Some(SearchEmbeddingManifest {
            model: format!(
                "model\0\n\t\u{0130}\u{1f680}{}",
                "x\"\\".repeat(next() as usize % 60)
            ),
            version: if case % 3 == 0 {
                None
            } else {
                Some(format!("version-{}", next()))
            },
            dimension,
        });
        let mut inputs = vectors(count, dimension);
        for (index, document) in inputs.iter_mut().enumerate() {
            if index > 0 && next().is_multiple_of(4) {
                document.embedding = None;
            } else {
                for value in document.embedding.as_mut().unwrap() {
                    let raw = f32::from_bits(next() as u32);
                    *value = if case % 7 == 0 || !raw.is_finite() {
                        0.0
                    } else {
                        raw
                    };
                }
                vectors_checked += 1;
            }
        }
        documents += inputs.len();
        let peak = trial(&inputs, &options, 16 * 1024 * 1024, true);
        assert_eq!(trial(&inputs, &options, peak, true), peak);
        trial(&inputs, &options, peak - 1, false);

        if case % 32 == 0 {
            let root = test_dir("rabitq_cancelled_campaign");
            let input = input(&root, 16 * 1024 * 1024, options, &inputs);
            let ledger = input.memory.ledger.clone();
            let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
            for document in &inputs[..inputs.len() / 2] {
                builder.push(document).unwrap();
            }
            input.task_context.cancellation().cancel();
            rabitq::evidence::take();
            assert!(builder
                .push(inputs.last().unwrap())
                .unwrap_err()
                .to_string()
                .contains("cancelled"));
            assert!(builder
                .finish()
                .unwrap_err()
                .to_string()
                .contains("cancelled"));
            assert_eq!(rabitq::evidence::take(), (0, 0, 0));
            drop(input);
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert_eq!(stage_directories(&root), 0);
            fs::remove_dir_all(root).unwrap();
            cancelled += 1;
        }
    }
    assert!(documents > 5000);
    assert!(vectors_checked > 3000);
    assert_eq!(cancelled, 16);
    assert_eq!(coverage.len(), 64);
    eprintln!("rabitq seed=0x2064ab1 cases=500 documents={documents} vectors={vectors_checked} configurations=64 exact=500 short=500 cancelled={cancelled}");
}
