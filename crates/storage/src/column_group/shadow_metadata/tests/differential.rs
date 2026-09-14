use super::*;

// Independent bitwise CRC32C, deliberately not the production integrity helper.
fn reference_crc(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn frame(body: &[u8]) -> Vec<u8> {
    let mut bytes = b"SKNSHKEY1".to_vec();
    bytes.extend(body);
    bytes.extend((body.len() as u64).to_le_bytes());
    bytes.extend(reference_crc(body).to_le_bytes());
    bytes.extend(b"SKNSHKEY1");
    bytes
}

fn body(keys: &[&[u8]]) -> Vec<u8> {
    let mut bytes = 1u32.to_le_bytes().to_vec();
    bytes.extend((keys.len() as u32).to_le_bytes());
    for key in keys {
        bytes.extend((key.len() as u32).to_le_bytes());
        bytes.extend(*key);
    }
    bytes
}

fn decode(bytes: &[u8]) -> Result<ShadowKeyDictionary> {
    ShadowKeyDictionary::decode(bytes, &mut ShadowMetadataBudget::new(u64::MAX, 0).unwrap())
}

fn storage_error(error: SkeinError) -> String {
    let SkeinError::Storage(message) = error else {
        panic!("unexpected error: {error:?}");
    };
    message
}

#[test]
fn dictionary_v1_framing_and_first_seen_ids_match_independent_bytes() {
    assert_eq!(reference_crc(b"123456789"), 0xe306_9283);
    let keys = [
        "z",
        "",
        "\u{65e5}\u{672c}\u{8a9e}",
        "a\0b",
        "z",
        "\u{1f980}",
    ];
    let mut dictionary = ShadowKeyDictionary::default();
    let mut budget = ShadowMetadataBudget::new(u64::MAX, 0).unwrap();
    assert!(dictionary.is_empty());
    let mut unique = Vec::new();
    for key in keys {
        let expected = match unique.iter().position(|previous| *previous == key) {
            Some(index) => index,
            None => {
                unique.push(key);
                unique.len() - 1
            }
        };
        assert_eq!(
            dictionary.intern(key, &mut budget).unwrap(),
            PropertyId(4 + expected as u32)
        );
    }
    let expected = frame(&body(
        &unique.iter().map(|key| key.as_bytes()).collect::<Vec<_>>(),
    ));
    assert_eq!(
        dictionary
            .encode(dictionary.encoded_len().unwrap())
            .unwrap(),
        expected
    );
    let decoded = decode(&expected).unwrap();
    for (index, key) in unique.iter().enumerate() {
        assert_eq!(decoded.key(PropertyId(4 + index as u32)), Some(*key));
        assert_eq!(decoded.id(key), Some(PropertyId(4 + index as u32)));
    }
    assert!(!dictionary.is_empty());
    assert_eq!(dictionary.len(), unique.len());
    assert_eq!(dictionary.estimated_bytes(), budget.used_bytes());
}

#[test]
fn valid_checksums_do_not_bypass_structural_dictionary_validation() {
    let valid = frame(&body(&[b"first", b"second"]));
    for length in 0..valid.len() {
        assert!(decode(&valid[..length]).is_err(), "truncated at {length}");
    }
    for position in 0..valid.len() {
        let mut corrupt = valid.clone();
        corrupt[position] ^= 1;
        assert!(decode(&corrupt).is_err(), "corrupted at {position}");
    }
    let mut version = body(&[]);
    version[..4].copy_from_slice(&2u32.to_le_bytes());
    let mut count = body(&[b"one"]);
    count[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut length = body(&[b"one"]);
    length[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut trailing = body(&[]);
    trailing.push(0);
    for (invalid, reason) in [
        (version, "unsupported version"),
        (count, "truncated"),
        (length, "truncated"),
        (trailing, "trailing bytes"),
        (body(&[b"same", b"same"]), "repeats a key"),
        (body(&[&[0xff]]), "non-UTF-8 key"),
        (vec![], "truncated"),
    ] {
        assert!(storage_error(decode(&frame(&invalid)).unwrap_err()).contains(reason));
    }
}

#[test]
fn metadata_budget_rejection_and_overflow_preserve_the_previous_account() {
    assert!(ShadowMetadataBudget::new(3, 4).is_err());
    let mut budget = ShadowMetadataBudget::new(10, 3).unwrap();
    budget.charge(7, "exact boundary").unwrap();
    assert_eq!(budget.used_bytes(), 10);
    assert_eq!(budget.peak_bytes(), 10);
    assert!(budget.charge(1, "over budget").is_err());
    assert_eq!(budget.used_bytes(), 10);
    budget.release(7);
    assert_eq!(budget.used_bytes(), 3);
    assert_eq!(budget.peak_bytes(), 10);
    let mut overflowing = ShadowMetadataBudget::new(u64::MAX, u64::MAX).unwrap();
    assert!(storage_error(overflowing.charge(1, "overflow").unwrap_err()).contains("overflows"));
    assert_eq!(overflowing.used_bytes(), u64::MAX);
    assert_eq!(overflowing.peak_bytes(), u64::MAX);
}

#[test]
fn dictionary_load_and_failed_publication_preserve_baseline_and_durable_bytes() {
    let root = unique_shadow_dir("metadata_load_budget");
    fs::create_dir_all(&root).unwrap();
    let mut dictionary = ShadowKeyDictionary::default();
    let mut budget = ShadowMetadataBudget::new(4096, 0).unwrap();
    dictionary.intern("known", &mut budget).unwrap();
    dictionary.persist(&root, &mut budget).unwrap();
    let path = root.join(SHADOW_KEY_DICTIONARY_FILE);
    let published = fs::read(&path).unwrap();
    let (loaded, mut loaded_budget) = ShadowKeyDictionary::load(&root, 7).unwrap();
    let baseline = 2 * ("known".len() as u64 + 48);
    assert_eq!(loaded_budget.used_bytes(), baseline);
    assert_eq!(loaded_budget.peak_bytes(), baseline);
    assert_eq!(loaded_budget.limit_bytes, 2 * 7 + 3 * baseline);
    assert_eq!(loaded.id("known"), dictionary.id("known"));

    let temporary = root.join(format!(".{SHADOW_KEY_DICTIONARY_FILE}.tmp"));
    fs::create_dir(&temporary).unwrap();
    assert!(loaded.persist(&root, &mut loaded_budget).is_err());
    assert_eq!(loaded_budget.used_bytes(), baseline);
    assert_eq!(fs::read(&path).unwrap(), published);
    fs::remove_dir(&temporary).unwrap();

    // A sparse oversized artifact must be rejected before its bytes are read.
    File::create(&path)
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();
    assert_eq!(
        storage_error(ShadowKeyDictionary::load(&root, 7).unwrap_err()),
        "columnar shadow key dictionary exceeds its size limit"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn type_lattice_and_layout_match_scalar_or_residual_reference() {
    let values = [
        Value::Int(7),
        Value::Float(f64::NAN),
        Value::Bool(true),
        Value::String("\u{65e5}\u{672c}\u{8a9e}".into()),
        Value::Binary(vec![1, 2]),
        Value::Null,
        Value::Uuid("00000000-0000-0000-0000-000000000000".parse().unwrap()),
        Value::List(vec![Value::Int(1)]),
        Value::Map(BTreeMap::new()),
    ];
    for (left_kind, left) in values.iter().enumerate() {
        for (right_kind, right) in values.iter().enumerate() {
            let mut dictionary = ShadowKeyDictionary::default();
            let mut types = TablePropertyTypes::default();
            let mut budget = ShadowMetadataBudget::new(4096, 0).unwrap();
            for value in [left, right] {
                types
                    .observe(
                        &BTreeMap::from([("key".into(), value.clone())]),
                        &mut dictionary,
                        &mut budget,
                    )
                    .unwrap();
            }
            let layout = shadow_table_layout(&types, &mut budget).unwrap();
            let typed = left_kind == right_kind && left_kind < 5;
            assert_eq!(
                layout.typed(),
                if typed { &[PropertyId(4)][..] } else { &[] }
            );
            assert_eq!(
                layout.typed_index().get(&PropertyId(4)),
                if typed { Some(&0) } else { None }
            );
            assert_eq!(
                budget.used_bytes(),
                2 * (3 + 48) + 48 + 64 + if typed { 96 } else { 0 }
            );
        }
    }
    let mut dictionary = ShadowKeyDictionary::default();
    let mut types = TablePropertyTypes::default();
    let mut budget = ShadowMetadataBudget::new(4096, 0).unwrap();
    for key in ["z", "a"] {
        types
            .observe(
                &BTreeMap::from([(key.into(), Value::Int(1))]),
                &mut dictionary,
                &mut budget,
            )
            .unwrap();
    }
    let layout = shadow_table_layout(&types, &mut budget).unwrap();
    assert_eq!(layout.typed(), [PropertyId(4), PropertyId(5)]);
    assert_eq!(
        layout.typed_index(),
        &BTreeMap::from([(PropertyId(4), 0), (PropertyId(5), 1)])
    );
}

fn campaign(seeds: u64, steps: usize) {
    for seed in 1..=seeds {
        let mut state = seed;
        let mut dictionary = ShadowKeyDictionary::default();
        let mut budget = ShadowMetadataBudget::new(u64::MAX, 0).unwrap();
        let mut reference: Vec<String> = Vec::new();
        let mut used = 0u64;
        for step in 0..steps {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let key = format!(
                "key-{}-{}",
                state % 17,
                ["", "\u{65e5}\u{672c}\u{8a9e}", "\0", "\u{1f980}"][(state % 4) as usize]
            );
            let previous = reference.iter().position(|known| *known == key);
            let cost = if previous.is_some() {
                0
            } else {
                2 * (key.len() as u64 + 48)
            };
            let reject = previous.is_none() && step % 5 == 0;
            budget.limit_bytes = used + cost - u64::from(reject);
            let result = dictionary.intern(&key, &mut budget);
            if reject {
                assert!(result.is_err(), "seed={seed}, step={step}");
            } else {
                let index = previous.unwrap_or_else(|| {
                    reference.push(key);
                    reference.len() - 1
                });
                assert_eq!(result.unwrap(), PropertyId(4 + index as u32));
                used += cost;
            }
            assert_eq!(budget.used_bytes(), used);
            assert_eq!(budget.peak_bytes(), used);
            assert_eq!(dictionary.estimated_bytes(), used);
            assert_eq!(dictionary.len(), reference.len());
            assert_eq!(dictionary.is_empty(), reference.is_empty());
            let expected = frame(&body(
                &reference
                    .iter()
                    .map(|key| key.as_bytes())
                    .collect::<Vec<_>>(),
            ));
            let actual = dictionary
                .encode(dictionary.encoded_len().unwrap())
                .unwrap();
            assert_eq!(actual, expected, "seed={seed}, step={step}");
            let mut decode_budget = ShadowMetadataBudget::new(used, 0).unwrap();
            let decoded = ShadowKeyDictionary::decode(&actual, &mut decode_budget).unwrap();
            assert_eq!(decode_budget.used_bytes(), used);
            assert_eq!(decoded.keys, reference);
            let mut corrupt = actual;
            let position = (state as usize) % corrupt.len();
            corrupt[position] ^= 1;
            assert!(
                decode(&corrupt).is_err(),
                "seed={seed}, step={step}, position={position}"
            );
        }
    }
    eprintln!(
        "shadow metadata: {seeds} seeds, {} admission/encoding/decoding/corruption cases",
        seeds as usize * steps
    );
}

#[test]
fn shadow_metadata_differential_smoke() {
    campaign(4, 16);
}

#[test]
#[ignore = "deterministic local shadow metadata campaign"]
fn shadow_metadata_differential_campaign() {
    campaign(128, 64);
}
