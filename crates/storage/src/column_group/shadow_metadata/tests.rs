use super::*;
use std::path::PathBuf;

mod differential;

fn unique_shadow_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("skein_columnar_shadow_{name}_{nanos}"))
}

#[test]
fn metadata_budget_is_charged_before_dictionary_serialization() {
    let root = unique_shadow_dir("dictionary_serialization_budget");
    fs::create_dir_all(&root).unwrap();
    let mut budget = ShadowMetadataBudget::new(1024, 0).unwrap();
    let mut dictionary = ShadowKeyDictionary::default();
    dictionary.intern("body", &mut budget).unwrap();
    let resident_bytes = budget.used_bytes();
    let encoded_bytes = dictionary.encoded_len().unwrap();
    budget.limit_bytes = resident_bytes + encoded_bytes - 1;

    let error = dictionary.persist(&root, &mut budget).unwrap_err();
    assert!(error.to_string().contains("key dictionary serialization"));
    assert_eq!(budget.used_bytes(), resident_bytes);
    assert!(!root.join(SHADOW_KEY_DICTIONARY_FILE).exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn shadow_key_dictionary_persists_append_only_and_fails_closed_on_corruption() {
    let root = unique_shadow_dir("dictionary");
    fs::create_dir_all(&root).unwrap();
    let (mut dictionary, mut metadata_budget) =
        ShadowKeyDictionary::load(&root, DEFAULT_SHADOW_METADATA_BUDGET_BYTES).unwrap();
    assert_eq!(dictionary.len(), 0);
    let name = dictionary.intern("name", &mut metadata_budget).unwrap();
    let age = dictionary.intern("age", &mut metadata_budget).unwrap();
    assert_eq!(name, PropertyId(FIRST_DICTIONARY_COLUMN));
    assert_eq!(age, PropertyId(FIRST_DICTIONARY_COLUMN + 1));
    assert_eq!(
        dictionary.intern("name", &mut metadata_budget).unwrap(),
        name
    );
    dictionary.persist(&root, &mut metadata_budget).unwrap();

    let (mut reloaded, mut reload_budget) =
        ShadowKeyDictionary::load(&root, DEFAULT_SHADOW_METADATA_BUDGET_BYTES).unwrap();
    assert_eq!(reloaded.key(name), Some("name"));
    assert_eq!(reloaded.key(age), Some("age"));
    assert_eq!(reloaded.key(PropertyId(0)), None);
    // Ids are stable across reload-and-extend.
    assert_eq!(reloaded.intern("age", &mut reload_budget).unwrap(), age);
    assert_eq!(
        reloaded.intern("score", &mut reload_budget).unwrap(),
        PropertyId(FIRST_DICTIONARY_COLUMN + 2)
    );

    let path = root.join(SHADOW_KEY_DICTIONARY_FILE);
    let mut bytes = fs::read(&path).unwrap();
    let flip = bytes.len() / 2;
    bytes[flip] ^= 0x01;
    fs::write(&path, &bytes).unwrap();
    assert!(ShadowKeyDictionary::load(&root, DEFAULT_SHADOW_METADATA_BUDGET_BYTES).is_err());
    fs::remove_dir_all(root).unwrap();
}
