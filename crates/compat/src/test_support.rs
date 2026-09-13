use super::Value;
use std::collections::BTreeMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn write_external_shadow_script(name: &str, content: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = unique_test_path(format!("{name}-{nonce}.sh"));
    fs::write(&path, content).unwrap();
    path.to_string_lossy().into_owned()
}

pub(super) fn unique_test_path(name: impl AsRef<str>) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("skein-{}-{nonce}", name.as_ref()))
}

pub(super) fn row(
    items: impl IntoIterator<Item = (&'static str, Value)>,
) -> BTreeMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
