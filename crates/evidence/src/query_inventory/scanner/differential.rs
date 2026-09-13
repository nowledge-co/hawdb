use super::*;
use crate::query_inventory::build_compatibility_query_inventory_from_json;
use std::sync::atomic::{AtomicU64, Ordering};

// Fixed query/family/slug answers: no production lexer, normalizer, classifier,
// or checksum helper participates in the expected inventory.
const QUERIES: [(&str, &str, &str); 5] = [
    (
        "MATCH (m:Memory {id: $id}) RETURN m.id",
        "read",
        "32af8e1208e77877",
    ),
    (
        "MATCH (m:Memory) SET m.seen = true",
        "mutation",
        "201c80767294c1fc",
    ),
    (
        "CREATE RANGE INDEX ON :Memory(created_at)",
        "schema",
        "791944457e89017f",
    ),
    ("CALL page_rank('g')", "procedure", "d561e22365a854ef"),
    (
        "BEGIN TRANSACTION",
        "transaction_control",
        "caa35bff74ccc6e6",
    ),
];

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-inventory-oracle-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn write(&self, relative: &str, content: &str) {
        let path = self.0.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn campaign(cases: usize) {
    let root = Scratch::new();
    for excluded in [
        "crates/nmem-content/src/lib.rs",
        "upstream_forks/graph/src/lib.rs",
        "crates/host/tests/query.rs",
        "crates/host/benches/query.rs",
        "crates/host/src/bin/query.rs",
        "target/generated.rs",
        ".git/query.rs",
        "crates/host/src/query.txt",
    ] {
        root.write(excluded, r#""MATCH (excluded) RETURN excluded""#);
    }
    for seed in 0..cases {
        let mut expected = Vec::new();
        // Write in reverse filename order; the inventory must remain sorted
        // by source path and then literal occurrence.
        for file in ["crates/host/src/z.rs", "crates/host/src/a.rs"] {
            let mut source = "\n".repeat(seed % 7);
            source.push_str(
                "// \"MATCH (comment) RETURN comment\"\n/* \"ROLLBACK\" */\n\
                 fn lifetime<'a>(x: &'a str) {}\nlet brace = '}';\n",
            );
            source.push_str("#[cfg(test)]\nmod hidden {\n");
            for _ in 0..(seed / 4) % 4 {
                source.push_str("mod nested {\n");
            }
            source.push_str(
                "let ignored = \"MATCH (hidden) RETURN hidden\";\n\
                 let raw = r##\"} { \\\" \"##;\n\
                 let brace = '}'; // }\n/* } */\n",
            );
            for _ in 0..(seed / 4) % 4 {
                source.push_str("}\n");
            }
            source.push_str("}\n");
            let mut file_expected = Vec::new();
            for (index, (query, family, slug)) in QUERIES.iter().enumerate() {
                let line = source.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let literal = match (seed + index) % 4 {
                    0 => format!("\"{};\"", query.replace(' ', "\\n")),
                    1 => format!("b\"{query}\""),
                    2 => format!("r###\"  {query};  \"###"),
                    _ => format!("br#\"{}\"#", query.replace(' ', "\n")),
                };
                source.push_str(&format!("let query_{index} = {literal};\n"));
                file_expected.push(super::super::CompatibilityQueryInventoryItem {
                    name: format!("{file}:{line}:{slug}"),
                    query_family: (*family).to_string(),
                    source: Some(format!("{file}:{line}")),
                    cypher: Some((*query).to_string()),
                });
            }
            source.push_str(
                "let template = \"MATCH (m) {clause} RETURN m\";\n\
                 let fragment = \"MATCH (m)\";\n\
                 let sql = \"CREATE TABLE t(id INT)\";\n",
            );
            root.write(file, &source);
            file_expected.extend(expected);
            expected = file_expected;
        }
        let actual = scan_nowledge_query_inventory_with_options(
            &root.0,
            NowledgeInventoryScanOptions {
                inventory_name: format!("case-{seed}"),
            },
        )
        .unwrap();
        assert_eq!(actual.name, format!("case-{seed}"));
        assert_eq!(actual.required_checks, expected, "seed {seed}");
        let artifact = compatibility_query_inventory_to_json(&actual);
        assert_eq!(
            build_compatibility_query_inventory_from_json(&artifact).unwrap(),
            actual,
            "artifact seed {seed}"
        );
        let default = scan_nowledge_query_inventory_to_json(&root.0).unwrap();
        assert_eq!(default["name"], "nowledge-scanned-inventory");
        assert_eq!(default["required_checks"], artifact["required_checks"]);
    }
}

#[test]
fn source_inventory_differential_smoke() {
    campaign(4);
}

#[test]
#[ignore = "bounded local source inventory differential campaign"]
fn source_inventory_differential_campaign() {
    // Exhaust all literal encodings, nesting depths, and leading-line offsets.
    campaign(112);
}
