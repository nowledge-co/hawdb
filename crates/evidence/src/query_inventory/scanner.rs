use super::{
    build_compatibility_query_inventory, compatibility_query_inventory_to_json,
    CompatibilityQueryCallSite, CompatibilityQueryInventory,
};
use skein_core::error::{Result, SkeinError};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_INVENTORY_NAME: &str = "nowledge-scanned-inventory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeInventoryScanOptions {
    pub inventory_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RustStringLiteral {
    value: String,
    line: usize,
}

impl Default for NowledgeInventoryScanOptions {
    fn default() -> Self {
        Self {
            inventory_name: DEFAULT_INVENTORY_NAME.to_string(),
        }
    }
}

pub fn scan_nowledge_query_inventory(
    root: impl AsRef<Path>,
) -> Result<CompatibilityQueryInventory> {
    scan_nowledge_query_inventory_with_options(root, NowledgeInventoryScanOptions::default())
}

pub fn scan_nowledge_query_inventory_with_options(
    root: impl AsRef<Path>,
    options: NowledgeInventoryScanOptions,
) -> Result<CompatibilityQueryInventory> {
    let root = root.as_ref();
    let mut files = Vec::new();
    collect_rust_files(root, &mut files)?;
    files.sort();

    let mut call_sites = Vec::new();
    for file in files {
        let content = fs::read_to_string(&file).map_err(|_| {
            SkeinError::Execution("failed to read inventory source file: io_error".to_string())
        })?;
        let relative = file.strip_prefix(root).unwrap_or(&file);
        let source_file = path_to_slash_string(relative);
        if !scan_source_file(&source_file) {
            continue;
        }
        let production_content = strip_cfg_test_modules(&content);
        for literal in extract_rust_string_literals(&production_content)? {
            let Some(cypher) = normalize_cypher_literal(&literal.value) else {
                continue;
            };
            let query_family = classify_query_family(&cypher);
            let name = format!(
                "{}:{}:{}",
                source_file,
                literal.line,
                stable_query_slug(&cypher)
            );
            call_sites.push(
                CompatibilityQueryCallSite::new(
                    name,
                    query_family,
                    format!("{source_file}:{}", literal.line),
                )
                .with_cypher(cypher),
            );
        }
    }

    build_compatibility_query_inventory(options.inventory_name, call_sites)
}

fn scan_source_file(source_file: &str) -> bool {
    if source_file.starts_with("crates/nmem-content/") {
        return false;
    }
    if source_file.starts_with("upstream_forks/") {
        return false;
    }
    let parts = source_file.split('/').collect::<Vec<_>>();
    if parts
        .iter()
        .any(|part| *part == "tests" || *part == "benches")
    {
        return false;
    }
    if source_file.contains("/src/bin/") {
        return false;
    }
    true
}

pub fn scan_nowledge_query_inventory_to_json(root: impl AsRef<Path>) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    Ok(compatibility_query_inventory_to_json(&inventory))
}

fn collect_rust_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = fs::metadata(root).map_err(|_| {
        SkeinError::Execution("failed to stat inventory path: io_error".to_string())
    })?;
    if metadata.is_file() {
        if root.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            output.push(root.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(root).map_err(|_| {
        SkeinError::Execution("failed to read inventory directory: io_error".to_string())
    })? {
        let entry = entry.map_err(|_| {
            SkeinError::Execution("failed to read inventory directory entry: io_error".to_string())
        })?;
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if file_name == "target" || file_name == ".git" {
            continue;
        }
        let metadata = entry.metadata().map_err(|_| {
            SkeinError::Execution("failed to stat inventory directory entry: io_error".to_string())
        })?;
        if metadata.is_dir() {
            collect_rust_files(&path, output)?;
        } else if metadata.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("rs")
        {
            output.push(path);
        }
    }
    Ok(())
}

fn extract_rust_string_literals(content: &str) -> Result<Vec<RustStringLiteral>> {
    let bytes = content.as_bytes();
    let mut literals = Vec::new();
    let mut index = 0;
    let mut line = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => {
                line += 1;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'\n' {
                        line += 1;
                    }
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b'\'' if looks_like_char_literal(bytes, index) => {
                let (next, newlines) = skip_char_literal(content, index)?;
                line += newlines;
                index = next;
            }
            b'\'' => {
                index += 1;
            }
            b'b' if bytes.get(index + 1) == Some(&b'"') => {
                let start_line = line;
                let (value, next, newlines) = parse_cooked_string(content, index + 1)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'b' if bytes.get(index + 1) == Some(&b'r')
                && raw_string_start(bytes, index + 1).is_some() =>
            {
                let start_line = line;
                let (value, next, newlines) = parse_raw_string(content, index + 1)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'"' => {
                let start_line = line;
                let (value, next, newlines) = parse_cooked_string(content, index)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            b'r' if raw_string_start(bytes, index).is_some() => {
                let start_line = line;
                let (value, next, newlines) = parse_raw_string(content, index)?;
                literals.push(RustStringLiteral {
                    value,
                    line: start_line,
                });
                line += newlines;
                index = next;
            }
            _ => {
                index += 1;
            }
        }
    }
    Ok(literals)
}

fn strip_cfg_test_modules(content: &str) -> String {
    let mut output = content.as_bytes().to_vec();
    let mut search_start = 0;
    while let Some(relative_start) = content[search_start..].find("#[cfg(test)]") {
        let attribute_start = search_start + relative_start;
        let after_attribute = attribute_start + "#[cfg(test)]".len();
        let Some(module_start) = cfg_test_module_start(content, after_attribute) else {
            search_start = after_attribute;
            continue;
        };
        let Some(module_end) = find_matching_rust_brace(content, module_start) else {
            break;
        };
        for byte in &mut output[attribute_start..=module_end] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
        search_start = module_end + 1;
    }
    String::from_utf8(output).expect("ASCII masking preserves valid UTF-8")
}

fn cfg_test_module_start(content: &str, after_attribute: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut index = skip_ascii_whitespace(bytes, after_attribute);
    if !bytes.get(index..)?.starts_with(b"mod") {
        return None;
    }
    index += b"mod".len();
    if !bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        return None;
    }
    index = skip_ascii_whitespace(bytes, index);
    let ident_start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    if index == ident_start {
        return None;
    }
    index = skip_ascii_whitespace(bytes, index);
    if bytes.get(index) == Some(&b'{') {
        Some(index)
    } else {
        None
    }
}

fn skip_ascii_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    index
}

fn find_matching_rust_brace(content: &str, open_brace: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut index = open_brace;
    let mut depth = 0_usize;
    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b'\'' if looks_like_char_literal(bytes, index) => {
                let (next, _) = skip_char_literal(content, index).ok()?;
                index = next;
            }
            b'\'' => {
                index += 1;
            }
            b'b' if bytes.get(index + 1) == Some(&b'"') => {
                let (_, next, _) = parse_cooked_string(content, index + 1).ok()?;
                index = next;
            }
            b'b' if bytes.get(index + 1) == Some(&b'r')
                && raw_string_start(bytes, index + 1).is_some() =>
            {
                let (_, next, _) = parse_raw_string(content, index + 1).ok()?;
                index = next;
            }
            b'"' => {
                let (_, next, _) = parse_cooked_string(content, index).ok()?;
                index = next;
            }
            b'r' if raw_string_start(bytes, index).is_some() => {
                let (_, next, _) = parse_raw_string(content, index).ok()?;
                index = next;
            }
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    None
}

fn skip_char_literal(content: &str, start: usize) -> Result<(usize, usize)> {
    let bytes = content.as_bytes();
    let mut index = start + 1;
    let mut newlines = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => return Ok((index + 1, newlines)),
            b'\\' => {
                index = (index + 2).min(bytes.len());
            }
            b'\n' => {
                newlines += 1;
                index += 1;
            }
            _ => index += 1,
        }
    }
    Err(SkeinError::Semantic(format!(
        "unterminated Rust char literal at byte {start}"
    )))
}

fn looks_like_char_literal(bytes: &[u8], start: usize) -> bool {
    let Some(next) = bytes.get(start + 1) else {
        return false;
    };
    if *next == b'\\' {
        return bytes[start + 2..].iter().take(8).any(|byte| *byte == b'\'');
    }
    bytes.get(start + 2) == Some(&b'\'')
}

fn parse_cooked_string(content: &str, start: usize) -> Result<(String, usize, usize)> {
    let bytes = content.as_bytes();
    let mut index = start + 1;
    let mut value = String::new();
    let mut newlines = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Ok((value, index + 1, newlines)),
            b'\\' => {
                index += 1;
                if index >= bytes.len() {
                    break;
                }
                match bytes[index] {
                    b'n' => value.push('\n'),
                    b'r' => value.push('\r'),
                    b't' => value.push('\t'),
                    b'\\' => value.push('\\'),
                    b'"' => value.push('"'),
                    b'\n' => newlines += 1,
                    other => value.push(other as char),
                }
                index += 1;
            }
            b'\n' => {
                value.push('\n');
                newlines += 1;
                index += 1;
            }
            byte => {
                value.push(byte as char);
                index += 1;
            }
        }
    }
    Err(SkeinError::Semantic(format!(
        "unterminated Rust string literal at byte {start}"
    )))
}

fn parse_raw_string(content: &str, start: usize) -> Result<(String, usize, usize)> {
    let bytes = content.as_bytes();
    let hashes = raw_string_start(bytes, start).expect("caller checked raw string start");
    let body_start = start + 2 + hashes;
    let terminator = format!("\"{}", "#".repeat(hashes));
    let rest = &content[body_start..];
    let Some(offset) = rest.find(&terminator) else {
        return Err(SkeinError::Semantic(format!(
            "unterminated Rust raw string literal at byte {start}"
        )));
    };
    let value = rest[..offset].to_string();
    let newlines = value.bytes().filter(|byte| *byte == b'\n').count();
    Ok((value, body_start + offset + terminator.len(), newlines))
}

fn raw_string_start(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'r') {
        return None;
    }
    let mut index = start + 1;
    let mut hashes = 0;
    while bytes.get(index) == Some(&b'#') {
        hashes += 1;
        index += 1;
    }
    if bytes.get(index) == Some(&b'"') {
        Some(hashes)
    } else {
        None
    }
}

fn normalize_cypher_literal(value: &str) -> Option<String> {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .trim()
        .to_string();
    if normalized.is_empty() || !looks_like_cypher(&normalized) {
        return None;
    }
    if looks_like_incomplete_match_fragment(&normalized) {
        return None;
    }
    if contains_unresolved_rust_format_placeholder(&normalized) {
        return None;
    }
    Some(normalized)
}

fn looks_like_incomplete_match_fragment(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    if !upper.starts_with("MATCH ") {
        return false;
    }
    ![
        " RETURN ",
        " WITH ",
        " SET ",
        " CREATE ",
        " MERGE ",
        " DELETE ",
        " DETACH DELETE ",
        " CALL ",
    ]
    .iter()
    .any(|marker| upper.contains(marker))
}

fn contains_unresolved_rust_format_placeholder(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut index = 0;
    let mut in_single_quote = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_single_quote && bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                } else {
                    in_single_quote = !in_single_quote;
                    index += 1;
                }
            }
            b'{' if bytes.get(index + 1) == Some(&b'{') => {
                index += 2;
            }
            b'}' if bytes.get(index + 1) == Some(&b'}') => {
                index += 2;
            }
            b'{' => {
                let content_start = index + 1;
                let Some(close_offset) = query[content_start..].find('}') else {
                    return true;
                };
                let content = query[content_start..content_start + close_offset].trim();
                if in_single_quote && content.is_empty() {
                    index = content_start + close_offset + 1;
                    continue;
                }
                if looks_like_rust_format_placeholder(content) {
                    return true;
                }
                index = content_start + close_offset + 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    false
}

fn looks_like_rust_format_placeholder(content: &str) -> bool {
    if content.is_empty() {
        return true;
    }
    let (head, format_spec) = content
        .split_once(':')
        .map(|(head, spec)| (head.trim(), Some(spec.trim_start())))
        .unwrap_or((content, None));
    let mut chars = head.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return false;
    }
    let Some(format_spec) = format_spec else {
        return true;
    };
    format_spec
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, '?' | '#' | '<' | '>' | '^' | '0' | '.' | '1'..='9'))
}

fn looks_like_cypher(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let starts_like_cypher = upper.starts_with("MATCH ")
        || upper.starts_with("MERGE (")
        || upper.starts_with("CREATE (")
        || upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE RELATIONSHIP ")
        || graph_index_ddl(&upper)
        || graph_procedure_call(&upper)
        || is_transaction_control(query);
    if !starts_like_cypher {
        return false;
    }
    upper.contains('(') || graph_procedure_call(&upper) || is_transaction_control(query)
}

fn classify_query_family(query: &str) -> &'static str {
    let upper = query.to_ascii_uppercase();
    if graph_procedure_call(&upper) {
        "procedure"
    } else if upper.starts_with("CREATE NODE ")
        || upper.starts_with("CREATE RELATIONSHIP ")
        || graph_index_ddl(&upper)
    {
        "schema"
    } else if is_transaction_control(query) {
        "transaction_control"
    } else if upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || upper.contains(" CREATE ")
        || upper.contains(" MERGE ")
        || upper.contains(" SET ")
        || upper.contains(" DELETE ")
        || upper.contains(" DETACH DELETE ")
    {
        "mutation"
    } else {
        "read"
    }
}

fn is_transaction_control(query: &str) -> bool {
    matches!(
        query,
        "BEGIN TRANSACTION" | "COMMIT" | "ROLLBACK" | "CHECKPOINT"
    )
}

fn graph_index_ddl(upper: &str) -> bool {
    (upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE RANGE INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX "))
        && upper.contains(" ON :")
}

fn graph_procedure_call(upper: &str) -> bool {
    matches!(
        procedure_name(upper).as_deref(),
        Some("PROJECT_GRAPH" | "PAGE_RANK" | "PAGERANK" | "LOUVAIN")
    )
}

fn procedure_name(upper: &str) -> Option<String> {
    let rest = upper.strip_prefix("CALL ")?;
    let name = rest
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn stable_query_slug(query: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in query.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod differential;
#[cfg(test)]
mod tests;
