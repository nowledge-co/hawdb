//! Source-derived query inventories, independent of fixture execution and cutover policy.

use skein_core::error::{Result, SkeinError};
use std::collections::BTreeMap;

mod scanner;
pub use scanner::{
    scan_nowledge_query_inventory, scan_nowledge_query_inventory_to_json,
    scan_nowledge_query_inventory_with_options, NowledgeInventoryScanOptions,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryInventory {
    pub name: String,
    pub required_checks: Vec<CompatibilityQueryInventoryItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryInventoryItem {
    pub name: String,
    pub query_family: String,
    pub source: Option<String>,
    pub cypher: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryCallSite {
    pub name: String,
    pub query_family: String,
    pub source: String,
    pub cypher: Option<String>,
}

impl CompatibilityQueryInventoryItem {
    pub fn new(name: impl Into<String>, query_family: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query_family: query_family.into(),
            source: None,
            cypher: None,
        }
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_cypher(mut self, cypher: impl Into<String>) -> Self {
        self.cypher = Some(cypher.into());
        self
    }
}

impl CompatibilityQueryCallSite {
    pub fn new(
        name: impl Into<String>,
        query_family: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            query_family: query_family.into(),
            source: source.into(),
            cypher: None,
        }
    }

    pub fn with_cypher(mut self, cypher: impl Into<String>) -> Self {
        self.cypher = Some(cypher.into());
        self
    }
}

pub fn build_compatibility_query_inventory(
    name: impl Into<String>,
    call_sites: impl IntoIterator<Item = CompatibilityQueryCallSite>,
) -> Result<CompatibilityQueryInventory> {
    let name = name.into();
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "compatibility query inventory name must not be empty".to_string(),
        ));
    }

    let mut seen = BTreeMap::new();
    let mut required_checks = Vec::new();
    for call_site in call_sites {
        let check_name = call_site.name.trim();
        if check_name.is_empty() {
            return Err(SkeinError::Semantic(
                "compatibility query call site name must not be empty".to_string(),
            ));
        }
        let query_family = call_site.query_family.trim();
        if query_family.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query call site '{check_name}' has no query family"
            )));
        }
        let source = call_site.source.trim();
        if source.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query call site '{check_name}' has no source"
            )));
        }
        if let Some(previous_source) = seen.insert(check_name.to_string(), source.to_string()) {
            return Err(SkeinError::Semantic(format!(
                "duplicate compatibility query call site '{check_name}' from '{previous_source}' and '{source}'"
            )));
        }

        let mut item =
            CompatibilityQueryInventoryItem::new(check_name.to_string(), query_family.to_string())
                .with_source(source.to_string());
        if let Some(cypher) = call_site.cypher {
            let cypher = cypher.trim();
            if !cypher.is_empty() {
                item = item.with_cypher(cypher.to_string());
            }
        }
        required_checks.push(item);
    }

    Ok(CompatibilityQueryInventory {
        name,
        required_checks,
    })
}

pub fn build_compatibility_query_inventory_from_json_str(
    artifact: &str,
) -> Result<CompatibilityQueryInventory> {
    let value = serde_json::from_str(artifact).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse compatibility query inventory artifact: {error}"
        ))
    })?;
    build_compatibility_query_inventory_from_json(&value)
}

pub fn build_compatibility_query_inventory_from_json(
    artifact: &serde_json::Value,
) -> Result<CompatibilityQueryInventory> {
    let object = artifact.as_object().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory artifact must be a JSON object".to_string(),
        )
    })?;
    let name = required_string_field(object, "name")?;
    if let Some(call_sites) = object.get("call_sites") {
        return build_compatibility_query_inventory(name, call_sites_from_json(call_sites)?);
    }
    if let Some(required_checks) = object.get("required_checks") {
        return build_compatibility_query_inventory_from_items(
            name.to_string(),
            inventory_items_from_json(required_checks)?,
        );
    }
    Err(SkeinError::Semantic(
        "compatibility query inventory artifact must contain 'call_sites' or 'required_checks'"
            .to_string(),
    ))
}

pub fn compatibility_query_inventory_to_json(
    inventory: &CompatibilityQueryInventory,
) -> serde_json::Value {
    serde_json::json!({
        "name": inventory.name,
        "required_checks": inventory
            .required_checks
            .iter()
            .map(inventory_item_to_json)
            .collect::<Vec<_>>(),
    })
}

fn build_compatibility_query_inventory_from_items(
    name: String,
    items: Vec<CompatibilityQueryInventoryItem>,
) -> Result<CompatibilityQueryInventory> {
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "compatibility query inventory name must not be empty".to_string(),
        ));
    }
    let mut seen = BTreeMap::new();
    let mut required_checks = Vec::new();
    for item in items {
        let check_name = item.name.trim();
        if check_name.is_empty() {
            return Err(SkeinError::Semantic(
                "compatibility query inventory item name must not be empty".to_string(),
            ));
        }
        let query_family = item.query_family.trim();
        if query_family.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query inventory item '{check_name}' has no query family"
            )));
        }
        let source = item
            .source
            .as_deref()
            .map(str::trim)
            .filter(|source| !source.is_empty())
            .map(str::to_string);
        let source_label = source.as_deref().unwrap_or("<unknown>");
        if let Some(previous_source) = seen.insert(check_name.to_string(), source_label.to_string())
        {
            return Err(SkeinError::Semantic(format!(
                "duplicate compatibility query inventory item '{check_name}' from '{previous_source}' and '{source_label}'"
            )));
        }
        let cypher = item
            .cypher
            .as_deref()
            .map(str::trim)
            .filter(|cypher| !cypher.is_empty())
            .map(str::to_string);
        required_checks.push(CompatibilityQueryInventoryItem {
            name: check_name.to_string(),
            query_family: query_family.to_string(),
            source,
            cypher,
        });
    }
    Ok(CompatibilityQueryInventory {
        name: name.trim().to_string(),
        required_checks,
    })
}

fn call_sites_from_json(value: &serde_json::Value) -> Result<Vec<CompatibilityQueryCallSite>> {
    let call_sites = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory 'call_sites' must be an array".to_string(),
        )
    })?;
    call_sites
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let object = value.as_object().ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query call site at index {index} must be a JSON object"
                ))
            })?;
            let mut call_site = CompatibilityQueryCallSite::new(
                required_string_field(object, "name")?,
                required_string_field(object, "query_family")?,
                required_string_field(object, "source")?,
            );
            if let Some(cypher) = optional_string_field(object, "cypher")? {
                call_site = call_site.with_cypher(cypher);
            }
            Ok(call_site)
        })
        .collect()
}

fn inventory_items_from_json(
    value: &serde_json::Value,
) -> Result<Vec<CompatibilityQueryInventoryItem>> {
    let items = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory 'required_checks' must be an array".to_string(),
        )
    })?;
    items
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let object = value.as_object().ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query inventory item at index {index} must be a JSON object"
                ))
            })?;
            let mut item = CompatibilityQueryInventoryItem::new(
                required_string_field(object, "name")?,
                required_string_field(object, "query_family")?,
            );
            if let Some(source) = optional_string_field(object, "source")? {
                item = item.with_source(source);
            }
            if let Some(cypher) = optional_string_field(object, "cypher")? {
                item = item.with_cypher(cypher);
            }
            Ok(item)
        })
        .collect()
}

fn inventory_item_to_json(item: &CompatibilityQueryInventoryItem) -> serde_json::Value {
    let mut object = serde_json::Map::from_iter([
        (
            "name".to_string(),
            serde_json::Value::String(item.name.clone()),
        ),
        (
            "query_family".to_string(),
            serde_json::Value::String(item.query_family.clone()),
        ),
    ]);
    if let Some(source) = &item.source {
        object.insert(
            "source".to_string(),
            serde_json::Value::String(source.clone()),
        );
    }
    if let Some(cypher) = &item.cypher {
        object.insert(
            "cypher".to_string(),
            serde_json::Value::String(cypher.clone()),
        );
    }
    serde_json::Value::Object(object)
}

fn required_string_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<&'a str> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "compatibility query inventory artifact field '{field}' must be a string"
            ))
        })
}

fn optional_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>> {
    match object.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query inventory artifact field '{field}' must be a string"
                ))
            }),
    }
}
