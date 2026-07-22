use super::SearchDocument;
use crate::error::{Result, SkeinError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const DEFAULT_FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const TURBOVEC_SEARCH_PROJECTION_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct TurbovecSearchHit {
    pub id: String,
    pub score: f64,
}

pub struct TurbovecSearchProjection {
    index: turbovec::IdMapIndex,
    numeric_to_document_id: BTreeMap<u64, String>,
    document_to_numeric_id: BTreeMap<String, u64>,
    dimension: usize,
    bit_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TurbovecSearchProjectionManifest {
    version: u32,
    dimension: usize,
    bit_width: usize,
    mappings: Vec<TurbovecSearchProjectionMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TurbovecSearchProjectionMapping {
    numeric_id: u64,
    document_id: String,
}

impl TurbovecSearchProjection {
    pub fn build_from_documents<'a>(
        documents: impl IntoIterator<Item = &'a SearchDocument>,
        bit_width: usize,
    ) -> Result<Option<Self>> {
        let mut dimension = None;
        let mut vectors = Vec::new();
        let mut numeric_to_document_id = BTreeMap::new();
        let mut document_to_numeric_id = BTreeMap::new();
        let mut numeric_ids = Vec::new();

        for document in documents {
            let Some(embedding) = document.embedding.as_ref() else {
                continue;
            };
            if !embedding.iter().all(|value| value.is_finite()) {
                return Err(SkeinError::Storage(format!(
                    "turbovec projection rejected non-finite embedding for {}",
                    document.id
                )));
            }
            match dimension {
                Some(existing) if existing != embedding.len() => {
                    return Err(SkeinError::Storage(format!(
                        "turbovec projection embedding dimension mismatch: expected {existing}, got {}",
                        embedding.len()
                    )));
                }
                Some(_) => {}
                None => dimension = Some(embedding.len()),
            }

            let numeric_id = stable_numeric_id(&document.id, &numeric_to_document_id);
            numeric_to_document_id.insert(numeric_id, document.id.clone());
            document_to_numeric_id.insert(document.id.clone(), numeric_id);
            numeric_ids.push(numeric_id);
            vectors.extend_from_slice(embedding);
        }

        let Some(dimension) = dimension else {
            return Ok(None);
        };
        let mut index = turbovec::IdMapIndex::new(dimension, bit_width).map_err(|error| {
            SkeinError::Storage(format!("turbovec projection construct failed: {error}"))
        })?;
        index
            .add_with_ids(&vectors, &numeric_ids)
            .map_err(|error| {
                SkeinError::Storage(format!("turbovec projection add failed: {error}"))
            })?;
        Ok(Some(Self {
            index,
            numeric_to_document_id,
            document_to_numeric_id,
            dimension,
            bit_width,
        }))
    }

    pub fn write_to_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        self.index.write(path).map_err(|error| {
            SkeinError::Storage(format!("turbovec projection write failed: {error}"))
        })?;
        let manifest = TurbovecSearchProjectionManifest {
            version: TURBOVEC_SEARCH_PROJECTION_MANIFEST_VERSION,
            dimension: self.dimension,
            bit_width: self.bit_width,
            mappings: self
                .numeric_to_document_id
                .iter()
                .map(
                    |(numeric_id, document_id)| TurbovecSearchProjectionMapping {
                        numeric_id: *numeric_id,
                        document_id: document_id.clone(),
                    },
                )
                .collect(),
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            SkeinError::Storage(format!(
                "turbovec projection manifest encode failed: {error}"
            ))
        })?;
        std::fs::write(manifest_path_for(path), manifest_bytes).map_err(|error| {
            SkeinError::Storage(format!(
                "turbovec projection manifest write failed: {error}"
            ))
        })?;
        Ok(())
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let index = turbovec::IdMapIndex::load(path).map_err(|error| {
            SkeinError::Storage(format!("turbovec projection load failed: {error}"))
        })?;
        let manifest_bytes = std::fs::read(manifest_path_for(path)).map_err(|error| {
            SkeinError::Storage(format!("turbovec projection manifest read failed: {error}"))
        })?;
        let manifest: TurbovecSearchProjectionManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| {
                SkeinError::Storage(format!(
                    "turbovec projection manifest decode failed: {error}"
                ))
            })?;
        if manifest.version != TURBOVEC_SEARCH_PROJECTION_MANIFEST_VERSION {
            return Err(SkeinError::Storage(format!(
                "unsupported turbovec projection manifest version {}",
                manifest.version
            )));
        }
        if index.dim_opt() != Some(manifest.dimension) {
            return Err(SkeinError::Storage(format!(
                "turbovec projection manifest dimension {} does not match index dimension {:?}",
                manifest.dimension,
                index.dim_opt()
            )));
        }
        if index.bit_width() != manifest.bit_width {
            return Err(SkeinError::Storage(format!(
                "turbovec projection manifest bit width {} does not match index bit width {}",
                manifest.bit_width,
                index.bit_width()
            )));
        }
        if index.len() != manifest.mappings.len() {
            return Err(SkeinError::Storage(format!(
                "turbovec projection manifest maps {} documents but index has {} vectors",
                manifest.mappings.len(),
                index.len()
            )));
        }

        let mut numeric_to_document_id = BTreeMap::new();
        let mut document_to_numeric_id = BTreeMap::new();
        for mapping in manifest.mappings {
            if numeric_to_document_id
                .insert(mapping.numeric_id, mapping.document_id.clone())
                .is_some()
            {
                return Err(SkeinError::Storage(format!(
                    "turbovec projection manifest contains duplicate numeric id {}",
                    mapping.numeric_id
                )));
            }
            if document_to_numeric_id
                .insert(mapping.document_id.clone(), mapping.numeric_id)
                .is_some()
            {
                return Err(SkeinError::Storage(format!(
                    "turbovec projection manifest contains duplicate document id {}",
                    mapping.document_id
                )));
            }
        }

        Ok(Self {
            index,
            numeric_to_document_id,
            document_to_numeric_id,
            dimension: manifest.dimension,
            bit_width: manifest.bit_width,
        })
    }

    pub fn search(
        &self,
        query_embedding: &[f32],
        limit: usize,
        allowlist: Option<&BTreeSet<String>>,
    ) -> Result<Vec<TurbovecSearchHit>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if query_embedding.len() != self.dimension {
            return Err(SkeinError::Storage(format!(
                "turbovec query dimension mismatch: expected {}, got {}",
                self.dimension,
                query_embedding.len()
            )));
        }
        if !query_embedding.iter().all(|value| value.is_finite()) {
            return Err(SkeinError::Storage(
                "turbovec query embedding contains a non-finite coordinate".to_string(),
            ));
        }

        let allowlist_ids = allowlist.map(|ids| {
            ids.iter()
                .filter_map(|id| self.document_to_numeric_id.get(id).copied())
                .collect::<Vec<_>>()
        });
        if allowlist_ids.as_ref().is_some_and(Vec::is_empty) {
            return Ok(Vec::new());
        }

        let search_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.index
                .search_with_allowlist(query_embedding, limit, allowlist_ids.as_deref())
        }));
        let (scores, ids) = search_result.map_err(|_| {
            SkeinError::Storage(
                "turbovec projection search failed because the id map is inconsistent".to_string(),
            )
        })?;
        Ok(ids
            .into_iter()
            .zip(scores)
            .filter_map(|(numeric_id, score)| {
                self.numeric_to_document_id
                    .get(&numeric_id)
                    .map(|id| TurbovecSearchHit {
                        id: id.clone(),
                        score: f64::from(score).max(0.0),
                    })
            })
            .collect())
    }

    pub fn document_count(&self) -> usize {
        self.index.len()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn bit_width(&self) -> usize {
        self.bit_width
    }
}

fn manifest_path_for(index_path: &Path) -> PathBuf {
    let file_name = index_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "projection.tvim".into());
    index_path.with_file_name(format!("{file_name}.ids.json"))
}

fn stable_numeric_id(document_id: &str, existing: &BTreeMap<u64, String>) -> u64 {
    let mut candidate = fnv1a_u64(document_id.as_bytes());
    while existing.contains_key(&candidate) {
        candidate = candidate.wrapping_add(1);
    }
    candidate
}

fn fnv1a_u64(bytes: &[u8]) -> u64 {
    let mut hash = DEFAULT_FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: &str, embedding: &[f32]) -> SearchDocument {
        SearchDocument {
            id: id.to_string(),
            title: id.to_string(),
            content: String::new(),
            embedding: Some(embedding.to_vec()),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn projection_search_returns_stable_document_ids() {
        let documents = vec![
            doc("memory:a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            doc("memory:b", &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ];
        let projection = TurbovecSearchProjection::build_from_documents(&documents, 4)
            .unwrap()
            .unwrap();

        let hits = projection
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, None)
            .unwrap();

        assert_eq!(projection.document_count(), 2);
        assert_eq!(projection.dimension(), 8);
        assert_eq!(projection.bit_width(), 4);
        assert_eq!(hits[0].id, "memory:a");
    }

    #[test]
    fn projection_search_honors_string_allowlist_without_panic() {
        let documents = vec![
            doc("memory:a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            doc("memory:b", &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ];
        let projection = TurbovecSearchProjection::build_from_documents(&documents, 4)
            .unwrap()
            .unwrap();
        let allowlist = BTreeSet::from(["memory:b".to_string()]);

        let hits = projection
            .search(
                &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                10,
                Some(&allowlist),
            )
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:b");
    }

    #[test]
    fn projection_search_empty_allowlist_returns_empty() {
        let documents = vec![doc("memory:a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])];
        let projection = TurbovecSearchProjection::build_from_documents(&documents, 4)
            .unwrap()
            .unwrap();
        let allowlist = BTreeSet::from(["missing".to_string()]);

        let hits = projection
            .search(
                &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                10,
                Some(&allowlist),
            )
            .unwrap();

        assert!(hits.is_empty());
    }

    #[test]
    fn projection_round_trips_index_and_document_id_manifest() {
        let path = unique_test_dir("turbovec_projection_roundtrip");
        std::fs::create_dir_all(&path).unwrap();
        let index_path = path.join("search_projection.tvim");
        let documents = vec![
            doc("memory:a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            doc("memory:b", &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ];
        let projection = TurbovecSearchProjection::build_from_documents(&documents, 4)
            .unwrap()
            .unwrap();

        projection.write_to_path(&index_path).unwrap();
        let loaded = TurbovecSearchProjection::load_from_path(&index_path).unwrap();
        let allowlist = BTreeSet::from(["memory:b".to_string()]);
        let hits = loaded
            .search(
                &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                10,
                Some(&allowlist),
            )
            .unwrap();

        assert_eq!(loaded.document_count(), 2);
        assert_eq!(loaded.dimension(), 8);
        assert_eq!(loaded.bit_width(), 4);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:b");

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_load_rejects_manifest_index_count_mismatch() {
        let path = unique_test_dir("turbovec_projection_corrupt_manifest");
        std::fs::create_dir_all(&path).unwrap();
        let index_path = path.join("search_projection.tvim");
        let documents = vec![doc("memory:a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])];
        let projection = TurbovecSearchProjection::build_from_documents(&documents, 4)
            .unwrap()
            .unwrap();
        projection.write_to_path(&index_path).unwrap();

        let manifest = TurbovecSearchProjectionManifest {
            version: TURBOVEC_SEARCH_PROJECTION_MANIFEST_VERSION,
            dimension: 8,
            bit_width: 4,
            mappings: Vec::new(),
        };
        std::fs::write(
            manifest_path_for(&index_path),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let error = match TurbovecSearchProjection::load_from_path(&index_path) {
            Ok(_) => panic!("corrupt turbovec projection manifest should be rejected"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("manifest maps 0 documents but index has 1 vectors"));

        std::fs::remove_dir_all(path).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
