//! Root-facade adapter for the search kernel.

pub use skein_search::*;

use crate::error::Result;
use crate::schema::Catalog;
use crate::store::{GraphScanControl, GraphStore, NodeRecord};
use std::collections::BTreeSet;

impl SearchProjectionSource for GraphStore {
    fn source_graph_commit_epoch(&self) -> u64 {
        self.commit_epoch()
    }

    fn estimated_projection_node_count(&self) -> usize {
        self.basic_statistics().node_count as usize
    }

    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> Result<()>,
    ) -> Result<()> {
        for node in self.node_records_owned() {
            visitor(node?)?;
        }
        Ok(())
    }

    fn projection_business_labels(
        &self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) -> Result<Vec<String>> {
        let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
            return Ok(Vec::new());
        };
        let Some(label_label_id) = catalog.label_id("Label") else {
            return Ok(Vec::new());
        };
        let mut labels = BTreeSet::new();
        let mut callback_error = None;
        self.visit_relationships_owned(Some(has_label_type_id), |relationship| {
            let label_node_id = if relationship.source == node.id {
                relationship.target
            } else if relationship.target == node.id {
                relationship.source
            } else {
                return GraphScanControl::Continue;
            };
            match self.node_owned(label_node_id) {
                Ok(Some(label)) if label.labels.contains(&label_label_id) => {
                    if let Some(value) = projection_business_label_value(&label) {
                        labels.insert(value);
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
            GraphScanControl::Continue
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        Ok(labels.into_iter().collect())
    }
}
