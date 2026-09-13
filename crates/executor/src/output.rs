//! Query result envelope shared by execution and embedded adapters.

use crate::{QueryRows, QuerySchema, QueryValueRows, Row};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput {
    pub rows: QueryRows,
}

impl QueryOutput {
    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self { rows: rows.into() }
    }

    pub fn schema(&self) -> &QuerySchema {
        self.rows.schema()
    }

    pub fn value_rows(&self) -> QueryValueRows<'_> {
        self.rows.value_rows()
    }

    /// Returns the deterministic payload accounting used by query result
    /// admission. Container allocation overhead is intentionally excluded.
    pub fn payload_bytes(&self) -> usize {
        self.rows.payload_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryRowsBuilder;
    use skein_core::{Value, ValueRef};

    #[test]
    fn query_output_preserves_schema_values_and_payload_without_map_materialization() {
        let schema = QuerySchema::try_new(["payload".to_string(), "id".to_string()]).unwrap();
        let mut rows = QueryRowsBuilder::with_schema(schema.clone(), 1);
        rows.push_values([Value::String("bytes".to_string()), Value::Int(7)])
            .unwrap();
        let output = QueryOutput {
            rows: rows.finish(),
        };
        assert_eq!(output.schema(), &schema);
        assert_eq!(output.rows.value(0, 0), Some(ValueRef::String("bytes")));
        assert_eq!(output.value_rows().count(), 1);
        assert_eq!(output.payload_bytes(), "payload".len() + "id".len() + 5 + 8);
        assert_eq!(output.clone(), output);
        let empty = QueryOutput::from_rows(Vec::new());
        assert!(empty.schema().is_empty());
        assert_eq!(empty.payload_bytes(), 0);
    }
}
