use super::*;
use skein_storage::RelationalOverflowRef;

pub(super) const PROJECTION: &str = "COUNT(*) AS rows, COUNT(r.n) AS present, SUM(n) AS total, COALESCE(SUM(n), NULL, 0, 9) AS fallback, SUM(OCTET_LENGTH(body)) AS body_bytes, SUM(OCTET_LENGTH(payload)) AS payload_bytes, COUNT(body) AS body_present";

pub(super) fn select(projection: &str) -> SelectStatement {
    parse(&format!("SELECT {projection} FROM records AS r"))
}

pub(super) fn parse(sql: &str) -> SelectStatement {
    let skein_sql::SqlStatement::Select(select) =
        skein_sql::prepare_postgres_sql(sql).unwrap().statement
    else {
        panic!("expected SELECT: {sql}");
    };
    select
}

pub(super) fn schema() -> RelationalTableSchema {
    RelationalTableSchema {
        name: "records".into(),
        columns: [
            ("n", RelationalScalarType::BigInt),
            ("body", RelationalScalarType::Text),
            ("payload", RelationalScalarType::Bytea),
            ("ratio", RelationalScalarType::DoublePrecision),
            ("flag", RelationalScalarType::Boolean),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: true,
            default: None,
        })
        .collect(),
        primary_key: vec!["n".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

pub(super) fn nz(bytes: usize) -> NonZeroUsize {
    NonZeroUsize::new(bytes).unwrap()
}

pub(super) fn ledger() -> QueryMemoryLedger {
    QueryMemoryLedger::new(nz(1024 * 1024))
}

pub(super) fn executor(
    projection: &str,
    rows: usize,
    ledger: &QueryMemoryLedger,
) -> ColumnarAggregateExecutor {
    ColumnarAggregateExecutor::try_new(
        &select(projection),
        &schema(),
        "records",
        "r",
        true,
        rows,
        nz(256 * 1024),
        ledger,
    )
    .unwrap()
    .expect("supported aggregate")
}

pub(super) fn overflow(scalar_type: RelationalScalarType, bytes: u64) -> RelationalValue {
    RelationalValue::Overflow(RelationalOverflowRef {
        digest: "00".repeat(32).parse().unwrap(),
        scalar_type,
        compressed_bytes: 1,
        uncompressed_bytes: bytes,
    })
}

pub(super) fn push_row(
    executor: &mut ColumnarAggregateExecutor,
    row: &[RelationalValue; 3],
) -> Result<()> {
    executor.push(|column| {
        let index = match column.name.as_str() {
            "n" => 0,
            "body" => 1,
            "payload" => 2,
            _ => panic!("unexpected column: {column:?}"),
        };
        Ok(&row[index])
    })
}

// Independent layout calculation for the seven projections in PROJECTION.
pub(super) fn reference_batch_bytes(rows: usize) -> usize {
    use std::mem::size_of;
    let slots = 7;
    let metadata = size_of::<BindingSchema>()
        + slots * size_of::<SlotDescriptor>()
        + (0..slots)
            .map(|index| format!("aggregate_{index}").len())
            .sum::<usize>()
        + size_of::<ColumnarBatch>()
        + slots * (size_of::<Arc<ColumnVector>>() + size_of::<ColumnVector>());
    metadata + 3 * rows + 4 * rows * size_of::<i64>() + 6 * rows.div_ceil(64) * size_of::<u64>()
}

pub(super) fn reference(rows: &[[RelationalValue; 3]]) -> Vec<(String, Value)> {
    let present = rows
        .iter()
        .filter(|row| row[0] != RelationalValue::Null)
        .count();
    let total = rows
        .iter()
        .filter_map(|row| match row[0] {
            RelationalValue::BigInt(value) => Some(i128::from(value)),
            RelationalValue::Null => None,
            _ => panic!("invalid numeric fixture"),
        })
        .sum::<i128>();
    let sum = if present == 0 {
        Value::Null
    } else {
        Value::Int(i64::try_from(total).unwrap())
    };
    let length_sum = |index: usize| {
        let lengths = rows
            .iter()
            .filter_map(|row| match &row[index] {
                RelationalValue::Null => None,
                RelationalValue::Text(value) => Some(value.len() as u64),
                RelationalValue::Bytea(value) => Some(value.len() as u64),
                RelationalValue::Overflow(reference) => Some(reference.uncompressed_bytes),
                _ => panic!("invalid length fixture"),
            })
            .collect::<Vec<_>>();
        if lengths.is_empty() {
            Value::Null
        } else {
            Value::Int(
                i64::try_from(lengths.iter().map(|value| u128::from(*value)).sum::<u128>())
                    .unwrap(),
            )
        }
    };
    [
        ("rows", Value::Int(rows.len() as i64)),
        ("present", Value::Int(present as i64)),
        ("total", sum.clone()),
        (
            "fallback",
            if sum == Value::Null {
                Value::Int(0)
            } else {
                sum
            },
        ),
        ("body_bytes", length_sum(1)),
        ("payload_bytes", length_sum(2)),
        (
            "body_present",
            Value::Int(
                rows.iter()
                    .filter(|row| row[1] != RelationalValue::Null)
                    .count() as i64,
            ),
        ),
    ]
    .into_iter()
    .map(|(name, value)| (name.into(), value))
    .collect()
}

pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub(super) fn rows(&mut self, count: usize) -> Vec<[RelationalValue; 3]> {
        (0..count)
            .map(|_| {
                let n = self.next();
                let body = self.next();
                let payload = self.next();
                [
                    if n.is_multiple_of(4) {
                        RelationalValue::Null
                    } else {
                        RelationalValue::BigInt((n % 2001) as i64 - 1000)
                    },
                    match body % 5 {
                        0 => RelationalValue::Null,
                        1 => overflow(RelationalScalarType::Text, body % 1024 + 1),
                        _ => RelationalValue::Text("\u{e9}\u{1f980}".repeat((body % 7) as usize)),
                    },
                    match payload % 5 {
                        0 => RelationalValue::Null,
                        1 => overflow(RelationalScalarType::Bytea, payload % 1024 + 1),
                        _ => RelationalValue::Bytea(vec![
                            (payload % 256) as u8;
                            (payload % 19) as usize
                        ]),
                    },
                ]
            })
            .collect()
    }
}
