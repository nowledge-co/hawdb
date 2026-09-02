use crate::binding::Binding;
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::QueryMemoryLease;
use pool::{process_marker, RunLease, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX};
use skein_core::{LabelId, RelTypeId, Result, SkeinError, Value};
use skein_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, ErrorKind, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod pool;

pub use pool::SpillPoolSnapshot;
pub(crate) use pool::{spill_pool_snapshot, SpillPool, SpillWriteReservation};

const MAX_SPILL_RECORD_BYTES: usize = 1024 * 1024 * 1024;
const MAX_VALUE_DEPTH: usize = 64;
static NEXT_SPILL_ID: AtomicU64 = AtomicU64::new(0);

pub struct SpillRun {
    lease: Arc<RunLease>,
}

impl SpillRun {
    pub(crate) fn create(pool: SpillPool, operator: &str) -> Result<(Self, SpillWriter)> {
        pool.begin_run(operator)?;
        let safe_operator: String = operator
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '-' {
                    character
                } else {
                    '-'
                }
            })
            .collect();
        for _ in 0..32 {
            let id = NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed);
            let path = pool.directory().join(format!(
                "{SPILL_FILE_PREFIX}{}-{safe_operator}-{id}{SPILL_FILE_SUFFIX}",
                process_marker()
            ));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => {
                    let lease = Arc::new(RunLease::new(path, pool));
                    return Ok((
                        Self {
                            lease: Arc::clone(&lease),
                        },
                        SpillWriter {
                            writer: BufWriter::new(file),
                            lease,
                        },
                    ));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    pool.cancel_run();
                    return Err(SkeinError::Execution(format!(
                        "failed to create spill run '{}': {error}",
                        path.display()
                    )));
                }
            }
        }
        pool.cancel_run();
        Err(SkeinError::Execution(
            "failed to allocate a unique spill run path".to_string(),
        ))
    }

    pub fn reader(&self) -> Result<SpillReader> {
        let file = File::open(self.lease.path()).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to open spill run '{}': {error}",
                self.lease.path().display()
            ))
        })?;
        Ok(SpillReader {
            reader: BufReader::new(file),
        })
    }
}

pub struct SpillWriter {
    writer: BufWriter<File>,
    lease: Arc<RunLease>,
}

impl SpillWriter {
    pub fn write(
        &mut self,
        ordinal: u64,
        binding: &Binding,
        spill_budget: &mut SpillBudgetTracker,
    ) -> Result<u64> {
        let encoded_len = binding_record_encoded_len(binding)?;
        let _staging_lease = spill_budget.reserve_staging(encoded_len)?;
        let mut payload = Vec::with_capacity(encoded_len);
        write_u64(&mut payload, ordinal)?;
        write_binding(&mut payload, binding)?;
        if payload.len() != encoded_len {
            return Err(SkeinError::Execution(format!(
                "spill binding codec declared {encoded_len} bytes but encoded {} bytes",
                payload.len()
            )));
        }
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            SkeinError::Execution("spill record exceeds the supported size".to_string())
        })?;
        let record_bytes = payload_len.saturating_add(8);
        let reservation = spill_budget.reserve_write(record_bytes)?;
        self.writer
            .write_all(&payload_len.to_le_bytes())
            .and_then(|_| self.writer.write_all(&payload))
            .map_err(|error| {
                SkeinError::Execution(format!("failed to write spill run: {error}"))
            })?;
        reservation.commit(&self.lease);
        spill_budget.commit_write(record_bytes);
        Ok(record_bytes)
    }

    pub(crate) fn write_record_payload(
        &mut self,
        payload: &[u8],
        spill_budget: &mut SpillBudgetTracker,
    ) -> Result<u64> {
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            SkeinError::Execution("spill record exceeds the supported size".to_string())
        })?;
        let record_bytes = payload_len.saturating_add(8);
        let reservation = spill_budget.reserve_write(record_bytes)?;
        self.writer
            .write_all(&payload_len.to_le_bytes())
            .and_then(|_| self.writer.write_all(payload))
            .map_err(|error| {
                SkeinError::Execution(format!("failed to write spill run: {error}"))
            })?;
        reservation.commit(&self.lease);
        spill_budget.commit_write(record_bytes);
        Ok(record_bytes)
    }

    pub fn finish(mut self) -> Result<()> {
        self.writer.flush().map_err(|error| {
            SkeinError::Execution(format!("failed to flush spill run: {error}"))
        })?;
        self.lease.mark_flushed();
        Ok(())
    }
}

pub struct SpillReader {
    reader: BufReader<File>,
}

pub(crate) struct SpillRecordPayload {
    bytes: Vec<u8>,
    _lease: Option<QueryMemoryLease>,
}

pub struct SpillBindingRecord {
    payload: SpillRecordPayload,
    decoded_binding_bytes: usize,
}

impl SpillBindingRecord {
    pub(crate) fn decoded_binding_bytes(&self) -> usize {
        self.decoded_binding_bytes
    }

    pub fn try_map<T>(
        self,
        operator: &str,
        max_item_bytes: usize,
        tracker: &mut OperatorMemoryTracker,
        map: impl FnOnce(u64, Binding) -> Result<T>,
        memory_bytes: impl FnOnce(&T) -> usize,
    ) -> Result<T> {
        let Self {
            payload,
            decoded_binding_bytes,
        } = self;
        tracker.try_charge(decoded_binding_bytes)?;
        let item = match decode_binding_record(payload.as_slice())
            .and_then(|(ordinal, binding)| map(ordinal, binding))
        {
            Ok(item) => item,
            Err(error) => {
                tracker.release(decoded_binding_bytes);
                return Err(error);
            }
        };
        let bytes = memory_bytes(&item);
        if bytes > max_item_bytes {
            tracker.release(decoded_binding_bytes);
            return Err(SkeinError::Execution(format!(
                "{operator} spill merge item uses {bytes} bytes, exceeding its {max_item_bytes}-byte allowance"
            )));
        }
        if let Err(error) = ensure_operator_item_fits(operator, bytes, tracker) {
            tracker.release(decoded_binding_bytes);
            return Err(error);
        }
        if bytes > decoded_binding_bytes {
            let additional_bytes = bytes - decoded_binding_bytes;
            if tracker.would_exceed(additional_bytes) {
                tracker.release(decoded_binding_bytes);
                return Err(SkeinError::Execution(format!(
                    "{operator} spill merge fan-in uses more than blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            if let Err(error) = tracker.try_charge(additional_bytes) {
                tracker.release(decoded_binding_bytes);
                return Err(error);
            }
        } else {
            tracker.release(decoded_binding_bytes - bytes);
        }
        Ok(item)
    }
}

impl SpillRecordPayload {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl SpillReader {
    pub fn read(&mut self, max_record_bytes: usize) -> Result<Option<(u64, Binding)>> {
        let mut encoded_len = [0u8; 8];
        let bytes_read = self.reader.read(&mut encoded_len).map_err(|error| {
            SkeinError::Execution(format!("failed to read spill record length: {error}"))
        })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut encoded_len[bytes_read..])
            .map_err(|error| {
                SkeinError::Execution(format!("truncated spill record length: {error}"))
            })?;
        let payload_len = usize::try_from(u64::from_le_bytes(encoded_len)).map_err(|_| {
            SkeinError::Execution("spill record length does not fit in memory".to_string())
        })?;
        let safety_limit = MAX_SPILL_RECORD_BYTES.min(max_record_bytes);
        if payload_len > safety_limit {
            return Err(SkeinError::Execution(format!(
                "spill record length {payload_len} exceeds the admitted limit {safety_limit}"
            )));
        }
        let mut payload = vec![0; payload_len];
        self.reader.read_exact(&mut payload).map_err(|error| {
            SkeinError::Execution(format!("truncated spill record payload: {error}"))
        })?;
        let mut cursor = Cursor::new(payload.as_slice());
        let ordinal = read_u64(&mut cursor)?;
        let binding = read_binding(&mut cursor)?;
        if cursor.position() != payload.len() as u64 {
            return Err(SkeinError::Execution(
                "spill record contains trailing bytes".to_string(),
            ));
        }
        Ok(Some((ordinal, binding)))
    }

    pub(crate) fn read_record_payload(
        &mut self,
        max_record_bytes: usize,
        spill_budget: &SpillBudgetTracker,
    ) -> Result<Option<SpillRecordPayload>> {
        let mut encoded_len = [0u8; 8];
        let bytes_read = self.reader.read(&mut encoded_len).map_err(|error| {
            SkeinError::Execution(format!("failed to read spill record length: {error}"))
        })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut encoded_len[bytes_read..])
            .map_err(|error| {
                SkeinError::Execution(format!("truncated spill record length: {error}"))
            })?;
        let payload_len = usize::try_from(u64::from_le_bytes(encoded_len)).map_err(|_| {
            SkeinError::Execution("spill record length does not fit in memory".to_string())
        })?;
        let safety_limit = MAX_SPILL_RECORD_BYTES.min(max_record_bytes);
        if payload_len > safety_limit {
            return Err(SkeinError::Execution(format!(
                "spill record length {payload_len} exceeds the admitted limit {safety_limit}"
            )));
        }
        let lease = spill_budget.reserve_staging(payload_len)?;
        let mut bytes = vec![0; payload_len];
        self.reader.read_exact(&mut bytes).map_err(|error| {
            SkeinError::Execution(format!("truncated spill record payload: {error}"))
        })?;
        Ok(Some(SpillRecordPayload {
            bytes,
            _lease: lease,
        }))
    }

    pub fn read_binding_record(
        &mut self,
        max_record_bytes: usize,
        spill_budget: &SpillBudgetTracker,
    ) -> Result<Option<SpillBindingRecord>> {
        let Some(payload) = self.read_record_payload(max_record_bytes, spill_budget)? else {
            return Ok(None);
        };
        let mut cursor = Cursor::new(payload.as_slice());
        read_u64(&mut cursor)?;
        let decoded_binding_bytes = estimate_binding_memory_bytes(&mut cursor)?;
        if cursor.position() != payload.as_slice().len() as u64 {
            return Err(SkeinError::Execution(
                "spill record contains trailing bytes".to_string(),
            ));
        }
        Ok(Some(SpillBindingRecord {
            payload,
            decoded_binding_bytes,
        }))
    }
}

fn decode_binding_record(payload: &[u8]) -> Result<(u64, Binding)> {
    let mut cursor = Cursor::new(payload);
    let ordinal = read_u64(&mut cursor)?;
    let binding = read_binding(&mut cursor)?;
    if cursor.position() != payload.len() as u64 {
        return Err(SkeinError::Execution(
            "spill record contains trailing bytes".to_string(),
        ));
    }
    Ok((ordinal, binding))
}

fn estimate_binding_memory_bytes(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let (mut payload_bytes, values) = estimate_value_map_payload(input, 0)?;
    let nodes = read_len(input)?;
    for _ in 0..nodes {
        payload_bytes = encoded_len_add(payload_bytes, skip_string(input)?)?;
        read_u64(input)?;
        payload_bytes = encoded_len_add(payload_bytes, std::mem::size_of::<u64>())?;
        let labels = read_len(input)?;
        for _ in 0..labels {
            read_u32(input)?;
        }
        payload_bytes = encoded_len_add(
            payload_bytes,
            encoded_len_mul(labels, std::mem::size_of::<LabelId>())?,
        )?;
        let (properties, _) = estimate_value_map_payload(input, 0)?;
        payload_bytes = encoded_len_add(payload_bytes, properties)?;
    }
    let relationships = read_len(input)?;
    for _ in 0..relationships {
        payload_bytes = encoded_len_add(payload_bytes, skip_string(input)?)?;
        read_u64(input)?;
        read_u64(input)?;
        read_u64(input)?;
        read_u32(input)?;
        payload_bytes = encoded_len_add(
            payload_bytes,
            std::mem::size_of::<u64>() * 3 + std::mem::size_of::<RelTypeId>(),
        )?;
        let (properties, _) = estimate_value_map_payload(input, 0)?;
        payload_bytes = encoded_len_add(payload_bytes, properties)?;
    }
    let entry_count = values
        .checked_add(nodes)
        .and_then(|count| count.checked_add(relationships))
        .ok_or_else(|| SkeinError::Execution("spill binding entry count overflow".to_string()))?;
    encoded_len_add(
        std::mem::size_of::<Binding>(),
        encoded_len_add(
            payload_bytes,
            encoded_len_mul(entry_count, std::mem::size_of::<usize>() * 6)?,
        )?,
    )
}

fn estimate_value_map_payload(input: &mut Cursor<&[u8]>, depth: usize) -> Result<(usize, usize)> {
    check_depth(depth)?;
    let count = read_len(input)?;
    let mut payload_bytes = 0usize;
    for _ in 0..count {
        payload_bytes = encoded_len_add(payload_bytes, skip_string(input)?)?;
        payload_bytes = encoded_len_add(payload_bytes, estimate_value_payload(input, depth + 1)?)?;
    }
    Ok((payload_bytes, count))
}

fn estimate_value_payload(input: &mut Cursor<&[u8]>, depth: usize) -> Result<usize> {
    check_depth(depth)?;
    match read_u8(input)? {
        0 => Ok(0),
        1 => match read_u8(input)? {
            0 | 1 => Ok(std::mem::size_of::<bool>()),
            value => Err(SkeinError::Execution(format!(
                "invalid boolean tag in spill record: {value}"
            ))),
        },
        2 => {
            read_i64(input)?;
            Ok(std::mem::size_of::<i64>())
        }
        3 => {
            read_u64(input)?;
            Ok(std::mem::size_of::<f64>())
        }
        4 => skip_string(input),
        5 => {
            let count = read_len(input)?;
            let mut payload_bytes = 0usize;
            for _ in 0..count {
                payload_bytes =
                    encoded_len_add(payload_bytes, estimate_value_payload(input, depth + 1)?)?;
            }
            Ok(payload_bytes)
        }
        6 => estimate_value_map_payload(input, depth + 1).map(|(bytes, _)| bytes),
        7 => skip_binary(input),
        tag => Err(SkeinError::Execution(format!(
            "invalid value tag in spill record: {tag}"
        ))),
    }
}

fn skip_string(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let len = read_len(input)?;
    let start = usize::try_from(input.position()).map_err(|_| {
        SkeinError::Execution("spill cursor position does not fit in memory".to_string())
    })?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SkeinError::Execution("spill string position overflow".to_string()))?;
    let bytes = input
        .get_ref()
        .get(start..end)
        .ok_or_else(|| SkeinError::Execution("truncated string in spill record".to_string()))?;
    std::str::from_utf8(bytes)
        .map_err(|error| SkeinError::Execution(format!("invalid spill string: {error}")))?;
    input.set_position(end as u64);
    Ok(len)
}

fn skip_binary(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let len = read_len(input)?;
    let start = usize::try_from(input.position()).map_err(|_| {
        SkeinError::Execution("spill cursor position does not fit in memory".to_string())
    })?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SkeinError::Execution("spill binary position overflow".to_string()))?;
    input
        .get_ref()
        .get(start..end)
        .ok_or_else(|| SkeinError::Execution("truncated binary in spill record".to_string()))?;
    input.set_position(end as u64);
    Ok(len)
}

fn binding_record_encoded_len(binding: &Binding) -> Result<usize> {
    encoded_len_add(8, binding_encoded_len(binding)?)
}

fn binding_encoded_len(binding: &Binding) -> Result<usize> {
    let mut bytes = value_map_encoded_len(&binding.values, 0)?;
    bytes = encoded_len_add(bytes, 8)?;
    for (name, node) in &binding.nodes {
        bytes = encoded_len_add(bytes, string_encoded_len(name)?)?;
        bytes = encoded_len_add(bytes, 8)?;
        bytes = encoded_len_add(bytes, 8)?;
        bytes = encoded_len_add(bytes, encoded_len_mul(node.labels.len(), 8)?)?;
        bytes = encoded_len_add(bytes, value_map_encoded_len(&node.properties, 0)?)?;
    }
    bytes = encoded_len_add(bytes, 8)?;
    for (name, relationship) in &binding.relationships {
        bytes = encoded_len_add(bytes, string_encoded_len(name)?)?;
        bytes = encoded_len_add(bytes, 8 * 4)?;
        bytes = encoded_len_add(bytes, value_map_encoded_len(&relationship.properties, 0)?)?;
    }
    Ok(bytes)
}

fn value_map_encoded_len(values: &BTreeMap<String, Value>, depth: usize) -> Result<usize> {
    check_depth(depth)?;
    values.iter().try_fold(8usize, |bytes, (name, value)| {
        encoded_len_add(
            encoded_len_add(bytes, string_encoded_len(name)?)?,
            value_encoded_len(value, depth + 1)?,
        )
    })
}

fn value_encoded_len(value: &Value, depth: usize) -> Result<usize> {
    check_depth(depth)?;
    match value {
        Value::Null => Ok(1),
        Value::Bool(_) => Ok(2),
        Value::Int(_) | Value::Float(_) => Ok(9),
        Value::String(value) => encoded_len_add(1, string_encoded_len(value)?),
        Value::Binary(value) => encoded_len_add(9, value.len()),
        Value::Uuid(_) => Ok(17),
        Value::List(values) => values.iter().try_fold(9usize, |bytes, value| {
            encoded_len_add(bytes, value_encoded_len(value, depth + 1)?)
        }),
        Value::Map(values) => encoded_len_add(1, value_map_encoded_len(values, depth + 1)?),
    }
}

fn string_encoded_len(value: &str) -> Result<usize> {
    encoded_len_add(8, value.len())
}

fn encoded_len_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| SkeinError::Execution("spill record encoded length overflow".to_string()))
}

fn encoded_len_mul(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| SkeinError::Execution("spill record encoded length overflow".to_string()))
}

fn write_binding(output: &mut Vec<u8>, binding: &Binding) -> Result<()> {
    write_value_map(output, &binding.values, 0)?;
    write_len(output, binding.nodes.len())?;
    for (name, node) in &binding.nodes {
        write_string(output, name)?;
        write_u64(output, node.id.0)?;
        write_len(output, node.labels.len())?;
        for label in &node.labels {
            write_u64(output, label.0 as u64)?;
        }
        write_value_map(output, &node.properties, 0)?;
    }
    write_len(output, binding.relationships.len())?;
    for (name, relationship) in &binding.relationships {
        write_string(output, name)?;
        write_u64(output, relationship.id.0)?;
        write_u64(output, relationship.source.0)?;
        write_u64(output, relationship.target.0)?;
        write_u64(output, relationship.rel_type.0 as u64)?;
        write_value_map(output, &relationship.properties, 0)?;
    }
    Ok(())
}

fn read_binding(input: &mut Cursor<&[u8]>) -> Result<Binding> {
    let values = read_value_map(input, 0)?;
    let mut nodes = BTreeMap::new();
    for _ in 0..read_len(input)? {
        let name = read_string(input)?;
        let id = NodeId(read_u64(input)?);
        let mut labels = BTreeSet::new();
        for _ in 0..read_len(input)? {
            labels.insert(LabelId(read_u32(input)?));
        }
        let properties = read_value_map(input, 0)?;
        nodes.insert(
            name,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
    }
    let mut relationships = BTreeMap::new();
    for _ in 0..read_len(input)? {
        let name = read_string(input)?;
        relationships.insert(
            name,
            RelRecord {
                id: RelId(read_u64(input)?),
                source: NodeId(read_u64(input)?),
                target: NodeId(read_u64(input)?),
                rel_type: RelTypeId(read_u32(input)?),
                properties: read_value_map(input, 0)?,
            },
        );
    }
    Ok(Binding {
        values,
        nodes,
        relationships,
    })
}

fn write_value_map(
    output: &mut Vec<u8>,
    values: &BTreeMap<String, Value>,
    depth: usize,
) -> Result<()> {
    check_depth(depth)?;
    write_len(output, values.len())?;
    for (name, value) in values {
        write_string(output, name)?;
        write_value(output, value, depth + 1)?;
    }
    Ok(())
}

fn read_value_map(input: &mut Cursor<&[u8]>, depth: usize) -> Result<BTreeMap<String, Value>> {
    check_depth(depth)?;
    let mut values = BTreeMap::new();
    for _ in 0..read_len(input)? {
        values.insert(read_string(input)?, read_value(input, depth + 1)?);
    }
    Ok(values)
}

fn write_value(output: &mut Vec<u8>, value: &Value, depth: usize) -> Result<()> {
    check_depth(depth)?;
    match value {
        Value::Null => output.push(0),
        Value::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        Value::Int(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            output.push(4);
            write_string(output, value)?;
        }
        Value::Binary(value) => {
            output.push(7);
            write_len(output, value.len())?;
            output.extend_from_slice(value);
        }
        Value::Uuid(value) => {
            output.push(8);
            output.extend_from_slice(value.as_bytes());
        }
        Value::List(values) => {
            output.push(5);
            write_len(output, values.len())?;
            for value in values {
                write_value(output, value, depth + 1)?;
            }
        }
        Value::Map(values) => {
            output.push(6);
            write_value_map(output, values, depth + 1)?;
        }
    }
    Ok(())
}

fn read_value(input: &mut Cursor<&[u8]>, depth: usize) -> Result<Value> {
    check_depth(depth)?;
    Ok(match read_u8(input)? {
        0 => Value::Null,
        1 => Value::Bool(match read_u8(input)? {
            0 => false,
            1 => true,
            value => {
                return Err(SkeinError::Execution(format!(
                    "invalid boolean tag in spill record: {value}"
                )));
            }
        }),
        2 => Value::Int(read_i64(input)?),
        3 => Value::Float(f64::from_bits(read_u64(input)?)),
        4 => Value::String(read_string(input)?),
        5 => {
            let mut values = Vec::new();
            for _ in 0..read_len(input)? {
                values.push(read_value(input, depth + 1)?);
            }
            Value::List(values)
        }
        6 => Value::Map(read_value_map(input, depth + 1)?),
        7 => {
            let len = read_len(input)?;
            let mut bytes = vec![0; len];
            input.read_exact(&mut bytes).map_err(|error| {
                SkeinError::Execution(format!("truncated binary in spill record: {error}"))
            })?;
            Value::Binary(bytes)
        }
        8 => {
            let mut bytes = [0_u8; 16];
            input.read_exact(&mut bytes).map_err(|error| {
                SkeinError::Execution(format!("truncated UUID in spill record: {error}"))
            })?;
            Value::Uuid(skein_core::Uuid::from_bytes(bytes))
        }
        tag => {
            return Err(SkeinError::Execution(format!(
                "invalid value tag in spill record: {tag}"
            )));
        }
    })
}

fn check_depth(depth: usize) -> Result<()> {
    if depth > MAX_VALUE_DEPTH {
        return Err(SkeinError::Execution(
            "spill value nesting exceeds the safety limit".to_string(),
        ));
    }
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    write_len(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(input: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_len(input)?;
    let mut bytes = vec![0; len];
    input.read_exact(&mut bytes).map_err(|error| {
        SkeinError::Execution(format!("truncated string in spill record: {error}"))
    })?;
    String::from_utf8(bytes)
        .map_err(|error| SkeinError::Execution(format!("invalid spill string: {error}")))
}

fn write_len(output: &mut Vec<u8>, value: usize) -> Result<()> {
    write_u64(
        output,
        u64::try_from(value).map_err(|_| {
            SkeinError::Execution("spill collection length is too large".to_string())
        })?,
    )
}

fn read_len(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let value = usize::try_from(read_u64(input)?).map_err(|_| {
        SkeinError::Execution("spill collection length does not fit in memory".to_string())
    })?;
    let remaining = input
        .get_ref()
        .len()
        .saturating_sub(input.position() as usize);
    if value > remaining {
        return Err(SkeinError::Execution(format!(
            "spill collection length {value} exceeds remaining payload {remaining}"
        )));
    }
    Ok(value)
}

fn write_u64(output: &mut Vec<u8>, value: u64) -> Result<()> {
    output
        .write_all(&value.to_le_bytes())
        .map_err(|error| SkeinError::Execution(format!("failed to encode spill integer: {error}")))
}

fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut bytes = [0; 1];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill tag: {error}")))?;
    Ok(bytes[0])
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    u32::try_from(read_u64(input)?)
        .map_err(|_| SkeinError::Execution("spill identifier exceeds u32".to_string()))
}

fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0; 8];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill integer: {error}")))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64(input: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut bytes = [0; 8];
    input
        .read_exact(&mut bytes)
        .map_err(|error| SkeinError::Execution(format!("truncated spill integer: {error}")))?;
    Ok(i64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::binding_memory_bytes;
    use crate::kernel::OperatorMemoryTracker;
    use crate::{ExecutionMemoryConfig, QueryMemoryClass, QueryMemoryLedger};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    fn test_memory(name: &str) -> ExecutionMemoryConfig {
        let nonce = NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed);
        ExecutionMemoryConfig {
            max_spill_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(16).unwrap(),
            max_total_spill_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_total_spill_runs: NonZeroUsize::new(64).unwrap(),
            min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
            spill_orphan_grace_period: Duration::ZERO,
            spill_directory: std::env::temp_dir().join(format!(
                "skein-spill-test-{}-{name}-{nonce}",
                std::process::id()
            )),
            ..ExecutionMemoryConfig::default()
        }
    }

    #[test]
    fn spill_staging_shares_the_query_root_with_operator_state() {
        let mut memory = test_memory("query-ledger");
        memory.query_memory_bytes = NonZeroUsize::new(200).unwrap();
        memory.blocking_operator_bytes = NonZeroUsize::new(256).unwrap();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut tracker = OperatorMemoryTracker::with_account(
            memory.blocking_operator_bytes,
            ledger.account(
                QueryMemoryClass::BlockingState,
                "test state",
                memory.blocking_operator_bytes,
            ),
        );
        tracker.try_charge(100).unwrap();
        let mut spill_budget = SpillBudgetTracker::with_ledger("Test", &memory, &ledger);
        let (run, mut writer) = spill_budget.create_run("query-ledger").unwrap();
        let binding = Binding {
            values: BTreeMap::from([("payload".to_string(), Value::String("x".repeat(64)))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };

        let error = writer.write(0, &binding, &mut spill_budget).unwrap_err();

        assert!(
            error.to_string().contains("query_memory_bytes 200"),
            "{error}"
        );
        assert_eq!(ledger.snapshot().used_bytes, 100);
        drop(writer);
        drop(run);
        drop(tracker);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(&memory.spill_directory).unwrap();
    }

    #[test]
    fn decoded_spill_state_is_admitted_before_payload_staging_is_released() {
        let mut memory = test_memory("decode-query-ledger");
        memory.query_memory_bytes = NonZeroUsize::new(300).unwrap();
        memory.blocking_operator_bytes = NonZeroUsize::new(300).unwrap();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut spill_budget = SpillBudgetTracker::with_ledger("Test", &memory, &ledger);
        let binding = Binding {
            values: BTreeMap::from([("payload".to_string(), Value::String("x".repeat(64)))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        let (run, mut writer) = spill_budget.create_run("decode-query-ledger").unwrap();
        writer.write(0, &binding, &mut spill_budget).unwrap();
        writer.finish().unwrap();

        let mut retained = OperatorMemoryTracker::with_account(
            memory.blocking_operator_bytes,
            ledger.account(
                QueryMemoryClass::BlockingState,
                "retained state",
                memory.blocking_operator_bytes,
            ),
        );
        retained.try_charge(100).unwrap();
        let mut decoded = OperatorMemoryTracker::with_account(
            memory.blocking_operator_bytes,
            ledger.account(
                QueryMemoryClass::BlockingState,
                "decoded state",
                memory.blocking_operator_bytes,
            ),
        );
        let mut reader = run.reader().unwrap();
        let record = reader
            .read_binding_record(memory.blocking_operator_bytes.get(), &spill_budget)
            .unwrap()
            .expect("spill record");
        let error = record
            .try_map(
                "Test merge",
                memory.blocking_operator_bytes.get(),
                &mut decoded,
                |_, binding| Ok(binding),
                binding_memory_bytes,
            )
            .unwrap_err();

        assert!(
            error.to_string().contains("query_memory_bytes 300"),
            "{error}"
        );
        assert_eq!(decoded.used_bytes, 0);
        assert_eq!(ledger.snapshot().used_bytes, 100);
        drop(reader);
        drop(run);
        drop(decoded);
        drop(retained);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(&memory.spill_directory).unwrap();
    }

    fn empty_binding() -> Binding {
        Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn spill_round_trip_preserves_bindings() {
        let binding = Binding {
            values: BTreeMap::from([
                ("binary".to_string(), Value::Binary(vec![0, 1, 0xfe, 0xff])),
                (
                    "nested".to_string(),
                    Value::Map(BTreeMap::from([(
                        "items".to_string(),
                        Value::List(vec![Value::Int(1), Value::String("two".to_string())]),
                    )])),
                ),
            ]),
            nodes: BTreeMap::from([(
                "n".to_string(),
                NodeRecord {
                    id: NodeId(7),
                    labels: BTreeSet::from([LabelId(3)]),
                    properties: BTreeMap::from([("score".to_string(), Value::Float(1.5))]),
                },
            )]),
            relationships: BTreeMap::from([(
                "r".to_string(),
                RelRecord {
                    id: RelId(11),
                    source: NodeId(7),
                    target: NodeId(8),
                    rel_type: RelTypeId(4),
                    properties: BTreeMap::from([("active".to_string(), Value::Bool(true))]),
                },
            )]),
        };
        let memory = test_memory("codec");
        let mut spill_budget = SpillBudgetTracker::new("CodecTest", &memory);
        let (run, mut writer) = spill_budget.create_run("codec-test").unwrap();
        writer.write(42, &binding, &mut spill_budget).unwrap();
        writer.finish().unwrap();
        let mut reader = run.reader().unwrap();
        assert_eq!(
            reader.read(usize::MAX).unwrap(),
            Some((42, binding.clone()))
        );
        assert_eq!(reader.read(usize::MAX).unwrap(), None);
        drop(reader);
        let mut accounted_reader = run.reader().unwrap();
        let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
        let record = accounted_reader
            .read_binding_record(memory.blocking_operator_bytes.get(), &spill_budget)
            .unwrap()
            .expect("spill record");
        let decoded = record
            .try_map(
                "CodecTest merge",
                memory.blocking_operator_bytes.get(),
                &mut tracker,
                |ordinal, binding| Ok((ordinal, binding)),
                |(_, binding)| binding_memory_bytes(binding),
            )
            .unwrap();
        assert_eq!(decoded, (42, binding));
        assert_eq!(tracker.used_bytes, binding_memory_bytes(&decoded.1));
        tracker.reset();
        drop(accounted_reader);
        drop(run);
        assert_eq!(memory.spill_pool_snapshot().unwrap().active_bytes, 0);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn shared_pool_rejects_concurrent_bytes_above_global_budget() {
        let mut memory = test_memory("global-bytes");
        memory.max_total_spill_bytes = NonZeroU64::new(80).unwrap();
        let mut first_budget = SpillBudgetTracker::new("First", &memory);
        let (first_run, mut first_writer) = first_budget.create_run("first").unwrap();
        first_writer
            .write(0, &empty_binding(), &mut first_budget)
            .unwrap();
        first_writer.finish().unwrap();

        let mut second_budget = SpillBudgetTracker::new("Second", &memory);
        let (second_run, mut second_writer) = second_budget.create_run("second").unwrap();
        second_writer
            .write(0, &empty_binding(), &mut second_budget)
            .unwrap();
        let error = second_writer
            .write(1, &empty_binding(), &mut second_budget)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("shared max_total_spill_bytes 80"));
        second_writer.finish().unwrap();

        let snapshot = memory.spill_pool_snapshot().unwrap();
        assert_eq!(snapshot.active_bytes, 80);
        assert_eq!(snapshot.active_runs, 2);
        assert_eq!(snapshot.pending_write_bytes, 0);
        drop(second_run);
        drop(first_run);
        let snapshot = memory.spill_pool_snapshot().unwrap();
        assert_eq!(snapshot.active_bytes, 0);
        assert_eq!(snapshot.active_runs, 0);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn shared_pool_releases_run_quota_when_run_is_removed() {
        let mut memory = test_memory("global-runs");
        memory.max_total_spill_runs = NonZeroUsize::new(1).unwrap();
        let mut first_budget = SpillBudgetTracker::new("First", &memory);
        let (first_run, first_writer) = first_budget.create_run("first").unwrap();
        first_writer.finish().unwrap();

        let mut second_budget = SpillBudgetTracker::new("Second", &memory);
        let error = match second_budget.create_run("second") {
            Ok(_) => panic!("shared run budget should reject a second live run"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("shared max_total_spill_runs 1"));
        drop(first_run);
        let (second_run, second_writer) = second_budget.create_run("second").unwrap();
        second_writer.finish().unwrap();
        drop(second_run);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn shared_pool_preserves_configured_free_space() {
        let mut memory = test_memory("free-space");
        memory.min_spill_free_bytes = NonZeroU64::new(u64::MAX).unwrap();
        let mut spill_budget = SpillBudgetTracker::new("FreeSpace", &memory);
        let (run, mut writer) = spill_budget.create_run("free-space").unwrap();
        let error = writer
            .write(0, &empty_binding(), &mut spill_budget)
            .unwrap_err();
        assert!(error.to_string().contains("min_spill_free_bytes"));
        drop(writer);
        drop(run);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn pool_startup_removes_only_eligible_skein_orphans() {
        let memory = test_memory("orphan-cleanup");
        std::fs::create_dir_all(&memory.spill_directory).unwrap();
        let orphan = memory
            .spill_directory
            .join("skein-spill-v1-stale-process-sort-1.spill");
        let unrelated = memory.spill_directory.join("application.data");
        std::fs::write(&orphan, b"orphan").unwrap();
        std::fs::write(&unrelated, b"keep").unwrap();

        let snapshot = memory.spill_pool_snapshot().unwrap();

        assert!(!orphan.exists());
        assert!(unrelated.exists());
        assert_eq!(snapshot.orphan_files_removed, 1);
        assert_eq!(snapshot.orphan_bytes_removed, 6);
        std::fs::remove_file(unrelated).unwrap();
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }
}
