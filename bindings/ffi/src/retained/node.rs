// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Indexed node owners reuse immutable rows and catalog labels. Containers are
//! traversed through admitted index paths, without materializing foreign maps.

use super::*;
use hawdb::{RetainedNodeCursor, RetainedNodeField, RetainedNodeLabels, RetainedNodeView, Value};

pub const HAWDB_RETAINED_NODE: u32 = 1;
pub const HAWDB_RETAINED_NULL: u32 = 2;
pub const HAWDB_RETAINED_BOOL: u32 = 3;
pub const HAWDB_RETAINED_NODE_INT64: u32 = 4;
pub const HAWDB_RETAINED_NODE_FLOAT64: u32 = 5;
pub const HAWDB_RETAINED_UTF8: u32 = 6;
pub const HAWDB_RETAINED_BINARY: u32 = 7;
pub const HAWDB_RETAINED_UUID: u32 = 8;
pub const HAWDB_RETAINED_LIST: u32 = 9;
pub const HAWDB_RETAINED_MAP: u32 = 10;

/// All pointers borrow this independently releasable owner. Scalars are original
/// native-endian storage, not descriptor copies. Names are UTF-8, not NUL-ended.
/// Containers and NULL have no data buffer. These are row views, not Arrow arrays.
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct HawdbRetainedNodeV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub owner_namespace: u64,
    pub owner_id: u64,
    pub kind: u32,
    pub flags: u32,
    pub child_count: u64,
    pub data: *const c_void,
    pub byte_length: u64,
    pub name: *const c_char,
    pub name_len: u64,
}

pub(super) struct NodeCursorState {
    pub cursor: RetainedNodeCursor,
    pub terminal_code: u32,
}
pub(super) struct NodeValue {
    // Path capacity is released before its originating view reservation.
    path: Box<[usize]>,
    pub view: RetainedNodeView,
}

enum Payload<'a> {
    Node,
    Identity(&'a u64),
    Labels(RetainedNodeLabels<'a>),
    Label(&'a str),
    Value(&'a Value),
}
impl<'a> Payload<'a> {
    fn child(self, view: &'a RetainedNodeView, index: usize) -> Result<(&'a str, Self)> {
        let invalid = || HAWDB_RETAINED_INVALID_COLUMN;
        Ok(match self {
            Self::Node => match index {
                0 => (
                    "_id",
                    Self::Identity(view.identity_storage().map_err(|e| error_code(&e))?),
                ),
                1 => {
                    let Some(RetainedNodeField::Labels(labels)) =
                        view.field("labels").map_err(|e| error_code(&e))?
                    else {
                        return Err(HAWDB_RETAINED_EXECUTION_ERROR);
                    };
                    ("labels", Self::Labels(labels))
                }
                _ => {
                    let (name, value) = view
                        .properties()
                        .map_err(|e| error_code(&e))?
                        .nth(index - 2)
                        .ok_or_else(invalid)?;
                    (name, Self::Value(value))
                }
            },
            Self::Labels(labels) => (
                "",
                Self::Label(labels.iter().nth(index).ok_or_else(invalid)?),
            ),
            Self::Value(Value::List(values)) => {
                ("", Self::Value(values.get(index).ok_or_else(invalid)?))
            }
            Self::Value(Value::Map(values)) => {
                let (name, value) = values.iter().nth(index).ok_or_else(invalid)?;
                (name, Self::Value(value))
            }
            _ => return Err(HAWDB_RETAINED_INVALID_COLUMN),
        })
    }
}
impl NodeValue {
    fn payload(&self) -> Result<(&str, Payload<'_>)> {
        let mut value = Payload::Node;
        let mut name = self.view.column_name();
        for &index in &self.path {
            (name, value) = value.child(&self.view, index)?;
        }
        Ok((name, value))
    }
    fn descriptor(&self, id: u64) -> Result<HawdbRetainedNodeV1> {
        let (name, payload) = self.payload()?;
        let mut out = HawdbRetainedNodeV1 {
            abi_version: HAWDB_RETAINED_ABI_V1,
            struct_size: std::mem::size_of::<HawdbRetainedNodeV1>() as u32,
            owner_namespace: namespace(),
            owner_id: id,
            flags: HAWDB_RETAINED_READ_ONLY,
            name: name.as_ptr().cast(),
            name_len: name.len() as u64,
            ..Default::default()
        };
        macro_rules! scalar {
            ($value:expr, $kind:expr) => {{
                out.kind = $kind;
                out.data = ptr::from_ref($value).cast();
                out.byte_length = std::mem::size_of_val($value) as u64;
            }};
        }
        macro_rules! bytes {
            ($value:expr, $kind:expr) => {{
                out.kind = $kind;
                out.byte_length = $value.len() as u64;
                if !$value.is_empty() {
                    out.data = $value.as_ptr().cast();
                }
            }};
        }
        match payload {
            Payload::Node => {
                out.kind = HAWDB_RETAINED_NODE;
                out.child_count = self
                    .view
                    .properties()
                    .map_err(|e| error_code(&e))?
                    .count()
                    .checked_add(2)
                    .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)? as u64;
            }
            Payload::Identity(value) => scalar!(value, HAWDB_RETAINED_NODE_INT64),
            Payload::Labels(labels) => {
                out.kind = HAWDB_RETAINED_LIST;
                out.child_count = labels.iter().len() as u64;
            }
            Payload::Label(value) => bytes!(value.as_bytes(), HAWDB_RETAINED_UTF8),
            Payload::Value(value) => match value {
                Value::Null => out.kind = HAWDB_RETAINED_NULL,
                Value::Bool(value) => scalar!(value, HAWDB_RETAINED_BOOL),
                Value::Int(value) => scalar!(value, HAWDB_RETAINED_NODE_INT64),
                Value::Float(value) => scalar!(value, HAWDB_RETAINED_NODE_FLOAT64),
                Value::String(value) => bytes!(value.as_bytes(), HAWDB_RETAINED_UTF8),
                Value::Binary(value) => bytes!(value, HAWDB_RETAINED_BINARY),
                Value::Uuid(value) => bytes!(value.as_bytes(), HAWDB_RETAINED_UUID),
                Value::List(value) => {
                    out.kind = HAWDB_RETAINED_LIST;
                    out.child_count = value.len() as u64;
                }
                Value::Map(value) => {
                    out.kind = HAWDB_RETAINED_MAP;
                    out.child_count = value.len() as u64;
                }
            },
        }
        Ok(out)
    }
    fn retained(&self, child: Option<usize>) -> Result<Self> {
        let length = self
            .path
            .len()
            .checked_add(usize::from(child.is_some()))
            .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)?;
        let path_bytes = length
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)?;
        let metadata = metadata_bytes(std::mem::size_of::<HawdbRetainedNodeV1>())
            .checked_add(512)
            .and_then(|bytes| bytes.checked_add(path_bytes))
            .ok_or(HAWDB_RETAINED_SIZE_OVERFLOW)?;
        // Admission precedes allocation and Arc cloning. No payload is copied.
        let view = self
            .view
            .try_retain_with_metadata(metadata)
            .map_err(|e| error_code(&e))?;
        let mut path = Vec::with_capacity(length);
        path.extend_from_slice(&self.path);
        if let Some(index) = child {
            path.push(index);
        }
        Ok(Self {
            path: path.into_boxed_slice(),
            view,
        })
    }
}

/// Open a strict single-match indexed node cursor. Input parameters use JSON;
/// results never do. All layout refusals are inherited from the native planner.
///
/// # Safety
/// db/request must be valid for this call. out must be aligned and writable for
/// out_size bytes. The library mapping must outlive every returned owner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_node_query(
    db: *mut HawdbDatabase,
    request: *const HawdbRetainedQueryV1,
    request_size: u32,
    out: *mut HawdbRetainedCursorV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            if db.is_null()
                || !db.is_aligned()
                || request.is_null()
                || !request.is_aligned()
                || (request_size as usize) < std::mem::size_of::<HawdbRetainedQueryV1>()
            {
                return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
            }
            let request = &*request;
            if request.abi_version != HAWDB_RETAINED_ABI_V1
                || request.struct_size > request_size
                || (request.struct_size as usize) < std::mem::size_of::<HawdbRetainedQueryV1>()
                || request.reserved != 0
                || request.flags
                    & !(HAWDB_RETAINED_REQUIRE_SOURCE_REUSE | HAWDB_RETAINED_REQUEST_WRITABLE)
                    != 0
            {
                return Err(HAWDB_RETAINED_INVALID_ARGUMENT);
            }
            if request.flags & HAWDB_RETAINED_REQUEST_WRITABLE != 0 {
                return Err(HAWDB_RETAINED_COPY_REQUIRED);
            }
            let text = read_str(
                FfiStr {
                    ptr: request.cypher,
                    len: bytes_len(request.cypher_len)?,
                },
                "cypher",
            )
            .map_err(|_| HAWDB_RETAINED_INVALID_ARGUMENT)?;
            let params = parse_cypher_params(FfiStr {
                ptr: request.params_json,
                len: bytes_len(request.params_len)?,
            })
            .map_err(|_| HAWDB_RETAINED_INVALID_ARGUMENT)?;
            let mut options = RetainedQueryOptions {
                require_source_reuse: true,
                ..Default::default()
            };
            if let Some(rows) = NonZeroUsize::new(request.batch_rows as usize) {
                options.batch_rows = rows;
            }
            if let Some(slots) = NonZeroUsize::new(request.outstanding_batches as usize) {
                options.outstanding_batches = slots;
            }
            if request.batch_bytes != 0 {
                options.batch_bytes = NonZeroUsize::new(bytes_len(request.batch_bytes)?)
                    .ok_or(HAWDB_RETAINED_INVALID_ARGUMENT)?;
            }
            options.adapter_metadata_bytes = metadata_bytes(
                std::mem::size_of::<HawdbRetainedNodeV1>()
                    .max(std::mem::size_of::<HawdbRetainedCursorV1>()),
            ) + std::mem::size_of::<Mutex<NodeCursorState>>()
                + 512;
            options.minimum_shared_handles = NonZeroUsize::new(3).expect("nonzero minimum");
            let id = next_id()?;
            let cursor = (&*db)
                .inner
                .lock()
                .map_err(|_| HAWDB_RETAINED_PANIC)?
                .query_with_params_retained_node(text, &params, options)
                .map_err(|e| error_code(&e))?;
            let out = HawdbRetainedCursorV1 {
                abi_version: HAWDB_RETAINED_ABI_V1,
                struct_size: std::mem::size_of::<HawdbRetainedCursorV1>() as u32,
                owner_namespace: namespace(),
                owner_id: id,
                column_count: 1,
            };
            register(
                id,
                Object::NodeCursor(Box::new(Mutex::new(NodeCursorState {
                    cursor,
                    terminal_code: 0,
                }))),
            );
            Ok(out)
        })
    }
}

/// Pull one independently owned node. EOF completes provisional values. A failed
/// registration aborts delivery rather than allowing a later successful EOF.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Returned pointers remain
/// readable until their owner is released; synchronize release with all reads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_node_next(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedNodeV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let object = lookup(owner_namespace, owner_id)?;
            let Object::NodeCursor(state) = object.as_ref() else {
                return Err(HAWDB_RETAINED_INVALID_HANDLE);
            };
            let id = next_id()?;
            let mut state = state.lock().map_err(|_| HAWDB_RETAINED_PANIC)?;
            if state.terminal_code != 0 {
                return Err(state.terminal_code);
            }
            let result = catch_unwind(AssertUnwindSafe(|| -> Result<_> {
                let Some(view) = state.cursor.next_node().map_err(|e| error_code(&e))? else {
                    return Err(HAWDB_RETAINED_EOF);
                };
                let value = NodeValue {
                    path: Box::default(),
                    view,
                };
                let descriptor = value.descriptor(id)?;
                register(id, Object::Node(value));
                Ok(descriptor)
            }));
            match result {
                Ok(Ok(descriptor)) => Ok(descriptor),
                Ok(Err(code)) => {
                    if !matches!(
                        code,
                        HAWDB_RETAINED_EOF | HAWDB_RETAINED_BACKPRESSURE | HAWDB_RETAINED_CLOSED
                    ) {
                        state.terminal_code = code;
                        state.cursor.abort_delivery();
                    }
                    Err(code)
                }
                Err(_) => {
                    state.terminal_code = HAWDB_RETAINED_PANIC;
                    state.cursor.abort_delivery();
                    Err(HAWDB_RETAINED_PANIC)
                }
            }
        })
    }
}

fn export_value(
    owner_namespace: u64,
    owner_id: u64,
    child: impl FnOnce(&NodeValue) -> Result<Option<usize>>,
) -> Result<HawdbRetainedNodeV1> {
    let object = lookup(owner_namespace, owner_id)?;
    let Object::Node(value) = object.as_ref() else {
        return Err(HAWDB_RETAINED_INVALID_HANDLE);
    };
    let index = child(value)?;
    if let Some(index) = index {
        value.payload()?.1.child(&value.view, index)?;
    }
    let id = next_id()?;
    let retained = value.retained(index)?;
    let descriptor = retained.descriptor(id)?;
    register(id, Object::Node(retained));
    Ok(descriptor)
}

/// Create an independent child owner. Node ordering is _id, labels, then native
/// sorted properties. Lists use index order; maps use native sorted key order.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Synchronize pointer reads
/// with release. A child survives release of all its ancestors and database.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_node_child(
    owner_namespace: u64,
    owner_id: u64,
    index: u64,
    out: *mut HawdbRetainedNodeV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            export_value(owner_namespace, owner_id, |_| {
                Ok(Some(
                    usize::try_from(index).map_err(|_| HAWDB_RETAINED_INVALID_COLUMN)?,
                ))
            })
        })
    }
}

/// Look up a node/map field without cloning its key or payload. Missing fields
/// return InvalidColumn and an empty output; other payload kinds refuse.
///
/// # Safety
/// name must be readable UTF-8 for name_len bytes. out must be aligned/writable
/// for out_size bytes. Synchronize returned pointer reads with owner release.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_node_field(
    owner_namespace: u64,
    owner_id: u64,
    name: *const c_char,
    name_len: u64,
    out: *mut HawdbRetainedNodeV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            let name = read_str(
                FfiStr {
                    ptr: name,
                    len: bytes_len(name_len)?,
                },
                "field",
            )
            .map_err(|_| HAWDB_RETAINED_INVALID_ARGUMENT)?;
            export_value(owner_namespace, owner_id, |value| {
                let index = match value.payload()?.1 {
                    Payload::Node => match name {
                        "_id" => Some(0),
                        "labels" => Some(1),
                        _ => value
                            .view
                            .properties()
                            .map_err(|e| error_code(&e))?
                            .position(|(key, _)| key == name)
                            .map(|i| i + 2),
                    },
                    Payload::Value(Value::Map(map)) => map.keys().position(|key| key == name),
                    _ => None,
                };
                Ok(Some(index.ok_or(HAWDB_RETAINED_INVALID_COLUMN)?))
            })
        })
    }
}

/// Retain an independent owner, preserving pointer identity and path.
///
/// # Safety
/// out must be aligned/writable for out_size bytes. Release its owner only after
/// all pointer reads finish, independently of the original owner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_retained_node_retain(
    owner_namespace: u64,
    owner_id: u64,
    out: *mut HawdbRetainedNodeV1,
    out_size: u32,
) -> u32 {
    unsafe {
        output(out, out_size, || {
            export_value(owner_namespace, owner_id, |_| Ok(None))
        })
    }
}

#[cfg(test)]
mod tests;
