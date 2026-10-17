# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Public capsules consumed through the actual standard C Data/Stream layouts."""

import ctypes as c
import gc
import importlib
import sys

import pytest
import hawdb

from .test_retained import assert_empty, cursor, fixture, native_buffer


class Schema(c.Structure):
    pass


class Array(c.Structure):
    pass


class Stream(c.Structure):
    pass


SchemaRelease = c.CFUNCTYPE(None, c.POINTER(Schema))
ArrayRelease = c.CFUNCTYPE(None, c.POINTER(Array))
StreamRelease = c.CFUNCTYPE(None, c.POINTER(Stream))
GetSchema = c.CFUNCTYPE(c.c_int, c.POINTER(Stream), c.POINTER(Schema))
GetNext = c.CFUNCTYPE(c.c_int, c.POINTER(Stream), c.POINTER(Array))
GetError = c.CFUNCTYPE(c.c_void_p, c.POINTER(Stream))
Schema._fields_ = [
    ("format", c.c_char_p), ("name", c.c_char_p), ("metadata", c.c_void_p),
    ("flags", c.c_int64), ("n_children", c.c_int64),
    ("children", c.POINTER(c.POINTER(Schema))), ("dictionary", c.POINTER(Schema)),
    ("release", SchemaRelease), ("private_data", c.c_void_p),
]
Array._fields_ = [
    ("length", c.c_int64), ("null_count", c.c_int64), ("offset", c.c_int64),
    ("n_buffers", c.c_int64), ("n_children", c.c_int64),
    ("buffers", c.POINTER(c.c_void_p)), ("children", c.POINTER(c.POINTER(Array))),
    ("dictionary", c.POINTER(Array)), ("release", ArrayRelease), ("private_data", c.c_void_p),
]
Stream._fields_ = [
    ("get_schema", GetSchema), ("get_next", GetNext), ("get_last_error", GetError),
    ("release", StreamRelease), ("private_data", c.c_void_p),
]
get_pointer = c.pythonapi.PyCapsule_GetPointer
get_pointer.argtypes = [c.py_object, c.c_char_p]
get_pointer.restype = c.c_void_p


def pointer(capsule, kind, name):
    return c.cast(get_pointer(capsule, name), c.POINTER(kind))


def move(source, kind, null_release):
    result = kind.from_buffer_copy(source.contents)
    source.contents.release = null_release()
    return result


def scores(array):
    column = array.children[0].contents
    values = c.cast(column.buffers[1], c.POINTER(c.c_int64))
    return [values[column.offset + row] for row in range(column.length)]


def test_array_capsules_identity_offsets_and_moved_lifetime(db):
    fixture(db)
    query = cursor(db, rows=9, suffix=" SKIP 1 LIMIT 2")
    batch = query.next_batch()
    column = batch.column(0)
    before = query.profile_copy()
    schema_cap, array_cap = batch.__arrow_c_array__()
    schema = pointer(schema_cap, Schema, b"arrow_schema")
    array = pointer(array_cap, Array, b"arrow_array")
    after = query.profile_copy()
    # Export admits descriptor capacity but does not advance execution/source work.
    assert after["query_peak_bytes"] >= before["query_peak_bytes"]
    assert {key: value for key, value in after.items() if key != "query_peak_bytes"} == {
        key: value for key, value in before.items() if key != "query_peak_bytes"
    }
    assert schema.contents.format == b"+s"
    assert schema.contents.children[0].contents.format == b"l"
    assert schema.contents.children[1].contents.format == b"L"
    assert array.contents.length == 2 and array.contents.offset == 0
    assert array.contents.children[0].contents.offset == 1
    with native_buffer(column) as original:
        assert array.contents.children[0].contents.buffers[1] == original.buf
        assert array.contents.children[2].contents.buffers[1] == original.buf
    moved_array = move(array, Array, ArrayRelease)
    moved_schema = move(schema, Schema, SchemaRelease)
    del schema_cap, array_cap
    gc.collect()
    for owner in (column, batch, query):
        owner.close()
    db.close()
    assert scores(moved_array) == [1, 2]
    assert moved_schema.children[0].contents.name == b"score"
    moved_array.release(c.byref(moved_array))
    moved_schema.release(c.byref(moved_schema))
    assert not moved_array.release and not moved_schema.release


def test_unconsumed_capsules_gc_and_schema_only_release_exactly(db):
    fixture(db)
    query = cursor(db)
    batch = query.next_batch()
    schema = batch.__arrow_c_schema__()
    caps = batch.__arrow_c_array__()
    batch.close()
    query.close()
    del batch, query, caps
    gc.collect()
    assert db.retained_snapshot_copy()["buffer_owners"] == 1
    # A schema-only owner keeps metadata without a numeric payload/slot.
    assert pointer(schema, Schema, b"arrow_schema").contents.n_children == 3
    del schema
    gc.collect()
    assert_empty(db)


def test_stream_no_pull_until_demand_backpressure_and_array_survives_stream(db):
    fixture(db)
    query = cursor(db)
    schema_cap = query.__arrow_c_schema__()
    assert query.profile_copy()["visited_rows"] == 0
    stream_cap = query.__arrow_c_stream__()
    assert query.status == "closed"  # successful ownership adoption
    stream = pointer(stream_cap, Stream, b"arrow_array_stream")
    schema = Schema()
    assert stream.contents.get_schema(stream, c.byref(schema)) == 0
    first, second, refused = Array(), Array(), Array()
    assert stream.contents.get_next(stream, c.byref(first)) == 0
    assert stream.contents.get_next(stream, c.byref(second)) == 0
    assert stream.contents.get_next(stream, c.byref(refused)) == 12
    assert not refused.release
    assert b"Backpressure" in c.string_at(stream.contents.get_last_error(stream))
    assert scores(first) == [0, 1, 2]
    first.release(c.byref(first))
    third = Array()
    assert stream.contents.get_next(stream, c.byref(third)) == 0
    assert scores(third) == [6, 7, 8]
    # Explicit stream release stops production but cannot revoke prior arrays.
    stream.contents.release(stream)
    db.close()
    del query, stream_cap, schema_cap
    gc.collect()
    assert scores(second) == [3, 4, 5]
    second.release(c.byref(second))
    third.release(c.byref(third))
    schema.release(c.byref(schema))


def test_empty_batch_is_not_eof(db):
    fixture(db, [None, None])
    query = cursor(db, rows=2)
    stream_cap = query.__arrow_c_stream__()
    stream = pointer(stream_cap, Stream, b"arrow_array_stream")
    empty, eof = Array(), Array()
    assert stream.contents.get_next(stream, c.byref(empty)) == 0
    assert empty.release and empty.length == 0
    empty.release(c.byref(empty))
    assert stream.contents.get_next(stream, c.byref(eof)) == 0
    assert not eof.release
    del stream_cap
    gc.collect()
    assert_empty(db)


def test_stream_sparse_failure_after_delivery_retains_earlier_array(db):
    fixture(db, values=(0, 1, 2, 3, 0, 5, 6, 7, 8))
    query = cursor(db, rows=3, minimum=2)
    stream_cap = query.__arrow_c_stream__()
    stream = pointer(stream_cap, Stream, b"arrow_array_stream")
    first = Array()
    assert stream.contents.get_next(stream, c.byref(first)) == 0
    assert scores(first) == [2]
    for _ in range(2):
        refused = Array()
        assert stream.contents.get_next(stream, c.byref(refused)) == 22
        assert not refused.release
        message = c.string_at(stream.contents.get_last_error(stream))
        assert b"SelectionRequiresMaterialization" in message
    stream.contents.release(stream)
    del stream_cap
    assert scores(first) == [2]
    assert db.retained_snapshot_copy()["retained_bytes"] > 0
    first.release(c.byref(first))
    assert_empty(db)


def test_sparse_array_and_stream_failure_is_explicit_and_sticky(db):
    fixture(db, [0, 2, 0, 3])
    query = cursor(db, rows=4, minimum=1)
    batch = query.next_batch()
    before = db.retained_snapshot_copy()
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        batch.__arrow_c_array__()
    assert failure.value.kind == "selection_requires_materialization"
    assert db.retained_snapshot_copy() == before
    for target in (batch.__arrow_c_array__, query.__arrow_c_stream__):
        with pytest.raises(hawdb.exceptions.RetainedError) as cast:
            target(object())
        assert cast.value.kind == "copy_required"
    batch.close()
    query.close()
    fresh = cursor(db, rows=4, minimum=1)
    cap = fresh.__arrow_c_stream__()
    stream = pointer(cap, Stream, b"arrow_array_stream")
    for _ in range(2):
        array = Array()
        assert stream.contents.get_next(stream, c.byref(array)) == 22
        assert not array.release
        assert b"SelectionRequiresMaterialization" in c.string_at(stream.contents.get_last_error(stream))
    del cap
    gc.collect()
    assert_empty(db)


def test_moved_child_code_module_reference_outlives_all_python_parents():
    module = importlib.import_module("hawdb._hawdb")
    gc.collect()
    baseline = sys.getrefcount(module)
    db = hawdb.open()
    fixture(db)
    query = cursor(db)
    batch = query.next_batch()
    schema_cap, array_cap = batch.__arrow_c_array__()
    schema = pointer(schema_cap, Schema, b"arrow_schema")
    array = pointer(array_cap, Array, b"arrow_array")
    child = move(array.contents.children[0], Array, ArrayRelease)
    field = move(schema.contents.children[0], Schema, SchemaRelease)
    array.contents.release(array)
    schema.contents.release(schema)
    batch.close()
    query.close()
    db.close()
    del batch, query, db, schema_cap, array_cap
    gc.collect()
    assert sys.getrefcount(module) >= baseline + 1
    assert c.cast(child.buffers[1], c.POINTER(c.c_int64))[child.offset] == 0
    assert field.name == b"score"
    child.release(c.byref(child))
    field.release(c.byref(field))
    gc.collect()
    # ctypes releases the GIL around a native callback. PyO3 safely defers
    # that final Python decref until its next attached extension entry.
    hawdb.capabilities()
    assert sys.getrefcount(module) == baseline


def test_gc_edges_preserve_native_external_module_roots(db):
    module = importlib.import_module("hawdb._hawdb")
    fixture(db)
    query = cursor(db)
    batch = query.next_batch()
    column = batch.column(0)
    for owner in (query, batch, column):
        assert module in gc.get_referents(owner)
    capsules = batch.__arrow_c_array__()
    # The native descriptor's shared reference is outside Python's GC graph.
    # Counting it as an internal Python edge could collect a live module.
    assert module not in gc.get_referents(batch)
    assert module in gc.get_referents(query)
    assert module in gc.get_referents(column)
    del capsules
    gc.collect()
    assert module in gc.get_referents(batch)
    for owner in (column, batch, query):
        owner.close()
    assert_empty(db)
