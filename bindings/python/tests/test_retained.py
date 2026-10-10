# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Actual read-only buffer consumers, native leases and explicit pressure."""

import ctypes
import gc
import math
import struct
from contextlib import contextmanager

import pytest

import hawdb

QUERY = (
    "MATCH (n:Item) WHERE n.score >= $min "
    "RETURN n.score AS score, id(n) AS identity, n.score AS again"
)


def fixture(db, values=range(9), dtype="INT"):
    db.execute("CREATE NODE TABLE Item")
    db.execute(f"CREATE PROPERTY ON NODE TABLE Item(score) TYPE {dtype}")
    for value in values:
        db.execute("CREATE (:Item {score: $score})", {"score": value})


def cursor(db, rows=3, minimum=0, suffix="", **options):
    return db.execute_retained(
        QUERY + suffix, {"min": minimum},
        options=hawdb.RetainedOptions(batch_rows=rows, **options),
    )


class NativeBuffer(ctypes.Structure):
    # Stable Py_buffer ABI (Python >= 3.11), used only as a real C consumer.
    _fields_ = [
        ("buf", ctypes.c_void_p), ("obj", ctypes.c_void_p),
        ("length", ctypes.c_ssize_t), ("itemsize", ctypes.c_ssize_t),
        ("readonly", ctypes.c_int), ("ndim", ctypes.c_int),
        ("format", ctypes.c_char_p),
        ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
        ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
        ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
        ("internal", ctypes.c_void_p),
    ]


get_buffer = ctypes.pythonapi.PyObject_GetBuffer
get_buffer.argtypes = [ctypes.py_object, ctypes.POINTER(NativeBuffer), ctypes.c_int]
get_buffer.restype = ctypes.c_int
release_buffer = ctypes.pythonapi.PyBuffer_Release
release_buffer.argtypes = [ctypes.POINTER(NativeBuffer)]
release_buffer.restype = None


@contextmanager
def native_buffer(owner, flags=0x11C):
    descriptor = NativeBuffer()
    assert get_buffer(owner, ctypes.byref(descriptor), flags) == 0
    try:
        yield descriptor
    finally:
        release_buffer(ctypes.byref(descriptor))


def assert_empty(db):
    state = db.retained_snapshot_copy()
    assert state["retained_bytes"] == 0
    assert state["view_handles"] == 0
    assert state["buffer_owners"] == 0


def test_numeric_identity_selection_and_readonly(db):
    fixture(db)
    query = cursor(db, rows=9, suffix=" SKIP 1 LIMIT 2")
    assert query.column_count == 3
    assert query.schema_copy() == [
        {"name": "score", "format": "q", "nullable": True, "node_identity": False},
        {"name": "identity", "format": "Q", "nullable": False, "node_identity": True},
        {"name": "again", "format": "q", "nullable": True, "node_identity": False},
    ]
    batch = next(query)
    assert batch.physical_rows == 9 and batch.row_count == 2
    column = batch.column(0)
    again = batch.column(2)
    selection = column.selection()
    assert selection.provenance_copy()["offset_bytes"] == 4
    assert column.provenance_copy() == again.provenance_copy()
    with native_buffer(column) as original, native_buffer(again) as repeated:
        assert original.buf == repeated.buf
        assert original.itemsize == 8 and original.format == b"q"
        assert original.readonly == 1 and original.shape[0] == 9
        assert original.strides[0] == 8 and not original.suboffsets
    values = memoryview(column)
    indices = memoryview(selection)
    assert values.readonly and indices.readonly
    assert values.format == "q" and indices.format == "I"
    assert list(indices) == [1, 2]
    assert [values[index] for index in indices] == [1, 2]
    assert [batch.value_copy(0, row) for row in range(2)] == [1, 2]
    with pytest.raises(TypeError):
        values[0] = 99
    assert column.validity() is None
    with pytest.raises(StopIteration):
        next(query)
    assert column.status == "completed"
    values.release()
    indices.release()
    for owner in (column, again, selection, batch, query):
        owner.close()
    assert_empty(db)


def test_memoryview_survives_every_parent_close_and_final_release(db):
    fixture(db)
    query = cursor(db)
    batch = query.next_batch()
    column = batch.column(0)
    retained = column.retain()
    original = column.provenance_copy()
    assert retained.provenance_copy() == original
    values = memoryview(column)
    tiny = values[1:2]
    retained_values = memoryview(retained)
    for owner in (column, retained, batch, query):
        owner.close()
    assert db.retained_snapshot_copy()["retained_bytes"] > 0
    assert list(values) == [0, 1, 2] and list(tiny) == [1]
    with pytest.raises(hawdb.exceptions.RetainedError) as refused:
        memoryview(column)
    assert refused.value.kind == "closed"
    db.close()
    del batch, query, column, retained
    gc.collect()
    values.release()
    assert list(tiny) == [1] and list(retained_values) == [0, 1, 2]
    tiny.release()
    retained_values.release()
    with pytest.raises(ValueError):
        retained_values[0]


def test_slots_pressure_precedes_source_and_last_memoryview_release_resumes(db):
    fixture(db)
    query = cursor(db)
    first = query.next_batch()
    column = first.column(0)
    values = memoryview(column)
    column.close()
    first.close()
    second = query.next_batch()
    held = second.column(0)
    second.close()
    before = query.profile_copy()
    with pytest.raises(hawdb.exceptions.BackpressureError) as pressure:
        query.next_batch()
    assert pressure.value.kind == "backpressure" and pressure.value.retryable
    assert query.profile_copy()["visited_rows"] == before["visited_rows"] == 6
    assert list(values) == [0, 1, 2]
    values.release()
    third = query.next_batch()
    assert third.value_copy(0, 0) == 6
    held.close()
    third.close()
    query.close()
    assert_empty(db)


def test_handle_pressure_is_shared_and_borrowed_scalar_consumption_works():
    with hawdb.open() as db:
        fixture(db)
        query = cursor(db, rows=9)
        batch = query.next_batch()
        column = batch.column(0)
        exports = []
        try:
            while True:
                try:
                    exports.append(memoryview(column))
                except hawdb.exceptions.BackpressureError:
                    break
                assert len(exports) <= 1024
            assert len(exports) == 1021
            snapshot = db.retained_snapshot_copy()
            assert snapshot["view_handles"] == snapshot["handle_limit"] == 1024
            with pytest.raises(hawdb.exceptions.BackpressureError):
                cursor(db)
            assert batch.value_copy(0, 8) == 8
            exports.pop().release()
            retry = memoryview(column)
            assert retry[8] == 8
            retry.release()
        finally:
            for view in exports:
                view.release()
            column.close()
            batch.close()
            query.close()
        assert_empty(db)


def test_native_buffer_requests_refuse_without_partial_owner(db):
    fixture(db)
    query = cursor(db)
    batch = query.next_batch()
    column = batch.column(0)
    before = db.retained_snapshot_copy()
    for flags in (1, 4, 0x10, 0x20, 0x200, -1):
        output = NativeBuffer()
        with pytest.raises(BufferError):
            get_buffer(column, ctypes.byref(output), flags)
        assert not output.obj and not output.internal
        assert db.retained_snapshot_copy()["view_handles"] == before["view_handles"]
        assert db.retained_snapshot_copy()["retained_bytes"] == before["retained_bytes"]
    with native_buffer(column, flags=0) as byte_consumer:
        assert not byte_consumer.format and not byte_consumer.shape
        assert byte_consumer.length == 24 and byte_consumer.readonly
    for kwargs in ({"writable": True}, {"dtype": "d"}):
        with pytest.raises(hawdb.exceptions.RetainedError) as refused:
            batch.column(0, **kwargs)
        assert refused.value.kind == "copy_required"
    column.close()
    batch.close()
    query.close()
    assert_empty(db)


def test_late_budget_failure_is_sticky_and_previous_values_remain_readable(db):
    fixture(db)
    query = cursor(db, max_result_rows=3)
    batch = query.next_batch()
    column = batch.column(0)
    values = memoryview(column)
    for _ in range(2):
        with pytest.raises(hawdb.exceptions.RetainedError) as failed:
            query.next_batch()
        assert failed.value.kind == "result_budget" and not failed.value.retryable
    assert query.status == column.status == "failed"
    assert query.profile_copy()["source_pinned_rows"] == 0
    assert list(values) == [0, 1, 2]
    values.release()
    column.close()
    batch.close()
    query.close()
    assert_empty(db)


def test_float_ieee_null_layout_and_uint_identity(db):
    nan = struct.unpack("=d", struct.pack("=Q", 0x7FF800000000002A))[0]
    fixture(db, [None, nan, -0.0, 0.0, math.inf], dtype="FLOAT")
    query = cursor(db, rows=5, minimum=-math.inf)
    batch = query.next_batch()
    column = batch.column(0)
    validity = column.validity()
    identity = batch.column(1)
    with memoryview(column) as values, memoryview(validity) as mask, memoryview(identity) as ids:
        bits = [struct.unpack("=Q", struct.pack("=d", value))[0] for value in values]
        assert bits[1:] == [0x7FF800000000002A, 1 << 63, 0, 0x7FF0000000000000]
        assert mask.format == "Q" and mask[0] == 30
        assert ids.format == "Q" and len(set(ids)) == 5
        assert identity.validity() is None
    for owner in (validity, identity, column, batch, query):
        owner.close()
    assert_empty(db)


def test_all_null_and_empty_results_keep_schema(db):
    fixture(db, [None, None], dtype="FLOAT")
    query = cursor(db, rows=2, minimum=-1.0)
    schema = query.schema_copy()
    batch = query.next_batch()
    assert batch.row_count == 0 and batch.physical_rows == 2
    column = batch.column(0)
    validity = column.validity()
    with memoryview(column) as values, memoryview(validity) as mask:
        assert values.format == "d" and values.shape == (2,)
        assert mask[0] == 0
    assert query.next_batch() is None
    assert query.schema_copy() == schema and column.status == "completed"
    for owner in (validity, column, batch, query):
        owner.close()
    assert_empty(db)


def test_integer_bounds_gc_and_explicit_closed_access(db):
    fixture(db, [-(1 << 63), (1 << 63) - 1])
    query = cursor(db, rows=2, minimum=-(1 << 63))
    batch = query.next_batch()
    column = batch.column(0)
    view = memoryview(column)
    assert list(view) == [-(1 << 63), (1 << 63) - 1]
    column.close()
    batch.close()
    query.close()
    del column, batch, query, view
    gc.collect()
    assert_empty(db)


def test_unsupported_query_and_configuration_fail_without_binding(db):
    fixture(db)
    assert db.retained_snapshot_copy() is None
    with pytest.raises(hawdb.exceptions.RetainedError) as unsupported:
        db.execute_retained("MATCH (n:Item) RETURN count(n)")
    assert unsupported.value.kind == "unsupported_plan"
    assert db.retained_snapshot_copy() is None
    with pytest.raises(hawdb.exceptions.RetainedError) as reuse:
        cursor(db, require_source_reuse=True)
    assert reuse.value.kind == "copy_required"
    with pytest.raises(hawdb.exceptions.RetainedError):
        hawdb.RetainedOptions(writable=True)
    for kwargs in ({"batch_rows": 0}, {"batch_bytes": 0}, {"outstanding_batches": 0}):
        with pytest.raises(ValueError):
            hawdb.RetainedOptions(**kwargs)
    assert db.retained_snapshot_copy() is None
