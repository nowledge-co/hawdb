# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Optional third-party consumers, using only documented public Arrow APIs.

HawDB and the default binding tests do not depend on PyArrow. Run this file in
an explicitly provisioned consumer environment to qualify that installed
version, instead of adding an Arrow SDK to the producer or default CI.
"""

import gc
import math
import struct

import pytest

import hawdb

from .test_retained import assert_empty, cursor, fixture, native_buffer

pa = pytest.importorskip("pyarrow")


def test_public_record_batch_preserves_native_addresses_offsets_and_lifetime(db):
    fixture(db)
    query = cursor(db, rows=9, suffix=" SKIP 1 LIMIT 2")
    batch = query.next_batch()
    column = batch.column(0)
    with native_buffer(column) as native:
        address = native.buf
    imported = pa.record_batch(batch)
    assert imported.column(0).buffers()[1].address == address
    assert imported.column(2).buffers()[1].address == address
    assert imported.column(0).offset == 1
    assert imported.column(0).to_pylist() == [1, 2]
    assert imported.schema.field(1).type == pa.uint64()
    assert imported.schema.field(1).metadata[b"hawdb:role"] == b"node_identity"
    tiny = imported.column(0).slice(1, 1)
    for owner in (column, batch, query):
        owner.close()
    del imported, column, batch, query
    gc.collect()
    assert tiny.to_pylist() == [2]
    assert tiny.buffers()[1].address == address
    assert db.retained_snapshot_copy()["retained_bytes"] > 0
    del tiny
    gc.collect()
    assert_empty(db)


def test_public_stream_reader_backpressure_is_error_and_release_allows_retry(db):
    fixture(db)
    query = cursor(db)
    reader = pa.RecordBatchReader.from_stream(query)
    assert query.status == "closed"
    assert reader.schema.field(0).type == pa.int64()
    first = reader.read_next_batch()
    second = reader.read_next_batch()
    with pytest.raises(pa.ArrowException, match="Backpressure"):
        reader.read_next_batch()
    assert first.column(0).to_pylist() == [0, 1, 2]
    del first
    gc.collect()
    third = reader.read_next_batch()
    assert third.column(0).to_pylist() == [6, 7, 8]
    with pytest.raises(StopIteration):
        reader.read_next_batch()
    reader.close()
    db.close()
    assert second.column(0).to_pylist() == [3, 4, 5]
    assert third.column(0).to_pylist() == [6, 7, 8]


def test_public_empty_schema_and_sparse_refusal(db):
    fixture(db, [None, None])
    query = cursor(db, rows=2)
    schema = pa.schema(query)
    assert schema.field(0).type == pa.int64()
    reader = pa.RecordBatchReader.from_stream(query)
    empty = reader.read_next_batch()
    assert empty.num_rows == 0 and empty.num_columns == 3
    with pytest.raises(StopIteration):
        reader.read_next_batch()
    reader.close()
    del empty, reader
    gc.collect()
    assert_empty(db)

    db.execute("MATCH (n:Item) SET n.score = 0")
    for value in (2, 0, 3):
        db.execute("CREATE (:Item {score: $score})", {"score": value})
    sparse_query = cursor(db, rows=5, minimum=1)
    sparse_batch = sparse_query.next_batch()
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        pa.record_batch(sparse_batch)
    assert failure.value.kind == "selection_requires_materialization"
    sparse_batch.close()
    sparse_query.close()
    reader = pa.RecordBatchReader.from_stream(cursor(db, rows=5, minimum=1))
    for _ in range(2):
        with pytest.raises(pa.ArrowException, match="SelectionRequiresMaterialization"):
            reader.read_next_batch()
    reader.close()
    del reader, sparse_batch, sparse_query
    gc.collect()
    assert_empty(db)


def test_public_numeric_bits_and_schema_cast_refusal(db):
    fixture(db, [-0.0, 0.0, float("inf"), float("nan")], dtype="FLOAT")
    query = cursor(db, rows=4, minimum=float("-inf"))
    batch = query.next_batch()
    imported = pa.record_batch(batch)
    values = imported.column(0).to_pylist()
    assert struct.pack("=d", values[0]) == struct.pack("=d", -0.0)
    assert struct.pack("=d", values[1]) == struct.pack("=d", 0.0)
    assert values[2] == float("inf") and math.isnan(values[3])
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        pa.record_batch(batch, schema=imported.schema)
    assert failure.value.kind == "copy_required"
    batch.close()
    query.close()
    del imported, batch, query
    gc.collect()
    assert_empty(db)
