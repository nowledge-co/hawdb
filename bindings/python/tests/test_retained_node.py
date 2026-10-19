# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Real Python buffer consumers of immutable indexed complete-node owners."""

import gc
import struct

import pytest

import hawdb
from .test_retained import assert_empty, native_buffer

QUERY = "MATCH (n:Item {id: $id}) RETURN n AS node"
TEXT = "original-\x00-\u754c-\U0001f642"


def seed(db):
    seed_indexed(db)
    db.execute("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")


def seed_indexed(db):
    db.execute("CREATE NODE TABLE Item")
    db.execute("CREATE INDEX ON :Item(id)")
    db.execute(
        "CREATE (:Item {id: 1, text: $text, data: $data, nested: $nested})",
        {"text": TEXT, "data": b"\x00\xffpayload", "nested": {
            "items": [None, True, 42, {"text": TEXT}],
        }},
    )
    # Tiny labels choose a scan even when an equality index exists. Exercise
    # the indexed delivery contract with enough distinct keys to select it.
    db.execute(
        "UNWIND $rows AS row CREATE (:Item {id: row.id})",
        {"rows": [{"id": key} for key in range(2, 129)]},
    )


def test_node_payload_buffer_keeps_original_address_after_update_and_close(db):
    seed(db)
    cursor = db.execute_retained_node(QUERY, {"id": 1})
    assert cursor.access_path == "retained_indexed_node_view"
    node = cursor.next_node()
    assert node.kind == "node"
    text = node.field("text")
    other = node.field("text")
    retained = text.retain()
    with native_buffer(text) as first, native_buffer(other) as second, native_buffer(retained) as third:
        assert first.buf == second.buf == third.buf
        assert first.length == len(TEXT.encode("utf-8"))
        assert first.readonly == 1
    view = memoryview(text)
    assert view.readonly and view.format == "B"
    with pytest.raises(TypeError):
        view[0] = 0
    db.execute("MATCH (n:Item {id: 1}) SET n.text = 'replacement'")
    assert cursor.next_node() is None
    assert node.status == text.status == "completed"
    node.close()
    text.close()
    other.close()
    retained.close()
    cursor.close()
    assert db.retained_snapshot_copy()["retained_bytes"] > 0
    assert bytes(view) == TEXT.encode("utf-8")
    view.release()
    gc.collect()
    assert_empty(db)


def test_nested_values_labels_and_binary_remain_readable_after_database_drop(db):
    seed(db)
    cursor = db.execute_retained_node(QUERY, {"id": 1})
    node = cursor.next_node()
    nested = node.field("nested")
    items = nested.field("items")
    assert nested.kind == "map" and items.kind == "list"
    assert items.item(0).scalar_copy() is None
    assert items.item(1).scalar_copy() is True
    assert items.item(2).scalar_copy() == 42
    text = items.item(3).field("text")
    labels = node.field("labels")
    label = labels.item(0)
    data = node.field("data")
    views = [memoryview(text), memoryview(label), memoryview(data)]
    assert cursor.next_node() is None
    assert node.field("_id").scalar_copy() >= 0
    db.close()
    for owner in (node, nested, items, text, labels, label, data):
        owner.close()
    cursor.close()
    assert [bytes(view) for view in views] == [TEXT.encode("utf-8"), b"Item", b"\x00\xffpayload"]
    for view in views:
        view.release()


def test_unfinished_close_preserves_buffer_and_terminal_status(db):
    seed(db)
    cursor = db.execute_retained_node(QUERY, {"id": 1})
    node = cursor.next_node()
    text = node.field("text")
    view = memoryview(text)
    cursor.close()
    assert node.status == text.status == "closed"
    node.close()
    text.close()
    assert bytes(view) == TEXT.encode("utf-8")
    view.release()
    assert_empty(db)


def test_missing_seek_and_explicit_refusals_do_not_materialize(db):
    seed(db)
    missing = db.execute_retained_node(QUERY, {"id": 999})
    assert db.retained_snapshot_copy()["retained_bytes"] == 0
    assert missing.next_node() is None
    missing.close()
    assert_empty(db)
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        db.execute_retained_node("MATCH (n:Item) RETURN n")
    assert failure.value.kind == "unsupported_plan"
    cursor = db.execute_retained_node(QUERY, {"id": 1})
    node = cursor.next_node()
    assert node.field("absent") is None
    text = node.field("text")
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        text.scalar_copy()
    assert failure.value.kind == "copy_required"
    with pytest.raises(hawdb.exceptions.RetainedError):
        memoryview(node)
    labels = node.field("labels")
    with pytest.raises(IndexError):
        labels.item(1)
    for owner in (labels, text, node):
        owner.close()
    cursor.close()
    assert_empty(db)


def test_indexed_lookup_requires_a_unique_constraint_before_admission(db):
    seed_indexed(db)
    for key in [1, 999]:
        with pytest.raises(hawdb.exceptions.RetainedError) as failure:
            db.execute_retained_node(QUERY, {"id": key})
        assert failure.value.kind == "unsupported_plan"
        assert not failure.value.retryable
        assert db.retained_snapshot_copy() is None


def test_node_admission_rolls_back_before_python_delivery(db):
    seed(db)
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        db.execute_retained_node(QUERY, {"id": 1}, options=hawdb.RetainedOptions(batch_bytes=1))
    assert failure.value.kind == "working_unit_too_large"
    assert db.retained_snapshot_copy() is None
    with db.transaction():
        with pytest.raises(hawdb.exceptions.RetainedError) as failure:
            db.execute_retained_node(QUERY, {"id": 1})
        assert failure.value.kind == "execution"


def test_fixed_width_fields_export_original_read_only_scalar_storage(db):
    seed(db)
    db.execute("MATCH (n:Item {id: 1}) SET n.ratio = $ratio", {"ratio": -0.0})
    cursor = db.execute_retained_node(QUERY, {"id": 1})
    node = cursor.next_node()
    identity = node.field("_id")
    identifier = node.field("id")
    ratio = node.field("ratio")
    nested = node.field("nested").field("items")
    boolean = nested.item(1)
    for owner, expected, format_ in ((identity, identity.scalar_copy(), "q"), (identifier, 1, "q"), (ratio, -0.0, "d"), (boolean, True, "?")):
        retained = owner.retain()
        with native_buffer(owner) as first, native_buffer(retained) as second:
            assert first.buf == second.buf
            assert first.readonly == 1
        view = memoryview(owner)
        assert view.format == format_ and view.shape == (1,)
        assert view[0] == expected
        if format_ == "d":
            assert bytes(view) == struct.pack("=d", -0.0)
        owner.close()
        retained.close()
        assert view[0] == expected
        view.release()
    nested.close()
    node.close()
    cursor.close()
    assert_empty(db)


def test_tiny_cost_selected_scan_refuses_without_owned_fallback(db):
    db.execute("CREATE NODE TABLE Item")
    db.execute("CREATE INDEX ON :Item(id)")
    db.execute("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")
    db.execute("CREATE (:Item {id: 1})")
    assert db.execute(QUERY, {"id": 1}).fetchone()["node"]["id"] == 1
    with pytest.raises(hawdb.exceptions.RetainedError) as failure:
        db.execute_retained_node(QUERY, {"id": 1})
    assert failure.value.kind == "unsupported_plan"
    # Rejection precedes lazy retained-governor construction and source capture.
    assert db.retained_snapshot_copy() is None
