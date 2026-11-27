# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""`QueryResult.counters`: the write operations one statement executed."""

import pytest

import hawdb

FIELDS = (
    "nodes_created",
    "nodes_deleted",
    "relationships_created",
    "relationships_deleted",
    "properties_set",
    "labels_added",
    "labels_removed",
    "indexes_added",
    "indexes_removed",
    "constraints_added",
    "constraints_removed",
)


def counts(counters):
    """The non-zero counters, keyed by field name."""
    return {
        name: getattr(counters, name) for name in FIELDS if getattr(counters, name)
    }


def test_create_counts_nodes_labels_and_properties(db):
    counters = db.execute("CREATE (:A {x: 1, y: 2})").counters
    assert isinstance(counters, hawdb.QueryCounters)
    assert counts(counters) == {
        "nodes_created": 1,
        "labels_added": 1,
        "properties_set": 2,
    }
    assert counters.contains_updates()


def test_create_relationship_and_detach_delete(db):
    created = db.execute("CREATE (:A {k: 1})-[:R]->(:B)").counters
    assert counts(created) == {
        "nodes_created": 2,
        "relationships_created": 1,
        "labels_added": 2,
        "properties_set": 1,
    }
    deleted = db.execute("MATCH (a:A) DETACH DELETE a").counters
    assert counts(deleted) == {"nodes_deleted": 1, "relationships_deleted": 1}


def test_same_value_set_counts_one_write(db):
    db.execute("CREATE (:A {k: 1, v: 5})")
    counters = db.execute("MATCH (n:A {k: 1}) SET n.v = 5").counters
    assert counts(counters) == {"properties_set": 1}
    assert counters.contains_updates()


def test_set_on_missing_node_and_reads_report_all_zeros(db):
    db.execute("CREATE (:A {k: 1})")
    for statement in (
        "MATCH (n:A {k: 2}) SET n.v = 5",
        "MATCH (n:A) RETURN n.k",
    ):
        counters = db.execute(statement).counters
        assert counts(counters) == {}
        assert not counters.contains_updates()


def test_sql_statements_report_no_counters(db):
    result = db.execute_sql("SELECT 1 AS one")
    assert not result.counters.contains_updates()


def test_transaction_statements_carry_their_own_counters(db):
    tx = db.transaction()
    first = tx.execute("CREATE (:A {k: 1})").counters
    second = tx.execute("MATCH (n:A) SET n.v = 1").counters
    committed = tx.commit().counters
    assert counts(first) == {"nodes_created": 1, "labels_added": 1, "properties_set": 1}
    assert counts(second) == {"properties_set": 1}
    assert not committed.contains_updates()


def test_counters_are_frozen_values(db):
    first = db.execute("CREATE (:A)").counters
    second = db.execute("CREATE (:A)").counters
    assert first == second
    assert hash(first) == hash(second)
    assert first != db.execute("MATCH (n) RETURN n").counters
    with pytest.raises(AttributeError):
        first.nodes_created = 5
    assert repr(first).startswith("hawdb.QueryCounters(nodes_created=1, ")
