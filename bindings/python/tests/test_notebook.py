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

"""Notebook HTML display and the optional IPython magics, without Jupyter."""

import copy
import os
import subprocess
import sys
import types

import pytest

import hawdb
from hawdb import _display


def _run_fresh(code):
    # A fresh interpreter, because this process has imported `hawdb.magic`;
    # PYTHONPATH carries the test path into it (Bazel sets no site-packages).
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


def test_html_table_lists_columns_and_rows(db):
    db.execute("CREATE (s:Stock {code: $code})", {"code": "603122"})
    html = db.execute("MATCH (s:Stock) RETURN s.code")._repr_html_()
    assert "<th>s.code</th>" in html
    assert "<td>603122</td>" in html
    assert "more row" not in html


def test_html_escapes_text(db):
    db.execute("CREATE (s:Stock {code: $code})", {"code": "<script>x</script>"})
    html = db.execute("MATCH (s:Stock) RETURN s.code")._repr_html_()
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_html_ignores_fetch_cursor(db):
    db.execute("CREATE (s:Stock {code: 'a'})")
    result = db.execute("MATCH (s:Stock) RETURN s.code")
    result.fetchall()
    assert "<td>a</td>" in result._repr_html_()


def test_html_caps_rows_and_notes_omitted(db):
    with db.transaction() as tx:
        for i in range(105):
            tx.execute("CREATE (s:Stock {n: $n})", {"n": i})
    html = db.execute("MATCH (s:Stock) RETURN s.n")._repr_html_()
    assert html.count("<tr>") == 101  # header plus 100 rows
    assert "5 more rows not shown (105 total)" in html


def test_render_html_limit_is_adjustable():
    rows = [{"n": i} for i in range(5)]
    html = _display.render_html(["n"], rows, limit=2)
    assert html.count("<tr>") == 3
    assert "3 more rows not shown (5 total)" in html
    assert "more row" not in _display.render_html(["n"], rows, limit=5)


def test_render_html_counts_rows_beyond_those_passed():
    html = _display.render_html(["n"], [{"n": 0}, {"n": 1}], total=1000, limit=2)
    assert html.count("<tr>") == 3
    assert "998 more rows not shown (1000 total)" in html


def test_html_notes_empty_results(db):
    html = db.execute("MATCH (s:Missing) RETURN s.code")._repr_html_()
    assert html == _display.render_html(["s.code"], [])
    assert "<th>s.code</th>" in html
    assert "<p>(no rows)</p>" in html


def test_plain_repr_is_unchanged(db):
    db.execute("CREATE (s:Stock {code: 'a'})")
    assert repr(db.execute("MATCH (s:Stock) RETURN s.code")) == (
        'hawdb.QueryResult(columns=["s.code"], rows=1)'
    )


def test_render_html_formats_null_and_bytes():
    html = _display.render_html(["a", "b"], [{"a": None, "b": b"\x00"}])
    assert "<td>NULL</td>" in html
    assert "<td>b&#x27;\\x00&#x27;</td>" in html


class FakeShell:
    """Mirrors IPython's `magics_manager.magics` layout and counts registrations."""

    def __init__(self):
        self.user_ns = {}
        self.registrations = 0
        self.magics_manager = types.SimpleNamespace(magics={"line": {}, "cell": {}})

    @property
    def magics(self):
        return {
            (kind, name): func
            for kind, table in self.magics_manager.magics.items()
            for name, func in table.items()
        }

    def register_magic_function(self, func, kind, name):
        self.registrations += 1
        self.magics_manager.magics[kind][name] = func


@pytest.fixture
def shell():
    from hawdb import magic

    fake = FakeShell()
    magic.register(fake)
    yield fake
    magic.unregister(fake)


def test_magics_register_names(shell):
    assert set(shell.magics) == {
        ("line", "hawdb"),
        ("cell", "cypher"),
        ("cell", "sql"),
    }


def test_register_is_idempotent_and_unload_cleans_up(shell):
    from hawdb import magic

    magic.register(shell)  # `%load_ext` after the import-time registration
    magic.load_ipython_extension(shell)
    assert shell.registrations == 3

    shell.magics[("line", "hawdb")]("open")
    db = magic._selected
    magic.unload_ipython_extension(shell)
    assert shell.magics == {}
    assert magic._selected is None and magic._shell is None
    with pytest.raises(RuntimeError, match="database is closed"):
        db.execute("MATCH (s:Stock) RETURN s.code")

    magic.load_ipython_extension(shell)  # reload after unload works
    assert len(shell.magics) == 3


def test_close_leaves_notebook_owned_handles_open(shell):
    line = shell.magics[("line", "hawdb")]
    cypher = shell.magics[("cell", "cypher")]
    mine = hawdb.open()
    shell.user_ns["mine"] = mine
    line("use mine")
    line("close")  # forgets the selection, does not close the user's handle
    mine.execute("CREATE (s:Stock {code: 'a'})")

    shell.user_ns["hawdb_db"] = mine
    with pytest.raises(ValueError, match="'hawdb_db' belongs to the notebook"):
        line("close")
    assert cypher("", "MATCH (s:Stock) RETURN s.code").row_count == 1


def test_cypher_cell_uses_namespace_db_and_params(shell):
    db = hawdb.open()
    shell.user_ns["hawdb_db"] = db
    shell.user_ns["params"] = {"code": "603122"}
    cypher = shell.magics[("cell", "cypher")]
    cypher("", "CREATE (s:Stock {code: $code})")
    result = cypher("", "MATCH (s:Stock) RETURN s.code")
    assert result.fetchall() == [{"s.code": "603122"}]


def test_sql_cell_uses_positional_params(shell):
    db = hawdb.open()
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
    shell.user_ns["hawdb_db"] = db
    sql = shell.magics[("cell", "sql")]
    shell.user_ns["params"] = [1, 7]
    sql("", "INSERT INTO t (id, v) VALUES ($1, $2)")
    shell.user_ns["params"] = [1]
    assert sql("", "SELECT v FROM t WHERE id = $1").fetchall() == [{"v": 7}]


def test_magic_line_rejects_any_text(shell):
    shell.user_ns["hawdb_db"] = hawdb.open()
    for kind, magic in (("cypher", "%%cypher"), ("sql", "%%sql")):
        cell = shell.magics[("cell", kind)]
        for line in ("--oops", "--params p", "x"):
            with pytest.raises(ValueError, match=f"{magic} takes no options"):
                cell(line, "MATCH (s:Stock) RETURN s.code")


def test_params_type_mismatch_names_the_variable(shell):
    shell.user_ns["hawdb_db"] = hawdb.open()
    shell.user_ns["params"] = [1]
    with pytest.raises(TypeError, match="%%cypher expects params to be a dict, got list"):
        shell.magics[("cell", "cypher")]("", "MATCH (s:Stock) RETURN s.code")
    shell.user_ns["params"] = {"a": 1}
    with pytest.raises(TypeError, match="%%sql expects params to be a list, got dict"):
        shell.magics[("cell", "sql")]("", "SELECT 1")
    shell.user_ns["params"] = (1,)  # the binding takes only a list
    with pytest.raises(TypeError, match="%%sql expects params to be a list, got tuple"):
        shell.magics[("cell", "sql")]("", "SELECT 1")


def test_hawdb_db_must_be_a_database(shell):
    shell.user_ns["hawdb_db"] = "./graph"
    with pytest.raises(TypeError, match="'hawdb_db' must be a hawdb.Database, got str"):
        shell.magics[("cell", "cypher")]("", "MATCH (s:Stock) RETURN s.code")


def test_line_magic_rejects_extra_arguments(shell):
    line = shell.magics[("line", "hawdb")]
    shell.user_ns["a"] = hawdb.open()
    shell.user_ns["b"] = hawdb.open()
    for text in ("close now", "use", "use a b"):
        with pytest.raises(ValueError, match="usage: %hawdb"):
            line(text)


def test_line_magic_open_use_close(shell):
    line = shell.magics[("line", "hawdb")]
    cypher = shell.magics[("cell", "cypher")]
    line("open")
    cypher("", "CREATE (s:Stock {code: 'a'})")
    assert cypher("", "MATCH (s:Stock) RETURN s.code").row_count == 1
    line("close")
    with pytest.raises(RuntimeError, match="no HawDB database selected"):
        cypher("", "MATCH (s:Stock) RETURN s.code")
    shell.user_ns["mine"] = hawdb.open()
    line("use mine")
    cypher("", "CREATE (s:Stock {code: 'b'})")
    assert cypher("", "MATCH (s:Stock) RETURN s.code").fetchall() == [{"s.code": "b"}]
    with pytest.raises(ValueError):
        line("use missing")
    with pytest.raises(ValueError):
        line("bogus")


def test_failed_open_keeps_the_current_selection(shell, tmp_path):
    line = shell.magics[("line", "hawdb")]
    cypher = shell.magics[("cell", "cypher")]
    line("open")
    cypher("", "CREATE (s:Stock {code: 'a'})")
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("x")
    with pytest.raises(hawdb.exceptions.Error):
        line(f"open {not_a_dir / 'graph'}")
    assert cypher("", "MATCH (s:Stock) RETURN s.code").row_count == 1


def test_open_strips_one_pair_of_quotes(shell, tmp_path):
    line = shell.magics[("line", "hawdb")]
    cypher = shell.magics[("cell", "cypher")]
    path = tmp_path / "my graph"
    line(f'open "{path}"')
    cypher("", "CREATE (s:Stock {code: 'a'})")
    line("close")
    line(f"open '{path}'")
    assert cypher("", "MATCH (s:Stock) RETURN s.code").row_count == 1
    assert not (tmp_path / '"my graph"').exists()


def test_unquote_leaves_backslashes_and_unmatched_quotes():
    from hawdb import magic

    assert magic._unquote(r'"C:\data\graph"') == r"C:\data\graph"
    assert magic._unquote(r"C:\data\graph") == r"C:\data\graph"
    assert magic._unquote("\"graph'") == "\"graph'"
    assert magic._unquote('"') == '"'


def test_import_without_ipython_does_not_import_it():
    code = (
        "import sys, hawdb, hawdb.magic; "
        "assert 'IPython' not in sys.modules"
    )
    _run_fresh(code)


def test_attribute_access_does_not_load_magics():
    code = (
        "import sys, hawdb; "
        "assert not hasattr(hawdb, 'magic'); "
        "assert 'hawdb.magic' not in sys.modules"
    )
    _run_fresh(code)


# Graph drawing for `%%cypher` (#1022).


def _node(node_id, *labels, **properties):
    return {"_id": node_id, "labels": list(labels), **properties}


def _relationship(rel_id, source, target, rel_type, **properties):
    return {
        "_id": rel_id,
        "source_id": source,
        "target_id": target,
        "type": rel_type,
        **properties,
    }


def _graph_db(db):
    db.execute(
        "CREATE (a:Stock {code: '603122', name: 'Hawk'})"
        "-[:HOLDS {since: 2020}]->(b:Fund {title: 'Alpha'})"
    )


def test_cypher_cell_draws_nodes_and_relationship(shell, db):
    _graph_db(db)
    shell.user_ns["hawdb_db"] = db
    cypher = shell.magics[("cell", "cypher")]
    result = cypher("", "MATCH (a)-[r]->(b) RETURN a, r, b")
    html = result._repr_html_()
    assert html.startswith("<svg")
    assert html.count("<circle") == 2
    assert html.count('marker-end="url(#hawdb-arrow)"') == 1
    assert ">Hawk</text>" in html  # `name` wins over `code`
    assert ">Alpha</text>" in html  # `title` when there is no `name`
    assert ">HOLDS</text>" in html
    assert "not drawn" not in html
    assert html.index("</svg>") < html.index("<table>")  # the table stays below


_GRAPH_QUERY = "MATCH (a)-[r]->(b) RETURN a, r, b"


def test_cypher_display_forwards_to_query_result(shell, db):
    _graph_db(db)
    shell.user_ns["hawdb_db"] = db
    plain = db.execute(_GRAPH_QUERY)

    result = shell.magics[("cell", "cypher")]("", _GRAPH_QUERY)

    assert isinstance(result.result, hawdb.QueryResult)
    assert repr(result) == repr(plain)
    assert len(result) == result.row_count == 1
    assert result.columns == plain.columns
    assert [row["r"]["type"] for row in result] == ["HOLDS"]


def test_query_result_html_stays_the_table(db):
    _graph_db(db)

    html = db.execute(_GRAPH_QUERY)._repr_html_()

    assert html.startswith("<table>")
    assert "<svg" not in html


def test_cypher_display_draws_after_the_cursor_is_exhausted(shell, db):
    _graph_db(db)
    shell.user_ns["hawdb_db"] = db
    result = shell.magics[("cell", "cypher")]("", _GRAPH_QUERY)
    result.fetchall()

    html = result._repr_html_()

    assert "<svg" in html


def test_cypher_display_copies(shell, db):
    shell.user_ns["hawdb_db"] = db
    result = shell.magics[("cell", "cypher")]("", _GRAPH_QUERY)

    duplicate = copy.copy(result)

    assert duplicate.result is result.result


def test_property_only_cypher_cell_is_the_table(shell, db):
    _graph_db(db)
    shell.user_ns["hawdb_db"] = db
    cypher = shell.magics[("cell", "cypher")]
    result = cypher("", "MATCH (a:Stock) RETURN a.code, a.name")
    assert result._repr_html_() == result.result._repr_html_()
    assert "<svg" not in result._repr_html_()


def test_sql_cell_never_draws_a_graph(shell):
    db = hawdb.open()
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, labels BIGINT)")
    db.execute_sql("INSERT INTO t (id, labels) VALUES (1, 2)")
    shell.user_ns["hawdb_db"] = db
    result = shell.magics[("cell", "sql")]("", "SELECT id, labels FROM t")
    assert isinstance(result, hawdb.QueryResult)
    assert "<svg" not in result._repr_html_()


def test_display_rows_ignore_cursor_and_clamp(db):
    with db.transaction() as tx:
        for i in range(3):
            tx.execute("CREATE (s:Stock {n: $n})", {"n": i})
    result = db.execute("MATCH (s:Stock) RETURN s.n ORDER BY s.n")
    result.fetchall()
    assert result._display_rows() == [{"s.n": 0}, {"s.n": 1}, {"s.n": 2}]
    assert result._display_rows(1, 2) == [{"s.n": 1}]
    assert result._display_rows(2, 1) == []
    assert result._display_rows(5) == []
    assert result.fetchone() is None  # the cursor did not move


def test_render_graph_recognizes_only_node_and_relationship_maps():
    rows = [
        {
            "a": {"_id": 1, "labels": "Stock"},  # labels must be a list
            "b": {"_id": True, "labels": []},  # a bool is not an id
            "c": {"_id": 2, "source_id": 1, "target_id": "3", "type": "X"},
            "d": {"_id": 3, "source_id": 1, "target_id": 2},  # no type
            "e": [_node(4, "Stock")],  # only top-level cells count
            "f": 7,
        }
    ]
    assert _display.render_graph(rows) == ""


def test_render_graph_deduplicates_by_id():
    a, b = _node(1, "Stock", code="a"), _node(2, "Stock", code="b")
    rows = [
        {"a": a, "r": _relationship(10, 1, 2, "T"), "b": b},
        {"a": a, "r": _relationship(10, 1, 2, "T"), "b": b},
        {"a": b, "r": _relationship(11, 2, 1, "T"), "b": a},
    ]
    html = _display.render_graph(rows)
    assert html.count("<circle") == 2
    assert html.count("marker-end") == 2


def test_render_graph_draws_missing_endpoints():
    rows = [{"r": _relationship(10, 7, 8, "KNOWS")}]
    html = _display.render_graph(rows)
    assert html.count("<circle") == 2
    assert html.count('stroke-dasharray="4 3"') == 2
    assert ">7</text>" in html and ">8</text>" in html
    assert "_id: 7 (not returned)" in html


def test_render_graph_node_text_falls_back_to_id():
    html = _display.render_graph([{"n": _node(42, "Thing", other="x")}])
    assert ">42</text>" in html
    assert "other: x" in html  # properties in the hover title


def test_render_graph_escapes_text():
    rows = [
        {
            "a": _node(1, "<L>", name="<script>alert(1)</script>"),
            "r": _relationship(2, 1, 1, '"><img src=x>'),
        }
    ]
    html = _display.render_graph(rows)
    assert "<script>" not in html and "<img" not in html
    assert "&lt;script&gt;" in html
    assert "&quot;&gt;&lt;img src=x&gt;" in html
    assert "labels: [&#x27;&lt;L&gt;&#x27;]" in html


def test_render_graph_notes_elements_beyond_the_window():
    rows = [
        {"a": _node(i, "Stock"), "r": _relationship(100 + i, i, i + 1, "NEXT")}
        for i in range(5)
    ]
    html = _display.render_graph(rows, limit=2)
    assert html.count("marker-end") == 2
    assert "3 nodes and 3 relationships not drawn (beyond the first 2 rows)." in html
    rest = [{"a": _node(0, "Stock")}, {"a": _node(9, "Stock")}]
    html = _display.render_graph(rows[:1], rest, limit=1)
    assert "1 node not drawn (beyond the first 1 rows)." in html


def test_render_graph_is_deterministic():
    rows = [
        {"a": _node(i, "Stock"), "r": _relationship(100 + i, i, (i + 1) % 6, "NEXT")}
        for i in range(6)
    ]
    assert _display.render_graph(rows) == _display.render_graph(rows)


def test_cypher_cell_caps_the_drawing(shell, db):
    with db.transaction() as tx:
        for i in range(105):
            tx.execute("CREATE (s:Stock {n: $n})", {"n": i})
    shell.user_ns["hawdb_db"] = db
    html = shell.magics[("cell", "cypher")]("", "MATCH (s:Stock) RETURN s")._repr_html_()
    assert html.count("<circle") == 100
    assert "5 nodes not drawn (beyond the first 100 rows)." in html
    assert "5 more rows not shown (105 total)" in html


def test_render_graph_does_not_import_ipython():
    _run_fresh(
        "import sys; from hawdb import _display; "
        "_display.render_graph([{'n': {'_id': 1, 'labels': []}}]); "
        "assert 'IPython' not in sys.modules"
    )


def test_cypher_display_is_an_iterator(shell, db):
    _graph_db(db)
    shell.user_ns["hawdb_db"] = db
    result = shell.magics[("cell", "cypher")]("", "MATCH (a)-[r]->(b) RETURN a, r, b")
    assert iter(result) is result
    assert next(result)["r"]["type"] == "HOLDS"
    with pytest.raises(StopIteration):
        next(result)


def test_relationship_with_labels_property_stays_an_edge(shell, db):
    db.execute("CREATE (a:A)-[:R {labels: ['tag']}]->(b:B)")
    shell.user_ns["hawdb_db"] = db
    cypher = shell.magics[("cell", "cypher")]
    html = cypher("", "MATCH (a)-[r]->(b) RETURN a, r, b")._repr_html_()
    assert html.count("<circle") == 2
    assert html.count("marker-end") == 1
    assert 'stroke-dasharray="4 3"' not in html  # both endpoints were returned
    relationship = {**_relationship(0, 0, 1, "R"), "labels": ["tag"]}
    assert _display.is_relationship(relationship)
    assert not _display.is_node(relationship)


def test_render_graph_scan_budget_gives_a_lower_bound():
    rows = [{"n": _node(i, "Stock")} for i in range(6)]

    html = _display.render_graph(rows, limit=1, scan_limit=2)

    assert "At least 2 nodes not drawn (beyond the first 1 rows)." in html
    assert "Rows after the first 3 were not checked" in html


def test_render_graph_exact_count_when_the_budget_covers_the_rest():
    rows = [{"n": _node(i, "Stock")} for i in range(6)]

    html = _display.render_graph(rows, limit=1, scan_limit=5)

    assert "<p>5 nodes not drawn (beyond the first 1 rows).</p>" in html
    assert "not checked" not in html


class FakeResult:
    """Rows of node maps, recording each `_display_rows` call."""

    def __init__(self, count):
        self.rows = [{"n": _node(i, "Stock")} for i in range(count)]
        self.columns = ["n"]
        self.calls = []

    def __len__(self):
        return len(self.rows)

    def _display_rows(self, start=0, stop=None):
        self.calls.append((start, stop))
        return self.rows[start:stop]


def test_cypher_display_scans_past_the_window_in_bounded_batches(monkeypatch):
    from hawdb import magic

    monkeypatch.setattr(magic, "_SCAN_BATCH_ROWS", 10)
    monkeypatch.setattr(_display, "GRAPH_SCAN_ROW_LIMIT", 25)
    fake = FakeResult(1000)

    html = magic.CypherResult(fake)._repr_html_()

    assert fake.calls == [(0, 100), (100, 110), (110, 120), (120, 130)]
    assert "At least 25 nodes not drawn" in html
    assert "Rows after the first 125 were not checked" in html


def test_cypher_display_stops_at_the_first_empty_batch(monkeypatch):
    from hawdb import magic

    monkeypatch.setattr(magic, "_SCAN_BATCH_ROWS", 10)
    fake = FakeResult(5)

    magic.CypherResult(fake)._repr_html_()

    assert fake.calls == [(0, 100), (100, 110)]


def test_property_only_display_reads_only_the_window():
    from hawdb import magic

    fake = FakeResult(1000)
    fake.rows = [{"n": i} for i in range(1000)]
    html = magic.CypherResult(fake)._repr_html_()
    assert "<svg" not in html
    assert fake.calls == [(0, 100)]


def test_render_graph_text_has_a_background_halo():
    html = _display.render_graph([{"n": _node(1, "Stock", name="a")}])

    assert ".hawdb-label{" in html
    assert 'class="hawdb-label">a</text>' in html
