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

"""The hand-written ``_hawdb.pyi`` stubs and the runtime signatures must not
drift. PyO3 publishes ``__text_signature__``, so ``inspect.signature`` reports
the real callable parameters; comparing both sides fails a binding change that
lands without the matching stub update.
"""

import ast
import inspect
from pathlib import Path

import pytest

import hawdb

STUB_PATH = Path(hawdb.__file__).with_name("_hawdb.pyi")

SIGNATURES = [
    ("open", hawdb.open),
    ("Database.__init__", hawdb.Database),
    ("Database.execute", hawdb.Database.execute),
    ("Database.execute_sql", hawdb.Database.execute_sql),
    ("Database.transaction", hawdb.Database.transaction),
    ("Database.read_transaction", hawdb.Database.read_transaction),
    ("Database.close", hawdb.Database.close),
    ("Transaction.execute", hawdb.Transaction.execute),
    ("Transaction.execute_sql", hawdb.Transaction.execute_sql),
    ("Transaction.commit", hawdb.Transaction.commit),
    ("Transaction.rollback", hawdb.Transaction.rollback),
    ("ReadTransaction.execute", hawdb.ReadTransaction.execute),
    ("ReadTransaction.execute_sql", hawdb.ReadTransaction.execute_sql),
    ("ReadTransaction.close", hawdb.ReadTransaction.close),
    ("QueryResult.fetchone", hawdb.QueryResult.fetchone),
    ("QueryResult.fetchmany", hawdb.QueryResult.fetchmany),
    ("QueryResult.fetchall", hawdb.QueryResult.fetchall),
]


def _stub_params(qualname):
    """Return the stub's parameter names and defaulted-parameter count."""
    cls_name, _, func_name = qualname.rpartition(".")
    tree = ast.parse(STUB_PATH.read_text())
    scope = tree.body
    if cls_name:
        cls = next(
            node
            for node in scope
            if isinstance(node, ast.ClassDef) and node.name == cls_name
        )
        scope = cls.body
    func = next(
        node
        for node in scope
        if isinstance(node, ast.FunctionDef) and node.name == func_name
    )
    args = func.args
    names = [
        arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)
    ]
    defaulted = len(args.defaults) + sum(
        default is not None for default in args.kw_defaults
    )
    return names, defaulted


@pytest.mark.parametrize("qualname,obj", SIGNATURES, ids=[name for name, _ in SIGNATURES])
def test_stub_signature_matches_runtime(qualname, obj):
    stub_names, stub_defaulted = _stub_params(qualname)
    params = list(inspect.signature(obj).parameters.values())
    runtime_names = [param.name for param in params]
    runtime_defaulted = sum(
        param.default is not inspect.Parameter.empty for param in params
    )

    # Stub methods spell `self`; the runtime call signature does not.
    stub_names = [name for name in stub_names if name != "self"]
    runtime_names = [name for name in runtime_names if name != "self"]

    assert runtime_names == stub_names
    assert runtime_defaulted == stub_defaulted
