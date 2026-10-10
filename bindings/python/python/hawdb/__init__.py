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

"""HawDB Python bindings: embed the database directly from Python."""

from hawdb._hawdb import (
    Database,
    QueryResult,
    ReadTransaction,
    RetainedOptions,
    RetainedCursor,
    RetainedBatch,
    RetainedBuffer,
    Transaction,
    __version__,
    capabilities,
    open,
)
from hawdb._hawdb import exceptions

connect = open


def __getattr__(name):
    # `hawdb.pydantic` needs the optional extra, so it loads on first use.
    if name == "pydantic":
        import importlib

        return importlib.import_module("hawdb.pydantic")
    raise AttributeError(f"module 'hawdb' has no attribute {name!r}")


__all__ = [
    "Database",
    "QueryResult",
    "ReadTransaction",
    "RetainedOptions",
    "RetainedCursor",
    "RetainedBatch",
    "RetainedBuffer",
    "Transaction",
    "capabilities",
    "connect",
    "exceptions",
    "open",
    "__version__",
]
