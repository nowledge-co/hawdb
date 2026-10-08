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

"""Shared fixtures for hawdb binding tests."""

import pytest

import hawdb


@pytest.fixture(params=["file", "memory"])
def db(request, tmp_path):
    """An open database on each backend.

    `file` opens a durable project under `tmp_path`; `memory` uses
    `hawdb.open()` with no path. Statement-level tests should take this
    fixture so both backends prove the same behavior. Tests that exercise
    durable-path semantics take `tmp_path` directly instead.
    """
    if request.param == "memory":
        handle = hawdb.open()
    else:
        handle = hawdb.open(tmp_path / "db")
    try:
        yield handle
    finally:
        handle.close()


@pytest.fixture(params=["file", "memory"])
def open_db(request, tmp_path):
    """The `hawdb.open` call itself, for tests that open their own handle."""
    if request.param == "memory":
        return hawdb.open
    return lambda: hawdb.open(tmp_path / "db")
