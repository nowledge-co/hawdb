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

"""The pydantic extra stays optional, with or without Pydantic installed."""

import os
import subprocess
import sys

import pytest


def _run_fresh(code):
    # A fresh interpreter, because other tests in this process may have
    # imported the extra already.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


def _pydantic_v2_installed():
    try:
        import pydantic
    except ImportError:
        return False
    return int(pydantic.VERSION.partition(".")[0]) >= 2


def test_import_hawdb_does_not_load_pydantic():
    _run_fresh(
        "import sys, hawdb\n"
        "assert 'pydantic' not in sys.modules\n"
        "assert 'hawdb.pydantic' not in sys.modules\n"
    )


def test_attribute_access_loads_the_extra():
    if not _pydantic_v2_installed():
        pytest.skip("pydantic v2 is not installed")
    _run_fresh(
        "import sys, hawdb\n"
        "assert callable(hawdb.pydantic.parse)\n"
        "assert 'pydantic' in sys.modules\n"
    )


def test_missing_pydantic_names_the_extra():
    if _pydantic_v2_installed():
        pytest.skip("pydantic v2 is installed")
    import hawdb

    with pytest.raises(ImportError, match=r"hawdb\[pydantic\]"):
        import hawdb.pydantic  # noqa: F401
    with pytest.raises(ImportError, match=r"hawdb\[pydantic\]"):
        hawdb.pydantic
    with pytest.raises(AttributeError):
        hawdb.not_a_module
