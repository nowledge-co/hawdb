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

"""Build-profile coverage: `hawdb.capabilities()` and the lite profile.

The lite-only assertions skip on the default build. Exercise them with:

    maturin develop --no-default-features --features lite
"""

import pytest

import hawdb
from hawdb import exceptions

CAPABILITIES = hawdb.capabilities()


def test_capabilities_reports_compiled_feature_flags():
    assert set(CAPABILITIES) == {
        "access_control",
        "full_text_search",
        "vector_search",
        "graph_analytics",
        "background_maintenance",
    }
    assert all(isinstance(enabled, bool) for enabled in CAPABILITIES.values())
    with pytest.raises(TypeError):
        CAPABILITIES["full_text_search"] = True


@pytest.mark.skipif(
    CAPABILITIES["full_text_search"],
    reason="capability is compiled in on the default build",
)
def test_lite_full_text_statement_raises_capability_error(tmp_path):
    db = hawdb.open(tmp_path / "lite")
    try:
        with pytest.raises(exceptions.CapabilityError):
            db.execute("CREATE FULLTEXT INDEX ON :Memory(title)")
        # A rejected capability leaves the handle usable.
        db.execute("CREATE (:Memory {title: 'still works'})")
    finally:
        db.close()


@pytest.mark.skipif(
    CAPABILITIES["vector_search"],
    reason="capability is compiled in on the default build",
)
def test_lite_vector_statement_raises_capability_error(tmp_path):
    db = hawdb.open(tmp_path / "lite-vector")
    try:
        with pytest.raises(exceptions.CapabilityError):
            db.execute(
                "CALL vector_search($embedding, topK := 1) RETURN id, score",
                {"embedding": [1.0, 0.0]},
            )
    finally:
        db.close()
