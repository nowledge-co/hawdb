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

"""Optional Pydantic v2 models for result rows and statement parameters.

Install the extra with ``pip install 'hawdb[pydantic]'``. ``import hawdb``
does not import this module or Pydantic; it loads on first access to
``hawdb.pydantic`` or with ``import hawdb.pydantic``.

``parse`` validates rows a statement already returned. ``params`` dumps a
model into the parameter dict ``Database.execute`` already takes. Neither
generates Cypher or SQL, and engine errors stay ``hawdb.exceptions``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from typing import Any, TypeVar

_INSTALL = "install it with \"pip install 'hawdb[pydantic]'\""

try:
    import pydantic
except ImportError as error:
    raise ImportError(f"hawdb.pydantic requires Pydantic v2; {_INSTALL}") from error

if int(pydantic.VERSION.partition(".")[0]) < 2:
    raise ImportError(f"hawdb.pydantic requires Pydantic v2, found {pydantic.VERSION}; {_INSTALL}")

__all__ = ["params", "parse"]

Model = TypeVar("Model", bound=pydantic.BaseModel)

# Scalars the binding converts into parameter values. `bool` is an `int`
# subclass, and `tuple` binds as a list. `params` checks them, including the
# engine's 64-bit `int` range, itself, so its errors name the field.
_SCALARS = (type(None), int, float, str, bytes, uuid.UUID)
_INT_MIN, _INT_MAX = -(2**63), 2**63 - 1


def parse(rows: Iterable[dict[str, Any]], model: type[Model]) -> list[Model]:
    """Validate the remaining rows as a list of ``model``.

    ``rows`` is usually a ``QueryResult``; ``parse`` consumes every row not
    fetched yet, even when it raises. Columns bind to fields by name, or
    through a field's ``validation_alias`` for a column such as ``s.code``.
    Unknown columns and missing fields follow the model's config. Invalid rows
    raise one ``pydantic.ValidationError`` whose error locations start with
    the row's index among the parsed rows, and no models are returned.

    Rows hold the values the binding returns: a UUID is a ``str`` and a tuple
    comes back as a list. A strict model needs ``Field(strict=False)`` on
    ``uuid.UUID`` and ``tuple`` fields to read back what ``params`` wrote.
    """
    if not (isinstance(model, type) and issubclass(model, pydantic.BaseModel)):
        raise TypeError(f"parse requires a pydantic.BaseModel subclass, got {model!r}")
    if isinstance(rows, Mapping):
        raise TypeError("parse takes an iterable of rows; wrap a single row as [row]")
    return pydantic.TypeAdapter(list[model]).validate_python(list(rows))


def params(model: pydantic.BaseModel) -> dict[str, Any]:
    """Dump ``model`` into a parameter dict for ``Database.execute``.

    The keys of ``model.model_dump()`` become parameter names. The dump is
    checked against the values the binding accepts (``None``, ``bool``,
    64-bit ``int``, ``float``, ``str``, ``bytes``, ``uuid.UUID``, lists, and
    string-keyed dicts) before any statement runs. An unsupported field such
    as a ``datetime`` or ``Decimal`` raises ``TypeError``, and an ``int``
    outside 64 bits raises ``OverflowError``. A batch is
    ``[params(m) for m in models]``.
    """
    if not isinstance(model, pydantic.BaseModel):
        raise TypeError(f"params requires a pydantic.BaseModel instance, got {model!r}")
    dumped = model.model_dump()
    if not isinstance(dumped, dict):
        raise TypeError(f"{type(model).__name__} does not dump to a parameter dict")
    _check(dumped, type(model).__name__)
    return dumped


def _check(value: Any, path: str) -> None:
    if isinstance(value, int) and not _INT_MIN <= value <= _INT_MAX:
        raise OverflowError(f"{path}: {value} is outside the 64-bit integer range")
    if isinstance(value, _SCALARS):
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path}: parameter maps require string keys, got {key!r}")
            _check(item, f"{path}.{key}")
        return
    raise TypeError(f"{path}: unsupported parameter type {type(value).__name__}")
