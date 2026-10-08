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

"""Coverage for the optional `hawdb[pydantic]` extra."""

import datetime
import decimal
import enum
import uuid

import pytest

pytest.importorskip("pydantic", minversion="2")

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationError

import hawdb
import hawdb.pydantic
from hawdb import exceptions


class Stock(BaseModel):
    code: str
    price: float


class AliasedStock(BaseModel):
    code: str = Field(validation_alias="s.code")
    price: float = Field(validation_alias="s.price")


class Code(BaseModel):
    code: str


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str


class Address(BaseModel):
    city: str


class Record(BaseModel):
    ident: uuid.UUID
    payload: bytes
    flags: list[bool]
    tags: tuple[str, ...]
    address: Address
    note: str | None = None


class Color(enum.Enum):
    RED = "red"


class Listing(BaseModel):
    code: str
    value: object


@pytest.fixture
def db(tmp_path):
    with hawdb.open(tmp_path / "graph") as handle:
        handle.execute(
            "UNWIND $rows AS row CREATE (:Stock {code: row.code, price: row.price})",
            {"rows": [{"code": "603122", "price": 12.5}, {"code": "000001", "price": 9.0}]},
        )
        yield handle


def test_parse_scalar_rows(db):
    result = db.execute("MATCH (s:Stock) RETURN s.code AS code, s.price AS price ORDER BY code")
    assert hawdb.pydantic.parse(result, Stock) == [
        Stock(code="000001", price=9.0),
        Stock(code="603122", price=12.5),
    ]
    assert result.fetchone() is None


def test_parse_consumes_only_remaining_rows(db):
    result = db.execute("MATCH (s:Stock) RETURN s.code AS code, s.price AS price ORDER BY code")
    assert result.fetchone() == {"code": "000001", "price": 9.0}
    assert hawdb.pydantic.parse(result, Stock) == [Stock(code="603122", price=12.5)]


def test_parse_binds_property_columns_through_aliases(db):
    result = db.execute("MATCH (s:Stock) RETURN s.code, s.price ORDER BY s.code")
    assert result.columns == ["s.code", "s.price"]
    assert hawdb.pydantic.parse(result, AliasedStock) == [
        AliasedStock(**{"s.code": "000001", "s.price": 9.0}),
        AliasedStock(**{"s.code": "603122", "s.price": 12.5}),
    ]


def test_parse_follows_model_config_for_unknown_and_missing_columns(db):
    query = "MATCH (s:Stock) RETURN s.code AS code, s.price AS price ORDER BY code"
    assert hawdb.pydantic.parse(db.execute(query), Code) == [
        Code(code="000001"),
        Code(code="603122"),
    ]
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        hawdb.pydantic.parse(db.execute(query), Strict)
    with pytest.raises(ValidationError, match="price"):
        hawdb.pydantic.parse(db.execute("MATCH (s:Stock) RETURN s.code AS code"), Stock)


def test_parse_rejects_an_invalid_row_by_index(db):
    db.execute("CREATE (:Stock {code: $code, price: $price})", {"code": "bad", "price": "n/a"})
    result = db.execute("MATCH (s:Stock) RETURN s.code AS code, s.price AS price ORDER BY code")
    with pytest.raises(ValidationError) as raised:
        hawdb.pydantic.parse(result, Stock)
    assert [error["loc"] for error in raised.value.errors()] == [(2, "price")]
    assert result.fetchone() is None


def test_strict_models_read_uuids_through_lax_fields(tmp_path):
    class StrictIdent(BaseModel):
        model_config = ConfigDict(strict=True)

        ident: uuid.UUID

    class LaxIdent(BaseModel):
        model_config = ConfigDict(strict=True)

        ident: uuid.UUID = Field(strict=False)

    ident = uuid.uuid4()
    query = "MATCH (r:Record) RETURN r.ident AS ident"
    with hawdb.open(tmp_path / "strict") as db:
        db.execute(
            "CREATE (:Record {ident: $ident})",
            hawdb.pydantic.params(StrictIdent(ident=ident)),
        )
        assert db.execute(query).fetchall() == [{"ident": str(ident)}]
        with pytest.raises(ValidationError, match="UUID"):
            hawdb.pydantic.parse(db.execute(query), StrictIdent)
        assert hawdb.pydantic.parse(db.execute(query), LaxIdent) == [LaxIdent(ident=ident)]


def test_parse_requires_a_model_class(db):
    with pytest.raises(TypeError, match="BaseModel subclass"):
        hawdb.pydantic.parse(db.execute("MATCH (s:Stock) RETURN s.code AS code"), dict)


def test_parse_rejects_a_single_row(db):
    row = db.execute("MATCH (s:Stock) RETURN s.code AS code").fetchone()
    with pytest.raises(TypeError, match=r"wrap a single row as \[row\]"):
        hawdb.pydantic.parse(row, Code)
    assert hawdb.pydantic.parse([row], Code) == [Code(**row)]


def test_params_round_trip_through_execute(tmp_path):
    record = Record(
        ident=uuid.uuid4(),
        payload=b"\x00\xff",
        flags=[True, False],
        tags=("a", "b"),
        address=Address(city="Hangzhou"),
    )
    with hawdb.open(tmp_path / "records") as db:
        db.execute(
            "CREATE (:Record {ident: $ident, payload: $payload, flags: $flags, "
            "tags: $tags, address: $address, note: $note})",
            hawdb.pydantic.params(record),
        )
        result = db.execute(
            "MATCH (r:Record) RETURN r.ident AS ident, r.payload AS payload, "
            "r.flags AS flags, r.tags AS tags, r.address AS address, r.note AS note"
        )
        assert hawdb.pydantic.parse(result, Record) == [record]


def test_params_batch_loads_in_one_statement(tmp_path):
    stocks = [Stock(code="603122", price=12.5), Stock(code="000001", price=9.0)]
    with hawdb.open(tmp_path / "batch") as db:
        db.execute(
            "UNWIND $rows AS row CREATE (:Stock {code: row.code, price: row.price})",
            {"rows": [hawdb.pydantic.params(stock) for stock in stocks]},
        )
        result = db.execute(
            "MATCH (s:Stock) RETURN s.code AS code, s.price AS price ORDER BY code"
        )
        assert hawdb.pydantic.parse(result, Stock) == sorted(stocks, key=lambda s: s.code)


# The binding would store a Decimal as a float, so `params` is what rejects it.
@pytest.mark.parametrize(
    ("value", "kind"),
    [
        (datetime.date(2026, 1, 2), "date"),
        (decimal.Decimal("1.10"), "Decimal"),
        (Color.RED, "Color"),
        ([1, {"at": datetime.date(2026, 1, 2)}], "date"),
    ],
)
def test_params_rejects_unsupported_values(value, kind):
    with pytest.raises(TypeError, match=rf"Listing\.value.*: unsupported parameter type {kind}"):
        hawdb.pydantic.params(Listing(code="603122", value=value))


# The binding would store these as floats, so `params` is what rejects them.
@pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
def test_params_rejects_ints_outside_64_bits(value):
    with pytest.raises(OverflowError, match=r"Listing\.value: .* outside the 64-bit integer"):
        hawdb.pydantic.params(Listing(code="603122", value=value))


def test_params_accepts_64_bit_int_bounds(tmp_path):
    bounds = [2**63 - 1, -(2**63)]
    with hawdb.open(tmp_path / "bounds") as db:
        db.execute(
            "CREATE (:Listing {code: $code, value: $value})",
            hawdb.pydantic.params(Listing(code="x", value=bounds)),
        )
        result = db.execute("MATCH (l:Listing) RETURN l.value AS value")
        assert result.fetchall() == [{"value": bounds}]


def test_params_rejects_non_string_map_keys():
    class Scores(BaseModel):
        by_year: dict[int, float]

    class Root(RootModel[dict[int, float]]):
        pass

    with pytest.raises(TypeError, match=r"Scores\.by_year: parameter maps require string keys"):
        hawdb.pydantic.params(Scores(by_year={2026: 1.0}))
    with pytest.raises(TypeError, match=r"Root: parameter maps require string keys"):
        hawdb.pydantic.params(Root({2026: 1.0}))


def test_params_requires_a_model_instance():
    with pytest.raises(TypeError, match="BaseModel instance"):
        hawdb.pydantic.params({"code": "603122"})


def test_engine_errors_stay_hawdb_exceptions(db):
    with pytest.raises(exceptions.ParseError):
        db.execute("THIS IS NOT CYPHER", hawdb.pydantic.params(Stock(code="x", price=1.0)))
