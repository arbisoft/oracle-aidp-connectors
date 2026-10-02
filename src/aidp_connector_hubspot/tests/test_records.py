"""Row mapping and column specs, using synthetic records only."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from aidp_connector_hubspot import records
from aidp_connector_hubspot.records import COLUMNS, association_row, merged_ids, object_row
from aidp_connector_hubspot.records import to_time as parse_time

CONTACT_COLUMNS, COMPANY_COLUMNS, DEAL_COLUMNS = COLUMNS["contacts"], COLUMNS["companies"], COLUMNS["deals"]
ASSOCIATION_COLUMNS = COLUMNS["associations"]

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
_COMMON = [
    ("id", "STRING"),
    ("created_at", "TIMESTAMP"),
    ("updated_at", "TIMESTAMP"),
    ("archived", "BOOLEAN"),
    ("archived_at", "TIMESTAMP"),
]
_TAIL = [("raw_json", "STRING"), ("_ingested_at", "TIMESTAMP")]


def test_columns_are_pinned():
    assert list(CONTACT_COLUMNS) == _COMMON + [
        ("email", "STRING"),
        ("first_name", "STRING"),
        ("last_name", "STRING"),
        ("phone", "STRING"),
        ("lifecycle_stage", "STRING"),
    ] + _TAIL
    assert list(COMPANY_COLUMNS) == _COMMON + [
        ("name", "STRING"),
        ("domain", "STRING"),
        ("industry", "STRING"),
    ] + _TAIL
    assert list(DEAL_COLUMNS) == _COMMON + [
        ("deal_name", "STRING"),
        ("amount", "DECIMAL(38,6)"),
        ("currency", "STRING"),
        ("deal_stage", "STRING"),
        ("pipeline", "STRING"),
        ("close_date", "TIMESTAMP"),
        ("owner_id", "STRING"),
    ] + _TAIL
    assert list(ASSOCIATION_COLUMNS) == [
        ("from_object", "STRING"),
        ("from_id", "STRING"),
        ("to_object", "STRING"),
        ("to_id", "STRING"),
        ("association_type_id", "INT"),
        ("category", "STRING"),
        ("label", "STRING"),
        ("_ingested_at", "TIMESTAMP"),
    ]
    assert COLUMNS == {
        "contacts": CONTACT_COLUMNS,
        "companies": COMPANY_COLUMNS,
        "deals": DEAL_COLUMNS,
        "associations": ASSOCIATION_COLUMNS,
    }


def _obj(props=None, **extra):
    base = {
        "id": "101",
        "createdAt": "2026-01-02T03:04:05.123Z",
        "updatedAt": "2026-02-03T04:05:06.789Z",
        "archived": False,
        "properties": props or {},
    }
    base.update(extra)
    return base


def test_contact_row_maps_every_column():
    obj = _obj(
        {
            "email": "a@example.test",
            "firstname": "Ada",
            "lastname": "Test",
            "phone": "555-0100",
            "lifecyclestage": "lead",
        }
    )
    row = object_row("contacts", obj, NOW)
    assert set(row) == {n for n, _ in CONTACT_COLUMNS}
    assert row["id"] == "101"
    assert row["created_at"] == datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
    assert row["updated_at"] == datetime(2026, 2, 3, 4, 5, 6, 789000, tzinfo=timezone.utc)
    assert row["archived"] is False and row["archived_at"] is None
    assert (row["email"], row["first_name"], row["last_name"]) == ("a@example.test", "Ada", "Test")
    assert (row["phone"], row["lifecycle_stage"]) == ("555-0100", "lead")
    assert row["_ingested_at"] is NOW


def test_company_row_maps_every_column():
    row = object_row("companies", _obj({"name": "Acme", "domain": "acme.test", "industry": "MINING"}), NOW)
    assert set(row) == {n for n, _ in COMPANY_COLUMNS}
    assert (row["name"], row["domain"], row["industry"]) == ("Acme", "acme.test", "MINING")


def test_deal_row_maps_every_column():
    obj = _obj(
        {
            "dealname": "Big deal",
            "amount": "1234.5",
            "deal_currency_code": "EUR",
            "dealstage": "closedwon",
            "pipeline": "default",
            "closedate": "2026-03-01T00:00:00Z",
            "hubspot_owner_id": "77",
        }
    )
    row = object_row("deals", obj, NOW)
    assert set(row) == {n for n, _ in DEAL_COLUMNS}
    assert row["deal_name"] == "Big deal"
    assert row["amount"] == Decimal("1234.500000")
    assert (row["currency"], row["deal_stage"], row["pipeline"]) == ("EUR", "closedwon", "default")
    assert row["close_date"] == datetime(2026, 3, 1, tzinfo=timezone.utc)
    assert row["owner_id"] == "77"


def test_association_row_maps_every_column():
    kind = {"category": "HUBSPOT_DEFINED", "typeId": 1, "label": "Primary"}
    row = association_row("contacts", "1", "companies", "2", kind, NOW)
    assert set(row) == {n for n, _ in ASSOCIATION_COLUMNS}
    assert row == {
        "from_object": "contacts",
        "from_id": "1",
        "to_object": "companies",
        "to_id": "2",
        "association_type_id": 1,
        "category": "HUBSPOT_DEFINED",
        "label": "Primary",
        "_ingested_at": NOW,
    }


def test_association_type_id_edge_cases():
    def tid(value):
        return association_row("deals", "1", "contacts", "2", {"typeId": value}, NOW)["association_type_id"]

    assert tid("3") == 3
    assert tid(3.0) == 3
    assert tid(3.5) == 3  # int() truncates, as in the live-validated notebook
    assert tid("x") is None
    assert tid(None) is None
    assert association_row("deals", "1", "contacts", "2", {}, NOW)["label"] is None


def test_archived_without_archived_at_and_only_true_counts():
    row = object_row("contacts", _obj(archived=True), NOW)
    assert row["archived"] is True and row["archived_at"] is None
    row = object_row("contacts", _obj(archived=True, archivedAt="2026-04-01T00:00:00Z"), NOW)
    assert row["archived_at"] == datetime(2026, 4, 1, tzinfo=timezone.utc)
    # archivedAt is ignored unless the record is archived; "true" strings do not count.
    row = object_row("contacts", _obj(archived="true", archivedAt="2026-04-01T00:00:00Z"), NOW)
    assert row["archived"] is False and row["archived_at"] is None


def test_deal_closedate_date_only_and_epoch_ms():
    date_only = object_row("deals", _obj({"closedate": "2026-03-01"}), NOW)
    assert date_only["close_date"] == datetime(2026, 3, 1, tzinfo=timezone.utc)
    epoch = object_row("deals", _obj({"closedate": "1772323200000"}), NOW)
    assert epoch["close_date"] == datetime(2026, 3, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1234.5", "1234.500000"),
        ("  42  ", "42.000000"),
        ("1e3", "1000.000000"),
        ("1.5E+2", "150.000000"),
        ("1.1234567", "1.123457"),
        ("-0.0000004", "0.000000"),
        ("9" * 32, "9" * 32 + ".000000"),
    ],
)
def test_amount_parsed_and_rounded(raw, expected):
    assert object_row("deals", _obj({"amount": raw}), NOW)["amount"] == Decimal(expected)


@pytest.mark.parametrize(
    "raw",
    ["1e40", "1" * 33, "9" * 32 + ".9999999", "NaN", "Infinity", "-inf", "abc", "", "  ", None, True],
)
def test_amount_bad_values_become_null(raw):
    assert object_row("deals", _obj({"amount": raw}), NOW)["amount"] is None


def test_parse_time_variants():
    utc = timezone.utc
    assert parse_time("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=utc)
    assert parse_time("2026-01-02T03:04:05.12Z") == datetime(2026, 1, 2, 3, 4, 5, 120000, tzinfo=utc)
    assert parse_time("2026-01-02T03:04:05.1234567Z") == datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=utc)
    assert parse_time("2026-01-02T03:04:05") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=utc)
    assert parse_time("2026-01-02T05:04:05.5+02:00") == datetime(2026, 1, 2, 3, 4, 5, 500000, tzinfo=utc)
    assert parse_time(1772323200000) == datetime(2026, 3, 1, tzinfo=utc)
    assert parse_time(" 1772323200000 ") == datetime(2026, 3, 1, tzinfo=utc)
    assert parse_time("-86400000") == datetime(1969, 12, 31, tzinfo=utc)
    assert parse_time(-1000) == datetime(1969, 12, 31, 23, 59, 59, tzinfo=utc)


@pytest.mark.parametrize("bad", [None, "", "  ", "not a date", "2026-13-45", True, [], {}, "9" * 30])
def test_parse_time_bad_values_are_none(bad):
    assert parse_time(bad) is None


def test_empty_properties_give_nulls():
    for name, cols in (("contacts", CONTACT_COLUMNS), ("companies", COMPANY_COLUMNS), ("deals", DEAL_COLUMNS)):
        for obj in ({"id": "5"}, {"id": "5", "properties": {}}, {"id": "5", "properties": None}):
            row = object_row(name, obj, NOW)
            assert set(row) == {n for n, _ in cols}
            assert row["id"] == "5" and row["archived"] is False
            data = [n for n, _ in cols if n not in ("id", "archived", "raw_json", "_ingested_at")]
            assert all(row[n] is None for n in data)


def test_raw_json_compact_sorted_unicode():
    obj = _obj({"firstname": "Zoë", "email": "z@example.test"})
    row = object_row("contacts", obj, NOW)
    assert row["raw_json"] == json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert "Zoë" in row["raw_json"] and "\\u" not in row["raw_json"]
    assert ", " not in row["raw_json"] and ": " not in row["raw_json"]
    assert json.loads(row["raw_json"]) == obj
    assert row["first_name"] == "Zoë"


def test_merged_ids():
    def ids(value):
        return merged_ids({"properties": {"hs_merged_object_ids": value}})

    assert ids("1;2; 3 ;;") == ["1", "2", "3"]
    assert ids("1;2;1;2") == ["1", "2"]
    assert ids(12345) == ["12345"]
    assert ids(None) == [] and ids("") == [] and ids(" ; ") == []
    assert merged_ids({}) == [] and merged_ids({"properties": None}) == []
    assert merged_ids({"properties": {}}) == []


def test_unknown_object_name_is_rejected():
    with pytest.raises(KeyError):
        object_row("tickets", _obj(), NOW)
    assert records.COLUMNS.keys() == {"contacts", "companies", "deals", "associations"}
