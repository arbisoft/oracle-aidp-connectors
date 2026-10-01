"""Validation of identifiers, MODE, OBJECTS and the token, before any SQL or request."""

from __future__ import annotations

import pytest

from fakes import FakeHubSpot, FakeSpark
from hubspot_client import HubSpotClient, run_sync, validate_options


def attempt(**kwargs):
    spark, fake = FakeSpark(), FakeHubSpot(objects={"contacts": []})
    args = dict(catalog="lake", schema="crm", session=fake)
    args.update(kwargs)
    try:
        run_sync(spark, "synthetic-token", args.pop("catalog"), args.pop("schema"), **args)
    finally:
        assert spark.statements == [] and fake.calls == []  # nothing ran before the error


@pytest.mark.parametrize("bad", ["<CATALOG>", "a b", "a;b", "a.b", "", "a-b"])
def test_bad_catalog_or_schema_is_refused(bad):
    with pytest.raises(ValueError, match="Replace any placeholder"):
        attempt(catalog=bad)
    with pytest.raises(ValueError, match="Replace any placeholder"):
        attempt(schema=bad)


@pytest.mark.parametrize("bad", ["<PREFIX>", "a-b", "a b"])
def test_bad_prefix_is_refused(bad):
    with pytest.raises(ValueError, match="table_prefix"):
        attempt(table_prefix=bad)


@pytest.mark.parametrize("bad", ["", "Full", "cdc", "FULL", None])
def test_mode_must_be_incremental_or_full(bad):
    with pytest.raises(ValueError, match="MODE must be 'incremental' or 'full'"):
        attempt(mode=bad)


def test_objects_must_be_known():
    with pytest.raises(ValueError, match=r"unknown entries: \['tickets'\]"):
        attempt(objects=["contacts", "tickets"])
    validate_options(["contacts", "companies", "deals", "associations"], "full")
    validate_options([], "incremental")


@pytest.mark.parametrize("bad", [-1, "300", None, True])
def test_overlap_must_be_a_non_negative_number(bad):
    with pytest.raises(ValueError, match="OVERLAP_SECONDS"):
        attempt(overlap_seconds=bad)


@pytest.mark.parametrize("bad", [0, -1, float("nan")])
def test_requests_per_second_must_be_positive(bad):
    with pytest.raises(ValueError, match="requests_per_second"):
        attempt(requests_per_second=bad)


@pytest.mark.parametrize("bad", ["", "  ", None, 123])
def test_token_must_be_a_non_blank_string(bad):
    spark = FakeSpark()
    with pytest.raises(ValueError, match="token"):
        run_sync(spark, bad, "lake", "crm", session=FakeHubSpot())
    assert spark.statements == []
    with pytest.raises(ValueError):
        HubSpotClient(bad)


def test_the_token_is_stripped():
    from fakes import FakeResponse, FakeSession

    session = FakeSession([FakeResponse(200, {})])
    HubSpotClient("  tok \n", session=session).call("GET", "/x")
    assert session.calls[0]["headers"]["Authorization"] == "Bearer tok"
