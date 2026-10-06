"""run(), format_summary and raise_on_failure: the summary shape, overrides and early validation."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from aidp_connector_hubspot.client import HubSpotClient
from aidp_connector_hubspot.config import ConfigError, parse_config
from aidp_connector_hubspot.runner import format_summary, raise_on_failure, run
from fakes import (
    FakeHubSpot, FakeResponse, FakeSession, FakeSpark, FakeTime, InMemoryState, InMemoryWriter, make_company,
    make_contact, ts,
)

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
KEYS = {"object", "mode", "status", "rows", "watermark", "note", "error"}


def config(objects, **sync):
    return parse_config({"target": {"catalog": "lake", "schema": "crm"}, "sync": {"objects": objects, **sync}})


def go(fake, objects, *, state=None, writer=None, mode_override=None, **sync):
    t = FakeTime()
    client = HubSpotClient("tok", session=fake, sleep=t.sleep, clock=t.clock, requests_per_second=1000)
    writer = writer or InMemoryWriter()
    state = state or InMemoryState()
    summaries = run(FakeSpark(), config(objects, **sync), "tok", mode_override, client=client, writer=writer,
                    state=state, now=lambda: NOW, log=lambda m: None)
    return summaries, writer, state


def test_one_summary_per_object_in_fixed_order_with_every_key():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]})
    summaries, _, _ = go(fake, ["companies", "contacts"])
    assert [s["object"] for s in summaries] == ["contacts", "companies"]
    assert all(set(s) == KEYS for s in summaries)
    first = summaries[0]
    assert first == {"object": "contacts", "mode": "full", "status": "SUCCESS", "rows": 1,
                     "watermark": NOW.isoformat(), "note": "first run", "error": None}


def test_second_run_is_incremental_and_has_no_note():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11))]})
    _, writer, state = go(fake, ["contacts"])
    summaries, _, _ = go(fake, ["contacts"], state=state, writer=writer)
    assert summaries[0]["mode"] == "incremental" and summaries[0]["note"] is None


def test_mode_override_beats_the_config_and_blank_is_ignored():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11))]})
    _, writer, state = go(fake, ["contacts"])
    assert go(fake, ["contacts"], state=state, writer=writer, mode_override=" FULL ")[0][0]["mode"] == "full"
    assert go(fake, ["contacts"], state=state, writer=writer, mode_override="  ")[0][0]["mode"] == "incremental"
    assert go(fake, ["contacts"], state=state, writer=writer, mode_override=None, mode="full")[0][0]["mode"] == "full"
    with pytest.raises(ConfigError, match="mode must be one of"):
        go(fake, ["contacts"], state=state, writer=writer, mode_override="cdc")


def test_associations_summary_says_it_is_always_full():
    fake = FakeHubSpot(objects={"contacts": [], "companies": [], "deals": []})
    summaries, _, _ = go(fake, ["contacts", "companies", "deals", "associations"])
    assoc = summaries[-1]
    assert assoc["object"] == "associations" and assoc["mode"] == "full" and assoc["note"]


def test_a_failing_object_is_reported_and_the_others_still_run():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]})
    writer = InMemoryWriter()
    writer.fail_on = {"contacts"}
    summaries, _, state = go(fake, ["contacts", "companies"], writer=writer)
    failed, ok = summaries
    assert failed["status"] == "FAILED" and failed["rows"] == 0
    assert failed["error"] == "write to contacts failed" and failed["watermark"] is None
    assert ok["status"] == "SUCCESS" and ok["rows"] == 1
    assert state.rows["contacts"]["last_status"] == "FAILED"
    with pytest.raises(RuntimeError, match="HubSpot sync failed for contacts: write to contacts failed"):
        raise_on_failure(summaries)


def test_a_failed_object_reports_its_previous_watermark():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11))]})
    _, writer, state = go(fake, ["contacts"])
    writer.fail_on = {"contacts"}
    summaries, _, _ = go(fake, ["contacts"], state=state, writer=writer)
    assert summaries[0]["status"] == "FAILED" and summaries[0]["watermark"] == NOW.isoformat()


def test_a_failed_associations_run_reports_the_stored_watermark():
    fake = FakeHubSpot(objects={"contacts": [], "companies": [], "deals": []})
    _, writer, state = go(fake, ["contacts", "companies", "deals", "associations"])
    writer.fail_on = {"associations"}
    summaries, _, _ = go(fake, ["associations", "contacts", "deals"], state=state, writer=writer)
    assoc = summaries[-1]
    assert assoc["object"] == "associations" and assoc["status"] == "FAILED"
    assert assoc["watermark"] == NOW.isoformat()


def test_failing_to_record_the_failure_does_not_hide_it():
    class BrokenState(InMemoryState):
        def set_failed(self, *args):
            raise OSError("state table unreachable")

    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]})
    writer = InMemoryWriter()
    writer.fail_on = {"contacts"}
    logs = []
    t = FakeTime()
    client = HubSpotClient("tok", session=fake, sleep=t.sleep, clock=t.clock, requests_per_second=1000)
    summaries = run(FakeSpark(), config(["contacts"]), "tok", client=client, writer=writer, state=BrokenState(),
                    now=lambda: NOW, log=logs.append)
    assert summaries[0]["status"] == "FAILED" and summaries[0]["error"] == "write to contacts failed"
    assert any("could not record FAILED state" in line for line in logs)


def test_an_exception_without_text_still_reports_its_type():
    class Silent(InMemoryState):
        def get_watermark(self, name):
            raise KeyError()

    fake = FakeHubSpot(objects={"contacts": []})
    summaries, _, _ = go(fake, ["contacts"], state=Silent())
    assert summaries[0]["status"] == "FAILED" and summaries[0]["error"] == "KeyError"


def test_format_summary_lines_up_one_row_per_object():
    text = format_summary([
        {"object": "contacts", "mode": "full", "status": "SUCCESS", "rows": 12, "watermark": "2026-09-30T12:00:00+00:00",
         "note": "first run", "error": None},
        {"object": "deals", "mode": "incremental", "status": "FAILED", "rows": 0, "watermark": None, "note": None,
         "error": "boom"},
    ])
    header, contacts, deals = text.splitlines()
    assert header.startswith("object") and "watermark / note / error" in header
    assert "SUCCESS" in contacts and "12" in contacts and "2026-09-30T12:00:00+00:00 | first run" in contacts
    assert "FAILED" in deals and deals.rstrip().endswith("boom")


def test_raise_on_failure_is_silent_on_success_and_names_every_failure():
    ok = {"object": "contacts", "status": "SUCCESS", "error": None}
    raise_on_failure([ok])
    raise_on_failure([])
    bad = [dict(ok, object="deals", status="FAILED", error="a"), dict(ok, object="companies", status="FAILED", error="b")]
    with pytest.raises(RuntimeError) as info:
        raise_on_failure(bad + [ok])
    assert str(info.value) == "HubSpot sync failed for deals: a; companies: b"


@pytest.mark.parametrize("bad", ["", "  ", None, 123])
def test_token_must_be_a_non_blank_string_and_is_checked_before_any_sql(bad):
    spark, fake = FakeSpark(), FakeHubSpot()
    with pytest.raises(ValueError, match="token"):
        run(spark, config(["contacts"]), bad)
    assert spark.statements == [] and fake.calls == []
    with pytest.raises(ValueError):
        HubSpotClient(bad)


@pytest.mark.parametrize("bad", [0, -1, float("nan")])
def test_client_refuses_a_non_positive_rate(bad):
    with pytest.raises(ValueError, match="requests_per_second"):
        HubSpotClient("tok", requests_per_second=bad)


def test_the_token_is_stripped():
    session = FakeSession([FakeResponse(200, {})])
    HubSpotClient("  tok \n", session=session).call("GET", "/x")
    assert session.calls[0]["headers"]["Authorization"] == "Bearer tok"


def test_settings_from_the_config_reach_the_client(monkeypatch):
    seen = {}

    class Spy(HubSpotClient):
        def __init__(self, token, **kwargs):
            seen.update(kwargs)
            super().__init__(token, **kwargs)

    monkeypatch.setattr("aidp_connector_hubspot.runner.HubSpotClient", Spy)
    cfg = parse_config({"hubspot": {"requests_per_second": 2, "api_version": "2026-04"},
                        "target": {"catalog": "lake", "schema": "crm"}, "sync": {"objects": ["contacts"]}})
    spark = FakeSpark()
    spark.fail_on = "CREATE SCHEMA"  # stop right after the client is built
    with pytest.raises(RuntimeError):
        run(spark, cfg, "tok", writer=None, log=lambda m: None)
    assert seen == {"requests_per_second": 2.0, "api_version": "2026-04"}
