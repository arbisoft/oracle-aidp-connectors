"""run() against FakeHubSpot, an in-memory writer and state; no network, no Spark."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from aidp_connector_hubspot.client import HubSpotClient
from aidp_connector_hubspot.config import parse_config
from aidp_connector_hubspot.runner import raise_on_failure, run
from fakes import (
    FakeHubSpot, FakeSpark, FakeTime, InMemoryState, InMemoryWriter, assoc_link, error_response, make_company,
    make_contact, make_deal, ts,
)
from fakes import API_VERSION

TOKEN = "pat-na1-synthetic-token-1234"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
ALL = ["contacts", "companies", "deals", "associations"]


def at(minute, hour=12):
    return datetime(2026, 9, 30, hour, minute, tzinfo=timezone.utc)


class Harness:
    def __init__(self, fake):
        self.fake = fake
        self.writer = InMemoryWriter()
        self.state = InMemoryState()
        self.logs = []
        self.error = None

    def run(self, objects, mode="incremental", now=NOW, **sync):
        """Run once. Returns {object: rows} for the objects that succeeded, or None (and sets ``error``)
        when any object failed, as raise_on_failure does."""
        self.fake.calls.clear()
        self.writer.operations.clear()
        fake_time = FakeTime()
        self.error = None
        config = parse_config({"target": {"catalog": "lake", "schema": "crm"},
                               "sync": {"objects": list(objects), "mode": mode, **sync}})
        client = HubSpotClient(TOKEN, session=self.fake, sleep=fake_time.sleep, clock=fake_time.clock,
                               requests_per_second=1000)
        self.summaries = run(FakeSpark(), config, TOKEN, client=client, writer=self.writer, state=self.state,
                             now=(lambda: now) if now else None, log=self.logs.append)
        try:
            raise_on_failure(self.summaries)
        except RuntimeError as exc:
            self.error = str(exc)
            return None
        return {item["object"]: item["rows"] for item in self.summaries}

    def row(self, table, row_id):
        return next(row for row in self.writer.tables[table] if row["id"] == row_id)


def test_first_run_loads_everything_live_and_archived_and_sets_watermarks():
    fake = FakeHubSpot(
        objects={
            "contacts": [make_contact(1, ts(5)), make_contact(2, ts(1), archived=True, archived_at=ts(9))],
            "companies": [make_company(10, ts(3))],
            "deals": [make_deal(20, ts(4))],
        },
        associations={("contacts", "companies"): {"1": [assoc_link(10)]}},
    )
    h = Harness(fake)
    counts = h.run(["associations", "deals", "contacts", "companies"])

    assert list(counts) == ALL  # fixed run order
    assert counts == {"contacts": 2, "companies": 1, "deals": 1, "associations": 1}
    assert h.writer.ids("contacts") == ["1", "2"]
    assert h.row("contacts", "2")["archived"] is True
    assert h.writer.operations[0] == ("overwrite", "contacts", 2)  # no watermark yet: full
    assert h.row("contacts", "1")["_ingested_at"] == NOW
    assert all(h.state.rows[o]["last_mode"] == "full" for o in ALL)
    assert all(h.state.rows[o]["last_status"] == "SUCCESS" and h.state.rows[o]["watermark"] == NOW for o in ALL)


def test_second_run_picks_up_an_edit_and_rewrites_only_recent_rows():
    fake = FakeHubSpot(objects={"contacts": [
        make_contact(1, ts(0, hour=9)),
        make_contact(2, ts(57, hour=11)),  # inside the overlap of the next run
        make_contact(3, ts(0, hour=9)),
    ]})
    h = Harness(fake)
    h.run(["contacts"])
    fake.objects["contacts"][2] = make_contact(3, ts(7, hour=12), lastname="Renamed")
    counts = h.run(["contacts"], now=at(10))

    # since = 12:00 - 300s = 11:55: contact 3 (edited) and 2 (overlap) are re-read; contact 1 is not.
    assert counts == {"contacts": 2}
    assert h.writer.operations[0] == ("merge", "contacts", 2)
    assert h.state.rows["contacts"]["last_mode"] == "incremental"
    assert h.row("contacts", "3")["last_name"] == "Renamed"
    assert h.writer.ids("contacts") == ["1", "2", "3"]
    assert h.row("contacts", "1")["_ingested_at"] == NOW  # untouched
    assert h.row("contacts", "2")["_ingested_at"] == at(10)


def test_overlap_window_follows_the_setting():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(50, hour=11))]})
    h = Harness(fake)
    h.state.set_watermark("contacts", "incremental", 1, NOW)
    for overlap, expected in ((300, 0), (900, 1)):
        h.writer.tables.clear()
        assert h.run(["contacts"], now=at(1), overlap_seconds=overlap) == {"contacts": expected}
        # 11:50 is outside 11:55 but inside 11:45


def test_no_changes_keeps_rows_and_advances_watermark_to_run_start():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(0, hour=9))]})
    h = Harness(fake)
    h.run(["contacts"])
    assert h.run(["contacts"], now=at(30)) == {"contacts": 0}
    assert h.writer.ids("contacts") == ["1"]
    assert h.state.get_watermark("contacts") == at(30)


def test_watermark_is_the_run_start_not_the_newest_record():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11))]})
    h = Harness(fake)
    h.run(["contacts"])
    assert h.state.get_watermark("contacts") == NOW
    assert any("watermark 2026-09-30T12:00:00+00:00" in line for line in h.logs)
    h.run(["contacts"], now=at(20))  # the incremental search is bounded at the run start too
    filters = h.fake.calls_to("/search")[0]["filterGroups"][0]["filters"]
    assert next(f for f in filters if f["operator"] == "LT")["value"] == str(int(at(20).timestamp() * 1000))


def test_run_at_is_set_per_run_not_per_import():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11))]})
    h = Harness(fake)
    h.run(["contacts"], now=at(0))
    h.run(["contacts"], mode="full", now=at(40))
    assert h.row("contacts", "1")["_ingested_at"] == at(40)
    assert h.state.rows["contacts"]["last_run_at"] == at(40)


def test_default_run_start_is_the_current_utc_time():
    h = Harness(FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]}))
    before = datetime.now(timezone.utc)
    h.run(["contacts"], now=None)
    assert before <= h.state.get_watermark("contacts") <= datetime.now(timezone.utc)


def test_full_mode_overwrites_and_loads_archived():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5)), make_contact(2, ts(6)), make_contact(3, ts(7))]})
    h = Harness(fake)
    h.run(["contacts"])
    del fake.objects["contacts"][0]  # hard-deleted upstream
    fake.objects["contacts"][0] = make_contact(2, ts(6), archived=True, archived_at=ts(30))

    assert h.run(["contacts"], mode="full", now=at(10)) == {"contacts": 2}

    assert h.writer.operations == [("overwrite", "contacts", 2)]
    assert h.writer.order_by["contacts"] == "updated_at"
    assert h.writer.ids("contacts") == ["2", "3"]
    assert h.row("contacts", "2")["archived"] is True
    assert h.row("contacts", "2")["archived_at"] == datetime(2026, 9, 30, 10, 30, tzinfo=timezone.utc)
    assert h.state.get_watermark("contacts") == at(10)
    assert h.state.rows["contacts"]["last_mode"] == "full"


def test_incremental_flags_archived_and_merged_ids():
    fake = FakeHubSpot(objects={"contacts": [make_contact(i, ts(0, hour=9)) for i in (1, 2, 3)]})
    h = Harness(fake)
    h.run(["contacts"])
    fake.objects["contacts"][0] = make_contact(1, ts(0, hour=9), archived=True, archived_at=ts(3, hour=11))
    # Contacts 2 and 3 were merged into 4: the survivor lists them, and they vanish without being archived.
    del fake.objects["contacts"][1:]
    fake.objects["contacts"].append(make_contact(4, ts(5, hour=12), hs_merged_object_ids="2;3"))

    h.run(["contacts"], now=at(10))

    assert [op[0] for op in h.writer.operations] == ["merge", "mark_archived"]
    assert h.writer.ids("contacts") == ["1", "2", "3", "4"]
    assert [h.row("contacts", i)["archived"] for i in "1234"] == [True, True, True, False]
    assert h.row("contacts", "1")["archived_at"] == datetime(2026, 9, 30, 11, 3, tzinfo=timezone.utc)
    assert h.row("contacts", "2")["archived_at"] == at(10)  # merge time = run start
    assert h.row("contacts", "2")["last_name"] == "User2"  # no other column touched


def test_incremental_with_zero_changes_still_flags_archived():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(0, hour=9))]})
    h = Harness(fake)
    h.run(["contacts"])
    fake.objects["contacts"][0] = make_contact(1, ts(0, hour=9), archived=True, archived_at=ts(3, hour=11))
    assert h.run(["contacts"], now=at(10)) == {"contacts": 0}
    assert h.row("contacts", "1")["archived"] is True


def test_incremental_on_an_empty_table_with_a_watermark_is_fine():
    h = Harness(FakeHubSpot(objects={"contacts": []}))
    h.state.set_watermark("contacts", "incremental", 0, NOW)
    assert h.run(["contacts"], now=at(10)) == {"contacts": 0}


def test_every_property_name_is_requested():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]}, properties={"contacts": ["email", "custom_x"]})
    Harness(fake).run(["contacts"], mode="full")
    listing = next(p for m, path, b, p in fake.calls if path == f"/crm/objects/{API_VERSION}/contacts" and p and "properties" in p)
    assert listing["properties"] == "email,custom_x"


def test_associations_are_always_a_full_overwrite_from_target_table_ids():
    fake = FakeHubSpot(
        objects={
            "contacts": [make_contact(1, ts(5)), make_contact(2, ts(5))],
            "companies": [make_company(10, ts(5))],
            "deals": [make_deal(20, ts(5))],
        },
        associations={
            ("contacts", "companies"): {"1": [assoc_link(10)], "99": [assoc_link(10)]},
            ("deals", "contacts"): {"20": [assoc_link(1), assoc_link(2)]},
            ("deals", "companies"): {"20": [assoc_link(10)]},
        },
    )
    h = Harness(fake)
    assert h.run(ALL)["associations"] == 4
    assert h.state.rows["associations"]["last_mode"] == "full"

    fake.associations[("deals", "contacts")]["20"] = [assoc_link(1)]  # a link removed upstream
    counts = h.run(ALL, now=at(10))

    assert counts["associations"] == 3
    assert h.state.rows["associations"]["last_mode"] == "full" and h.state.rows["contacts"]["last_mode"] == "incremental"
    assert ("overwrite", "associations", 3) in h.writer.operations
    assert len(h.writer.tables["associations"]) == 3  # the removed link is gone
    sent = [[i["id"] for i in body["inputs"]] for body in fake.calls_to("/batch/read") if body and "properties" not in body]
    assert ["1", "2"] in sent and ["20"] in sent and ["99"] not in sent  # ids come from the tables


def test_associations_skip_archived_source_rows():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(1, ts(5)), make_contact(2, ts(5), archived=True, archived_at=ts(6))],
                 "companies": [make_company(10, ts(5))]},
        associations={("contacts", "companies"): {"1": [assoc_link(10)], "2": [assoc_link(10)]}},
    )
    h = Harness(fake)
    h.run(["contacts", "companies"])
    h.run(["contacts", "companies", "associations"], mode="full", now=at(10))
    assert [r["from_id"] for r in h.writer.tables["associations"]] == ["1"]


def test_association_rows_carry_every_type_and_the_run_time():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]},
        associations={("contacts", "companies"): {"1": [("10", [
            {"category": "HUBSPOT_DEFINED", "typeId": 1, "label": None},
            {"category": "USER_DEFINED", "typeId": 7, "label": "Boss"},
        ])]}},
    )
    h = Harness(fake)
    h.run(["contacts", "companies", "associations"])
    rows = h.writer.tables["associations"]
    assert sorted(r["association_type_id"] for r in rows) == [1, 7]
    assert all(r["_ingested_at"] == NOW for r in rows)


def test_missing_source_table_merges_other_pairs_and_reports_failed():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))], "deals": []},
        associations={("contacts", "companies"): {"1": [assoc_link(10)]}},
    )
    h = Harness(fake)
    h.run(["contacts", "companies"])
    h.writer.fail_on = {"deals"}  # deals is selected, but its table is never written

    assert h.run(["contacts", "companies", "deals", "associations"], now=at(10)) is None

    assert "associations:" in h.error and "deals->contacts" in h.error and "lake.crm.deals" in h.error
    assert "contacts->companies" not in h.error
    assert len(h.writer.tables["associations"]) == 1  # the healthy pair was still written
    assert h.state.rows["associations"]["last_status"] == "FAILED"
    assert [op[0] for op in h.writer.operations if op[1] == "associations"] == ["merge"]  # never an overwrite


def test_associations_merge_instead_of_overwrite_when_a_source_object_is_not_selected():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]},
        associations={("contacts", "companies"): {"1": [assoc_link(10)]}},
    )
    h = Harness(fake)
    assert h.run(["contacts", "companies", "associations"])["associations"] == 1  # deals not selected: pairs skipped
    assert [op[0] for op in h.writer.operations if op[1] == "associations"] == ["merge"]
    assert any("skipping deals->contacts" in line for line in h.logs)


def test_failed_object_keeps_its_watermark_and_others_continue():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]})
    h = Harness(fake)
    h.run(["contacts", "companies"])
    fake.objects["contacts"][0] = make_contact(1, ts(7, hour=13))
    h.writer.fail_on = {"contacts"}

    assert h.run(["contacts", "companies"], now=at(20, hour=13)) is None

    assert h.error.startswith("HubSpot sync failed for contacts: write to contacts failed")
    assert h.state.get_watermark("contacts") == NOW
    assert h.state.rows["contacts"]["last_status"] == "FAILED" and h.state.rows["contacts"]["last_rows"] == 1
    assert h.state.get_watermark("companies") == at(20, hour=13)
    assert h.state.rows["companies"]["last_status"] == "SUCCESS"


@pytest.mark.parametrize("hook", ["get_watermark", "set_watermark"])
def test_state_error_on_one_object_does_not_stop_the_others(hook):
    class FlakyState(InMemoryState):
        def get_watermark(self, name):
            if hook == "get_watermark" and name == "contacts":
                raise OSError("state read failed")
            return super().get_watermark(name)

        def set_watermark(self, name, mode, rows, run_at):
            if hook == "set_watermark" and name == "contacts":
                raise OSError("state write failed")
            super().set_watermark(name, mode, rows, run_at)

    h = Harness(FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]}))
    h.state = FlakyState()

    assert h.run(["contacts", "companies"]) is None

    assert h.error.startswith("HubSpot sync failed for contacts: state ")
    assert h.state.rows["contacts"]["watermark"] is None
    assert h.state.rows["contacts"]["last_status"] == "FAILED"
    assert h.state.get_watermark("companies") == NOW


def test_failure_text_is_truncated_and_has_no_portal_id():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]})
    fake.errors[f"/crm/properties/{API_VERSION}/contacts"] = error_response(
        400, "VALIDATION_ERROR", "bad request " + "x" * 3000, groupName="portal-4242"
    )
    h = Harness(fake)
    assert h.run(["contacts"]) is None
    failure = next(line for line in h.logs if "FAILED" in line)
    assert len(failure) < 1100 and "portal-4242" not in h.error and TOKEN not in h.error
    assert h.state.get_watermark("contacts") is None
    assert h.state.rows["contacts"]["last_status"] == "FAILED"


def test_a_failed_first_run_leaves_no_watermark_so_the_next_run_is_full_again():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]})
    h = Harness(fake)
    h.writer.fail_on = {"contacts"}
    h.run(["contacts"])
    h.writer.fail_on = set()
    assert h.run(["contacts"], now=at(5)) == {"contacts": 1}
    assert h.state.rows["contacts"]["last_mode"] == "full"


def test_a_hubspot_error_status_is_reported_without_the_token():
    fake = FakeHubSpot(objects={"contacts": []})
    fake.errors[f"/crm/properties/{API_VERSION}/contacts"] = error_response(401, "INVALID_AUTHENTICATION", "bad credentials")
    h = Harness(fake)
    assert h.run(["contacts"]) is None
    assert "401" in h.error and "INVALID_AUTHENTICATION" in h.error and TOKEN not in h.error + "".join(h.logs)


def test_no_token_text_in_logs_on_success():
    h = Harness(FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]}))
    h.run(["contacts"])
    assert TOKEN not in "".join(h.logs)


def test_only_selected_objects_are_synced():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]})
    h = Harness(fake)
    assert h.run(["companies"]) == {"companies": 1}
    assert h.writer.tables.keys() == {"companies"} and h.state.rows.keys() == {"companies"}


def test_objects_are_isolated_by_watermark():
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))]})
    h = Harness(fake)
    h.run(["contacts"])
    h.run(["contacts", "companies"], now=at(10))
    assert h.state.rows["contacts"]["last_mode"] == "incremental"
    assert h.state.rows["companies"]["last_mode"] == "full"  # companies had no watermark of its own


def test_run_retries_a_503_from_the_properties_endpoint():
    """One 503 on the properties call is retried by the client and the run still succeeds.
    The harness runs at 1000 requests per second on a fake clock, so throttling itself is not exercised here."""
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]})
    original = fake.request
    state = {"failed": False}

    def flaky(method, url, **kwargs):
        if not state["failed"] and url.endswith("/crm/properties/2026-03/contacts"):
            state["failed"] = True
            return error_response(503, "SERVER", "down")
        return original(method, url, **kwargs)

    fake.request = flaky
    assert Harness(fake).run(["contacts"]) == {"contacts": 1}
    assert state["failed"]
