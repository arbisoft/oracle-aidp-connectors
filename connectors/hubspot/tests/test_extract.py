"""Tests for the extractors against the in-memory FakeHubSpot. Synthetic data only."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fakes import FakeHubSpot, assoc_link, client_for, make_contact, ts
import hubspot_client as extract
from hubspot_client import API_VERSION, HubSpotError, association_row

PROPS = ["email", "firstname"]
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def at(minute, hour=10):
    return datetime(2026, 9, 30, hour, minute, tzinfo=timezone.utc)


def ids(records):
    return [r["id"] for r in records]


def test_iter_all_pages_across_boundaries():
    fake = FakeHubSpot({"contacts": [make_contact(i) for i in range(1, 8)]}, page_size=3)
    got = list(extract.iter_all(client_for(fake), "contacts", PROPS))
    assert ids(got) == [str(i) for i in range(1, 8)]
    lists = [c for c in fake.calls if c[0] == "GET"]
    assert len(lists) == 3
    assert lists[0][3]["properties"] == "email,firstname"
    assert lists[0][3]["archived"] == "false"


def test_iter_all_exact_multiple_and_empty():
    fake = FakeHubSpot({"contacts": [make_contact(i) for i in range(1, 7)]}, page_size=3)
    assert len(list(extract.iter_all(client_for(fake), "contacts", PROPS))) == 6
    assert list(extract.iter_all(client_for(FakeHubSpot({"contacts": []})), "contacts", PROPS)) == []


def test_iter_all_archived_only():
    fake = FakeHubSpot({"contacts": [
        make_contact(1), make_contact(2, archived=True, archived_at=ts(5)), make_contact(3),
    ]})
    assert ids(extract.iter_all(client_for(fake), "contacts", PROPS, archived=True)) == ["2"]
    assert ids(extract.iter_all(client_for(fake), "contacts", PROPS)) == ["1", "3"]


def test_iter_archived():
    fake = FakeHubSpot({"contacts": [
        make_contact(1), make_contact(2, archived=True, archived_at=ts(5)),
        make_contact(4, archived=True, archived_at=None),
    ]})
    got = list(extract.iter_archived(client_for(fake), "contacts"))
    assert got == [("2", datetime(2026, 9, 30, 10, 5, tzinfo=timezone.utc)), ("4", None)]


def test_iter_archived_empty():
    assert list(extract.iter_archived(client_for(FakeHubSpot({"contacts": [make_contact(1)]})), "contacts")) == []


def test_iter_modified_window_is_half_open():
    fake = FakeHubSpot({"contacts": [
        make_contact(1, modified=ts(9)),   # before since
        make_contact(2, modified=ts(10)),  # == since: included
        make_contact(3, modified=ts(20)),
        make_contact(4, modified=ts(30)),  # == until: excluded
        make_contact(5, modified=ts(31)),
    ]})
    got = list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(10), at(30)))
    assert ids(got) == ["2", "3"]
    assert set(got[0]["properties"]) == {"email", "firstname"}


def test_iter_modified_search_request_shape():
    fake = FakeHubSpot({"contacts": [make_contact(1, modified=ts(10))]})
    list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    body = fake.calls_to("/contacts/search")[0]
    assert body["sorts"] == [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]
    assert body["limit"] == extract.SEARCH_PAGE
    assert [(f["propertyName"], f["operator"]) for f in body["filterGroups"][0]["filters"]] == [
        ("lastmodifieddate", "GTE"), ("lastmodifieddate", "LT"),
    ]


def test_iter_modified_empty():
    fake = FakeHubSpot({"contacts": [make_contact(1, modified=ts(50))]})
    assert list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30))) == []
    assert fake.calls_to("/batch/read") == []


def test_iter_modified_pages_within_a_query(monkeypatch):
    monkeypatch.setattr(extract, "SEARCH_PAGE", 4)
    fake = FakeHubSpot({"contacts": [make_contact(i, modified=ts(10)) for i in range(1, 11)]})
    # The fake honours the requested limit, so 10 results need 3 pages and no restart.
    got = list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    assert ids(got) == [str(i) for i in range(1, 11)]
    assert len(fake.calls_to("/contacts/search")) == 3


@pytest.mark.parametrize("cap,page", [(5, 200), (5, 2), (4, 4), (1, 200), (10, 200)])
def test_keyset_restart_loses_nothing_and_duplicates_nothing(monkeypatch, cap, page):
    monkeypatch.setattr(extract, "SEARCH_CAP", cap)
    monkeypatch.setattr(extract, "SEARCH_PAGE", page)
    records = [make_contact(i, modified=ts(10)) for i in range(1, 24)]
    records.append(make_contact(99, modified=ts(59)))  # outside the window
    fake = FakeHubSpot({"contacts": records}, search_cap=cap)
    got = ids(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    assert got == [str(i) for i in range(1, 24)]
    later = [b for b in fake.calls_to("/contacts/search") if len(b["filterGroups"][0]["filters"]) == 3]
    if cap < 23:
        assert later, "expected at least one restart with an hs_object_id filter"
        assert later[0]["filterGroups"][0]["filters"][2]["operator"] == "GT"


def test_keyset_restart_restarts_from_last_id(monkeypatch):
    monkeypatch.setattr(extract, "SEARCH_CAP", 5)
    fake = FakeHubSpot({"contacts": [make_contact(i * 10, modified=ts(10)) for i in range(1, 8)]}, search_cap=5)
    list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    restarts = [b["filterGroups"][0]["filters"][2]["value"] for b in fake.calls_to("/contacts/search")
                if len(b["filterGroups"][0]["filters"]) == 3]
    assert restarts == ["50"]  # the restart returns 60 and 70, under the cap, and ends the run


def test_batch_read_is_chunked_by_100(monkeypatch):
    fake = FakeHubSpot({"contacts": [make_contact(i, modified=ts(10)) for i in range(1, 251)]})
    got = list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    assert len(got) == 250 and ids(got) == [str(i) for i in range(1, 251)]
    reads = fake.calls_to("/batch/read")
    assert [len(b["inputs"]) for b in reads] == [100, 100, 50]
    assert reads[0]["properties"] == PROPS


def test_batch_read_207_skips_records_deleted_after_search():
    fake = FakeHubSpot({"contacts": [make_contact(i, modified=ts(10)) for i in range(1, 6)]})
    client = client_for(fake)
    original = fake._batch_read

    def vanish(name, body):
        fake.objects[name] = [r for r in fake.objects[name] if r["id"] != "3"]
        return original(name, body)

    fake._batch_read = vanish
    assert ids(extract.iter_modified(client, "contacts", PROPS, at(0), at(30))) == ["1", "2", "4", "5"]


def test_batch_read_207_other_categories_raise():
    fake = FakeHubSpot({"contacts": [make_contact(i, modified=ts(10)) for i in range(1, 4)]})
    original = fake._batch_read

    def partial_failure(name, body):
        response = original(name, body)
        response._payload["errors"] = [{"category": "RATE_LIMITS", "message": "slow down"}]
        return response

    fake._batch_read = partial_failure
    with pytest.raises(HubSpotError) as info:
        list(extract.iter_modified(client_for(fake), "contacts", PROPS, at(0), at(30)))
    assert info.value.category == "RATE_LIMITS"


def test_iter_all_long_property_list_pages_ids_then_batch_reads(monkeypatch):
    monkeypatch.setattr(extract, "URL_LIMIT", 5)
    monkeypatch.setattr(extract, "READ_BATCH", 2)
    fake = FakeHubSpot({"contacts": [
        make_contact(i, archived=i == 4, archived_at=ts(5) if i == 4 else None) for i in range(1, 6)
    ]}, page_size=3)
    live = list(extract.iter_all(client_for(fake), "contacts", PROPS))
    assert ids(live) == ["1", "2", "3", "5"]
    assert live[0]["properties"]["email"] == "user1@example.com"  # full record came from batch read
    lists = [c for c in fake.calls if c[0] == "GET"]
    assert all("properties" not in c[3] for c in lists)
    assert [[i["id"] for i in b["inputs"]] for b in fake.calls_to("/batch/read")] == [["1", "2"], ["3", "5"]]
    fake.calls.clear()
    archived = list(extract.iter_all(client_for(fake), "contacts", PROPS, archived=True))
    assert ids(archived) == ["4"] and archived[0]["archived"] is True
    call = next(c for c in fake.calls if c[1].endswith("/batch/read"))
    assert call[3] == {"archived": "true"} and "archived" not in call[2]  # query param, never a body field


def test_iter_all_short_property_list_still_uses_a_single_list_call():
    fake = FakeHubSpot({"contacts": [make_contact(1)]})
    list(extract.iter_all(client_for(fake), "contacts", PROPS))
    assert fake.calls_to("/batch/read") == []


def test_associations_basic_shape_and_association_row():
    fake = FakeHubSpot({"contacts": [], "companies": []}, associations={
        ("contacts", "companies"): {"1": [assoc_link(10), assoc_link(11, type_id=2, label="Primary")]},
    })
    links = list(extract.iter_associations(client_for(fake), "contacts", "companies", ["1"]))
    assert [(a, b) for a, b, _ in links] == [("1", "10"), ("1", "11")]
    row = association_row("contacts", *links[1][:1], "companies", links[1][1], links[1][2], NOW)
    assert row["association_type_id"] == 2 and row["label"] == "Primary"
    assert row["from_object"] == "contacts" and row["to_object"] == "companies"
    assert row["category"] == "HUBSPOT_DEFINED"


def test_associations_multiple_types_on_one_pair_give_one_row_each():
    both = ("5", [{"category": "HUBSPOT_DEFINED", "typeId": 1, "label": None},
                  {"category": "USER_DEFINED", "typeId": 9, "label": "Boss"}])
    fake = FakeHubSpot({"contacts": [], "companies": []}, associations={("contacts", "companies"): {"1": [both]}})
    links = list(extract.iter_associations(client_for(fake), "contacts", "companies", ["1"]))
    assert [kind["typeId"] for _, _, kind in links] == [1, 9]


def test_associations_207_no_links_is_skipped():
    fake = FakeHubSpot({"deals": [], "contacts": []}, associations={("deals", "contacts"): {"1": [assoc_link(7)]}})
    links = list(extract.iter_associations(client_for(fake), "deals", "contacts", ["1", "2", "3"]))
    assert [(a, b) for a, b, _ in links] == [("1", "7")]


def test_associations_all_without_links_and_empty_input():
    fake = FakeHubSpot({"deals": [], "contacts": []})
    assert list(extract.iter_associations(client_for(fake), "deals", "contacts", ["1", "2"])) == []
    assert list(extract.iter_associations(client_for(fake), "deals", "contacts", [])) == []
    assert len(fake.calls) == 1  # empty input makes no request


class _Stub:
    def __init__(self, errors):
        self.errors = errors

    api_version = API_VERSION

    def call(self, method, path, **kwargs):
        return {"status": "COMPLETE_WITH_ERRORS", "results": [], "errors": self.errors}


@pytest.mark.parametrize("category", ["OBJECT_NOT_FOUND", "NO_ASSOCIATIONS_FOUND"])
def test_associations_live_api_no_links_shape_is_skipped(category):
    # The live API says OBJECT_NOT_FOUND ("No company is associated with contact 1."); the category alone decides.
    err = {"category": category, "message": "anything at all"}
    assert list(extract.iter_associations(_Stub([err]), "contacts", "companies", ["1"])) == []


def test_associations_fake_emits_the_live_shape_by_default_and_the_other_on_request():
    fake = FakeHubSpot({"contacts": [], "companies": []})
    payload = fake.request("POST", "https://api.hubapi.com/crm/associations/2026-03/contacts/companies/batch/read",
                           json={"inputs": [{"id": "7"}]}).json()
    assert payload["errors"][0]["category"] == "OBJECT_NOT_FOUND"
    assert payload["errors"][0]["message"] == "No company is associated with contact 7."
    fake.no_links_category = "NO_ASSOCIATIONS_FOUND"
    assert list(extract.iter_associations(client_for(fake), "contacts", "companies", ["7"])) == []


def test_associations_other_207_categories_raise_whatever_the_message():
    err = {"category": "VALIDATION_ERROR", "message": "No company is associated with contact 1."}
    with pytest.raises(HubSpotError):
        list(extract.iter_associations(_Stub([err]), "contacts", "companies", ["1"]))


def test_associations_unknown_207_category_raises():
    fake = FakeHubSpot({"deals": [], "contacts": []}, associations={("deals", "contacts"): {"1": [assoc_link(7)]}})
    fake.fail_ids[("deals", "contacts")] = {"2"}
    with pytest.raises(HubSpotError) as info:
        list(extract.iter_associations(client_for(fake), "deals", "contacts", ["1", "2"]))
    assert info.value.category == "VALIDATION_ERROR"


def test_associations_follow_each_inputs_own_cursor():
    fake = FakeHubSpot(
        {"contacts": [], "companies": []},
        associations={("contacts", "companies"): {
            "1": [assoc_link(100 + i) for i in range(5)],  # 3 pages of 2
            "2": [assoc_link(200)],                        # single page
            "3": [assoc_link(300 + i) for i in range(3)],  # 2 pages
        }},
        assoc_page_size=2,
    )
    links = list(extract.iter_associations(client_for(fake), "contacts", "companies", ["1", "2", "3"]))
    got = {}
    for from_id, to_id, _ in links:
        got.setdefault(from_id, []).append(to_id)
    assert got == {
        "1": ["100", "101", "102", "103", "104"], "2": ["200"], "3": ["300", "301", "302"],
    }
    sent = [b["inputs"] for b in fake.calls_to("/batch/read")]
    assert sent[0] == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    assert sent[1] == [{"id": "1", "after": "2"}, {"id": "3", "after": "2"}]
    assert sent[2] == [{"id": "1", "after": "4"}]


def test_associations_groups_of_at_most_1000():
    fake = FakeHubSpot({"deals": [], "companies": []}, associations={("deals", "companies"): {"1": [assoc_link(7)]}})
    many = [str(i) for i in range(1, 2502)]
    list(extract.iter_associations(client_for(fake), "deals", "companies", many))
    assert [len(b["inputs"]) for b in fake.calls_to("/batch/read")] == [1000, 1000, 501]


def test_associations_path_uses_api_version():
    fake = FakeHubSpot({"deals": [], "companies": []})
    list(extract.iter_associations(client_for(fake), "deals", "companies", ["1"]))
    assert fake.calls[0][1] == "/crm/associations/2026-03/deals/companies/batch/read"


def test_property_names():
    fake = FakeHubSpot({"contacts": []}, properties={"contacts": ["email", "firstname", "hs_object_id"]})
    assert extract.property_names(client_for(fake), "contacts") == ["email", "firstname", "hs_object_id"]
    assert fake.calls[0][1] == "/crm/properties/2026-03/contacts"


def test_property_names_derived_and_empty():
    fake = FakeHubSpot({"contacts": [make_contact(1)], "companies": []})
    assert "email" in extract.property_names(client_for(fake), "contacts")
    assert extract.property_names(client_for(fake), "companies") == []
