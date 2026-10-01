"""Tests for the HubSpot client: auth, throttling, retries, error sanitizing, pagination."""

from __future__ import annotations

import pytest

from fakes import (
    FakeHubSpot, FakeResponse, FakeSession, FakeTime, assoc_link, client_for, error_response, make_contact,
)
from hubspot_client import API_VERSION, HubSpotClient, HubSpotError

TOKEN = "test-token-abc123"
PORTAL = "99887766"
OK = FakeResponse(200, {"ok": True})


def make(responses, **kwargs):
    fake_time = FakeTime()
    session = FakeSession(responses)
    client = HubSpotClient(TOKEN, session=session, sleep=fake_time.sleep, clock=fake_time.clock, **kwargs)
    return client, session, fake_time


def rate_limited(policy="TEN_SECONDLY_ROLLING"):
    return error_response(
        429, "RATE_LIMITS", "You have reached your ten_secondly_rolling limit.", policy_name=policy,
        groupName=f"portal-{PORTAL}",
    )


def test_token_required():
    with pytest.raises(ValueError):
        HubSpotClient("")


def test_bearer_header_base_url_and_timeout():
    client, session, _ = make([OK], timeout=42)
    assert client.call("GET", "/crm/objects/2026-03/contacts", params={"limit": 1}) == {"ok": True}
    call = session.calls[0]
    assert call["headers"]["Authorization"] == f"Bearer {TOKEN}"
    assert call["url"] == "https://api.hubapi.com/crm/objects/2026-03/contacts"
    assert call["params"] == {"limit": 1}
    assert call["timeout"] == 42


def test_custom_base_url_and_body_passed():
    client, session, _ = make([OK], base_url="http://localhost:1234/")
    client.call("POST", "/x", body={"a": 1})
    assert session.calls[0]["url"] == "http://localhost:1234/x"
    assert session.calls[0]["json"] == {"a": 1}


def test_throttle_spaces_requests():
    client, _, fake_time = make([OK, OK, OK], requests_per_second=4)
    for _ in range(3):
        client.call("GET", "/x")
    assert fake_time.sleeps == [0.25, 0.25]


def test_throttle_skips_sleep_when_slow_enough():
    client, _, fake_time = make([OK, OK], requests_per_second=2)
    client.call("GET", "/x")
    fake_time.now += 5
    client.call("GET", "/x")
    assert fake_time.sleeps == []


def test_requests_per_second_must_be_positive():
    with pytest.raises(ValueError):
        HubSpotClient(TOKEN, requests_per_second=0)


def test_retries_5xx_with_exponential_backoff():
    responses = [error_response(500), error_response(502), error_response(503), error_response(504), OK]
    client, session, fake_time = make(responses, requests_per_second=1000)
    assert client.call("GET", "/x") == {"ok": True}
    assert len(session.calls) == 5
    backoffs = [s for s in fake_time.sleeps if s >= 1]
    assert backoffs == [1, 2, 4, 8]


def test_backoff_is_capped_at_30_seconds():
    responses = [error_response(500)] * 7 + [OK]
    client, _, fake_time = make(responses, max_attempts=10, requests_per_second=1000)
    client.call("GET", "/x")
    backoffs = [s for s in fake_time.sleeps if s >= 1]
    assert backoffs == [1, 2, 4, 8, 16, 30, 30]


def test_5xx_gives_up_after_max_attempts():
    client, session, _ = make([error_response(503, "SERVER", "down")] * 3, max_attempts=3)
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert excinfo.value.status == 503
    assert len(session.calls) == 3


def test_connection_errors_retry_and_share_counter_with_5xx():
    client, session, fake_time = make(
        [ConnectionError("reset"), TimeoutError("slow"), error_response(500), OK], requests_per_second=1000
    )
    assert client.call("GET", "/x") == {"ok": True}
    assert [s for s in fake_time.sleeps if s >= 1] == [1, 2, 4]
    assert len(session.calls) == 4


def test_connection_error_reraised_when_exhausted():
    client, session, _ = make([ConnectionError("reset")] * 2, max_attempts=2)
    with pytest.raises(ConnectionError):
        client.call("GET", "/x")
    assert len(session.calls) == 2


def test_429_ten_secondly_backs_off_exponentially_without_retry_after():
    client, session, fake_time = make([rate_limited(), rate_limited(), rate_limited(), OK], requests_per_second=1000)
    assert client.call("GET", "/x") == {"ok": True}
    assert [s for s in fake_time.sleeps if s >= 1] == [1, 2, 4]
    assert len(session.calls) == 4


def test_429_backoff_is_capped():
    client, _, fake_time = make([rate_limited()] * 8 + [OK], requests_per_second=1000)
    client.call("GET", "/x")
    assert [s for s in fake_time.sleeps if s >= 1] == [1, 2, 4, 8, 16, 30, 30, 30]


def test_429_retry_after_is_honored_when_present():
    limited = FakeResponse(429, rate_limited().json(), headers={"Retry-After": "3"})
    client, _, fake_time = make([limited, OK], requests_per_second=1000)
    client.call("GET", "/x")
    assert [s for s in fake_time.sleeps if s >= 1] == [3.0]


def test_429_retry_after_http_date_falls_back_to_backoff():
    limited = FakeResponse(429, rate_limited().json(), headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    client, _, fake_time = make([limited, OK], requests_per_second=1000)
    client.call("GET", "/x")
    assert [s for s in fake_time.sleeps if s >= 1] == [1]


def test_429_daily_stops_immediately():
    client, session, fake_time = make([rate_limited("DAILY"), OK])
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert excinfo.value.status == 429
    assert excinfo.value.policy_name == "DAILY"
    assert len(session.calls) == 1
    assert fake_time.sleeps == []


def test_429_gives_up_after_max_rate_limit_waits():
    client, session, _ = make([rate_limited()] * 4, max_rate_limit_waits=3)
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert excinfo.value.policy_name == "TEN_SECONDLY_ROLLING"
    assert len(session.calls) == 4


def test_429_without_json_body_still_retries():
    client, _, _ = make([FakeResponse(429, None, text="slow down"), OK], requests_per_second=1000)
    assert client.call("GET", "/x") == {"ok": True}


def test_207_is_success_and_body_returned():
    body = {"status": "COMPLETE_WITH_ERRORS", "results": [], "errors": [{"category": "NO_ASSOCIATIONS_FOUND"}]}
    client, session, _ = make([FakeResponse(207, body)])
    assert client.call("POST", "/x") == body
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409])
def test_other_4xx_fail_at_once(status):
    client, session, fake_time = make([error_response(status, "BAD", "nope"), OK])
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert excinfo.value.status == status
    assert excinfo.value.category == "BAD"
    assert excinfo.value.message == "nope"
    assert len(session.calls) == 1
    assert fake_time.sleeps == []


def test_error_text_is_sanitized_for_429():
    client, _, _ = make([rate_limited("DAILY")])
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    text = str(excinfo.value) + repr(excinfo.value)
    assert PORTAL not in text
    assert "groupName" not in text
    assert TOKEN not in text
    assert "RATE_LIMITS" in text and "ten_secondly_rolling" in text


def test_error_text_never_echoes_non_json_body():
    body = f"<html>portal {PORTAL} failed</html>"
    client, _, _ = make([FakeResponse(400, None, text=body)])
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert PORTAL not in str(excinfo.value)
    assert excinfo.value.category == "unknown"


def test_error_text_omits_extra_json_fields_after_retries_exhausted():
    payload = {"message": "down", "category": "SERVER", "portalId": PORTAL, "context": {"ids": [PORTAL]}}
    client, _, _ = make([FakeResponse(500, payload)], max_attempts=1)
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert PORTAL not in str(excinfo.value)


def test_token_never_in_error_text():
    client, _, _ = make([error_response(401, "INVALID_AUTHENTICATION", "bad credentials")])
    with pytest.raises(HubSpotError) as excinfo:
        client.call("GET", "/x")
    assert TOKEN not in str(excinfo.value)


def page(items, after=None):
    payload = {"results": items}
    if after:
        payload["paging"] = {"next": {"after": after}}
    return FakeResponse(200, payload)


def test_pages_follows_cursor_and_sets_limit():
    client, session, _ = make([page([1, 2], "c1"), page([3], "c2"), page([4])])
    assert list(client.pages("/p", {"archived": "true"})) == [1, 2, 3, 4]
    assert [c["params"] for c in session.calls] == [
        {"archived": "true", "limit": 100},
        {"archived": "true", "limit": 100, "after": "c1"},
        {"archived": "true", "limit": 100, "after": "c2"},
    ]


def test_pages_stops_on_repeated_cursor():
    client, session, _ = make([page([1], "c1"), page([2], "c1"), page([3], "c1")])
    assert list(client.pages("/p")) == [1, 2]
    assert len(session.calls) == 2


def test_pages_handles_missing_results_and_does_not_mutate_params():
    client, _, _ = make([FakeResponse(200, {})])
    params = {"a": "b"}
    assert list(client.pages("/p", params)) == []
    assert params == {"a": "b"}


def test_client_against_fake_hubspot_list_and_associations():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(i) for i in range(1, 6)]},
        associations={("contacts", "companies"): {"1": [assoc_link(7)]}},
        page_size=2,
    )
    client = client_for(fake)
    ids = [r["id"] for r in client.pages(f"/crm/objects/{API_VERSION}/contacts")]
    assert ids == ["1", "2", "3", "4", "5"]
    reply = client.call(
        "POST", f"/crm/associations/{API_VERSION}/contacts/companies/batch/read", body={"inputs": [{"id": "2"}]}
    )
    assert reply["errors"][0]["category"] == "OBJECT_NOT_FOUND"


def test_fake_hubspot_search_cap_and_assoc_cursor():
    fake = FakeHubSpot(
        objects={"contacts": [make_contact(i) for i in range(1, 8)]},
        associations={("contacts", "companies"): {"1": [assoc_link(n) for n in range(10, 15)]}},
        search_cap=3,
        assoc_page_size=2,
    )
    client = client_for(fake)
    search = f"/crm/objects/{API_VERSION}/contacts/search"
    first = client.call("POST", search, body={"limit": 200, "filterGroups": [{"filters": [
        {"propertyName": "hs_object_id", "operator": "GT", "value": "0"}]}]})
    assert [r["id"] for r in first["results"]] == ["1", "2", "3"]
    with pytest.raises(HubSpotError):
        client.call("POST", search, body={"limit": 200, "after": "3"})
    assoc = f"/crm/associations/{API_VERSION}/contacts/companies/batch/read"
    reply = client.call("POST", assoc, body={"inputs": [{"id": "1", "after": "2"}]})
    assert [t["toObjectId"] for t in reply["results"][0]["to"]] == [12, 13]
    assert reply["results"][0]["paging"]["next"]["after"] == "4"
