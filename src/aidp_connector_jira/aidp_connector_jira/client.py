"""Jira Cloud REST API v3 client: HTTP retries, site checks, JQL and paging.

Read-only. Uses only ``requests``. Credentials are never logged.
"""

from __future__ import annotations

import math
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import JiraAuthError, JiraError, JiraRateLimitError
from .records import TYPED_FIELDS

# --------------------------------------------------------------------------
# HTTP retry engine
# --------------------------------------------------------------------------

MAX_BACKOFF_SECONDS = 60.0
TRANSIENT_STATUSES = (502, 503, 504)


def _is_permanent_request_error(exc) -> bool:
    """A requests error that retrying cannot fix, e.g. InvalidURL or MissingSchema."""
    try:
        import requests
    except ImportError:
        return False
    permanent = (
        requests.exceptions.InvalidURL,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidSchema,
        requests.exceptions.InvalidHeader,
        requests.exceptions.URLRequired,
    )
    return isinstance(exc, permanent)


def _retry_after_seconds(response, fallback: float) -> float:
    value = response.headers.get("Retry-After")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(seconds):  # NaN/inf would crash or stall sleep()
        return fallback
    return min(max(seconds, 0.0), MAX_BACKOFF_SECONDS)


def _request_json(make_request, *, max_retries=5, sleep=time.sleep) -> dict:
    """Shared 429/auth/error/JSON handling for one GET or POST call.

    ``make_request`` is a zero-argument callable that performs the actual
    ``session.get(...)`` / ``session.post(...)`` and returns the response.
    """
    attempt = 0
    while True:
        try:
            response = make_request()
        except OSError as exc:  # requests' connection/timeout errors subclass OSError
            if _is_permanent_request_error(exc):
                raise JiraError(
                    "request failed ({}); check the site name".format(type(exc).__name__)
                ) from None
            if attempt >= max_retries:
                raise JiraError(
                    "network error ({}) after {} retries".format(type(exc).__name__, max_retries)
                ) from None
            sleep(min(2 ** attempt, MAX_BACKOFF_SECONDS))
            attempt += 1
            continue
        status = response.status_code
        if status in TRANSIENT_STATUSES and attempt < max_retries:
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status == 429:
            if attempt >= max_retries:
                raise JiraRateLimitError(
                    "rate limited (HTTP 429) after {} retries".format(max_retries)
                )
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status in (401, 403):
            raise JiraAuthError("HTTP {}: check the credentials".format(status))
        if status >= 400:
            raise JiraError(
                "HTTP {} from the API: {}".format(status, _safe_body(response))
            )
        try:
            payload = response.json()
        except ValueError:
            raise JiraError(
                "response was not JSON; check the site/instance name and URL"
            ) from None
        if not isinstance(payload, dict):
            raise JiraError(
                "response JSON was a {}, not an object — every caller expects"
                " a dict".format(type(payload).__name__)
            )
        return payload


def _safe_body(response) -> str:
    try:
        return str(response.json())[:300]
    except ValueError:
        return "<non-JSON body>"


def post_json(session, url, body, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """POST ``body`` to ``url`` and return the parsed JSON, with bounded 429 retries."""
    return _request_json(
        lambda: session.post(url, json=body, timeout=timeout),
        max_retries=max_retries, sleep=sleep,
    )


def get_json(session, url, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """GET ``url`` and return the parsed JSON, with the same retry/error handling as post_json."""
    return _request_json(
        lambda: session.get(url, timeout=timeout),
        max_retries=max_retries, sleep=sleep,
    )


def redact(text: str, *secrets: str) -> str:
    """Replace every non-empty secret in ``text`` with ``***``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


# --------------------------------------------------------------------------
# Jira Cloud
# --------------------------------------------------------------------------


_JIRA_CLOUD_HOST = re.compile(r"^[a-z0-9][a-z0-9-]*\.atlassian\.net$")


def normalize_site(site: str) -> str:
    """Return a bare host name from ``site`` or raise ValueError."""
    host = re.sub(r"^https?://", "", (site or "").strip(), flags=re.I).rstrip("/").lower()
    # Basic auth sends the API token to this host, so accept only Jira Cloud sites.
    if not _JIRA_CLOUD_HOST.match(host):
        raise ValueError(
            "site must be a Jira Cloud host name such as <site>.atlassian.net"
        )
    return host


def credentials(site, email, api_token) -> Tuple[str, str, str]:
    """Check and normalise ``(site, email, api_token)`` from any source.

    ``auth.read_credentials`` passes the values read from the Credential
    Store or the environment. Values are
    stripped; a blank value raises ``JiraError`` naming it, never echoing any
    value; ``site`` must be a Jira Cloud host (see ``normalize_site``).
    """
    values = {"site": site, "email": email, "api_token": api_token}
    missing = [name for name, value in values.items() if not str(value or "").strip()]
    if missing:
        raise JiraError("missing credential value(s): " + ", ".join(missing))
    return normalize_site(str(site)), str(email).strip(), str(api_token).strip()


def jira_session(email: str, api_token: str):
    """A requests.Session with HTTP Basic auth (email:api_token) and JSON Accept."""
    import requests

    session = requests.Session()
    session.auth = (email, api_token)
    session.headers.update({"Accept": "application/json"})
    return session


_FORBIDDEN_IN_QUERY = re.compile(r"order\s+by", re.I)


def format_jql_timestamp(value: datetime, tz=timezone.utc) -> str:
    """``YYYY-MM-DD HH:MM`` in ``tz`` (JQL date-time literal).

    Jira interprets a JQL date-time literal in the *searching account's own*
    timezone, not UTC — pass the account's zone (see ``account_timezone()``) as
    ``tz``, not just the default. A naive ``value`` is treated as already being
    in UTC before conversion to ``tz``.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(tz).strftime("%Y-%m-%d %H:%M")


def build_jql(*, since=None, until=None, query=None, tz=timezone.utc) -> str:
    """JQL for one search. Caller query is ANDed with the watermark window.

    ``tz`` must be the searching account's own timezone (``account_timezone()``)
    for the bounds to mean what they say; the default UTC is only correct for
    an account whose Jira profile timezone is UTC.
    """
    if query and _FORBIDDEN_IN_QUERY.search(query):
        raise ValueError("query must not contain ORDER BY; paging adds its own ordering")
    parts = []
    # query.strip() guards a whitespace-only query, which would otherwise
    # produce a bare, invalid "()" clause after stripping.
    if query and query.strip():
        parts.append("(%s)" % query.strip())
    if since is not None:
        parts.append('updated >= "%s"' % format_jql_timestamp(since, tz))
    if until is not None:
        parts.append('updated <= "%s"' % format_jql_timestamp(until, tz))
    parts.append("ORDER BY updated ASC, key ASC")
    return " AND ".join(parts[:-1]) + (" " if parts[:-1] else "") + parts[-1]


def account_timezone(session, site, *, timeout=60, max_retries=5, sleep=time.sleep) -> str:
    """The searching account's IANA timezone name, from GET /rest/api/3/myself.

    Jira interprets JQL date-time literals in this timezone, not UTC. Assuming
    UTC silently skips or delays issues for any account whose Jira profile
    timezone is not UTC, so ``search_issues`` calls this itself when ``tz_name``
    is omitted. Call it directly only to reuse one lookup across several searches.
    """
    host = normalize_site(site)
    url = "https://{}/rest/api/3/myself".format(host)
    payload = get_json(session, url, timeout=timeout, max_retries=max_retries, sleep=sleep)
    tz_name = payload.get("timeZone")
    if not tz_name:
        raise JiraError("account profile response has no timeZone field")
    return tz_name


MAX_PAGE_SIZE = 5000


def search_issues(
    session,
    site,
    *,
    query=None,
    fields=None,
    since=None,
    overlap_seconds=300,
    until=None,
    page_size=100,
    tz_name=None,
    timeout=60,
    max_retries=5,
    sleep=time.sleep,
    now=None,
) -> Iterator[dict]:
    """Yield every issue matching ``query`` and updated within a fixed window.

    Pages via the server's opaque ``nextPageToken``; never builds or assumes an
    offset. ``until`` is fixed at the first request (default: now), so issues
    updated during the read fall into the next run rather than this one.

    The recommended watermark contract, used by the example notebook: after
    writing the result, read ``MAX(updated)`` back from the target table and
    pass it as the next run's ``since`` — this needs no extra state beyond the
    table itself. Passing this call's ``until`` as the next call's ``since`` is
    an equally correct alternative for a caller that already tracks it, since
    both name the same instant; pick one contract per caller and be
    consistent. De-duplicate on issue ``key`` either way, since the overlap
    window re-reads a few issues on purpose.

    ``tz_name`` is the searching account's IANA timezone name (from
    ``account_timezone(session, site)``), e.g. ``"Asia/Karachi"``. Jira compares
    the JQL watermark bounds in that timezone, not UTC. When omitted, it is
    fetched from the account profile (one extra GET /myself); pass it to reuse a
    lookup or to force a zone, e.g. ``"UTC"``.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be between 1 and {}".format(MAX_PAGE_SIZE))
    build_jql(query=query)  # validates the caller's query before any request
    if tz_name is None:
        # Jira reads JQL date-times in the account's own timezone; never guess UTC.
        tz_name = account_timezone(
            session, site, timeout=timeout, max_retries=max_retries, sleep=sleep
        )

    host = normalize_site(site)
    url = "https://{}/rest/api/3/search/jql".format(host)
    upper = until if until is not None else (now() if now else datetime.now(timezone.utc))
    lower = since - timedelta(seconds=overlap_seconds) if since is not None else None
    try:
        tz = ZoneInfo(tz_name) if tz_name else timezone.utc
    except (ZoneInfoNotFoundError, ValueError):
        raise JiraError("unknown timezone name {!r}".format(tz_name)) from None
    # Default to every typed column, not just key+updated — a caller who passes
    # no fields= must still get summary/status/etc. populated in to_dataframe.
    if fields is None:
        fields = [name for name, _ in TYPED_FIELDS if name not in ("key", "updated")]
    field_list = list(dict.fromkeys(["key", "updated", *fields]))
    jql = build_jql(since=lower, until=upper, query=query, tz=tz)

    token = None
    seen_tokens = set()
    while True:
        body = {"jql": jql, "fields": field_list, "maxResults": page_size}
        if token is not None:
            body["nextPageToken"] = token
        payload = post_json(
            session, url, body, timeout=timeout, max_retries=max_retries, sleep=sleep
        )
        yield from payload.get("issues", [])
        if payload.get("isLast"):
            return
        next_token = payload.get("nextPageToken")
        if not next_token:
            raise JiraError("server did not report isLast but returned no nextPageToken")
        if next_token in seen_tokens:
            raise JiraError("paging did not advance; the server repeated a nextPageToken")
        seen_tokens.add(next_token)
        token = next_token
