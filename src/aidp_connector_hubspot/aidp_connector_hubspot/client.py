"""HubSpot REST client: auth, throttling, retries, rate-limit handling and pagination.

Read-only against HubSpot. The token is passed in by the caller and is never
logged or put in an error message.
"""

from __future__ import annotations

import math
import time

import requests

from .config import DEFAULT_API_VERSION

API_URL = "https://api.hubapi.com"


class HubSpotError(RuntimeError):
    """Any failure talking to HubSpot. Messages never contain the token."""

    def __init__(self, status, category, message, policy_name=None):
        super().__init__(f"HubSpot API error {status} ({category}): {message}")
        self.status, self.category, self.message, self.policy_name = status, category, message, policy_name


def _backoff(n: int) -> int:
    return min(2 ** (n - 1), 30)


class HubSpotClient:
    """One throttled session with retries. ``session``, ``sleep`` and ``clock`` can be injected."""

    def __init__(self, token, *, session=None, sleep=time.sleep, clock=time.monotonic,
                 requests_per_second=4.0, api_version=DEFAULT_API_VERSION, base_url=API_URL, timeout=60,
                 max_attempts=8, max_rate_limit_waits=20):
        if not isinstance(token, str) or not token.strip():
            raise ValueError("token must be a non-empty string")
        if not requests_per_second > 0:
            raise ValueError("requests_per_second must be greater than 0")
        self._headers = {"Authorization": f"Bearer {token.strip()}", "Content-Type": "application/json"}
        self.session = session if session is not None else requests.Session()
        self.sleep, self.clock = sleep, clock
        self.interval = 1.0 / requests_per_second
        self.api_version = api_version
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts, self.max_rate_limit_waits = max_attempts, max_rate_limit_waits
        self._next_call = 0.0

    def call(self, method, path, body=None, params=None):
        """One request with throttle and retries. A 207 is returned: the caller reads ``errors``."""
        failures = waits = 0
        while True:
            gap = self._next_call - self.clock()
            if gap > 0:
                self.sleep(gap)
            self._next_call = self.clock() + self.interval
            try:
                resp = self.session.request(method, self.base_url + path, headers=self._headers,
                                            json=body, params=params, timeout=self.timeout)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    ConnectionError, TimeoutError):
                failures += 1
                if failures >= self.max_attempts:
                    raise
                self.sleep(_backoff(failures))
                continue
            status = resp.status_code
            if status < 400:
                return resp.json()
            try:  # only these fields are kept: other fields can carry portal ids
                err = resp.json()
                category, message, policy = str(err.get("category")), str(err.get("message")), err.get("policyName")
            except (ValueError, AttributeError):
                category, message, policy = "unknown", "response body was not JSON", None
            if status == 429 and policy != "DAILY" and waits < self.max_rate_limit_waits:
                waits += 1
                try:  # HubSpot usually sends no Retry-After; honor it when it is a number of seconds
                    delay = float(resp.headers.get("Retry-After"))
                    if not math.isfinite(delay) or delay < 0:
                        raise ValueError
                    delay = min(delay, 30)
                except (TypeError, ValueError):
                    delay = _backoff(waits)
                self.sleep(delay)
            elif status in (500, 502, 503, 504) and failures < self.max_attempts - 1:
                failures += 1
                self.sleep(_backoff(failures))
            else:
                raise HubSpotError(status, category, message, policy)

    def pages(self, path, params=None):
        """Follow the ``after`` cursor of a list endpoint."""
        cursor, seen = None, set()
        while True:
            page = self.call("GET", path, params=dict(params or {}, limit=100, **({"after": cursor} if cursor else {})))
            yield from page.get("results") or []
            cursor = ((page.get("paging") or {}).get("next") or {}).get("after")
            if not cursor or cursor in seen:  # a missing or repeated cursor ends the loop
                return
            seen.add(cursor)
