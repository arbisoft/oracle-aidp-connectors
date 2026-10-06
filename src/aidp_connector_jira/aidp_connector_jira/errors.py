"""Errors raised by the connector. Messages never contain credentials."""


class JiraError(Exception):
    """Any failure talking to Jira Cloud. Messages never contain credentials."""


class JiraAuthError(JiraError):
    """HTTP 401 or 403."""


class JiraRateLimitError(JiraError):
    """HTTP 429 persisted after the bounded number of retries."""
