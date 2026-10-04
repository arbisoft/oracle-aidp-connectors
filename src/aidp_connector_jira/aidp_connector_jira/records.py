"""Jira issues as flat rows, and a Spark DataFrame of them."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .errors import JiraError

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

TYPED_FIELDS = [
    ("key", "STRING"), ("summary", "STRING"), ("status", "STRING"),
    ("priority", "STRING"), ("assignee", "STRING"), ("reporter", "STRING"),
    ("issuetype", "STRING"), ("project", "STRING"),
    ("created", "TIMESTAMP"), ("updated", "TIMESTAMP"),
]

_NAME_FIELDS = {"status", "priority", "issuetype"}
_PERSON_FIELDS = {"assignee", "reporter"}


def _parse_jira_timestamp(name, text):
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT)
    except (TypeError, ValueError):
        raise JiraError("field {} is not a Jira timestamp".format(name)) from None


def normalize_issue(issue: dict):
    """One issue as a tuple following TYPED_FIELDS order, plus a trailing
    ``raw_fields`` JSON string holding any requested field not in TYPED_FIELDS
    (e.g. a custom field) — never silently dropped."""
    fields = issue.get("fields") or {}
    out = []
    for name, sql_type in TYPED_FIELDS:
        if name == "key":
            out.append(issue.get("key"))
            continue
        value = fields.get(name)
        if value is None:
            out.append(None)
        elif name in _NAME_FIELDS:
            out.append(value.get("name"))
        elif name in _PERSON_FIELDS:
            out.append(value.get("displayName"))
        elif name == "project":
            out.append(value.get("key"))
        elif sql_type == "TIMESTAMP":
            out.append(_parse_jira_timestamp(name, value))
        else:
            out.append(value)
    typed_names = {name for name, _ in TYPED_FIELDS}
    extra = {k: v for k, v in fields.items() if k not in typed_names}
    out.append(json.dumps(extra, sort_keys=True, ensure_ascii=False) if extra else None)
    return tuple(out)


def to_dataframe(spark, issues):
    """A Spark DataFrame: one typed column per TYPED_FIELDS entry, plus a
    trailing ``raw_fields`` JSON-string column for any other requested field."""
    ddl = ", ".join("{} {}".format(n, t) for n, t in TYPED_FIELDS)
    ddl += ", raw_fields STRING"
    # The overlap window (or an issue updated mid-paging) can return one key
    # twice; keep only the newest row so a later MERGE never sees duplicates.
    updated_at = [n for n, _ in TYPED_FIELDS].index("updated")
    latest = {}
    for row in map(normalize_issue, issues):
        seen = latest.get(row[0])
        if seen is None or (row[updated_at] or datetime.min.replace(tzinfo=timezone.utc)) >= (
            seen[updated_at] or datetime.min.replace(tzinfo=timezone.utc)
        ):
            latest[row[0]] = row
    return spark.createDataFrame(list(latest.values()), schema=ddl)
