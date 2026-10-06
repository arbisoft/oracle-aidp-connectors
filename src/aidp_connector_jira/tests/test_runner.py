from datetime import datetime, timezone

import pytest

from aidp_connector_jira import runner
from aidp_connector_jira.config import ConfigError, parse_config
from fakes import FakeSearch

CREDS = ("example.atlassian.net", "me@example.com", "tok")
NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)
TABLE = "cat.raw.issue"


def config(**sync):
    return parse_config({
        "jira": {"credential_name": "jira"},
        "target": {"catalog": "cat", "schema": "raw", "table": "issue"},
        "sync": {"jql": "project = KAN", **sync},
    })


def issue(n, updated="2026-10-01T10:00:00.000+0000", **fields):
    return {"key": f"KAN-{n}", "fields": {"summary": f"issue {n}", "updated": updated, **fields}}


def one_page(*issues):
    return FakeSearch([{"issues": list(issues), "isLast": True}], account_tz="UTC")


class _DF:
    def __init__(self, spark, rows):
        self.spark, self.rows = spark, rows
        self.write = self

    def count(self):
        return len(self.rows)

    def format(self, fmt):
        assert fmt == "delta"
        return self

    def mode(self, mode):
        assert mode == "overwrite"
        return self

    def option(self, key, value):
        assert (key, value) == ("overwriteSchema", "true")
        return self

    def saveAsTable(self, name):
        self.spark.saved.append(name)

    def createOrReplaceTempView(self, name):
        self.spark.views.append(name)


class _Result:
    def __init__(self, micros):
        self.micros = micros

    def first(self):
        return {"m": self.micros}


class FakeSpark:
    """The catalog, SQL, DataFrame and write calls the runner makes."""

    def __init__(self, tables=(), watermark_micros=None):
        self.tables, self.watermark_micros = set(tables), watermark_micros
        self.queries, self.saved, self.views, self.frames = [], [], [], []
        spark = self

        class _Catalog:
            def tableExists(self, name):
                return name in spark.tables

        self.catalog = _Catalog()

    def sql(self, query):
        self.queries.append(query)
        return _Result(self.watermark_micros)

    def createDataFrame(self, rows, schema):
        self.frames.append((rows, schema))
        return _DF(self, rows)

    def table(self, name):
        return _DF(self, [None] * 7)


def jql(session):
    return session.calls[-1]["json"]["jql"]


def test_first_run_reads_everything_and_creates_the_table():
    spark, session = FakeSpark(), one_page(issue(1), issue(2))
    summary = runner.run(spark, config(), CREDS, session=session, now=lambda: NOW, log=lambda _: None)
    assert session.get_calls == ["https://example.atlassian.net/rest/api/3/myself"]  # auth check first
    assert spark.queries[0] == "CREATE SCHEMA IF NOT EXISTS cat.raw"
    assert 'updated >= "' not in jql(session) and 'updated <= "2026-10-04 12:00"' in jql(session)
    assert spark.saved == [TABLE] and not spark.views
    assert summary == {"jql": "project = KAN", "table": TABLE, "mode": "full", "since": None,
                       "rows_read": 2, "rows_in_table": 7}


def test_later_run_reads_from_the_table_watermark_and_merges_on_key():
    spark = FakeSpark(tables={TABLE}, watermark_micros=1759312800000000)  # 2025-10-01T10:00:00Z
    session = one_page(issue(1))
    summary = runner.run(spark, config(overlap_seconds=300), CREDS, session=session, now=lambda: NOW,
                         log=lambda _: None)
    assert "unix_micros(max(updated))" in spark.queries[1]
    assert 'updated >= "2025-10-01 09:55"' in jql(session)  # watermark minus the overlap
    assert spark.views == ["incoming_issue"] and not spark.saved
    assert "MERGE INTO cat.raw.issue t USING incoming_issue s ON t.key = s.key" in spark.queries[-1]
    assert summary["mode"] == "incremental" and summary["since"] == "2025-10-01T10:00:00+00:00"


def test_an_existing_empty_table_is_a_full_re_read_merged_on_key():
    spark = FakeSpark(tables={TABLE}, watermark_micros=None)
    summary = runner.run(spark, config(), CREDS, session=one_page(), now=lambda: NOW, log=lambda _: None)
    assert summary["mode"] == "full re-read" and spark.views == ["incoming_issue"]


@pytest.mark.parametrize("mode", ["full", " FULL "])
def test_full_override_rereads_everything_and_overwrites(mode):
    spark = FakeSpark(tables={TABLE}, watermark_micros=1759312800000000)
    session = one_page(issue(1))
    summary = runner.run(spark, config(), CREDS, mode, session=session, now=lambda: NOW, log=lambda _: None)
    assert not any("unix_micros" in q for q in spark.queries)
    assert 'updated >= "' not in jql(session)
    assert spark.saved == [TABLE] and not spark.views
    assert summary["mode"] == "full refresh" and summary["since"] is None


def test_unknown_override_fails_before_any_request():
    session = one_page()
    with pytest.raises(ConfigError, match="mode must be one of"):
        runner.run(FakeSpark(), config(), CREDS, "ful", session=session, log=lambda _: None)
    assert session.get_calls == [] and session.calls == []


def test_extra_fields_are_requested_and_land_in_raw_fields():
    spark = FakeSpark()
    session = one_page(issue(1, customfield_10016=5))
    runner.run(spark, config(extra_fields=["customfield_10016"]), CREDS, session=session, now=lambda: NOW,
               log=lambda _: None)
    assert "customfield_10016" in session.calls[-1]["json"]["fields"]
    (rows, schema), = spark.frames
    assert rows[0][-1] == '{"customfield_10016": 5}' and schema.endswith("raw_fields STRING")


def test_preview_returns_only_the_first_issues_and_writes_nothing():
    spark = FakeSpark()
    session = FakeSearch([{"issues": [issue(n) for n in range(1, 4)], "isLast": False, "nextPageToken": "t"},
                          {"issues": [issue(4)], "isLast": True}])
    df = runner.preview(spark, config(), CREDS, limit=2, session=session)
    assert [row[0] for row in df.rows] == ["KAN-1", "KAN-2"]
    assert len(session.calls) == 1 and session.calls[0]["json"]["maxResults"] == 2
    assert not spark.saved and not spark.queries


def test_format_summary_skips_empty_values():
    text = runner.format_summary({"table": TABLE, "mode": "full", "since": None})
    assert "since" not in text and TABLE in text
