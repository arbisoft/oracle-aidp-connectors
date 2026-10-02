import json
from datetime import datetime, timezone

import pytest

from aidp_connector_mongodb import runner
from aidp_connector_mongodb.config import parse_config
from aidp_connector_mongodb.reader import MongoAuthError
from mongo_fakes import FakeJavaException, FakePy4JError, FakeSchema, FakeSpark, field

URI = "mongodb://reader:S3cret@h1.example.net:27017/"
NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
TABLE = "cat.raw.coll"
TABLE_SCHEMA = FakeSchema({"type": "struct", "fields": [field("_id", "string"), field("date", "timestamp")]})


def config(**sync):
    return parse_config({
        "mongodb": {"database": "db", "collection": "coll"},
        "target": {"catalog": "cat", "schema": "raw", "table": "coll"},
        "sync": sync,
    })


class _Writer:
    def __init__(self, spark):
        self.spark = spark

    def format(self, fmt):
        assert fmt == "delta"
        return self

    def saveAsTable(self, name):
        if self.spark.write_error is not None:
            raise self.spark.write_error
        self.spark.saved.append(name)
        self.spark.tables.add(name)


class _Table:
    def __init__(self, rows):
        self.schema, self.rows = TABLE_SCHEMA, rows

    def count(self):
        return self.rows


class _Result:
    def __init__(self, micros):
        self.micros = micros

    def first(self):
        return {"m": self.micros}


class RunSpark(FakeSpark):
    """FakeSpark plus the catalog, SQL and write calls the runner makes."""

    def __init__(self, tables=(), watermark_micros=None, write_error=None):
        super().__init__(inferred={"type": "struct", "fields": [field("_id", "string")]})
        self.tables, self.watermark_micros, self.write_error = set(tables), watermark_micros, write_error
        self.queries, self.saved, self.views = [], [], []
        spark = self

        class _Catalog:
            def tableExists(self, name):
                return name in spark.tables

        self.catalog = _Catalog()

    def sql(self, query):
        self.queries.append(query)
        return _Result(self.watermark_micros)

    def table(self, name):
        return _Table(41079)

    @property
    def read(self):
        reader = super().read
        load, spark = reader.load, self

        def load_with_write():
            df = load()
            df.write = _Writer(spark)
            df.createOrReplaceTempView = spark.views.append
            return df

        reader.load = load_with_write
        return reader


def pipelines(spark):
    return [r.options.get("aggregation.pipeline") for r in spark.loads]


def test_first_run_creates_the_schema_and_table_from_a_full_read():
    spark = RunSpark()
    summary = runner.run(spark, config(watermark_field="date"), URI, now=lambda: NOW, log=lambda _: None)
    assert spark.queries[0] == "CREATE SCHEMA IF NOT EXISTS cat.raw"
    assert spark.saved == [TABLE] and not spark.views
    assert pipelines(spark) == [None, None, None]  # connection check, inference, read: no date filter
    assert summary == {"collection": "db.coll", "table": TABLE, "mode": "full", "since": None,
                       "rows_in_table": 41079}


def test_later_run_reads_from_the_table_watermark_and_merges_on_id():
    spark = RunSpark(tables={TABLE}, watermark_micros=1505263031500000)  # 2017-09-13T00:37:11.5Z
    summary = runner.run(spark, config(watermark_field="date"), URI, now=lambda: NOW, log=lambda _: None)
    check, read = spark.loads
    assert read.explicit_schema is TABLE_SCHEMA  # the table's schema, not a new sample
    match = json.loads(read.options["aggregation.pipeline"])[0]["$match"]["date"]
    assert match == {"$gte": {"$date": "2017-09-13T00:32:11.500Z"}, "$lte": {"$date": "2026-10-02T12:00:00.000Z"}}
    assert spark.views == ["incoming_document"] and not spark.saved
    assert "MERGE INTO cat.raw.coll t USING incoming_document s ON t._id = s._id" in spark.queries[-1]
    assert "unix_micros(max(`date`))" in spark.queries[1]
    assert summary["mode"] == "incremental" and summary["since"] == "2017-09-13T00:37:11.500000+00:00"


def test_no_watermark_field_rereads_the_whole_collection_and_merges():
    spark = RunSpark(tables={TABLE})
    summary = runner.run(spark, config(), URI, log=lambda _: None)
    assert pipelines(spark) == [None, None]
    assert not any("unix_micros" in q for q in spark.queries)
    assert spark.views == ["incoming_document"] and summary["mode"] == "full re-read"


def test_srv_uri_is_resolved_before_any_read(monkeypatch):
    monkeypatch.setattr(runner, "resolve_srv", lambda spark, uri: URI)
    spark = RunSpark()
    runner.run(spark, config(), "mongodb+srv://reader:S3cret@cluster0.example.net/", log=lambda _: None)
    assert all(r.options["connection.uri"].startswith(URI) for r in spark.loads)


def test_a_failed_write_is_explained_and_redacted():
    error = FakePy4JError(FakeJavaException("bad auth : authentication failed for reader:S3cret"))
    spark = RunSpark(write_error=error)
    with pytest.raises(MongoAuthError) as exc:
        runner.run(spark, config(), URI, log=lambda _: None)
    assert "S3cret" not in str(exc.value) and "reader" not in str(exc.value)


def test_format_summary_skips_empty_values():
    text = runner.format_summary({"table": TABLE, "mode": "full", "since": None})
    assert "since" not in text and "cat.raw.coll" in text
