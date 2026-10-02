import json
from datetime import datetime, timezone

import pytest

from aidp_connector_mongodb import runner
from aidp_connector_mongodb.config import ConfigError, parse_config
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

    def mode(self, mode):
        assert mode == "overwrite"
        return self

    def option(self, key, value):
        assert (key, value) == ("overwriteSchema", "true")
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


@pytest.mark.parametrize("mode", ["full", " FULL "])
def test_full_override_resamples_and_overwrites_an_existing_table(mode):
    spark = RunSpark(tables={TABLE}, watermark_micros=1505263031500000)
    summary = runner.run(spark, config(watermark_field="date"), URI, mode, now=lambda: NOW, log=lambda _: None)
    check, inference, read = spark.loads
    assert read.explicit_schema is not TABLE_SCHEMA  # freshly sampled, not the table's
    assert pipelines(spark) == [None, None, None]  # no date filter: the whole collection
    assert not any("unix_micros" in q for q in spark.queries)
    assert spark.saved == [TABLE] and not spark.views
    assert summary["mode"] == "full refresh" and summary["since"] is None


@pytest.mark.parametrize("mode", [None, "", "  ", "incremental"])
def test_empty_or_incremental_override_is_a_normal_run(mode):
    spark = RunSpark(tables={TABLE})
    assert runner.run(spark, config(), URI, mode, log=lambda _: None)["mode"] == "full re-read"
    assert spark.views == ["incoming_document"]


def test_unknown_override_fails_before_connecting():
    spark = RunSpark(tables={TABLE})
    with pytest.raises(ConfigError, match="mode must be one of"):
        runner.run(spark, config(), URI, "ful", log=lambda _: None)
    assert spark.loads == []


def test_listed_fields_plus_id_and_watermark_reach_the_first_read():
    spark = RunSpark()
    spark.inferred = {"type": "struct", "fields": [field("_id", "string"), field("title", "string"),
                                                   field("plot", "string"), field("date", "timestamp")]}
    runner.run(spark, config(fields=["title"], watermark_field="date"), URI, now=lambda: NOW, log=lambda _: None)
    read = spark.loads[-1]
    assert [f["name"] for f in read.explicit_schema.jsonValue()["fields"]] == ["_id", "title", "date"]


def test_format_summary_skips_empty_values():
    text = runner.format_summary({"table": TABLE, "mode": "full", "since": None})
    assert "since" not in text and "cat.raw.coll" in text
