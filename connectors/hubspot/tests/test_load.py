"""Writer against FakeSpark: the SQL it issues, batching, deduplication and cleanup."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fakes import FakeSpark
from hubspot_client import COLUMNS, Writer, ddl

SMALL = (("id", "STRING"), ("updated_at", "TIMESTAMP"), ("_ingested_at", "TIMESTAMP"))
STAGING = "lake.hubspot_raw.h_contacts__staging_r1"


def writer(spark, **kwargs):
    return Writer(spark, "lake", "hubspot_raw", "h_", run_id="r1", **kwargs)


def rows(count):
    return [{"id": f"r{i}", "updated_at": i, "_ingested_at": 0} for i in range(count)]


def test_ddl_matches_the_column_specs():
    assert ddl(SMALL) == "id STRING, updated_at TIMESTAMP, _ingested_at TIMESTAMP"
    text = ddl(COLUMNS["deals"])
    assert text.startswith("id STRING, created_at TIMESTAMP, updated_at TIMESTAMP, archived BOOLEAN, archived_at TIMESTAMP")
    assert "amount DECIMAL(38,6)" in text and text.endswith("raw_json STRING, _ingested_at TIMESTAMP")
    assert ddl(COLUMNS["associations"]) == (
        "from_object STRING, from_id STRING, to_object STRING, to_id STRING, "
        "association_type_id INT, category STRING, label STRING, _ingested_at TIMESTAMP"
    )


def test_table_names_use_catalog_schema_and_prefix():
    assert writer(FakeSpark()).table_name("contacts") == "lake.hubspot_raw.h_contacts"
    assert Writer(FakeSpark(), "c", "s").table_name("deals") == "c.s.deals"


def test_ensure_schema():
    spark = FakeSpark()
    writer(spark).ensure_schema()
    assert spark.statements == ["CREATE SCHEMA IF NOT EXISTS lake.hubspot_raw"]


def test_overwrite_stages_in_batches_then_inserts_overwrite():
    spark = FakeSpark()
    w = writer(spark)
    original = w.stage
    w.stage = lambda staging, rows_, columns: original(staging, rows_, columns, batch_size=2)
    assert w.write_table("contacts", iter(rows(5)), order_by="_ingested_at", overwrite=True, columns=SMALL) == 5
    assert [len(batch) for _, batch, _ in spark.saved] == [2, 2, 1]
    assert {table for table, _, _ in spark.saved} == {STAGING}
    assert {mode for _, _, mode in spark.saved} == {"append"}
    assert spark.saved[0][1][0] == ("r0", 0, 0)
    assert spark.schemas[0] == ddl(SMALL)
    assert spark.statements[0] == "CREATE TABLE IF NOT EXISTS lake.hubspot_raw.h_contacts " f"({ddl(SMALL)}) USING DELTA"
    assert spark.statements[1] == f"DROP TABLE IF EXISTS {STAGING}"
    assert spark.statements[2] == f"CREATE TABLE {STAGING} ({ddl(SMALL)}) USING DELTA"
    final = [s for s in spark.statements if s.startswith("INSERT OVERWRITE TABLE lake.hubspot_raw.h_contacts ")]
    assert len(final) == 1 and "PARTITION BY id ORDER BY _ingested_at DESC" in final[0]
    assert spark.statements[-1] == f"DROP TABLE IF EXISTS {STAGING}"


def test_overwrite_with_no_rows_still_empties_the_table():
    spark = FakeSpark()
    assert writer(spark).write_table("contacts", [], overwrite=True, columns=SMALL) == 0
    assert spark.saved == []
    assert any(s.startswith("INSERT OVERWRITE TABLE lake.hubspot_raw.h_contacts ") for s in spark.statements)


def test_default_columns_come_from_the_table_name():
    spark = FakeSpark()
    writer(spark).write_table("companies", [])
    assert f"({ddl(COLUMNS['companies'])}) USING DELTA" in spark.statements[0]


def test_merge_deduplicates_keeping_newest_updated_at():
    spark = FakeSpark()
    assert writer(spark).write_table("contacts", rows(3), columns=SMALL) == 3
    merge = [s for s in spark.statements if s.startswith("MERGE INTO")]
    assert len(merge) == 1
    assert merge[0].startswith("MERGE INTO lake.hubspot_raw.h_contacts t USING (SELECT id, updated_at, _ingested_at FROM (")
    assert "ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS _rn" in merge[0]
    assert f"FROM {STAGING}) WHERE _rn = 1" in merge[0]
    assert "ON t.id = s.id" in merge[0]
    assert "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *" in merge[0]
    assert spark.statements[-1] == f"DROP TABLE IF EXISTS {STAGING}"


def test_association_composite_key_merge_and_overwrite():
    from hubspot_client import ASSOCIATION_KEY

    link = {"from_object": "deals", "from_id": "1", "to_object": "contacts", "to_id": "2",
            "association_type_id": 3, "category": "HUBSPOT_DEFINED", "label": None, "_ingested_at": 0}
    spark = FakeSpark()
    w = Writer(spark, "lake", "hubspot_raw", "h_", run_id="r1")
    assert w.write_table("associations", [link, dict(link)], key=ASSOCIATION_KEY, order_by="_ingested_at") == 2
    assert w.write_table("associations", [link], key=ASSOCIATION_KEY, order_by="_ingested_at", overwrite=True) == 1
    merge = next(s for s in spark.statements if s.startswith("MERGE INTO lake.hubspot_raw.h_associations "))
    assert ("ON t.from_object = s.from_object AND t.from_id = s.from_id AND t.to_object = s.to_object "
            "AND t.to_id = s.to_id AND t.association_type_id = s.association_type_id") in merge
    part = "PARTITION BY from_object, from_id, to_object, to_id, association_type_id ORDER BY _ingested_at DESC"
    assert part in merge
    overwrite = next(s for s in spark.statements if s.startswith("INSERT OVERWRITE TABLE lake.hubspot_raw.h_associations "))
    assert part in overwrite
    assert spark.saved[0][1][0][4] == 3  # tuple in column order


def test_mark_archived_stages_ids_and_touches_only_archive_columns():
    at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    spark = FakeSpark()
    writer(spark).mark_archived("contacts", [("1", at), ("2", None), ("1", None)])
    staging = "lake.hubspot_raw.h_contacts__archived_r1"
    assert spark.saved == [(staging, [("1", at), ("2", None), ("1", None)], "append")]
    assert spark.schemas == ["id STRING, archived_at TIMESTAMP"]
    merge = [s for s in spark.statements if s.startswith("MERGE INTO")]
    assert len(merge) == 1
    assert merge[0].startswith("MERGE INTO lake.hubspot_raw.h_contacts t USING (")
    assert "PARTITION BY id ORDER BY archived_at DESC NULLS LAST" in merge[0]
    assert "ON t.id = s.id" in merge[0]
    assert merge[0].endswith("WHEN MATCHED THEN UPDATE SET archived = true, archived_at = s.archived_at")
    assert "INSERT" not in merge[0]
    assert not any(s.startswith("CREATE TABLE IF NOT EXISTS lake.hubspot_raw.h_contacts ") for s in spark.statements)
    assert spark.statements[-1] == f"DROP TABLE IF EXISTS {staging}"


def test_mark_archived_with_no_ids_runs_no_merge():
    spark = FakeSpark()
    writer(spark).mark_archived("contacts", [])
    assert not any(s.startswith("MERGE") for s in spark.statements)
    assert spark.statements[-1].startswith("DROP TABLE IF EXISTS")


def test_mark_archived_drops_staging_on_failure():
    spark = FakeSpark()
    spark.fail_on = "MERGE INTO"
    with pytest.raises(RuntimeError, match="simulated failure"):
        writer(spark).mark_archived("contacts", [("1", None)])
    assert spark.statements[-1] == "DROP TABLE IF EXISTS lake.hubspot_raw.h_contacts__archived_r1"


def test_staging_names_differ_between_runs():
    first, second = FakeSpark(), FakeSpark()
    Writer(first, "lake", "s").write_table("contacts", rows(1), columns=SMALL)
    Writer(second, "lake", "s").write_table("contacts", rows(1), columns=SMALL)
    assert first.saved[0][0].startswith("lake.s.contacts__staging_")
    assert first.saved[0][0] != second.saved[0][0]


def test_staging_is_dropped_when_the_final_statement_fails():
    spark = FakeSpark()
    spark.fail_on = "MERGE INTO"
    with pytest.raises(RuntimeError, match="simulated failure"):
        writer(spark).write_table("contacts", rows(1), columns=SMALL)
    assert spark.statements[-1] == f"DROP TABLE IF EXISTS {STAGING}"


def test_staging_is_dropped_when_the_row_source_fails():
    def broken():
        yield rows(1)[0]
        raise RuntimeError("hubspot went away")

    spark = FakeSpark()
    with pytest.raises(RuntimeError, match="hubspot went away"):
        writer(spark).write_table("contacts", broken(), overwrite=True, columns=SMALL)
    assert not any(s.startswith("INSERT OVERWRITE") for s in spark.statements)
    assert spark.statements[-1] == f"DROP TABLE IF EXISTS {STAGING}"


@pytest.mark.parametrize("bad", ["a; DROP TABLE x", "a b", "", "a.b", "a-b", "x`y", None])
def test_unsafe_catalog_or_schema_is_rejected_before_any_sql(bad):
    spark = FakeSpark()
    with pytest.raises(ValueError, match="catalog"):
        Writer(spark, bad, "s")
    with pytest.raises(ValueError, match="schema"):
        Writer(spark, "c", bad)
    assert spark.statements == []


@pytest.mark.parametrize("bad", ["a; DROP", "a b", "a.b", "a-b", None])
def test_unsafe_prefix_is_rejected(bad):
    with pytest.raises(ValueError, match="table_prefix"):
        Writer(FakeSpark(), "c", "s", bad)


def test_empty_prefix_and_digits_are_allowed():
    assert Writer(FakeSpark(), "c1", "_s2", "").table_name("t") == "c1._s2.t"
