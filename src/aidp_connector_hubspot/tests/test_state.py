"""State table against FakeSpark."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fakes import FakeSpark
from aidp_connector_hubspot.state import STATE_DDL, StateStore

TABLE = "lake.hubspot_raw.hubspot_sync_state"
RUN_AT = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
WATERMARK = datetime(2026, 9, 30, 10, 12, 0, 345678, tzinfo=timezone.utc)


def micros(value):
    return int(value.timestamp()) * 1_000_000 + value.microsecond


def test_ensure_creates_the_table():
    spark = FakeSpark()
    StateStore(spark, TABLE).ensure()
    assert spark.statements == [
        f"CREATE TABLE IF NOT EXISTS {TABLE} (object_name STRING, watermark TIMESTAMP, "
        "last_mode STRING, last_status STRING, last_rows BIGINT, last_run_at TIMESTAMP) USING DELTA"
    ]


def test_get_with_no_row_or_null_watermark_returns_none():
    assert StateStore(FakeSpark(), TABLE).get_watermark("contacts") is None
    assert StateStore(FakeSpark([("unix_micros", [(None,)])]), TABLE).get_watermark("contacts") is None


def test_get_propagates_a_select_error():
    spark = FakeSpark()
    spark.fail_on = "unix_micros"
    with pytest.raises(RuntimeError, match="simulated failure"):
        StateStore(spark, TABLE).get_watermark("contacts")


def test_get_converts_microseconds_to_utc_without_using_the_session_timezone():
    spark = FakeSpark([("unix_micros", [(micros(WATERMARK),)])])
    assert StateStore(spark, TABLE).get_watermark("contacts") == WATERMARK
    assert spark.statements == [f"SELECT unix_micros(watermark) FROM {TABLE} WHERE object_name = 'contacts'"]


def test_set_watermark_merges_a_success_row_at_the_run_start():
    spark = FakeSpark()
    StateStore(spark, TABLE).set_watermark("contacts", "incremental", 7, RUN_AT)
    assert spark.views["_hubspot_state_update"] == [("contacts", RUN_AT, "incremental", "SUCCESS", 7, RUN_AT)]
    assert spark.schemas == [STATE_DDL]
    assert spark.statements == [
        f"MERGE INTO {TABLE} t USING _hubspot_state_update s ON t.object_name = s.object_name "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    ]


def test_set_failed_keeps_watermark_and_rows():
    spark = FakeSpark()
    StateStore(spark, TABLE).set_failed("deals", "incremental", RUN_AT)
    assert spark.views["_hubspot_state_failed"] == [("deals", None, "incremental", "FAILED", None, RUN_AT)]
    merge = spark.statements[-1]
    assert "WHEN MATCHED THEN UPDATE SET last_mode = s.last_mode, last_status = s.last_status, last_run_at = s.last_run_at" in merge
    assert "watermark = s.watermark" not in merge and "last_rows = s.last_rows" not in merge
    assert merge.endswith("WHEN NOT MATCHED THEN INSERT *")


def test_each_update_issues_one_merge_with_the_latest_values():
    spark = FakeSpark()
    state = StateStore(spark, TABLE)
    later = datetime(2026, 9, 30, 13, 0, tzinfo=timezone.utc)
    state.set_watermark("contacts", "full", 3, RUN_AT)
    state.set_watermark("contacts", "incremental", 5, later)
    assert len([s for s in spark.statements if s.startswith("MERGE INTO")]) == 2
    assert spark.views["_hubspot_state_update"] == [("contacts", later, "incremental", "SUCCESS", 5, later)]


@pytest.mark.parametrize("bad", ["contacts' OR '1'='1", "tickets", "", None])
def test_object_name_is_checked_before_any_sql(bad):
    spark = FakeSpark()
    store = StateStore(spark, TABLE)
    for call in (lambda: store.get_watermark(bad), lambda: store.set_watermark(bad, "full", 1, RUN_AT),
                 lambda: store.set_failed(bad, "full", RUN_AT)):
        with pytest.raises(ValueError, match="unknown object"):
            call()
    assert spark.statements == []
