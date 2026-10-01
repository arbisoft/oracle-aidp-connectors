"""run_sync with the real Writer and StateTable on FakeSpark: the whole SQL script of one run."""

from __future__ import annotations

from datetime import datetime, timezone

from fakes import FakeHubSpot, FakeSpark, FakeTime, make_contact, ts
from hubspot_client import run_sync

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def run(spark, fake, **kwargs):
    t = FakeTime()
    return run_sync(spark, "synthetic-token", "lake", "crm", objects=["contacts"], session=fake, sleep=t.sleep,
                    clock=t.clock, requests_per_second=1000, now=NOW, run_id="r1", log=lambda m: None, **kwargs)


def test_first_run_script_creates_schema_and_state_then_overwrites_and_records_success():
    spark = FakeSpark()
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5)), make_contact(2, ts(6))]})
    assert run(spark, fake) == {"contacts": 2}
    s = spark.statements
    assert s[0] == "CREATE SCHEMA IF NOT EXISTS lake.crm"
    assert s[1].startswith("CREATE TABLE IF NOT EXISTS lake.crm.hubspot_sync_state (object_name STRING")
    assert s[2] == "SELECT unix_micros(watermark) FROM lake.crm.hubspot_sync_state WHERE object_name = 'contacts'"
    assert s[3].startswith("CREATE TABLE IF NOT EXISTS lake.crm.contacts (")
    assert s[-2].startswith("MERGE INTO lake.crm.hubspot_sync_state")
    assert s[-1] == "SELECT * FROM lake.crm.hubspot_sync_state"
    assert any(x.startswith("INSERT OVERWRITE TABLE lake.crm.contacts ") for x in s)
    assert not any(x.startswith("MERGE INTO lake.crm.contacts") for x in s)
    assert spark.views["_hubspot_state_update"] == [("contacts", NOW, "full", "SUCCESS", 2, NOW)]
    staged = [row for table, batch, _ in spark.saved if table == "lake.crm.contacts__staging_r1" for row in batch]
    assert len(staged) == 2 and staged[0][0] == "1"


def test_second_run_with_a_watermark_merges_then_flags_archived():
    micros = int(datetime(2026, 9, 30, 11, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000
    spark = FakeSpark([("unix_micros", [(micros,)])])
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5, hour=11)), make_contact(2, ts(0, hour=9))]})
    assert run(spark, fake) == {"contacts": 1}
    merges = [x for x in spark.statements if x.startswith("MERGE INTO lake.crm.contacts ")]
    archived = [x for x in spark.statements if "lake.crm.contacts t USING" in x and "SET archived = true" in x]
    assert len(merges) == 1 and archived == []  # no archived ids in HubSpot: nothing staged, no flag merge
    assert not any(x.startswith("INSERT OVERWRITE") for x in spark.statements)
    assert spark.views["_hubspot_state_update"] == [("contacts", NOW, "incremental", "SUCCESS", 1, NOW)]


def test_a_failure_records_failed_state_and_raises_after_the_summary():
    import pytest

    spark = FakeSpark()
    spark.fail_on = "INSERT OVERWRITE"
    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))]})
    with pytest.raises(RuntimeError, match="HubSpot sync failed for contacts: simulated failure"):
        run(spark, fake)
    assert spark.views["_hubspot_state_failed"] == [("contacts", None, "full", "FAILED", None, NOW)]
    assert "_hubspot_state_update" not in spark.views
    assert "DROP TABLE IF EXISTS lake.crm.contacts__staging_r1" in spark.statements
    assert spark.statements[-1] == "SELECT * FROM lake.crm.hubspot_sync_state"
