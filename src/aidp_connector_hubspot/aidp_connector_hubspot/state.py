"""One row per object: its watermark and the outcome of its last run."""

from __future__ import annotations

from datetime import timedelta

from .config import SUPPORTED_OBJECTS
from .records import EPOCH


STATE_DDL = ("object_name STRING, watermark TIMESTAMP, last_mode STRING, last_status STRING, "
             "last_rows BIGINT, last_run_at TIMESTAMP")


def _check(name):
    if name not in SUPPORTED_OBJECTS:
        raise ValueError(f"unknown object {name!r}; expected one of {', '.join(SUPPORTED_OBJECTS)}")


class StateStore:
    """One row per object: the watermark and the status of the last run."""

    def __init__(self, spark, table):
        self.spark, self.table = spark, table

    def ensure(self):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({STATE_DDL}) USING DELTA")

    def get_watermark(self, name):
        _check(name)
        # read microseconds so the result does not depend on the Spark session time zone
        rows = self.spark.sql(f"SELECT unix_micros(watermark) FROM {self.table} "
                              f"WHERE object_name = '{name}'").collect()
        return EPOCH + timedelta(microseconds=int(rows[0][0])) if rows and rows[0][0] is not None else None

    def set_watermark(self, name, mode, count, run_at):
        _check(name)
        self.spark.createDataFrame([(name, run_at, mode, "SUCCESS", count, run_at)], STATE_DDL) \
            .createOrReplaceTempView("_hubspot_state_update")
        self.spark.sql(f"MERGE INTO {self.table} t USING _hubspot_state_update s ON t.object_name = s.object_name "
                       "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

    def set_failed(self, name, mode, run_at):
        _check(name)
        # records the failure but leaves watermark and last_rows as they were
        self.spark.createDataFrame([(name, None, mode, "FAILED", None, run_at)], STATE_DDL) \
            .createOrReplaceTempView("_hubspot_state_failed")
        self.spark.sql(f"MERGE INTO {self.table} t USING _hubspot_state_failed s ON t.object_name = s.object_name "
                       "WHEN MATCHED THEN UPDATE SET last_mode = s.last_mode, last_status = s.last_status, "
                       "last_run_at = s.last_run_at WHEN NOT MATCHED THEN INSERT *")
