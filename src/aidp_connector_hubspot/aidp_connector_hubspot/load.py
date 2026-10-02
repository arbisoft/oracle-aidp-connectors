"""Write rows to Delta tables in the target catalog and schema.

Rows are staged in a Delta table, then moved into the target by one MERGE or
INSERT OVERWRITE. Table and column names come from validated configuration and
from constants in this package, never from HubSpot data. Data values travel
through DataFrames.
"""

from __future__ import annotations

import uuid

from .records import COLUMNS


def ddl(columns):
    return ", ".join(f"{n} {t}" for n, t in columns)


class Writer:
    """Stages rows in a Delta table, then MERGEs or INSERT OVERWRITEs them into the target."""

    def __init__(self, spark, target, run_id=None):
        self.spark, self.target = spark, target
        self.run_id = run_id or uuid.uuid4().hex[:8]

    def table_name(self, name):
        return self.target.table(name)

    def ensure_schema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.target.qualified_schema}")

    def stage(self, staging, rows, columns, batch_size=5000):
        self.spark.sql(f"DROP TABLE IF EXISTS {staging}")
        self.spark.sql(f"CREATE TABLE {staging} ({ddl(columns)}) USING DELTA")
        names, total, batch = [n for n, _ in columns], 0, []
        for row in rows:
            batch.append(tuple(row[n] for n in names))
            if len(batch) >= batch_size:
                self._append(staging, batch, columns)
                total, batch = total + len(batch), []
        if batch:
            self._append(staging, batch, columns)
            total += len(batch)
        return total

    def _append(self, staging, batch, columns):
        self.spark.createDataFrame(batch, ddl(columns)).write.format("delta").mode("append").saveAsTable(staging)

    def write_table(self, name, rows, key=("id",), order_by="updated_at", overwrite=False, columns=None):
        table, staging = self.table_name(name), f"{self.table_name(name)}__staging_{self.run_id}"
        columns = columns or COLUMNS[name]
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl(columns)}) USING DELTA")
        try:
            count = self.stage(staging, rows, columns)
            # Delta rejects a MERGE whose source holds two rows for one key, so keep the newest
            source = (f"SELECT {', '.join(n for n, _ in columns)} FROM (SELECT *, ROW_NUMBER() OVER "
                      f"(PARTITION BY {', '.join(key)} ORDER BY {order_by} DESC) AS _rn FROM {staging}) "
                      "WHERE _rn = 1")
            if overwrite:
                self.spark.sql(f"INSERT OVERWRITE TABLE {table} {source}")
            else:
                on = " AND ".join(f"t.{k} = s.{k}" for k in key)
                self.spark.sql(f"MERGE INTO {table} t USING ({source}) s ON {on} "
                               "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS {staging}")
        return count

    def mark_archived(self, name, pairs):
        """Flag ids archived, touching no other column and adding no rows."""
        table, staging = self.table_name(name), f"{self.table_name(name)}__archived_{self.run_id}"
        columns = [("id", "STRING"), ("archived_at", "TIMESTAMP")]
        try:
            if self.stage(staging, ({"id": i, "archived_at": at} for i, at in pairs), columns) == 0:
                return
            source = ("SELECT id, archived_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id "
                      f"ORDER BY archived_at DESC NULLS LAST) AS _rn FROM {staging}) WHERE _rn = 1")
            self.spark.sql(f"MERGE INTO {table} t USING ({source}) s ON t.id = s.id "
                           "WHEN MATCHED THEN UPDATE SET archived = true, archived_at = s.archived_at")
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS {staging}")
