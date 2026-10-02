"""Run one sync: read the configured collection and land it in its Delta table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from .config import Config
from .reader import check_connection, explain_error, latest_watermark, read_collection, resolve_srv


def run(
    spark: Any,
    config: Config,
    uri: str,
    *,
    now: Optional[Callable[[], datetime]] = None,
    log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    """Load the collection into ``target.table`` and return a summary dict.

    The first run reads the whole collection and creates the table. Later runs
    reuse the table's schema, so a type the sample guesses differently cannot
    change it, and read only documents whose ``watermark_field`` moved past the
    table's newest value (minus ``overlap_seconds``), or the whole collection
    again when there is no watermark field. Both MERGE on ``_id``.
    """
    mongo, sync, table = config.mongodb, config.sync, config.target.qualified_table
    # Executors cannot resolve mongodb+srv:// DNS records (verified on AIDP), so resolve them here, on the driver.
    uri = resolve_srv(spark, uri)
    check_connection(spark, uri, mongo.database, mongo.collection,
                     server_selection_timeout_ms=mongo.server_selection_timeout_ms)
    log("connection ok")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target.qualified_schema}")
    exists = spark.catalog.tableExists(table)
    since, schema = None, None
    if exists:
        schema = spark.table(table).schema
        if sync.watermark_field:
            since = latest_watermark(spark, table, sync.watermark_field)
    until = (now() if now else datetime.now(timezone.utc)) if sync.watermark_field else None  # fixed for this run

    df = read_collection(
        spark, uri, mongo.database, mongo.collection,
        schema=schema, string_fields=sync.string_fields, sample_size=sync.sample_size,
        watermark_field=sync.watermark_field, since=since, until=until, overlap_seconds=sync.overlap_seconds,
        server_selection_timeout_ms=mongo.server_selection_timeout_ms,
    )
    # The MongoDB read happens here, at the write, so errors are explained here too.
    try:
        if not exists:
            df.write.format("delta").saveAsTable(table)
        else:
            df.createOrReplaceTempView("incoming_document")
            spark.sql(
                f"MERGE INTO {table} t USING incoming_document s ON t._id = s._id "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
    except Exception as exc:
        raise explain_error(exc, uri) from None

    if not exists:
        mode = "full"
    elif since is None:
        mode = "full re-read"  # no watermark field, or the table holds no watermark value yet
    else:
        mode = "incremental"
    return {
        "collection": f"{mongo.database}.{mongo.collection}",
        "table": table,
        "mode": mode,
        "since": since.isoformat() if since else None,
        "rows_in_table": spark.table(table).count(),
    }


def format_summary(summary: Dict[str, Any]) -> str:
    return "\n".join(f"{key:<14}{value}" for key, value in summary.items() if value is not None)
