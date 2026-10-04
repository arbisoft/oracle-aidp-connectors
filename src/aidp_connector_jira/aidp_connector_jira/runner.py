"""Run one sync: read the configured issues and land them in their Delta table."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any, Callable, Dict, Optional, Tuple

from .client import account_timezone, jira_session, search_issues
from .config import Config, normalize_mode
from .records import TYPED_FIELDS, to_dataframe

Credentials = Tuple[str, str, str]


def _fields(config: Config):
    """Typed fields plus ``sync.extra_fields``; the extra ones land in ``raw_fields``."""
    typed = [name for name, _ in TYPED_FIELDS if name not in ("key", "updated")]
    return list(dict.fromkeys([*typed, *config.sync.extra_fields]))


def latest_updated(spark: Any, table: str) -> Optional[datetime]:
    """``max(updated)`` of ``table`` as an aware UTC datetime, or None.

    Read as epoch microseconds: collecting a TIMESTAMP gives a naive datetime
    in the Python process's local timezone, which would shift the watermark.
    """
    micros = spark.sql(f"SELECT unix_micros(max(updated)) AS m FROM {table}").first()["m"]
    if micros is None:
        return None
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)


def preview(spark: Any, config: Config, creds: Credentials, limit: int = 10, *, session: Any = None) -> Any:
    """A DataFrame of the first ``limit`` issues matching ``sync.jql``. Writes
    nothing and fetches only the pages those issues need."""
    site, email, token = creds
    session = session or jira_session(email, token)
    issues = search_issues(session, site, query=config.sync.jql, fields=_fields(config),
                           page_size=min(limit, config.sync.page_size))
    return to_dataframe(spark, islice(issues, limit))


def run(
    spark: Any,
    config: Config,
    creds: Credentials,
    mode_override: Optional[str] = None,
    *,
    session: Any = None,
    now: Optional[Callable[[], datetime]] = None,
    log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    """Load the issues matching ``sync.jql`` into ``target.table`` and return a summary dict.

    The first run reads every matching issue and creates the table. Later runs
    read issues updated since the table's newest ``updated`` value (minus
    ``overlap_seconds``) and MERGE them on ``key``. ``mode_override="full"``
    re-reads every matching issue and overwrites the table, which purges issues
    deleted in Jira. ``None``, empty or ``"incremental"`` is the normal run.
    """
    full = bool(str(mode_override or "").strip()) and normalize_mode(mode_override) == "full"
    sync, table = config.sync, config.target.qualified_table
    site, email, token = creds
    session = session or jira_session(email, token)
    # Jira reads JQL date-times in the account's own timezone. This one request
    # also proves network access and the credentials before anything is written.
    tz_name = account_timezone(session, site)
    log(f"connection ok; account timezone {tz_name}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target.qualified_schema}")
    exists = spark.catalog.tableExists(table)
    since = latest_updated(spark, table) if exists and not full else None
    until = now() if now else datetime.now(timezone.utc)  # fixed for this run

    issues = search_issues(
        session, site, query=sync.jql, fields=_fields(config), since=since, until=until,
        overlap_seconds=sync.overlap_seconds, page_size=sync.page_size, tz_name=tz_name,
    )
    df = to_dataframe(spark, list(issues))
    rows_read = df.count()
    if full or not exists:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    else:
        df.createOrReplaceTempView("incoming_issue")
        spark.sql(
            f"MERGE INTO {table} t USING incoming_issue s ON t.key = s.key "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )

    if full:
        mode = "full refresh"
    elif not exists:
        mode = "full"
    elif since is None:
        mode = "full re-read"  # the table holds no issue yet
    else:
        mode = "incremental"
    return {
        "jql": sync.jql,
        "table": table,
        "mode": mode,
        "since": since.isoformat() if since else None,
        "rows_read": rows_read,
        "rows_in_table": spark.table(table).count(),
    }


def format_summary(summary: Dict[str, Any]) -> str:
    return "\n".join(f"{key:<14}{value}" for key, value in summary.items() if value is not None)
