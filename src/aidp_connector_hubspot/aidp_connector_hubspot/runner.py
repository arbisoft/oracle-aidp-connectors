"""Run one sync: decide full or incremental per object, extract, write, record state."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Any, Callable, Dict, List, Optional

from .client import HubSpotClient
from .config import SUPPORTED_OBJECTS, Config, normalize_mode
from .extract import iter_all, iter_archived, iter_associations, iter_modified, property_names
from .load import Writer
from .records import ASSOCIATION_KEY, association_row, merged_ids, object_row
from .state import StateStore

STATE_TABLE = "hubspot_sync_state"
ASSOCIATION_PAIRS = [("contacts", "companies"), ("deals", "contacts"), ("deals", "companies")]
_MAX_ERROR_LENGTH = 1000


def run(
    spark: Any,
    config: Config,
    token: str,
    mode_override: Optional[str] = None,
    *,
    client: Any = None,
    writer: Any = None,
    state: Any = None,
    now: Optional[Callable[[], datetime]] = None,
    log: Callable[[str], None] = print,
) -> List[Dict[str, Any]]:
    """Sync the configured objects and return one summary dict per object.

    One object failing does not stop the others; call ``raise_on_failure`` on
    the result to fail the job. ``client``, ``writer``, ``state`` and ``now``
    exist for tests; leave them unset on a cluster.
    """
    mode = config.sync.mode
    if mode_override is not None and str(mode_override).strip():
        mode = normalize_mode(mode_override)
    if client is None:
        client = HubSpotClient(
            token,
            requests_per_second=config.hubspot.requests_per_second,
            api_version=config.hubspot.api_version,
        )
    writer = writer or Writer(spark, config.target)
    state = state or StateStore(spark, writer.table_name(STATE_TABLE))
    writer.ensure_schema()
    state.ensure()
    # the next watermark is the run start, not the newest record seen
    run_at = now() if now else datetime.now(timezone.utc)
    summaries = [
        _sync_one(client, writer, state, config, name, mode, run_at, log)
        for name in SUPPORTED_OBJECTS
        if name in config.sync.objects
    ]
    if hasattr(state, "table"):
        spark.sql(f"SELECT * FROM {state.table}").show(truncate=False)
    return summaries


def format_summary(summaries: List[Dict[str, Any]]) -> str:
    lines = [f"{'object':<14}{'mode':<13}{'status':<9}{'rows':>8}  watermark / note / error"]
    for item in summaries:
        detail = " | ".join(str(part) for part in (item["watermark"], item["note"], item["error"]) if part)
        lines.append(f"{item['object']:<14}{item['mode']:<13}{item['status']:<9}{item['rows']:>8}  {detail}")
    return "\n".join(lines)


def raise_on_failure(summaries: List[Dict[str, Any]]) -> None:
    failed = [f"{item['object']}: {item['error']}" for item in summaries if item["status"] != "SUCCESS"]
    if failed:
        raise RuntimeError("HubSpot sync failed for " + "; ".join(failed))


def _summary(name, mode, status, rows, watermark, note, error) -> Dict[str, Any]:
    return {
        "object": name,
        "mode": mode,
        "status": status,
        "rows": rows,
        "watermark": watermark.isoformat() if watermark else None,
        "note": note,
        "error": error,
    }


def _sync_one(client, writer, state, config, name, mode, run_at, log) -> Dict[str, Any]:
    run_mode, since, note = "full", None, None
    try:
        if name == "associations":
            note = "re-read in full on every run"
        else:
            since = state.get_watermark(name)
            if mode == "incremental" and since is None:
                note = "first run"
            elif mode == "incremental":
                run_mode = "incremental"
        if name == "associations":
            count = sync_associations(client, writer, config.sync.objects, run_at, log)
        else:
            count = sync_object(client, writer, name, run_mode, since, run_at, config.sync.overlap_seconds)
        state.set_watermark(name, run_mode, count, run_at)
    except Exception as exc:  # one object failing must not stop the others
        error = (str(exc) or type(exc).__name__)[:_MAX_ERROR_LENGTH]
        log(f"{name}: FAILED, {error}")
        try:
            state.set_failed(name, run_mode, run_at)
        except Exception as state_exc:  # best effort: the failure is already in the summary
            log(f"{name}: could not record FAILED state, {str(state_exc)[:200]}")
        if since is None:
            try:
                since = state.get_watermark(name)  # report the stored watermark, best effort
            except Exception:
                pass
        return _summary(name, run_mode, "FAILED", 0, since, note, error)
    log(f"{name}: {count} row(s), {run_mode}, watermark {run_at.isoformat()}")
    return _summary(name, run_mode, "SUCCESS", count, run_at, note, None)


def sync_object(client, writer, name, mode, since, run_at, overlap_seconds=300):
    properties = property_names(client, name)
    if mode == "full":
        records = chain(iter_all(client, name, properties), iter_all(client, name, properties, archived=True))
        return writer.write_table(name, (object_row(name, r, run_at) for r in records), overwrite=True)
    merged = []

    def rows():
        for r in iter_modified(client, name, properties, since - timedelta(seconds=overlap_seconds), run_at):
            merged.extend(merged_ids(r))
            yield object_row(name, r, run_at)

    count = writer.write_table(name, rows())
    # after the merge, so the table exists; merged-away ids vanish from HubSpot without being archived
    writer.mark_archived(name, list(iter_archived(client, name)) + [(i, run_at) for i in merged])
    return count


def sync_associations(client, writer, objects, run_at, log=print):
    sources, missing = {}, []
    for pair in ASSOCIATION_PAIRS:
        if pair[0] not in objects:
            log(f"associations: skipping {pair[0]}->{pair[1]}, {pair[0]} is not in sync.objects")
            continue
        try:
            sources[pair] = [str(r[0]) for r in writer.spark.sql(
                f"SELECT id FROM {writer.table_name(pair[0])} WHERE NOT archived").collect()]
        except Exception:
            missing.append(f"{pair[0]}->{pair[1]}: source table {writer.table_name(pair[0])} "
                           "cannot be read, sync it first")

    def rows():
        for (from_obj, to_obj), ids in sources.items():
            for from_id, to_id, kind in iter_associations(client, from_obj, to_obj, ids):
                yield association_row(from_obj, from_id, to_obj, to_id, kind, run_at)

    # a full overwrite would drop the old links of a skipped or failed pair, so a partial run only merges
    count = writer.write_table("associations", rows(), key=ASSOCIATION_KEY, order_by="_ingested_at",
                               overwrite=len(sources) == len(ASSOCIATION_PAIRS))
    if missing:
        raise RuntimeError("; ".join(missing) + f" ({count} row(s) merged for the other pairs)")
    return count
