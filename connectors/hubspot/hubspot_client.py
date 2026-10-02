"""HubSpot CRM reader and Delta loader for AIDP notebooks.

Read-only against HubSpot. Uses only ``requests``. The token is passed in by the
caller (the example notebook reads it from the AIDP Credential Store) and is
never logged or put in an error message. Upload this single file to a workspace
folder, put that folder on ``sys.path`` and ``import hubspot_client``; the
example notebook shows the steps.

Loads contacts, companies, deals and their associations into Delta tables:
an incremental run searches ids changed since a per-object watermark, reads the
full records, merges them, and flags archived and merged-away ids; a full run
lists live and archived records and overwrites the table.

Sections: HTTP client, extraction, row mapping, Delta writer, state table,
sync.
"""

from __future__ import annotations

import json
import math
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from itertools import chain

import requests

API_URL = "https://api.hubapi.com"
API_VERSION = "2026-03"
ALL_OBJECTS = ("contacts", "companies", "deals", "associations")
MODIFIED = {"contacts": "lastmodifieddate", "companies": "hs_lastmodifieddate", "deals": "hs_lastmodifieddate"}
ASSOCIATION_PAIRS = [("contacts", "companies"), ("deals", "contacts"), ("deals", "companies")]
SEARCH_CAP, SEARCH_PAGE, READ_BATCH, ASSOC_BATCH, URL_LIMIT = 10000, 200, 100, 1000, 15000
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# --------------------------------------------------------------------------
# HTTP client
# --------------------------------------------------------------------------


class HubSpotError(RuntimeError):
    """Any failure talking to HubSpot. Messages never contain the token."""

    def __init__(self, status, category, message, policy_name=None):
        super().__init__(f"HubSpot API error {status} ({category}): {message}")
        self.status, self.category, self.message, self.policy_name = status, category, message, policy_name


def _backoff(n: int) -> int:
    return min(2 ** (n - 1), 30)


class HubSpotClient:
    """One throttled session with retries. ``session``, ``sleep`` and ``clock`` can be injected."""

    def __init__(self, token, *, session=None, sleep=time.sleep, clock=time.monotonic,
                 requests_per_second=4.0, api_version=API_VERSION, base_url=API_URL, timeout=60,
                 max_attempts=8, max_rate_limit_waits=20):
        if not isinstance(token, str) or not token.strip():
            raise ValueError("token must be a non-empty string")
        if not requests_per_second > 0:
            raise ValueError("requests_per_second must be greater than 0")
        self._headers = {"Authorization": f"Bearer {token.strip()}", "Content-Type": "application/json"}
        self.session = session if session is not None else requests.Session()
        self.sleep, self.clock = sleep, clock
        self.interval = 1.0 / requests_per_second
        self.api_version = api_version
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts, self.max_rate_limit_waits = max_attempts, max_rate_limit_waits
        self._next_call = 0.0

    def call(self, method, path, body=None, params=None):
        """One request with throttle and retries. A 207 is returned: the caller reads ``errors``."""
        failures = waits = 0
        while True:
            gap = self._next_call - self.clock()
            if gap > 0:
                self.sleep(gap)
            self._next_call = self.clock() + self.interval
            try:
                resp = self.session.request(method, self.base_url + path, headers=self._headers,
                                            json=body, params=params, timeout=self.timeout)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    ConnectionError, TimeoutError):
                failures += 1
                if failures >= self.max_attempts:
                    raise
                self.sleep(_backoff(failures))
                continue
            status = resp.status_code
            if status < 400:
                return resp.json()
            try:  # only these fields are kept: other fields can carry portal ids
                err = resp.json()
                category, message, policy = str(err.get("category")), str(err.get("message")), err.get("policyName")
            except (ValueError, AttributeError):
                category, message, policy = "unknown", "response body was not JSON", None
            if status == 429 and policy != "DAILY" and waits < self.max_rate_limit_waits:
                waits += 1
                try:  # HubSpot usually sends no Retry-After; honor it when it is a number of seconds
                    delay = float(resp.headers.get("Retry-After"))
                    if not math.isfinite(delay) or delay < 0:
                        raise ValueError
                    delay = min(delay, 30)
                except (TypeError, ValueError):
                    delay = _backoff(waits)
                self.sleep(delay)
            elif status in (500, 502, 503, 504) and failures < self.max_attempts - 1:
                failures += 1
                self.sleep(_backoff(failures))
            else:
                raise HubSpotError(status, category, message, policy)

    def pages(self, path, params=None):
        """Follow the ``after`` cursor of a list endpoint."""
        cursor, seen = None, set()
        while True:
            page = self.call("GET", path, params=dict(params or {}, limit=100, **({"after": cursor} if cursor else {})))
            yield from page.get("results") or []
            cursor = ((page.get("paging") or {}).get("next") or {}).get("after")
            if not cursor or cursor in seen:  # a missing or repeated cursor ends the loop
                return
            seen.add(cursor)


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------


def objects_path(client, obj):
    return f"/crm/objects/{client.api_version}/{obj}"


def check_207(payload, tolerated):
    for e in payload.get("errors") or []:
        if str(e.get("category")) not in tolerated:
            raise HubSpotError(207, e.get("category"), e.get("message"))


def property_names(client, obj):
    results = client.call("GET", f"/crm/properties/{client.api_version}/{obj}").get("results") or []
    return [p["name"] for p in results if p.get("name")]


def batch_read(client, obj, properties, ids, archived=False):
    body = {"properties": properties, "inputs": [{"id": i} for i in ids]}
    payload = client.call("POST", objects_path(client, obj) + "/batch/read", body=body,
                          params={"archived": "true"} if archived else None)
    check_207(payload, ("OBJECT_NOT_FOUND",))
    return payload.get("results") or []


def read_by_ids(client, obj, properties, ids, archived=False):
    batch = []
    for i in ids:
        batch.append(str(i))
        if len(batch) == READ_BATCH:
            yield from batch_read(client, obj, properties, batch, archived)
            batch = []
    if batch:
        yield from batch_read(client, obj, properties, batch, archived)


def iter_all(client, obj, properties, archived=False):
    flag = "true" if archived else "false"
    joined = ",".join(properties)
    if len(joined) <= URL_LIMIT:
        return client.pages(objects_path(client, obj), {"properties": joined, "archived": flag})
    # too many properties for a URL: list ids only, then fetch the records by batch
    ids = (r["id"] for r in client.pages(objects_path(client, obj), {"archived": flag}))
    return read_by_ids(client, obj, properties, ids, archived)


def iter_archived(client, obj):
    for r in client.pages(objects_path(client, obj), {"archived": "true"}):
        yield str(r["id"]), to_time(r.get("archivedAt"))


def search_ids(client, obj, since, until):
    """Ids with ``since <= modified < until``, paged by keyset on hs_object_id past the 10,000 cap."""
    ms = lambda d: str(int(d.timestamp() * 1000))  # noqa: E731
    prop = MODIFIED[obj]
    window = [{"propertyName": prop, "operator": "GTE", "value": ms(since)},
              {"propertyName": prop, "operator": "LT", "value": ms(until)}]
    last_id = None
    while True:
        filters = window + ([{"propertyName": "hs_object_id", "operator": "GT", "value": str(last_id)}]
                            if last_id else [])
        body = {"filterGroups": [{"filters": filters}], "properties": ["hs_object_id"], "limit": SEARCH_PAGE,
                "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]}
        seen, cursor = 0, None
        while True:
            page = client.call("POST", objects_path(client, obj) + "/search",
                               body=dict(body, after=cursor) if cursor else body)
            for r in page.get("results") or []:
                last_id = int(r["id"])
                seen += 1
                yield str(r["id"])
            cursor = ((page.get("paging") or {}).get("next") or {}).get("after")
            if not cursor or seen >= SEARCH_CAP:
                break
        if seen < SEARCH_CAP:
            return


def iter_modified(client, obj, properties, since, until):
    return read_by_ids(client, obj, properties, search_ids(client, obj, since, until))


def iter_associations(client, from_obj, to_obj, from_ids):
    """Yield (from_id, to_id, association_type) for each link, following per-input cursors."""
    path = f"/crm/associations/{client.api_version}/{from_obj}/{to_obj}/batch/read"
    pending = [{"id": str(i)} for i in from_ids]
    while pending:
        following = []
        for start in range(0, len(pending), ASSOC_BATCH):
            payload = client.call("POST", path, body={"inputs": pending[start:start + ASSOC_BATCH]})
            check_207(payload, ("NO_ASSOCIATIONS_FOUND", "OBJECT_NOT_FOUND"))
            for res in payload.get("results") or []:
                from_id = str(res["from"]["id"])
                for target in res.get("to") or []:
                    for kind in target.get("associationTypes") or []:
                        yield from_id, str(target["toObjectId"]), kind
                cursor = ((res.get("paging") or {}).get("next") or {}).get("after")
                if cursor:
                    following.append({"id": from_id, "after": cursor})
        pending = following


# --------------------------------------------------------------------------
# Row mapping
# --------------------------------------------------------------------------

COMMON = [("id", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
          ("archived", "BOOLEAN"), ("archived_at", "TIMESTAMP")]
TAIL = [("raw_json", "STRING"), ("_ingested_at", "TIMESTAMP")]
COLUMNS = {
    "contacts": COMMON + [("email", "STRING"), ("first_name", "STRING"), ("last_name", "STRING"),
                          ("phone", "STRING"), ("lifecycle_stage", "STRING")] + TAIL,
    "companies": COMMON + [("name", "STRING"), ("domain", "STRING"), ("industry", "STRING")] + TAIL,
    "deals": COMMON + [("deal_name", "STRING"), ("amount", "DECIMAL(38,6)"), ("currency", "STRING"),
                       ("deal_stage", "STRING"), ("pipeline", "STRING"), ("close_date", "TIMESTAMP"),
                       ("owner_id", "STRING")] + TAIL,
    "associations": [("from_object", "STRING"), ("from_id", "STRING"), ("to_object", "STRING"),
                     ("to_id", "STRING"), ("association_type_id", "INT"), ("category", "STRING"),
                     ("label", "STRING"), ("_ingested_at", "TIMESTAMP")],
}
ASSOCIATION_KEY = ["from_object", "from_id", "to_object", "to_id", "association_type_id"]
PROPERTY_MAP = {  # column -> HubSpot property
    "contacts": {"email": "email", "first_name": "firstname", "last_name": "lastname", "phone": "phone",
                 "lifecycle_stage": "lifecyclestage"},
    "companies": {"name": "name", "domain": "domain", "industry": "industry"},
    "deals": {"deal_name": "dealname", "currency": "deal_currency_code", "deal_stage": "dealstage",
              "pipeline": "pipeline", "owner_id": "hubspot_owner_id"},
}


def to_time(value):
    """ISO 8601 string or epoch milliseconds to an aware UTC datetime, else None."""
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        if re.fullmatch(r"-?\d+", text):
            return EPOCH + timedelta(milliseconds=int(text))
        text = re.sub(r"[Zz]$", "+00:00", text)
        text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text, count=1)
        parsed = datetime.fromisoformat(text)
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def to_amount(value):
    """Round to 6 places; None if unparseable or too large for DECIMAL(38,6)."""
    try:
        with localcontext() as ctx:
            ctx.prec = 50  # the default 28 digits would reject a valid DECIMAL(38,6) value
            number = Decimal(str(value).strip()).quantize(Decimal("0.000001"))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number.adjusted() <= 31 else None


def to_text(value):
    return None if value is None else str(value)


def object_row(obj, rec, run_at):
    props = rec.get("properties") if isinstance(rec.get("properties"), dict) else {}
    archived = rec.get("archived") is True
    row = {"id": to_text(rec.get("id")), "created_at": to_time(rec.get("createdAt")),
           "updated_at": to_time(rec.get("updatedAt")), "archived": archived,
           "archived_at": to_time(rec.get("archivedAt")) if archived else None,
           "raw_json": json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
           "_ingested_at": run_at}
    row.update({col: to_text(props.get(prop)) for col, prop in PROPERTY_MAP[obj].items()})
    if obj == "deals":
        row["amount"], row["close_date"] = to_amount(props.get("amount")), to_time(props.get("closedate"))
    return row


def association_row(from_obj, from_id, to_obj, to_id, kind, run_at):
    try:
        type_id = int(kind.get("typeId"))
    except (TypeError, ValueError):
        type_id = None
    return {"from_object": from_obj, "from_id": from_id, "to_object": to_obj, "to_id": to_id,
            "association_type_id": type_id, "category": to_text(kind.get("category")),
            "label": to_text(kind.get("label")), "_ingested_at": run_at}


def merged_ids(rec):
    raw = (rec.get("properties") or {}).get("hs_merged_object_ids")
    return list(dict.fromkeys(p.strip() for p in str(raw or "").split(";") if p.strip()))


# --------------------------------------------------------------------------
# Delta writer
# --------------------------------------------------------------------------


def ddl(columns):
    return ", ".join(f"{n} {t}" for n, t in columns)


class Writer:
    """Stages rows in a Delta table, then MERGEs or INSERT OVERWRITEs them into the target."""

    def __init__(self, spark, catalog, schema, table_prefix="", run_id=None):
        for name, value, pattern in (("catalog", catalog, r"\w+"), ("schema", schema, r"\w+"),
                                     ("table_prefix", table_prefix, r"\w*")):
            if not isinstance(value, str) or not re.fullmatch(pattern, value, re.ASCII):
                raise ValueError(f"{name} must contain only letters, digits and underscores, "
                                 f"got {value!r}. Replace any placeholder.")
        self.spark, self.catalog, self.schema, self.prefix = spark, catalog, schema, table_prefix
        self.run_id = run_id or uuid.uuid4().hex[:8]

    def table_name(self, name):
        return f"{self.catalog}.{self.schema}.{self.prefix}{name}"

    def ensure_schema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

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


# --------------------------------------------------------------------------
# State table
# --------------------------------------------------------------------------

STATE_DDL = ("object_name STRING, watermark TIMESTAMP, last_mode STRING, last_status STRING, "
             "last_rows BIGINT, last_run_at TIMESTAMP")


class StateTable:
    """One row per object: the watermark and the status of the last run."""

    def __init__(self, spark, table):
        self.spark, self.table = spark, table

    def ensure(self):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({STATE_DDL}) USING DELTA")

    def get_watermark(self, name):
        # read microseconds so the result does not depend on the Spark session time zone
        rows = self.spark.sql(f"SELECT unix_micros(watermark) FROM {self.table} "
                              f"WHERE object_name = '{name}'").collect()
        return EPOCH + timedelta(microseconds=int(rows[0][0])) if rows and rows[0][0] is not None else None

    def set_watermark(self, name, mode, count, run_at):
        self.spark.createDataFrame([(name, run_at, mode, "SUCCESS", count, run_at)], STATE_DDL) \
            .createOrReplaceTempView("_hubspot_state_update")
        self.spark.sql(f"MERGE INTO {self.table} t USING _hubspot_state_update s ON t.object_name = s.object_name "
                       "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

    def set_failed(self, name, mode, run_at):
        # records the failure but leaves watermark and last_rows as they were
        self.spark.createDataFrame([(name, None, mode, "FAILED", None, run_at)], STATE_DDL) \
            .createOrReplaceTempView("_hubspot_state_failed")
        self.spark.sql(f"MERGE INTO {self.table} t USING _hubspot_state_failed s ON t.object_name = s.object_name "
                       "WHEN MATCHED THEN UPDATE SET last_mode = s.last_mode, last_status = s.last_status, "
                       "last_run_at = s.last_run_at WHEN NOT MATCHED THEN INSERT *")


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------


def validate_options(objects, mode, overlap_seconds=300):
    if mode not in ("incremental", "full"):
        raise ValueError(f"MODE must be 'incremental' or 'full', got {mode!r}")
    unknown = sorted(set(objects) - set(ALL_OBJECTS))
    if unknown:
        raise ValueError(f"OBJECTS has unknown entries: {unknown}")
    if isinstance(overlap_seconds, bool) or not isinstance(overlap_seconds, (int, float)) or overlap_seconds < 0:
        raise ValueError(f"OVERLAP_SECONDS must be 0 or more, got {overlap_seconds!r}")
    try:
        timedelta(seconds=overlap_seconds)  # raises for NaN, inf and values too large
    except (OverflowError, ValueError):
        raise ValueError(f"OVERLAP_SECONDS must be 0 or more, got {overlap_seconds!r}") from None


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
            log(f"associations: skipping {pair[0]}->{pair[1]}, {pair[0]} is not in OBJECTS")
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


def run_sync(spark, token, catalog, schema, *, table_prefix="", objects=ALL_OBJECTS, mode="incremental",
             overlap_seconds=300, requests_per_second=4.0, api_version=API_VERSION, session=None,
             sleep=time.sleep, clock=time.monotonic, now=None, run_id=None, log=print,
             writer=None, state=None) -> dict:
    """Sync the selected objects. One object failing does not stop the others; raises at the end if any failed.

    Returns the row count per object that succeeded. ``writer`` and ``state`` are test hooks.
    """
    objects = list(objects)
    validate_options(objects, mode, overlap_seconds)
    writer = writer or Writer(spark, catalog, schema, table_prefix, run_id)
    client = HubSpotClient(token, session=session, sleep=sleep, clock=clock,
                           requests_per_second=requests_per_second, api_version=api_version)
    state = state or StateTable(spark, writer.table_name("hubspot_sync_state"))
    writer.ensure_schema()
    state.ensure()
    run_at = now or datetime.now(timezone.utc)  # the next watermark: the run start, not the newest record seen
    counts, failed = {}, []
    for name in [o for o in ALL_OBJECTS if o in objects]:
        run_mode = "full"
        try:
            since = state.get_watermark(name) if name != "associations" else None
            run_mode = "full" if name == "associations" or mode == "full" or since is None else "incremental"
            if name == "associations":
                count = sync_associations(client, writer, objects, run_at, log)
            else:
                count = sync_object(client, writer, name, run_mode, since, run_at, overlap_seconds)
            state.set_watermark(name, run_mode, count, run_at)
        except Exception as exc:  # one object failing must not stop the others
            failed.append(f"{name}: {str(exc)[:1000]}")
            log(f"{name}: FAILED, {failed[-1]}")
            try:
                state.set_failed(name, run_mode, run_at)
            except Exception as state_exc:  # best effort: the failure is already recorded in `failed`
                log(f"{name}: could not record FAILED state, {str(state_exc)[:200]}")
            continue
        counts[name] = count
        log(f"{name}: {count} row(s), {run_mode}, watermark {run_at.isoformat()}")
    if hasattr(state, "table"):
        spark.sql(f"SELECT * FROM {state.table}").show(truncate=False)
    if failed:
        raise RuntimeError("HubSpot sync failed for " + "; ".join(failed))
    return counts
