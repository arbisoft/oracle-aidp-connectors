"""Test doubles: no network, no Spark. All data is synthetic."""

from __future__ import annotations

import json as _json
from datetime import datetime, timezone

from hubspot_client import API_VERSION, MODIFIED

FAKE_BASE_URL = "https://api.hubapi.com"


class FakeTime:
    """A clock that only moves when something sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text is not None else _json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON body")
        return self._payload


def error_response(status, category="ERROR", message="boom", policy_name=None, headers=None, **extra):
    payload = {"status": "error", "message": message, "category": category}
    if policy_name:
        payload["policyName"] = policy_name
    payload.update(extra)
    return FakeResponse(status, payload, headers=headers)


class FakeSession:
    """Replays queued responses (or raises queued exceptions) and records calls."""

    def __init__(self, responses):
        self._queue = list(responses)
        self.calls = []

    def request(self, method, url, headers=None, json=None, params=None, timeout=None):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "json": json, "params": params, "timeout": timeout}
        )
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ts(minute, hour=10):
    """A HubSpot-style timestamp on 2026-09-30."""
    return f"2026-09-30T{hour:02d}:{minute:02d}:00.000Z"


def make_contact(object_id, modified=None, archived=False, archived_at=None, **props):
    return _make("contacts", object_id, modified, archived, archived_at, {
        "email": f"user{object_id}@example.com", "firstname": "Test", "lastname": f"User{object_id}", **props,
    })


def make_company(object_id, modified=None, archived=False, archived_at=None, **props):
    return _make("companies", object_id, modified, archived, archived_at, {
        "name": f"Company {object_id}", "domain": f"company{object_id}.example.com", **props,
    })


def make_deal(object_id, modified=None, archived=False, archived_at=None, **props):
    return _make("deals", object_id, modified, archived, archived_at, {
        "dealname": f"Deal {object_id}", "amount": "100.50", "dealstage": "appointmentscheduled", **props,
    })


def _make(object_name, object_id, modified, archived, archived_at, props):
    modified = modified or ts(0)
    props = dict(props, hs_object_id=str(object_id), **{MODIFIED[object_name]: modified})
    return {
        "id": str(object_id),
        "properties": props,
        "createdAt": ts(0, hour=8),
        "updatedAt": modified,
        "archived": archived,
        "archivedAt": archived_at,
    }


def assoc_link(to_id, type_id=1, category="HUBSPOT_DEFINED", label=None):
    """One target of an association: (to_id, [association types])."""
    return (str(to_id), [{"category": category, "typeId": type_id, "label": label}])


def _singular(name):
    return "company" if name == "companies" else name.rstrip("s")


def _to_ms(value):
    text = str(value)
    if text.lstrip("-").isdigit():
        return int(text)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int(parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp() * 1000)


class FakeHubSpot:
    """An in-memory HubSpot CRM that answers the endpoints the connector calls.

    Use it as the ``session`` of a HubSpotClient.

    * ``objects`` maps object name to records (see ``make_contact`` and friends).
    * ``associations`` maps ``(from_object, to_object)`` to ``{from_id: [assoc_link(...)]}``.
    * ``search_cap`` is the most results one search query can page through
      (HubSpot: 10,000); a cursor at or past it returns a 400. Tests set it small.
    * ``assoc_page_size`` is how many links one batch-read input returns before
      it hands back its own ``paging.next.after`` cursor.
    * ``errors[path]`` is a FakeResponse (or exception) returned for that path.
    * ``fail_ids[(from_object, to_object)]`` is a set of ids whose association
      read returns a 207 error with the given category in ``fail_category``.
    * ``no_links_category`` is the category of the 207 error for an input with no
      links: the live API's ``OBJECT_NOT_FOUND`` ("No company is associated with
      contact 1."), or set ``NO_ASSOCIATIONS_FOUND``.
    """

    def __init__(self, objects=None, associations=None, properties=None, page_size=100, search_cap=10000,
                 assoc_page_size=500):
        self.objects = {name: list(records) for name, records in (objects or {}).items()}
        self.associations = {pair: dict(links) for pair, links in (associations or {}).items()}
        self.properties = dict(properties or {})
        self.page_size = page_size
        self.search_cap = search_cap
        self.assoc_page_size = assoc_page_size
        self.calls = []
        self.errors = {}
        self.fail_ids = {}
        self.fail_category = "VALIDATION_ERROR"
        self.no_links_category = "OBJECT_NOT_FOUND"

    def request(self, method, url, headers=None, json=None, params=None, timeout=None):
        path = url.replace(FAKE_BASE_URL, "")
        self.calls.append((method, path, json, params))
        if path in self.errors:
            item = self.errors[path]
            if isinstance(item, Exception):
                raise item
            return item
        parts = path.strip("/").split("/")
        head = "/".join(parts[:3])
        if head == f"crm/objects/{API_VERSION}" and len(parts) >= 4:
            name = parts[3]
            if name not in self.objects:
                return error_response(404, "OBJECT_NOT_FOUND", f"unknown object {name}")
            if len(parts) == 4 and method == "GET":
                return self._list(name, params or {})
            if parts[4:] == ["search"]:
                return self._search(name, json or {})
            if parts[4:] == ["batch", "read"]:
                self._query = params or {}  # read by _batch_read, whose signature tests monkeypatch
                return self._batch_read(name, json or {})
        if head == f"crm/properties/{API_VERSION}" and len(parts) == 4:
            names = self.properties.get(parts[3])
            if names is None:
                seen = set()
                for record in self.objects.get(parts[3], []):
                    seen.update(record["properties"])
                names = sorted(seen)
            return FakeResponse(200, {"results": [{"name": n} for n in names]})
        if head == f"crm/associations/{API_VERSION}" and len(parts) == 7 and parts[5:] == ["batch", "read"]:
            return self._assoc_read(parts[3], parts[4], json or {})
        return error_response(404, "NOT_FOUND", f"no route for {path}")

    @staticmethod
    def _shape(record, wanted):
        props = record["properties"]
        if wanted:
            props = {k: v for k, v in props.items() if k in wanted}
        out = {k: v for k, v in record.items() if k != "properties"}
        out["properties"] = dict(props)
        return out

    def _list(self, name, args):
        archived = str(args.get("archived", "false")).lower() == "true"
        wanted = [p for p in str(args.get("properties") or "").split(",") if p]
        pool = sorted(
            (r for r in self.objects[name] if bool(r["archived"]) == archived), key=lambda r: int(r["id"])
        )
        size = min(int(args.get("limit", 100)), self.page_size)
        start = int(args.get("after") or 0)
        chunk = pool[start:start + size]
        payload = {"results": [self._shape(r, wanted) for r in chunk]}
        if start + size < len(pool):
            payload["paging"] = {"next": {"after": str(start + size)}}
        return FakeResponse(200, payload)

    def _search(self, name, body):
        """Ids only, live records only, filtered and sorted like HubSpot's search."""
        matches = [r for r in self.objects[name] if not r["archived"]]
        for group in body.get("filterGroups", [])[:1]:
            for flt in group.get("filters", []):
                matches = [r for r in matches if self._passes(name, r, flt)]
        matches.sort(key=lambda r: int(r["id"]))  # only hs_object_id ascending is modelled
        limit = min(int(body.get("limit", 10)), 200)
        start = int(body.get("after") or 0)
        if start >= self.search_cap:
            return error_response(400, "VALIDATION_ERROR", "Search results are limited to the first results")
        chunk = matches[start:min(start + limit, self.search_cap)]
        payload = {"total": len(matches), "results": [{"id": r["id"]} for r in chunk]}
        # Like HubSpot, a cursor is offered while matches remain; following it past the cap is the 400 above.
        if start + len(chunk) < len(matches):
            payload["paging"] = {"next": {"after": str(start + len(chunk))}}
        return FakeResponse(200, payload)

    @staticmethod
    def _passes(name, record, flt):
        prop, op, raw = flt["propertyName"], flt["operator"], flt["value"]
        actual = record["properties"].get(prop)
        if actual is None:
            return False
        if prop == "hs_object_id":
            left, right = int(actual), int(raw)
        else:
            left, right = _to_ms(actual), _to_ms(raw)
        return {"GT": left > right, "GTE": left >= right, "LT": left < right, "LTE": left <= right,
                "EQ": left == right}[op]

    def _batch_read(self, name, body):
        # Like HubSpot: archived records are only read when ?archived=true is on the URL.
        archived = self._query.get("archived") == "true"
        by_id = {r["id"]: r for r in self.objects[name] if bool(r.get("archived")) == archived}
        wanted = list(body.get("properties") or [])
        ids = [str(i["id"]) for i in body.get("inputs", [])]
        found = [self._shape(by_id[i], wanted) for i in ids if i in by_id]
        missing = [i for i in ids if i not in by_id]
        if missing:
            errors = [{"status": "error", "category": "OBJECT_NOT_FOUND", "context": {"ids": missing}}]
            return FakeResponse(207, {"status": "COMPLETE_WITH_ERRORS", "results": found, "errors": errors})
        return FakeResponse(200, {"status": "COMPLETE", "results": found})

    def _assoc_read(self, from_object, to_object, body):
        inputs = body.get("inputs", [])
        if len(inputs) > 1000:
            return error_response(400, "VALIDATION_ERROR", "too many inputs")
        links = self.associations.get((from_object, to_object), {})
        failing = set(self.fail_ids.get((from_object, to_object), ()))
        results, empty, failed = [], [], []
        for item in inputs:
            from_id = str(item["id"])
            if from_id in failing:
                failed.append(from_id)
                continue
            targets = links.get(from_id, [])
            if not targets:
                empty.append(from_id)
                continue
            start = int(item.get("after") or 0)
            chunk = targets[start:start + self.assoc_page_size]
            entry = {
                "from": {"id": from_id},
                "to": [{"toObjectId": int(t), "associationTypes": list(types)} for t, types in chunk],
            }
            if start + self.assoc_page_size < len(targets):
                entry["paging"] = {"next": {"after": str(start + self.assoc_page_size)}}
            results.append(entry)
        errors = []
        for from_id in empty:
            message = f"No {_singular(to_object)} is associated with {_singular(from_object)} {from_id}."
            errors.append({"status": "error", "category": self.no_links_category, "message": message,
                           "context": {"ids": [from_id]}})
        if failed:
            errors.append({"status": "error", "category": self.fail_category, "context": {"ids": failed}})
        if errors:
            return FakeResponse(207, {"status": "COMPLETE_WITH_ERRORS", "results": results, "errors": errors})
        return FakeResponse(200, {"status": "COMPLETE", "results": results})

    def calls_to(self, suffix):
        """Bodies of the POST calls whose path ends with ``suffix``."""
        return [body for method, path, body, _ in self.calls if path.endswith(suffix)]


def client_for(fake, **kwargs):
    from hubspot_client import HubSpotClient

    fake_time = FakeTime()
    return HubSpotClient("test-token", session=fake, sleep=fake_time.sleep, clock=fake_time.clock, **kwargs)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return list(self._rows)

    def show(self, **kwargs):
        pass


class _FakeWriter:
    def __init__(self, spark, rows):
        self._spark = spark
        self._rows = rows
        self._mode = None

    def format(self, _name):
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def saveAsTable(self, table):
        self._spark.saved.append((table, self._rows, self._mode))


class _FakeFrame:
    def __init__(self, spark, rows, schema):
        self._spark = spark
        self.rows = rows
        self.schema = schema

    @property
    def write(self):
        return _FakeWriter(self._spark, self.rows)

    def createOrReplaceTempView(self, name):
        self._spark.views[name] = self.rows


class FakeSpark:
    """Records SQL and DataFrame writes. ``results`` is a list of
    (substring, rows): the first entry whose substring occurs in a statement
    supplies what ``.collect()`` returns for it. ``fail_on`` makes any statement
    containing that text raise."""

    def __init__(self, results=()):
        self.statements = []
        self.saved = []
        self.views = {}
        self.schemas = []
        self.fail_on = None
        self._results = list(results)

    def sql(self, statement):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError(f"simulated failure: {self.fail_on}")
        for fragment, rows in self._results:
            if fragment in statement:
                return _FakeResult(rows)
        return _FakeResult([])

    def createDataFrame(self, data, schema=None):
        self.schemas.append(schema)
        return _FakeFrame(self, list(data), schema)


def _dedupe(rows, key, order_by):
    best = {}
    for row in rows:
        identity = tuple(row[column] for column in key)
        current = best.get(identity)
        if current is None or row[order_by] >= current[order_by]:
            best[identity] = row
    return list(best.values())


class InMemoryWriter:
    """Same methods and semantics as hubspot_client.Writer, holding tables as lists of dicts.

    ``spark`` answers ``SELECT id FROM <table> WHERE NOT archived`` from those tables, as
    sync_associations asks it to; a table that was never written raises.
    """

    catalog, schema, prefix = "lake", "crm", ""

    def __init__(self):
        self.tables = {}
        self.fail_on = set()
        self.operations = []
        self.order_by = {}
        self.spark = self

    def table_name(self, name):
        return f"lake.crm.{name}"

    def ensure_schema(self):
        pass

    # the minimal spark surface used by sync_associations
    def sql(self, statement):
        name = statement.split("FROM lake.crm.")[1].split(" ")[0]
        if name not in self.tables:
            raise RuntimeError(f"Table or view not found: lake.crm.{name}")
        rows = [(r["id"],) for r in self.tables[name] if not r["archived"]]
        return _FakeResult(rows)

    def write_table(self, name, rows, key=("id",), order_by="updated_at", overwrite=False, columns=None):
        from hubspot_client import COLUMNS

        rows = list(rows)
        expected = [column for column, _ in COLUMNS[name]]
        for row in rows:
            assert set(row) == set(expected), f"{name}: row keys do not match the declared columns"
        if name in self.fail_on:
            raise RuntimeError(f"write to {name} failed")
        self.operations.append(("overwrite" if overwrite else "merge", name, len(rows)))
        self.order_by[name] = order_by
        incoming = _dedupe(rows, key, order_by)
        if overwrite:
            self.tables[name] = incoming
        else:
            touched = {tuple(row[column] for column in key) for row in incoming}
            kept = [r for r in self.tables.get(name, []) if tuple(r[column] for column in key) not in touched]
            self.tables[name] = kept + incoming
        return len(rows)

    def mark_archived(self, name, pairs):
        """Flag matching ids archived; touches no other column, adds no rows."""
        if name in self.fail_on:
            raise RuntimeError(f"write to {name} failed")
        stamps = {object_id: archived_at for object_id, archived_at in pairs}
        self.operations.append(("mark_archived", name, len(stamps)))
        for row in self.tables.get(name, []):
            if row["id"] in stamps:
                row["archived"] = True
                row["archived_at"] = stamps[row["id"]]

    def ids(self, name):
        return sorted(row["id"] for row in self.tables.get(name, []))


class InMemoryState:
    def __init__(self):
        self.rows = {}
        self.ensured = False

    def ensure(self):
        self.ensured = True

    def get_watermark(self, object_name):
        row = self.rows.get(object_name)
        return row["watermark"] if row else None

    def set_watermark(self, object_name, mode, rows, run_at):
        self.rows[object_name] = {
            "watermark": run_at, "last_mode": mode, "last_status": "SUCCESS", "last_rows": rows,
            "last_run_at": run_at,
        }

    def set_failed(self, object_name, mode, run_at):
        old = self.rows.get(object_name, {})
        self.rows[object_name] = {
            "watermark": old.get("watermark"), "last_mode": mode, "last_status": "FAILED",
            "last_rows": old.get("last_rows"), "last_run_at": run_at,
        }
