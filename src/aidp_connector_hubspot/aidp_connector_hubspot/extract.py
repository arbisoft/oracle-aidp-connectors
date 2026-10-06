"""Generators over HubSpot: id search, batch read, listing, archived ids and associations."""

from __future__ import annotations

from .client import HubSpotError
from .records import to_time

MODIFIED = {"contacts": "lastmodifieddate", "companies": "hs_lastmodifieddate", "deals": "hs_lastmodifieddate"}
SEARCH_CAP, SEARCH_PAGE, READ_BATCH, ASSOC_BATCH, URL_LIMIT = 10000, 200, 100, 1000, 15000


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
