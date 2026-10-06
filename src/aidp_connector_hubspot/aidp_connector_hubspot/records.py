"""HubSpot records -> flat rows, and the column specs of the target tables."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

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
