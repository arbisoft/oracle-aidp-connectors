"""Load and validate the connector configuration.

The configuration never holds the connection URI: it holds a password. It
names where to read the URI from (an AIDP Credential Store entry, or an
environment variable for local runs). One configuration loads one collection.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MODES = ("incremental", "full")


class ConfigError(ValueError):
    """The configuration is missing something or holds an invalid value."""


@dataclass(frozen=True)
class MongoSettings:
    database: str
    collection: str
    credential_name: Optional[str] = None
    credential_key: str = "uri"
    uri_env: str = "MONGODB_URI"
    server_selection_timeout_ms: int = 10000


@dataclass(frozen=True)
class TargetSettings:
    catalog: str
    schema: str
    table: str

    @property
    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def qualified_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class SyncSettings:
    watermark_field: Optional[str] = None
    string_fields: Tuple[str, ...] = ()
    fields: Tuple[str, ...] = ()
    sample_size: int = 10000
    overlap_seconds: int = 300

    @property
    def columns(self) -> Tuple[str, ...]:
        """Top-level fields to load, or () for all. ``_id``, ``string_fields`` and
        ``watermark_field`` are always kept: the merge and the watermark need them."""
        if not self.fields:
            return ()
        extra = (self.watermark_field,) if self.watermark_field else ()
        return tuple(dict.fromkeys(("_id", *self.fields, *self.string_fields, *extra)))


@dataclass(frozen=True)
class Config:
    mongodb: MongoSettings
    target: TargetSettings
    sync: SyncSettings


def normalize_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in MODES:
        raise ConfigError(f"mode must be one of {', '.join(MODES)}; got {value!r}")
    return mode


def load_config(path: str) -> Config:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        return parse_config(yaml.safe_load(handle))


def parse_config(data: Any) -> Config:
    if not isinstance(data, Mapping):
        raise ConfigError("configuration must be a mapping with 'mongodb', 'target' and 'sync' sections")
    unknown = sorted(set(data) - {"mongodb", "target", "sync"})
    if unknown:
        raise ConfigError(f"unknown section(s): {', '.join(map(str, unknown))}")
    mongodb = _section(
        data,
        "mongodb",
        ("database", "collection", "credential_name", "credential_key", "uri_env", "server_selection_timeout_ms"),
    )
    target = _section(data, "target", ("catalog", "schema", "table"))
    sync = _section(data, "sync", ("watermark_field", "string_fields", "fields", "sample_size", "overlap_seconds"),
                    required=False)
    return Config(mongodb=_mongodb(mongodb), target=_target(target), sync=_sync(sync))


def _section(data: Mapping, name: str, allowed: Tuple[str, ...], required: bool = True) -> Mapping:
    value = data.get(name)
    if value is None:
        if required:
            raise ConfigError(f"missing section '{name}'")
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"'{name}' must be a mapping")
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise ConfigError(f"unknown key(s) in '{name}': {', '.join(map(str, unknown))}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} is required and must be a non-empty string")
    return value.strip()


def _integer(value: Any, name: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{name} must be an integer of {minimum} or more")
    return value


def _field_name(value: Any, name: str) -> str:
    # Used in SQL as `field`, so a backtick would break out of the quoting.
    value = _text(value, name)
    if "`" in value:
        raise ConfigError(f"{name} must not contain a backtick")
    return value


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(
            f"{name} must contain only letters, digits and underscores and not start with a digit; got {value!r}"
        )
    return value


def _names(section: Mapping, key: str) -> Tuple[str, ...]:
    names = section.get(key) or []
    if not isinstance(names, (list, tuple)):
        raise ConfigError(f"sync.{key} must be a list of field names")
    return tuple(_text(name, f"sync.{key} entry") for name in names)


def _mongodb(section: Mapping) -> MongoSettings:
    credential_name = section.get("credential_name")
    return MongoSettings(
        database=_text(section.get("database"), "mongodb.database"),
        collection=_text(section.get("collection"), "mongodb.collection"),
        credential_name=str(credential_name) if credential_name else None,
        credential_key=str(section.get("credential_key") or "uri"),
        uri_env=str(section.get("uri_env") or "MONGODB_URI"),
        server_selection_timeout_ms=_integer(
            section.get("server_selection_timeout_ms", 10000), "mongodb.server_selection_timeout_ms", 1),
    )


def _target(section: Mapping) -> TargetSettings:
    return TargetSettings(
        catalog=_identifier(section.get("catalog"), "target.catalog"),
        schema=_identifier(section.get("schema"), "target.schema"),
        table=_identifier(section.get("table"), "target.table"),
    )


def _sync(section: Mapping) -> SyncSettings:
    watermark = section.get("watermark_field")
    return SyncSettings(
        watermark_field=_field_name(watermark, "sync.watermark_field") if watermark is not None else None,
        string_fields=_names(section, "string_fields"),
        fields=_names(section, "fields"),
        sample_size=_integer(section.get("sample_size", 10000), "sync.sample_size", 1),
        overlap_seconds=_integer(section.get("overlap_seconds", 300), "sync.overlap_seconds", 0),
    )
