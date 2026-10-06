"""Load and validate the connector configuration.

The configuration never holds the HubSpot token. It names where the notebook
should read the token from (an AIDP Credential Store entry, or an environment
variable for local runs).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Mapping, Optional, Tuple

# Run order: associations last, because they are read from the ids in the other tables.
SUPPORTED_OBJECTS: Tuple[str, ...] = ("contacts", "companies", "deals", "associations")
MODES: Tuple[str, ...] = ("incremental", "full")
DEFAULT_API_VERSION = "2026-03"

_IDENTIFIER = re.compile(r"\w+", re.ASCII)
_PREFIX = re.compile(r"\w*", re.ASCII)


class ConfigError(ValueError):
    """The configuration is missing something or holds an invalid value."""


@dataclass(frozen=True)
class HubSpotSettings:
    credential_name: Optional[str] = None
    credential_key: str = "secret"
    token_env: str = "HUBSPOT_TOKEN"
    api_version: str = DEFAULT_API_VERSION
    requests_per_second: float = 4.0


@dataclass(frozen=True)
class TargetSettings:
    catalog: str
    schema: str
    table_prefix: str = ""

    def __post_init__(self):
        # names go straight into SQL, so a hand-built TargetSettings is checked too
        for name, value, pattern in (("target.catalog", self.catalog, _IDENTIFIER),
                                     ("target.schema", self.schema, _IDENTIFIER),
                                     ("target.table_prefix", self.table_prefix, _PREFIX)):
            if not isinstance(value, str) or not pattern.fullmatch(value):
                raise ConfigError(
                    f"{name} must contain only letters, digits and underscores, "
                    f"got {value!r}. Replace any placeholder."
                )

    @property
    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}{name}"


@dataclass(frozen=True)
class SyncSettings:
    objects: Tuple[str, ...]
    mode: str = "incremental"
    overlap_seconds: float = 300


@dataclass(frozen=True)
class Config:
    hubspot: HubSpotSettings
    target: TargetSettings
    sync: SyncSettings


def normalize_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in MODES:
        raise ConfigError(f"mode must be one of {', '.join(MODES)}; got {value!r}")
    return mode


def load_config(path: str) -> Config:
    import yaml

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc.strerror or exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file {path} is not valid YAML: {exc}") from exc
    return parse_config(data)


def parse_config(data: Any) -> Config:
    if not isinstance(data, Mapping):
        raise ConfigError("configuration must be a mapping with 'target' and 'sync' sections")
    unknown = sorted(set(data) - {"hubspot", "target", "sync"})
    if unknown:
        raise ConfigError(f"unknown section(s): {', '.join(map(str, unknown))}")
    hubspot = _section(
        data,
        "hubspot",
        ("credential_name", "credential_key", "token_env", "api_version", "requests_per_second"),
        required=False,
    )
    target = _section(data, "target", ("catalog", "schema", "table_prefix"))
    sync = _section(data, "sync", ("mode", "objects", "overlap_seconds"))
    return Config(hubspot=_hubspot(hubspot), target=_target(target), sync=_sync(sync))


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


def _hubspot(section: Mapping) -> HubSpotSettings:
    rate = section.get("requests_per_second", 4.0)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not rate > 0:
        raise ConfigError("hubspot.requests_per_second must be a number greater than 0")
    credential_name = section.get("credential_name")
    return HubSpotSettings(
        credential_name=str(credential_name) if credential_name else None,
        credential_key=str(section.get("credential_key") or "secret"),
        token_env=str(section.get("token_env") or "HUBSPOT_TOKEN"),
        api_version=str(section.get("api_version") or DEFAULT_API_VERSION),
        requests_per_second=float(rate),
    )


def _identifier(value: Any, name: str, allow_empty: bool = False) -> str:
    if value is None or value == "":
        if allow_empty:
            return ""
        raise ConfigError(f"{name} is required")
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ConfigError(
            f"{name} must contain only letters, digits and underscores, "
            f"got {value!r}. Replace any placeholder."
        )
    return value


def _target(section: Mapping) -> TargetSettings:
    return TargetSettings(
        catalog=_identifier(section.get("catalog"), "target.catalog"),
        schema=_identifier(section.get("schema"), "target.schema"),
        table_prefix=_identifier(section.get("table_prefix"), "target.table_prefix", allow_empty=True),
    )


def _sync(section: Mapping) -> SyncSettings:
    raw_mode = section.get("mode")
    mode = normalize_mode(raw_mode) if raw_mode is not None else "incremental"

    objects = section.get("objects")
    if not isinstance(objects, (list, tuple)) or not objects:
        raise ConfigError(
            f"sync.objects must be a non-empty list drawn from: {', '.join(SUPPORTED_OBJECTS)}"
        )
    for name in objects:
        if name not in SUPPORTED_OBJECTS:
            raise ConfigError(
                f"sync.objects: unknown object {name!r}; supported: {', '.join(SUPPORTED_OBJECTS)}"
            )
    if len(set(objects)) != len(objects):
        raise ConfigError("sync.objects contains a duplicate entry")

    overlap = section.get("overlap_seconds", 300)
    message = f"sync.overlap_seconds must be 0 or more, got {overlap!r}"
    if isinstance(overlap, bool) or not isinstance(overlap, (int, float)) or not overlap >= 0:
        raise ConfigError(message)
    try:
        timedelta(seconds=overlap)  # raises for inf and values too large
    except (OverflowError, ValueError):
        raise ConfigError(message) from None

    return SyncSettings(objects=tuple(objects), mode=mode, overlap_seconds=overlap)
