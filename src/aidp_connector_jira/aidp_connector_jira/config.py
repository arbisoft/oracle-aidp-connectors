"""Load and validate the connector configuration.

The configuration never holds the API token. It names where to read the
credentials from (an AIDP Credential Store entry, or environment variables
for local runs). One configuration loads one JQL query into one table.
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
class JiraSettings:
    credential_name: Optional[str] = None


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
    jql: str
    extra_fields: Tuple[str, ...] = ()
    page_size: int = 100
    overlap_seconds: int = 300


@dataclass(frozen=True)
class Config:
    jira: JiraSettings
    target: TargetSettings
    sync: SyncSettings


def normalize_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in MODES:
        raise ConfigError(f"mode must be one of {', '.join(MODES)}; got {value!r}")
    return mode


_NO_PARAMETER = "\x00__NO_PARAMETER__"


def job_parameter(name: str, get_parameter: Any) -> Optional[str]:
    """A job parameter's value, stripped, or None when it is missing or blank.

    ``get_parameter`` is ``oidlUtils.parameters.getParameter`` on AIDP. Names
    are case-sensitive there, so ``name``, its upper and its lower case are
    tried: whoever registers the job may write 'mode', not 'MODE'.
    """
    for candidate in dict.fromkeys((name, name.upper(), name.lower())):
        value = get_parameter(candidate, _NO_PARAMETER)
        if value is not None and value != _NO_PARAMETER and str(value).strip():
            return str(value).strip()
    return None


def load_config(path: str) -> Config:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        return parse_config(yaml.safe_load(handle))


def parse_config(data: Any) -> Config:
    if not isinstance(data, Mapping):
        raise ConfigError("configuration must be a mapping with 'jira', 'target' and 'sync' sections")
    unknown = sorted(set(data) - {"jira", "target", "sync"})
    if unknown:
        raise ConfigError(f"unknown section(s): {', '.join(map(str, unknown))}")
    jira = _section(data, "jira", ("credential_name",), required=False)
    target = _section(data, "target", ("catalog", "schema", "table"))
    sync = _section(data, "sync", ("jql", "extra_fields", "page_size", "overlap_seconds"))
    credential_name = jira.get("credential_name")
    return Config(
        jira=JiraSettings(credential_name=str(credential_name).strip() if credential_name else None),
        target=TargetSettings(
            catalog=_identifier(target.get("catalog"), "target.catalog"),
            schema=_identifier(target.get("schema"), "target.schema"),
            table=_identifier(target.get("table"), "target.table"),
        ),
        sync=SyncSettings(
            jql=_jql(sync.get("jql")),
            extra_fields=_names(sync, "extra_fields"),
            page_size=_integer(sync.get("page_size", 100), "sync.page_size", 1, 5000),
            overlap_seconds=_integer(sync.get("overlap_seconds", 300), "sync.overlap_seconds", 0),
        ),
    )


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


def _jql(value: Any) -> str:
    jql = _text(value, "sync.jql")
    if re.search(r"order\s+by", jql, re.I):
        raise ConfigError("sync.jql must not contain ORDER BY; the connector adds its own ordering")
    return jql


def _names(section: Mapping, key: str) -> Tuple[str, ...]:
    names = section.get(key) or []
    if not isinstance(names, (list, tuple)):
        raise ConfigError(f"sync.{key} must be a list of field names")
    return tuple(_text(name, f"sync.{key} entry") for name in names)


def _integer(value: Any, name: str, minimum: int, maximum: Optional[int] = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or (
            maximum is not None and value > maximum):
        bounds = f"from {minimum} to {maximum}" if maximum is not None else f"of {minimum} or more"
        raise ConfigError(f"{name} must be an integer {bounds}")
    return value


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(
            f"{name} must contain only letters, digits and underscores and not start with a digit; got {value!r}"
        )
    return value
