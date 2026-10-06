"""Configuration parsing: defaults, unknown keys, and the checks that run before any SQL or request."""

from __future__ import annotations

import pytest

from aidp_connector_hubspot.config import ConfigError, TargetSettings, load_config, normalize_mode, parse_config


def raw(**sections):
    data = {
        "target": {"catalog": "lake", "schema": "crm"},
        "sync": {"objects": ["contacts"]},
    }
    for name, values in sections.items():
        data[name] = {**data.get(name, {}), **values}
    return data


def test_defaults():
    cfg = parse_config(raw())
    assert cfg.hubspot.credential_name is None
    assert cfg.hubspot.credential_key == "secret"
    assert cfg.hubspot.token_env == "HUBSPOT_TOKEN"
    assert cfg.hubspot.api_version == "2026-03"
    assert cfg.hubspot.requests_per_second == 4.0
    assert cfg.target.table_prefix == ""
    assert cfg.target.qualified_schema == "lake.crm"
    assert cfg.target.table("contacts") == "lake.crm.contacts"
    assert cfg.sync.mode == "incremental"
    assert cfg.sync.overlap_seconds == 300
    assert cfg.sync.objects == ("contacts",)


def test_every_key_is_read():
    cfg = parse_config(raw(
        hubspot={"credential_name": "hs", "credential_key": "k", "token_env": "T", "api_version": "2026-04",
                 "requests_per_second": 2},
        target={"table_prefix": "h_"},
        sync={"mode": "full", "overlap_seconds": 60.5, "objects": ["deals", "contacts"]},
    ))
    assert (cfg.hubspot.credential_name, cfg.hubspot.credential_key, cfg.hubspot.token_env) == ("hs", "k", "T")
    assert cfg.hubspot.api_version == "2026-04" and cfg.hubspot.requests_per_second == 2.0
    assert cfg.target.table("deals") == "lake.crm.h_deals"
    assert cfg.sync.mode == "full" and cfg.sync.overlap_seconds == 60.5
    assert cfg.sync.objects == ("deals", "contacts")


def test_hubspot_section_is_optional():
    assert parse_config(raw()).hubspot.token_env == "HUBSPOT_TOKEN"
    assert parse_config({**raw(), "hubspot": None}).hubspot.credential_key == "secret"


def test_load_config_reads_yaml(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("target: {catalog: lake, schema: crm}\nsync: {objects: [companies]}\n", encoding="utf-8")
    assert load_config(str(path)).sync.objects == ("companies",)


def test_missing_config_file_names_the_path(tmp_path):
    path = str(tmp_path / "absent.yaml")
    with pytest.raises(ConfigError, match="absent.yaml") as info:
        load_config(path)
    assert isinstance(info.value.__cause__, OSError)


def test_malformed_yaml_names_the_path(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("target: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="bad.yaml.*not valid YAML") as info:
        load_config(str(path))
    assert info.value.__cause__ is not None


@pytest.mark.parametrize("data", [None, [], "x"])
def test_config_must_be_a_mapping(data):
    with pytest.raises(ConfigError, match="mapping"):
        parse_config(data)


def test_unknown_section_is_rejected():
    with pytest.raises(ConfigError, match=r"unknown section\(s\): notion"):
        parse_config({**raw(), "notion": {}})


@pytest.mark.parametrize("section", ["hubspot", "target", "sync"])
def test_unknown_key_is_rejected(section):
    with pytest.raises(ConfigError, match=rf"unknown key\(s\) in '{section}': typo"):
        parse_config(raw(**{section: {"typo": 1}}))


def test_required_sections_and_values():
    with pytest.raises(ConfigError, match="missing section 'target'"):
        parse_config({"sync": {"objects": ["contacts"]}})
    with pytest.raises(ConfigError, match="missing section 'sync'"):
        parse_config({"target": {"catalog": "a", "schema": "b"}})
    with pytest.raises(ConfigError, match="target.catalog is required"):
        parse_config(raw(target={"catalog": None}))
    with pytest.raises(ConfigError, match="'target' must be a mapping"):
        parse_config({**raw(), "target": "lake.crm"})


@pytest.mark.parametrize("bad", ["<CATALOG>", "a b", "a;b", "a.b", "a-b", 5])
def test_bad_catalog_or_schema_is_refused(bad):
    with pytest.raises(ConfigError, match="Replace any placeholder"):
        parse_config(raw(target={"catalog": bad}))
    with pytest.raises(ConfigError, match="Replace any placeholder"):
        parse_config(raw(target={"schema": bad}))


@pytest.mark.parametrize("bad", ["a; DROP TABLE x", "x`y", "<PREFIX>", "a-b", "a b", "a.b", 5])
def test_bad_prefix_is_refused(bad):
    with pytest.raises(ConfigError, match="table_prefix"):
        parse_config(raw(target={"table_prefix": bad}))


def test_digits_and_underscores_are_valid_identifiers():
    cfg = parse_config(raw(target={"catalog": "c1", "schema": "_s2", "table_prefix": ""}))
    assert cfg.target.table("t") == "c1._s2.t"


@pytest.mark.parametrize("bad", ["", "cdc", "weekly"])
def test_mode_must_be_incremental_or_full(bad):
    with pytest.raises(ConfigError, match="mode must be one of incremental, full"):
        parse_config(raw(sync={"mode": bad}))


def test_mode_is_case_insensitive_like_the_job_parameter():
    assert parse_config(raw(sync={"mode": " FULL "})).sync.mode == "full"
    assert normalize_mode("Incremental") == "incremental"
    with pytest.raises(ConfigError):
        normalize_mode("cdc")


def test_objects_must_be_known():
    with pytest.raises(ConfigError, match="unknown object 'tickets'"):
        parse_config(raw(sync={"objects": ["contacts", "tickets"]}))
    cfg = parse_config(raw(sync={"objects": ["contacts", "companies", "deals", "associations"]}))
    assert len(cfg.sync.objects) == 4


@pytest.mark.parametrize("bad", [None, [], "contacts"])
def test_objects_must_be_a_non_empty_list(bad):
    with pytest.raises(ConfigError, match="non-empty list"):
        parse_config(raw(sync={"objects": bad}))


def test_duplicate_objects_are_refused():
    with pytest.raises(ConfigError, match="duplicate"):
        parse_config(raw(sync={"objects": ["contacts", "contacts"]}))


@pytest.mark.parametrize("bad", [-1, "300", None, True, float("nan"), float("inf"), 1e30, 10 ** 30])
def test_overlap_must_be_a_finite_non_negative_number(bad):
    with pytest.raises(ConfigError, match="overlap_seconds"):
        parse_config(raw(sync={"overlap_seconds": bad}))


def test_overlap_of_zero_is_allowed():
    assert parse_config(raw(sync={"overlap_seconds": 0})).sync.overlap_seconds == 0


@pytest.mark.parametrize("bad", [0, -1, float("nan"), "4", True])
def test_requests_per_second_must_be_positive(bad):
    with pytest.raises(ConfigError, match="requests_per_second"):
        parse_config(raw(hubspot={"requests_per_second": bad}))


@pytest.mark.parametrize("bad", ["", "a; DROP TABLE x", "x`y", "a b", "a.b", "a-b", None])
def test_hand_built_target_settings_are_checked(bad):
    with pytest.raises(ConfigError, match="catalog.*Replace any placeholder"):
        TargetSettings(bad, "s")
    with pytest.raises(ConfigError, match="schema.*Replace any placeholder"):
        TargetSettings("c", bad)


@pytest.mark.parametrize("bad", ["a; DROP", "x`y", "a b", "a.b", "a-b", None])
def test_hand_built_prefix_is_checked(bad):
    with pytest.raises(ConfigError, match="table_prefix.*Replace any placeholder"):
        TargetSettings("c", "s", bad)


def test_empty_prefix_is_allowed_on_hand_built_settings():
    assert TargetSettings("c", "s", "").table("t") == "c.s.t"
