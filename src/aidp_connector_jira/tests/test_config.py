import pytest

from aidp_connector_jira.config import ConfigError, parse_config


def base(**sync):
    return {
        "jira": {"credential_name": "jira"},
        "target": {"catalog": "cat", "schema": "raw", "table": "issue"},
        "sync": {"jql": "project = KAN", **sync},
    }


def test_defaults():
    cfg = parse_config(base())
    assert cfg.jira.credential_name == "jira"
    assert cfg.target.qualified_table == "cat.raw.issue" and cfg.target.qualified_schema == "cat.raw"
    assert cfg.sync.jql == "project = KAN" and cfg.sync.extra_fields == ()
    assert (cfg.sync.page_size, cfg.sync.overlap_seconds) == (100, 300)


def test_jira_section_is_optional():
    data = base()
    del data["jira"]
    assert parse_config(data).jira.credential_name is None


def test_sync_values_are_read():
    cfg = parse_config(base(extra_fields=["customfield_10016"], page_size=5000, overlap_seconds=0))
    assert cfg.sync.extra_fields == ("customfield_10016",)
    assert (cfg.sync.page_size, cfg.sync.overlap_seconds) == (5000, 0)


@pytest.mark.parametrize("data, message", [
    (None, "mapping"),
    ({**base(), "extra": {}}, "unknown section"),
    ({"jira": {}, "target": base()["target"]}, "missing section 'sync'"),
    ({**base(), "jira": {"credential_name": "jira", "token": "x"}}, "unknown key"),
    ({**base(), "target": {"catalog": "cat", "schema": "raw", "table": "my-table"}}, "target.table"),
    (base(jql=" "), "sync.jql"),
    (base(jql="project = KAN ORDER BY created"), "ORDER BY"),
    (base(extra_fields="customfield_1"), "list"),
    (base(page_size=0), "sync.page_size"),
    (base(page_size=5001), "sync.page_size"),
    (base(page_size=True), "sync.page_size"),
    (base(overlap_seconds=-1), "sync.overlap_seconds"),
])
def test_invalid_configs_are_rejected(data, message):
    with pytest.raises(ConfigError, match=message):
        parse_config(data)
