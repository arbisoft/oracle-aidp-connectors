import pytest

from aidp_connector_jira.auth import read_credentials
from aidp_connector_jira.config import JiraSettings
from aidp_connector_jira.errors import JiraError

STORE = {"site": " https://Example.atlassian.net/ ", "email": "me@example.com\n", "token": "s3cr3t"}
ENV = {"JIRA_SITE": "example.atlassian.net", "JIRA_EMAIL": "me@example.com", "JIRA_API_TOKEN": "s3cr3t"}


def getter(values):
    calls = []

    def get(name, key):
        calls.append((name, key))
        return values[key]

    get.calls = calls
    return get


def test_reads_the_three_keys_from_the_credential_store():
    get = getter(STORE)
    assert read_credentials(JiraSettings("jira"), get, environ={}) == (
        "example.atlassian.net", "me@example.com", "s3cr3t")
    assert get.calls == [("jira", "site"), ("jira", "email"), ("jira", "token")]


def test_falls_back_to_the_environment_without_a_credential_name_or_a_getter():
    expected = ("example.atlassian.net", "me@example.com", "s3cr3t")
    assert read_credentials(JiraSettings(None), getter(STORE), environ=ENV) == expected
    assert read_credentials(JiraSettings("jira"), None, environ=ENV) == expected


def test_missing_environment_variables_are_named_without_echoing_values():
    with pytest.raises(JiraError) as exc:
        read_credentials(JiraSettings(None), environ={**ENV, "JIRA_API_TOKEN": " "})
    assert "JIRA_API_TOKEN" in str(exc.value) and "s3cr3t" not in str(exc.value)
    assert "me@example.com" not in str(exc.value)


def test_a_blank_credential_store_value_is_named_without_echoing_the_others():
    with pytest.raises(JiraError) as exc:
        read_credentials(JiraSettings("jira"), getter({**STORE, "email": ""}), environ={})
    assert "email" in str(exc.value) and "s3cr3t" not in str(exc.value)


def test_a_site_that_is_not_jira_cloud_is_rejected():
    with pytest.raises(ValueError):
        read_credentials(JiraSettings("jira"), getter({**STORE, "site": "jira.example.com"}), environ={})
