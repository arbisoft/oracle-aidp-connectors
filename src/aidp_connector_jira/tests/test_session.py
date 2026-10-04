import pytest

from aidp_connector_jira import client as j


def test_normalize_site_strips_scheme_and_trailing_slash():
    assert j.normalize_site("https://example.atlassian.net/") == "example.atlassian.net"
    assert j.normalize_site("HTTP://example.atlassian.net") == "example.atlassian.net"
    assert j.normalize_site("  example.atlassian.net  ") == "example.atlassian.net"


@pytest.mark.parametrize("bad", ["", "   ", "https://", "example.atlassian.net/jira", "a b"])
def test_normalize_site_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        j.normalize_site(bad)


def test_session_uses_basic_auth_and_json_accept_header():
    session = j.jira_session("me@example.com", "tok3n")
    assert session.auth == ("me@example.com", "tok3n")
    assert session.headers["Accept"] == "application/json"


def test_redact_replaces_every_secret_and_ignores_empty_ones():
    text = "failed for hunter2 and tok3n"
    assert j.redact(text, "hunter2", "tok3n", "") == "failed for *** and ***"


@pytest.mark.parametrize("bad", [
    "attacker.example",
    "example.atlassian.net.evil.com",
    "evil.com#.atlassian.net",
    "me@example.atlassian.net",
    "example.atlassian.net:8443",
    "atlassian.net",
    "a.b.atlassian.net",
    "-x.atlassian.net",
])
def test_normalize_site_rejects_hosts_that_are_not_jira_cloud_sites(bad):
    # The Basic-auth header carries the API token, so it must only ever go to
    # a <site>.atlassian.net host.
    with pytest.raises(ValueError):
        j.normalize_site(bad)


def test_normalize_site_lowercases_the_host():
    assert j.normalize_site("HTTPS://Example.Atlassian.NET/") == "example.atlassian.net"


def test_credentials_strips_values_and_normalises_the_site():
    assert j.credentials(" https://Example.atlassian.net/ ", " me@example.com\n", " tok ") == (
        "example.atlassian.net", "me@example.com", "tok")


@pytest.mark.parametrize("site, email, token, name", [
    ("", "me@example.com", "s3cr3t-value", "site"),
    ("example.atlassian.net", "  ", "s3cr3t-value", "email"),
    ("example.atlassian.net", "me@example.com", None, "api_token"),
])
def test_credentials_names_a_blank_value_without_echoing_the_others(site, email, token, name):
    with pytest.raises(j.JiraError) as exc:
        j.credentials(site, email, token)
    message = str(exc.value)
    assert name in message and "me@example.com" not in message and "s3cr3t-value" not in message


def test_credentials_rejects_a_site_that_is_not_jira_cloud():
    with pytest.raises(ValueError):
        j.credentials("jira.example.com", "me@example.com", "tok")
