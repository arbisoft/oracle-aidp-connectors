import pytest

from aidp_connector_notion.auth import NotionTokenError, read_token
from aidp_connector_notion.config import NotionSettings


class FakeSecrets:
    def __init__(self, value="from-store"):
        self.value = value
        self.calls = []

    def get(self, name, key):
        self.calls.append((name, key))
        return self.value


def test_reads_credential_store_when_named():
    secrets = FakeSecrets()
    settings = NotionSettings(credential_name="notion_token", credential_key="secret")
    assert read_token(settings, secrets.get, environ={"NOTION_TOKEN": "from-env"}) == "from-store"
    assert secrets.calls == [("notion_token", "secret")]


def test_empty_credential_is_an_error():
    settings = NotionSettings(credential_name="notion_token")
    with pytest.raises(NotionTokenError, match="notion_token"):
        read_token(settings, FakeSecrets(value="").get, environ={})


def test_falls_back_to_environment_off_platform():
    settings = NotionSettings(credential_name="notion_token")
    assert read_token(settings, None, environ={"NOTION_TOKEN": "from-env"}) == "from-env"


def test_uses_environment_when_no_credential_named():
    secrets = FakeSecrets()
    settings = NotionSettings(token_env="MY_TOKEN")
    assert read_token(settings, secrets.get, environ={"MY_TOKEN": "from-env"}) == "from-env"
    assert secrets.calls == []


def test_missing_token_names_the_variable():
    with pytest.raises(NotionTokenError, match="MY_TOKEN"):
        read_token(NotionSettings(token_env="MY_TOKEN"), None, environ={})
