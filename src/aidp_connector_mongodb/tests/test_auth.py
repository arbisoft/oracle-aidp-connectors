import pytest

from aidp_connector_mongodb.auth import read_uri
from aidp_connector_mongodb.config import MongoSettings
from aidp_connector_mongodb.reader import MongoError

URI = "mongodb+srv://reader:S3cret@cluster0.abcde.mongodb.net/"


def test_reads_the_credential_store_entry_and_key():
    calls = []

    def get_secret(name, key):
        calls.append((name, key))
        return "  " + URI + "\n"

    assert read_uri(MongoSettings("db", "c", credential_name="mongo"), get_secret) == URI
    assert calls == [("mongo", "uri")]


def test_empty_credential_raises():
    with pytest.raises(MongoError, match="no value for key 'uri'"):
        read_uri(MongoSettings("db", "c", credential_name="mongo"), lambda name, key: "")


def test_falls_back_to_the_environment_off_aidp():
    settings = MongoSettings("db", "c", credential_name="mongo")
    assert read_uri(settings, None, {"MONGODB_URI": URI}) == URI


def test_missing_uri_names_the_variable():
    with pytest.raises(MongoError, match="MONGODB_URI"):
        read_uri(MongoSettings("db", "c"), None, {})


def test_a_non_mongodb_value_is_rejected_without_echoing_it():
    with pytest.raises(MongoError) as exc:
        read_uri(MongoSettings("db", "c"), None, {"MONGODB_URI": "https://user:pw@example.com"})
    assert "pw" not in str(exc.value) and "example.com" not in str(exc.value)
