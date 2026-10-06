import pytest

from aidp_connector_mongodb.config import ConfigError, parse_config


def base(**sync):
    return {
        "mongodb": {"database": "db", "collection": "coll", "credential_name": "mongo"},
        "target": {"catalog": "cat", "schema": "raw", "table": "coll"},
        "sync": sync,
    }


def test_defaults():
    cfg = parse_config(base())
    assert cfg.mongodb.credential_key == "uri" and cfg.mongodb.uri_env == "MONGODB_URI"
    assert cfg.mongodb.server_selection_timeout_ms == 10000
    assert cfg.target.qualified_table == "cat.raw.coll" and cfg.target.qualified_schema == "cat.raw"
    assert cfg.sync.watermark_field is None and cfg.sync.string_fields == ()
    assert cfg.sync.fields == () and cfg.sync.columns == ()
    assert (cfg.sync.sample_size, cfg.sync.overlap_seconds) == (10000, 300)


def test_sync_section_is_optional():
    data = base()
    del data["sync"]
    assert parse_config(data).sync.watermark_field is None


def test_sync_values_are_read():
    cfg = parse_config(base(watermark_field="updatedAt", string_fields=["year"], sample_size=500,
                            overlap_seconds=0))
    assert cfg.sync.watermark_field == "updatedAt" and cfg.sync.string_fields == ("year",)
    assert (cfg.sync.sample_size, cfg.sync.overlap_seconds) == (500, 0)


def test_columns_always_keep_id_string_fields_and_the_watermark():
    cfg = parse_config(base(fields=["title", "year"], string_fields=["year", "rated"],
                            watermark_field="updatedAt"))
    assert cfg.sync.fields == ("title", "year")
    assert cfg.sync.columns == ("_id", "title", "year", "rated", "updatedAt")


@pytest.mark.parametrize("data, message", [
    (None, "mapping"),
    ({**base(), "extra": {}}, "unknown section"),
    ({"target": base()["target"]}, "missing section 'mongodb'"),
    ({**base(), "mongodb": {"database": "db", "collection": "c", "uri": "mongodb://x"}}, "unknown key"),
    ({**base(), "mongodb": {"database": "db", "collection": " "}}, "mongodb.collection"),
    ({**base(), "target": {"catalog": "cat", "schema": "raw", "table": "my-table"}}, "target.table"),
    ({**base(), "target": {"catalog": "<CATALOG>", "schema": "raw", "table": "t"}}, "target.catalog"),
    (base(watermark_field="a`b"), "backtick"),
    (base(watermark_field=""), "sync.watermark_field"),
    (base(string_fields="year"), "list"),
    (base(fields="title"), "sync.fields must be a list"),
    (base(fields=["title", " "]), "sync.fields entry"),
    (base(sample_size=0), "sync.sample_size"),
    (base(sample_size=True), "sync.sample_size"),
    (base(overlap_seconds=-1), "sync.overlap_seconds"),
])
def test_invalid_configs_are_rejected(data, message):
    with pytest.raises(ConfigError, match=message):
        parse_config(data)
