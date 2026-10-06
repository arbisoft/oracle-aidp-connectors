"""The sample notebook: committed clean, no secrets, uses the package, and runs against fakes."""

import json
import re
from pathlib import Path

import pytest

from aidp_connector_hubspot.config import SUPPORTED_OBJECTS, load_config

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = ROOT / "hubspot_ingest.ipynb"


def cells():
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]


def code_of():
    return "\n".join("".join(c["source"]) for c in cells() if c["cell_type"] == "code")


def test_notebook_has_no_outputs_real_hosts_or_inline_tokens():
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    assert nb["nbformat"] == 4
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            assert cell["outputs"] == [] and cell["execution_count"] is None
        text = "".join(cell["source"])
        assert not re.search(r"pat-[a-z]{2}\d", text)
        assert not re.search(r"\b\d{6,}\b", text)  # no portal ids
        for host in re.findall(r"[\w-]+\.hubapi\.com", text):
            assert host == "api.hubapi.com"
        assert not re.search(r"token\s*=\s*['\"][^'\"<]", text, re.I)


def test_notebook_reads_the_token_from_the_credential_store_and_never_imports_aidputils():
    code = code_of()
    assert "aidputils.secrets.get" in code
    assert "read_token(config.hubspot, get_secret)" in code
    assert not re.search(r"^\s*(import aidputils|from aidputils)", code, re.M)
    assert "oidlUtils.parameters.getParameter" in code


def test_notebook_uses_the_installed_package_not_a_helper_folder():
    code = code_of()
    assert "from aidp_connector_hubspot import" in code
    assert "sys.path" not in code and "hubspot_client" not in code and "HELPER_DIR" not in code
    assert "run(spark, config, token, mode_override=MODE)" in code
    assert "format_summary(summaries)" in code and "raise_on_failure(summaries)" in code


def test_notebook_config_cell_points_at_the_yaml_and_the_sample_yaml_matches_the_package():
    assert 'CONFIG_PATH = "/Workspace/<path-to>/hubspot_ingest.yaml"' in code_of()
    cfg = load_config(str(ROOT / "hubspot_ingest.sample.yaml"))
    assert set(cfg.sync.objects) == set(SUPPORTED_OBJECTS)
    assert cfg.sync.mode == "incremental" and cfg.sync.overlap_seconds == 300
    assert cfg.hubspot.requests_per_second == 4.0 and cfg.hubspot.api_version == "2026-03"


def test_every_code_cell_compiles():
    for cell in cells():
        if cell["cell_type"] == "code":
            compile("".join(cell["source"]), "cell", "exec")


@pytest.mark.parametrize("configured", ["contacts", "companies"])
def test_the_notebook_runs_end_to_end_with_a_fake_spark_and_secret_store(tmp_path, monkeypatch, configured):
    """Execute the code cells, with the install line left commented out, against fakes."""
    from aidp_connector_hubspot import client as client_module
    from fakes import FakeHubSpot, FakeSpark, make_company, make_contact, ts

    config_path = tmp_path / "hubspot_ingest.yaml"
    config_path.write_text(
        "hubspot: {credential_name: hubspot_token}\n"
        "target: {catalog: lake, schema: crm}\n"
        f"sync: {{objects: [{configured}]}}\n",
        encoding="utf-8",
    )
    code = code_of().replace("/Workspace/<path-to>/hubspot_ingest.yaml", str(config_path))

    class Secrets:
        def get(self, name, key):
            assert (name, key) == ("hubspot_token", "secret")
            return "synthetic-token"

    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [make_company(10, ts(5))], "deals": []})
    real = client_module.HubSpotClient.__init__
    monkeypatch.setattr(
        client_module.HubSpotClient, "__init__",
        lambda self, token, **kw: real(self, token, **dict(kw, session=fake, sleep=lambda s: None)),
    )
    spark = FakeSpark([("SELECT id FROM", [])])
    shown = []
    spark.table = lambda name: shown.append(name) or type("T", (), {"show": lambda self, **kw: None})()
    namespace = {"spark": spark, "aidputils": type("A", (), {"secrets": Secrets()})()}
    exec(compile(code, "notebook", "exec"), namespace)
    assert [(s["object"], s["status"], s["rows"]) for s in namespace["summaries"]] == [(configured, "SUCCESS", 1)]
    assert shown == [f"lake.crm.{configured}"]  # the state table is printed by run(), not again here
    assert namespace["MODE"] is None and namespace["token"] == "synthetic-token"


def test_notebook_does_not_fail_when_no_data_table_is_configured(tmp_path):
    code = code_of().split("shown = ")[1]
    namespace = {"config": load_config(str(ROOT / "hubspot_ingest.sample.yaml")), "spark": None}
    namespace["config"] = type("C", (), {"sync": type("S", (), {"objects": ("associations",)})(),
                                         "target": namespace["config"].target})()
    exec(compile("prefix = ''\nschema = 's'\nshown = " + code, "cell", "exec"), namespace)
    assert namespace["shown"] == []
