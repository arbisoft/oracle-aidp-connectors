import ast
import json
import re
from pathlib import Path

from aidp_connector_mongodb.config import load_config

SAMPLE = Path(__file__).resolve().parent.parent


def test_sample_config_is_valid_and_has_no_watermark_by_default():
    cfg = load_config(str(SAMPLE / "mongodb_ingest.sample.yaml"))
    assert cfg.mongodb.credential_name == "mongodb_uri" and cfg.mongodb.credential_key == "uri"
    assert cfg.sync.watermark_field is None


def test_notebook_is_valid_and_clean():
    notebook = json.loads((SAMPLE / "mongodb_ingest.ipynb").read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
    assert code_cells
    for cell in code_cells:
        assert cell["outputs"] == [] and cell["execution_count"] is None
        ast.parse("".join(cell["source"]))
    for cell in notebook["cells"]:
        if cell["cell_type"] == "markdown":
            # AIDP renders a space inside parentheses in Markdown as %20.
            assert not re.search(r"\([^)]*\s[^)]*\)", "".join(cell["source"])), "".join(cell["source"])[:60]
    for path in (SAMPLE / "mongodb_ingest.ipynb", SAMPLE / "mongodb_ingest.sample.yaml"):
        text = path.read_text(encoding="utf-8")
        # No URI with credentials, and no real Atlas host.
        assert not re.search(r"mongodb(\+srv)?://[^<\s\"']+:[^<\s\"']+@", text), path.name
        assert set(re.findall(r"[\w.<>-]+\.mongodb\.net", text)) <= {"<cluster>.mongodb.net"}, path.name


def test_notebook_reads_the_uri_from_the_credential_store_and_never_loads_jars_at_runtime():
    notebook = json.loads((SAMPLE / "mongodb_ingest.ipynb").read_text(encoding="utf-8"))
    code = "".join("".join(c["source"]) for c in notebook["cells"] if c["cell_type"] == "code")
    assert "aidputils.secrets.get" in code and "read_uri(" in code
    assert "addJar" not in code


def _job_parameter_cell():
    notebook = json.loads((SAMPLE / "mongodb_ingest.ipynb").read_text(encoding="utf-8"))
    return next(
        "".join(cell["source"]) for cell in notebook["cells"]
        if cell["cell_type"] == "code" and "def job_parameter" in "".join(cell["source"])
    )


class _FakeParameters:
    """Behaves like the platform: names are case-sensitive, a missing name returns the default."""

    def __init__(self, values):
        self._values = values

    def getParameter(self, name, default):
        return self._values.get(name, default)


class _FakeOidlUtils:
    def __init__(self, values):
        self.parameters = _FakeParameters(values)


def _run_cell(namespace):
    exec(compile(_job_parameter_cell(), "job_parameter_cell", "exec"), namespace)
    return namespace["MODE"]


def test_notebook_reads_mode_parameter_in_any_case():
    assert _run_cell({"oidlUtils": _FakeOidlUtils({"mode": " full "})}) == "full"
    assert _run_cell({"oidlUtils": _FakeOidlUtils({"MODE": "incremental"})}) == "incremental"


def test_notebook_mode_is_none_when_parameter_missing_blank_or_off_platform():
    assert _run_cell({"oidlUtils": _FakeOidlUtils({})}) is None
    assert _run_cell({"oidlUtils": _FakeOidlUtils({"MODE": "  "})}) is None
    assert _run_cell({}) is None  # no oidlUtils: not running on AIDP
