import ast
import json
import re
from pathlib import Path

from aidp_connector_jira.config import job_parameter, load_config

SAMPLE = Path(__file__).resolve().parent.parent
NOTEBOOK = SAMPLE / "jira_ingest.ipynb"


def _cells(kind):
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == kind]


def test_sample_config_is_valid():
    cfg = load_config(str(SAMPLE / "jira_ingest.sample.yaml"))
    assert cfg.jira.credential_name == "jira" and cfg.sync.jql
    assert cfg.sync.extra_fields == ()


def test_notebook_is_valid_and_clean():
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert cell["outputs"] == [] and cell["execution_count"] is None
            ast.parse("".join(cell["source"]))
    for path in (NOTEBOOK, SAMPLE / "jira_ingest.sample.yaml"):
        text = path.read_text(encoding="utf-8")
        # No real site and no token typed in.
        assert set(re.findall(r"[\w<>-]+\.atlassian\.net", text)) <= {"<site>.atlassian.net"}, path.name
        assert not re.search(r"token\s*[=:]\s*['\"][^'\"<]", text, re.I), path.name
    for text in _cells("markdown"):
        # AIDP renders a space inside parentheses in Markdown as %20.
        assert not re.search(r"\([^)]*\s[^)]*\)", text), text[:60]


def test_notebook_holds_no_logic_and_reads_the_credential_store():
    code = "\n".join(_cells("code"))
    assert not re.search(r"^\s*(def|class) ", code, re.M), "logic belongs in the package"
    assert "aidputils.secrets.get" in code and "read_credentials(" in code


def _run_mode_cell(namespace):
    cell = next(text for text in _cells("code") if "job_parameter(" in text and "MODE" in text)
    namespace["job_parameter"] = job_parameter  # imported by the notebook's first code cell
    exec(compile(cell, "mode_cell", "exec"), namespace)
    return namespace["MODE"]


class _FakeOidlUtils:
    """Behaves like the platform: names are case-sensitive, a missing name returns the default."""

    def __init__(self, values):
        self.parameters = self
        self._values = values

    def getParameter(self, name, default):
        return self._values.get(name, default)


def test_notebook_reads_mode_parameter_in_any_case():
    assert _run_mode_cell({"oidlUtils": _FakeOidlUtils({"mode": " full "})}) == "full"
    assert _run_mode_cell({"oidlUtils": _FakeOidlUtils({"MODE": "incremental"})}) == "incremental"


def test_notebook_mode_is_none_when_parameter_missing_blank_or_off_platform():
    assert _run_mode_cell({"oidlUtils": _FakeOidlUtils({})}) is None
    assert _run_mode_cell({"oidlUtils": _FakeOidlUtils({"MODE": "  "})}) is None
    assert _run_mode_cell({}) is None  # no oidlUtils: not running on AIDP
