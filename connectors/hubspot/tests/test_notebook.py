import inspect
import json
import re
from pathlib import Path

NOTEBOOKS = sorted(Path(__file__).resolve().parents[1].joinpath("examples").glob("*.ipynb"))


def code_of(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text())["cells"] if c["cell_type"] == "code")


def test_at_least_one_example_notebook_exists():
    assert NOTEBOOKS


def test_notebooks_have_no_outputs_real_hosts_or_inline_tokens():
    for path in NOTEBOOKS:
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == [], path.name
                assert cell["execution_count"] is None, path.name
            text = "".join(cell["source"])
            assert not re.search(r"pat-[a-z]{2}\d", text), path.name
            assert not re.search(r"\b\d{6,}\b", text), path.name  # no portal ids
            for host in re.findall(r"[\w-]+\.hubapi\.com", text):
                assert host == "api.hubapi.com", (path.name, host)
            assert not re.search(r"token\s*=\s*['\"][^'\"<]", text, re.I), path.name


def test_notebooks_read_the_token_from_the_credential_store():
    for path in NOTEBOOKS:
        code = code_of(path)
        assert "aidputils.secrets.get(" in code and "CREDENTIAL_NAME" in code, path.name
        assert "run_sync(" in code, path.name


def test_notebooks_import_the_helper_from_a_folder_on_sys_path():
    for path in NOTEBOOKS:
        code = code_of(path)
        assert "sys.path.insert(0, HELPER_DIR)" in code, path.name
        assert code.index("sys.path.insert") < code.index("import hubspot_client"), path.name


def test_notebook_options_exist_in_the_helper_and_default_to_the_documented_values():
    import hubspot_client as h

    code = code_of(NOTEBOOKS[0])
    assert 'MODE = "incremental"' in code and "OVERLAP_SECONDS = 300" in code
    assert set(re.findall(r'"(\w+)"', code.split("OBJECTS = ")[1].split("\n")[0])) == set(h.ALL_OBJECTS)
    params = inspect.signature(h.run_sync).parameters
    for keyword in ("table_prefix", "objects", "mode", "overlap_seconds", "requests_per_second"):
        assert keyword in params and f"{keyword}=" in code


def test_the_notebook_runs_end_to_end_with_a_fake_spark_and_secret_store(tmp_path, monkeypatch):
    """Execute the code cells, with placeholders replaced, against fakes."""
    import hubspot_client as h
    from fakes import FakeHubSpot, FakeSpark, make_contact, ts

    code = code_of(NOTEBOOKS[0]).replace("<WORKSPACE_PATH_TO_HELPER_FOLDER>", str(Path(h.__file__).parent))
    code = code.replace("<CATALOG>", "lake").replace("<SCHEMA>", "crm")
    code = code.replace("<CREDENTIAL_NAME>", "hubspot").replace("<KEY>", "token")

    class Secrets:
        def get(self, name, key):
            assert (name, key) == ("hubspot", "token")
            return "synthetic-token"

    fake = FakeHubSpot(objects={"contacts": [make_contact(1, ts(5))], "companies": [], "deals": []})
    real = h.HubSpotClient.__init__
    monkeypatch.setattr(h.HubSpotClient, "__init__",
                        lambda self, token, **kw: real(self, token, **dict(kw, session=fake, sleep=lambda s: None)))
    spark = FakeSpark([("SELECT id FROM", [])])
    namespace = {"spark": spark, "aidputils": type("A", (), {"secrets": Secrets()})()}
    namespace["spark"].table = lambda name: type("T", (), {"show": lambda self, **kw: None})()
    exec(compile(code, "notebook", "exec"), namespace)
    assert namespace["counts"]["contacts"] == 1
