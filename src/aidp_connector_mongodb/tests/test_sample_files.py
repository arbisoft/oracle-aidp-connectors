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
