import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # build_connector_jar.py sits next to pyproject.toml

import build_connector_jar as b  # noqa: E402

SERVICE = "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"


def jar(entries):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return out.getvalue()


def test_merge_combines_classes_drops_manifests_and_signatures_and_joins_services():
    first = jar({"META-INF/MANIFEST.MF": "old", "META-INF/X.SF": "sig", "com/a/A.class": "a",
                 SERVICE: "com.a.Provider\n", "dup.properties": "first"})
    second = jar({"META-INF/MANIFEST.MF": "old", "org/b/": "", "org/b/B.class": "b",
                  SERVICE: "org.b.Provider\n", "dup.properties": "second"})
    merged = zipfile.ZipFile(io.BytesIO(b.merge([first, second])))
    names = merged.namelist()
    assert names[0] == "META-INF/MANIFEST.MF" and names.count("META-INF/MANIFEST.MF") == 1
    assert merged.read("META-INF/MANIFEST.MF").decode() == b.MANIFEST
    assert {"com/a/A.class", "org/b/B.class", "META-INF/NOTICE"} <= set(names)
    assert "META-INF/X.SF" not in names and "org/b/" not in names
    assert merged.read(SERVICE) == b"com.a.Provider\norg.b.Provider\n"
    assert merged.read("dup.properties") == b"first"


def test_merge_is_reproducible():
    jars = [jar({"com/a/A.class": "a"}), jar({"org/b/B.class": "b"})]
    assert b.merge(jars) == b.merge(list(reversed(jars)))
