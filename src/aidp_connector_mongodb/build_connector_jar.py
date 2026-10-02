"""Build one jar holding the MongoDB Spark Connector and its four driver jars.

Installing one cluster library instead of five. Downloads the jars from Maven
Central, checks each SHA-256, and merges them. No class appears in more than
one jar and none is signed, so a plain merge is enough: no relocation. Entries
are written in a fixed order with a fixed timestamp, so every build of the same
jars gives the same file and the same SHA-256.

    python build_connector_jar.py            # writes dist/<BUNDLE>
"""

from __future__ import annotations

import hashlib
import io
import sys
import urllib.request
import zipfile
from pathlib import Path

MAVEN = "https://repo1.maven.org/maven2/org/mongodb/"
# Same jars and SHA-256 as README.md step 1.
JARS = {
    "spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar":
        "1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe",
    "mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar":
        "341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2",
    "mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar":
        "ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec",
    "bson/5.1.4/bson-5.1.4.jar":
        "bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676",
    "bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar":
        "698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655",
}
BUNDLE = "mongo-spark-connector-bundle_2.12-10.7.0.jar"
MANIFEST = "Manifest-Version: 1.0\r\nCreated-By: aidp-connector-mongodb build_connector_jar.py\r\n\r\n"
NOTICE = (
    "This jar combines, unmodified, the following Apache License 2.0 jars from\n"
    "Maven Central (https://www.apache.org/licenses/LICENSE-2.0):\n\n"
    + "".join(f"  org.mongodb/{path.rsplit('/', 1)[1]}\n" for path in JARS)
)
_EPOCH = (1980, 1, 1, 0, 0, 0)


def download(path: str, sha256: str) -> bytes:
    with urllib.request.urlopen(MAVEN + path, timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise SystemExit(f"SHA-256 mismatch for {path}: not writing the bundle")
    return data


def merge(jars) -> bytes:
    """Merge jar contents (bytes) into one jar. ``META-INF/services`` files are
    concatenated; any other duplicate keeps the first jar's copy. Manifests and
    signatures are dropped and a fresh manifest is written."""
    entries = {}
    for jar in jars:
        with zipfile.ZipFile(io.BytesIO(jar)) as source:
            for name in source.namelist():
                upper = name.upper()
                if name.endswith("/") or upper == "META-INF/MANIFEST.MF" or (
                        upper.startswith("META-INF/") and upper.endswith((".SF", ".RSA", ".DSA", ".EC"))):
                    continue
                data = source.read(name)
                if name in entries and name.startswith("META-INF/services/"):
                    entries[name] = entries[name].rstrip(b"\n") + b"\n" + data
                else:
                    entries.setdefault(name, data)
    entries["META-INF/NOTICE"] = NOTICE.encode()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as bundle:
        # The manifest goes first, as java.util.jar expects.
        for name, data in [("META-INF/MANIFEST.MF", MANIFEST.encode()), *sorted(entries.items())]:
            info = zipfile.ZipInfo(name, _EPOCH)
            info.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(info, data)
    return out.getvalue()


def main() -> None:
    target = Path(__file__).resolve().parent / "dist" / BUNDLE
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(merge(download(path, sha) for path, sha in JARS.items()))
    print(f"{target}\nSHA-256 {hashlib.sha256(target.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    sys.exit(main())
