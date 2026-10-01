# MongoDB Atlas connector

Read a MongoDB Atlas collection into Spark on AIDP with `mongodb.py`, using
the MongoDB Spark Connector: a full load on the first run, then incremental
loads on a Date watermark field, merged on `_id`. `mongodb.py` is
self-contained; upload that one file.

Live-tested on AIDP 2026-10-01: see `live-results/RESULTS.md`.
Running a live test: `LIVE_TEST_GUIDE.md`. A self-contained showcase notebook
for the same approach is proposed for oracle-aidp-samples, as
`data-engineering/ingestion/MongoDB_Atlas/`.

## Setup on AIDP

1. **Jars.** Download these five jars from Maven Central and check them
   against these SHA-256 values:

   | Jar | SHA-256 |
   |---|---|
   | [mongo-spark-connector_2.12-10.7.0.jar](https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar) | `1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe` |
   | [mongodb-driver-sync-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar) | `341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2` |
   | [mongodb-driver-core-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar) | `ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec` |
   | [bson-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson/5.1.4/bson-5.1.4.jar) | `bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676` |
   | [bson-record-codec-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar) | `698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655` |

   Put them in a workspace folder, install each from the cluster's
   **Library** tab (**Install Library → Workspace**, one at a time), then
   **Actions → Restart**.
2. **Credential.** In the AIDP Credential Store, create a **Secret Token**
   credential with key `uri` and the `mongodb+srv://<user>:<password>@<cluster>.mongodb.net/`
   connection string of a read-only database user.
3. **Network.** Add the cluster's outbound IP to the Atlas project's IP
   access list, and check it again after a cluster restart.
4. Upload `mongodb.py`, open `examples/mongodb_collection_load.ipynb`, fill
   in the placeholders, and run it top to bottom.

## Gotchas

Laptop findings are from 2026-09-30 against Atlas M0; AIDP findings are from
the live run on 2026-10-01.

- **Install the jars as cluster libraries** (AIDP). Loading them into a
  running session with `SparkContext.addJar` lets the driver read, but every
  executor task fails with `UnknownReason`, even `limit(1)`.
- **Only one library change runs at a time per cluster** (AIDP): installing
  a jar while another install is still running fails with "ongoing
  operation".
- **Executors cannot resolve `mongodb+srv://`** (AIDP): `Failed looking up
  TXT record`. The notebook calls `resolve_srv(spark, uri)`, which looks up
  the SRV and TXT records on the driver and gives Spark an equivalent
  `mongodb://` URI, rebuilt on every run so Atlas host changes are picked up.
- **Use the Credential Store** (AIDP). Notebooks cannot prompt for input
  (`getpass` fails), and the cluster UI had no environment-variable setting.
- **The Atlas IP access list comes first.** A blocked IP fails after the
  server-selection timeout with `SSLException: Received fatal alert:
  internal_error`. The helper names the likely cause and sets a 10 s timeout
  instead of the driver's 30 s.
- **Schema inference can silently lose data.** A Decimal128 wider than the
  sampled precision becomes `null` with no error. `read_collection` widens
  the inferred schema first (decimals to `decimal(38, >=10)`, ints to longs).
- **A field whose type varies between documents fails the read** with
  `UnknownReason` when the sample didn't see the other type. Pass it in
  `string_fields=`. (On AIDP, `UnknownReason` on even `limit(1)` means the
  jars are missing on the executors instead.)
- **The watermark field must hold BSON Dates.** A Date bound never matches a
  string field, so the read returns 0 documents.
- **Don't read the watermark back by collecting a TIMESTAMP.** PySpark
  returns it in the Python process's local timezone. Use
  `latest_watermark()`.
- **Don't check for lost values with `df.filter(col.isNull())`.** It is
  pushed down to MongoDB, which only counts server-side nulls. Count on the
  written table instead.
- **Use a read-only database user.** Atlas's quick-setup user has
  `atlasAdmin`.
- `resolve_srv` does not support the `srvMaxHosts` and `srvServiceName`
  URI options.

## Test

From the repository root:

    pip install -r requirements-dev.txt
    pytest -q
