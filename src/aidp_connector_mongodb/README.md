# aidp-connector-mongodb

A MongoDB connector for Oracle AI Data Platform (AIDP) Workbench. It copies one MongoDB collection into a Delta table in an AIDP catalog with the MongoDB Spark Connector: a full load on the first run, then incremental loads merged on `_id`.

AIDP has no built-in MongoDB source. This connector fills that gap: a small Python package that you install on an AIDP cluster and run from a notebook or a scheduled job. It is plain Python driving the MongoDB Spark Connector, not a `type` of the built-in `aidataplatform` Spark format.

## What it does

On each run the connector:

1. Reads `mongodb_ingest.yaml`: the database and collection, the target table, and the watermark field.
2. Reads the connection URI from the AIDP Credential Store.
3. Resolves a `mongodb+srv://` URI on the Spark driver, because AIDP executors cannot.
4. Checks the connection by sampling one document, so a wrong password or a blocked IP fails fast with a clear message.
5. Reads the collection with the MongoDB Spark Connector, filtered in MongoDB to what changed since the last run, and creates the table or merges into it on `_id`.

```
MongoDB / Atlas ──MongoDB Spark Connector──▶ AIDP cluster (executors) ──▶ <catalog>.<schema>.<table>
  (one collection)    aidp_connector_mongodb on the driver
```

## What you get

One Delta table per configuration, `<catalog>.<schema>.<table>`, with one row per document:

| MongoDB | Delta column |
|---|---|
| `ObjectId` `_id` | `string`, the 24-character hex value |
| Date | `timestamp` |
| Embedded document | `struct<...>` |
| Array | `array<...>` |
| Decimal128 | `decimal(38, s)` with `s` of at least 10 |
| 32-bit int | `bigint` |
| A field whose type differs between documents | `string`, when listed in `sync.string_fields` |

Nested fields stay nested: query them with `awards.wins` or `explode(genres)`.

## Refresh

| Run | `watermark_field` set | `watermark_field: null` |
|---|---|---|
| First (no table yet) | Reads the whole collection and creates the table. | Same. |
| Later | Reads documents whose watermark field is newer than the table's newest value minus `overlap_seconds`, filtered in MongoDB, and merges them on `_id`. | Re-reads the whole collection and merges it on `_id`. |

Merges never remove rows: a document deleted in MongoDB stays in the table. A **full refresh** (job parameter `MODE=full`, or `run(..., "full")`) re-reads the whole collection with a freshly sampled schema and overwrites the table, which purges deleted documents and picks up new fields.

## Using it in Oracle AIDP

You need a MongoDB deployment the AIDP cluster can reach (Atlas works), an AIDP workspace with a cluster you can install libraries on, and an existing catalog to write to. To build the wheel you need [uv](https://docs.astral.sh/uv/) on your machine.

### 1. Install the MongoDB Spark Connector jars

Download these five jars from Maven Central and check their SHA-256:

| Jar | SHA-256 |
|---|---|
| [mongo-spark-connector_2.12-10.7.0.jar](https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar) | `1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe` |
| [mongodb-driver-sync-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar) | `341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2` |
| [mongodb-driver-core-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar) | `ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec` |
| [bson-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson/5.1.4/bson-5.1.4.jar) | `bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676` |
| [bson-record-codec-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar) | `698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655` |

Upload them to a workspace folder, install each from the cluster **Library** tab (**Install Library → Workspace**, one at a time, waiting for each to finish), then restart the cluster.

**Or install one jar instead of five.** From this folder, on your machine:

```bash
python build_connector_jar.py
```

It downloads the same five jars, checks the SHA-256 values above, and merges them, unmodified, into `dist/mongo-spark-connector-bundle_2.12-10.7.0.jar` (SHA-256 `b75cd0a6b93e07d2187e7da2f0f4ccb10a5639b2aa135cd711028d59105ee58f`; every build gives the same file). Install that one file the same way, then restart the cluster. Verified on AIDP on 2026-10-02 (see [Validation status](#validation-status)). Loading them at runtime with `SparkContext.addJar` does not work on AIDP: the driver can read, but every executor task fails. Nor can Spark fetch them from Maven itself: AIDP rejects the cluster Spark property `spark.jars.packages` as reserved (`SPARK_CONFIGURATION_PROPERTY_RESERVED`, verified 2026-10-02).

### 2. Create a read-only database user

The connector only reads. In Atlas, create a database user with the **Only read any database** role, or `read` on the source database. Atlas's quick-setup user has `atlasAdmin`, which is more than needed.

### 3. Allow the cluster's IP

Atlas only accepts connections from IPs on the project's IP access list. Find the AIDP cluster's outbound IP from a scratch notebook cell, `urllib.request.urlopen("https://checkip.amazonaws.com").read()`, without saving the output, and add it under **Network Access**. A change takes a minute or two to apply. Check the IP again after a cluster restart.

### 4. Store the URI in AIDP

Add the connection string, `mongodb+srv://<user>:<password>@<cluster>.mongodb.net/`, to the AIDP **Credential Store**, for example as a **Secret Token** credential named `mongodb_uri` with the URI under the key `uri`. The connector reads it at run time, so the password never appears in the notebook or the config file.

### 5. Build the package and upload the files

From this folder, on your machine:

```bash
uv build
```

This writes `dist/aidp_connector_mongodb-<version>-py3-none-any.whl`. Upload three files to a folder in your AIDP workspace:

| File | Purpose |
|---|---|
| `dist/aidp_connector_mongodb-<version>-py3-none-any.whl` | The connector package |
| `mongodb_ingest.ipynb` | Notebook that runs a sync |
| `mongodb_ingest.sample.yaml`, renamed to `mongodb_ingest.yaml` | Your configuration |

### 6. Install the package on the cluster

Pick one:

- **Cluster library (recommended for jobs):** add the wheel from the cluster **Library** tab and restart the cluster.
- **Notebook-scoped:** in step 1 of `mongodb_ingest.ipynb`, uncomment the `%pip install` line and set the path.

Either way, `pyyaml` is installed as a dependency.

### 7. Configure

Edit `mongodb_ingest.yaml`. At a minimum set:

```yaml
mongodb:
  credential_name: mongodb_uri   # the Credential Store entry from step 4
  database: my_database
  collection: my_collection
target:
  catalog: my_catalog            # must already exist
  schema: mongodb_raw            # created if missing
  table: my_collection
sync:
  watermark_field: updatedAt     # a BSON Date updated on every write, or null
```

Every key is described in the [configuration reference](#configuration-reference). The file holds no secret.

### 8. Run the notebook

Open `mongodb_ingest.ipynb`, attach it to the cluster, set the path in step 2 to your `mongodb_ingest.yaml`, and run all cells. The last cell prints:

```
collection    my_database.my_collection
table         my_catalog.mongodb_raw.my_collection
mode          incremental
since         2026-10-02T08:14:00+00:00
rows_in_table 41079
```

### 9. Schedule it

Create an AIDP job that runs `mongodb_ingest.ipynb` on the cluster where the jars and the package are installed, and set its **maximum concurrent runs to 1**.

For deletes, add a second, less frequent schedule, for example weekly, with the job parameter `MODE` set to `full`. It overrides the normal run for that run only.

### Loading several collections

One configuration loads one collection into one table. To load several, keep one config file per collection and run the notebook once per file, or schedule one job each. Each collection needs its own settings: whether it has a Date field to use as the watermark, and which fields mix types between documents.

## Using the package from your own code

```python
from aidp_connector_mongodb import format_summary, load_config, read_uri, run

config = load_config("/Workspace/<path-to>/mongodb_ingest.yaml")
uri = read_uri(config.mongodb, aidputils.secrets.get)

summary = run(spark, config, uri)               # or run(..., "full")
print(format_summary(summary))
```

- `run` takes an optional mode: `"full"` overwrites the table; `None`, empty or `"incremental"` is the normal run. Any other value raises `ConfigError` before connecting.
- `run` returns a summary dict (`collection`, `table`, `mode`, `since`, `rows_in_table`) and raises on failure.
- `parse_config` accepts an already-loaded dict in place of a YAML path.
- `read_uri` reads `mongodb.credential_name` through the getter you pass. It falls back to the `mongodb.uri_env` environment variable when no credential is named or no getter is given.
- Errors are `MongoError`, or `MongoAuthError` for a rejected user or password. Their messages carry the root Java cause and a hint, and never the URI's user, password or hosts.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `mongodb.credential_name` | none | Credential Store entry holding the URI. |
| `mongodb.credential_key` | `uri` | Key inside that credential. |
| `mongodb.uri_env` | `MONGODB_URI` | Environment variable used when no credential is named. |
| `mongodb.database` | required | Source database. |
| `mongodb.collection` | required | Source collection. |
| `mongodb.server_selection_timeout_ms` | `10000` | How long to wait for a server before failing. The driver default is 30 s. |
| `target.catalog` | required | Target catalog. Must exist. |
| `target.schema` | required | Target schema. Created if missing. |
| `target.table` | required | Target table. |
| `sync.watermark_field` | `null` | A field holding BSON Dates, updated on every write. `null` re-reads the whole collection every run. |
| `sync.fields` | `[]` | Top-level fields to load. Their types are inferred from the sample. `_id`, `string_fields` and `watermark_field` are always kept. `[]` loads every field. |
| `sync.string_fields` | `[]` | Top-level fields read as text because their type differs between documents. |
| `sync.sample_size` | `10000` | Documents sampled on the first run to infer the schema. |
| `sync.overlap_seconds` | `300` | How far before the watermark an incremental run re-reads. |

Unknown keys are rejected, so a typo fails fast instead of being ignored.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `MongoAuthError: authentication failed` | Wrong user or password in the credential. |
| `TLS handshake aborted` / `SSLException: Received fatal alert: internal_error` | The cluster's outbound IP is not on the Atlas IP access list (step 3). |
| `Failed looking up TXT record` | A `mongodb+srv://` URI reached the executors. `run` resolves it on the driver; use `run` or call `resolve_srv` yourself. |
| `ClassNotFoundException: mongodb.DefaultSource` | The jars are not installed on the cluster (step 1). |
| `UnknownReason` on even a one-row read | The executors cannot use the connector: install the jars as cluster libraries and restart. |
| `UnknownReason` on a full read | A field holds different types in different documents. Add it to `sync.string_fields`, drop the table and run again. |
| An installation fails with "ongoing operation" | Only one library change runs at a time per cluster. Wait for the previous one. |
| `ModuleNotFoundError: aidp_connector_mongodb` | The wheel is not installed on the cluster the notebook is attached to (step 6). |

## Known limits

- **Deletes.** Merges never remove rows. A document deleted in MongoDB stays in the table until a full refresh (`MODE=full`).
- **Watermark field.** It must hold BSON Dates: a string field never matches the date filter, so incremental runs read nothing. It should be a last-modified time; a creation time does not catch edits. A document whose field is missing, null or not a Date is loaded by the first run only.
- **Schema.** It is inferred once, on the first run, from `sample_size` documents, then widened (decimals to precision 38, ints to longs), because the connector otherwise silently reads wider values as `null`. Later runs reuse the table's schema, so fields that first appear later are not added until a full refresh. The same goes for a change to `sync.fields`.
- **Checking for lost values.** Count on the written table. `df.filter(col.isNull())` on the DataFrame is pushed down to MongoDB, which counts only server-side nulls.
- **SRV options.** `srvMaxHosts` and `srvServiceName` are not supported.
- **One run at a time.** Two overlapping runs would merge into the same table.

## Validation status

- **Unit tests:** configuration, URI handling and redaction, SRV resolution, the watermark pipeline, schema widening, error explanation and the runner, offline with no network and no Spark.
- **Live run on AIDP, 2026-10-01 (Spark 3.5.0, Python 3.11.13):** the same read and write logic, then a single-file helper driven by a notebook, against a sample Atlas collection of about 41,000 documents with a read-only user and the URI from the Credential Store. The first run loaded every document; an incremental run merged with no duplicates.
- **Live run on AIDP, 2026-10-02 (Python 3.11):** this package form: the wheel and the combined jar `mongo-spark-connector-bundle_2.12-10.7.0.jar` installed as cluster libraries (no other MongoDB jars), the YAML config, `run`. Same sample collection (41,079 documents), read-only user, URI from the Credential Store, `watermark_field: date`, `fields: [name, text]` unless noted. All PASS:
  - first run: `mode full`, 41,079 rows, table columns exactly `_id, name, text, date` (the watermark field kept automatically);
  - rerun: `mode incremental`, 41,079 rows, `count(*) = count(DISTINCT _id)`;
  - `MODE=full` (set in a notebook cell, not a job parameter): `mode full refresh`, 41,079 rows;
  - `MODE=ful`: `ConfigError` raised before any connection;
  - `watermark_field: null`, into a new table: `mode full`, then `mode full re-read`, 41,079 rows both times, `count(*) = count(DISTINCT _id)`;
  - more than one executor: on a cluster with two workers, the collection read in 12 partitions (`PaginateBySizePartitioner`, 1 MB) was read by two executors on two hosts, 41,079 rows in total. With the default partitioner the collection is small enough to read on one executor.
- **Not yet run on AIDP:** the `MODE` job parameter read from a real job (no job set up; the notebook reads it with `oidlUtils.parameters.getParameter`, which Oracle documents for this, and a unit test covers the cell); a collection with changing data, so deletes purged by a full refresh.

## Development

```
aidp_connector_mongodb/
  pyproject.toml              package metadata (built with uv)
  aidp_connector_mongodb/     the package
    config.py                 load and validate mongodb_ingest.yaml
    auth.py                   read_uri(): Credential Store, then environment variable
    reader.py                 URI handling, SRV resolution, the MongoDB read, schema widening, errors
    runner.py                 run(): connection check, read, create or merge, summary
  mongodb_ingest.ipynb        sample notebook that runs a sync
  mongodb_ingest.sample.yaml  sample configuration
  build_connector_jar.py      merges the five connector jars into one (not in the wheel)
  tests/                      offline unit tests: no network, no Spark
```

Run the tests:

```bash
uv run --with pytest pytest
```
