# MongoDB Atlas connector: live AIDP test guide

For whoever has hands-on AIDP access. It repeats the run that passed on
2026-10-01 (`live-results/RESULTS.md`), e.g. after a code change or on a new
cluster. Never commit a real hostname, instance URL, IP address, OCID,
catalog name or credential, including in a saved notebook output.

## What you need

- An AIDP workspace with a running cluster and notebook access, and
  permission to install cluster libraries and create Credential Store entries.
- A MongoDB Atlas cluster. A free M0 is enough: create it at
  `mongodb.com/cloud/atlas` and click **Load Sample Dataset**.
- A **read-only** database user: **Database Access → Add New Database User**,
  password auth, built-in role **Only read any database**.
- The connection string from **Connect → Drivers**:
  `mongodb+srv://<user>:<password>@<cluster>.mongodb.net/`. URL-encode any
  `@ : / ? # %` in the password.

## Step 1 — Install the connector jars (once per cluster)

1. Download the five jars listed in `README.md` and check their SHA-256.
2. Upload them through the AIDP workspace UI into a folder.
3. Cluster → **Library** tab → **Install Library** → **Workspace** → select a
   jar → **Install**. One at a time: a second install while one is running
   fails with "ongoing operation" (see the AsyncOperations view).
4. When all five are listed, **Actions → Restart**.

## Step 2 — Store the URI

Credential Store → **Create → Credentials** → type **Secret Token**, a name
(e.g. `mongodb_atlas`), key `uri`, value: the connection string. Creating one
needs the `CREATE_CREDENTIAL` permission on the Master Catalog.

## Step 3 — Atlas IP access list

In a scratch cell, print the driver's and the executors' outbound IPs:

```python
def outbound_ip(_=None):
    import urllib.request
    return urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()

print("driver:", outbound_ip())
print("executors:", sorted(set(spark.sparkContext.parallelize(range(16), 16).map(outbound_ip).collect())))
```

Add each IP under Atlas **Network Access** with `/32` and wait for **Active**.
Delete the cell's output; never commit the IPs. Repeat after a restart.

## Step 4 — Run the notebook

1. Upload `connectors/mongodb/mongodb.py` and
   `examples/mongodb_collection_load.ipynb` to a workspace folder.
2. Fill in the configuration cell: `HELPER_DIR` (the folder holding
   `mongodb.py`), `CREDENTIAL_NAME`, `TARGET` (`<catalog>.<schema>.mongodb_comments`;
   the schema must exist), `DATABASE = "sample_mflix"`,
   `COLLECTION = "comments"`, `WATERMARK_FIELD = "date"`.
3. Run top to bottom. **Check the Connection** must print `connection ok`;
   the write prints `documents in target: 41079`.
4. Run the whole notebook a second time.

If you re-upload `mongodb.py` without restarting, reload it:
`import importlib, mongodb as m; m = importlib.reload(m)`.

## Step 5 — Check

1. After the first run: 41,079 rows.
2. After the second run: `SELECT count(*), count(DISTINCT _id) FROM <TARGET>`
   returns two equal numbers, still 41,079. Optionally print `df.count()`
   after the read cell on the second run: it should be tiny (1 on 2026-10-01,
   the newest document inside the 300 s overlap).
3. Nothing printed the URI, password, cluster host, IP or a storage path.

## Step 6 — Report

Send: which checks passed, any error text with hosts, IPs, users, passwords
and `oci://` paths removed, `spark.version`, the Python version, the worker
shape and count, and whether the access list held specific IPs or
`0.0.0.0/0`. It goes into `live-results/RESULTS.md` as a dated row.

## If something goes wrong

The helper's errors start with a hint when they recognise the failure:

| Message starts with | Cause |
|---|---|
| `MongoDB Spark connector is not installed` | The jars are not cluster libraries, or the cluster was not restarted after installing them. |
| `a Spark task failed and Spark could not report why` | If even `df.limit(1).collect()` fails, the executors lack the jars. Otherwise a field's type differs from the sample: use `STRING_FIELDS`. |
| `DNS TXT lookup failed` | The URI reached the executors unresolved: call `m.resolve_srv(spark, uri)` before reading. |
| `TLS handshake aborted … IP access list` | An outbound IP is missing from Atlas, or was added less than a couple of minutes ago. |
| `authentication failed` | Wrong user or password, or unencoded special characters in the password. |
| `SRV DNS lookup failed` | Wrong host, or the driver cannot resolve DNS SRV records. |
