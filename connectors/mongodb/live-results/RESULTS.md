# MongoDB Atlas connector: live AIDP test results

## 2026-10-01 — PASS

**Environment:** AIDP cluster, Spark 3.5.0, Python 3.11.13, AMD workers
(2 OCPUs, 32 GB) autoscaling 1–10. Atlas M0, `sample_mflix.comments`,
read-only database user, the cluster's outbound IP (`/32`) on the Atlas IP
access list. URI from the AIDP Credential Store. Connector jars installed as
cluster libraries.

**What ran:** this `mongodb.py`, the same code under the name
`mongodb_client.py`, with a notebook that imports it and follows the same
steps as `examples/mongodb_collection_load.ipynb`.

| Check | Result |
|---|---|
| Connection check | PASS |
| Full load, first run | PASS — 41,079 documents written to Delta |
| Incremental run from the top | PASS — 1 document read, inside the 300 s overlap, and merged |
| Duplicates after the second run | PASS — 41,079 rows, 41,079 distinct `_id` |

**Verified AIDP facts:**
- Maven Central is reachable from the cluster.
- Loading the jars at runtime with `SparkContext.addJar` is not enough for a
  Spark DataSource: the driver can use the connector, but every executor task
  fails with `UnknownReason`, even a one-row read. Jars installed as cluster
  libraries plus a restart work.
- Only one library change runs at a time per cluster ("ongoing operation").
- Notebooks cannot prompt for input: `getpass` raises
  `StdinNotImplementedError`.
- `aidputils.secrets.get(name, key=None)` reads a Credential Store entry;
  without `key` it returns the whole map.
- The driver resolves SRV and TXT DNS records; executors cannot resolve the
  TXT record, so `resolve_srv` resolves the URI on the driver.
- On this cluster, executors used the same outbound IP as the driver.

**Not covered:** jars reaching more than one executor; a source with
changing data; OCI Vault credentials.
