# HubSpot connector: live AIDP test results

The runs below used the earlier single-file version (`hubspot_client.py` with
`examples/hubspot_load.ipynb`). Both have since been replaced by the
`aidp_connector_hubspot` package and `hubspot_ingest.ipynb`, which keep the same
HubSpot calls, tables and logic. The packaged version has not been run live yet;
`../LIVE_TEST_GUIDE.md` describes how to repeat these runs with it.

## 2026-10-01: PASS for `hubspot_client.py` with `examples/hubspot_load.ipynb`

**What ran:** the example notebook, importing `hubspot_client.py` from a workspace
folder, on a shared AIDP cluster against a real, small HubSpot test account. The
token came from the AIDP Credential Store. Tables were written to a new schema.

**Cluster:** Spark 3.5.0, Python 3.11.

| Run | Check | Result |
|---|---|---|
| 1. First load | Contacts 14, companies 7, deals 45 (including archived), associations 113 rows; about 2 minutes | PASS |
| 2. Rerun, nothing changed | 0 rows for contacts, companies and deals; associations 113; watermark moved to the new run start | PASS |
| 3. Rename one deal in HubSpot, rerun | 1 deals row read and updated; table still 45 rows and 45 distinct ids | PASS |
| 4. Delete that deal in HubSpot, rerun | Deal flagged `archived` with its `archived_at`; associations 113 -> 111; 2 contacts rows also read, because HubSpot updates the modified date of contacts linked to a deleted deal | PASS |
| 5. Full refresh (`mode="full"`) | Contacts 14, companies 7, deals 45, associations 111 | PASS |

**Verified AIDP facts:**
- `requests` is already on the cluster: nothing was installed.
- The cluster reaches the HubSpot API over HTTPS, and the token read from the
  Credential Store worked as a Bearer token.
- Uploading the single `hubspot_client.py` file to a workspace folder and
  importing it with `sys.path.insert(0, HELPER_DIR)` works.
- Staging in a Delta table, then `MERGE` or `INSERT OVERWRITE`, works on the
  cluster, as does reading the watermark back with `unix_micros`.
- Rerunning only the sync cell advances the watermark, because `run_sync` takes
  the run start on every call.

**Not covered:** a contact merge, 429 rate-limit retries against HubSpot's real
limit, a scheduled job, accounts larger than a few dozen records, a non-empty deal
currency (the column is NULL in this account), and the 10,000-result search
restart (unit-tested offline only).

## 2026-10-01: PASS for the standalone sample notebook

The single-notebook version of this logic, `HubSpot.ipynb` for the AIDP samples
repo (arbisoft/oracle-aidp-samples#7), ran on the same cluster and account.
`hubspot_client.py` was written from that notebook's functions.

| Run | Check | Result |
|---|---|---|
| 1. First load | Contacts 14, companies 7, deals 45, associations 115 rows | PASS |
| 2. Rerun, nothing changed | 0 rows for contacts, companies and deals; associations 115 | PASS |
| 3. Rename one deal, rerun | 1 deals row read and updated, no duplicate | PASS |
| 4. Delete one deal, rerun | Deal flagged archived; associations 115 -> 113 | PASS |
| 5. Full refresh | Contacts 14, companies 7, deals 45, associations 113 | PASS |

Found during this run and fixed in the notebook: the run timestamp was set in the
configuration cell, so rerunning only the sync cell kept the old watermark.

## 2026-10-02: PASS after the review fixes

After the Copilot review, three fixes went into both `hubspot_client.py` and
`HubSpot.ipynb`: a `Retry-After` that is negative, NaN or infinite falls back to
backoff, `OVERLAP_SECONDS` is checked before any request, and reading or writing
the watermark now sits inside the per-object failure handling. Both versions were
rerun on the same cluster and account against their existing test schemas.

| Run | Check | Result |
|---|---|---|
| 1. Incremental run, helper and sample | Companies and deals 0 rows, associations 111, SUCCESS for all four objects; contacts 2 rows, changed in HubSpot since each schema's last run | PASS |
| 2. Rerun, nothing changed | 0 rows for contacts, companies and deals; associations 111; watermark moved | PASS |

**Not covered:** the new failure paths run only in the offline tests (211 pass).
