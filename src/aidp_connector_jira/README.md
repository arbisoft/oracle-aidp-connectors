# aidp-connector-jira

A Jira Cloud connector for Oracle AI Data Platform (AIDP) Workbench. It copies the Jira issues matching a JQL query into a Delta table in an AIDP catalog, and keeps the table up to date with incremental runs.

AIDP has no built-in Jira source. This connector fills that gap: a small Python package that you install on an AIDP cluster and run from a notebook or a scheduled job.

## What it does

On each run the connector:

1. Reads `jira_ingest.yaml`: the JQL query, the target catalog, schema and table, and the read settings.
2. Reads the Jira site, email and API token from the AIDP Credential Store.
3. Checks the credentials with one request that also returns the account's timezone: Jira reads date-times in a query in that timezone, not UTC.
4. Searches the [Jira Cloud REST API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/) from the Spark driver, paging with the token Jira returns and retrying rate limits, 502/503/504 and network errors a bounded number of times.
5. Creates the table on the first run, or merges the new and changed issues into it on `key`.

```
Jira Cloud ──REST API──▶ AIDP cluster (driver) ──▶ <catalog>.<schema>.<table>
```

It is plain Python on the driver, not a `type` of the built-in `aidataplatform` Spark format. It needs only `requests`, which AIDP clusters already have.

## What you get

One Delta table per configuration, `<catalog>.<schema>.<table>`, with one row per issue:

| Column | Type | From |
|---|---|---|
| `key` | string | Issue key, e.g. `PROJ-1`. The merge key. |
| `summary` | string | Title. |
| `status`, `priority`, `issuetype` | string | Their names. |
| `assignee`, `reporter` | string | Display names. |
| `project` | string | Project key. |
| `created`, `updated` | timestamp | `updated` is the watermark. |
| `raw_fields` | string | JSON of the fields in `sync.extra_fields`, e.g. custom fields. Null when none are configured. |

## Refresh

| Run | What it reads | How it writes |
|---|---|---|
| First (no table yet) | Every issue matching `sync.jql`. | Creates the table. |
| Later | Issues updated since the table's newest `updated`, minus `overlap_seconds`. | Merges on `key`. |
| Full refresh (job parameter `MODE=full`, or `run(..., "full")`) | Every matching issue. | Overwrites the table, which purges issues deleted in Jira or no longer matching the query. |

## Using it in Oracle AIDP

You need a Jira Cloud site, an AIDP workspace with a cluster you can install libraries on, and an existing catalog to write to. To build the wheel you need [uv](https://docs.astral.sh/uv/) on your machine.

### 1. Create an API token

Create an [API token](https://id.atlassian.com/manage/api-tokens) for an Atlassian account that can see the issues to load. The connector only reads.

### 2. Store the credentials in AIDP

Add a **Secret Token** credential to the AIDP **Credential Store**, for example named `jira`, with three keys:

| Key | Value |
|---|---|
| `site` | `<site>.atlassian.net`. Any other host is rejected, because the API token is sent to it. |
| `email` | The Atlassian account's email. |
| `token` | The API token. |

### 3. Build the package and upload the files

From this folder, on your machine:

```bash
uv build
```

This writes `dist/aidp_connector_jira-<version>-py3-none-any.whl`. Upload three files to a folder in your AIDP workspace:

| File | Purpose |
|---|---|
| `dist/aidp_connector_jira-<version>-py3-none-any.whl` | The connector package |
| `jira_ingest.ipynb` | Notebook that runs a sync |
| `jira_ingest.sample.yaml`, renamed to `jira_ingest.yaml` | Your configuration |

### 4. Install the package on the cluster

- **Cluster library (recommended for jobs):** add the wheel from the cluster **Library** tab and restart the cluster.
- **Notebook-scoped:** in step 1 of `jira_ingest.ipynb`, uncomment the `%pip install` line and set the path.

### 5. Configure

Edit `jira_ingest.yaml`. At a minimum set:

```yaml
jira:
  credential_name: jira         # the Credential Store entry from step 2
target:
  catalog: my_catalog           # must already exist
  schema: jira_raw              # created if missing
  table: jira_issue
sync:
  jql: "project = PROJ"          # no ORDER BY
```

Every key is described in the [configuration reference](#configuration-reference). The file holds no secret.

### 6. Run the notebook

Open `jira_ingest.ipynb`, attach it to the cluster, set the path in step 2 to your `jira_ingest.yaml`, and run all cells. Step 5 prints:

```
connection ok; account timezone Europe/London
jql           project = PROJ
table         my_catalog.jira_raw.jira_issue
mode          incremental
since         2026-10-02T08:14:00+00:00
rows_read     3
rows_in_table 412
```

### 7. Schedule it

Create an AIDP job that runs `jira_ingest.ipynb` on the cluster where the package is installed, and set its **maximum concurrent runs to 1**.

For deletes, add a second, less frequent schedule, for example weekly, with the job parameter `MODE` set to `full`. It overrides the normal run for that run only.

### Loading several queries

One configuration loads one query into one table. To load several, keep one config file per query and run the notebook once per file, or schedule one job each.

## Using the package from your own code

```python
from aidp_connector_jira import format_summary, load_config, preview, read_credentials, run

config = load_config("/Workspace/<path-to>/jira_ingest.yaml")
creds = read_credentials(config.jira, aidputils.secrets.get)

preview(spark, config, creds).show()            # first 10 matching issues, writes nothing
summary = run(spark, config, creds)              # or run(..., "full")
print(format_summary(summary))
```

- `run` returns a summary dict (`jql`, `table`, `mode`, `since`, `rows_read`, `rows_in_table`) and raises on failure. Its optional mode is `"full"` to overwrite the table; `None`, empty or `"incremental"` is the normal run; any other value raises `ConfigError` before any request.
- `preview(spark, config, creds, limit=10)` returns the first `limit` matching issues as a DataFrame, fetching only the pages it needs.
- `parse_config` accepts an already-loaded dict in place of a YAML path.
- `read_credentials` reads the keys `site`, `email` and `token` of `jira.credential_name` through the getter you pass. It falls back to the `JIRA_SITE`, `JIRA_EMAIL` and `JIRA_API_TOKEN` environment variables when no credential is named or no getter is given.
- Errors are `JiraError`, `JiraAuthError` for HTTP 401/403, or `JiraRateLimitError` when 429 persists. Their messages never contain the email or the token.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `jira.credential_name` | none | Credential Store entry with keys `site`, `email` and `token`. |
| `target.catalog` | required | Target catalog. Must exist. |
| `target.schema` | required | Target schema. Created if missing. |
| `target.table` | required | Target table. |
| `sync.jql` | required | Which issues to load. No `ORDER BY`: the connector adds its own. |
| `sync.extra_fields` | `[]` | Other Jira field IDs to load, e.g. `customfield_10016`, as JSON in `raw_fields`. |
| `sync.page_size` | `100` | Issues per search request, 1 to 5000. |
| `sync.overlap_seconds` | `300` | How far before the watermark an incremental run re-reads. |

Unknown keys are rejected, so a typo fails fast instead of being ignored.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `JiraAuthError: HTTP 401` or `403` | Wrong email or token in the credential, or the account cannot see the project. |
| `ValueError: site must be a Jira Cloud host name` | The credential's `site` is not `<site>.atlassian.net`. |
| `JiraError: HTTP 400 from the API` | The JQL is invalid; the message includes Jira's explanation. |
| `ModuleNotFoundError: aidp_connector_jira` | The wheel is not installed on the cluster the notebook is attached to (step 4). |

## Known limits

- **Deletes.** An incremental run does not see issues deleted in Jira. A full refresh removes them.
- **Minute precision.** JQL bounds are rounded to the minute, so an edit made less than a minute before a run is picked up by the next run, not that one. Nothing is lost: the next run starts from the newest `updated` in the table, minus the overlap.
- **Daylight saving.** Bounds are written in the account's timezone; around a clock change the ambiguous hour can shift a bound. The default 300-second overlap covers normal cases only.
- **Memory.** A run holds every issue it reads in driver memory. For a very large first load, narrow `sync.jql` and load in windows.
- **Custom fields** are JSON in `raw_fields`, not typed columns.
- **`ORDER BY` check.** It is a plain text check, so a query with `order by` inside a quoted string is rejected.
- **One run at a time.** Two overlapping runs would merge into the same table.

## Validation status

- **Unit tests:** configuration, credential lookup, the HTTP retry engine, JQL building, paging, row mapping and the runner, offline with no network and no Spark.
- **Live runs on AIDP, 2026-10-01:** the earlier single-file version (`jira_client.py` and an example notebook), against a real Jira Cloud site, credentials from the Credential Store. Full load into a new table matched Jira's count for the query; a rerun re-read only issues inside the overlap; an issue edited in Jira was updated on the next run; a rerun with a very large overlap re-read every issue and merged with no duplicate keys. Verified there: `requests` is already on the cluster, the cluster reaches Jira Cloud over HTTPS, and an edit less than a minute before a run arrives on the next run.
- **Live run on AIDP, 2026-10-05 (Python 3.11):** this package form: the wheel installed as a cluster library, the YAML config, `run`, credentials from the Credential Store, a Jira Cloud site whose account timezone is not UTC. All PASS:
  - first run into a new table: row count equal to Jira's count for the query, `count(*) = count(DISTINCT key)`;
  - one issue edited in Jira, then a rerun: `mode incremental`, `since` exactly the newest `updated` in the table (the watermark read with `unix_micros`, no session timezone set), only the edited issue and issues inside the overlap read, the edited row updated, row count unchanged with no duplicates;
  - `MODE=full` (set in a notebook cell, not a job parameter): `mode full refresh`, every matching issue read, row count unchanged;
  - `MODE=ful`: `ConfigError` raised before any request;
  - the `oracle-aidp-samples` Jira notebook, which calls this package: `preview` showed the first matching issues without writing; a first load into a new table matched Jira's count; a rerun was incremental with the row count unchanged.
- **Not yet run on AIDP:** `sync.extra_fields`; the `MODE` job parameter read from a real job; issues deleted in Jira purged by a full refresh.

## Development

```
aidp_connector_jira/
  pyproject.toml              package metadata (built with uv)
  aidp_connector_jira/        the package
    config.py                 load and validate jira_ingest.yaml; the MODE job parameter
    auth.py                   read_credentials(): Credential Store, then environment variables
    client.py                 HTTP retries, site check, JQL, account timezone, paged search
    records.py                issues -> rows and a DataFrame
    errors.py                 JiraError, JiraAuthError, JiraRateLimitError
    runner.py                 run() and preview()
  jira_ingest.ipynb           sample notebook that runs a sync
  jira_ingest.sample.yaml     sample configuration
  tests/                      offline unit tests: no network, no Spark
```

Run the tests:

```bash
uv run --with pytest pytest
```
