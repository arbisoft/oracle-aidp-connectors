# aidp-connector-hubspot

A HubSpot connector for Oracle AI Data Platform (AIDP) Workbench. It copies HubSpot CRM contacts, companies, deals and the associations between them into Delta tables in an AIDP catalog, and keeps them up to date with incremental runs. It is read-only against HubSpot.

AIDP has no built-in HubSpot source. This connector is a small Python package that you install on an AIDP cluster and run from a notebook or a scheduled job.

## What it does

Sales and marketing data lives in HubSpot. Once it sits in Delta tables next to your other data, you can:

- query and report on it with SQL, for example pipeline value per deal stage,
- join it with data from other systems, for example billing or product usage,
- keep a queryable copy of the current HubSpot state outside HubSpot. The tables hold the latest version of each record, not its change history.

On each run the connector:

1. Reads `hubspot_ingest.yaml`: which HubSpot objects to ingest, the target catalog and schema, and the refresh mode.
2. Reads the HubSpot token from the AIDP Credential Store.
3. Calls the [HubSpot CRM API](https://developers.hubspot.com/docs/api/crm/understanding-the-crm) from the Spark driver, within a client-side rate limit, retrying throttled and failed requests.
4. Writes the results to Delta tables in `<catalog>.<schema>`, through a staging table so a failed read never leaves a target table half-written.
5. Records a watermark per object in `hubspot_sync_state`, so the next run reads only what changed.

```
HubSpot account ──REST API──▶ AIDP cluster (driver) ──▶ staging Delta table ──▶ <catalog>.<schema>.contacts
 (CRM objects the token        aidp_connector_hubspot                             .companies
  can read)                                                                       .deals
                                                                                  .associations
                                                                                  .hubspot_sync_state
```

It is plain Python, not a `type` of the built-in `aidataplatform` Spark format. The token is only sent to `api.hubapi.com` and is never logged or put in an error message.

## What you get

One Delta table per object in `<catalog>.<schema>`:

| Table | Holds | Key | Columns |
|---|---|---|---|
| `contacts` | Every contact, including archived ones | `id` | `id`, `created_at`, `updated_at`, `archived`, `archived_at`, `email`, `first_name`, `last_name`, `phone`, `lifecycle_stage`, `raw_json`, `_ingested_at` |
| `companies` | Every company, including archived ones | `id` | the same common columns, then `name`, `domain`, `industry`, `raw_json`, `_ingested_at` |
| `deals` | Every deal, including archived ones | `id` | the same common columns, then `deal_name`, `amount` as `DECIMAL(38,6)` in the deal's own currency, `currency`, `deal_stage`, `pipeline`, `close_date`, `owner_id`, `raw_json`, `_ingested_at` |
| `associations` | Links contacts to companies, deals to contacts and deals to companies | `from_object`, `from_id`, `to_object`, `to_id`, `association_type_id` | plus `category`, `label`, `_ingested_at` |
| `hubspot_sync_state` | One row per object: watermark and outcome of the last run | `object_name` | `watermark`, `last_mode`, `last_status`, `last_rows`, `last_run_at` |

`raw_json` holds the complete record with every fetched property, so custom properties can be extracted downstream with `get_json_object` or `from_json` (see [step 8](#8-query-the-data)). A typed value that cannot be parsed becomes NULL instead of failing the run.

## Refresh modes

| Object | `incremental` (default) | `full` |
|---|---|---|
| `contacts`, `companies`, `deals` | Searches the ids modified from `watermark - overlap_seconds` up to the run start, reads the full records in batches of 100 and merges them on `id`. Then flags archived and merged-away ids as `archived = true`, touching no other column. | Lists every live and archived record and overwrites the table. |
| `associations` | Always re-read in full from the ids in the `contacts` and `deals` tables (the sources of the three pairs), then overwritten when all three pairs were read. | Same. |

- **First run.** An object with no watermark is loaded in full, even in `incremental` mode.
- **Search paging.** HubSpot search returns at most 10,000 results per query. The connector restarts the query from the last id when it reaches that cap, so nothing is lost or duplicated.
- **Archived and merged records.** An incremental run lists archived ids and flags them. It also flags the ids listed in `hs_merged_object_ids`, because a merged-away id disappears from HubSpot without being archived.
- **Watermark.** The start time of the run, set only after the table write succeeds. A failed object keeps its old watermark and is retried over the same window. The other objects still run, `hubspot_sync_state` shows `last_status` per object, and `raise_on_failure` fails the job at the end if any object failed.
- **Associations.** Run them after the objects they start from, in the same run or later. Only pairs whose first object is in `sync.objects` are read. The table is overwritten only when all three pairs (contacts to companies, deals to contacts, deals to companies) are read. If a pair is skipped or its source table cannot be read, the other pairs are merged instead of overwritten, so links deleted in HubSpot stay in the table, and `associations` is reported as failed.
- **Staging.** Rows go to a staging Delta table first, then `MERGE` or `INSERT OVERWRITE` into the target keeping the newest row per key. The staging table is dropped even on failure.

## Using it in Oracle AIDP

You need a HubSpot account where you can create a service key or private app token, an AIDP workspace with a cluster you can install libraries on, outbound HTTPS access from the cluster to `api.hubapi.com`, and an existing catalog to write to. To build the wheel you need [uv](https://docs.astral.sh/uv/) on your machine.

### 1. Create a HubSpot token

Create a HubSpot service key, or a legacy private app token, with these scopes:

- `crm.objects.contacts.read`
- `crm.objects.companies.read`
- `crm.objects.deals.read`

The connector only reads, so no write scope is needed. Copy the token.

### 2. Store the token in AIDP

Add the token to the AIDP **Credential Store** as a **Secret Token** credential, for example named `hubspot_token` with the token under the key `secret`. The connector reads it at run time, so the token never appears in the notebook or the config file.

### 3. Build the package

From this folder, on your machine:

```bash
uv build
```

This writes `dist/aidp_connector_hubspot-<version>-py3-none-any.whl`.

### 4. Upload the files

Upload three files to a folder in your AIDP workspace, for example `/Workspace/Shared/hubspot/`:

| File | Purpose |
|---|---|
| `dist/aidp_connector_hubspot-<version>-py3-none-any.whl` | The connector package |
| `hubspot_ingest.ipynb` | Notebook that runs a sync |
| `hubspot_ingest.sample.yaml`, renamed to `hubspot_ingest.yaml` | Your configuration |

### 5. Install the package on the cluster

Pick one:

- **Cluster library (recommended for jobs):** add the wheel from the cluster **Library** tab and restart the cluster. Every notebook and job on that cluster can then `import aidp_connector_hubspot`.
- **Notebook-scoped:** in step 1 of `hubspot_ingest.ipynb`, uncomment the line `%pip install /Workspace/<path-to>/aidp_connector_hubspot-<version>-py3-none-any.whl` and set the path.

Either way, `requests` and `pyyaml` are installed as dependencies.

### 6. Configure

Edit `hubspot_ingest.yaml`. At a minimum set:

```yaml
hubspot:
  credential_name: hubspot_token   # the Credential Store entry from step 2
target:
  catalog: my_catalog              # must already exist
  schema: hubspot_raw              # created if missing
sync:
  mode: incremental
  objects: [contacts, companies, deals, associations]
```

Every key is described in the [configuration reference](#configuration-reference). The file holds no secret, so it is safe to version.

### 7. Run the notebook

Open `hubspot_ingest.ipynb`, attach it to the cluster, set `CONFIG_PATH` in step 2 to your `hubspot_ingest.yaml`, and run all cells. The notebook prints the installed package version, the target and objects, and finally one line per object:

An example of the summary after a first run:

```
object        mode         status       rows  watermark / note / error
contacts      full         SUCCESS       120  2026-10-02T08:14:00+00:00 | first run
companies     full         SUCCESS        35  2026-10-02T08:14:00+00:00 | first run
deals         full         SUCCESS        60  2026-10-02T08:14:00+00:00 | first run
associations  full         SUCCESS       250  2026-10-02T08:14:00+00:00 | re-read in full on every run
```

If any object fails, the last cell raises an error naming it. The other objects still complete. The first run is a full load; later runs read only what changed.

### 8. Query the data

```sql
-- Deals by stage
SELECT deal_stage, COUNT(*) AS deals, SUM(amount) AS amount
FROM my_catalog.hubspot_raw.deals
WHERE NOT archived
GROUP BY deal_stage;

-- A custom property pulled out of raw_json
SELECT id, email, get_json_object(raw_json, '$.properties.my_custom_property') AS my_custom_property
FROM my_catalog.hubspot_raw.contacts
WHERE NOT archived;

-- Contacts and the companies they belong to
SELECT c.email, co.name
FROM my_catalog.hubspot_raw.associations a
JOIN my_catalog.hubspot_raw.contacts c ON c.id = a.from_id
JOIN my_catalog.hubspot_raw.companies co ON co.id = a.to_id
WHERE a.from_object = 'contacts' AND a.to_object = 'companies' AND NOT c.archived AND NOT co.archived;

-- Outcome of the last run per object
SELECT * FROM my_catalog.hubspot_raw.hubspot_sync_state;
```

### 9. Schedule it

Create an AIDP job that runs `hubspot_ingest.ipynb` on the cluster where the package is installed:

- Run it as often as you need fresh data, for example hourly. Each run is incremental, as set in the config.
- Set the job's **maximum concurrent runs to 1**. Two overlapping runs would write the same tables and watermarks.
- For records that incremental runs cannot see (see [known limits](#known-limits)), add a second, less frequent schedule, for example weekly, with the job parameter `MODE` set to `full`. It overrides `sync.mode` for that run only.

## Using the package from your own code

The notebook is a thin wrapper. Any AIDP notebook or job with a Spark session can do the same:

```python
from aidp_connector_hubspot import format_summary, load_config, raise_on_failure, read_token, run

config = load_config("/Workspace/Shared/hubspot/hubspot_ingest.yaml")
token = read_token(config.hubspot, aidputils.secrets.get)

summaries = run(spark, config, token)            # or run(..., mode_override="full")
print(format_summary(summaries))
raise_on_failure(summaries)                      # fail the job if any object failed
```

- `run` returns one summary dict per object (`object`, `mode`, `status`, `rows`, `watermark`, `note`, `error`). One object failing does not stop the others.
- `parse_config` accepts an already-loaded dict in place of a YAML path.
- `read_token` reads `hubspot.credential_name` through the getter you pass (`aidputils.secrets.get` is a global the AIDP runtime provides in notebooks). It falls back to the `hubspot.token_env` environment variable when no credential is named or no getter is given, and raises `HubSpotTokenError` if neither yields a token.
- A HubSpot API failure on one object is reported in that object's summary as a `HubSpotError` message with the HTTP status and category, never the token.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `hubspot.credential_name` | none | Credential Store entry holding the token. |
| `hubspot.credential_key` | `secret` | Key inside that credential. |
| `hubspot.token_env` | `HUBSPOT_TOKEN` | Environment variable used when no credential is named. |
| `hubspot.api_version` | `2026-03` | Dated CRM API version used in every path. |
| `hubspot.requests_per_second` | `4` | Client-side throttle. HubSpot search allows 5 per second. |
| `target.catalog` | required | Target catalog. Must exist. |
| `target.schema` | required | Target schema. Created if missing. |
| `target.table_prefix` | empty | Prefix for every table name, e.g. `hubspot_` gives `hubspot_contacts`. |
| `sync.mode` | `incremental` | `incremental` or `full`. |
| `sync.objects` | required | Any of `contacts`, `companies`, `deals`, `associations`. |
| `sync.overlap_seconds` | `300` | How far before the watermark an incremental run re-reads. |

`catalog`, `schema` and `table_prefix` may contain only letters, digits and underscores. A placeholder such as `<CATALOG>` is rejected before any SQL runs. Unknown keys, unknown object names, a bad `mode`, a negative, `NaN`, infinite or too large `overlap_seconds` and a `requests_per_second` of 0 or less are rejected too, so a typo fails fast instead of being ignored.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `HubSpotTokenError: No HubSpot token found` | `hubspot.credential_name` is not set and the `HUBSPOT_TOKEN` environment variable is empty. |
| `HubSpotTokenError: Credential '...' has no value for key '...'` | The Credential Store entry exists but not under `hubspot.credential_key`. |
| An object fails with `HubSpot API error 401 (INVALID_AUTHENTICATION)` | The credential does not hold a valid token, or the service key or private app was removed. |
| An object fails with a 403 and `MISSING_SCOPES` | The token lacks one of the three read scopes (step 1). Add it or remove the object from `sync.objects`. |
| `associations` fails with *source table ... cannot be read, sync it first* | The `contacts` or `deals` table that a pair starts from does not exist yet or cannot be read. Run those objects first. The other pairs were still merged. |
| `ConfigError: ... Replace any placeholder` | A `<PLACEHOLDER>` value was left in `target`. |
| `ModuleNotFoundError: aidp_connector_hubspot` | The wheel is not installed on the cluster the notebook is attached to (step 5). |

## Known limits

- Search is eventually consistent. An incremental run misses an edit when HubSpot's search index lags the edit by more than `sync.overlap_seconds`. A full refresh picks it up. Raise `overlap_seconds` if you see this, and schedule a periodic full run.
- Contacts erased by a GDPR delete and records archived more than 90 days ago are never flagged by incremental runs. A full run clears them.
- Table schemas are fixed. If a later version changes a column, drop the tables and run a full load.
- Associations are rebuilt by every run that reads all three pairs, incremental runs included, not only by a full run. That is one batch call per 1,000 ids per pair. On large accounts run associations less often than the objects.
- Requests run on the Spark driver, one at a time. There is no checkpoint inside an object: if a load fails part-way, the next run starts that object again.
- When `sync.objects` leaves out a source object, or a source table is unreadable, associations are merged instead of overwritten. Links deleted in HubSpot stay in the table until a run reads all three pairs again.
- Run one instance at a time. Two overlapping runs would write the same tables and watermarks.
- Not covered: custom objects, tickets, owners, pipelines, engagements, property history and OAuth apps.
- CRM data includes names, emails and phone numbers. The tables inherit the catalog's access controls, so grant access accordingly.

## Validation

This package (`aidp-connector-hubspot` 0.1.0) ran live on AIDP on 2026-10-02 (Spark 3.5.0, Python 3.11, shared cluster) against a small HubSpot test account. The wheel was installed with `%pip install` from a workspace path, the token was read from the Credential Store, and the notebook and YAML config ran as shipped. Five runs passed: first load, rerun with no changes, rename a deal, delete a deal, and full refresh (`MODE` set to `full` in the notebook). See [`live-results/RESULTS.md`](live-results/RESULTS.md) for the counts.

Not run live: `MODE` as a real job parameter, a scheduled job, a contact merge, HubSpot's real 429 rate limit, accounts larger than a few dozen records, a non-empty deal currency, the 10,000-result search restart, and the failure paths. The offline tests (272) cover those that can run without HubSpot. The earlier single-file version (`hubspot_client.py`) also passed live runs, listed in the same file. [`LIVE_TEST_GUIDE.md`](LIVE_TEST_GUIDE.md) lists the steps to repeat the live run.

## Development

```
aidp_connector_hubspot/
  pyproject.toml                package metadata (built with uv)
  aidp_connector_hubspot/       the package
    config.py                   load and validate hubspot_ingest.yaml
    auth.py                     read_token(): Credential Store, then environment variable
    client.py                   HubSpot REST client: auth, throttling, retries, Retry-After, pagination
    extract.py                  generators over search, batch read, archived listing and associations
    records.py                  HubSpot records -> flat rows, table column specs
    load.py                     staged writes to Delta: merge, overwrite, flag archived
    state.py                    watermarks in hubspot_sync_state
    runner.py                   run(): full/incremental decision per object, summaries
  hubspot_ingest.ipynb          sample notebook that runs a sync
  hubspot_ingest.sample.yaml    sample configuration
  LIVE_TEST_GUIDE.md            steps for a live run on AIDP
  live-results/RESULTS.md       results of live runs
  tests/                        offline unit tests: no network, no Spark
```

The wheel contains only the `aidp_connector_hubspot` package. The notebook and sample config are not part of it.

Run the tests:

```bash
uv run --with pytest pytest
```

`uv run` installs the package and its dependencies into a local `.venv` first. Without uv: `pip install -e . pytest && pytest`.

The unit tests need no network and no Spark. They cover configuration, token lookup, the HTTP client, extraction, row mapping, the generated SQL, watermark handling and the full/incremental logic, using an in-memory HubSpot and in-memory tables.
