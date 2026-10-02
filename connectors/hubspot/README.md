# HubSpot connector

Load HubSpot CRM contacts, companies, deals and the associations between them into Delta tables on Oracle AI Data Platform (AIDP) Workbench with `hubspot_client.py`. The first run of each object is a full load; later runs are incremental, using a per-object watermark kept in a state table. Read-only against HubSpot.

Status: live PASS on AIDP, 2026-10-01 (Spark 3.5.0, Python 3.11): first load, rerun with no changes, edit, delete and full refresh. See `live-results/RESULTS.md` for what was not covered, and `LIVE_TEST_GUIDE.md` to repeat it.

## Contents

| Path | Purpose |
|---|---|
| `hubspot_client.py` | Single, self-contained helper module the notebook imports: throttled HubSpot client with bounded retry on 429 and 500/502/503/504, id search with keyset paging past HubSpot's 10,000-result cap, batch read, archived listing, association reads, row mapping, Delta writer, state table and `run_sync`. Needs only `requests`. |
| `examples/hubspot_load.ipynb` | Example notebook: token from the Credential Store, then `run_sync`. |
| `tests/` | Unit tests using fakes; no network, Spark or credentials needed. |

## Requirements

- A HubSpot service key, or a legacy private app token, with the scopes `crm.objects.contacts.read`, `crm.objects.companies.read` and `crm.objects.deals.read`.
- On AIDP: a **Secret Token** credential in the AIDP Credential Store holding the token under one key. The notebook reads it with `aidputils.secrets.get` and passes it to `run_sync`. The token is only sent to `api.hubapi.com` and never logged or put in an error message.
- `requests` on the cluster; it is already installed on AIDP.
- Outbound HTTPS access from the cluster to `api.hubapi.com`.
- The target catalog must exist. The schema is created if missing.

## Usage

1. Upload `hubspot_client.py` to a workspace folder.
2. Create the Credential Store entry.
3. Open `examples/hubspot_load.ipynb`, set `HELPER_DIR`, `CREDENTIAL_NAME`, `CREDENTIAL_KEY`, `CATALOG` and `SCHEMA`, and run the cells.

The notebook puts `HELPER_DIR` on `sys.path` and runs `import hubspot_client as h`, then `h.run_sync(spark, token, CATALOG, SCHEMA, ...)`.

Options of `run_sync`:

| Option | Default | Meaning |
|---|---|---|
| `table_prefix` | empty | Prefix for every table name. Letters, digits and underscores. |
| `objects` | all four | Any of `contacts`, `companies`, `deals`, `associations`. |
| `mode` | `incremental` | `incremental` merges changes since the watermark; `full` overwrites the tables. |
| `overlap_seconds` | `300` | How far before the watermark an incremental run re-reads. |
| `requests_per_second` | `4.0` | Client-side throttle. HubSpot search allows 5 per second. |
| `api_version` | `2026-03` | Dated CRM API version used in every path. |

`catalog`, `schema` and `table_prefix` may contain only letters, digits and underscores. A placeholder such as `<CATALOG>` is rejected before any SQL runs, as are an unknown `mode`, an unknown object name and a negative `overlap_seconds`.

## What you get

One Delta table per object in `<catalog>.<schema>`:

| Table | Key | Content |
|---|---|---|
| `contacts` | `id` | `id`, `created_at`, `updated_at`, `archived`, `archived_at`, `email`, `first_name`, `last_name`, `phone`, `lifecycle_stage`, `raw_json`, `_ingested_at` |
| `companies` | `id` | the same common columns, then `name`, `domain`, `industry`, `raw_json`, `_ingested_at` |
| `deals` | `id` | the same common columns, then `deal_name`, `amount` as `DECIMAL(38,6)` in the deal's own currency, `currency`, `deal_stage`, `pipeline`, `close_date`, `owner_id`, `raw_json`, `_ingested_at` |
| `associations` | `from_object`, `from_id`, `to_object`, `to_id`, `association_type_id` | plus `category`, `label`, `_ingested_at` |
| `hubspot_sync_state` | `object_name` | `watermark`, `last_mode`, `last_status`, `last_rows`, `last_run_at` |

`raw_json` holds the complete record with every fetched property, so custom properties can be extracted downstream with `get_json_object` or `from_json`. A typed value that cannot be parsed becomes NULL instead of failing the run.

## How a run works

- **First run, or `mode="full"`:** lists every live and archived record and overwrites the table.
- **Incremental run:** searches the ids modified from `watermark - overlap_seconds` up to the run start, reads the full records in batches of 100, and merges them on `id`. Search paging restarts from the last id when it reaches HubSpot's 10,000-result cap.
- **Archived and merged records:** an incremental run lists archived ids and flags them `archived = true`, and flags ids listed in `hs_merged_object_ids`, because a merged-away id disappears from HubSpot without being archived. No other column is touched.
- **Watermark:** the start time of the run, set only after the table write succeeds. A failed object keeps its old watermark and is retried over the same window, and the other objects still run. `hubspot_sync_state` shows `last_status` per object, and the run raises at the end if any object failed.
- **Associations:** always re-read in full from the ids of the synced `contacts`, `companies` and `deals` tables, for contacts to companies, deals to contacts and deals to companies. Run them after those objects. Only pairs whose first object is in `objects` are read; if a pair is skipped or its source table cannot be read, the other pairs are merged instead of overwritten and `associations` is reported as failed.
- **Staging:** rows go to a staging Delta table first, then `MERGE` or `INSERT OVERWRITE` into the target keeping the newest row per key, and the staging table is dropped even on failure.

## Run the tests

From the repository root:

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Known limits

- Search is eventually consistent. An edit indexed later than `overlap_seconds` after the run start is missed by incremental runs and only picked up by a full run. Raise `overlap_seconds` if you see this, and schedule a periodic full run.
- Contacts erased by a GDPR delete and records archived more than 90 days ago are never flagged by incremental runs. A full run clears them.
- Table schemas are fixed. If a later version changes a column, drop the tables and run a full load.
- Every association link is re-read on every run: one batch call per 1,000 ids per pair. On large accounts run associations less often than the objects.
- Requests run on the Spark driver, one at a time. There is no checkpoint inside an object: if a load fails part-way, the next run starts that object again.
- When `objects` leaves out a source object, or a source table is unreadable, associations are merged instead of overwritten. Links deleted in HubSpot for the pairs that were refreshed are not removed until the next full run of all objects.
- Run one instance at a time. Two overlapping runs would write the same tables and watermarks.
- Not covered: custom objects, tickets, owners, pipelines, engagements, property history and OAuth apps.
- CRM data includes names, emails and phone numbers. The tables inherit the catalog's access controls, so grant access accordingly.
