# aidp-connector-notion

A Notion connector for Oracle AI Data Platform (AIDP) Workbench. It copies the content of a Notion workspace (pages, databases, page text and users) into Delta tables in an AIDP catalog, and keeps them up to date with incremental (CDC) runs.

AIDP has no built-in Notion source. This connector fills that gap: a small Python package that you install on an AIDP cluster and run from a notebook or a scheduled job.

## What it does

Teams keep a lot of knowledge in Notion: specs, runbooks, meeting notes, project trackers. Once that content sits in Delta tables next to your other data, you can:

- query and report on it with SQL, for example open tasks per owner from a Notion project database,
- join it with data from other systems,
- feed page text into search, embedding or RAG pipelines on AIDP,
- keep a history-friendly, queryable copy outside Notion.

On each run the connector:

1. Reads `notion_ingest.yaml`: which Notion objects to ingest, the target catalog and schema, and the refresh mode.
2. Reads the Notion integration token from the AIDP Credential Store.
3. Calls the [Notion REST API](https://developers.notion.com/reference/intro) from the Spark driver, within Notion's rate limit, retrying throttled and failed requests.
4. Writes the results to Delta tables in `<catalog>.<schema>`, through a staging table so a failed read never leaves a target table half-written.
5. Records a watermark per object in `notion_sync_state`, so the next run reads only what changed.

```
Notion workspace ──REST API──▶ AIDP cluster (driver) ──▶ staging Delta table ──▶ <catalog>.<schema>.pages
 (content shared                aidp_connector_notion                             .data_sources
  with the integration)                                                           .blocks
                                                                                  .users
                                                                                  .notion_sync_state
```

It is plain Python, not a `type` of the built-in `aidataplatform` Spark format.

## What you get

One Delta table per object in `<catalog>.<schema>`:

| Table | Holds | Key | Typed columns (plus `raw_json`, `_ingested_at`) |
|---|---|---|---|
| `pages` | Every page, including database rows | `id` | `created_time`, `last_edited_time`, `created_by_id`, `last_edited_by_id`, `in_trash`, `parent_type`, `parent_id`, `url`, `public_url`, `title` |
| `data_sources` | Notion databases (their data sources) | `id` | same common columns, `url`, `title` |
| `blocks` | The content of each page: paragraphs, headings, lists, to-dos, tables and so on, in reading order | `page_id`, `id` | same common columns, `type`, `has_children`, `page_id`, `depth`, `position` |
| `users` | Workspace members and bots | `id` | `type`, `name`, `avatar_url`, `email` |
| `notion_sync_state` | One row per object: watermark and outcome of the last run | `object_name` | `watermark`, `last_mode`, `last_status`, `last_rows`, `last_run_at`, `last_error` |

`raw_json` holds the complete API object, so page properties and block text can be pulled out downstream with `get_json_object` or `from_json` (see [step 8](#8-query-the-data)).

## Refresh modes

| Object | `cdc` (default) | `full` |
|---|---|---|
| `pages`, `data_sources` | Reads objects edited since the last watermark and merges them on `id`. Trashed objects are picked up and flagged `in_trash`. | Reloads everything and overwrites the table. |
| `blocks` | Re-reads the block tree of each changed page and replaces that page's rows, so deleted blocks disappear. Blocks of trashed pages are removed. | Walks every page and overwrites the table. |
| `users` | Always a full overwrite: Notion users carry no edit time. | Same. |

The first CDC run of an object has no watermark and behaves as a full load. A watermark moves forward only after the table write succeeds, so a failed run is retried over the same window.

## Using it in Oracle AIDP

You need a Notion workspace where you can create integrations, an AIDP workspace with a cluster you can install libraries on, and an existing catalog to write to. To build the wheel you need [uv](https://docs.astral.sh/uv/) on your machine.

### 1. Create a Notion integration

1. In Notion, open **Settings → Connections → Develop or manage integrations** and create an **internal** integration for your workspace.
2. Under capabilities, enable **Read content**. If you will ingest `users`, also enable **Read user information including email addresses**. The connector only reads, so no other capability is needed.
3. Copy the integration secret.

### 2. Share content with the integration

The Notion API only returns pages and databases shared with the integration. On each top-level page or database you want to ingest, open the **•••** menu → **Connections** and add the integration. Sharing a page also shares everything under it.

### 3. Store the secret in AIDP

Add the integration secret to the AIDP **Credential Store**, for example as a credential named `notion_token` with the secret under the key `secret`. The connector reads it at run time, so the token never appears in the notebook or the config file.

### 4. Build the package and upload the files

From this folder, on your machine:

```bash
uv build
```

This writes `dist/aidp_connector_notion-<version>-py3-none-any.whl`. Upload three files to a folder in your AIDP workspace, for example `/Workspace/Shared/notion/`:

| File | Purpose |
|---|---|
| `dist/aidp_connector_notion-<version>-py3-none-any.whl` | The connector package |
| `notion_ingest.ipynb` | Notebook that runs a sync |
| `notion_ingest.sample.yaml`, renamed to `notion_ingest.yaml` | Your configuration |

### 5. Install the package on the cluster

Pick one:

- **Cluster library (recommended for jobs):** add the wheel from the cluster **Library** tab and restart the cluster. Every notebook and job on that cluster can then `import aidp_connector_notion`.
- **Notebook-scoped:** in step 1 of `notion_ingest.ipynb`, uncomment the line `%pip install /Workspace/<path-to>/aidp_connector_notion-<version>-py3-none-any.whl` and set the path.

Either way, `requests` and `pyyaml` are installed as dependencies.

### 6. Configure

Edit `notion_ingest.yaml`. At a minimum set:

```yaml
notion:
  credential_name: notion_token   # the Credential Store entry from step 3
target:
  catalog: my_catalog             # must already exist
  schema: notion_raw              # created if missing
sync:
  mode: cdc
  objects: [pages, data_sources, blocks, users]
```

Every key is described in the [configuration reference](#configuration-reference). The file holds no secret, so it is safe to version.

### 7. Run the notebook

Open `notion_ingest.ipynb`, attach it to the cluster, set `CONFIG_PATH` in step 2 to your `notion_ingest.yaml`, and run all cells. The notebook prints the installed package version, the target and objects, and finally one line per object:

```
object        mode  status       rows  watermark / note / error
users         full  SUCCESS        42  no change tracking
data_sources  cdc   SUCCESS         3  2026-10-02T08:14:00+00:00
pages         cdc   SUCCESS        57  2026-10-02T09:02:00+00:00
blocks        cdc   SUCCESS      1893  2026-10-02T09:02:00+00:00
```

If any object fails, the last cell raises an error naming it. The other objects still complete. The first run is a full load; later runs read only what changed.

### 8. Query the data

```sql
-- Most recently edited pages
SELECT id, title, last_edited_time, in_trash
FROM my_catalog.notion_raw.pages
ORDER BY last_edited_time DESC LIMIT 20;

-- Rows of one Notion database, with a property pulled out of raw_json
SELECT title, get_json_object(raw_json, '$.properties.Status.status.name') AS status
FROM my_catalog.notion_raw.pages
WHERE parent_type = 'data_source_id' AND parent_id = '<data source id>' AND NOT in_trash;

-- Text of a page, in reading order
SELECT depth, position, type, get_json_object(raw_json, CONCAT('$.', type, '.rich_text[0].plain_text')) AS first_text
FROM my_catalog.notion_raw.blocks
WHERE page_id = '<page id>'
ORDER BY depth, position;

-- Outcome of the last run per object
SELECT * FROM my_catalog.notion_raw.notion_sync_state;
```

Property names such as `Status` are those of your own Notion database.

### 9. Schedule it

Create an AIDP job that runs `notion_ingest.ipynb` on the cluster where the package is installed:

- Run it as often as you need fresh data, for example hourly. Each run is CDC, as set in the config.
- Set the job's **maximum concurrent runs to 1**. Two overlapping runs would write the same tables and watermarks.
- For deletes, add a second, less frequent schedule, for example weekly, with the job parameter `MODE` set to `full`. It overrides `sync.mode` for that run only and purges pages that were permanently deleted or un-shared, which CDC cannot see.

## Using the package from your own code

The notebook is a thin wrapper. Any AIDP notebook or job with a Spark session can do the same:

```python
from aidp_connector_notion import format_summary, load_config, raise_on_failure, read_token, run

config = load_config("/Workspace/Shared/notion/notion_ingest.yaml")
token = read_token(config.notion, aidputils.secrets.get)

summaries = run(spark, config, token)            # or run(..., mode_override="full")
print(format_summary(summaries))
raise_on_failure(summaries)                      # fail the job if any object failed
```

- `run` returns one summary dict per object (`object`, `mode`, `status`, `rows`, `watermark`, `note`, `error`). One object failing does not stop the others.
- `parse_config` accepts an already-loaded dict in place of a YAML path.
- `read_token` reads `notion.credential_name` through the getter you pass (`aidputils.secrets.get` is a global the AIDP runtime provides in notebooks). It falls back to the `notion.token_env` environment variable when no credential is named or no getter is given, and raises `NotionTokenError` if neither yields a token.
- A token Notion rejects raises `NotionAuthError` before anything is written.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `notion.credential_name` | none | Credential Store entry holding the token. |
| `notion.credential_key` | `secret` | Key inside that credential. |
| `notion.token_env` | `NOTION_TOKEN` | Environment variable used when no credential is named. |
| `notion.api_version` | `2026-03-11` | Value of the `Notion-Version` header. |
| `notion.requests_per_second` | `3` | Client-side throttle. Notion allows 3 per integration, more on Business and Enterprise plans. |
| `target.catalog` | required | Target catalog. Must exist. |
| `target.schema` | required | Target schema. Created if missing. |
| `target.table_prefix` | empty | Prefix for every table name, e.g. `notion_` gives `notion_pages`. |
| `sync.mode` | `cdc` | `cdc` or `full`. |
| `sync.objects` | required | Any of `pages`, `data_sources`, `blocks`, `users`. |
| `sync.overlap_seconds` | `120` | How far before the watermark CDC re-reads. Keep it at 120 or more. |

Unknown keys and unknown object names are rejected, so a typo fails fast instead of being ignored.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `NotionAuthError: Notion rejected the token (401 ...)` | The credential does not hold a valid integration secret, or the integration was removed from the workspace. |
| `NotionTokenError: No Notion token found` | `notion.credential_name` is not set and the `NOTION_TOKEN` environment variable is empty. |
| `pages` succeeds with note *no pages visible* | Nothing is shared with the integration yet (step 2). |
| `users` fails with a 403 | The integration lacks the *Read user information* capability. Enable it or remove `users` from `sync.objects`. |
| `blocks` note *N page(s) skipped* | Those pages were deleted or un-shared between discovery and reading their content. The next run catches up. |
| `ModuleNotFoundError: aidp_connector_notion` | The wheel is not installed on the cluster the notebook is attached to (step 5). |

## Known limits

- **Speed.** Requests run on the Spark driver, one at a time, within Notion's rate limit. Blocks need at least one request per page: a first load of 10,000 pages takes roughly an hour at 3 requests/second. There is no checkpoint inside an object: if a load fails part-way, the next run starts that object again. Server errors and dropped connections are retried for about a minute and a half before a request gives up.
- **Deletes.** CDC sees trashing, but not permanent deletion or content that was un-shared from the integration. Run a full refresh periodically to purge.
- **Users.** No change tracking, and Notion's user list does not include guests.
- **Comments** are not ingested.
- **Search lag.** Notion search is eventually consistent. An edit made shortly before a run arrives on the next run, as long as it is indexed within `sync.overlap_seconds` of any newer edit the connector has already seen. An edit indexed later than that is missed by CDC and only picked up by a full refresh. Raise `overlap_seconds` if you see this (each run then re-reads more), and schedule a periodic full refresh.
- **One run at a time.** Do not let two runs for the same target overlap: set the job's concurrency to 1. Staging tables are per run, but both runs would still write the same target tables and watermarks.
- **Unreadable nested blocks.** If Notion returns 403 or 404 for the children of a block inside a readable page, that block is kept and its children are skipped.
- **Block replacement** in CDC is a delete followed by an insert. If a run dies between the two, the watermark is not advanced and the next run repeats it.
- **Synced blocks.** The original synced block's children are ingested; references to it are recorded without their children.
- **Schema changes.** Table schemas are fixed. If a later version adds a column, drop the tables and run a full refresh.

## Development

```
aidp_connector_notion/
  pyproject.toml              package metadata (built with uv)
  aidp_connector_notion/      the package
    config.py                 load and validate notion_ingest.yaml
    auth.py                   read_token(): Credential Store, then environment variable
    client.py                 Notion REST client: auth, pagination, throttling, retries
    extract.py                generators over search, block children and users
    records.py                Notion objects -> flat rows, table column specs
    load.py                   staged writes to Delta: overwrite, merge, per-page replace
    state.py                  watermarks in notion_sync_state
    runner.py                 run(): full/CDC decision per object, summaries
  notion_ingest.ipynb         sample notebook that runs a sync
  notion_ingest.sample.yaml   sample configuration
  tests/                      offline unit tests: no network, no Spark
```

The wheel contains only the `aidp_connector_notion` package. The notebook and sample config are not part of it.

Run the tests:

```bash
uv run --with pytest pytest
```

`uv run` installs the package and its dependencies into a local `.venv` first. Without uv: `pip install -e . pytest && pytest`.

The unit tests need no network and no Spark. They cover configuration, token lookup, the HTTP client, extraction, row mapping, the generated SQL, watermark handling and the full/CDC logic, using an in-memory Notion and in-memory tables.
