# oracle-aidp-connectors

Connectors for Oracle AI Data Platform (AIDP) Workbench that load data from sources AIDP has no built-in connector for.

Each connector is a self-contained, installable Python package with its own README and a sample notebook. Build its wheel, install it on an AIDP cluster, and run the notebook interactively or as a scheduled job to land the source's data in Delta tables in your catalog.

Independent Arbisoft project, not affiliated with or endorsed by Oracle.

## Connectors

| Connector | Source | Ingests | Refresh | Status | Docs |
|---|---|---|---|---|---|
| [`aidp-connector-notion`](src/aidp_connector_notion) | Notion | Pages, databases (data sources), page content (blocks), users | Full and CDC | Unit-tested; run on a live AIDP cluster | [README](src/aidp_connector_notion/README.md) |
| [`aidp-connector-hubspot`](src/aidp_connector_hubspot) | HubSpot | Contacts, companies, deals, associations | Full and incremental | Unit-tested | [README](src/aidp_connector_hubspot/README.md) |

## Repository layout

Every connector lives in its own folder under `src/` and does not depend on the others:

```
oracle-aidp-connectors/
  README.md                       this file
  LICENSE
  src/
    aidp_connector_<source>/      one folder per connector
      README.md                   what it does, setup in AIDP, configuration, known limits
      pyproject.toml              package metadata and dependencies
      aidp_connector_<source>/    the Python package
      <source>_ingest.ipynb       sample notebook that runs the connector
      <source>_ingest.sample.yaml sample configuration
      tests/                      offline unit tests: no network, no Spark
```

## Using a connector in AIDP

The steps are the same for every connector. The connector's README has the details: source-side setup, configuration keys and what lands in which table.

1. **Build the wheel** from the connector's folder:

   ```bash
   cd src/aidp_connector_<source>
   uv build
   ```

   This writes `dist/aidp_connector_<source>-<version>-py3-none-any.whl`. It needs [uv](https://docs.astral.sh/uv/).
2. **Upload** the wheel, the sample notebook and the sample config to your AIDP workspace.
3. **Store credentials** for the source in the AIDP Credential Store. Connectors read secrets at run time, never from the notebook or the config file.
4. **Install** the wheel on the cluster from its **Library** tab, or with `%pip install <path-to-wheel>` in the notebook.
5. **Configure** your copy of the sample config: target catalog and schema, and what to ingest.
6. **Run** the sample notebook, then schedule it as an AIDP job.

## Adding a connector

Create `src/aidp_connector_<source>/` following the layout above, and keep to these conventions:

- **Naming.** Folder and import name `aidp_connector_<source>`, distribution name `aidp-connector-<source>`.
- **Self-contained.** Its own `pyproject.toml` (built with `uv_build`) and dependencies. No imports from other connectors.
- **README.md.** What the connector does, the tables it writes, step-by-step setup in AIDP, a configuration reference, known limits, and validation status.
- **Sample notebook.** `<source>_ingest.ipynb`, committed with outputs cleared and no secrets. It installs or imports the package, loads the config, reads credentials from the Credential Store and runs the sync.
- **Sample config.** `<source>_ingest.sample.yaml`, holding no secrets.
- **Tests.** Offline unit tests under `tests/`, with no network and no Spark, that run with `uv run --with pytest pytest`.
- **Listing.** Add a row to the [Connectors](#connectors) table. Mark a connector as live-validated only after it has been run against a real source from an AIDP workspace.

## Running tests

Tests run per connector, from its folder:

```bash
cd src/aidp_connector_<source>
uv run --with pytest pytest
```

## Licence

MIT, see `LICENSE`.
