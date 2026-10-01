# Jira Cloud connector

Load Jira Cloud issues into a Delta table on Oracle AI Data Platform (AIDP) Workbench with `jira_client.py`. The first run is a full load; later runs are incremental, using the newest `updated` value already in the target table as the watermark. Read-only against Jira.

Status: live PASS on AIDP, 2026-10-01. See `live-results/RESULTS.md` and `LIVE_TEST_GUIDE.md`.

## Contents

| Path | Purpose |
|---|---|
| `jira_client.py` | Single, self-contained helper module the notebook imports: bounded retry on 429 and 502/503/504, credential checks for Credential Store values, or lookup from OCI Vault then environment, JQL paging, and DataFrame conversion. Needs only `requests`, plus `oci` if you use OCI Vault. |
| `examples/jira_issue_load.ipynb` | Example notebook: read issues, then write or `MERGE` into Delta. |
| `tests/` | Unit tests using fakes; no network, Spark or credentials needed. |

## Requirements

- A Jira Cloud site and an [API token](https://id.atlassian.com/manage/api-tokens). `JIRA_SITE` must be a `<site>.atlassian.net` host; any other host is rejected, because the API token is sent to it.
- On AIDP: a **Secret Token** credential in the AIDP Credential Store with keys `site`, `email` and `token`. The notebook reads them with `aidputils.secrets.get` and passes them to `j.credentials(site, email, api_token)`, which strips them and checks the site. The AIDP cluster tested had no environment-variable setting.
- Outside AIDP: `j.credentials_from_env()` reads `JIRA_SITE`, `JIRA_EMAIL` and `JIRA_API_TOKEN` from OCI Vault, with `OCI_VAULT_ID` set, or from environment variables.
- `requests` on the cluster; it is already installed on AIDP.
- Only for OCI Vault credentials: the `oci` package and an OCI config the cluster can read (`~/.oci/config`). Secrets are looked up in `OCI_COMPARTMENT_ID` if set, otherwise the tenancy root. If the Vault lookup fails, the connector falls back to environment variables and reports the Vault error if those are missing too.

## Usage

1. Upload `jira_client.py` to a workspace folder.
2. Create the Credential Store entry.
3. Open `examples/jira_issue_load.ipynb`, set `HELPER_DIR`, `CREDENTIAL_NAME`, `TARGET` and `JQL`, and run the cells.

## Run the tests

From the repository root:

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Known limits

- The `updated` watermark does not see Jira issue deletes.
- Jira reads JQL date-time literals in the searching account's own timezone, so `search_issues` fetches it from the account profile when `tz_name` is omitted.
- Custom fields are kept in the trailing `raw_fields` JSON column, not as typed columns.
- The read holds every issue in driver memory; for a very large first load, narrow `JQL` and load in windows.
- The `ORDER BY` guard is a plain text check, so a query with `order by` inside a quoted string is rejected.
- An edit made less than a minute before a run is picked up by the next run, not that one: JQL bounds are rounded to the minute.
- JQL bounds have minute precision in the account timezone, so around a daylight-saving change the ambiguous hour can shift a bound; the default 300-second overlap covers normal cases only.
