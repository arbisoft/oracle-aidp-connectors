# Jira Cloud connector: live AIDP test results

## Credential Store option — NOT RUN

After the 2026-10-01 PASS below, `j.credentials(site, email, api_token)` was
added and the example notebook now reads the three values from the Credential
Store and passes them in, instead of a temporary cell copying them into
environment variables. The search, paging and write code is unchanged, and
reading the Credential Store with `aidputils.secrets.get` is verified on AIDP,
but this notebook version has not run end to end yet.

## 2026-10-01 — PASS

**What ran:** this `jira_client.py` with `examples/jira_issue_load.ipynb`,
following `LIVE_TEST_GUIDE.md`, against a real Jira Cloud site. Credentials
came from the AIDP Credential Store, copied into `JIRA_SITE`, `JIRA_EMAIL` and
`JIRA_API_TOKEN` by a temporary notebook cell, because the cluster UI had no
environment-variable setting.

| Run | Check | Result |
|---|---|---|
| 1. Full load into a new table | Row count matches Jira's count for the JQL | PASS |
| 2. Rerun | Few issues re-read, inside the overlap; row count unchanged | PASS |
| 3. Edit one issue's title in Jira, rerun | That issue's row updated; row count unchanged | PASS |
| 4. Rerun with `OVERLAP_SECONDS = 10000000` | Every issue re-read; `MERGE` succeeds; row count unchanged; no duplicate keys | PASS |

**Verified AIDP facts:**
- `requests` is already on the cluster: nothing was installed.
- The cluster reaches Jira Cloud over HTTPS, and the account-timezone lookup
  and token-paged search work from it.
- An edit made less than a minute before a run is picked up by the *next*
  run, not that one: JQL bounds are rounded to the minute, so the upper bound
  can fall just before the edit. Nothing is lost, because the next run's
  window starts from the newest `updated` in the table, minus the overlap.

**Not recorded:** the exact issue count, and the cluster's Spark and Python
versions for this run. **Not covered:** OCI Vault credentials.

## 2026-09-30 — PASS (earlier version)

An earlier, multi-module version of this connector, which used a shared
retry and credentials package, ran end to end on a live AIDP workspace. The
cluster's runtime versions and the credential method were not recorded.
