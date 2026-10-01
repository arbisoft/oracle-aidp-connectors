# Jira Cloud connector: live AIDP test results

## This version — NOT RUN

`jira_client.py` in this folder has not run on AIDP yet. It is unit-tested
(122 tests). Its original pull request (arbisoft/oracle-aidp-samples#2)
reports a run against a real Jira site with a local Spark 3.5.1 and Delta
table. Nothing covers uploading it to a workspace, credentials on a
real cluster, network access from the cluster to Jira, or a real
`<catalog>.<schema>.<table>` target. Follow `LIVE_TEST_GUIDE.md`.

## Earlier version — 2026-09-30 — PASS

An earlier, multi-module version of this connector, which used a shared
retry and credentials package, ran end to end on a live AIDP workspace: row
count matched Jira, no duplicates after a small page size forced many pages,
and an incremental rerun updated only the edited issue. The cluster's
runtime versions and the credential method were not recorded.
This result does not cover the current `jira_client.py`.
