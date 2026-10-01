# Jira Cloud connector: live AIDP test guide

For whoever has hands-on AIDP access. Never commit a real site name, email,
token, catalog name or any notebook output that shows them.

1. Upload `jira_client.py` to a workspace folder and import
   `examples/jira_issue_load.ipynb`.
2. Provide `JIRA_SITE`, `JIRA_EMAIL` and `JIRA_API_TOKEN` as cluster
   environment variables, or as OCI Vault secrets plus `OCI_VAULT_ID`. Never
   paste the token into a cell. If the cluster has no environment-variable
   setting, store the three values in the AIDP Credential Store and copy them
   into the environment with a cell before *Read Issues*:
   ```python
   import os
   for env, key in (("JIRA_SITE", "site"), ("JIRA_EMAIL", "email"), ("JIRA_API_TOKEN", "token")):
       os.environ[env] = aidputils.secrets.get(name="<CREDENTIAL_NAME>", key=key)
   ```
3. In the configuration cell set `HELPER_DIR`, `TARGET`
   (`<catalog>.<schema>.jira_issue`; the schema must exist) and `JQL`.
4. Run the notebook top to bottom, four times:
   - **Full load:** the row count matches the Jira count for the JQL.
   - **Rerun:** few issues re-read, inside the 300 s overlap; row count
     unchanged.
   - **After editing one issue in Jira:** that issue is updated. Wait at
     least a minute after the edit before rerunning: JQL bounds are rounded
     to the minute, so an edit made within the same minute is picked up by
     the run after.
   - **With `OVERLAP_SECONDS = 10000000`:** everything is re-read, the
     `MERGE` succeeds, row count unchanged.
5. Check for duplicates: `SELECT count(*) - count(DISTINCT key) FROM <TARGET>`
   returns 0.
6. Report the result with the cluster's Spark and Python versions and the
   credential method used; remove the token, site and email from any error
   text. It goes into `live-results/RESULTS.md` as a dated row.
