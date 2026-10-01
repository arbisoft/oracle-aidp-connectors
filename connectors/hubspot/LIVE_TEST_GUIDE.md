# HubSpot connector: live AIDP test guide

For whoever has hands-on AIDP access. Never commit a real token, portal id,
catalog name or any notebook output that shows them.

1. Upload `hubspot_client.py` to a workspace folder and import
   `examples/hubspot_load.ipynb`.
2. In the HubSpot account, make sure a test portal has a few contacts,
   companies and deals, some with associations. Create a service key (or a
   legacy private app token) with the scopes `crm.objects.contacts.read`,
   `crm.objects.companies.read` and `crm.objects.deals.read`.
3. In the AIDP Credential Store, create a **Secret Token** credential with the
   token under one key. Never paste the token into a cell. Check it reads
   back, printing key names only:
   `print(list(aidputils.secrets.get(name="<CREDENTIAL_NAME>").keys()))`.
4. In the configuration cell set `HELPER_DIR`, `CREDENTIAL_NAME`,
   `CREDENTIAL_KEY`, `CATALOG` (it must exist) and `SCHEMA` (created if
   missing). Use a schema that holds nothing else.
5. Run the notebook top to bottom, five times, noting the counts it prints:
   - **First load** (`MODE = "incremental"`, no state yet): each object is
     loaded in full. The row counts match the HubSpot counts for contacts,
     companies and deals, archived records included. `hubspot_sync_state` has
     one `SUCCESS` row per object.
   - **No change:** rerun with nothing edited in HubSpot. Contacts, companies
     and deals report 0 rows; the associations table is unchanged.
   - **After editing one contact in HubSpot** (for example its last name):
     wait at least a minute, rerun. One row is reported for contacts and that
     row is updated in the table; the row count is unchanged.
   - **After deleting one contact that has associations:** rerun. The contact
     row stays with `archived = true` and an `archived_at`, and its links are
     gone from `associations`, so that table has fewer rows.
   - **Full refresh:** set `MODE = "full"` and rerun. The counts match the
     current HubSpot counts, and the deleted contact is still present as
     archived.
6. Check the tables with Spark:
   `SELECT count(*) - count(DISTINCT id) FROM <CATALOG>.<SCHEMA>.contacts`
   returns 0 (the same for companies and deals), and
   `SELECT * FROM <CATALOG>.<SCHEMA>.hubspot_sync_state` shows
   `last_status = 'SUCCESS'` for every object.
7. Optional failure check: set `CREDENTIAL_KEY` to a wrong key or use an
   expired token and rerun. The run raises an error that names the HTTP
   status and category but not the token, and `hubspot_sync_state` shows
   `FAILED` while the old watermarks stay.
8. Report the result with the cluster's Spark and Python versions; remove the
   token, portal id and catalog from any error text. It goes into
   `live-results/RESULTS.md` as a dated row.
