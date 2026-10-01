# oracle-aidp-connectors

Connectors for the Oracle AI Data Platform (AIDP) Workbench: tested Python
helpers and example notebooks for loading data from sources AIDP has no
built-in connector for. Each connector is unit-tested offline and run against
a real source from a live AIDP workspace before it is listed as supported.

Independent Arbisoft project, not affiliated with or endorsed by Oracle.

## Connectors

| Connector | Ingests | Status | Docs |
|---|---|---|---|

## Layout

```
connectors/<source>/
  <helper>.py        the helper module the notebook imports
  conftest.py        puts the helper and its tests on the import path
  tests/             offline unit tests: no network, no Spark
  examples/          example notebook
  README.md          setup, usage and gotchas
  LIVE_TEST_GUIDE.md how to repeat the live AIDP test
  live-results/      dated results of live AIDP runs
```

## Run the tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Licence

MIT, see `LICENSE`.
