# erpl-dlt

> **Licence notice, above the fold.** This package — the Python glue — is
> Apache-2.0. The ERPL DuckDB extension binaries it loads are **separately
> licensed under BUSL-1.1** by DataZoo GmbH. Installing `erpl-dlt` grants you no
> licence to them. See [NOTICE](NOTICE).

dlt sources for SAP, built on the [ERPL](https://erpl.io) DuckDB extensions.

```bash
pip install "erpl-dlt[extensions]"
```

```python
import dlt
from erpl_dlt import erpl_rfc_source

pipeline = dlt.pipeline(pipeline_name="sap", destination="duckdb", dataset_name="sap_raw")
pipeline.run(erpl_rfc_source(table_names=["SFLIGHT", "SBOOK"]))
```

Five SAP read paths, one connection layer:

| Source | Reads |
|---|---|
| `erpl_rfc_source` | Any table or CDS view over RFC, with projection and filter push-down |
| `erpl_odata_source` | Any OData v2/v4 service |
| `erpl_odp_source` | ODP delta — real server-side change data, over RFC or the SAP Gateway |
| `erpl_bics_source` | BW InfoProviders and BEx queries |
| `erpl_invoke_source` | Remote-enabled function modules and BAPIs |

## Configuration

Credentials resolve through dlt, so nothing needs to be passed in code:

```toml
# .dlt/secrets.toml
[sources.erpl_dlt.credentials]
ashost = "sap.example.com"
sysnr = "00"
client = "100"
user = "SVC_DLT"
password = "..."
```

They are bound as parameters to DuckDB's `CREATE SECRET`, never formatted into
SQL text — query text reaches logs.

## What the sources do

```python
from erpl_dlt import erpl_rfc_source

erpl_rfc_source(
    table_names=["SFLIGHT", "SBOOK"],
    # Pushed into SAP. On a 55-column table, naming two columns measured 3.8x
    # faster than reading all of them.
    columns={"SBOOK": ["CARRID", "CONNID", "FLDATE", "LOCCURAM"]},
    # Becomes a WHERE clause that DuckDB pushes into the RFC call, so SAP does
    # the filtering. `EXPLAIN` shows it as `Filters:` inside SAP_READ_TABLE.
    cursor_columns={"SBOOK": "FLDATE"},
    primary_keys={"SBOOK": ["CARRID", "CONNID", "FLDATE", "BOOKID"]},
)
```

Extraction is Arrow-native: batches come from DuckDB's `to_arrow_reader` and go
straight to dlt, so no row becomes a Python dict. SAP's types arrive correct
without casting — `DATS` as `date32[day]`, `CURR` as `decimal128`, an empty SAP
date as NULL. dlt's naming normaliser will rename columns containing digits:
`AS4DATE` lands as `as4_date`.

Each resource takes its own DuckDB cursor, so `parallelized=True` is safe.

`erpl_odp_source` uses SAP's ODQ subscriber process as the delta identity. By
default that name is stable for the dlt pipeline name and ODP provider, and it
is capped at SAP's 32-character field width with a hash suffix so long names do
not collide by truncation. If you need a pre-existing SAP subscriber, pass
`subscriber_processes={provider_name: "SUBSCRIBER"}`; after a delta run records a
subscriber in dlt state, a later run that would use a different subscriber is
refused instead of silently starting a new delta queue.

ODP delta reads are not transactional with respect to the destination load.
SAP's pointer can advance while dlt commits nothing; what can be recovered then
depends on ODQ confirmation semantics. ERPL's source shows a recover mode for
the last unconfirmed packet, but whether a particular installed ERPL build
exposes that mode is checked from `duckdb_functions()` at runtime.

## Development

```bash
uv venv && uv pip install -e ".[dev]"
just check            # ruff, mypy --strict, unit tests
ERPL_IT=1 just it     # integration tests against a real SAP system
```

Integration tests are skipped unless `ERPL_IT=1` and credentials are set. They
use no mocks: they assert that a projection reaches SAP, that `EXPLAIN` shows
the predicate inside the scan, that ODP's delta token survives a fresh process,
and that DECIMAL/DATE/TIME survive into the destination schema.

## Status

Tested against ERPL v2026.09.04, DuckDB 1.5.5, dlt 1.30, Python 3.10–3.13, on
Linux x86-64 — the only platform ERPL publishes these extensions for.
