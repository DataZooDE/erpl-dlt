# erpl-dlt — plan

A pip-installable dlt source package wrapping the ERPL DuckDB extensions.
Distribution `erpl-dlt`, import `erpl_dlt`. The name is free on PyPI (404).

This plan takes the original brief as input and **adapts it to what the
extensions actually do** (see `NOTES.md`) and to what already exists and works in
`erpl-airbyte`. Every departure from the brief is called out and justified.

## What changes from the brief, and why

| Brief said | Reality | Consequence |
|---|---|---|
| "erpl … via RFC, BAPI and OData", plus erpl-web | `erpl` is a trampoline for `erpl_rfc`, `erpl_bics`, `erpl_odp`; `erpl_web` is separate and covers **both** generic OData *and* ODP-over-OData | Five source flavours are available, not two. See scope below. |
| Cast DATS/TIMS in SQL; don't let currency land as float | The extension already yields `DATE`, `TIME`, `DECIMAL(15,2)` → Arrow `date32[day]`, `decimal128(15,2)` | **Drop the casting layer.** Verify types, don't manufacture them. |
| `fetch_record_batch(batch_size)` | Deprecated in DuckDB 1.5.5 | Use `to_arrow_reader(batch_size)`. |
| Push `last_value` into a hand-built WHERE / `$filter` | DuckDB pushes ordinary SQL predicates into the scan (`EXPLAIN` shows `Filters:` inside `SAP_READ_TABLE`) | Emit a plain SQL `WHERE`; assert push-down in `EXPLAIN`, not in string-building. Keep `FILTER :=` for ABAP-only predicates. |
| Map `00000000`/`000000` to NULL | Never observed; unknown | Settle it first (Slice 0). Do not write sentinel code against a guess. |
| `dlt>=1.0`, Python 3.10–3.13 | dlt 1.30.0 requires `>=3.10,<3.15` | `dlt>=1.30,<2`, Python `>=3.10,<3.14` (DuckDB wheels + the base image we already target). |

Two traps the brief does not mention, both already solved in `erpl-airbyte`:

- **The process core-dumps** without an `RTLD_GLOBAL` preload of the SAP/ICU
  shared objects. `LD_LIBRARY_PATH` cannot be set after start.
- **`erpl_web` delta tokens need a persistent DuckDB file**; `:memory:` is refused.

## Scope

Ship **three** sources in v0.1, in this order:

1. `erpl_rfc_source(...)` — tables and CDS views. The workhorse.
2. `erpl_odata_source(...)` — generic OData via `odata_read`, with `$top`/`$skip`/
   `$expand`/`max_page_size` exposed.
3. `erpl_odp_source(...)` — ODP delta, over RFC or the Gateway.

`erpl_bics_source` and `erpl_rfc_invoke_source` are **out of scope for v0.1** and
named here so the module layout leaves room: BICS is a stateful multi-statement
session and function-module invocation needs `RETURN`-table failure handling —
both are solved in `erpl-airbyte` and both deserve their own slice.

## Reuse from erpl-airbyte

Copied with attribution, not re-derived. These are already exercised against a
real SAP system by 541 unit tests and 57 e2e tests:

| From | Reused as | Why |
|---|---|---|
| `source_sap/session.py` | `erpl_dlt/connection.py` | Extension loading, `allow_unsigned_extensions`, `extension_directory`, per-thread cursors, `CREATE SECRET` with bound parameters, and `preload_native_libraries()` — without which this segfaults. |
| `packages/erpl-extensions` | a dependency | **The distribution problem is already solved.** The extensions ship as a platform-tagged wheel (72 MB) rather than being downloaded at import time. Answers the brief's "private repo vs community repo" question by sidestepping it, and keeps `pip install erpl-dlt` working offline. A `custom_extension_repository` path stays available for people who want it. |
| `source_sap/types.py` | `erpl_dlt/typing.py` | The DDIC map, for *validating* the Arrow schema rather than building it. |
| `source_sap/protocols/rfc.py` | `erpl_dlt/rfc.py` | Column projection, `FILTER` rendering, `MAX_ROWS`, and the measured guidance: `threads` is the throughput knob, `partitions` is a trap. |
| `source_sap/protocols/odp_odata.py` | `erpl_dlt/odp.py` | Delta-token seeding and read-back, including the DELETE+INSERT transaction that the table's PK + UNIQUE constraint forces. |
| `unit_tests/test_quoting_per_call_site.py` | `tests/test_quoting.py` | Hostile-input tests at every interpolation site. |

## Module layout

```
erpl_dlt/
  __init__.py      # erpl_rfc_source, erpl_odata_source, erpl_odp_source, credential specs, __version__
  connection.py    # ErplConnection: load, preload, secrets, cursor-per-thread
  config.py        # @configspec SapRfcCredentials / ODataCredentials / ErplSettings
  rfc.py           # table + CDS view resources
  odata.py         # generic OData resources
  odp.py           # ODP delta resources (RFC and Gateway)
  query.py         # SQL building: identifiers, literals, projection, predicates
  typing.py        # Arrow/dlt schema helpers and DDIC expectations
  settings.py      # batch size, extension repo, thread budget
```

## Public API

```python
def erpl_rfc_source(
    table_names: Sequence[str] | None = None,
    *,
    credentials: SapRfcCredentials = dlt.secrets.value,
    columns: Mapping[str, Sequence[str]] | None = None,
    filters: Mapping[str, str] | None = None,        # ABAP WHERE, per table
    incremental: Mapping[str, str] | None = None,    # table -> cursor column
    primary_keys: Mapping[str, Sequence[str]] | None = None,
    batch_size: int = 50_000,
    threads: int | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> DltSource: ...
```

`erpl_odata_source(service_url, entity_sets=..., top=..., expand=...)` and
`erpl_odp_source(context, names=..., transport="rfc"|"odata", subscriber=...)`
follow the same shape. Every resource is built by passing the generator
*function* to `dlt.resource`, so hints stay overridable via `apply_hints`.

## Implementation order — vertical slices, each ending green

0. **Settle the unknowns.** The DATS sentinel, the TIMS mapping, and whether
   `odata_read` pushes `$filter` server-side or filters client-side. Record in
   `NOTES.md`; only then write code that depends on them.
1. **`connection.py` + `config.py`.** Extensions load, preload happens, secret is
   created from bound parameters, cursor per thread. Gate: `sap_rfc_ping`
   succeeds against the trial system; a test asserts no secret appears in any
   rendered SQL.
2. **One RFC resource end to end.** `SFLIGHT` → Arrow batches → `pipeline.run`
   into local DuckDB. Gate: row count matches `sap_read_table` directly, and the
   destination schema has `DATE`/`DECIMAL`, not strings.
3. **Projection, limits, incremental.** Gate: `EXPLAIN` shows the predicate
   inside `SAP_READ_TABLE` on the second run and no filter on the first.
4. **Parallelism.** Three resources with `parallelized=True`. Gate: they run
   concurrently on separate cursors and the row counts are unchanged.
5. **Generic OData** (`odata_read`), then **ODP delta** (both transports). Gate
   for ODP: a second run in a *fresh process* returns zero rows — the property
   that proves the token in dlt state is sufficient, which is exactly how the
   Airbyte connector's ODP test is written.
6. **Polish**: README with the licence notice above the fold, `NOTICE`, examples,
   `justfile`.

## Testing

Mirroring what worked in `erpl-airbyte`: unit tests that need no SAP, plus real
end-to-end tests against the ABAP trial with **no mocks**.

- **Unit** — query building, hint defaults, incremental state, secret redaction,
  batching. A fake query builder over ordinary DuckDB tables covers pipeline
  mechanics in CI.
- **Push-down** — assert against `EXPLAIN` output, not against a string we built.
  This is the one place where asserting the SQL text would be a test that cannot
  fail for the right reason.
- **Integration** — gated on `ERPL_IT=1` plus credentials, skipped by default.
  Full `pipeline.run` into DuckDB with row counts and destination types asserted.
- **Round-trip types** — a table carrying DEC, CURR, QUAN, DATS, TIMS, CHAR and
  (once Slice 0 settles it) the null-date sentinel.

## Packaging and licence

- hatchling, `uv` for the dev workflow, `dlt>=1.30,<2`, `duckdb>=1.5.5,<2`,
  `pyarrow`. No pandas.
- Extras: `dev` only. `erpl-extensions` as a runtime dependency, with an extra
  for people who supply their own extension directory.
- **Apache-2.0 for the Python glue**, with a `NOTICE` stating plainly that the
  ERPL extension binaries are separately licensed **BUSL-1.1** and that
  installing this package grants no licence to them. Above the fold in the README.
- `__version__`, plus the ERPL version the package was tested against
  (v2026.09.04) recorded in `settings.py`.

## Out of scope

No transformations, no destination-specific code, no publishing workflow, no
attempt at `dlt-hub/verified-sources`.
