# Review request: erpl-dlt

## Problem statement

This is a **code review**, not a bug hunt for a known failure. The package is
new, all its gates are green, and I want a second pair of eyes on whether the
green is real.

`erpl-dlt` is a [dlt](https://dlthub.com) source package wrapping the ERPL
DuckDB extensions, which expose SAP (RFC, BW/BICS, ODP, OData) as SQL table
functions. It ships five sources:

| Module | Source | Wraps |
|---|---|---|
| `rfc.py` | `erpl_rfc_source` | `sap_read_table` |
| `odata.py` | `erpl_odata_source` | `odata_read` |
| `odp.py` | `erpl_odp_source`, `erpl_odp_odata_source` | `sap_odp_read_full` / `sap_odp_read_delta` / `odp_odata_read` |
| `bics.py` | `erpl_bics_source` | `sap_bics_begin` … `sap_bics_result` |
| `invoke.py` | `erpl_invoke_source` | `sap_rfc_invoke` |

Extraction is Arrow-native: `cursor.execute(sql).to_arrow_reader(batch_size)`
yields `pyarrow.RecordBatch` straight into dlt. No row becomes a Python dict.

## What I want reviewed, in priority order

1. **Incremental and delta state.** `erpl_odp_odata_source` seeds a delta token
   into erpl_web's own `odp_subscriptions` table before reading and reads the
   advanced token back afterwards (`odp.py`). `erpl_odp_source` records a
   `subscriber_process` in dlt state; SAP owns the actual pointer. Is the
   state written at the right moment? What happens on a partial failure, a
   retry, or two pipelines sharing a subscriber? Is there a path where a run
   appears to succeed but the next run silently re-loads everything, or worse,
   skips changes?

2. **SQL injection surface.** Identifiers cannot be parameterised, so
   `query.py:check_identifier` validates them against `^[A-Z0-9_/]{1,30}$` and
   they are then interpolated bare. Values go through `sql_literal` /
   `sql_value_literal` / `abap_literal`. Look for any path where a value
   reaches a statement without passing one of those — especially `bics.py`
   (BEx variables, filters, characteristics), `invoke.py` (`_render` for nested
   structures and tables) and `odp.py`.

3. **Thread safety.** `ErplConnection.cursor()` returns `self._connection.cursor()`
   under a lock; resources run with `parallelized=True`. Is one cursor per
   resource execution actually sufficient for DuckDB here? Is the lock doing
   anything useful, or is it theatre?

4. **Tests that cannot fail.** `tests/` has 90 tests, 18 of which hit a real SAP
   system. I am specifically worried about assertions that pass vacuously —
   equality between two empty results, a `pytest.raises` that would catch an
   error from the wrong cause, or an integration test that would pass against a
   system with no data. Name any you find.

5. **dlt API misuse.** The incremental is bound as a parameter default inside a
   per-table factory (`rfc.py:make_reader`, `odata.py:make_reader`) because
   passing it at call time raises `IncrementalUnboundError`. Is that the right
   idiom? Is `dlt.current.resource_state()` used correctly in `odp.py`? Are the
   write dispositions and primary keys sensible defaults?

## Specific questions

- Is there any way the ODP delta token or subscriber can be lost or advanced
  without the corresponding rows reaching the destination?
- `query.py:incremental_predicate` uses `>=` deliberately, arguing that SAP's
  one-day resolution makes `>` drop boundary rows and that dlt deduplicates.
  Is that reasoning right for dlt's merge semantics?
- `connection.py:preload_native_libraries` swallows `OSError` per library.
  Without the preload the process core-dumps on missing ICU symbols. Is best
  effort the right posture, or should a failed preload be fatal?
- Anything in the package that would embarrass us on PyPI.

## Environment

- Python 3.13, dlt 1.30.0, duckdb 1.5.5, pyarrow 25.0.1
- ERPL v2026.09.04, SAP ABAP Platform Trial reachable at localhost
- Gates all green: `ruff check`, `ruff format --check`, `mypy --strict`, 90 tests
  (72 need no SAP; 18 integration tests run against the real system)

## Success criteria

Concrete findings with `file:line`, ranked by severity, each with the input or
sequence that triggers it. "Looks fine" for an area is a useful answer if it is
backed by what you checked. Do not rewrite the package; point at what is wrong.


---

## What a previous review already found and fixed

An Antigravity review of this package found five defects, all now fixed at HEAD
`fb17d4f`. Do **not** re-report them; look for what it missed.

1. `bics.py` rendered BEx variables as `{'V' AS NAME, ...}`, which is not DuckDB
   struct syntax and failed at the parser. Now `{'NAME': 'V', ...}`, verified
   against a live BW system.
2. `invoke.py:_render(None)` produced the string `'None'`. Now `NULL`.
3. `invoke.py`'s module docstring claimed parameter names are validated against
   `sap_rfc_describe_function`. They are not; the claim was corrected.
4. `connection.py` interpolated `custom_extension_repository` into `SET`. Now bound.
5. `odata.py` folded property names to upper case via `check_identifier`, which
   breaks OData's case-sensitive comparison. It now has its own case-preserving
   check.

`tests/test_generated_sql_parses.py` was added: every statement every builder
produces is parsed with `EXPLAIN`, because substring assertions had let (1) through.

## Where I already suspect problems, and want your independent judgement

These are my own observations, not findings. Confirm, refute or sharpen them:

- `odp.py:121` derives `subscriber_process` deterministically from context and
  name. Two pipelines reading the same provider therefore share one SAP-side
  ODQ queue. The state written at `odp.py:132` is never read back by anything,
  so it looks decorative.
- If a run crashes mid-snapshot, SAP's delta pointer may already have advanced
  while dlt committed nothing. Is the initial snapshot recoverable, or lost?
- `rfc.py:120` sets `write_disposition="replace"` whenever no primary key is
  given -- *including* when a cursor column is configured. An incremental cursor
  with a replace disposition seems incoherent.
- Resource names: `rfc.py:119` uses the lowercased table name, `odata.py:105`
  the last URL segment, `odp.py:139`/`:210` a derived name. Can two sources in
  one pipeline collide?

## Success criteria

Concrete findings with `file:line`, ranked by severity, each with the input or
sequence that triggers it. Say plainly where you checked and found nothing.
Do not rewrite the package.
