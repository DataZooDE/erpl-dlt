# erpl API surface, as observed

Phase 0 discovery. Every line is marked **verified** (I ran it against ERPL
v2026.09.04 + DuckDB 1.5.5 with a live SAP ABAP Platform Trial), **assumed**
(consistent with the code I read but not executed), or **unknown**.

## Shape of the extensions

- **verified** — "erpl" is not one extension. `erpl` is a trampoline whose payload
  unpacks into **`erpl_rfc`**, **`erpl_bics`** and **`erpl_odp`**; **`erpl_web`**
  is a separate extension. Four `.duckdb_extension` files plus five shared
  objects (SAP NW RFC SDK + ICU), 194 MB unpacked.
- **verified** — loaded together they expose **75 functions**: 57 table
  functions, 4 scalar, 14 pragmas.
- **verified** — the extensions are unsigned: DuckDB needs
  `allow_unsigned_extensions` and an `extension_directory`.

## Reading tables (`erpl_rfc`)

- **verified** — `sap_read_table(table_name, ...)`; described by DuckDB as
  "Supports projection pushdown, filter pushdown, and parallel reads via THREADS".
- **verified** — named parameters in use by the Airbyte connector, all exercised
  against a real system: `COLUMNS := [...]`, `FILTER := '<ABAP WHERE>'`,
  `MAX_ROWS`, `THREADS`, `PARTITIONS`, `FETCH_SIZE`.
- **verified** — **DuckDB pushes ordinary SQL predicates and projections into the
  RFC call.** `EXPLAIN SELECT CARRID, FLDATE FROM sap_read_table('SFLIGHT')
  WHERE CARRID = 'LH'` shows, inside the `SAP_READ_TABLE` node:
  `Projections: CARRID` and `Filters: CARRID='LH'`.
- **verified** (from the extension source) — pushdown is a *pure optimisation*:
  DuckDB re-evaluates every filter regardless, so correctness never depends on it
  but the SAP-side selection does. There is an escape hatch,
  `erpl_rfc_pushdown_filters`.
- **verified** — `threads := 0` (the default) means "one RFC call per projected
  column", executed concurrently. This is where throughput comes from: measured
  on a 55-column, 164,673-row table, 1,479 rows/s at one in-flight round trip
  rising to 12,919 at 32.
- **verified** — `PARTITIONS := n` replaces that column-parallel path with a
  row-window scheduler that opens ~3 RFC connections instead of 8 and is ~5.5x
  *slower* on a wide table. Leave it off.
- **verified** — `FETCH_SIZE` is a **byte** budget per round trip, not rows, and
  a partitioned scan divides it across workers.

## Other read paths

- **verified** — `sap_rfc_invoke(function, {params})`, with
  `sap_rfc_describe_function` for the interface; BAPIs report failure in a
  `RETURN` table rather than by raising.
- **verified** — BICS is a *stateful session*: `sap_bics_begin` → `sap_bics_rows`
  / `sap_bics_columns` / `sap_bics_filter` / `sap_bics_set_char_prop` →
  `sap_bics_result`. Each statement is its own CREATE_DATA_AREA → OPEN →
  SET_STATE → CLOSE cycle inside ERPL, so nothing is left open server-side.
- **verified** — ODP over RFC: `sap_odp_read_full(...)` and
  `sap_odp_read_delta(odp_context, odp_name, subscriber_process, ...)`. The first
  delta call for a subscriber performs SAP's auto-DELTAINIT: full snapshot *and*
  a server-side delta pointer. `PRAGMA sap_odp_close_delta_cursor` releases the
  cursor; closing is not confirming, so it cannot cost the next run its packets.
- **verified** — `erpl_web` covers **two** OData paths, not one:
  - generic: `odata_read(url, ...)` — "OData v2/v4 with automatic version
    detection and predicate pushdown"; named parameters include `top`, `skip`,
    `expand`, `count`, `strict_typing`, `max_page_size`. Also `odata_attach`
    (attaches every entity set of a service as views), `odata_describe`,
    `odata_sap_show`.
  - ODP-specific: `odp_odata_read(url, ...)` with delta-token support,
    `odp_odata_show`, `odp_odata_list_subscriptions`,
    `PRAGMA odp_odata_remove_subscription`.
- **verified** — `odp_odata_read` keeps its delta token in erpl_web's own
  `erpl_web.odp_subscriptions` table, which requires a **persistent** DuckDB
  file; `:memory:` is refused.

## Types

- **verified** — the extension already produces correct DuckDB types; nothing
  needs casting in SQL. `SELECT * FROM sap_read_table('SFLIGHT')` yields
  `FLDATE DATE`, `PRICE DECIMAL(15,2)`, `SEATSMAX BIGINT`, `CARRID VARCHAR`.
- **verified** — through Arrow those arrive as `date32[day]`,
  `decimal128(15, 2)`, `int64`, `string`. A currency field does **not** land as
  a float.
- **verified** — TIMS maps to `TIME`: `DD02L.AS4TIME` came back as
  `datetime.time(16, 7, 5)`, `typeof` `TIME`.
- **verified** — an empty SAP date (`'00000000'`) becomes **NULL**, and the
  column stays `DATE`. `USR02` has 6 rows and `count(GLTGV) = 0`. The extension
  handles the sentinel; nothing downstream needs to.
- **verified** — `odata_read` pushes predicates too: `EXPLAIN … WHERE ITEMID =
  'X'` shows `Filters: ITEMID='X'` inside the `ODATA_READ` node.
- **verified** — dlt's snake_case normaliser splits letter/digit boundaries, so
  `AS4DATE` lands as `as4_date`. SAP column names are full of digits, so this
  affects most tables.
- **verified** — a SQL predicate needs a *SQL* literal even though the column is
  SAP's: `FLDATE >= '20260905'` fails with "invalid date field format: expected
  (YYYY-MM-DD)". The compact DDIC form belongs only in an ABAP `FILTER :=`
  string. Both forms exist in `query.py` and are tested against each other.

## Arrow extraction

- **verified** — `con.execute(sql).to_arrow_reader(batch_size)` streams
  `RecordBatch`es over an erpl scan: 334 rows in one batch from SFLIGHT.
- **verified** — `fetch_record_batch()` still works but is **deprecated** in
  DuckDB 1.5.5: *"fetch_record_batch() is deprecated, use to_arrow_reader()
  instead"*.

## Runtime traps

- **verified** — without preloading the SAP/ICU shared objects the process
  **core-dumps**: *"Could not open the ICU common library … LD_LIBRARY_PATH is
  currently set to &lt;not set&gt;"*. `LD_LIBRARY_PATH` is read by the dynamic
  loader at process start, so nothing Python does afterwards can fix it.
  Loading them with `ctypes.CDLL(..., mode=RTLD_GLOBAL)` before the first `LOAD`
  does fix it — that is what the Airbyte connector does and it worked here
  unchanged.
- **verified** — a single DuckDB connection is not safe across threads;
  `con.cursor()` per worker is.
- **verified** — credentials go through `CREATE SECRET (TYPE sap_rfc, ASHOST $h,
  …)` with **bound parameters**, so no secret is ever interpolated into SQL text.
  `erpl_web` uses a separate `http_basic` secret scoped to a URL prefix.

## ODP delta recovery semantics (verified from the ERPL source)

Read from `erpl/odp/src/odp_fetch.cpp` while reviewing the ODP subscriber
lifecycle. These decide whether a crash can lose an initial snapshot.

- **verified** — `I_EXTRACTION_MODE` has three values (`odp_fetch.cpp:113-119`):
  `'F'` full with no queue registered, `'D'` delta (auto-DELTAINIT on a fresh
  subscriber), and **`'R'` RECOVER, which "re-streams the last unconfirmed
  packet without advancing the pointer"**.
- **verified** — so the ODQ pointer advances on *confirmation*, not on fetch,
  and an unconfirmed packet is re-deliverable. That is what makes a crashed run
  recoverable in principle.
- **verified** — `I_SUBSCRIBER_RUN` is a fresh timestamp every open
  (`GenerateRunId`, `odp_fetch.cpp:149`), while `I_SUBSCRIBER_PROCESS` is the
  stable subscriber. The run id is what SAP uses to tell one extraction attempt
  from another.
- **unknown** — whether `erpl_dlt` can *reach* recover mode. `odp_rfc_query`
  (`erpl_dlt/odp.py`) only ever asks for full or delta; nothing exposes `'R'`.
  So after a crash the next run asks for `'D'` again, and whether SAP re-streams
  the unconfirmed packet or moves on is **not established**. This is the open
  question behind the subscriber-lifecycle design decision, and it should be
  settled by experiment against a real system before the ODP source is called
  production-ready.
