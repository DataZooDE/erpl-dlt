"""ODP delta: real server-side change data, over RFC or the SAP Gateway.

This is the one source where the position does not live in the pipeline. SAP
owns it, which changes what state means:

* **Over RFC**, the position is an ODQ subscription identified by a
  *subscriber process*. The first delta call for a subscriber performs SAP's
  auto-DELTAINIT: it returns the whole current snapshot *and* registers the
  pointer. dlt state therefore carries the subscriber name, not a cursor value.
* **Over the Gateway**, the position is a delta token. ``erpl_web`` keeps it in
  its own ``odp_subscriptions`` table inside the DuckDB file -- which this
  package treats as scratch, so the token is seeded from dlt state before the
  read and read back out afterwards.

Both are checkpointed only after the whole resource has been consumed. A
mid-stream checkpoint would let the next run resume past packets that were
fetched but never handed to a destination.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator, Sequence
from typing import Any

import dlt
import pyarrow as pa
from dlt.sources import DltResource

from erpl_dlt.config import ErplSettings, ODataCredentials, SapRfcCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import ConfigurationError, QueryError, check_identifier, check_unique_names, sql_literal
from erpl_dlt.settings import ODP_RFC_EXTENSIONS, WEB_EXTENSIONS

logger = logging.getLogger("erpl_dlt")

#: Control columns ODP adds to every row. ODQ_CHANGEMODE says what happened to
#: the row and is data; the rest number the rows of one extraction and differ
#: between two extractions of identical data.
ODP_SEQUENCE_COLUMNS = ("ODQ_TSN", "ODQ_RECORDNO", "ODQ_UNITNO", "ODQ_ENTITYCNTR")
#: ERPL's own parameter for re-streaming: "Pass recover=true to re-stream the
#: last unconfirmed packet without advancing the pointer" (erpl_odp_extension.cpp).
ODP_RECOVERY_PARAMETER = "recover"


def _safe_subscriber_piece(value: str, *, fallback: str) -> str:
    safe = "".join(ch for ch in value.upper() if ch.isalnum() or ch == "_").strip("_")
    return safe or fallback


def check_subscriber_process(value: str, *, what: str = "subscriber_process") -> str:
    candidate = _safe_subscriber_piece(str(value), fallback="")
    if candidate != value.upper() or not (1 <= len(candidate) <= 32):
        raise QueryError(f"{what} {value!r} must be 1-32 characters of A-Z, 0-9 and underscore.")
    return candidate


def subscriber_process(context: str, name: str, *, pipeline_name: str | None = None, prefix: str = "DLT") -> str:
    """A stable subscriber name for one pipeline and provider.

    Stable because SAP identifies the delta pointer by it: a name that changed
    between runs would silently start a new full load every time. Distinct by
    pipeline because two pipelines sharing a subscriber share one SAP queue.
    """
    context = check_identifier(context, what="ODP context")
    if pipeline_name is None:
        pipeline_name = str(dlt.current.pipeline().pipeline_name)
    prefix = check_subscriber_process(prefix, what="subscriber prefix")
    fingerprint = hashlib.sha1(f"{pipeline_name}\0{context}\0{name}".encode()).hexdigest()[:10].upper()
    stem_width = 32 - len(prefix) - len(fingerprint) - 2
    if stem_width < 1:
        raise QueryError("subscriber prefix leaves no room for a stable ODP subscriber hash.")
    stem = _safe_subscriber_piece(f"{pipeline_name}_{name}", fallback="ODP")[:stem_width]
    return f"{prefix}_{stem}_{fingerprint}"


def odp_rfc_query(
    context: str,
    name: str,
    subscriber: str,
    *,
    delta: bool,
    columns: Sequence[str] | None,
    recovery_parameter: str | None = None,
) -> str:
    """``sap_odp_read_full`` or ``sap_odp_read_delta`` for one provider."""
    context = check_identifier(context, what="ODP context")
    args = [sql_literal(context), sql_literal(name)]
    if delta:
        args.append(sql_literal(subscriber))
        if recovery_parameter:
            args.append(f"{check_identifier(recovery_parameter, what='recovery parameter').lower()} := true")
    if columns:
        checked = [check_identifier(column, what="column name") for column in columns]
        args.append("columns := [" + ", ".join(sql_literal(column) for column in checked) + "]")
    function = "sap_odp_read_delta" if delta else "sap_odp_read_full"
    return f"SELECT * FROM {function}({', '.join(args)})"


def odp_recovery_parameter(cursor: Any) -> str | None:
    """The named parameter that selects recover mode.

    Not discovered by inspection: ``duckdb_functions()`` reports a table
    function's *named* parameters positionally as ``col3``, ``col4`` and so on,
    so probing for a name always comes back empty and would silently disable
    recovery. The parameter is part of ERPL's documented surface; an ERPL too
    old to know it fails the call with a binder error, which is the honest
    outcome.
    """
    del cursor  # kept for call-site symmetry; see the docstring
    return ODP_RECOVERY_PARAMETER


def close_delta_cursor(cursor: Any, context: str, name: str, subscriber: str) -> None:
    """Release the ODQ delta cursor. Best effort, and never fatal.

    ERPL's own documentation: "Close the cursor with PRAGMA
    sap_odp_close_delta_cursor when finished -- delta cursors do not auto-close."
    A refusal is routine rather than exceptional: SAP will not close a cursor
    left mid-fetch, and reports it, which is more than the silence of not
    trying.
    """
    statement = (
        f"PRAGMA sap_odp_close_delta_cursor({sql_literal(check_identifier(context, what='ODP context'))}, "
        f"{sql_literal(subscriber)}, {sql_literal(name)})"
    )
    try:
        row = cursor.execute(statement).fetchone()
    except Exception as exc:  # noqa: BLE001 - see the docstring
        logger.warning("Could not close the ODP delta cursor for %s: %s", name, exc)
        return
    status = str(row[0]) if row else "UNKNOWN"
    if status.startswith("CLOSED"):
        logger.debug("Closed the ODP delta cursor for %s.", name)
    elif status.startswith("NOT_FOUND"):
        logger.debug("No open ODP delta cursor for %s.", name)
    else:
        logger.warning(
            "The ODP delta cursor for %s could not be closed (%s). It stays reserved on SAP until it times "
            "out; PRAGMA sap_odp_drop clears it sooner.",
            name,
            status,
        )


def resolve_subscriber_process(
    *,
    context: str,
    name: str,
    pipeline_name: str,
    explicit: str | None,
    state: dict[str, Any],
    resource: str,
) -> str:
    expected = (
        check_subscriber_process(explicit)
        if explicit is not None
        else subscriber_process(context, name, pipeline_name=pipeline_name)
    )
    stored = state.get("subscriber_process")
    if state.get("initialized") and not stored:
        raise ConfigurationError(f"{resource}: ODP state says it is initialized but has no subscriber_process.")
    if stored is None:
        return expected
    stored_subscriber = check_subscriber_process(str(stored), what=f"{resource} stored subscriber_process")
    if stored_subscriber != expected:
        raise ConfigurationError(
            f"{resource}: dlt state is tied to ODP subscriber {stored_subscriber!r}, but this run would use "
            f"{expected!r}. That would start a different SAP delta queue. Reset this resource's state, "
            "restore the previous pipeline name, or pass subscriber_processes with the stored value."
        )
    return stored_subscriber


def seed_delta_token_statements(url: str, entity_set: str, token: str) -> list[tuple[str, list[Any]]]:
    """Restore a Gateway delta token into erpl_web's own subscription table.

    DELETE then INSERT rather than an upsert: ``odp_subscriptions`` carries both
    a primary key and a UNIQUE(service_url, entity_set_name), and DuckDB refuses
    ``INSERT OR REPLACE`` on a table with two conflict targets. The pair runs in
    one transaction, because a crash between them would leave the resource with
    no stored position at all.
    """
    return [
        (
            "DELETE FROM erpl_web.odp_subscriptions WHERE service_url = ? AND entity_set_name = ?",
            [url, entity_set],
        ),
        (
            # Every NOT NULL column has to be named: erpl_web's own
            # subscription_id is timestamp-prefixed and changes each run, which
            # is also why the delete matches on the unique key instead.
            "INSERT INTO erpl_web.odp_subscriptions "
            "(subscription_id, service_url, entity_set_name, secret_name, delta_token, "
            " subscription_status, preference_applied, schema_version) "
            "VALUES (?, ?, ?, NULL, ?, 'active', TRUE, 1)",
            [f"erpl_dlt_{entity_set}", url, entity_set, token],
        ),
    ]


@dlt.source(name="erpl_odp")
def erpl_odp_source(
    names: Sequence[str] = dlt.config.value,
    credentials: SapRfcCredentials = dlt.secrets.value,
    *,
    context: str = "ABAP_CDS",
    settings: ErplSettings | None = None,
    columns: dict[str, Sequence[str]] | None = None,
    primary_keys: dict[str, Sequence[str]] | None = None,
    subscriber_processes: dict[str, str] | None = None,
    full_refresh: bool = False,
    recover: bool = False,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """ODP providers over RFC, with SAP-side delta.

    The first incremental run performs SAP's DELTAINIT -- a full snapshot plus a
    registered delta pointer -- and every later run returns only what changed.
    Set ``full_refresh`` to read a snapshot without touching the pointer.
    """
    if full_refresh and recover:
        raise ConfigurationError("ODP recovery is a delta mode; use either full_refresh or recover, not both.")
    unknown_subscribers = sorted(set(subscriber_processes or {}) - set(names))
    if unknown_subscribers:
        raise ConfigurationError(
            "subscriber_processes contains providers that are not in names: " + ", ".join(unknown_subscribers)
        )

    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=ODP_RFC_EXTENSIONS,
        settings=config,
        rfc_credentials=credentials,
        connection=connection,
    )

    def read_provider(name: str) -> Iterator[pa.RecordBatch]:
        state = dlt.current.resource_state().setdefault("odp", {})
        delta = not full_refresh
        cursor = erpl.cursor()
        recovery_parameter = None
        subscriber = ""
        if delta:
            subscriber = resolve_subscriber_process(
                context=context,
                name=name,
                pipeline_name=str(dlt.current.pipeline().pipeline_name),
                explicit=(subscriber_processes or {}).get(name),
                state=state,
                resource=name,
            )
            if recover:
                recovery_parameter = odp_recovery_parameter(cursor)
                if recovery_parameter is None:
                    raise ConfigurationError(
                        "sap_odp_read_delta does not expose an extraction-mode parameter in duckdb_functions(), "
                        "so erpl_dlt cannot request ODQ recover mode with this ERPL build."
                    )
        sql = odp_rfc_query(
            context,
            name,
            subscriber,
            delta=delta,
            columns=(columns or {}).get(name),
            recovery_parameter=recovery_parameter,
        )
        logger.debug("%s: %s", name, sql)
        try:
            reader = cursor.execute(sql).to_arrow_reader(config.batch_size)
            yield from reader
        finally:
            # ERPL is explicit that "delta cursors do not auto-close", so this
            # runs however the read ended. Closing is not confirming -- a cursor
            # left mid-fetch is refused rather than consumed -- so releasing it
            # on the failure path cannot cost the next run its packets, and
            # leaving it open would strand an ODQ cursor on the SAP system until
            # it times out.
            if delta:
                close_delta_cursor(cursor, context, name, subscriber)
        # Only once the whole provider has been consumed: the pointer on SAP has
        # advanced, and saying so earlier would strand packets nobody emitted.
        if delta:
            state["subscriber_process"] = subscriber
            state["initialized"] = True

    check_unique_names([f"{context}_{n}".lower().replace("$", "_").replace("/", "_") for n in names], what="provider")
    for name in names:
        key = list((primary_keys or {}).get(name) or [])
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            read_provider,
            name=f"{context}_{name}".lower().replace("$", "_").replace("/", "_"),
            write_disposition="merge" if key else "append",
            primary_key=key or None,
        )(name)


@dlt.source(name="erpl_odp_odata")
def erpl_odp_odata_source(
    entity_set_urls: Sequence[str] = dlt.config.value,
    credentials: ODataCredentials = dlt.secrets.value,
    *,
    settings: ErplSettings | None = None,
    primary_keys: dict[str, Sequence[str]] | None = None,
    full_refresh: bool = False,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """ODP providers over the SAP Gateway, with the delta token in dlt state."""
    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=WEB_EXTENSIONS,
        settings=config,
        odata_credentials=credentials,
        connection=connection,
    )

    def read_entity_set(url: str) -> Iterator[pa.RecordBatch]:
        entity_set = url.rstrip("/").rsplit("/", 1)[-1]
        state = dlt.current.resource_state().setdefault("odp_odata", {})
        cursor = erpl.cursor()
        token = None if full_refresh else state.get("delta_token")

        if token:
            # erpl_web reads its position from its own table, and the DuckDB file
            # is scratch here, so put it back before reading.
            cursor.execute("SELECT 1 FROM odp_odata_list_subscriptions() LIMIT 1").fetchall()
            cursor.execute("BEGIN TRANSACTION")
            try:
                for sql, parameters in seed_delta_token_statements(url, entity_set, str(token)):
                    cursor.execute(sql, parameters)
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

        args = ["?"]
        if full_refresh:
            args.append("force_full_load := true")
        reader = cursor.execute(f"SELECT * FROM odp_odata_read({', '.join(args)})", [url]).to_arrow_reader(
            config.batch_size
        )
        yield from reader

        row = cursor.execute(
            "SELECT delta_token FROM odp_odata_list_subscriptions() "
            "WHERE service_url = ? AND entity_set_name = ? ORDER BY last_updated DESC LIMIT 1",
            [url, entity_set],
        ).fetchone()
        if row and row[0]:
            state["delta_token"] = str(row[0])
        elif not full_refresh:
            logger.warning(
                "%s returned no delta token, so the next run loads everything again. "
                "This usually means the entity set is not delta-enabled.",
                entity_set,
            )

    for url in entity_set_urls:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        key = list((primary_keys or {}).get(url) or [])
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            read_entity_set,
            name=name.lower(),
            write_disposition="merge" if key else "append",
            primary_key=key or None,
        )(url)
