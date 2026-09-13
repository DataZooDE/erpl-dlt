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

import logging
from collections.abc import Iterator, Sequence
from typing import Any

import dlt
import pyarrow as pa
from dlt.sources import DltResource

from erpl_dlt.config import ErplSettings, ODataCredentials, SapRfcCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import check_identifier, sql_literal
from erpl_dlt.settings import ODP_RFC_EXTENSIONS, WEB_EXTENSIONS

logger = logging.getLogger("erpl_dlt")

#: Control columns ODP adds to every row. ODQ_CHANGEMODE says what happened to
#: the row and is data; the rest number the rows of one extraction and differ
#: between two extractions of identical data.
ODP_SEQUENCE_COLUMNS = ("ODQ_TSN", "ODQ_RECORDNO", "ODQ_UNITNO", "ODQ_ENTITYCNTR")


def subscriber_process(context: str, name: str, prefix: str = "DLT") -> str:
    """A stable subscriber name for one provider.

    Stable because SAP identifies the delta pointer by it: a name that changed
    between runs would silently start a new full load every time.
    """
    context = check_identifier(context, what="ODP context")
    safe = "".join(ch for ch in name.upper() if ch.isalnum())[:20]
    return f"{prefix}_{safe}"[:32]


def odp_rfc_query(context: str, name: str, subscriber: str, *, delta: bool, columns: Sequence[str] | None) -> str:
    """``sap_odp_read_full`` or ``sap_odp_read_delta`` for one provider."""
    context = check_identifier(context, what="ODP context")
    args = [sql_literal(context), sql_literal(name)]
    if delta:
        args.append(sql_literal(subscriber))
    if columns:
        checked = [check_identifier(column, what="column name") for column in columns]
        args.append("columns := [" + ", ".join(sql_literal(column) for column in checked) + "]")
    function = "sap_odp_read_delta" if delta else "sap_odp_read_full"
    return f"SELECT * FROM {function}({', '.join(args)})"


def seed_delta_token_statements(url: str, entity_set: str, token: str) -> list[tuple[str, list[Any]]]:
    """Restore a Gateway delta token into erpl_web's own subscription table.

    DELETE then INSERT rather than an upsert: ``odp_subscriptions`` carries both
    a primary key and a UNIQUE(service_url, entity_set_name), and DuckDB refuses
    ``INSERT OR REPLACE`` on a table with two conflict targets. The pair runs in
    one transaction, because a crash between them would leave the resource with
    no stored position at all.
    """
    return [
        ("DELETE FROM erpl_web.odp_subscriptions WHERE service_url = ? AND entity_set_name = ?", [url, entity_set]),
        (
            "INSERT INTO erpl_web.odp_subscriptions (service_url, entity_set_name, delta_token, last_updated) "
            "VALUES (?, ?, ?, now())",
            [url, entity_set, token],
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
    full_refresh: bool = False,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """ODP providers over RFC, with SAP-side delta.

    The first incremental run performs SAP's DELTAINIT -- a full snapshot plus a
    registered delta pointer -- and every later run returns only what changed.
    Set ``full_refresh`` to read a snapshot without touching the pointer.
    """
    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=ODP_RFC_EXTENSIONS,
        settings=config,
        rfc_credentials=credentials,
        connection=connection,
    )

    def read_provider(name: str) -> Iterator[pa.RecordBatch]:
        subscriber = subscriber_process(context, name)
        state = dlt.current.resource_state().setdefault("odp", {})
        delta = not full_refresh
        sql = odp_rfc_query(context, name, subscriber, delta=delta, columns=(columns or {}).get(name))
        logger.debug("%s: %s", name, sql)
        cursor = erpl.cursor()
        reader = cursor.execute(sql).to_arrow_reader(config.batch_size)
        yield from reader
        # Only once the whole provider has been consumed: the pointer on SAP has
        # advanced, and saying so earlier would strand packets nobody emitted.
        if delta:
            state["subscriber_process"] = subscriber
            state["initialized"] = True

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
