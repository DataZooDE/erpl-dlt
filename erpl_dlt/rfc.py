"""Tables and CDS views over RFC.

The extract is Arrow-native: ``to_arrow_reader`` streams ``RecordBatch`` objects
straight from DuckDB into dlt, so no row ever becomes a Python dict. The SAP
types arrive correct at the source -- ``DATS`` as ``date32[day]``, ``CURR`` as
``decimal128``, an empty SAP date as NULL -- so nothing needs casting or
patching downstream.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import dlt
import pyarrow as pa
from dlt.sources import DltResource

from erpl_dlt.config import ErplSettings, SapRfcCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import incremental_predicate, table_read_query
from erpl_dlt.settings import RFC_EXTENSIONS

logger = logging.getLogger("erpl_dlt")


def _batches(
    connection: ErplConnection,
    sql: str,
    batch_size: int,
    parameters: list[Any] | None = None,
) -> Iterator[pa.RecordBatch]:
    """Stream one query's results as Arrow batches on this thread's cursor."""
    cursor = connection.cursor()
    result = cursor.execute(sql, parameters) if parameters else cursor.execute(sql)
    # to_arrow_reader, not fetch_record_batch: the latter is deprecated in
    # DuckDB 1.5 and tells you so on every call.
    yield from result.to_arrow_reader(batch_size)


@dlt.source(name="erpl_rfc")
def erpl_rfc_source(
    table_names: Sequence[str] = dlt.config.value,
    credentials: SapRfcCredentials = dlt.secrets.value,
    *,
    settings: ErplSettings | None = None,
    columns: Mapping[str, Sequence[str]] | None = None,
    abap_filters: Mapping[str, str] | None = None,
    cursor_columns: Mapping[str, str] | None = None,
    primary_keys: Mapping[str, Sequence[str]] | None = None,
    max_rows: int | None = None,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """One dlt resource per SAP table or CDS view.

    Args:
        table_names: tables or CDS views to read, e.g. ``["SFLIGHT", "SBOOK"]``.
        credentials: resolved from ``[sources.erpl_dlt.credentials]`` by default.
        columns: per table, the columns to read. Pushed into SAP, and by far the
            most effective setting here -- naming two of 55 columns measured
            3.8x faster than reading them all.
        abap_filters: per table, an ABAP ``WHERE`` fragment evaluated by SAP.
            For predicates SQL can express, prefer ``cursor_columns`` or
            ``apply_hints``; DuckDB pushes those into the RFC call for you.
        cursor_columns: per table, the column to sync incrementally on.
        primary_keys: per table, the key. Sets ``merge`` where given, and
            ``replace`` where not.
        max_rows: a SAP-side row cap, useful for a first look at a large table.
        connection: an already-open DuckDB connection to use instead of opening
            one. Its lifetime stays yours.
    """
    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=RFC_EXTENSIONS,
        settings=config,
        rfc_credentials=credentials,
        connection=connection,
    )

    def make_reader(table: str, cursor_column: str | None) -> Any:
        """Build the generator for one table.

        The incremental has to be the *default value* of a parameter on the
        function dlt wraps -- passing one in at call time raises
        IncrementalUnboundError. Since each table has its own cursor column, the
        function is defined here, per table, rather than shared.
        """

        # B008 (no calls in argument defaults) is exactly what dlt requires
        # here: an incremental bound anywhere else raises
        # IncrementalUnboundError when the resource is built dynamically.
        def read(
            incremental: dlt.sources.incremental[Any] | None = (
                dlt.sources.incremental(cursor_column.upper()) if cursor_column else None  # noqa: B008
            ),
        ) -> Iterator[pa.RecordBatch]:
            where = None
            if cursor_column and incremental is not None and incremental.last_value is not None:
                where = incremental_predicate(cursor_column, incremental.last_value)
            elif cursor_column:
                logger.info("%s: first run, reading the whole table", table)
            sql = table_read_query(
                table,
                columns=(columns or {}).get(table),
                where=where,
                abap_filter=(abap_filters or {}).get(table),
                max_rows=max_rows,
            )
            logger.debug("%s: %s", table, sql)
            yield from _batches(erpl, sql, config.batch_size)

        return read

    for table in table_names:
        key = list((primary_keys or {}).get(table) or [])
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            make_reader(table, (cursor_columns or {}).get(table)),
            name=table.lower(),
            write_disposition="merge" if key else "replace",
            primary_key=key or None,
            parallelized=True,
        )()
