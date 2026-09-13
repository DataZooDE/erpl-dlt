"""BW queries and InfoProviders, through BICS.

BICS is not a table read. One extract is a *session*: open a data area, set the
rows, columns, filters and display properties, then ask for the result. Only the
last statement yields anything, so a resource runs the preamble on its own
cursor before it streams a single batch.

Two consequences worth knowing before using this:

* **BW does not paginate.** It materialises the whole result set or none of it,
  so a large cube is bounded by slicing -- one session per characteristic
  member -- rather than by a batch size.
* **Mandatory BEx variables must be bound.** BW refuses to return a result while
  one is unfilled, and the symptom is an empty resource rather than an error.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import dlt
import pyarrow as pa
from dlt.sources import DltResource

from erpl_dlt.config import ErplSettings, SapRfcCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import sql_literal
from erpl_dlt.settings import BICS_EXTENSIONS

logger = logging.getLogger("erpl_dlt")


def session_statements(
    session_id: str,
    cube: str,
    *,
    query: str | None = None,
    rows: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    variables: Sequence[Mapping[str, str]] | None = None,
    filters: Sequence[Mapping[str, Any]] | None = None,
    variant: str | None = None,
) -> list[str]:
    """The begin → configure → result sequence for one BICS session.

    The last statement is the only one that returns rows; everything before it
    is setup that must run on the same cursor, in order.
    """
    begin = [sql_literal(cube), f"id := {sql_literal(session_id)}", "return := 'RESULT'"]
    if query:
        begin.append(f"query := {sql_literal(query)}")
    if variant:
        begin.append(f"variant := {sql_literal(variant)}")
    if variables:
        rendered = []
        for variable in variables:
            low = str(variable.get("low", ""))
            high = str(variable.get("high", ""))
            operator = variable.get("op") or ("BT" if high else "EQ")
            # `{'NAME': 'V', ...}` is DuckDB struct syntax; `{'V' AS NAME}` is
            # not, and fails at the parser before reaching SAP.
            rendered.append(
                "{"
                + ", ".join(
                    [
                        f"'NAME': {sql_literal(variable.get('name', ''))}",
                        f"'SIGN': {sql_literal(variable.get('sign', 'I'))}",
                        f"'OP': {sql_literal(operator)}",
                        f"'LOW': {sql_literal(low)}",
                        f"'HIGH': {sql_literal(high)}",
                    ]
                )
                + "}"
            )
        begin.append("variables := [" + ", ".join(rendered) + "]")

    statements = [f"SELECT * FROM sap_bics_begin({', '.join(begin)})"]
    if rows:
        row_list = ", ".join(sql_literal(item) for item in rows)
        statements.append(f"SELECT * FROM sap_bics_rows({sql_literal(session_id)}, {row_list}, op := 'SET')")
    if columns:
        column_list = ", ".join(sql_literal(item) for item in columns)
        statements.append(f"SELECT * FROM sap_bics_columns({sql_literal(session_id)}, {column_list}, op := 'SET')")
    for member_filter in filters or []:
        characteristic = member_filter.get("characteristic")
        if not characteristic:
            continue
        members = "".join(f", {sql_literal(member)}" for member in member_filter.get("members") or [])
        statements.append(
            f"SELECT * FROM sap_bics_filter({sql_literal(session_id)}, {sql_literal(characteristic)}{members}, "
            "op := 'SET')"
        )
    statements.append(f"SELECT * FROM sap_bics_result({sql_literal(session_id)})")
    return statements


@dlt.source(name="erpl_bics")
def erpl_bics_source(
    cube: str = dlt.config.value,
    credentials: SapRfcCredentials = dlt.secrets.value,
    *,
    queries: Sequence[str] | None = None,
    settings: ErplSettings | None = None,
    rows: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    variables: Sequence[Mapping[str, str]] | None = None,
    variant: str | None = None,
    slice_by: Mapping[str, Sequence[str]] | None = None,
    primary_key: Sequence[str] | None = None,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """One resource per BEx query, or one for the InfoProvider itself.

    Args:
        cube: the InfoProvider.
        queries: BEx queries on it. Omit to read the provider directly.
        rows / columns: characteristics and key figures.
        variables: BEx variables as ``{"name", "low", "high", "sign", "op"}``.
        variant: a saved variant, used instead of ``variables``.
        slice_by: ``{characteristic: [members]}`` -- one BICS session per member,
            which is the only way to bound memory on a large cube.
    """
    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=BICS_EXTENSIONS,
        settings=config,
        rfc_credentials=credentials,
        connection=connection,
    )

    def read_query(query: str | None, slice_filter: Mapping[str, Any] | None) -> Iterator[pa.RecordBatch]:
        # A fresh id per execution: two resources running in parallel must not
        # share a BICS session, and a retry must not resume a half-set one.
        session_id = f"dlt_{uuid.uuid4().hex[:12]}"
        statements = session_statements(
            session_id,
            cube,
            query=query,
            rows=rows,
            columns=columns,
            variables=variables,
            filters=[slice_filter] if slice_filter else None,
            variant=variant,
        )
        cursor = erpl.cursor()
        for statement in statements[:-1]:
            cursor.execute(statement).fetchall()
        reader = cursor.execute(statements[-1]).to_arrow_reader(config.batch_size)
        yield from reader

    targets: list[tuple[str | None, Mapping[str, Any] | None, str]] = []
    for query in list(queries) if queries else [None]:
        base = (query or cube).lower().replace("/", "_")
        if slice_by:
            # One session per member: BW materialises a whole result set, so
            # slicing is the memory control, and each slice is its own resource
            # so a failure does not cost the others.
            for characteristic, members in slice_by.items():
                for member in members:
                    slice_filter: dict[str, Any] = {"characteristic": characteristic, "members": [member]}
                    safe_member = "".join(ch if ch.isalnum() else "_" for ch in str(member)).lower()
                    targets.append((query, slice_filter, f"{base}_{safe_member}"))
        else:
            targets.append((query, None, base))

    for target_query, target_filter, name in targets:
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            read_query,
            name=name,
            write_disposition="merge" if primary_key else "replace",
            primary_key=list(primary_key) if primary_key else None,
        )(target_query, target_filter)
