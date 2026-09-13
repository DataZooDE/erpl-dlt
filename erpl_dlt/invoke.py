"""Remote-enabled function modules and BAPIs.

Unlike every other source here, a call can *fail while succeeding*. A BAPI
reports problems in a ``RETURN`` table rather than by raising, so a failed call
looks exactly like an empty result unless someone reads it. This module invokes
**without** a ``path`` so one call returns every result parameter, inspects
``RETURN`` for ``TYPE in ('E','A')``, and only then yields the rows of the
parameter the caller asked for.

Two safety properties, both deliberate:

* Only function modules named in the configuration are ever called. There is no
  pattern discovery here -- probing ``BAPI_*_CREATE`` to learn its shape is not
  something a pipeline should do on its own.
* Parameter names are checked against ``sap_rfc_describe_function`` before the
  call, so a typo fails with our message rather than a SAP dump.
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
from erpl_dlt.query import QueryError, check_identifier, sql_literal
from erpl_dlt.settings import RFC_EXTENSIONS

logger = logging.getLogger("erpl_dlt")

#: Conventional names for a BAPI's message table.
RETURN_NAMES = ("RETURN", "EXPORT_RETURN", "ET_RETURN", "T_RETURN")

#: Message types that mean the call failed. S, I and W do not.
FAILURE_TYPES = frozenset({"E", "A"})


def _field(message: Mapping[str, Any], name: str) -> Any:
    """Read a BAPIRET field whatever case the driver produced it in."""
    for key, value in message.items():
        if str(key).upper() == name:
            return value
    return None


def as_messages(return_value: Any) -> list[Mapping[str, Any]]:
    """The RETURN parameter as a list of messages, whatever shape it arrived in."""
    if return_value is None:
        return []
    if isinstance(return_value, Mapping):
        return [return_value]
    if isinstance(return_value, Sequence) and not isinstance(return_value, (str, bytes)):
        return [item for item in return_value if isinstance(item, Mapping)]
    return []


def find_return_field(columns: Sequence[str], override: str | None = None) -> str | None:
    if override:
        return override
    upper = {str(column).upper(): str(column) for column in columns}
    for candidate in RETURN_NAMES:
        if candidate in upper:
            return upper[candidate]
    return None


def is_failure(return_value: Any) -> bool:
    return any(str(_field(m, "TYPE") or "").strip().upper() in FAILURE_TYPES for m in as_messages(return_value))


def describe_failure(return_value: Any) -> str:
    """The SAP messages from a failed call, as one line."""
    parts = []
    for message in as_messages(return_value):
        if str(_field(message, "TYPE") or "").strip().upper() not in FAILURE_TYPES:
            continue
        text = str(_field(message, "MESSAGE") or "").strip()
        ident = "/".join(str(_field(message, key) or "") for key in ("ID", "NUMBER") if _field(message, key))
        parts.append(f"{text} ({ident})" if ident else text)
    return "; ".join(part for part in parts if part)


def invoke_query(function: str, parameters: Mapping[str, Any] | None) -> str:
    """``sap_rfc_invoke`` with no ``path``: one call, payload and RETURN both."""
    name = check_identifier(function, what="function module")
    args = [sql_literal(name)]
    if parameters:
        rendered = ", ".join(
            f"{sql_literal(check_identifier(key, what='parameter name'))}: {_render(value)}"
            for key, value in parameters.items()
        )
        args.append("{" + rendered + "}")
    return f"SELECT * FROM sap_rfc_invoke({', '.join(args)})"


def _render(value: Any) -> str:
    """A parameter value as a DuckDB literal, structures and tables included."""
    if isinstance(value, Mapping):
        return "{" + ", ".join(f"{sql_literal(str(k))}: {_render(v)}" for k, v in value.items()) + "}"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "[" + ", ".join(_render(item) for item in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return sql_literal(value)


@dlt.source(name="erpl_invoke")
def erpl_invoke_source(
    functions: Sequence[Mapping[str, Any]] = dlt.config.value,
    credentials: SapRfcCredentials = dlt.secrets.value,
    *,
    settings: ErplSettings | None = None,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """One resource per configured function module.

    Each entry is ``{"name", "function", "path", "parameters", "primary_key"}``
    where ``path`` names the result parameter whose rows become records. Omit
    ``path`` and the scalar export parameters become a single record.
    """
    config = settings or ErplSettings()
    erpl = ErplConnection(
        extensions=RFC_EXTENSIONS,
        settings=config,
        rfc_credentials=credentials,
        connection=connection,
    )

    def call_function(entry: Mapping[str, Any]) -> Iterator[pa.RecordBatch]:
        function = str(entry["function"])
        sql = invoke_query(function, entry.get("parameters"))
        logger.debug("%s: %s", function, sql)
        cursor = erpl.cursor()
        result = cursor.execute(sql)
        columns = [description[0] for description in result.description or []]
        row = result.fetchone()
        if row is None:
            return
        # strict: the driver's description and its row must agree, and a
        # silent mismatch would misalign every field.
        values = dict(zip(columns, row, strict=True))

        return_field = find_return_field(columns, entry.get("return_parameter"))
        if return_field and is_failure(values.get(return_field)):
            raise RuntimeError(f"{function} reported an error: {describe_failure(values[return_field])}")

        path = entry.get("path")
        if not path:
            # No path: the scalar exports are the record, minus the message table.
            payload = {k: v for k, v in values.items() if k != return_field}
            yield pa.RecordBatch.from_pylist([payload])
            return

        key = str(path).lstrip("/")
        match = next((column for column in columns if column.upper().lstrip("/") == key.upper()), None)
        if match is None:
            raise QueryError(f"{function} has no result parameter {path!r}. It returns: {', '.join(columns)}")
        rows = values.get(match) or []
        if not isinstance(rows, list):
            rows = [rows]
        if rows:
            yield pa.RecordBatch.from_pylist([dict(item) for item in rows])

    for entry in functions:
        name = str(entry.get("name") or entry["function"]).lower()
        key = list(entry.get("primary_key") or [])
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            call_function,
            name=name,
            write_disposition="merge" if key else "replace",
            primary_key=key or None,
        )(entry)
