"""Building the SQL that reaches the extensions.

Two rules, both load-bearing:

* **Identifiers are validated, never quoted.** A table or column name is
  interpolated bare, because SQL has no way to parameterise one -- so it is
  checked against the shape SAP allows and rejected otherwise.
* **Literals are escaped at the point of use.** Every value that becomes part of
  a statement goes through :func:`sql_literal`, including values that came back
  from dlt's incremental state, which is replayed from storage and is not
  necessarily ours.
"""

from __future__ import annotations

import datetime
import re
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

#: SAP field and table names: A-Z, digits, underscore and slash, up to 30.
SAP_IDENTIFIER = re.compile(r"^[A-Z0-9_/]{1,30}$")

#: Longest value accepted from incremental state on its way into a statement.
#: SAP takes an ABAP WHERE clause as 72-character lines, so a multi-kilobyte
#: value dumps inside SAP rather than failing here. Real cursors are short.
MAX_CURSOR_VALUE = 255


class QueryError(ValueError):
    """A query could not be built from the given configuration."""


class ConfigurationError(ValueError):
    """The source was configured in a way that would lose or duplicate data."""


def check_unique_names(names: list[str], *, what: str = "resource") -> None:
    """Refuse a source whose resources would share a name.

    Names are derived -- a table name, the last segment of a URL, an ODP
    provider with its punctuation folded -- so two different objects can land on
    one name, and dlt would then give them one table and one incremental state.
    """
    seen: dict[str, int] = {}
    for name in names:
        seen[name] = seen.get(name, 0) + 1
    collisions = sorted(name for name, count in seen.items() if count > 1)
    if collisions:
        raise ConfigurationError(
            f"two or more {what}s resolve to the same name: {', '.join(collisions)}. "
            "They would share a destination table and one incremental state. Rename or split the source."
        )


def resolve_write_disposition(*, primary_key: object, cursor_column: object, explicit: object, resource: str) -> str:
    """Pick a write disposition, refusing the combination that silently truncates.

    `replace` with an incremental cursor is the trap: the second run extracts
    only rows at or after the cursor, and `replace` then overwrites the whole
    table with that increment. Measured against dlt 1.30 in isolation -- 10 rows
    became 6 -- and it does not announce itself, because a run where nothing
    changed deduplicates to zero rows and leaves the table alone. It only bites
    once there is real data to load.
    """
    if explicit:
        return str(explicit)
    if primary_key:
        return "merge"
    if cursor_column:
        raise ConfigurationError(
            f"{resource}: an incremental cursor needs a primary key. Without one the disposition would "
            "be 'replace', and the second run would overwrite the table with just the increment. "
            "Pass primary_keys for this resource, or set write_disposition='append' to accept "
            "duplicates at the cursor boundary."
        )
    return "replace"


#: OData property names are case-sensitive and mixed case is normal
#: (``SalesOrderItem``), so they are checked but never folded.
ODATA_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def check_odata_identifier(name: str, *, what: str = "property") -> str:
    """Return ``name`` unchanged, or raise if it is not an OData property name.

    Deliberately not :func:`check_identifier`: uppercasing ``SalesOrderItem``
    into ``SALESORDERITEM`` would produce a predicate the service rejects, since
    OData compares property names case-sensitively.
    """
    candidate = str(name).strip()
    if not ODATA_IDENTIFIER.match(candidate):
        raise QueryError(
            f"{what} {name!r} is not a valid OData property name: a letter or underscore followed by "
            "letters, digits and underscores."
        )
    return candidate


def check_identifier(name: str, *, what: str = "name") -> str:
    """Return ``name`` uppercased, or raise if it is not a SAP identifier."""
    candidate = str(name).strip().upper()
    if not SAP_IDENTIFIER.match(candidate):
        raise QueryError(
            f"{what} {name!r} is not a valid SAP identifier: 1-30 characters of A-Z, 0-9, underscore and slash."
        )
    return candidate


def sql_literal(value: Any) -> str:
    """A DuckDB string literal, with quotes doubled."""
    return "'" + str(value).replace("'", "''") + "'"


def abap_literal(value: Any) -> str:
    """A value as ABAP's WHERE clause wants it.

    Dates and times go back to their compact DDIC form: SAP rejects
    ``'2026-09-05'`` for a ``D(8,0)`` column with "is not a valid value".
    """
    if isinstance(value, datetime.datetime):
        return "'" + value.strftime("%Y%m%d%H%M%S") + "'"
    if isinstance(value, datetime.date):
        return "'" + value.strftime("%Y%m%d") + "'"
    if isinstance(value, datetime.time):
        return "'" + value.strftime("%H%M%S") + "'"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    text = str(value)
    if len(text) > MAX_CURSOR_VALUE:
        raise QueryError(
            f"A cursor value of {len(text)} characters is over the {MAX_CURSOR_VALUE} this source will send "
            "to SAP. Reset the pipeline's state for this resource."
        )
    return "'" + text.replace("'", "''") + "'"


def table_read_query(
    table: str,
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    abap_filter: str | None = None,
    max_rows: int | None = None,
    threads: int | None = None,
    extra_params: dict[str, Any] | None = None,
) -> str:
    """A ``sap_read_table`` query.

    ``where`` is ordinary SQL and is pushed into the RFC call by DuckDB --
    ``EXPLAIN`` shows it as ``Filters:`` inside the ``SAP_READ_TABLE`` node.
    ``abap_filter`` is passed to SAP verbatim as ``FILTER :=`` and exists for
    predicates ABAP can express and SQL cannot.
    """
    name = check_identifier(table, what="table name")
    projection = "*"
    args: list[str] = [sql_literal(name)]
    if columns:
        checked = [check_identifier(column, what="column name") for column in columns]
        # Both: COLUMNS tells SAP what to select, the projection list tells
        # DuckDB, and the two must agree or the scan reads more than it emits.
        args.append("COLUMNS := [" + ", ".join(sql_literal(column) for column in checked) + "]")
        projection = ", ".join(checked)
    if abap_filter:
        args.append(f"FILTER := {sql_literal(abap_filter)}")
    if max_rows is not None:
        args.append(f"MAX_ROWS := {int(max_rows)}")
    if threads is not None:
        args.append(f"THREADS := {int(threads)}")
    for key, value in (extra_params or {}).items():
        args.append(
            f"{check_identifier(key, what='parameter')} := {value if isinstance(value, int) else sql_literal(value)}"
        )
    sql = f"SELECT {projection} FROM sap_read_table({', '.join(args)})"
    if where:
        sql += f" WHERE {where}"
    return sql


def sql_value_literal(value: Any) -> str:
    """A value as a *SQL* literal, typed where DuckDB needs the type.

    Not the same as :func:`abap_literal`, and the difference matters. The
    extensions expose SAP's DATS and TIMS columns as DuckDB ``DATE`` and
    ``TIME``, so a predicate DuckDB pushes into the scan has to be written in
    SQL -- ``DATE '2026-09-05'``. The compact ``'20260905'`` form belongs only
    in an ABAP ``FILTER :=`` string, which SAP parses itself; sending it here
    fails with "invalid date field format".
    """
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, datetime.date):
        return "DATE '" + value.isoformat() + "'"
    if isinstance(value, datetime.time):
        return "TIME '" + value.isoformat() + "'"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    text = str(value)
    if len(text) > MAX_CURSOR_VALUE:
        raise QueryError(
            f"A cursor value of {len(text)} characters is over the {MAX_CURSOR_VALUE} this source will send "
            "to SAP. Reset the pipeline's state for this resource."
        )
    return sql_literal(text)


def incremental_predicate(column: str, last_value: Any) -> str:
    """``column >= last_value`` as SQL, for DuckDB to push into the scan.

    ``>=`` rather than ``>``: SAP date and time columns have one-day or
    one-second resolution, so a strict comparison drops every row that shares
    the boundary value. Duplicates are dlt's to deduplicate and it is equipped
    for it; a dropped row is nobody's.
    """
    name = check_identifier(column, what="cursor column")
    return f"{name} >= {sql_value_literal(last_value)}"


def odata_incremental_predicate(property_name: str, last_value: Any) -> str:
    """The same comparison for an OData property, with its case preserved."""
    return f"{check_odata_identifier(property_name)} >= {sql_value_literal(last_value)}"


def abap_predicate(column: str, last_value: Any) -> str:
    """The same comparison as an ABAP ``WHERE`` fragment, for ``FILTER :=``."""
    name = check_identifier(column, what="cursor column")
    return f"{name} >= {abap_literal(last_value)}"


def odata_read_query(
    url: str,
    *,
    where: str | None = None,
    top: int | None = None,
    skip: int | None = None,
    expand: str | None = None,
    max_page_size: int | None = None,
) -> tuple[str, list[Any]]:
    """An ``odata_read`` query, with the URL bound rather than interpolated.

    Returns ``(sql, parameters)``. The URL is a parameter because it is user
    input that frequently contains quotes in ``$filter`` expressions.
    """
    args = ["?"]
    if top is not None:
        args.append(f"top := {int(top)}")
    if skip is not None:
        args.append(f"skip := {int(skip)}")
    if expand:
        args.append(f"expand := {sql_literal(expand)}")
    if max_page_size is not None:
        args.append(f"max_page_size := {int(max_page_size)}")
    sql = f"SELECT * FROM odata_read({', '.join(args)})"
    if where:
        sql += f" WHERE {where}"
    return sql, [url]
