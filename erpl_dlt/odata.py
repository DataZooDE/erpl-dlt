"""Any OData v2/v4 service, through ``erpl_web``.

Version detection is the extension's job. Push-down is DuckDB's: a predicate in
the SQL ``WHERE`` shows up as ``Filters:`` inside the ``ODATA_READ`` node, which
is how an incremental cursor reaches the service rather than being applied after
the rows arrive.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import dlt
import pyarrow as pa
from dlt.sources import DltResource

from erpl_dlt.config import ErplSettings, ODataCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import (
    QueryError,
    check_unique_names,
    odata_incremental_predicate,
    odata_read_query,
    resolve_write_disposition,
)
from erpl_dlt.settings import WEB_EXTENSIONS

logger = logging.getLogger("erpl_dlt")


def resolve_entity_url(base_url: str, entity_set: str) -> str:
    """Join a service and an entity set, refusing to leave the service's host.

    An absolute URL in configuration would otherwise let the source fetch any
    host reachable from where it runs, using credentials scoped to this service.
    """
    text = str(entity_set).strip()
    if not text.lower().startswith(("http://", "https://")):  # ignore-https-check
        return base_url.rstrip("/") + "/" + text.lstrip("/")
    base, target = urlsplit(base_url), urlsplit(text)
    if (target.scheme, target.netloc) != (base.scheme, base.netloc):
        raise QueryError(
            f"The entity-set URL {text!r} points at {target.scheme}://{target.netloc}, which is not the "
            f"configured service ({base.scheme}://{base.netloc}). Use a path relative to base_url."
        )
    return text


@dlt.source(name="erpl_odata")
def erpl_odata_source(
    entity_sets: Sequence[str] = dlt.config.value,
    credentials: ODataCredentials = dlt.secrets.value,
    *,
    settings: ErplSettings | None = None,
    cursor_columns: Mapping[str, str] | None = None,
    primary_keys: Mapping[str, Sequence[str]] | None = None,
    top: int | None = None,
    expand: str | None = None,
    max_page_size: int | None = None,
    write_disposition: str | None = None,
    connection: Any | None = None,
) -> Iterator[DltResource]:
    """One dlt resource per OData entity set.

    Args:
        entity_sets: entity-set names or paths relative to ``credentials.base_url``.
        cursor_columns: per entity set, the field to sync incrementally on.
        primary_keys: per entity set, the key. ``merge`` where given.
        top / expand / max_page_size: passed to ``odata_read``.
        write_disposition: overrides the default; see `resolve_write_disposition`.
    """
    config = settings or ErplSettings()
    if credentials.is_insecure:
        logger.warning(
            "base_url is plain http, so these credentials and the data travel in the clear. "
            "Use https for anything but a local test service."
        )
    erpl = ErplConnection(
        extensions=WEB_EXTENSIONS,
        settings=config,
        odata_credentials=credentials,
        connection=connection,
    )

    def make_reader(entity_set: str, cursor_column: str | None) -> Any:
        """One generator per entity set; see the note in rfc.py about binding."""

        # See the note in rfc.py: dlt requires the incremental in the default.
        def read(
            incremental: dlt.sources.incremental[Any] | None = (
                dlt.sources.incremental(cursor_column) if cursor_column else None  # noqa: B008
            ),
        ) -> Iterator[pa.RecordBatch]:
            url = resolve_entity_url(credentials.base_url, entity_set)
            where = None
            if cursor_column and incremental is not None and incremental.last_value is not None:
                where = odata_incremental_predicate(cursor_column, incremental.last_value)
            sql, parameters = odata_read_query(url, where=where, top=top, expand=expand, max_page_size=max_page_size)
            logger.debug("%s: %s", entity_set, sql)
            reader = erpl.cursor().execute(sql, parameters).to_arrow_reader(config.batch_size)
            yield from reader

        return read

    check_unique_names([e.rstrip("/").rsplit("/", 1)[-1].lower() for e in entity_sets], what="entity set")
    for entity_set in entity_sets:
        name = entity_set.rstrip("/").rsplit("/", 1)[-1].lower()
        key = list((primary_keys or {}).get(entity_set) or [])
        yield dlt.resource(  # type: ignore[call-overload]  # dlt's overloads do not cover dynamically built resources
            make_reader(entity_set, (cursor_columns or {}).get(entity_set)),
            name=name,
            write_disposition=resolve_write_disposition(
                primary_key=key,
                cursor_column=(cursor_columns or {}).get(entity_set),
                explicit=write_disposition,
                resource=entity_set,
            ),
            primary_key=key or None,
            parallelized=True,
        )()
