"""The embedded DuckDB connection: extensions, native libraries, secrets.

Three things here are not obvious and each one was learned the hard way against
a real system:

1. The SAP NetWeaver RFC and ICU shared objects must be loaded into the global
   symbol namespace *before* DuckDB loads ``erpl_rfc``. Without that the process
   does not raise -- it **core-dumps** with "Could not open the ICU common
   library". ``LD_LIBRARY_PATH`` is read by the dynamic loader at process start,
   so nothing Python does afterwards can substitute for this.
2. A single ``DuckDBPyConnection`` is not safe to use from several threads. Each
   worker takes ``con.cursor()``, which shares the database and isolates the
   execution state.
3. Credentials are bound as parameters to ``CREATE SECRET``. They are never
   formatted into SQL text, because query text reaches logs.
"""

from __future__ import annotations

import ctypes
import logging
import tempfile
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from erpl_dlt.config import ErplSettings, ODataCredentials, SapRfcCredentials

if TYPE_CHECKING:  # pragma: no cover - typing only
    from types import TracebackType

logger = logging.getLogger("erpl_dlt")

RFC_SECRET_NAME = "erpl_dlt_sap"
HTTP_SECRET_NAME = "erpl_dlt_http"

#: Suffixes of the shared objects shipped beside the extensions.
_NATIVE_SUFFIXES = (".so", ".so.50")


def default_extension_dir(settings: ErplSettings | None = None) -> str | None:
    """Where the extensions are, configuration first, then the wheel.

    Returns ``None`` when neither is available, so the caller can fall back to
    DuckDB's own extension resolution (a custom repository, or a local build).
    """
    if settings is not None and settings.extension_dir:
        return settings.extension_dir
    try:
        from erpl_extensions import extension_dir, is_populated
    except ImportError:
        return None
    # A wheel built without its payload installs cleanly; using it anyway would
    # fail later, inside DuckDB, in a message about extensions rather than about
    # packaging.
    return str(extension_dir()) if is_populated() else None


def preload_native_libraries(extension_dir: str | None) -> None:
    """Put the SAP and ICU symbols where the extension will look for them.

    Best effort: a failure here is not worth ending an extract for, because the
    real error arrives from DuckDB's own ``LOAD`` naming the extension.
    """
    if not extension_dir:
        return
    root = Path(extension_dir)
    if not root.is_dir():
        return
    for candidate in sorted(root.rglob("*")):
        if not candidate.is_file() or not candidate.name.endswith(_NATIVE_SUFFIXES):
            continue
        try:
            ctypes.CDLL(str(candidate), mode=ctypes.RTLD_GLOBAL)
        except OSError as exc:  # noqa: PERF203 - see the docstring
            logger.debug("Could not preload %s: %s", candidate.name, exc)


class ErplConnection:
    """An embedded DuckDB with the ERPL extensions loaded and secrets in place.

    Use it as a context manager, or pass an already-open connection in and this
    class will leave its lifetime alone.
    """

    def __init__(
        self,
        *,
        extensions: Iterable[str],
        settings: ErplSettings | None = None,
        rfc_credentials: SapRfcCredentials | None = None,
        odata_credentials: ODataCredentials | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.settings = settings or ErplSettings()
        self._extensions = tuple(extensions)
        self._owns_connection = connection is None
        self._temp_dir: tempfile.TemporaryDirectory[str] | None = None
        self._lock = threading.Lock()
        self._connection = connection if connection is not None else self._open()
        if self._owns_connection:
            self._bootstrap(rfc_credentials, odata_credentials)

    # ---- lifetime ------------------------------------------------------------

    def _database_path(self) -> str:
        if self.settings.database_path:
            return self.settings.database_path
        # Not ':memory:': erpl_web keeps ODP delta subscriptions in its own table
        # and refuses an in-memory database.
        self._temp_dir = tempfile.TemporaryDirectory(prefix="erpl-dlt-")
        return str(Path(self._temp_dir.name) / "erpl.duckdb")

    def _open(self) -> duckdb.DuckDBPyConnection:
        extension_dir = default_extension_dir(self.settings)
        preload_native_libraries(extension_dir)
        config: dict[str, str | bool | int | float | list[str]] = {"threads": str(self.settings.duckdb_threads)}
        if self.settings.allow_unsigned_extensions:
            config["allow_unsigned_extensions"] = "true"
        if extension_dir:
            config["extension_directory"] = extension_dir
        return duckdb.connect(self._database_path(), config=config)

    def _bootstrap(
        self,
        rfc_credentials: SapRfcCredentials | None,
        odata_credentials: ODataCredentials | None,
    ) -> None:
        con = self._connection
        if self.settings.custom_extension_repository:
            con.execute(f"SET custom_extension_repository = '{self.settings.custom_extension_repository}'")
        for extension in self._extensions:
            try:
                con.load_extension(extension)
            except Exception as exc:
                raise RuntimeError(
                    f"Could not load the ERPL extension {extension!r}. Install erpl-dlt[extensions], "
                    "or point settings.extension_dir at a directory containing the .duckdb_extension files."
                ) from exc
        # Telemetry off: this is someone's production SAP system.
        con.execute("SET erpl_telemetry_enabled = false")
        if rfc_credentials is not None:
            self._create_secret(RFC_SECRET_NAME, "sap_rfc", rfc_credentials.secret_parameters())
        if odata_credentials is not None and odata_credentials.secret_type:
            self._create_secret(HTTP_SECRET_NAME, odata_credentials.secret_type, odata_credentials.secret_parameters())

    def _create_secret(self, name: str, secret_type: str, parameters: dict[str, str]) -> None:
        """Register a secret without ever putting its values in the SQL text."""
        if not parameters:
            return
        # Keys are field names we control; only the values are user data, and
        # every one of them is bound.
        assignments = ", ".join(f"{key} ${key.lower()}" for key in parameters)
        sql = f"CREATE OR REPLACE SECRET {name} (TYPE {secret_type}, {assignments})"
        bound = {key.lower(): value for key, value in parameters.items()}
        self._connection.execute(sql, bound)

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A cursor of its own for the calling thread."""
        with self._lock:
            return self._connection.cursor()

    def close(self) -> None:
        if self._owns_connection:
            self._connection.close()
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

    def __enter__(self) -> ErplConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ---- convenience ---------------------------------------------------------

    def ping(self) -> bool:
        """Whether the SAP system answers. Cheap, and a real round trip."""
        try:
            self.cursor().execute("PRAGMA sap_rfc_ping").fetchall()
        except Exception as exc:
            logger.warning("SAP did not answer a ping: %s", exc)
            return False
        return True

    def execute(self, sql: str, parameters: list[Any] | None = None) -> duckdb.DuckDBPyConnection:
        cursor = self.cursor()
        return cursor.execute(sql, parameters) if parameters else cursor.execute(sql)
