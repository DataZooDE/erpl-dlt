"""Credentials and settings, resolved through dlt's configuration injection.

Values come from ``.dlt/secrets.toml`` under ``[sources.erpl_dlt.credentials]``,
from environment variables, or from a vault, with no code change. Nothing here
ever reaches a SQL string: the connection layer binds every value as a
parameter to ``CREATE SECRET``.
"""

from __future__ import annotations

import dlt
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.typing import TSecretStrValue

from erpl_dlt.settings import DEFAULT_BATCH_SIZE, DEFAULT_DUCKDB_THREADS


@configspec
class SapRfcCredentials(CredentialsConfiguration):
    """An SAP logon, direct or load-balanced.

    Fill in either ``ashost`` + ``sysnr`` (direct) or ``mshost`` + ``sysid`` +
    ``group`` (load-balanced). ``password`` is optional because SNC and SSO2
    identify the user instead.
    """

    client: str = dlt.config.value
    user: str = dlt.config.value
    password: TSecretStrValue | None = dlt.secrets.value

    # Direct logon.
    ashost: str | None = None
    sysnr: str | None = None

    # Load-balanced logon.
    mshost: str | None = None
    sysid: str | None = None
    group: str | None = None

    lang: str = "EN"
    saprouter: str | None = None

    # SNC. Without these the RFC connection is unencrypted and the password
    # travels in the clear, which is fine for a local trial and wrong elsewhere.
    snc_mode: str | None = None
    snc_partnername: str | None = None
    snc_lib: str | None = None
    snc_qop: str | None = None

    def on_resolved(self) -> None:
        if not (self.ashost or self.mshost):
            raise ValueError(
                "SAP logon needs either 'ashost' (direct) or 'mshost' (load-balanced). Neither was configured."
            )
        if self.ashost and not self.sysnr:
            raise ValueError("A direct logon needs 'sysnr' alongside 'ashost'.")
        if self.mshost and not (self.sysid and self.group):
            raise ValueError("A load-balanced logon needs 'sysid' and 'group' alongside 'mshost'.")
        if not self.client or not self.user:
            raise ValueError("SAP logon needs 'client' and 'user'.")

    def secret_parameters(self) -> dict[str, str]:
        """The ``CREATE SECRET`` fields, as bound parameters."""
        fields = {
            "ASHOST": self.ashost,
            "SYSNR": self.sysnr,
            "MSHOST": self.mshost,
            "SYSID": self.sysid,
            "GROUP": self.group,
            "CLIENT": self.client,
            "USER": self.user,
            "PASSWD": self.password,
            "LANG": self.lang,
            "SAPROUTER": self.saprouter,
            "SNC_MODE": self.snc_mode,
            "SNC_PARTNERNAME": self.snc_partnername,
            "SNC_LIB": self.snc_lib,
            "SNC_QOP": self.snc_qop,
        }
        return {key: str(value) for key, value in fields.items() if value not in (None, "")}


@configspec
class ODataCredentials(CredentialsConfiguration):
    """An OData service: basic auth, or a bearer token."""

    base_url: str = dlt.config.value
    username: str | None = None
    password: TSecretStrValue | None = None
    token: TSecretStrValue | None = None

    def on_resolved(self) -> None:
        if not self.base_url:
            raise ValueError("An OData source needs 'base_url'.")
        if self.username and not self.password:
            raise ValueError("'username' was given without 'password'.")

    @property
    def is_insecure(self) -> bool:
        """A plain-http service sends these credentials in the clear."""
        return self.base_url.strip().lower().startswith("http://")  # ignore-https-check

    def secret_parameters(self) -> dict[str, str]:
        if self.token:
            return {"SCOPE": self.base_url, "TOKEN": str(self.token)}
        if self.username:
            return {"SCOPE": self.base_url, "USERNAME": self.username, "PASSWORD": str(self.password)}
        return {}

    @property
    def secret_type(self) -> str | None:
        if self.token:
            return "bearer"
        if self.username:
            return "http_basic"
        return None


@configspec
class ErplSettings(BaseConfiguration):
    """How the embedded DuckDB is set up. Sensible without configuration."""

    batch_size: int = DEFAULT_BATCH_SIZE
    duckdb_threads: int = DEFAULT_DUCKDB_THREADS

    #: Where the ``.duckdb_extension`` files live. Unset means "ask the
    #: erpl-extensions package", which is how they arrive for most people.
    extension_dir: str | None = None

    #: ERPL's extensions are unsigned, so this is on by default. It applies to
    #: the connection this package opens, not to any connection you pass in.
    allow_unsigned_extensions: bool = True

    #: A private extension repository, for people not using the wheel.
    custom_extension_repository: str | None = None

    #: erpl_web stores ODP delta tokens in its own table, which needs a file --
    #: it refuses ``:memory:``. Unset means a temporary file per connection.
    database_path: str | None = None
