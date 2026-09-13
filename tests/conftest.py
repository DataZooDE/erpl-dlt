"""Integration tests run only when a real SAP system is configured.

`ERPL_IT=1` plus credentials. Skipped by default, so `pytest` is safe anywhere.
"""

import os

import pytest

from erpl_dlt.config import ErplSettings, SapRfcCredentials


def _enabled() -> bool:
    return os.environ.get("ERPL_IT") == "1"


@pytest.fixture(scope="session")
def sap_credentials() -> SapRfcCredentials:
    if not _enabled():
        pytest.skip("set ERPL_IT=1 and the ERPL_SAP_* variables to run integration tests")
    credentials = SapRfcCredentials(
        ashost=os.environ.get("ERPL_SAP_ASHOST", "localhost"),
        sysnr=os.environ.get("ERPL_SAP_SYSNR", "00"),
        client=os.environ.get("ERPL_SAP_CLIENT", "001"),
        user=os.environ.get("ERPL_SAP_USER", "DEVELOPER"),
        password=os.environ.get("ERPL_SAP_PASSWORD"),
        lang=os.environ.get("ERPL_SAP_LANG", "EN"),
    )
    credentials.on_resolved()
    return credentials


@pytest.fixture(scope="session")
def settings() -> ErplSettings:
    return ErplSettings(batch_size=5_000, extension_dir=os.environ.get("ERPL_EXTENSION_DIR"))


@pytest.fixture()
def pipelines_dir(tmp_path) -> str:
    return str(tmp_path / "dlt")
