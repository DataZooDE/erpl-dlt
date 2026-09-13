"""dlt sources for SAP, via the ERPL DuckDB extensions.

The Python glue here is Apache-2.0. The extension binaries it loads are
separately licensed under BUSL-1.1 -- see NOTICE.
"""

from erpl_dlt.bics import erpl_bics_source
from erpl_dlt.config import ErplSettings, ODataCredentials, SapRfcCredentials
from erpl_dlt.connection import ErplConnection
from erpl_dlt.invoke import erpl_invoke_source
from erpl_dlt.odata import erpl_odata_source
from erpl_dlt.odp import erpl_odp_odata_source, erpl_odp_source
from erpl_dlt.rfc import erpl_rfc_source
from erpl_dlt.settings import ERPL_VERSION

__version__ = "0.1.0"

#: The ERPL release this package is tested against.
__erpl_version__ = ERPL_VERSION

__all__ = [
    "ErplConnection",
    "ErplSettings",
    "ODataCredentials",
    "SapRfcCredentials",
    "__erpl_version__",
    "__version__",
    "erpl_bics_source",
    "erpl_invoke_source",
    "erpl_odata_source",
    "erpl_odp_odata_source",
    "erpl_odp_source",
    "erpl_rfc_source",
]
