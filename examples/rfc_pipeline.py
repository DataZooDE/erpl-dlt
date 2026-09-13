"""Replicate SAP tables into DuckDB.

    ERPL_SAP_PASSWORD=... python examples/rfc_pipeline.py

Credentials normally come from .dlt/secrets.toml:

    [sources.erpl_dlt.credentials]
    ashost = "sap.example.com"
    sysnr = "00"
    client = "100"
    user = "SVC_DLT"
    password = "..."
"""

import os

import dlt

from erpl_dlt import SapRfcCredentials, erpl_rfc_source

credentials = SapRfcCredentials(
    ashost=os.environ.get("ERPL_SAP_ASHOST", "localhost"),
    sysnr="00",
    client="001",
    user=os.environ.get("ERPL_SAP_USER", "DEVELOPER"),
    password=os.environ["ERPL_SAP_PASSWORD"],
)

pipeline = dlt.pipeline(pipeline_name="sap_rfc", destination="duckdb", dataset_name="sap_raw")

info = pipeline.run(
    erpl_rfc_source(
        table_names=["SFLIGHT", "SBOOK"],
        credentials=credentials,
        # Naming columns pushes the projection into SAP. On a 55-column table
        # that measured 3.8x faster than reading everything.
        columns={"SBOOK": ["CARRID", "CONNID", "FLDATE", "BOOKID", "LOCCURAM"]},
        # An incremental cursor becomes a WHERE clause DuckDB pushes into the
        # RFC call, so SAP does the filtering.
        cursor_columns={"SBOOK": "FLDATE"},
        primary_keys={"SBOOK": ["CARRID", "CONNID", "FLDATE", "BOOKID"]},
    )
)
print(info)
