"""Replicate an OData service, and an ODP delta feed, into DuckDB.

    ERPL_ODATA_URL=... python examples/odata_pipeline.py
"""

import os

import dlt

from erpl_dlt import ODataCredentials, erpl_odata_source, erpl_odp_odata_source

credentials = ODataCredentials(
    base_url=os.environ.get("ERPL_ODATA_BASE_URL", "http://localhost:50000"),
    username=os.environ.get("ERPL_ODATA_USER"),
    password=os.environ.get("ERPL_ODATA_PASSWORD"),
)

pipeline = dlt.pipeline(pipeline_name="sap_odata", destination="duckdb", dataset_name="sap_odata")

# Any OData v2/v4 service.
pipeline.run(
    erpl_odata_source(
        entity_sets=["/sap/opu/odata/sap/Z_SRV/Entities"],
        credentials=credentials,
        max_page_size=5000,
    )
)

# ODP over the Gateway: the delta token lives in dlt state, so the second run
# returns only what changed -- even in a fresh process.
pipeline.run(
    erpl_odp_odata_source(
        entity_set_urls=[os.environ["ERPL_ODATA_URL"]],
        credentials=credentials,
    )
)
