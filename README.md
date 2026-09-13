# erpl-dlt

> **Licence notice, above the fold.** This package — the Python glue — is
> Apache-2.0. The ERPL DuckDB extension binaries it loads are **separately
> licensed under BUSL-1.1** by DataZoo GmbH. Installing `erpl-dlt` grants you no
> licence to them. See [NOTICE](NOTICE).

dlt sources for SAP, built on the [ERPL](https://erpl.io) DuckDB extensions.

```bash
pip install "erpl-dlt[extensions]"
```

```python
import dlt
from erpl_dlt import erpl_rfc_source

pipeline = dlt.pipeline(pipeline_name="sap", destination="duckdb", dataset_name="sap_raw")
pipeline.run(erpl_rfc_source(table_names=["SFLIGHT", "SBOOK"]))
```

Five SAP read paths, one connection layer:

| Source | Reads |
|---|---|
| `erpl_rfc_source` | Any table or CDS view over RFC, with projection and filter push-down |
| `erpl_odata_source` | Any OData v2/v4 service |
| `erpl_odp_source` | ODP delta — real server-side change data, over RFC or the SAP Gateway |
| `erpl_bics_source` | BW InfoProviders and BEx queries |
| `erpl_invoke_source` | Remote-enabled function modules and BAPIs |
