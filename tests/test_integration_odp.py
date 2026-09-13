"""ODP delta against a real system.

The defining property is not "rows arrive" -- it is that the *second* run
returns only changes, and that the position survives a process boundary. dlt
state has to be sufficient on its own, because the DuckDB file this package uses
is scratch.
"""

import os
import subprocess
import sys
import textwrap

import dlt
import pytest

from erpl_dlt import erpl_odp_source

pytestmark = pytest.mark.integration

ODP_CONTEXT = os.environ.get("ERPL_SAP_ODP_CONTEXT", "ABAP_CDS")
ODP_NAME = os.environ.get("ERPL_SAP_ODP_NAME", "")
ODATA_URL = os.environ.get("ERPL_SAP_ODP_ODATA_URL", "")


def _needs_odp():
    if not ODP_NAME:
        pytest.skip("set ERPL_SAP_ODP_NAME to a provisioned ODP provider")


class TestOdpOverRfc:
    def test_the_first_run_returns_the_snapshot(self, sap_credentials, settings, pipelines_dir):
        _needs_odp()
        pipeline = dlt.pipeline(
            pipeline_name="it_odp", destination="duckdb", dataset_name="odp", pipelines_dir=pipelines_dir
        )
        # full_refresh: a delta read is history-dependent by design -- once an
        # earlier run has consumed the queue, a later one correctly returns
        # nothing and creates no table. Asserting "rows arrived" therefore has
        # to ask for a snapshot, which also leaves the delta pointer alone.
        pipeline.run(
            erpl_odp_source(names=[ODP_NAME], credentials=sap_credentials, context=ODP_CONTEXT, full_refresh=True)
        )
        table = f"{ODP_CONTEXT}_{ODP_NAME}".lower().replace("$", "_")
        with pipeline.sql_client() as client:
            count = client.execute_sql(f"SELECT count(*) FROM {table}")[0][0]
        assert count > 0, "a full ODP read must return the current snapshot"

    def test_the_subscriber_is_recorded_so_the_next_run_is_a_delta(self, sap_credentials, settings, pipelines_dir):
        _needs_odp()
        pipeline = dlt.pipeline(
            pipeline_name="it_odp_state", destination="duckdb", dataset_name="odp", pipelines_dir=pipelines_dir
        )
        pipeline.run(erpl_odp_source(names=[ODP_NAME], credentials=sap_credentials, context=ODP_CONTEXT))
        state = pipeline.state["sources"]["erpl_odp"]["resources"]
        recorded = [r for r in state.values() if r.get("odp", {}).get("initialized")]
        assert recorded, "nothing recorded the subscriber, so every run would be a full load"
        assert recorded[0]["odp"]["subscriber_process"].startswith("DLT_")


@pytest.mark.skipif(not ODATA_URL, reason="set ERPL_SAP_ODP_ODATA_URL")
class TestOdpOverTheGateway:
    def test_a_second_run_in_a_fresh_process_returns_nothing(self, tmp_path):
        """The property that matters: state alone is enough.

        Run twice in *separate processes* so the second gets a brand-new DuckDB
        file. If the delta token in dlt state were not sufficient, the second run
        would load everything again instead of nothing.
        """
        script = textwrap.dedent(
            f"""
            import os, sys, dlt
            from erpl_dlt import ODataCredentials, erpl_odp_odata_source
            credentials = ODataCredentials(
                base_url={os.environ.get("ERPL_ODATA_BASE_URL", "http://localhost:50000")!r},
                username={os.environ.get("ERPL_SAP_USER", "DEVELOPER")!r},
                password=os.environ["ERPL_SAP_PASSWORD"],
            )
            pipeline = dlt.pipeline(
                pipeline_name="it_odp_odata", destination="duckdb", dataset_name="odp",
                pipelines_dir={str(tmp_path / "dlt")!r},
            )
            info = pipeline.run(erpl_odp_odata_source(
                entity_set_urls=[{ODATA_URL!r}], credentials=credentials))
            table = {ODATA_URL.rstrip("/").rsplit("/", 1)[-1].lower()!r}
            with pipeline.sql_client() as client:
                try:
                    print("ROWS", client.execute_sql(f"SELECT count(*) FROM {{table}}")[0][0])
                except Exception:
                    print("ROWS 0")
            """
        )
        first = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=900)
        assert "ROWS" in first.stdout, first.stderr[-2000:]
        first_rows = int(first.stdout.strip().split("ROWS")[-1])
        assert first_rows > 0, "the first run must load the snapshot"

        second = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=900)
        assert "ROWS" in second.stdout, second.stderr[-2000:]
        second_rows = int(second.stdout.strip().split("ROWS")[-1])
        # Unchanged data: a delta must add nothing.
        assert second_rows == first_rows, f"the second run changed the row count: {first_rows} -> {second_rows}"
