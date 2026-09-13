"""Function modules against a real system.

The property that matters is not that a call returns rows -- it is that a call
which *fails inside SAP* does not look like a call that returned nothing.
"""

import dlt
import pytest

from erpl_dlt import erpl_invoke_source

pytestmark = pytest.mark.integration


class TestScalarExports:
    def test_a_module_without_a_path_yields_one_record(self, sap_credentials, settings, pipelines_dir):
        # STFC_CONNECTION echoes its input and ships with every system.
        pipeline = dlt.pipeline(
            pipeline_name="it_invoke", destination="duckdb", dataset_name="fm", pipelines_dir=pipelines_dir
        )
        pipeline.run(
            erpl_invoke_source(
                functions=[
                    {
                        "name": "echo",
                        "function": "STFC_CONNECTION",
                        "parameters": {"REQUTEXT": "hello from dlt"},
                    }
                ],
                credentials=sap_credentials,
                settings=settings,
            )
        )
        with pipeline.sql_client() as client:
            rows = client.execute_sql("SELECT count(*) FROM echo")[0][0]
            echo = client.execute_sql("SELECT echotext FROM echo LIMIT 1")[0][0]
        assert rows == 1
        assert "hello from dlt" in str(echo)


class TestFailuresAreNotSilence:
    def test_a_bad_parameter_fails_the_resource(self, sap_credentials, settings, pipelines_dir):
        pipeline = dlt.pipeline(
            pipeline_name="it_invoke_bad", destination="duckdb", dataset_name="fm", pipelines_dir=pipelines_dir
        )
        with pytest.raises(Exception) as caught:
            pipeline.run(
                erpl_invoke_source(
                    functions=[{"name": "bad", "function": "STFC_CONNECTION", "parameters": {"NOPE": "x"}}],
                    credentials=sap_credentials,
                    settings=settings,
                )
            )
        # Our message or SAP's -- but not an empty, successful-looking load.
        assert "NOPE" in str(caught.value) or "parameter" in str(caught.value).lower()

    def test_a_missing_result_parameter_is_refused(self, sap_credentials, settings, pipelines_dir):
        pipeline = dlt.pipeline(
            pipeline_name="it_invoke_path", destination="duckdb", dataset_name="fm", pipelines_dir=pipelines_dir
        )
        with pytest.raises(Exception) as caught:
            pipeline.run(
                erpl_invoke_source(
                    functions=[
                        {
                            "name": "nopath",
                            "function": "STFC_CONNECTION",
                            "path": "/NO_SUCH_TABLE",
                            "parameters": {"REQUTEXT": "x"},
                        }
                    ],
                    credentials=sap_credentials,
                    settings=settings,
                )
            )
        assert "NO_SUCH_TABLE" in str(caught.value)
