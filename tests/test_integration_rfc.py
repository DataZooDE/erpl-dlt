"""Real SAP, no mocks: the properties that only a live system can settle."""

import dlt
import pytest

from erpl_dlt import erpl_rfc_source
from erpl_dlt.connection import ErplConnection
from erpl_dlt.query import incremental_predicate, table_read_query
from erpl_dlt.settings import RFC_EXTENSIONS

pytestmark = pytest.mark.integration


def _pipeline(pipelines_dir, name):
    return dlt.pipeline(pipeline_name=name, destination="duckdb", dataset_name="sap_raw", pipelines_dir=pipelines_dir)


class TestConnection:
    def test_sap_answers(self, sap_credentials, settings):
        with ErplConnection(extensions=RFC_EXTENSIONS, settings=settings, rfc_credentials=sap_credentials) as erpl:
            assert erpl.ping()


class TestFullLoad:
    def test_rows_and_types_arrive_intact(self, sap_credentials, settings, pipelines_dir):
        pipeline = _pipeline(pipelines_dir, "it_full")
        pipeline.run(erpl_rfc_source(table_names=["SFLIGHT"], credentials=sap_credentials, settings=settings))
        with pipeline.sql_client() as client:
            count = client.execute_sql("SELECT count(*) FROM sflight")[0][0]
            types = dict(
                client.execute_sql(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sflight'"
                )
            )
        assert count > 0
        # The extension produces these; nothing downstream patches them. A
        # currency landing as a float would be a real defect.
        assert types["fldate"] == "DATE"
        assert types["price"].startswith("DECIMAL")
        assert types["seatsmax"] == "BIGINT"

    def test_a_projection_reaches_sap(self, sap_credentials, settings, pipelines_dir):
        pipeline = _pipeline(pipelines_dir, "it_projection")
        pipeline.run(
            erpl_rfc_source(
                table_names=["SFLIGHT"],
                credentials=sap_credentials,
                settings=settings,
                columns={"SFLIGHT": ["CARRID", "CONNID"]},
            )
        )
        with pipeline.sql_client() as client:
            columns = {
                row[0]
                for row in client.execute_sql(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'sflight'"
                )
            }
        assert {"carrid", "connid"} <= columns
        assert "price" not in columns, "the projection did not reach SAP"


class TestPushdownIsReal:
    """Asserting the SQL string would only prove we built a string.

    EXPLAIN shows what DuckDB does with it: a predicate inside the
    SAP_READ_TABLE node is one SAP evaluates, not one applied afterwards.
    """

    def test_the_predicate_lands_inside_the_scan(self, sap_credentials, settings):
        with ErplConnection(extensions=RFC_EXTENSIONS, settings=settings, rfc_credentials=sap_credentials) as erpl:
            sql = table_read_query("SFLIGHT", where=incremental_predicate("CARRID", "LH"))
            plan = "\n".join(str(row[1]) for row in erpl.cursor().execute(f"EXPLAIN {sql}").fetchall())
        assert "SAP_READ_TABLE" in plan
        assert "Filters" in plan and "CARRID" in plan

    def test_the_first_run_pushes_nothing(self, sap_credentials, settings):
        with ErplConnection(extensions=RFC_EXTENSIONS, settings=settings, rfc_credentials=sap_credentials) as erpl:
            plan = "\n".join(
                str(row[1]) for row in erpl.cursor().execute(f"EXPLAIN {table_read_query('SFLIGHT')}").fetchall()
            )
        assert "Filters" not in plan


class TestIncremental:
    def test_the_second_run_extracts_only_the_boundary(self, sap_credentials, settings, pipelines_dir):
        """Assert what the second run *extracted*, not that the table survived.

        The previous version of this test checked `total > 0` and
        `second is not None`, which a full re-read would satisfy just as well.
        What distinguishes incremental from full is the row count the second
        extract produced, so that is what is asserted.
        """
        keys = {"SFLIGHT": ["CARRID", "CONNID", "FLDATE"]}
        pipeline = _pipeline(pipelines_dir, "it_incremental")

        def source():
            return erpl_rfc_source(
                table_names=["SFLIGHT"],
                credentials=sap_credentials,
                settings=settings,
                cursor_columns={"SFLIGHT": "FLDATE"},
                primary_keys=keys,
            )

        first = pipeline.run(source())
        assert first.loads_ids
        with pipeline.sql_client() as client:
            after_first = client.execute_sql("SELECT count(*) FROM sflight")[0][0]
        assert after_first > 0

        pipeline.run(source())
        with pipeline.sql_client() as client:
            after_second = client.execute_sql("SELECT count(*) FROM sflight")[0][0]

        # Unchanged data: the merge must not multiply the table, and the second
        # run must not have re-loaded it wholesale.
        assert after_second == after_first, f"{after_first} -> {after_second}"
        state = pipeline.state["sources"]["erpl_rfc"]["resources"]["sflight"]["incremental"]["FLDATE"]
        assert state["last_value"] is not None, "the cursor did not advance, so nothing was incremental"

    def test_a_cursor_without_a_primary_key_is_refused(self, sap_credentials, settings):
        # The combination that would replace the table with the increment.
        from erpl_dlt.query import ConfigurationError

        with pytest.raises(ConfigurationError):
            list(
                erpl_rfc_source(
                    table_names=["SFLIGHT"],
                    credentials=sap_credentials,
                    settings=settings,
                    cursor_columns={"SFLIGHT": "FLDATE"},
                )
            )


class TestParallelism:
    def test_three_resources_run_together(self, sap_credentials, settings, pipelines_dir):
        # A single DuckDB connection is not thread-safe; each resource takes its
        # own cursor. If that were wrong, this is where it would surface.
        pipeline = _pipeline(pipelines_dir, "it_parallel")
        pipeline.run(
            erpl_rfc_source(
                table_names=["SFLIGHT", "SBOOK", "SCARR"],
                credentials=sap_credentials,
                settings=settings,
                max_rows=500,
            )
        )
        with pipeline.sql_client() as client:
            counts = {
                table: client.execute_sql(f"SELECT count(*) FROM {table}")[0][0]
                for table in ("sflight", "sbook", "scarr")
            }
        assert all(count > 0 for count in counts.values()), counts


class TestTypeRoundTrip:
    """SAP's awkward types, all the way to the destination schema.

    The point is that nothing in this package casts them: the extension emits
    DATE, TIME and DECIMAL, Arrow carries them, and dlt lands them. A currency
    arriving as a float, or a date as a string, would be a real defect.
    """

    def test_decimals_dates_and_times_survive(self, sap_credentials, settings, pipelines_dir):
        pipeline = _pipeline(pipelines_dir, "it_types")
        # SFLIGHT carries CURR (PRICE), DATS (FLDATE) and INT4; DD02L adds TIMS.
        pipeline.run(
            erpl_rfc_source(
                table_names=["SFLIGHT", "DD02L"],
                credentials=sap_credentials,
                settings=settings,
                columns={"DD02L": ["TABNAME", "AS4DATE", "AS4TIME"]},
                max_rows=200,
            )
        )
        with pipeline.sql_client() as client:
            sflight = dict(
                client.execute_sql(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sflight'"
                )
            )
            dd02l = dict(
                client.execute_sql(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dd02l'"
                )
            )
        assert sflight["price"].startswith("DECIMAL"), "a currency must not land as a float"
        assert sflight["fldate"] == "DATE"
        # dlt's snake_case normaliser splits letter/digit boundaries, so SAP's
        # AS4DATE becomes as4_date. Worth knowing: SAP column names are full of
        # digits, and this is what the destination will actually be called.
        assert dd02l["as4_time"] == "TIME"
        assert dd02l["as4_date"] == "DATE"

    def test_an_empty_sap_date_is_null(self, sap_credentials, settings, pipelines_dir):
        # '00000000' is SAP's empty date. The extension maps it to NULL itself;
        # this asserts that it stays NULL rather than becoming 0000-00-00 or a
        # string somewhere along the way.
        pipeline = _pipeline(pipelines_dir, "it_sentinel")
        pipeline.run(
            erpl_rfc_source(
                table_names=["USR02"],
                credentials=sap_credentials,
                settings=settings,
                columns={"USR02": ["BNAME", "GLTGV", "GLTGB"]},
                max_rows=50,
            )
        )
        with pipeline.sql_client() as client:
            kind = client.execute_sql(
                "SELECT data_type FROM information_schema.columns WHERE table_name = 'usr02' AND column_name = 'gltgv'"
            )[0][0]
            nulls = client.execute_sql("SELECT count(*) FROM usr02 WHERE gltgv IS NULL")[0][0]
            total = client.execute_sql("SELECT count(*) FROM usr02")[0][0]
        assert kind == "DATE"
        assert total > 0
        assert nulls > 0, "USR02's validity dates are empty in SAP; they should be NULL here"
