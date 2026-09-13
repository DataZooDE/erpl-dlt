"""BW queries against a real system.

BICS is a session, not a table read, so the property under test is that the
preamble and the result run on the same cursor in the right order -- and that
two sessions can exist without colliding.
"""

import os

import dlt
import duckdb
import pytest

from erpl_dlt import erpl_bics_source
from erpl_dlt.bics import session_statements
from erpl_dlt.connection import ErplConnection
from erpl_dlt.settings import BICS_EXTENSIONS

pytestmark = pytest.mark.integration

CUBE = os.environ.get("ERPL_SAP_BICS_CUBE", "")


@pytest.fixture(autouse=True)
def _needs_bw():
    if not CUBE:
        pytest.skip("set ERPL_SAP_BICS_CUBE to a BW InfoProvider (0D_NW_C01 ships with the trial)")


class TestBicsSession:
    def test_a_provider_returns_rows(self, sap_credentials, settings, pipelines_dir):
        pipeline = dlt.pipeline(
            pipeline_name="it_bics", destination="duckdb", dataset_name="bw", pipelines_dir=pipelines_dir
        )
        pipeline.run(erpl_bics_source(cube=CUBE, credentials=sap_credentials, settings=settings))
        # Ask the destination what it called the table rather than guessing:
        # dlt renames anything starting with a digit, and BW names often do.
        with pipeline.sql_client() as client:
            tables = [
                row[0]
                for row in client.execute_sql(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bw' "
                    "AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'"
                )
            ]
            assert tables, "the BICS session produced no table at all"
            count = client.execute_sql(f'SELECT count(*) FROM "{tables[0]}"')[0][0]
        assert count > 0

    def test_the_preamble_must_precede_the_result(self, sap_credentials, settings):
        """Running sap_bics_result without its session is an error, not an empty
        result -- which is what makes the ordering a real requirement rather
        than a stylistic one."""
        with ErplConnection(extensions=BICS_EXTENSIONS, settings=settings, rfc_credentials=sap_credentials) as erpl:
            statements = session_statements("it_order_probe", CUBE)
            with pytest.raises(duckdb.Error):
                erpl.cursor().execute(statements[-1]).fetchall()

    def test_two_sessions_do_not_collide(self, sap_credentials, settings):
        # Each execution gets a fresh session id, so two resources running at
        # once cannot overwrite one another's state on the BW side.
        results: dict[str, int] = {}
        with ErplConnection(extensions=BICS_EXTENSIONS, settings=settings, rfc_credentials=sap_credentials) as erpl:
            for session_id in ("dlt_probe_a", "dlt_probe_b"):
                cursor = erpl.cursor()
                statements = session_statements(session_id, CUBE)
                for statement in statements[:-1]:
                    cursor.execute(statement).fetchall()
                table = cursor.execute(statements[-1]).fetch_arrow_table()
                results[session_id] = table.num_rows
        # `>= 0` was vacuous: every integer satisfies it. Two independent
        # sessions over the same provider must return the same row count, and a
        # non-zero one, or they are not really independent.
        assert len(results) == 2
        assert all(count > 0 for count in results.values()), results
        assert len(set(results.values())) == 1, f"the two sessions disagreed: {results}"
