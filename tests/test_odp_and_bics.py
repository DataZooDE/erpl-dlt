"""The two sources whose state and shape are not ordinary table reads."""

import pytest

from erpl_dlt.bics import session_statements
from erpl_dlt.odp import (
    odp_recovery_parameter,
    odp_rfc_query,
    resolve_subscriber_process,
    seed_delta_token_statements,
    subscriber_process,
)
from erpl_dlt.query import ConfigurationError, QueryError


class TestSubscriberName:
    def test_it_is_stable_for_a_provider(self):
        # SAP identifies the delta pointer by this name. If it changed between
        # runs, every run would silently be a full load.
        first = subscriber_process("ABAP_CDS", "SEPM_ISOI$P", pipeline_name="sap")
        second = subscriber_process("ABAP_CDS", "SEPM_ISOI$P", pipeline_name="sap")
        assert first == second

    def test_it_survives_sap_punctuation(self):
        assert "$" not in subscriber_process("ABAP_CDS", "SEPM_ISOI$P", pipeline_name="sap")

    def test_it_fits_sap_s_field(self):
        assert len(subscriber_process("ABAP_CDS", "A" * 60, pipeline_name="sap")) <= 32

    def test_the_pipeline_is_part_of_the_identity(self):
        first = subscriber_process("ABAP_CDS", "SEPM_ISOI$P", pipeline_name="sales")
        second = subscriber_process("ABAP_CDS", "SEPM_ISOI$P", pipeline_name="finance")
        assert first != second

    def test_long_names_keep_a_distinguishing_suffix(self):
        first = subscriber_process("ABAP_CDS", "A" * 60, pipeline_name="pipeline_" + "X" * 60)
        second = subscriber_process("ABAP_CDS", "A" * 60, pipeline_name="pipeline_" + "Y" * 60)
        assert first != second
        assert len(first) <= 32
        assert len(second) <= 32

    def test_a_hostile_context_is_refused(self):
        with pytest.raises(QueryError):
            subscriber_process("ABAP_CDS'; DROP", "X")

    def test_state_subscriber_is_authoritative(self):
        state = {"subscriber_process": "DLT_OLD", "initialized": True}
        with pytest.raises(ConfigurationError, match="different SAP delta queue"):
            resolve_subscriber_process(
                context="ABAP_CDS",
                name="SEPM_ISOI$P",
                pipeline_name="sap",
                explicit=None,
                state=state,
                resource="SEPM_ISOI$P",
            )

    def test_an_explicit_subscriber_can_match_existing_state(self):
        state = {"subscriber_process": "DLT_SHARED", "initialized": True}
        assert (
            resolve_subscriber_process(
                context="ABAP_CDS",
                name="SEPM_ISOI$P",
                pipeline_name="sap",
                explicit="DLT_SHARED",
                state=state,
                resource="SEPM_ISOI$P",
            )
            == "DLT_SHARED"
        )


class TestOdpQueries:
    def test_a_full_read_takes_no_subscriber(self):
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=False, columns=None)
        assert "sap_odp_read_full" in sql and "SUB" not in sql

    def test_a_delta_read_carries_the_subscriber(self):
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=True, columns=None)
        assert "sap_odp_read_delta" in sql and "'SUB'" in sql

    def test_columns_are_pushed(self):
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=True, columns=["A", "B"])
        assert "columns := ['A', 'B']" in sql


class TestDeltaTokenSeeding:
    def test_it_is_delete_then_insert(self):
        # odp_subscriptions has a primary key AND a unique constraint, so DuckDB
        # refuses INSERT OR REPLACE: two conflict targets.
        statements = seed_delta_token_statements("https://gw/E", "E", "TOKEN")
        assert [s.split()[0] for s, _ in statements] == ["DELETE", "INSERT"]

    def test_values_are_bound(self):
        for sql, parameters in seed_delta_token_statements("https://gw/E", "E", "TOK'EN"):
            assert "TOK'EN" not in sql
            assert "TOK'EN" in parameters or "E" in parameters

    def test_every_not_null_column_is_supplied(self):
        # erpl_web's table requires subscription_id and the status columns; an
        # INSERT without them fails only on the *second* run, when a token
        # exists to seed.
        insert = seed_delta_token_statements("https://gw/E", "E", "T")[1][0]
        for column in ("subscription_id", "subscription_status", "preference_applied", "schema_version"):
            assert column in insert


class TestBicsSession:
    def test_the_last_statement_is_the_only_one_that_returns_rows(self):
        statements = session_statements("sid", "0D_NW_C01", query="Q", rows=["0CALMONTH"])
        assert statements[-1].startswith("SELECT * FROM sap_bics_result")
        assert statements[0].startswith("SELECT * FROM sap_bics_begin")

    def test_rows_and_columns_are_set_before_the_result(self):
        statements = session_statements("sid", "C", rows=["R"], columns=["K"])
        order = [s.split("FROM ")[1].split("(")[0] for s in statements]
        assert order == ["sap_bics_begin", "sap_bics_rows", "sap_bics_columns", "sap_bics_result"]

    def test_the_session_id_ties_the_statements_together(self):
        for statement in session_statements("sid42", "C", rows=["R"]):
            assert "'sid42'" in statement

    def test_variables_render_as_bex_ranges(self):
        statements = session_statements("s", "C", variables=[{"name": "V", "low": "202601"}])
        assert "'NAME': 'V'" in statements[0] and "'OP': 'EQ'" in statements[0]

    def test_a_high_value_makes_it_a_between(self):
        statements = session_statements("s", "C", variables=[{"name": "V", "low": "a", "high": "b"}])
        assert "'OP': 'BT'" in statements[0]

    def test_a_slice_filter_is_applied(self):
        statements = session_statements("s", "C", filters=[{"characteristic": "0CALMONTH", "members": ["202601"]}])
        assert any("sap_bics_filter" in s and "'202601'" in s for s in statements)

    def test_quotes_in_a_member_cannot_escape(self):
        statements = session_statements("s", "C", filters=[{"characteristic": "X", "members": ["a'b"]}])
        assert "'a''b'" in " ".join(statements)


class TestRecoveryMode:
    """ERPL's recover parameter, and why it is not discovered by inspection."""

    def test_a_recovery_read_asks_for_it(self):
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=True, columns=None, recovery_parameter="recover")
        assert "recover := true" in sql

    def test_it_is_a_boolean_not_a_mode_string(self):
        # ERPL documents `recover (BOOLEAN, default false)`. An extraction-mode
        # string would be rejected by the binder.
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=True, columns=None, recovery_parameter="recover")
        assert "'R'" not in sql

    def test_a_full_read_never_recovers(self):
        sql = odp_rfc_query("ABAP_CDS", "P", "SUB", delta=False, columns=None, recovery_parameter="recover")
        assert "recover" not in sql

    def test_the_parameter_name_is_not_probed_from_duckdb_functions(self):
        # duckdb_functions() reports named parameters positionally (col3, col4,
        # ...), so a probe for the name always comes back empty and would
        # silently disable recovery for everyone.
        from erpl_dlt.odp import ODP_RECOVERY_PARAMETER

        assert ODP_RECOVERY_PARAMETER == "recover"
        assert odp_recovery_parameter(cursor=None) == "recover"


class TestDeltaCursorIsReleased:
    """ERPL: "delta cursors do not auto-close"."""

    class _Cursor:
        def __init__(self, answer="CLOSED", explode=False):
            self.statements: list[str] = []
            self._answer = answer
            self._explode = explode

        def execute(self, statement, *args):
            self.statements.append(statement)
            if self._explode:
                raise RuntimeError("connection gone")
            return self

        def fetchone(self):
            return (self._answer,)

    def test_it_closes_with_the_subscriber_and_provider(self):
        from erpl_dlt.odp import close_delta_cursor

        cursor = self._Cursor()
        close_delta_cursor(cursor, "ABAP_CDS", "SEPM_ISOI$P", "DLT_X")
        assert "sap_odp_close_delta_cursor" in cursor.statements[0]
        assert "'ABAP_CDS'" in cursor.statements[0]
        assert "'DLT_X'" in cursor.statements[0]
        assert "'SEPM_ISOI$P'" in cursor.statements[0]

    def test_a_refusal_is_reported_not_raised(self, caplog):
        from erpl_dlt.odp import close_delta_cursor

        close_delta_cursor(self._Cursor(answer="REFUSED: mid-fetch"), "ABAP_CDS", "P", "SUB")
        assert "could not be closed" in caplog.text

    def test_a_broken_connection_does_not_fail_the_extract(self):
        from erpl_dlt.odp import close_delta_cursor

        close_delta_cursor(self._Cursor(explode=True), "ABAP_CDS", "P", "SUB")

    def test_a_hostile_context_cannot_escape_the_pragma(self):
        from erpl_dlt.odp import close_delta_cursor
        from erpl_dlt.query import QueryError

        with pytest.raises(QueryError):
            close_delta_cursor(self._Cursor(), "ABAP'; DROP", "P", "SUB")
