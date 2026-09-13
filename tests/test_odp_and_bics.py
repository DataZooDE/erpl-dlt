"""The two sources whose state and shape are not ordinary table reads."""

import pytest

from erpl_dlt.bics import session_statements
from erpl_dlt.odp import odp_rfc_query, seed_delta_token_statements, subscriber_process
from erpl_dlt.query import QueryError


class TestSubscriberName:
    def test_it_is_stable_for_a_provider(self):
        # SAP identifies the delta pointer by this name. If it changed between
        # runs, every run would silently be a full load.
        first = subscriber_process("ABAP_CDS", "SEPM_ISOI$P")
        second = subscriber_process("ABAP_CDS", "SEPM_ISOI$P")
        assert first == second

    def test_it_survives_sap_punctuation(self):
        assert "$" not in subscriber_process("ABAP_CDS", "SEPM_ISOI$P")

    def test_it_fits_sap_s_field(self):
        assert len(subscriber_process("ABAP_CDS", "A" * 60)) <= 32

    def test_a_hostile_context_is_refused(self):
        with pytest.raises(QueryError):
            subscriber_process("ABAP_CDS'; DROP", "X")


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
        assert "'V' AS NAME" in statements[0] and "'EQ' AS OP" in statements[0]

    def test_a_high_value_makes_it_a_between(self):
        statements = session_statements("s", "C", variables=[{"name": "V", "low": "a", "high": "b"}])
        assert "'BT' AS OP" in statements[0]

    def test_a_slice_filter_is_applied(self):
        statements = session_statements("s", "C", filters=[{"characteristic": "0CALMONTH", "members": ["202601"]}])
        assert any("sap_bics_filter" in s and "'202601'" in s for s in statements)

    def test_quotes_in_a_member_cannot_escape(self):
        statements = session_statements("s", "C", filters=[{"characteristic": "X", "members": ["a'b"]}])
        assert "'a''b'" in " ".join(statements)
