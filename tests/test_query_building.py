"""What reaches SAP, and what must never reach it."""

import datetime

import pytest

from erpl_dlt.query import (
    MAX_CURSOR_VALUE,
    QueryError,
    abap_literal,
    abap_predicate,
    check_identifier,
    incremental_predicate,
    odata_read_query,
    sql_value_literal,
    table_read_query,
)


class TestIdentifiers:
    """A table or column name cannot be parameterised, so it is validated."""

    @pytest.mark.parametrize("name", ["SFLIGHT", "/DMO/FLIGHT", "MARA", "ZZ_TABLE_1"])
    def test_real_names_pass(self, name):
        assert check_identifier(name) == name.upper()

    @pytest.mark.parametrize(
        "hostile",
        ["SFLIGHT'; DROP TABLE x", "SFLIGHT OR 1=1", "SFLIGHT--", "", "A" * 31, "table name", "tab;le"],
    )
    def test_hostile_names_are_refused(self, hostile):
        with pytest.raises(QueryError):
            check_identifier(hostile)

    def test_the_checked_spelling_is_what_gets_used(self):
        # Validating one string and sending another is how a check gets bypassed.
        assert "'SFLIGHT'" in table_read_query(" sflight ")


class TestTableQuery:
    def test_a_plain_read(self):
        assert table_read_query("SFLIGHT") == "SELECT * FROM sap_read_table('SFLIGHT')"

    def test_columns_are_pushed_and_projected(self):
        sql = table_read_query("SFLIGHT", columns=["CARRID", "FLDATE"])
        # COLUMNS tells SAP; the projection tells DuckDB. Both, or the scan reads
        # more than it emits.
        assert "COLUMNS := ['CARRID', 'FLDATE']" in sql
        assert sql.startswith("SELECT CARRID, FLDATE FROM")

    def test_an_abap_filter_is_quoted(self):
        sql = table_read_query("SFLIGHT", abap_filter="CARRID = 'LH'")
        assert "FILTER := 'CARRID = ''LH'''" in sql

    def test_max_rows_and_threads_are_integers(self):
        sql = table_read_query("SFLIGHT", max_rows=10, threads=4)
        assert "MAX_ROWS := 10" in sql and "THREADS := 4" in sql


class TestIncrementalPushdown:
    """The filter must be in the query, not applied after the rows arrive."""

    def test_a_last_value_becomes_a_where_clause(self):
        sql = table_read_query("SFLIGHT", where=incremental_predicate("FLDATE", datetime.date(2026, 9, 5)))
        # SQL, not ABAP: the column is a DuckDB DATE, and '20260905' fails there
        # with "invalid date field format". The compact form is for FILTER :=.
        assert sql.endswith("WHERE FLDATE >= DATE '2026-09-05'")

    def test_the_two_literal_forms_do_not_get_confused(self):
        day = datetime.date(2026, 9, 5)
        assert sql_value_literal(day) == "DATE '2026-09-05'"
        assert abap_literal(day) == "'20260905'"
        assert abap_predicate("FLDATE", day) == "FLDATE >= '20260905'"

    def test_the_first_run_has_no_filter(self):
        assert "WHERE" not in table_read_query("SFLIGHT")

    def test_dates_go_back_to_sap_form_for_abap(self):
        # SAP rejects '2026-09-05' for a D(8,0) column.
        assert abap_literal(datetime.date(2026, 9, 5)) == "'20260905'"
        assert abap_literal(datetime.time(10, 30)) == "'103000'"
        assert abap_literal(datetime.datetime(2026, 9, 5, 10, 30)) == "'20260905103000'"

    def test_the_boundary_is_inclusive(self):
        # SAP dates have one-day resolution: '>' drops every row sharing the
        # boundary. dlt deduplicates; nothing recovers a dropped row.
        assert ">=" in incremental_predicate("FLDATE", datetime.date(2026, 1, 1))

    def test_a_hostile_cursor_value_cannot_escape_the_literal(self):
        assert incremental_predicate("ERDAT", "x' OR '1'='1") == "ERDAT >= 'x'' OR ''1''=''1'"
        assert abap_predicate("ERDAT", "x' OR '1'='1") == "ERDAT >= 'x'' OR ''1''=''1'"

    def test_an_absurd_cursor_value_is_refused(self):
        # State is replayed from storage and is not necessarily ours.
        with pytest.raises(QueryError):
            incremental_predicate("ERDAT", "A" * (MAX_CURSOR_VALUE + 1))


class TestODataQuery:
    def test_the_url_is_bound_not_interpolated(self):
        sql, parameters = odata_read_query("https://gw/svc/Entity")
        assert "?" in sql and parameters == ["https://gw/svc/Entity"]
        assert "https://gw" not in sql

    def test_options_are_named_parameters(self):
        sql, _ = odata_read_query("https://gw/svc/E", top=10, expand="Items", max_page_size=500)
        assert "top := 10" in sql and "expand := 'Items'" in sql and "max_page_size := 500" in sql
