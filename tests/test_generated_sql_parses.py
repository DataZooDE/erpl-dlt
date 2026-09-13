"""Every generated statement must parse.

Asserting substrings proves we built a string; it does not prove we built a
statement. This file is the reason that distinction matters: the BEx variables
block rendered `{'V' AS NAME, ...}`, which is not DuckDB struct syntax, and
every substring assertion about it passed.

`EXPLAIN` parses and binds without executing. Binder errors are expected here --
the ERPL table functions are not loaded in a bare DuckDB -- so only parser
errors are failures.
"""

import duckdb
import pytest

from erpl_dlt.bics import session_statements
from erpl_dlt.invoke import invoke_query
from erpl_dlt.odp import odp_rfc_query, seed_delta_token_statements
from erpl_dlt.query import (
    incremental_predicate,
    odata_incremental_predicate,
    odata_read_query,
    table_read_query,
)


def assert_parses(sql: str, parameters: list | None = None) -> None:
    try:
        duckdb.connect().execute(f"EXPLAIN {sql}", parameters or [])
    except Exception as exc:
        message = str(exc)
        if "Parser Error" in message or "syntax error" in message:
            raise AssertionError(f"{message.splitlines()[0]}\n  in: {sql}") from exc


class TestTableQueries:
    @pytest.mark.parametrize(
        "sql",
        [
            table_read_query("SFLIGHT"),
            table_read_query("SFLIGHT", columns=["CARRID", "FLDATE"]),
            table_read_query("SFLIGHT", abap_filter="CARRID = 'LH'"),
            table_read_query("SFLIGHT", max_rows=10, threads=4),
            table_read_query("SFLIGHT", where=incremental_predicate("FLDATE", "2026-09-05")),
        ],
    )
    def test_it_parses(self, sql):
        assert_parses(sql)


class TestODataQueries:
    def test_a_plain_read_parses(self):
        sql, parameters = odata_read_query("https://gw/svc/E")
        assert_parses(sql, parameters)

    def test_options_parse(self):
        sql, parameters = odata_read_query("https://gw/svc/E", top=10, skip=5, expand="Items", max_page_size=500)
        assert_parses(sql, parameters)

    def test_an_incremental_filter_parses(self):
        where = odata_incremental_predicate("SalesOrderItem", "0000000010")
        sql, parameters = odata_read_query("https://gw/svc/E", where=where)
        assert_parses(sql, parameters)

    def test_the_property_keeps_its_case(self):
        # OData compares property names case-sensitively; SALESORDERITEM is a
        # different field from SalesOrderItem, and the service would reject it.
        assert odata_incremental_predicate("SalesOrderItem", "x").startswith("SalesOrderItem >=")


class TestOdpQueries:
    @pytest.mark.parametrize("delta", [True, False])
    def test_provider_reads_parse(self, delta):
        assert_parses(odp_rfc_query("ABAP_CDS", "P", "SUB", delta=delta, columns=["A", "B"]))

    def test_the_token_seed_parses(self):
        for sql, parameters in seed_delta_token_statements("https://gw/E", "E", "TOKEN"):
            assert_parses(sql, parameters)


class TestBicsSessions:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"query": "Q"},
            {"rows": ["0CALMONTH"], "columns": ["0AMOUNT"]},
            {"variables": [{"name": "0CALMONTH", "low": "202601"}]},
            {"variables": [{"name": "V", "low": "a", "high": "b"}]},
            {"filters": [{"characteristic": "0COMP_CODE", "members": ["1000", "2000"]}]},
            {"variant": "MY_VARIANT"},
        ],
    )
    def test_every_statement_parses(self, kwargs):
        for statement in session_statements("sid", "0D_NW_C01", **kwargs):
            assert_parses(statement)


class TestInvokeQueries:
    @pytest.mark.parametrize(
        "parameters",
        [
            None,
            {"REQUTEXT": "hello"},
            {"OPTS": {"LOW": "A"}},
            {"ROWS": ["x", "y"]},
            {"N": 5, "FLAG": True, "MISSING": None},
            {"NESTED": [{"A": 1}, {"A": 2}]},
        ],
    )
    def test_it_parses(self, parameters):
        assert_parses(invoke_query("BAPI_FLIGHT_GETLIST", parameters))

    def test_a_none_parameter_is_null_not_the_word(self):
        assert "NULL" in invoke_query("Z_FN", {"X": None})
        assert "'None'" not in invoke_query("Z_FN", {"X": None})
