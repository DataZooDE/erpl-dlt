"""A BAPI reports failure in data. Unread, a failed call looks like no data."""

import pytest

from erpl_dlt.invoke import (
    describe_failure,
    find_return_field,
    invoke_query,
    is_failure,
)
from erpl_dlt.query import QueryError


class TestFailureDetection:
    def test_an_error_type_is_a_failure(self):
        assert is_failure([{"TYPE": "E", "MESSAGE": "Booking is locked"}])

    def test_an_abort_is_a_failure(self):
        assert is_failure([{"TYPE": "A", "MESSAGE": "gone"}])

    @pytest.mark.parametrize("kind", ["S", "I", "W", "", None])
    def test_other_types_are_not(self, kind):
        assert not is_failure([{"TYPE": kind, "MESSAGE": "fine"}])

    def test_a_lowercase_return_table_is_read_the_same_way(self):
        # Whether the driver uppercases its column names is not the BAPI's
        # problem, and must not decide whether we notice an error.
        assert is_failure([{"type": "E", "message": "broke"}])

    def test_nothing_at_all_is_not_a_failure(self):
        assert not is_failure(None) and not is_failure([])


class TestFailureDescription:
    def test_the_sap_message_survives(self):
        assert "Booking is locked" in describe_failure([{"TYPE": "E", "MESSAGE": "Booking is locked"}])

    def test_the_message_identity_survives(self):
        assert "BC/007" in describe_failure([{"TYPE": "E", "MESSAGE": "x", "ID": "BC", "NUMBER": "007"}])

    def test_lowercase_is_described_too(self):
        # The reader that classifies and the reader that describes must agree,
        # or a detected failure reports itself as nothing at all.
        table = [{"type": "E", "message": "broke"}]
        assert is_failure(table) and describe_failure(table)

    def test_successes_are_not_described(self):
        assert describe_failure([{"TYPE": "S", "MESSAGE": "fine"}]) == ""


class TestReturnFieldDiscovery:
    @pytest.mark.parametrize("name", ["RETURN", "ET_RETURN", "T_RETURN", "EXPORT_RETURN"])
    def test_conventional_names_are_found(self, name):
        assert find_return_field(["FLIGHT_LIST", name]) == name

    def test_an_override_wins(self):
        assert find_return_field(["MESSAGES"], override="MESSAGES") == "MESSAGES"

    def test_a_module_without_one_is_fine(self):
        assert find_return_field(["ECHOTEXT", "RESPTEXT"]) is None


class TestInvokeQuery:
    def test_no_path_is_passed(self):
        # One call has to return both the payload and RETURN, or a failure
        # cannot be seen before rows are emitted.
        sql = invoke_query("BAPI_FLIGHT_GETLIST", {"AIRLINE": "LH"})
        assert "path" not in sql
        assert sql.startswith("SELECT * FROM sap_rfc_invoke('BAPI_FLIGHT_GETLIST'")

    def test_parameters_are_escaped(self):
        sql = invoke_query("STFC_CONNECTION", {"REQUTEXT": "it's fine"})
        assert "'it''s fine'" in sql

    def test_nested_structures_and_tables_render(self):
        sql = invoke_query("Z_FN", {"OPTS": {"LOW": "A"}, "ROWS": ["x", "y"], "N": 5, "FLAG": True})
        assert "{'LOW': 'A'}" in sql and "['x', 'y']" in sql and "5" in sql and "true" in sql

    @pytest.mark.parametrize("hostile", ["BAPI'; DROP", "fn name", "A" * 31])
    def test_a_hostile_function_name_is_refused(self, hostile):
        with pytest.raises(QueryError):
            invoke_query(hostile, None)

    def test_a_hostile_parameter_name_is_refused(self):
        with pytest.raises(QueryError):
            invoke_query("STFC_CONNECTION", {"X'; DROP": "1"})
