"""Query text reaches logs. Credentials must not be in it."""

from unittest.mock import MagicMock

from erpl_dlt.config import ODataCredentials, SapRfcCredentials
from erpl_dlt.connection import ErplConnection

PASSWORD = "sup3r-s3cret-pa55word"


def _recording_connection():
    connection = MagicMock()
    connection.cursor.return_value = MagicMock()
    return connection


class TestRfcSecret:
    def test_the_password_is_bound_not_formatted(self):
        connection = _recording_connection()
        erpl = ErplConnection(extensions=(), connection=connection)
        credentials = SapRfcCredentials(
            ashost="localhost", sysnr="00", client="001", user="DEVELOPER", password=PASSWORD
        )
        erpl._create_secret("s", "sap_rfc", credentials.secret_parameters())

        sql, bound = connection.execute.call_args[0]
        assert PASSWORD not in sql, "the password was formatted into the SQL text"
        assert bound["passwd"] == PASSWORD, "the password never reached the parameters"
        assert "$passwd" in sql

    def test_every_value_is_a_placeholder(self):
        connection = _recording_connection()
        erpl = ErplConnection(extensions=(), connection=connection)
        credentials = SapRfcCredentials(
            ashost="sap.example.com", sysnr="42", client="100", user="SVC_USER", password=PASSWORD
        )
        erpl._create_secret("s", "sap_rfc", credentials.secret_parameters())
        sql = connection.execute.call_args[0][0]
        for value in ("sap.example.com", "SVC_USER", PASSWORD):
            assert value not in sql


class TestODataSecret:
    def test_basic_auth_is_bound(self):
        connection = _recording_connection()
        erpl = ErplConnection(extensions=(), connection=connection)
        credentials = ODataCredentials(base_url="https://gw", username="u", password=PASSWORD)
        erpl._create_secret("h", credentials.secret_type, credentials.secret_parameters())
        sql, bound = connection.execute.call_args[0]
        assert PASSWORD not in sql and bound["password"] == PASSWORD

    def test_a_bearer_token_is_bound(self):
        connection = _recording_connection()
        erpl = ErplConnection(extensions=(), connection=connection)
        credentials = ODataCredentials(base_url="https://gw", token=PASSWORD)
        assert credentials.secret_type == "bearer"
        erpl._create_secret("h", credentials.secret_type, credentials.secret_parameters())
        sql, bound = connection.execute.call_args[0]
        assert PASSWORD not in sql and bound["token"] == PASSWORD
