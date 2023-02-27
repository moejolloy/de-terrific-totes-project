import boto3
from moto import mock_secretsmanager, mock_s3
from unittest.mock import patch
import pytest
import os
import botocore.errorfactory
import botocore.exceptions as be
import pg8000.exceptions as pge
import logging

logger = logging.getLogger("TestLogger")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# Test SecretsManager
@pytest.fixture
def secretsmanager(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="us-east-1")


@pytest.fixture
def create_secret(secretsmanager):
    secretsmanager.create_secret(
        Name="MySecret",
        SecretString='{"username":"username","password":"password"}'
    )


def test_will_return_dictionary_containing_correct_values(
    secretsmanager, create_secret
):
    from src.ingestion import get_secret_value

    secrets_dict = get_secret_value("MySecret")
    assert secrets_dict == {"password": "password", "username": "username"}


def test_will_throw_and_log_error_if_secret_name_not_found_in_secretsmanager(
    secretsmanager, create_secret, caplog
):
    from src.ingestion import get_secret_value

    with pytest.raises(botocore.errorfactory.ClientError):
        get_secret_value("NotMySecret")
    expected = "The requested secret NotMySecret was not found"
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == expected


def test_will_throw_and_log_error_if_secret_name_incorrect_type(
    secretsmanager, create_secret, caplog
):
    from src.ingestion import get_secret_value

    with pytest.raises(be.ParamValidationError):
        get_secret_value(8)

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "The request has invalid params"


def test_logging_all_other_errors(caplog):
    import src.ingestion

    with patch("src.ingestion.secrets.get_secret_value") as mock:
        response_error = Exception
        mock.side_effect = response_error

        with pytest.raises(RuntimeError):
            src.ingestion.get_secret_value("MySecret")

        assert caplog.records[0].levelno == logging.ERROR


MOCK_CREDS = {"host": "test_host",
              "port": "test_port",
              "user": "test_user",
              "password": "test_pass",
              "database": "test_db"}


# Test Get Connection
def test_get_connection_raises_error_if_key_not_in_secret(caplog):
    from src.ingestion import get_connection

    with pytest.raises(KeyError):
        get_connection(credentials={"test": "test"})

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == (
        "Credentials key not in secret")


def test_get_connection_raises_error_if_details_are_incorrect(caplog):
    from src.ingestion import get_connection

    with pytest.raises(pge.InterfaceError):
        get_connection(credentials=MOCK_CREDS)

    assert caplog.records[0].levelno == logging.ERROR


# Test SQL Helpers
def test_get_keys_from_table_names_applies_correct_suffix():
    from src.ingestion import get_keys_from_table_names

    result = get_keys_from_table_names(["table_1", "table_2", "table_3"])
    assert result == ["table_1.csv", "table_2.csv", "table_3.csv"]


columns123 = ["col_1", "col_2", "col_3"]


@patch("src.ingestion.Connection")
def test_sql_select_column_headers_returns_column_headers(mock_connection):

    from src.ingestion import sql_select_column_headers

    test_result = [{"name": "col_1"}, {"name": "col_2"}, {"name": "col_3"}]
    mock_connection().columns = test_result
    assert sql_select_column_headers(MOCK_CREDS, "table") == columns123


@patch("src.ingestion.Connection")
def test_select_column_headers_logs_error_if_table_does_not_exist(
    mock_connection, caplog
):
    from src.ingestion import sql_select_column_headers

    mock_connection().run.side_effect = pge.DatabaseError

    with pytest.raises(pge.DatabaseError):
        sql_select_column_headers(MOCK_CREDS, "test")

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == (
        "DatabaseError: test does not exist in database")


@patch(
    "src.ingestion.sql_select_column_headers", return_value=columns123)
def test_collect_column_headers_colates_lists_returned_from_sql(mock_sql):
    from src.ingestion import collect_column_headers

    result = collect_column_headers(MOCK_CREDS, ["table_1", "table_2"])
    assert result == [columns123, columns123]


@patch("src.ingestion.Connection")
def test_select_query_returns_list_of_row_data_from_database(mock_connection):
    from src.ingestion import sql_select_query

    test_result = [["Alex", 1], ["Rachael", 2], ["Joe", 3]]
    mock_connection().run.return_value = test_result
    assert sql_select_query(MOCK_CREDS, "table") == test_result


@patch("src.ingestion.Connection")
def test_select_query_logs_error_if_table_does_not_exist(
    mock_connection, caplog
):
    from src.ingestion import sql_select_query

    mock_connection().run.side_effect = pge.DatabaseError

    with pytest.raises(pge.DatabaseError):
        sql_select_query(MOCK_CREDS, "test")

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == (
        "DatabaseError: test does not exist in database")


@patch("src.ingestion.Connection")
def test_select_updated_returns_true_if_database_has_been_updated_at_interval(
    mock_connection,
):
    from src.ingestion import sql_select_updated

    test_result = [["some data", 1]]
    mock_connection().run.return_value = test_result
    assert sql_select_updated(MOCK_CREDS, "table", "2 days")


@patch("src.ingestion.Connection")
def test_select_updated_returns_false_if_database_is_not_updated_at_interval(
    mock_connection
):
    from src.ingestion import sql_select_updated

    test_result = []
    mock_connection().run.return_value = test_result
    assert not sql_select_updated(MOCK_CREDS, "table", "1 day")


@patch("src.ingestion.Connection")
def test_select_updated_logs_error_if_table_does_not_exist(
        mock_connection, caplog):
    from src.ingestion import sql_select_updated

    mock_connection().run.side_effect = pge.DatabaseError

    with pytest.raises(pge.DatabaseError):
        sql_select_updated(MOCK_CREDS, "test", 1)

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == (
        "DatabaseError: test does not exist in database")


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def bucket_name():
    return 'test_bucket'


@pytest.fixture
def s3_bucket(s3, bucket_name):
    s3.create_bucket(Bucket=bucket_name)


def test_check_key_exists_returns_true_if_key_in_s3(s3, s3_bucket):
    import src.ingestion

    name = 'test_bucket'
    key = 'test.csv'

    s3.put_object(Bucket=name, Key=key)
    assert src.ingestion.check_key_exists(name, key)


def test_check_key_exists_returns_false_if_no_key_in_s3(s3, s3_bucket):
    import src.ingestion

    with patch('src.ingestion.s3_client.head_object') as mock:
        mock.side_effect = boto3.client(
            's3').exceptions.NoSuchKey({}, '')

    assert not src.ingestion.check_key_exists('test_bucket', 'test.csv')
