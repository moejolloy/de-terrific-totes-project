import boto3
from moto import mock_s3
from unittest.mock import patch
import pytest
import os
import botocore.errorfactory
import logging


BUCKET_NAME = "test_ingestion_bucket"
BUCKET_KEY = "test.csv"
MOCK_QUERY_RETURN = [[1, "row_1", 1], [2, "row_2", 2]]
TABLE_NAME = "test_table"
TABLE_COLUMNS = ["column_id", "column_2", "column_3"]

logger = logging.getLogger("TestLogger")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def bucket_name():
    return BUCKET_NAME


@pytest.fixture
def s3_bucket(s3, bucket_name):
    s3.create_bucket(Bucket=bucket_name)


# Test For Uploading To Bucket
@patch("src.ingestion.sql_select_query", return_value=MOCK_QUERY_RETURN)
def test_function_returns_correct_format_for_df(s3, s3_bucket):
    import src.ingestion

    result = src.ingestion.data_to_bucket_csv_file(
        "test_creds", TABLE_NAME, TABLE_COLUMNS, BUCKET_NAME, BUCKET_KEY
    )
    assert result == [
        {"column_id": 1, "column_2": "row_1", "column_3": 1},
        {"column_id": 2, "column_2": "row_2", "column_3": 2},
    ]


def test_file_will_be_uploaded_to_bucket(s3, s3_bucket):
    import src.ingestion

    with patch("src.ingestion.sql_select_query",
               return_value=MOCK_QUERY_RETURN):

        src.ingestion.data_to_bucket_csv_file(
            "test_creds", TABLE_NAME, TABLE_COLUMNS, BUCKET_NAME, BUCKET_KEY
        )

    obj_list = s3.list_objects_v2(Bucket=BUCKET_NAME)
    obj_list["Contents"]
    bucket_object_list = [item["Key"] for item in obj_list["Contents"]]
    assert bucket_object_list == ["test.csv"]


def test_file_in_bucket_has_correct_data(s3, s3_bucket):
    import src.ingestion

    with patch("src.ingestion.sql_select_query",
               return_value=MOCK_QUERY_RETURN):
        src.ingestion.data_to_bucket_csv_file(
            "test_creds", TABLE_NAME, TABLE_COLUMNS, BUCKET_NAME, BUCKET_KEY
        )

    data = s3.get_object(Bucket=BUCKET_NAME, Key="test.csv")["Body"].read()
    assert data == b"column_id,column_2,column_3\n1,row_1,1\n2,row_2,2\n"


@patch("src.ingestion.sql_select_query", return_value=MOCK_QUERY_RETURN)
def test_function_raises_and_logs_error_if_bucket_does_not_exist(s3, s3_bucket,
                                                                 caplog):
    import src.ingestion

    with pytest.raises(botocore.errorfactory.ClientError):
        src.ingestion.data_to_bucket_csv_file(
            "test_creds", TABLE_NAME, TABLE_COLUMNS, "no_bucket", BUCKET_KEY
        )

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "no_bucket does not exist in your S3"


@patch("src.ingestion.sql_select_query", return_value=MOCK_QUERY_RETURN)
def test_function_raises_and_logs_error_if_bucket_key_invalid(s3, s3_bucket,
                                                              caplog):
    import src.ingestion

    with pytest.raises(botocore.exceptions.ParamValidationError):
        src.ingestion.data_to_bucket_csv_file(
            "test_creds", TABLE_NAME, TABLE_COLUMNS, BUCKET_NAME, 5
        )

    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "The request has invalid params"


@patch("src.ingestion.get_secret_value")
@patch("src.ingestion.Connection")
@patch("src.ingestion.data_to_bucket_csv_file")
@patch("src.ingestion.check_key_exists", return_value=False)
@patch("src.ingestion.sql_select_updated", return_value=False)
def test_function_uploads_data_for_first_time_on_s3(
    mock_sql, mock_no_key, mock_upload_function, mock_connection,
    mock_secret, caplog
):
    import src.ingestion

    test_query = []
    mock_connection().run.return_value = test_query
    test_headers = []
    mock_connection().columns = test_headers

    src.ingestion.lambda_handler({}, {})
    assert mock_upload_function.call_count == 11

    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == "SUCCESSFUL INGESTION"


@patch("src.ingestion.get_secret_value")
@patch("src.ingestion.Connection")
@patch("src.ingestion.data_to_bucket_csv_file")
@patch("src.ingestion.check_key_exists", return_value=True)
@patch("src.ingestion.sql_select_updated", return_value=True)
def test_function_uploads_data_if_updated_is_true(
    mock_sql, mock_key, mock_upload_function, mock_connection,
    mock_secret, caplog
):
    import src.ingestion

    test_query = []
    mock_connection().run.return_value = test_query
    test_headers = []
    mock_connection().columns = test_headers

    src.ingestion.lambda_handler({}, {})
    assert mock_upload_function.call_count == 11

    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == "SUCCESSFUL INGESTION"


@patch("src.ingestion.get_secret_value")
@patch("src.ingestion.Connection")
@patch("src.ingestion.data_to_bucket_csv_file")
@patch("src.ingestion.check_key_exists", return_value=True)
@patch("src.ingestion.sql_select_updated", return_value=False)
def test_function_does_not_upload_data_if_updated_is_false(
    mock_sql, mock_key, mock_upload_function, mock_connection,
    mock_secret, caplog
):
    import src.ingestion

    test_query = []
    mock_connection().run.return_value = test_query
    test_headers = []
    mock_connection().columns = test_headers

    src.ingestion.lambda_handler({}, {})
    assert mock_upload_function.call_count == 0

    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == "NO FILES TO UPDATE"
