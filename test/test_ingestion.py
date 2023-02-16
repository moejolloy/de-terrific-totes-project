import boto3
from moto import mock_s3
from unittest.mock import patch
import pytest
import os
import botocore.errorfactory
import botocore.exceptions as be
import logging

BUCKET_NAME = 'test_ingestion_bucket'
BUCKET_KEY = 'test.csv'
MOCK_QUERY_RETURN = [[1, 'row_1', 1], [2, 'row_2', 2]]
TABLE_NAME = 'test_table'
TABLE_COLUMNS = ['column_id', 'column_2', 'column_3']

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
    s3.create_bucket(Bucket = bucket_name)


# Test For Uploading To Bucket
@patch('src.ingestion.sql_get_all_data', return_value = MOCK_QUERY_RETURN)
def test_function_returns_correct_format_for_df(s3, s3_bucket):
    import src.ingestion


    result = src.ingestion.data_to_bucket_csv_file(
                                                    TABLE_NAME, 
                                                    TABLE_COLUMNS, 
                                                    BUCKET_NAME, 
                                                    BUCKET_KEY)
    assert result == [
            {'column_id': 1, 'column_2': 'row_1', 'column_3': 1}, 
            {'column_id': 2, 'column_2': 'row_2', 'column_3': 2}]


def test_file_will_be_uploaded_to_bucket(s3, s3_bucket):
    import src.ingestion


    with patch('src.ingestion.sql_get_all_data', return_value = MOCK_QUERY_RETURN):
        src.ingestion.data_to_bucket_csv_file(
                                                TABLE_NAME, 
                                                TABLE_COLUMNS, 
                                                BUCKET_NAME, 
                                                BUCKET_KEY)
    
    obj_list = s3.list_objects_v2(Bucket = BUCKET_NAME)
    obj_list['Contents']
    bucket_object_list=[item['Key'] for item in obj_list['Contents']]
    assert bucket_object_list == ['test.csv']


def test_file_in_bucket_has_correct_data(s3, s3_bucket):
    import src.ingestion


    with patch('src.ingestion.sql_get_all_data', return_value = MOCK_QUERY_RETURN):
        src.ingestion.data_to_bucket_csv_file(
                                                TABLE_NAME, 
                                                TABLE_COLUMNS, 
                                                BUCKET_NAME, 
                                                BUCKET_KEY)

    data = s3.get_object(Bucket=BUCKET_NAME, Key='test.csv')['Body'].read()
    assert data == b',column_id,column_2,column_3\n0,1,row_1,1\n1,2,row_2,2\n'


def test_connection():
    import src.ingestion

    with patch('src.ingestion.conn.run') as mock:
        mock.return_value = []
    assert src.ingestion.sql_get_column_headers(['column_1', 'column_2']) == []
