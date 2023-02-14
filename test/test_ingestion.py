from src.ingestion import get_connection, data_table_to_csv, move_csv_to_bucket
import boto3
from moto import mock_s3
from unittest.mock import patch
import pytest
import os
import csv
import pg8000.native as pg

conn = pg.Connection('alex', password = 'password', database = 'test_raw_data')

data_table_to_csv(
                conn,
                'SELECT * FROM test_table;', 
                './test/test-ingestion/test.csv', 
                ['column_id','column_1','column_2']
                )


def test_formats_data_properly_in_csv():

    test_table_columns = ['column_id','column_1','column_2']
    with open('./test/test-ingestion/test.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            assert row == test_table_columns
            break
        for index, row in enumerate(reader):
            if index == 0:
                assert row == ['1', 'row 1', '1']
            if index == 1:
                assert row == ['2', 'row 2', '2']


def test_handles_unable_to_connect_to_db_error():
    with pytest.raises(Exception):
        assert get_connection('alx', 'pass', 'data', 'localhost')


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


def test_puts_csvs_to_bucket(s3, capsys):
    from src.ingestion import create_bucket
    BUCKET_NAME = 'test-bucket-00000009988'
    create_bucket()
    move_csv_to_bucket(BUCKET_NAME)
    captured = capsys.readouterr()
    
    assert captured.out == 'csv added to bucket\n'


    

