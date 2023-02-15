from src.ingestion import get_connection, database_to_bucket_csv_file
import boto3
from moto import mock_s3
from unittest.mock import patch
import pytest
import os
import csv
import pg8000.native as pg
import testing.postgresql
from sqlalchemy import create_engine

conn = pg.Connection( )


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


def test_1(s3):
    s3.create_bucket(Bucket = 'test_ingestion_bucket')
    from src.ingestion import database_to_bucket_csv_file
    test_columns = [
      'column_id'
      'column_2',
      'column_3'
    ]
    database_to_bucket_csv_file('test_table', test_columns, 'test_ingestion_bucket', 'test.csv')







# sql_query = (
#             f'DROP DATABASE IF EXISTS test_raw_data;'
#             f'CREATE DATABASE test_raw_data;'
#             f'\c test_raw_data'
#             f'CREATE TABLE test_table' 
#                 f'('
#                 f'column_id SERIAL PRIMARY KEY,'
#                 f'column_2 VARCHAR NOT NULL,'
#                 f'column_3 INT NOT NULL'
#             f');'

#             f'INSERT INTO test_table ('
#                 f'column_2,'
#                 f'column_3'
#             f')'
#             f'VALUES'
#             f'("row 1", 1),'
#             f'("row 2", 2);'
# )









    

