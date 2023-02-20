from unittest.mock import patch
import io
from botocore.response import StreamingBody
import boto3
import pandas as pd
import src.utils
import pytest
import logging


def test_load_csv_from_s3_reads_csv_file_from_s3_and_returns_dataframe():
    with patch('src.utils.s3.get_object') as mock:
        read_file = b'ID\n1\n2'
        body = StreamingBody(io.BytesIO(read_file), len(read_file))
        response = {'Body': body}
        mock.return_value = response
        data = {"ID": [1, 2]}
        df = pd.DataFrame(data=data)
        assert src.utils.load_csv_from_s3("", "").equals(df)


def test_load_csv_from_s3_reads_more_complex_csv_file_from_s3():
    with patch('src.utils.s3.get_object') as mock:
        read_file = b'ID,Name,Age\n1,Sam,31\n2,Joe,14\n3,Max,44'
        body = StreamingBody(io.BytesIO(read_file), len(read_file))
        response = {'Body': body}
        mock.return_value = response
        data = {"ID": [1, 2, 3], "Name": [
            "Sam", "Joe", "Max"], "Age": [31, 14, 44]}
        df = pd.DataFrame(data=data)
        assert src.utils.load_csv_from_s3("", "").equals(df)


def test_load_csv_from_s3_NoSuchBucket_error_handled():
    with patch('src.utils.s3.get_object') as mock:
        response_error = boto3.client(
            's3').exceptions.NoSuchBucket({}, '')
        mock.side_effect = response_error
        with pytest.raises(boto3.client(
                's3').exceptions.NoSuchBucket):
            src.utils.load_csv_from_s3('Test', 'Test')


def test_load_csv_from_s3_NoSuchKey_error_handled():
    with patch('src.utils.s3.get_object') as mock:
        response_error = boto3.client(
            's3').exceptions.NoSuchKey({}, '')
        mock.side_effect = response_error
        with pytest.raises(boto3.client(
                's3').exceptions.NoSuchKey):
            src.utils.load_csv_from_s3('Test', 'Test')


def test_load_csv_from_s3_other_errors_handled():
    with patch('src.utils.s3.get_object') as mock:
        response_error = Exception
        mock.side_effect = response_error
        with pytest.raises(RuntimeError):
            src.utils.load_csv_from_s3('Test', 'Test')


logger = logging.getLogger("TestLogger")


def test_logging_NoSuchBucket_error(caplog):
    with patch('src.utils.s3.get_object') as mock:
        response_error = boto3.client(
            's3').exceptions.NoSuchBucket({}, '')
        mock.side_effect = response_error
        with pytest.raises(Exception):
            src.utils.load_csv_from_s3('Test', 'Test')
        assert caplog.records[0].levelno == logging.CRITICAL
        assert caplog.records[0].msg == "Bucket does not exist"


def test_logging_NoSuchKey_error(caplog):
    with patch('src.utils.s3.get_object') as mock:
        response_error = boto3.client(
            's3').exceptions.NoSuchKey({}, '')
        mock.side_effect = response_error
        with pytest.raises(Exception):
            src.utils.load_csv_from_s3('Test', 'Test')
        assert caplog.records[0].levelno == logging.CRITICAL
        assert caplog.records[0].msg == "Key not found in bucket"


def test_logging_other_errors_get_object(caplog):
    with patch('src.utils.s3.get_object') as mock:
        response_error = Exception
        mock.side_effect = response_error
        with pytest.raises(RuntimeError):
            src.utils.load_csv_from_s3('Test', 'Test')
        assert caplog.records[0].levelno == logging.CRITICAL


def test_export_parquet_to_s3_FileNotFoundError_handled():
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = FileNotFoundError
        mock.side_effect = response_error
        with pytest.raises(FileNotFoundError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')


def test_export_parquet_to_s3_ValueError_handled():
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = ValueError
        mock.side_effect = response_error
        with pytest.raises(ValueError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')


def test_export_parquet_to_s3_AttributeError_handled():
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = AttributeError
        mock.side_effect = response_error
        with pytest.raises(AttributeError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')


def test_export_parquet_to_s3_other_errors_handled():
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = Exception
        mock.side_effect = response_error
        with pytest.raises(RuntimeError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')


def test_logging_FileNotFoundError(caplog):
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = FileNotFoundError
        mock.side_effect = response_error
        with pytest.raises(FileNotFoundError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')
        assert caplog.records[0].levelno == logging.CRITICAL
        assert caplog.records[0].msg == "Bucket not found."


def test_logging_ValueError(caplog):
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = ValueError
        mock.side_effect = response_error
        with pytest.raises(ValueError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')
        assert caplog.records[0].levelno == logging.CRITICAL
        assert caplog.records[0].msg == "Key not of correct format."


def test_logging_AttributeError(caplog):
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = AttributeError
        mock.side_effect = response_error
        with pytest.raises(AttributeError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')
        assert caplog.records[0].levelno == logging.CRITICAL
        assert caplog.records[0].msg == "Object passed to the function is"
        + "not of type DataFrame."


def test_logging_other_errors_to_parquest(caplog):
    with patch('src.utils.pd.DataFrame.to_parquet') as mock:
        response_error = RuntimeError
        mock.side_effect = response_error
        with pytest.raises(RuntimeError):
            src.utils.export_parquet_to_s3(pd.DataFrame, '', '')
        assert caplog.records[0].levelno == logging.CRITICAL
